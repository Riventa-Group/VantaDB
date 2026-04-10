use colored::*;
use dialoguer::{Input, Password};
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::io;
use std::path::PathBuf;
use std::time::Instant;

use crate::auth::{AclManager, AuthManager, CertManager, Role, User};
use crate::cli::completer::VantaCompleter;
use crate::cli::handler;
use crate::db::DatabaseManager;
use crate::server::audit::AuditLogger;
use crate::server::metrics::MetricsCollector;
use crate::storage::StorageEngine;

pub struct Shell {
    pub user: User,
    pub auth: AuthManager,
    pub db_manager: DatabaseManager,
    pub acl_manager: AclManager,
    pub cert_manager: CertManager,
    pub audit_logger: AuditLogger,
    pub metrics: MetricsCollector,
    pub current_db: Option<String>,
    pub current_tx: Option<String>,
    pub data_dir: PathBuf,
}

impl Shell {
    pub fn data_dir() -> PathBuf {
        if let Ok(dir) = std::env::var("VANTADB_DATA_DIR") {
            return PathBuf::from(dir);
        }
        let new_path = dirs::data_dir()
            .unwrap_or_else(|| dirs::home_dir().unwrap_or_else(|| PathBuf::from(".")))
            .join("vantadb");

        // Warn if legacy path exists but new path doesn't
        let legacy_path = dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".vantadb");
        if legacy_path.exists() && !new_path.exists() {
            eprintln!(
                "  {} Legacy data directory {} detected.",
                "⚠".truecolor(255, 180, 50),
                legacy_path.display()
            );
            eprintln!(
                "    VantaDB now uses {}",
                new_path.display()
            );
            eprintln!(
                "    To migrate: mv {} {}",
                legacy_path.display(),
                new_path.display()
            );
        }

        // Use legacy path if it exists and new path doesn't (backward compat)
        if legacy_path.exists() && !new_path.exists() {
            return legacy_path;
        }

        new_path
    }

    pub fn init() -> io::Result<(StorageEngine, AuthManager, DatabaseManager, AclManager, CertManager, AuditLogger)> {
        let data_dir = Self::data_dir();
        let engine = std::sync::Arc::new(StorageEngine::open(&data_dir.join("system"))?);
        let auth = AuthManager::new(StorageEngine::open(&data_dir.join("system"))?)?;
        let db_manager = DatabaseManager::new(&data_dir.join("data"))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let acl_manager = AclManager::new(std::sync::Arc::clone(&engine))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let cert_manager = CertManager::bootstrap(&data_dir, std::sync::Arc::clone(&engine))?;
        let audit_logger = AuditLogger::new(std::sync::Arc::clone(&engine))?;
        Ok((StorageEngine::open(&data_dir.join("system"))?, auth, db_manager, acl_manager, cert_manager, audit_logger))
    }

    pub fn print_banner() {
        println!();
        println!(
            "{}",
            "  ╦  ╦┌─┐┌┐┌┌┬┐┌─┐╔╦╗╔╗ "
                .bold()
                .truecolor(120, 80, 255)
        );
        println!(
            "{}",
            "  ╚╗╔╝├─┤│││ │ ├─┤ ║║╠╩╗"
                .bold()
                .truecolor(140, 90, 255)
        );
        println!(
            "{}",
            "   ╚╝ ┴ ┴┘└┘ ┴ ┴ ┴═╩╝╚═╝"
                .bold()
                .truecolor(160, 100, 255)
        );
        println!();
        println!(
            "  {}  {}",
            format!("v{}", env!("CARGO_PKG_VERSION")).dimmed(),
            "| Next-Gen Database Engine".dimmed()
        );
        println!(
            "  {}",
            "─────────────────────────────".truecolor(60, 60, 80)
        );
        println!();
    }

    pub fn login(auth: &AuthManager) -> io::Result<Option<User>> {
        println!(
            "  {} {}",
            "→".truecolor(120, 80, 255),
            "Authentication Required".bold()
        );
        println!();

        let username: String = Input::new()
            .with_prompt(format!("  {} {}", "▸".truecolor(120, 80, 255), "Username"))
            .interact_text()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let username = username.trim().to_string();

        let password = Password::new()
            .with_prompt(format!("  {} {}", "▸".truecolor(120, 80, 255), "Password"))
            .interact()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        match auth.authenticate(&username, &password)? {
            Some(user) => {
                println!();
                let role_display = if user.role.is_root() {
                    "(superuser)".to_string()
                } else {
                    format!("({})", user.role)
                };
                println!(
                    "  {} Authenticated as {} {}",
                    "✓".green().bold(),
                    user.username.bold().cyan(),
                    role_display.dimmed()
                );
                Ok(Some(user))
            }
            None => {
                println!();
                println!(
                    "  {} {}",
                    "✗".red().bold(),
                    "Invalid username or password".red()
                );
                Ok(None)
            }
        }
    }

    pub fn new(
        user: User,
        auth: AuthManager,
        db_manager: DatabaseManager,
        acl_manager: AclManager,
        cert_manager: CertManager,
        audit_logger: AuditLogger,
    ) -> Self {
        Self {
            user,
            auth,
            db_manager,
            acl_manager,
            cert_manager,
            audit_logger,
            metrics: MetricsCollector::new(),
            current_db: None,
            current_tx: None,
            data_dir: Self::data_dir(),
        }
    }

    pub fn run(&mut self) -> io::Result<()> {
        println!();
        println!(
            "  {} Type {} for available commands",
            "ℹ".truecolor(120, 80, 255),
            "help".bold().truecolor(120, 80, 255)
        );
        println!();

        let mut completer = VantaCompleter::new();
        completer.update_databases(self.db_manager.list_databases());

        let mut rl = Editor::new().map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        rl.set_helper(Some(completer));

        // Load persistent history
        let history_path = self.data_dir.join("history.txt");
        let _ = rl.load_history(&history_path);

        loop {
            let prompt = self.build_prompt();

            // Update completer with current databases and collections
            if let Some(helper) = rl.helper_mut() {
                helper.update_databases(self.db_manager.list_databases());
                if let Some(ref db) = self.current_db {
                    if let Ok(cols) = self.db_manager.list_collections(db) {
                        helper.update_collections(cols);
                    }
                }
            }

            match rl.readline(&prompt) {
                Ok(line) => {
                    let input = line.trim();
                    if input.is_empty() {
                        continue;
                    }

                    rl.add_history_entry(input)
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

                    let start = Instant::now();
                    let should_exit = handler::handle_command(self, input)?;
                    let elapsed = start.elapsed();

                    if elapsed.as_millis() > 0 {
                        println!(
                            "  {}",
                            format!("({:.2}ms)", elapsed.as_secs_f64() * 1000.0).dimmed()
                        );
                    }

                    if should_exit {
                        println!();
                        println!(
                            "  {} {}",
                            "←".truecolor(120, 80, 255),
                            "Goodbye.".dimmed()
                        );
                        println!();
                        break;
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    // Ctrl+C — ignore, print new prompt
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    // Ctrl+D — exit
                    println!();
                    println!(
                        "  {} {}",
                        "←".truecolor(120, 80, 255),
                        "Goodbye.".dimmed()
                    );
                    println!();
                    break;
                }
                Err(e) => {
                    eprintln!("  {} Read error: {}", "✗".red().bold(), e);
                    break;
                }
            }
        }

        // Save history
        let _ = rl.save_history(&history_path);

        Ok(())
    }

    fn build_prompt(&self) -> String {
        let user_part = if self.user.role == Role::Root {
            self.user
                .username
                .truecolor(255, 180, 50)
                .bold()
                .to_string()
        } else {
            self.user.username.cyan().bold().to_string()
        };

        let db_part = match &self.current_db {
            Some(db) => format!("/{}", db.truecolor(120, 200, 120)),
            None => String::new(),
        };

        let tx_part = if self.current_tx.is_some() {
            format!(" {}", "[tx]".truecolor(255, 180, 50))
        } else {
            String::new()
        };

        let arrow = "❯".truecolor(120, 80, 255).bold();

        format!("  {}{}{} {} ", user_part, db_part, tx_part, arrow)
    }
}
