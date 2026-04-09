use colored::*;
use dialoguer::{Input, Password};
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Instant;

#[cfg(unix)]
fn is_running_as_root() -> bool {
    unsafe { libc::geteuid() == 0 }
}

#[cfg(not(unix))]
fn is_running_as_root() -> bool {
    false
}

use crate::auth::{AuthManager, Role, User};
use crate::cli::handler;
use crate::db::DatabaseManager;
use crate::storage::StorageEngine;

pub struct Shell {
    pub user: User,
    pub auth: AuthManager,
    pub db_manager: DatabaseManager,
    pub current_db: Option<String>,
    pub current_tx: Option<String>,
    _data_dir: PathBuf,
}

impl Shell {
    pub fn data_dir() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".vantadb")
    }

    pub fn init() -> io::Result<(StorageEngine, AuthManager, DatabaseManager)> {
        let data_dir = Self::data_dir();
        let engine = StorageEngine::open(&data_dir.join("system"))?;
        let auth = AuthManager::new(engine)?;
        let db_manager = DatabaseManager::new(&data_dir.join("data"))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok((StorageEngine::open(&data_dir.join("system"))?, auth, db_manager))
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

        if username == "root" {
            // Root login requires running as system root (sudo)
            if !is_running_as_root() {
                println!();
                println!(
                    "  {} {}",
                    "✗".red().bold(),
                    "Root login requires elevated privileges. Run with sudo.".red()
                );
                println!(
                    "  {} {}",
                    "ℹ".truecolor(120, 80, 255),
                    "sudo vantadb --login".dimmed()
                );
                return Ok(None);
            }

            match auth.authenticate("root", "")? {
                Some(user) => {
                    println!();
                    println!(
                        "  {} Authenticated as {} {}",
                        "✓".green().bold(),
                        "root".bold().truecolor(255, 180, 50),
                        "(superuser)".dimmed()
                    );
                    Ok(Some(user))
                }
                None => {
                    println!("  {} {}", "✗".red().bold(), "Authentication failed".red());
                    Ok(None)
                }
            }
        } else {
            let password = Password::new()
                .with_prompt(format!("  {} {}", "▸".truecolor(120, 80, 255), "Password"))
                .interact()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            match auth.authenticate(&username, &password)? {
                Some(user) => {
                    println!();
                    println!(
                        "  {} Authenticated as {} {}",
                        "✓".green().bold(),
                        user.username.bold().cyan(),
                        format!("({})", user.role).dimmed()
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
    }

    pub fn new(user: User, auth: AuthManager, db_manager: DatabaseManager) -> Self {
        Self {
            user,
            auth,
            db_manager,
            current_db: None,
            current_tx: None,
            _data_dir: Self::data_dir(),
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

        loop {
            let prompt = self.build_prompt();
            print!("{}", prompt);
            io::stdout().flush()?;

            let mut input = String::new();
            match io::stdin().read_line(&mut input) {
                Ok(0) => break, // EOF
                Ok(_) => {}
                Err(e) => {
                    eprintln!("  {} Read error: {}", "✗".red().bold(), e);
                    continue;
                }
            }

            let input = input.trim();
            if input.is_empty() {
                continue;
            }

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

        let arrow = "❯".truecolor(120, 80, 255).bold();

        format!("  {}{} {} ", user_part, db_part, arrow)
    }
}
