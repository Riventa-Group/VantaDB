use colored::*;
use dialoguer::Password;
use serde_json::Value;
use std::io;

use crate::auth::Role;
use crate::bench;
use crate::cli::Shell;

pub fn handle_command(shell: &mut Shell, input: &str) -> io::Result<bool> {
    let parts: Vec<&str> = input.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(false);
    }

    match parts[0].to_lowercase().as_str() {
        "help" => cmd_help(shell),
        "exit" | "quit" | "q" => return Ok(true),
        "clear" | "cls" => cmd_clear(),
        "whoami" => cmd_whoami(shell),
        "use" => cmd_use(shell, &parts),
        "show" => cmd_show(shell, &parts),
        "create" => cmd_create(shell, &parts),
        "drop" => cmd_drop(shell, &parts),
        "insert" => cmd_insert(shell, &parts),
        "find" => cmd_find(shell, &parts),
        "delete" => cmd_delete(shell, &parts),
        "count" => cmd_count(shell, &parts),
        "users" => cmd_users(shell),
        "adduser" => cmd_adduser(shell, &parts),
        "rmuser" => cmd_rmuser(shell, &parts),
        "passwd" => cmd_passwd(shell, &parts),
        "status" => cmd_status(shell),
        "benchmark" | "bench" => cmd_benchmark(),
        _ => {
            println!(
                "  {} Unknown command: {}. Type {} for help.",
                "✗".red().bold(),
                parts[0].yellow(),
                "help".bold().truecolor(120, 80, 255)
            );
        }
    }

    Ok(false)
}

fn cmd_help(_shell: &Shell) {
    println!();
    println!("  {}", "COMMANDS".bold().truecolor(120, 80, 255));
    println!("  {}", "─────────────────────────────────────────────".truecolor(60, 60, 80));
    println!();

    let cmds = [
        ("Navigation", vec![
            ("use <db>", "Switch to a database"),
            ("show dbs", "List all databases"),
            ("show collections", "List collections in current db"),
            ("status", "Show server status"),
        ]),
        ("Database", vec![
            ("create db <name>", "Create a new database"),
            ("drop db <name>", "Delete a database"),
            ("create collection <n>", "Create a collection"),
            ("drop collection <name>", "Delete a collection"),
        ]),
        ("Documents", vec![
            ("insert <col> <json>", "Insert a document"),
            ("find <col>", "Find all documents"),
            ("find <col> <id>", "Find document by ID"),
            ("find <col> where <f> = <v>", "Find by field value"),
            ("delete <col> <id>", "Delete a document"),
            ("count <col>", "Count documents"),
        ]),
        ("Users", vec![
            ("users", "List all users"),
            ("adduser <name> <role>", "Create a user"),
            ("rmuser <name>", "Delete a user"),
            ("passwd <name>", "Change password"),
            ("whoami", "Show current user"),
        ]),
        ("General", vec![
            ("benchmark", "Run full benchmark suite"),
            ("clear", "Clear the screen"),
            ("exit", "Exit VantaDB"),
        ]),
    ];

    for (section, entries) in cmds {
        println!("  {}", section.bold().dimmed());
        for (cmd, desc) in entries {
            println!(
                "    {:<28} {}",
                cmd.truecolor(120, 200, 120),
                desc.dimmed()
            );
        }
        println!();
    }
}

fn cmd_clear() {
    print!("\x1B[2J\x1B[1;1H");
}

fn cmd_whoami(shell: &Shell) {
    let role_color = match shell.user.role {
        Role::Root => shell.user.role.to_string().truecolor(255, 180, 50),
        Role::Admin => shell.user.role.to_string().truecolor(255, 100, 100),
        _ => shell.user.role.to_string().cyan(),
    };
    println!(
        "  {} {} ({})",
        "▸".truecolor(120, 80, 255),
        shell.user.username.bold(),
        role_color
    );
}

fn cmd_use(shell: &mut Shell, parts: &[&str]) {
    if parts.len() < 2 {
        println!("  {} Usage: {}", "✗".red().bold(), "use <database>".yellow());
        return;
    }
    let db_name = parts[1];
    if shell.db_manager.database_exists(db_name) {
        shell.current_db = Some(db_name.to_string());
        println!(
            "  {} Switched to database {}",
            "✓".green().bold(),
            db_name.bold().truecolor(120, 200, 120)
        );
    } else {
        println!(
            "  {} Database '{}' not found",
            "✗".red().bold(),
            db_name.yellow()
        );
    }
}

fn cmd_show(shell: &Shell, parts: &[&str]) {
    if parts.len() < 2 {
        println!(
            "  {} Usage: {} or {}",
            "✗".red().bold(),
            "show dbs".yellow(),
            "show collections".yellow()
        );
        return;
    }

    match parts[1] {
        "dbs" | "databases" => {
            let dbs = shell.db_manager.list_databases();
            if dbs.is_empty() {
                println!("  {} No databases found", "ℹ".truecolor(120, 80, 255));
            } else {
                println!();
                println!("  {}", "DATABASES".bold().dimmed());
                for db in &dbs {
                    let marker = if shell.current_db.as_deref() == Some(db) {
                        "●".green().to_string()
                    } else {
                        "○".dimmed().to_string()
                    };
                    println!("  {} {}", marker, db.truecolor(120, 200, 120));
                }
                println!();
                println!("  {}", format!("{} database(s)", dbs.len()).dimmed());
            }
        }
        "collections" | "cols" => {
            let db = match &shell.current_db {
                Some(db) => db.clone(),
                None => {
                    println!(
                        "  {} No database selected. Use {}",
                        "✗".red().bold(),
                        "use <database>".yellow()
                    );
                    return;
                }
            };
            match shell.db_manager.list_collections(&db) {
                Ok(Ok(cols)) => {
                    if cols.is_empty() {
                        println!("  {} No collections in '{}'", "ℹ".truecolor(120, 80, 255), db);
                    } else {
                        println!();
                        println!("  {} {}", "COLLECTIONS IN".bold().dimmed(), db.bold().dimmed());
                        for col in &cols {
                            println!("  {} {}", "○".dimmed(), col.cyan());
                        }
                        println!();
                        println!("  {}", format!("{} collection(s)", cols.len()).dimmed());
                    }
                }
                Ok(Err(e)) => println!("  {} {}", "✗".red().bold(), e.red()),
                Err(e) => println!("  {} {}", "✗".red().bold(), e.to_string().red()),
            }
        }
        _ => {
            println!(
                "  {} Unknown: show {}. Try {} or {}",
                "✗".red().bold(),
                parts[1].yellow(),
                "show dbs".truecolor(120, 200, 120),
                "show collections".truecolor(120, 200, 120)
            );
        }
    }
}

fn cmd_create(shell: &Shell, parts: &[&str]) {
    if parts.len() < 3 {
        println!(
            "  {} Usage: {} or {}",
            "✗".red().bold(),
            "create db <name>".yellow(),
            "create collection <name>".yellow()
        );
        return;
    }

    match parts[1] {
        "db" | "database" => {
            if !shell.user.role.can_admin() {
                println!("  {} Permission denied", "✗".red().bold());
                return;
            }
            match shell.db_manager.create_database(parts[2]) {
                Ok(Ok(())) => println!(
                    "  {} Database '{}' created",
                    "✓".green().bold(),
                    parts[2].bold().truecolor(120, 200, 120)
                ),
                Ok(Err(e)) => println!("  {} {}", "✗".red().bold(), e.yellow()),
                Err(e) => println!("  {} {}", "✗".red().bold(), e.to_string().red()),
            }
        }
        "collection" | "col" => {
            if !shell.user.role.can_write() {
                println!("  {} Permission denied", "✗".red().bold());
                return;
            }
            let db = match &shell.current_db {
                Some(db) => db.clone(),
                None => {
                    println!(
                        "  {} No database selected. Use {}",
                        "✗".red().bold(),
                        "use <database>".yellow()
                    );
                    return;
                }
            };
            match shell.db_manager.create_collection(&db, parts[2]) {
                Ok(Ok(())) => println!(
                    "  {} Collection '{}' created in '{}'",
                    "✓".green().bold(),
                    parts[2].bold().cyan(),
                    db
                ),
                Ok(Err(e)) => println!("  {} {}", "✗".red().bold(), e.yellow()),
                Err(e) => println!("  {} {}", "✗".red().bold(), e.to_string().red()),
            }
        }
        _ => {
            println!(
                "  {} Unknown: create {}. Try {} or {}",
                "✗".red().bold(),
                parts[1].yellow(),
                "create db".truecolor(120, 200, 120),
                "create collection".truecolor(120, 200, 120)
            );
        }
    }
}

fn cmd_drop(shell: &mut Shell, parts: &[&str]) {
    if parts.len() < 3 {
        println!(
            "  {} Usage: {} or {}",
            "✗".red().bold(),
            "drop db <name>".yellow(),
            "drop collection <name>".yellow()
        );
        return;
    }

    if !shell.user.role.can_admin() {
        println!("  {} Permission denied", "✗".red().bold());
        return;
    }

    match parts[1] {
        "db" | "database" => {
            match shell.db_manager.drop_database(parts[2]) {
                Ok(Ok(())) => {
                    if shell.current_db.as_deref() == Some(parts[2]) {
                        shell.current_db = None;
                    }
                    println!(
                        "  {} Database '{}' dropped",
                        "✓".green().bold(),
                        parts[2].bold()
                    );
                }
                Ok(Err(e)) => println!("  {} {}", "✗".red().bold(), e.yellow()),
                Err(e) => println!("  {} {}", "✗".red().bold(), e.to_string().red()),
            }
        }
        "collection" | "col" => {
            let db = match &shell.current_db {
                Some(db) => db.clone(),
                None => {
                    println!(
                        "  {} No database selected. Use {}",
                        "✗".red().bold(),
                        "use <database>".yellow()
                    );
                    return;
                }
            };
            match shell.db_manager.drop_collection(&db, parts[2]) {
                Ok(Ok(())) => println!(
                    "  {} Collection '{}' dropped from '{}'",
                    "✓".green().bold(),
                    parts[2].bold(),
                    db
                ),
                Ok(Err(e)) => println!("  {} {}", "✗".red().bold(), e.yellow()),
                Err(e) => println!("  {} {}", "✗".red().bold(), e.to_string().red()),
            }
        }
        _ => {
            println!("  {} Unknown: drop {}", "✗".red().bold(), parts[1].yellow());
        }
    }
}

fn cmd_insert(shell: &Shell, parts: &[&str]) {
    if parts.len() < 3 {
        println!(
            "  {} Usage: {}",
            "✗".red().bold(),
            "insert <collection> {\"key\": \"value\"}".yellow()
        );
        return;
    }

    if !shell.user.role.can_write() {
        println!("  {} Permission denied", "✗".red().bold());
        return;
    }

    let db = match &shell.current_db {
        Some(db) => db.clone(),
        None => {
            println!(
                "  {} No database selected. Use {}",
                "✗".red().bold(),
                "use <database>".yellow()
            );
            return;
        }
    };

    let collection = parts[1];
    let json_str: String = parts[2..].join(" ");

    let doc: Value = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => {
            println!("  {} Invalid JSON: {}", "✗".red().bold(), e.to_string().red());
            return;
        }
    };

    if !doc.is_object() {
        println!("  {} Document must be a JSON object", "✗".red().bold());
        return;
    }

    match shell.db_manager.insert(&db, collection, doc) {
        Ok(Ok(id)) => println!(
            "  {} Inserted with _id: {}",
            "✓".green().bold(),
            id.truecolor(180, 180, 255)
        ),
        Ok(Err(e)) => println!("  {} {}", "✗".red().bold(), e.yellow()),
        Err(e) => println!("  {} {}", "✗".red().bold(), e.to_string().red()),
    }
}

fn cmd_find(shell: &Shell, parts: &[&str]) {
    if parts.len() < 2 {
        println!(
            "  {} Usage: {}, {}, or {}",
            "✗".red().bold(),
            "find <collection>".yellow(),
            "find <collection> <id>".yellow(),
            "find <col> where <field> = <value>".yellow()
        );
        return;
    }

    let db = match &shell.current_db {
        Some(db) => db.clone(),
        None => {
            println!(
                "  {} No database selected. Use {}",
                "✗".red().bold(),
                "use <database>".yellow()
            );
            return;
        }
    };

    let collection = parts[1];

    // find <col> where <field> = <value>
    if parts.len() >= 5 && parts[2] == "where" {
        let field = parts[3];
        if parts.len() < 5 || (parts.len() >= 5 && parts[4] != "=") {
            println!(
                "  {} Usage: {}",
                "✗".red().bold(),
                "find <col> where <field> = <value>".yellow()
            );
            return;
        }
        let value_str = parts[5..].join(" ");
        let value: Value = serde_json::from_str(&value_str)
            .unwrap_or(Value::String(value_str));

        match shell.db_manager.find_where(&db, collection, field, &value) {
            Ok(Ok(docs)) => print_documents(&docs),
            Ok(Err(e)) => println!("  {} {}", "✗".red().bold(), e.yellow()),
            Err(e) => println!("  {} {}", "✗".red().bold(), e.to_string().red()),
        }
        return;
    }

    // find <col> <id>
    if parts.len() == 3 {
        match shell.db_manager.find_by_id(&db, collection, parts[2]) {
            Ok(Ok(Some(doc))) => print_documents(&[doc]),
            Ok(Ok(None)) => println!("  {} Document not found", "ℹ".truecolor(120, 80, 255)),
            Ok(Err(e)) => println!("  {} {}", "✗".red().bold(), e.yellow()),
            Err(e) => println!("  {} {}", "✗".red().bold(), e.to_string().red()),
        }
        return;
    }

    // find <col> (all)
    match shell.db_manager.find_all(&db, collection) {
        Ok(Ok(docs)) => print_documents(&docs),
        Ok(Err(e)) => println!("  {} {}", "✗".red().bold(), e.yellow()),
        Err(e) => println!("  {} {}", "✗".red().bold(), e.to_string().red()),
    }
}

fn print_documents(docs: &[Value]) {
    if docs.is_empty() {
        println!("  {} No documents found", "ℹ".truecolor(120, 80, 255));
        return;
    }
    println!();
    for doc in docs {
        let formatted = serde_json::to_string_pretty(doc).unwrap_or_default();
        for (i, line) in formatted.lines().enumerate() {
            if i == 0 {
                println!("  {} {}", "▸".truecolor(120, 80, 255), line.truecolor(200, 200, 220));
            } else {
                println!("    {}", line.truecolor(200, 200, 220));
            }
        }
        println!();
    }
    println!("  {}", format!("{} document(s)", docs.len()).dimmed());
}

fn cmd_delete(shell: &Shell, parts: &[&str]) {
    if parts.len() < 3 {
        println!(
            "  {} Usage: {}",
            "✗".red().bold(),
            "delete <collection> <id>".yellow()
        );
        return;
    }

    if !shell.user.role.can_write() {
        println!("  {} Permission denied", "✗".red().bold());
        return;
    }

    let db = match &shell.current_db {
        Some(db) => db.clone(),
        None => {
            println!(
                "  {} No database selected. Use {}",
                "✗".red().bold(),
                "use <database>".yellow()
            );
            return;
        }
    };

    match shell.db_manager.delete_by_id(&db, parts[1], parts[2]) {
        Ok(Ok(true)) => println!("  {} Document deleted", "✓".green().bold()),
        Ok(Ok(false)) => println!("  {} Document not found", "ℹ".truecolor(120, 80, 255)),
        Ok(Err(e)) => println!("  {} {}", "✗".red().bold(), e.yellow()),
        Err(e) => println!("  {} {}", "✗".red().bold(), e.to_string().red()),
    }
}

fn cmd_count(shell: &Shell, parts: &[&str]) {
    if parts.len() < 2 {
        println!(
            "  {} Usage: {}",
            "✗".red().bold(),
            "count <collection>".yellow()
        );
        return;
    }

    let db = match &shell.current_db {
        Some(db) => db.clone(),
        None => {
            println!(
                "  {} No database selected. Use {}",
                "✗".red().bold(),
                "use <database>".yellow()
            );
            return;
        }
    };

    match shell.db_manager.count(&db, parts[1]) {
        Ok(Ok(n)) => println!(
            "  {} {} document(s) in '{}'",
            "▸".truecolor(120, 80, 255),
            n.to_string().bold(),
            parts[1]
        ),
        Ok(Err(e)) => println!("  {} {}", "✗".red().bold(), e.yellow()),
        Err(e) => println!("  {} {}", "✗".red().bold(), e.to_string().red()),
    }
}

fn cmd_users(shell: &Shell) {
    if !shell.user.role.can_admin() {
        println!("  {} Permission denied", "✗".red().bold());
        return;
    }

    let users = shell.auth.list_users();
    println!();
    println!("  {}", "USERS".bold().dimmed());
    for name in &users {
        if let Ok(Some(u)) = shell.auth.get_user(name) {
            let role_display = match u.role {
                Role::Root => u.role.to_string().truecolor(255, 180, 50),
                Role::Admin => u.role.to_string().truecolor(255, 100, 100),
                _ => u.role.to_string().cyan(),
            };
            let marker = if name == &shell.user.username {
                "●".green()
            } else {
                "○".dimmed()
            };
            println!("  {} {} ({})", marker, name.bold(), role_display);
        }
    }
    println!();
    println!("  {}", format!("{} user(s)", users.len()).dimmed());
}

fn cmd_adduser(shell: &Shell, parts: &[&str]) {
    if !shell.user.role.can_admin() {
        println!("  {} Permission denied", "✗".red().bold());
        return;
    }

    if parts.len() < 3 {
        println!(
            "  {} Usage: {}",
            "✗".red().bold(),
            "adduser <username> <role>".yellow()
        );
        println!(
            "  {} Roles: admin, readwrite (rw), readonly (ro)",
            "ℹ".truecolor(120, 80, 255)
        );
        return;
    }

    let username = parts[1];
    let role = match Role::from_str(parts[2]) {
        Some(r) => r,
        None => {
            println!(
                "  {} Invalid role '{}'. Use: admin, readwrite, readonly",
                "✗".red().bold(),
                parts[2].yellow()
            );
            return;
        }
    };

    let password = match Password::new()
        .with_prompt(format!(
            "  {} {}",
            "▸".truecolor(120, 80, 255),
            "Password for new user"
        ))
        .with_confirmation("  ▸ Confirm password", "  ✗ Passwords don't match")
        .interact()
    {
        Ok(p) => p,
        Err(_) => {
            println!("  {} Cancelled", "✗".red().bold());
            return;
        }
    };

    match shell.auth.create_user(username, &password, role, vec![]) {
        Ok(Ok(())) => println!(
            "  {} User '{}' created",
            "✓".green().bold(),
            username.bold().cyan()
        ),
        Ok(Err(e)) => println!("  {} {}", "✗".red().bold(), e.yellow()),
        Err(e) => println!("  {} {}", "✗".red().bold(), e.to_string().red()),
    }
}

fn cmd_rmuser(shell: &Shell, parts: &[&str]) {
    if !shell.user.role.is_root() {
        println!("  {} Only root can remove users", "✗".red().bold());
        return;
    }

    if parts.len() < 2 {
        println!(
            "  {} Usage: {}",
            "✗".red().bold(),
            "rmuser <username>".yellow()
        );
        return;
    }

    match shell.auth.delete_user(parts[1]) {
        Ok(Ok(())) => println!(
            "  {} User '{}' removed",
            "✓".green().bold(),
            parts[1].bold()
        ),
        Ok(Err(e)) => println!("  {} {}", "✗".red().bold(), e.yellow()),
        Err(e) => println!("  {} {}", "✗".red().bold(), e.to_string().red()),
    }
}

fn cmd_passwd(shell: &Shell, parts: &[&str]) {
    let target = if parts.len() >= 2 {
        parts[1]
    } else {
        &shell.user.username
    };

    // Only root/admin can change other users' passwords
    if target != shell.user.username && !shell.user.role.can_admin() {
        println!("  {} Permission denied", "✗".red().bold());
        return;
    }

    let password = match Password::new()
        .with_prompt(format!(
            "  {} {}",
            "▸".truecolor(120, 80, 255),
            "New password"
        ))
        .with_confirmation("  ▸ Confirm password", "  ✗ Passwords don't match")
        .interact()
    {
        Ok(p) => p,
        Err(_) => {
            println!("  {} Cancelled", "✗".red().bold());
            return;
        }
    };

    match shell.auth.set_password(target, &password) {
        Ok(Ok(())) => println!(
            "  {} Password updated for '{}'",
            "✓".green().bold(),
            target.bold()
        ),
        Ok(Err(e)) => println!("  {} {}", "✗".red().bold(), e.yellow()),
        Err(e) => println!("  {} {}", "✗".red().bold(), e.to_string().red()),
    }
}

fn cmd_benchmark() {
    let data_dir = Shell::data_dir();
    if let Err(e) = bench::run_benchmark(&data_dir) {
        println!("  {} Benchmark failed: {}", "✗".red().bold(), e.to_string().red());
    }
}

fn cmd_status(shell: &Shell) {
    let dbs = shell.db_manager.list_databases();
    let users = shell.auth.list_users();

    println!();
    println!("  {}", "VANTADB STATUS".bold().truecolor(120, 80, 255));
    println!("  {}", "─────────────────────────────".truecolor(60, 60, 80));
    println!(
        "  {} {}",
        "Version:".dimmed(),
        "0.1.0".truecolor(120, 200, 120)
    );
    println!(
        "  {} {}",
        "Data dir:".dimmed(),
        Shell::data_dir().display().to_string().truecolor(200, 200, 220)
    );
    println!(
        "  {} {}",
        "User:".dimmed(),
        shell.user.username.bold()
    );
    println!(
        "  {} {}",
        "Databases:".dimmed(),
        dbs.len().to_string().bold()
    );
    println!(
        "  {} {}",
        "Users:".dimmed(),
        users.len().to_string().bold()
    );
    if let Some(db) = &shell.current_db {
        println!(
            "  {} {}",
            "Current DB:".dimmed(),
            db.bold().truecolor(120, 200, 120)
        );
    }
    println!();
}
