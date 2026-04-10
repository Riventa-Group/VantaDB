use colored::*;
use dialoguer::Password;
use serde_json::Value;
use std::io;

use crate::auth::Role;
use crate::bench;
use crate::cli::Shell;
use crate::db::{CollectionSchema, QueryOptions, VantaError};

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
        "update" => cmd_update(shell, &parts),
        "query" => cmd_query(shell, &parts),
        "aggregate" | "agg" => cmd_aggregate(shell, &parts),
        "schema" => cmd_schema(shell, &parts),
        "begin" => cmd_begin_tx(shell),
        "commit" => cmd_commit_tx(shell),
        "rollback" => cmd_rollback_tx(shell),
        "tx" => cmd_tx_op(shell, &parts),
        "users" => cmd_users(shell),
        "adduser" => cmd_adduser(shell, &parts),
        "rmuser" => cmd_rmuser(shell, &parts),
        "passwd" => cmd_passwd(shell, &parts),
        "status" => cmd_status(shell),
        "benchmark" | "bench" => cmd_benchmark(),
        "acl" => super::cmd_security::cmd_acl(shell, &parts),
        "certs" => super::cmd_security::cmd_certs(shell, &parts),
        "audit" => super::cmd_ops::cmd_audit(shell, &parts),
        "metrics" => super::cmd_ops::cmd_metrics(shell),
        "health" => super::cmd_ops::cmd_health(shell),
        "backup" => super::cmd_ops::cmd_backup(shell, &parts),
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

/// Display a VantaError to the user with appropriate coloring.
fn print_error(e: &VantaError) {
    match e {
        VantaError::NotFound { .. } | VantaError::AlreadyExists { .. } => {
            println!("  {} {}", "✗".red().bold(), e.to_string().yellow());
        }
        VantaError::ValidationFailed { .. } => {
            println!("  {} {}", "✗".red().bold(), e.to_string().yellow());
        }
        _ => {
            println!("  {} {}", "✗".red().bold(), e.to_string().red());
        }
    }
}

fn cmd_help(_shell: &Shell) {
    println!();
    println!(
        "  {}",
        "COMMANDS".bold().truecolor(120, 80, 255)
    );
    println!(
        "  {}",
        "─────────────────────────────────────────────".truecolor(60, 60, 80)
    );
    println!();

    let cmds = [
        (
            "Navigation",
            vec![
                ("use <db>", "Switch to a database"),
                ("show dbs", "List all databases"),
                ("show collections", "List collections in current db"),
                ("show indexes <col>", "List indexes on a collection"),
                ("status", "Show server status"),
            ],
        ),
        (
            "Database",
            vec![
                ("create db <name>", "Create a new database"),
                ("drop db <name>", "Delete a database"),
                ("create collection <n>", "Create a collection"),
                ("drop collection <name>", "Delete a collection"),
            ],
        ),
        (
            "Documents",
            vec![
                ("insert <col> <json>", "Insert a document"),
                ("find <col>", "Find all documents"),
                ("find <col> <id>", "Find document by ID"),
                ("find <col> where <f> = <v>", "Find by field value"),
                ("  --sort <field> --desc", "  Sort results"),
                ("  --page N --size N", "  Paginate results"),
                ("delete <col> <id>", "Delete a document"),
                ("count <col>", "Count documents"),
            ],
        ),
        (
            "Update",
            vec![
                ("update <col> <id> <json>", "Update doc by ID"),
                ("update <col> where <filter>", "Update matching docs"),
                ("  set <patch>", "  ... with patch"),
            ],
        ),
        (
            "Rich Query",
            vec![
                ("query <col> <filter>", "Query with filter expression"),
                ("  --sort/--desc/--page/--size", "  With pagination/sort"),
            ],
        ),
        (
            "Aggregation",
            vec![
                ("aggregate <col> <pipeline>", "Run aggregation pipeline"),
                ("  (alias: agg)", "  JSON array of stages"),
            ],
        ),
        (
            "Indexes",
            vec![
                ("create index <col> <field>", "Create index"),
                ("create index <col> <f> unique", "Create unique index"),
                ("drop index <col> <field>", "Drop index"),
            ],
        ),
        (
            "Schema",
            vec![
                ("schema set <col> <json>", "Set collection schema"),
                ("schema get <col>", "Get collection schema"),
                ("schema drop <col>", "Remove schema"),
            ],
        ),
        (
            "Transactions",
            vec![
                ("begin", "Start a transaction"),
                ("tx insert <col> <json>", "Insert in transaction"),
                ("tx update <col> <id> <json>", "Update in transaction"),
                ("tx delete <col> <id>", "Delete in transaction"),
                ("commit", "Commit transaction"),
                ("rollback", "Rollback transaction"),
            ],
        ),
        (
            "Users",
            vec![
                ("users", "List all users"),
                ("adduser <name> <role>", "Create a user"),
                ("rmuser <name>", "Delete a user"),
                ("passwd <name>", "Change password"),
                ("whoami", "Show current user"),
            ],
        ),
        (
            "Security",
            vec![
                ("acl set <u> <db> [col] <perm>", "Set ACL permission"),
                ("acl get <user>", "Show user ACLs"),
                ("acl delete <user>", "Remove all ACLs"),
                ("certs issue <user>", "Issue client certificate"),
                ("certs revoke <serial>", "Revoke certificate"),
                ("certs list", "List all certificates"),
                ("certs ca", "Print CA certificate"),
            ],
        ),
        (
            "Operations",
            vec![
                ("audit [--user X --op Y]", "Query audit log"),
                ("metrics", "Show server metrics"),
                ("health", "Show health status"),
                ("backup", "Create a backup"),
                ("backup list", "List backups"),
                ("backup restore <path>", "Restore from backup"),
            ],
        ),
        (
            "General",
            vec![
                ("benchmark", "Run full benchmark suite"),
                ("clear", "Clear the screen"),
                ("exit", "Exit VantaDB"),
            ],
        ),
    ];

    for (section, entries) in cmds {
        println!("  {}", section.bold().dimmed());
        for (cmd, desc) in entries {
            println!(
                "    {:<32} {}",
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
        println!(
            "  {} Usage: {}",
            "✗".red().bold(),
            "use <database>".yellow()
        );
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
            "  {} Usage: {}, {}, or {}",
            "✗".red().bold(),
            "show dbs".yellow(),
            "show collections".yellow(),
            "show indexes <col>".yellow()
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
            let db = match require_db(shell) {
                Some(db) => db,
                None => return,
            };
            match shell.db_manager.list_collections(&db) {
                Ok(cols) => {
                    if cols.is_empty() {
                        println!(
                            "  {} No collections in '{}'",
                            "ℹ".truecolor(120, 80, 255),
                            db
                        );
                    } else {
                        println!();
                        println!(
                            "  {} {}",
                            "COLLECTIONS IN".bold().dimmed(),
                            db.bold().dimmed()
                        );
                        for col in &cols {
                            println!("  {} {}", "○".dimmed(), col.cyan());
                        }
                        println!();
                        println!(
                            "  {}",
                            format!("{} collection(s)", cols.len()).dimmed()
                        );
                    }
                }
                Err(e) => print_error(&e),
            }
        }
        "indexes" => {
            if parts.len() < 3 {
                println!(
                    "  {} Usage: {}",
                    "✗".red().bold(),
                    "show indexes <collection>".yellow()
                );
                return;
            }
            let db = match require_db(shell) {
                Some(db) => db,
                None => return,
            };
            match shell.db_manager.list_indexes(&db, parts[2]) {
                Ok(indexes) => {
                    if indexes.is_empty() {
                        println!(
                            "  {} No indexes on '{}'",
                            "ℹ".truecolor(120, 80, 255),
                            parts[2]
                        );
                    } else {
                        println!();
                        println!(
                            "  {} {}",
                            "INDEXES ON".bold().dimmed(),
                            parts[2].bold().dimmed()
                        );
                        for idx in &indexes {
                            let unique_tag = if idx.unique {
                                " (unique)".truecolor(255, 180, 50).to_string()
                            } else {
                                String::new()
                            };
                            println!(
                                "  {} {}{}",
                                "○".dimmed(),
                                idx.field.cyan(),
                                unique_tag
                            );
                        }
                        println!();
                        println!(
                            "  {}",
                            format!("{} index(es)", indexes.len()).dimmed()
                        );
                    }
                }
                Err(e) => print_error(&e),
            }
        }
        _ => {
            println!(
                "  {} Unknown: show {}. Try {}, {}, or {}",
                "✗".red().bold(),
                parts[1].yellow(),
                "show dbs".truecolor(120, 200, 120),
                "show collections".truecolor(120, 200, 120),
                "show indexes <col>".truecolor(120, 200, 120)
            );
        }
    }
}

fn cmd_create(shell: &Shell, parts: &[&str]) {
    if parts.len() < 3 {
        println!(
            "  {} Usage: {}, {}, or {}",
            "✗".red().bold(),
            "create db <name>".yellow(),
            "create collection <name>".yellow(),
            "create index <col> <field>".yellow()
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
                Ok(()) => println!(
                    "  {} Database '{}' created",
                    "✓".green().bold(),
                    parts[2].bold().truecolor(120, 200, 120)
                ),
                Err(e) => print_error(&e),
            }
        }
        "collection" | "col" => {
            if !shell.user.role.can_write() {
                println!("  {} Permission denied", "✗".red().bold());
                return;
            }
            let db = match require_db(shell) {
                Some(db) => db,
                None => return,
            };
            match shell.db_manager.create_collection(&db, parts[2]) {
                Ok(()) => println!(
                    "  {} Collection '{}' created in '{}'",
                    "✓".green().bold(),
                    parts[2].bold().cyan(),
                    db
                ),
                Err(e) => print_error(&e),
            }
        }
        "index" | "idx" => {
            if !shell.user.role.can_write() {
                println!("  {} Permission denied", "✗".red().bold());
                return;
            }
            if parts.len() < 4 {
                println!(
                    "  {} Usage: {}",
                    "✗".red().bold(),
                    "create index <collection> <field> [unique]".yellow()
                );
                return;
            }
            let db = match require_db(shell) {
                Some(db) => db,
                None => return,
            };
            let unique = parts.len() > 4 && parts[4] == "unique";
            match shell.db_manager.create_index(&db, parts[2], parts[3], unique) {
                Ok(()) => {
                    let unique_msg = if unique { " (unique)" } else { "" };
                    println!(
                        "  {} Index on '{}'{} created in '{}'",
                        "✓".green().bold(),
                        parts[3].bold().cyan(),
                        unique_msg,
                        parts[2]
                    );
                }
                Err(e) => print_error(&e),
            }
        }
        _ => {
            println!(
                "  {} Unknown: create {}",
                "✗".red().bold(),
                parts[1].yellow()
            );
        }
    }
}

fn cmd_drop(shell: &mut Shell, parts: &[&str]) {
    if parts.len() < 3 {
        println!(
            "  {} Usage: {}, {}, or {}",
            "✗".red().bold(),
            "drop db <name>".yellow(),
            "drop collection <name>".yellow(),
            "drop index <col> <field>".yellow()
        );
        return;
    }

    match parts[1] {
        "db" | "database" => {
            if !shell.user.role.can_admin() {
                println!("  {} Permission denied", "✗".red().bold());
                return;
            }
            match shell.db_manager.drop_database(parts[2]) {
                Ok(()) => {
                    if shell.current_db.as_deref() == Some(parts[2]) {
                        shell.current_db = None;
                    }
                    println!(
                        "  {} Database '{}' dropped",
                        "✓".green().bold(),
                        parts[2].bold()
                    );
                }
                Err(e) => print_error(&e),
            }
        }
        "collection" | "col" => {
            if !shell.user.role.can_admin() {
                println!("  {} Permission denied", "✗".red().bold());
                return;
            }
            let db = match require_db(shell) {
                Some(db) => db,
                None => return,
            };
            match shell.db_manager.drop_collection(&db, parts[2]) {
                Ok(()) => println!(
                    "  {} Collection '{}' dropped from '{}'",
                    "✓".green().bold(),
                    parts[2].bold(),
                    db
                ),
                Err(e) => print_error(&e),
            }
        }
        "index" | "idx" => {
            if !shell.user.role.can_admin() {
                println!("  {} Permission denied", "✗".red().bold());
                return;
            }
            if parts.len() < 4 {
                println!(
                    "  {} Usage: {}",
                    "✗".red().bold(),
                    "drop index <collection> <field>".yellow()
                );
                return;
            }
            let db = match require_db(shell) {
                Some(db) => db,
                None => return,
            };
            match shell.db_manager.drop_index(&db, parts[2], parts[3]) {
                Ok(()) => println!(
                    "  {} Index on '{}' dropped from '{}'",
                    "✓".green().bold(),
                    parts[3].bold(),
                    parts[2]
                ),
                Err(e) => print_error(&e),
            }
        }
        _ => {
            println!(
                "  {} Unknown: drop {}",
                "✗".red().bold(),
                parts[1].yellow()
            );
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

    let db = match require_db(shell) {
        Some(db) => db,
        None => return,
    };

    let collection = parts[1];
    let json_str: String = parts[2..].join(" ");

    let doc: Value = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => {
            println!(
                "  {} Invalid JSON: {}",
                "✗".red().bold(),
                e.to_string().red()
            );
            return;
        }
    };

    if !doc.is_object() {
        println!("  {} Document must be a JSON object", "✗".red().bold());
        return;
    }

    match shell.db_manager.insert(&db, collection, doc) {
        Ok(id) => println!(
            "  {} Inserted with _id: {}",
            "✓".green().bold(),
            id.truecolor(180, 180, 255)
        ),
        Err(e) => print_error(&e),
    }
}

// ---- Update command ------------------------------------------

fn cmd_update(shell: &Shell, parts: &[&str]) {
    if parts.len() < 4 {
        println!(
            "  {} Usage: {} or {}",
            "✗".red().bold(),
            "update <col> <id> <json>".yellow(),
            "update <col> where <filter> set <patch>".yellow()
        );
        return;
    }

    if !shell.user.role.can_write() {
        println!("  {} Permission denied", "✗".red().bold());
        return;
    }

    let db = match require_db(shell) {
        Some(db) => db,
        None => return,
    };

    let collection = parts[1];

    // update <col> where <filter> set <patch>
    if parts[2] == "where" {
        // Find the "set" keyword
        let set_idx = parts.iter().position(|&p| p == "set");
        match set_idx {
            Some(idx) if idx > 3 && idx + 1 < parts.len() => {
                let filter_str = parts[3..idx].join(" ");
                let patch_str = parts[idx + 1..].join(" ");

                let filter: Value = match serde_json::from_str(&filter_str) {
                    Ok(v) => v,
                    Err(e) => {
                        println!(
                            "  {} Invalid filter JSON: {}",
                            "✗".red().bold(),
                            e.to_string().red()
                        );
                        return;
                    }
                };
                let patch: Value = match serde_json::from_str(&patch_str) {
                    Ok(v) => v,
                    Err(e) => {
                        println!(
                            "  {} Invalid patch JSON: {}",
                            "✗".red().bold(),
                            e.to_string().red()
                        );
                        return;
                    }
                };

                match shell
                    .db_manager
                    .update_where(&db, collection, &filter, &patch)
                {
                    Ok(count) => println!(
                        "  {} {} document(s) updated",
                        "✓".green().bold(),
                        count.to_string().bold()
                    ),
                    Err(e) => print_error(&e),
                }
            }
            _ => {
                println!(
                    "  {} Usage: {}",
                    "✗".red().bold(),
                    "update <col> where <filter_json> set <patch_json>".yellow()
                );
            }
        }
        return;
    }

    // update <col> <id> <json>
    let id = parts[2];
    let json_str = parts[3..].join(" ");
    let patch: Value = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => {
            println!(
                "  {} Invalid JSON: {}",
                "✗".red().bold(),
                e.to_string().red()
            );
            return;
        }
    };

    match shell.db_manager.update_by_id(&db, collection, id, patch) {
        Ok(true) => println!("  {} Document updated", "✓".green().bold()),
        Ok(false) => {
            println!(
                "  {} Document not found",
                "ℹ".truecolor(120, 80, 255)
            )
        }
        Err(e) => print_error(&e),
    }
}

// ---- Rich query command --------------------------------------

fn cmd_query(shell: &Shell, parts: &[&str]) {
    let (opts, parts) = extract_query_options(parts);

    if parts.len() < 3 {
        println!(
            "  {} Usage: {}",
            "✗".red().bold(),
            "query <collection> <filter_json> [--sort <f> --desc --page N --size N]".yellow()
        );
        return;
    }

    let db = match require_db(shell) {
        Some(db) => db,
        None => return,
    };

    let collection = parts[1];
    let filter_str = parts[2..].join(" ");
    let filter: Value = match serde_json::from_str(&filter_str) {
        Ok(v) => v,
        Err(e) => {
            println!(
                "  {} Invalid filter JSON: {}",
                "✗".red().bold(),
                e.to_string().red()
            );
            return;
        }
    };

    match shell.db_manager.query(&db, collection, &filter, &opts) {
        Ok((docs, total)) => print_documents_paged(&docs, total, &opts),
        Err(e) => print_error(&e),
    }
}

// ---- Aggregation command -------------------------------------

fn cmd_aggregate(shell: &Shell, parts: &[&str]) {
    if parts.len() < 3 {
        println!(
            "  {} Usage: {}",
            "✗".red().bold(),
            "aggregate <collection> <pipeline_json>".yellow()
        );
        println!(
            "  {} Example: {}",
            "ℹ".truecolor(120, 80, 255),
            "aggregate users [{\"$group\":{\"_id\":\"$city\",\"count\":{\"$sum\":1}}}]".dimmed()
        );
        return;
    }

    let db = match require_db(shell) {
        Some(db) => db,
        None => return,
    };

    let collection = parts[1];
    let pipeline_str = parts[2..].join(" ");
    let pipeline: Vec<Value> = match serde_json::from_str(&pipeline_str) {
        Ok(v) => v,
        Err(e) => {
            println!(
                "  {} Invalid pipeline JSON (must be an array): {}",
                "✗".red().bold(),
                e.to_string().red()
            );
            return;
        }
    };

    match shell.db_manager.aggregate(&db, collection, &pipeline) {
        Ok(results) => {
            if results.is_empty() {
                println!(
                    "  {} No results",
                    "ℹ".truecolor(120, 80, 255)
                );
            } else {
                println!();
                for doc in &results {
                    let formatted = serde_json::to_string_pretty(doc).unwrap_or_default();
                    for (i, line) in formatted.lines().enumerate() {
                        if i == 0 {
                            println!(
                                "  {} {}",
                                "▸".truecolor(120, 80, 255),
                                line.truecolor(200, 200, 220)
                            );
                        } else {
                            println!("    {}", line.truecolor(200, 200, 220));
                        }
                    }
                    println!();
                }
                println!(
                    "  {}",
                    format!("{} result(s)", results.len()).dimmed()
                );
            }
        }
        Err(e) => print_error(&e),
    }
}

// ---- Schema command ------------------------------------------

fn cmd_schema(shell: &Shell, parts: &[&str]) {
    if parts.len() < 3 {
        println!(
            "  {} Usage: {}, {}, or {}",
            "✗".red().bold(),
            "schema set <col> <json>".yellow(),
            "schema get <col>".yellow(),
            "schema drop <col>".yellow()
        );
        return;
    }

    let db = match require_db(shell) {
        Some(db) => db,
        None => return,
    };

    match parts[1] {
        "set" => {
            if !shell.user.role.can_admin() {
                println!("  {} Permission denied", "✗".red().bold());
                return;
            }
            if parts.len() < 4 {
                println!(
                    "  {} Usage: {}",
                    "✗".red().bold(),
                    "schema set <collection> <schema_json>".yellow()
                );
                return;
            }
            let collection = parts[2];
            let json_str = parts[3..].join(" ");
            let schema_val: Value = match serde_json::from_str(&json_str) {
                Ok(v) => v,
                Err(e) => {
                    println!(
                        "  {} Invalid JSON: {}",
                        "✗".red().bold(),
                        e.to_string().red()
                    );
                    return;
                }
            };
            let schema = match CollectionSchema::from_json(&schema_val) {
                Ok(s) => s,
                Err(e) => {
                    println!("  {} {}", "✗".red().bold(), e.red());
                    return;
                }
            };
            match shell.db_manager.set_schema(&db, collection, &schema) {
                Ok(()) => println!(
                    "  {} Schema set on '{}'",
                    "✓".green().bold(),
                    collection.bold().cyan()
                ),
                Err(e) => print_error(&e),
            }
        }
        "get" => {
            let collection = parts[2];
            match shell.db_manager.get_schema(&db, collection) {
                Ok(Some(schema)) => {
                    let json = serde_json::to_string_pretty(&schema.to_json()).unwrap_or_default();
                    println!();
                    for line in json.lines() {
                        println!("  {}", line.truecolor(200, 200, 220));
                    }
                    println!();
                }
                Ok(None) => println!(
                    "  {} No schema on '{}'",
                    "ℹ".truecolor(120, 80, 255),
                    collection
                ),
                Err(e) => print_error(&e),
            }
        }
        "drop" => {
            if !shell.user.role.can_admin() {
                println!("  {} Permission denied", "✗".red().bold());
                return;
            }
            let collection = parts[2];
            match shell.db_manager.drop_schema(&db, collection) {
                Ok(()) => println!(
                    "  {} Schema removed from '{}'",
                    "✓".green().bold(),
                    collection.bold()
                ),
                Err(e) => print_error(&e),
            }
        }
        _ => {
            println!(
                "  {} Unknown: schema {}",
                "✗".red().bold(),
                parts[1].yellow()
            );
        }
    }
}

// ---- Transaction commands ------------------------------------

fn cmd_begin_tx(shell: &mut Shell) {
    if shell.current_tx.is_some() {
        println!(
            "  {} Transaction already in progress. Commit or rollback first.",
            "✗".red().bold()
        );
        return;
    }
    let tx_id = shell.db_manager.begin_transaction();
    println!(
        "  {} Transaction started: {}",
        "✓".green().bold(),
        tx_id.truecolor(180, 180, 255)
    );
    shell.current_tx = Some(tx_id);
}

fn cmd_commit_tx(shell: &mut Shell) {
    let tx_id = match &shell.current_tx {
        Some(id) => id.clone(),
        None => {
            println!(
                "  {} No active transaction",
                "✗".red().bold()
            );
            return;
        }
    };

    if !shell.user.role.can_write() {
        println!("  {} Permission denied", "✗".red().bold());
        return;
    }

    match shell.db_manager.commit_transaction(&tx_id) {
        Ok(()) => {
            println!("  {} Transaction committed", "✓".green().bold());
            shell.current_tx = None;
        }
        Err(e) => print_error(&e),
    }
}

fn cmd_rollback_tx(shell: &mut Shell) {
    let tx_id = match &shell.current_tx {
        Some(id) => id.clone(),
        None => {
            println!(
                "  {} No active transaction",
                "✗".red().bold()
            );
            return;
        }
    };

    match shell.db_manager.rollback_transaction(&tx_id) {
        Ok(()) => {
            println!("  {} Transaction rolled back", "✓".green().bold());
            shell.current_tx = None;
        }
        Err(e) => print_error(&e),
    }
}

fn cmd_tx_op(shell: &Shell, parts: &[&str]) {
    if parts.len() < 3 {
        println!(
            "  {} Usage: {}, {}, or {}",
            "✗".red().bold(),
            "tx insert <col> <json>".yellow(),
            "tx update <col> <id> <json>".yellow(),
            "tx delete <col> <id>".yellow()
        );
        return;
    }

    let tx_id = match &shell.current_tx {
        Some(id) => id.clone(),
        None => {
            println!(
                "  {} No active transaction. Use {}",
                "✗".red().bold(),
                "begin".yellow()
            );
            return;
        }
    };

    let db = match require_db(shell) {
        Some(db) => db,
        None => return,
    };

    match parts[1] {
        "insert" => {
            if parts.len() < 4 {
                println!(
                    "  {} Usage: {}",
                    "✗".red().bold(),
                    "tx insert <collection> <json>".yellow()
                );
                return;
            }
            let collection = parts[2].to_string();
            let json_str = parts[3..].join(" ");
            let doc: Value = match serde_json::from_str(&json_str) {
                Ok(v) => v,
                Err(e) => {
                    println!(
                        "  {} Invalid JSON: {}",
                        "✗".red().bold(),
                        e.to_string().red()
                    );
                    return;
                }
            };
            match shell.db_manager.tx_insert(&tx_id, db, collection, doc) {
                Ok(()) => println!("  {} Insert queued", "✓".green().bold()),
                Err(e) => print_error(&e),
            }
        }
        "update" => {
            if parts.len() < 5 {
                println!(
                    "  {} Usage: {}",
                    "✗".red().bold(),
                    "tx update <collection> <id> <json>".yellow()
                );
                return;
            }
            let collection = parts[2].to_string();
            let id = parts[3].to_string();
            let json_str = parts[4..].join(" ");
            let patch: Value = match serde_json::from_str(&json_str) {
                Ok(v) => v,
                Err(e) => {
                    println!(
                        "  {} Invalid JSON: {}",
                        "✗".red().bold(),
                        e.to_string().red()
                    );
                    return;
                }
            };
            match shell.db_manager.tx_update(&tx_id, db, collection, id, patch) {
                Ok(()) => println!("  {} Update queued", "✓".green().bold()),
                Err(e) => print_error(&e),
            }
        }
        "delete" => {
            if parts.len() < 4 {
                println!(
                    "  {} Usage: {}",
                    "✗".red().bold(),
                    "tx delete <collection> <id>".yellow()
                );
                return;
            }
            let collection = parts[2].to_string();
            let id = parts[3].to_string();
            match shell.db_manager.tx_delete(&tx_id, db, collection, id) {
                Ok(()) => println!("  {} Delete queued", "✓".green().bold()),
                Err(e) => print_error(&e),
            }
        }
        _ => {
            println!(
                "  {} Unknown tx op: {}",
                "✗".red().bold(),
                parts[1].yellow()
            );
        }
    }
}

// ---- Find command (existing + pagination) --------------------

fn extract_query_options<'a>(parts: &[&'a str]) -> (QueryOptions, Vec<&'a str>) {
    let mut opts = QueryOptions::default();
    let mut remaining = Vec::new();
    let mut i = 0;
    while i < parts.len() {
        match parts[i] {
            "--sort" if i + 1 < parts.len() => {
                opts.sort_field = Some(parts[i + 1].to_string());
                i += 2;
            }
            "--desc" => {
                opts.sort_descending = true;
                i += 1;
            }
            "--page" if i + 1 < parts.len() => {
                opts.page = parts[i + 1].parse().unwrap_or(0);
                i += 2;
            }
            "--size" if i + 1 < parts.len() => {
                opts.page_size = parts[i + 1].parse().unwrap_or(50);
                i += 2;
            }
            _ => {
                remaining.push(parts[i]);
                i += 1;
            }
        }
    }
    (opts, remaining)
}

fn cmd_find(shell: &Shell, parts: &[&str]) {
    let (opts, parts) = extract_query_options(parts);
    let parts: Vec<&str> = parts;

    if parts.len() < 2 {
        println!(
            "  {} Usage: {}, {}, or {}",
            "✗".red().bold(),
            "find <collection>".yellow(),
            "find <collection> <id>".yellow(),
            "find <col> where <field> = <value>".yellow()
        );
        println!(
            "  {} Options: {}",
            "ℹ".truecolor(120, 80, 255),
            "--sort <field> --desc --page N --size N".dimmed()
        );
        return;
    }

    let db = match require_db(shell) {
        Some(db) => db,
        None => return,
    };

    let collection = parts[1];
    let has_opts = opts.sort_field.is_some() || opts.page > 0;

    // find <col> where <field> = <value>
    if parts.len() >= 5 && parts[2] == "where" {
        let field = parts[3];
        if parts.len() < 6 || parts[4] != "=" {
            println!(
                "  {} Usage: {}",
                "✗".red().bold(),
                "find <col> where <field> = <value>".yellow()
            );
            return;
        }
        let value_str = parts[5..].join(" ");
        let value: Value =
            serde_json::from_str(&value_str).unwrap_or(Value::String(value_str));

        match shell
            .db_manager
            .find_where_query(&db, collection, field, &value, &opts)
        {
            Ok((docs, total)) => print_documents_paged(&docs, total, &opts),
            Err(e) => print_error(&e),
        }
        return;
    }

    // find <col> <id>
    if parts.len() == 3 && !has_opts {
        match shell.db_manager.find_by_id(&db, collection, parts[2]) {
            Ok(Some(doc)) => print_documents(&[doc]),
            Ok(None) => println!(
                "  {} Document not found",
                "ℹ".truecolor(120, 80, 255)
            ),
            Err(e) => print_error(&e),
        }
        return;
    }

    // find <col>
    match shell.db_manager.find_all_query(&db, collection, &opts) {
        Ok((docs, total)) => print_documents_paged(&docs, total, &opts),
        Err(e) => print_error(&e),
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
                println!(
                    "  {} {}",
                    "▸".truecolor(120, 80, 255),
                    line.truecolor(200, 200, 220)
                );
            } else {
                println!("    {}", line.truecolor(200, 200, 220));
            }
        }
        println!();
    }
    println!("  {}", format!("{} document(s)", docs.len()).dimmed());
}

fn print_documents_paged(docs: &[Value], total: usize, opts: &QueryOptions) {
    if docs.is_empty() {
        println!("  {} No documents found", "ℹ".truecolor(120, 80, 255));
        return;
    }
    println!();
    for doc in docs {
        let formatted = serde_json::to_string_pretty(doc).unwrap_or_default();
        for (i, line) in formatted.lines().enumerate() {
            if i == 0 {
                println!(
                    "  {} {}",
                    "▸".truecolor(120, 80, 255),
                    line.truecolor(200, 200, 220)
                );
            } else {
                println!("    {}", line.truecolor(200, 200, 220));
            }
        }
        println!();
    }
    if opts.page > 0 {
        let size = if opts.page_size == 0 { 50 } else { opts.page_size } as usize;
        let total_pages = (total + size - 1) / size;
        println!(
            "  {}",
            format!(
                "showing {} of {} document(s) — page {}/{}",
                docs.len(),
                total,
                opts.page,
                total_pages
            )
            .dimmed()
        );
    } else {
        println!("  {}", format!("{} document(s)", total).dimmed());
    }
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

    let db = match require_db(shell) {
        Some(db) => db,
        None => return,
    };

    match shell.db_manager.delete_by_id(&db, parts[1], parts[2]) {
        Ok(true) => println!("  {} Document deleted", "✓".green().bold()),
        Ok(false) => println!(
            "  {} Document not found",
            "ℹ".truecolor(120, 80, 255)
        ),
        Err(e) => print_error(&e),
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

    let db = match require_db(shell) {
        Some(db) => db,
        None => return,
    };

    match shell.db_manager.count(&db, parts[1]) {
        Ok(n) => println!(
            "  {} {} document(s) in '{}'",
            "▸".truecolor(120, 80, 255),
            n.to_string().bold(),
            parts[1]
        ),
        Err(e) => print_error(&e),
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
        println!(
            "  {} Benchmark failed: {}",
            "✗".red().bold(),
            e.to_string().red()
        );
    }
}

fn cmd_status(shell: &Shell) {
    let dbs = shell.db_manager.list_databases();
    let users = shell.auth.list_users();

    println!();
    println!(
        "  {}",
        "VANTADB STATUS".bold().truecolor(120, 80, 255)
    );
    println!(
        "  {}",
        "─────────────────────────────".truecolor(60, 60, 80)
    );
    println!(
        "  {} {}",
        "Version:".dimmed(),
        env!("CARGO_PKG_VERSION").truecolor(120, 200, 120)
    );
    println!(
        "  {} {}",
        "Data dir:".dimmed(),
        Shell::data_dir()
            .display()
            .to_string()
            .truecolor(200, 200, 220)
    );
    println!("  {} {}", "User:".dimmed(), shell.user.username.bold());
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
    if let Some(tx) = &shell.current_tx {
        println!(
            "  {} {}",
            "Active TX:".dimmed(),
            tx.truecolor(180, 180, 255)
        );
    }
    println!();
}

// ---- Helpers -------------------------------------------------

fn require_db(shell: &Shell) -> Option<String> {
    match &shell.current_db {
        Some(db) => Some(db.clone()),
        None => {
            println!(
                "  {} No database selected. Use {}",
                "✗".red().bold(),
                "use <database>".yellow()
            );
            None
        }
    }
}
