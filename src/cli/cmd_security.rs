use colored::*;

use crate::auth::acl::Permission;
use crate::cli::Shell;

pub fn cmd_acl(shell: &mut Shell, parts: &[&str]) {
    if parts.len() < 2 {
        println!("  {} Usage: acl <set|get|delete> ...", "✗".red().bold());
        println!("    acl set <user> <db> [collection] <permission>");
        println!("    acl get <user>");
        println!("    acl delete <user>");
        return;
    }

    match parts[1] {
        "set" => cmd_acl_set(shell, parts),
        "get" => cmd_acl_get(shell, parts),
        "delete" => cmd_acl_delete(shell, parts),
        _ => println!("  {} Unknown acl subcommand: {}", "✗".red().bold(), parts[1]),
    }
}

fn cmd_acl_set(shell: &mut Shell, parts: &[&str]) {
    // acl set <user> <db> <permission>
    // acl set <user> <db> <collection> <permission>
    if parts.len() < 5 {
        println!("  {} Usage: acl set <user> <db> [collection] <permission>", "✗".red().bold());
        return;
    }

    if !shell.user.role.can_admin() {
        println!("  {} Admin role required", "✗".red().bold());
        return;
    }

    let username = parts[2];
    let database = parts[3];

    let (collection, permission_str) = if parts.len() >= 6 {
        (Some(parts[4]), parts[5])
    } else {
        (None, parts[4])
    };

    let permission = match Permission::from_str(permission_str) {
        Some(p) => p,
        None => {
            println!("  {} Invalid permission '{}'. Use: none, readonly, readwrite, admin", "✗".red().bold(), permission_str);
            return;
        }
    };

    let result = if let Some(col) = collection {
        shell.acl_manager.set_collection_permission(username, database, col, permission)
    } else {
        shell.acl_manager.set_database_permission(username, database, permission)
    };

    match result {
        Ok(()) => {
            let scope = collection.map_or(database.to_string(), |c| format!("{}/{}", database, c));
            println!("  {} ACL set for {} on {}", "✓".green().bold(), username.cyan(), scope.truecolor(120, 200, 120));
        }
        Err(e) => println!("  {} {}", "✗".red().bold(), e),
    }
}

fn cmd_acl_get(shell: &mut Shell, parts: &[&str]) {
    if parts.len() < 3 {
        println!("  {} Usage: acl get <user>", "✗".red().bold());
        return;
    }

    let username = parts[2];
    match shell.acl_manager.get_user_acl(username) {
        Some(acl) => {
            if acl.databases.is_empty() {
                println!("  {} No ACLs set for {}", "ℹ".truecolor(120, 80, 255), username.cyan());
                return;
            }
            println!("  {} ACLs for {}:", "ℹ".truecolor(120, 80, 255), username.cyan());
            for db_acl in &acl.databases {
                println!("    {} → {}", db_acl.database.truecolor(120, 200, 120), db_acl.permission);
                for col_acl in &db_acl.collections {
                    println!("      {}/{} → {}", db_acl.database, col_acl.collection.truecolor(200, 200, 120), col_acl.permission);
                }
            }
        }
        None => println!("  {} No ACLs set for {}", "ℹ".truecolor(120, 80, 255), username.cyan()),
    }
}

fn cmd_acl_delete(shell: &mut Shell, parts: &[&str]) {
    if parts.len() < 3 {
        println!("  {} Usage: acl delete <user>", "✗".red().bold());
        return;
    }

    if !shell.user.role.can_admin() {
        println!("  {} Admin role required", "✗".red().bold());
        return;
    }

    let username = parts[2];
    match shell.acl_manager.delete_user_acl(username) {
        Ok(()) => println!("  {} ACLs deleted for {}", "✓".green().bold(), username.cyan()),
        Err(e) => println!("  {} {}", "✗".red().bold(), e),
    }
}

pub fn cmd_certs(shell: &mut Shell, parts: &[&str]) {
    if parts.len() < 2 {
        println!("  {} Usage: certs <issue|revoke|list|ca>", "✗".red().bold());
        return;
    }

    match parts[1] {
        "issue" => {
            if parts.len() < 3 {
                println!("  {} Usage: certs issue <username>", "✗".red().bold());
                return;
            }
            if !shell.user.role.can_admin() {
                println!("  {} Admin role required", "✗".red().bold());
                return;
            }
            match shell.cert_manager.issue_cert(parts[2]) {
                Ok((cert_pem, _key_pem, serial)) => {
                    println!("  {} Certificate issued for {}", "✓".green().bold(), parts[2].cyan());
                    println!("    Serial: {}", serial.dimmed());
                    println!("    Certificate PEM written to stdout");
                    println!("{}", cert_pem);
                }
                Err(e) => println!("  {} {}", "✗".red().bold(), e),
            }
        }
        "revoke" => {
            if parts.len() < 3 {
                println!("  {} Usage: certs revoke <serial>", "✗".red().bold());
                return;
            }
            if !shell.user.role.can_admin() {
                println!("  {} Admin role required", "✗".red().bold());
                return;
            }
            match shell.cert_manager.revoke_cert(parts[2]) {
                Ok(true) => println!("  {} Certificate {} revoked", "✓".green().bold(), parts[2]),
                Ok(false) => println!("  {} Certificate not found", "✗".red().bold()),
                Err(e) => println!("  {} {}", "✗".red().bold(), e),
            }
        }
        "list" => {
            let certs = shell.cert_manager.list_certs();
            if certs.is_empty() {
                println!("  {} No certificates issued", "ℹ".truecolor(120, 80, 255));
            } else {
                println!("  {} {} certificates:", "ℹ".truecolor(120, 80, 255), certs.len());
                for c in &certs {
                    let status = if c.revoked { "REVOKED".red().to_string() } else { "active".green().to_string() };
                    println!("    {} {} ({}): {}", c.serial.dimmed(), c.username.cyan(), c.issued_at.dimmed(), status);
                }
            }
        }
        "ca" => {
            println!("{}", shell.cert_manager.ca_cert_pem());
        }
        _ => println!("  {} Unknown certs subcommand: {}", "✗".red().bold(), parts[1]),
    }
}
