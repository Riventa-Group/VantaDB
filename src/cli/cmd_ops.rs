use colored::*;

use crate::cli::Shell;
use crate::server::audit::AuditFilter;

pub fn cmd_audit(shell: &mut Shell, parts: &[&str]) {
    if !shell.user.role.can_admin() {
        println!("  {} Admin role required", "✗".red().bold());
        return;
    }

    let mut filter = AuditFilter::default();
    filter.limit = 20;

    // Parse flags: --user X --op Y --db Z --limit N
    let mut i = 1;
    while i < parts.len() {
        match parts[i] {
            "--user" if i + 1 < parts.len() => { filter.username = Some(parts[i + 1].to_string()); i += 2; }
            "--op" if i + 1 < parts.len() => { filter.operation = Some(parts[i + 1].to_string()); i += 2; }
            "--db" if i + 1 < parts.len() => { filter.database = Some(parts[i + 1].to_string()); i += 2; }
            "--limit" if i + 1 < parts.len() => { filter.limit = parts[i + 1].parse().unwrap_or(20); i += 2; }
            _ => { i += 1; }
        }
    }

    let events = shell.audit_logger.query(&filter);
    if events.is_empty() {
        println!("  {} No audit events found", "ℹ".truecolor(120, 80, 255));
        return;
    }

    println!("  {} {} audit events:", "ℹ".truecolor(120, 80, 255), events.len());
    println!();
    for event in &events {
        let status = if event.success { "✓".green().to_string() } else { "✗".red().to_string() };
        let db_info = event.database.as_deref().unwrap_or("-");
        println!(
            "    {} {} {:<15} {:<20} {}",
            status,
            event.timestamp.format("%H:%M:%S").to_string().dimmed(),
            event.username.cyan(),
            event.operation.truecolor(120, 200, 120),
            db_info.dimmed(),
        );
        if let Some(ref err) = event.error {
            println!("      {} {}", "→".dimmed(), err.red());
        }
    }
}

pub fn cmd_metrics(shell: &mut Shell) {
    if !shell.user.role.can_admin() {
        println!("  {} Admin role required", "✗".red().bold());
        return;
    }

    let counters = shell.metrics.all_counters();
    let gauges = shell.metrics.all_gauges();
    let latencies = shell.metrics.all_latencies();

    println!("  {}", "COUNTERS".bold().truecolor(120, 80, 255));
    if counters.is_empty() {
        println!("    (none)");
    } else {
        for (name, value) in &counters {
            println!("    {:<35} {}", name.truecolor(120, 200, 120), value.to_string().bold());
        }
    }

    println!();
    println!("  {}", "GAUGES".bold().truecolor(120, 80, 255));
    for (name, value) in &gauges {
        println!("    {:<35} {}", name.truecolor(120, 200, 120), value.to_string().bold());
    }

    if !latencies.is_empty() {
        println!();
        println!("  {}", "LATENCY (μs)".bold().truecolor(120, 80, 255));
        println!("    {:<25} {:>8} {:>8} {:>8}", "operation", "p50", "p95", "p99");
        println!("    {}", "─".repeat(55).truecolor(60, 60, 80));
        for (op, p50, p95, p99) in &latencies {
            println!("    {:<25} {:>8} {:>8} {:>8}", op.truecolor(120, 200, 120), p50, p95, p99);
        }
    }
}

pub fn cmd_health(shell: &mut Shell) {
    let uptime = shell.metrics.uptime_seconds();
    let db_count = shell.db_manager.list_databases().len();
    let active_tx = shell.metrics.gauge("active_transactions");

    println!("  {}", "HEALTH".bold().truecolor(120, 80, 255));
    println!("    {:<20} {}", "Status:".dimmed(), "healthy".green().bold());
    println!("    {:<20} {}", "Version:".dimmed(), env!("CARGO_PKG_VERSION").truecolor(120, 200, 120));
    println!("    {:<20} {}s", "Uptime:".dimmed(), uptime);
    println!("    {:<20} {}", "Databases:".dimmed(), db_count);
    println!("    {:<20} {}", "Active transactions:".dimmed(), active_tx);
}

pub fn cmd_backup(shell: &mut Shell, parts: &[&str]) {
    if !shell.user.role.can_admin() {
        println!("  {} Admin role required", "✗".red().bold());
        return;
    }

    if parts.len() < 2 || parts[1] == "create" {
        // Create backup
        match crate::db::backup::create_backup(&shell.db_manager, &shell.data_dir) {
            Ok(result) => {
                println!("  {} Backup created", "✓".green().bold());
                println!("    Path:      {}", result.path.truecolor(120, 200, 120));
                println!("    Size:      {} bytes", result.size_bytes);
                println!("    Databases: {}", result.database_count);
                println!("    Documents: {}", result.document_count);
                println!("    Timestamp: {}", result.timestamp.dimmed());
            }
            Err(e) => println!("  {} Backup failed: {}", "✗".red().bold(), e),
        }
    } else if parts[1] == "list" {
        let backups = crate::db::backup::list_backups(&shell.data_dir);
        if backups.is_empty() {
            println!("  {} No backups found", "ℹ".truecolor(120, 80, 255));
        } else {
            println!("  {} {} backups:", "ℹ".truecolor(120, 80, 255), backups.len());
            for b in &backups {
                println!("    {} ({} bytes) - {}", b.timestamp.truecolor(120, 200, 120), b.size_bytes, b.path.dimmed());
            }
        }
    } else if parts[1] == "restore" {
        if parts.len() < 3 {
            println!("  {} Usage: backup restore <path>", "✗".red().bold());
            return;
        }
        match crate::db::backup::restore_backup(&shell.db_manager, parts[2]) {
            Ok(()) => println!("  {} Backup restored from {}", "✓".green().bold(), parts[2].truecolor(120, 200, 120)),
            Err(e) => println!("  {} Restore failed: {}", "✗".red().bold(), e),
        }
    } else {
        println!("  {} Usage: backup [create|list|restore <path>]", "✗".red().bold());
    }
}

pub fn cmd_cluster(shell: &mut Shell) {
    if !shell.user.role.can_admin() {
        println!("  {} Admin role required", "✗".red().bold());
        return;
    }

    // In CLI mode (direct access), show single-node info
    let db_count = shell.db_manager.list_databases().len();
    println!("  {}", "CLUSTER STATUS".bold().truecolor(120, 80, 255));
    println!("    {:<20} {}", "Mode:".dimmed(), "single-node");
    println!("    {:<20} {}", "Role:".dimmed(), "leader".green());
    println!("    {:<20} {}", "Databases:".dimmed(), db_count);
    println!("    {:<20} {}", "Active tx:".dimmed(), shell.metrics.gauge("active_transactions"));
}
