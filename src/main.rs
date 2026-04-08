#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod auth;
mod bench;
mod cli;
mod db;
mod selfcheck;
mod server;
mod storage;

use clap::Parser;
use colored::*;
use std::process;

use cli::Shell;

#[derive(Parser)]
#[command(
    name = "vantadb",
    about = "VantaDB — Next-Gen Database Engine",
    version = "0.1.0",
    author = "Riventa Group"
)]
struct Cli {
    /// Start an interactive session (authenticate and enter shell)
    #[arg(long)]
    login: bool,

    /// Show server status
    #[arg(long)]
    status: bool,

    /// Run full benchmark suite
    #[arg(long)]
    benchmark: bool,

    /// Start the gRPC server
    #[arg(long)]
    serve: bool,

    /// Port for the gRPC server
    #[arg(long, default_value = "5432")]
    port: u16,

    /// Run self-check: connect to a running server and test every feature via gRPC
    #[arg(long = "self-check")]
    self_check: bool,

    /// Username for self-check authentication
    #[arg(long)]
    user: Option<String>,

    /// Password for self-check authentication
    #[arg(long)]
    password: Option<String>,
}

fn main() {
    let cli = Cli::parse();

    if cli.self_check {
        run_self_check(cli.user, cli.password, cli.port);
    } else if cli.benchmark {
        run_benchmark();
    } else if cli.status {
        run_status();
    } else if cli.serve {
        run_server(cli.port);
    } else {
        run_login();
    }
}

fn run_login() {
    Shell::print_banner();

    let (_engine, auth, db_manager) = match Shell::init() {
        Ok(v) => v,
        Err(e) => {
            eprintln!(
                "  {} Failed to initialize: {}",
                "✗".red().bold(),
                e.to_string().red()
            );
            process::exit(1);
        }
    };

    let user = match Shell::login(&auth) {
        Ok(Some(user)) => user,
        Ok(None) => {
            process::exit(1);
        }
        Err(e) => {
            eprintln!(
                "  {} Auth error: {}",
                "✗".red().bold(),
                e.to_string().red()
            );
            process::exit(1);
        }
    };

    let mut shell = Shell::new(user, auth, db_manager);
    if let Err(e) = shell.run() {
        eprintln!(
            "  {} Shell error: {}",
            "✗".red().bold(),
            e.to_string().red()
        );
        process::exit(1);
    }
}

fn run_status() {
    Shell::print_banner();

    let (_engine, auth, db_manager) = match Shell::init() {
        Ok(v) => v,
        Err(e) => {
            eprintln!(
                "  {} Failed to initialize: {}",
                "✗".red().bold(),
                e.to_string().red()
            );
            process::exit(1);
        }
    };

    let dbs = db_manager.list_databases();
    let users = auth.list_users();

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
        "0.1.0".truecolor(120, 200, 120)
    );
    println!(
        "  {} {}",
        "Data dir:".dimmed(),
        Shell::data_dir()
            .display()
            .to_string()
            .truecolor(200, 200, 220)
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
    println!();
}

fn run_server(port: u16) {
    Shell::print_banner();

    let (_engine, auth, db_manager) = match Shell::init() {
        Ok(v) => v,
        Err(e) => {
            eprintln!(
                "  {} Failed to initialize: {}",
                "✗".red().bold(),
                e.to_string().red()
            );
            process::exit(1);
        }
    };

    println!(
        "  {} Starting gRPC server on port {}...",
        "→".truecolor(120, 80, 255),
        port.to_string().bold()
    );
    println!();

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    rt.block_on(async move {
        if let Err(e) = server::start(auth, db_manager, port).await {
            eprintln!(
                "  {} Server error: {}",
                "✗".red().bold(),
                e.to_string().red()
            );
            process::exit(1);
        }
    });
}

fn run_self_check(user: Option<String>, password: Option<String>, port: u16) {
    let user = match user {
        Some(u) => u,
        None => {
            eprintln!(
                "  {} --user is required for self-check",
                "✗".red().bold()
            );
            process::exit(1);
        }
    };
    let password = match password {
        Some(p) => p,
        None => {
            eprintln!(
                "  {} --password is required for self-check",
                "✗".red().bold()
            );
            process::exit(1);
        }
    };

    Shell::print_banner();

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    rt.block_on(async move {
        let success = selfcheck::run(port, &user, &password).await;
        if !success {
            process::exit(1);
        }
    });
}

fn run_benchmark() {
    Shell::print_banner();

    let data_dir = Shell::data_dir();
    if let Err(e) = bench::run_benchmark(&data_dir) {
        eprintln!(
            "  {} Benchmark failed: {}",
            "✗".red().bold(),
            e.to_string().red()
        );
        process::exit(1);
    }
}
