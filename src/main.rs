#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod auth;
mod bench;
mod cli;
mod config;
mod db;
mod raft;
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
    version = env!("CARGO_PKG_VERSION"),
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

    /// Disable TLS (plain-text gRPC, for development only)
    #[arg(long)]
    no_tls: bool,

    /// Path to client TLS certificate PEM (for self-check mTLS)
    #[arg(long)]
    tls_cert: Option<String>,

    /// Path to client TLS key PEM (for self-check mTLS)
    #[arg(long)]
    tls_key: Option<String>,

    /// Path to CA certificate PEM (for self-check TLS verification)
    #[arg(long)]
    tls_ca: Option<String>,

    /// Path to configuration file (default: auto-search)
    #[arg(long)]
    config: Option<String>,

    /// Dump effective configuration and exit
    #[arg(long = "dump-config")]
    dump_config: bool,
}

fn main() {
    let cli = Cli::parse();

    // Load configuration
    let mut cfg = config::Config::load(cli.config.as_deref());

    // CLI flags override config values
    if cli.port != 5432 {
        cfg.server.port = cli.port;
    }
    if cli.no_tls {
        cfg.server.tls = false;
    }

    if cli.dump_config {
        print!("{}", cfg.dump());
        return;
    }

    if cli.self_check {
        run_self_check(cli.user, cli.password, cfg.server.port, cli.tls_cert, cli.tls_key, cli.tls_ca);
    } else if cli.benchmark {
        run_benchmark();
    } else if cli.serve {
        run_server(&cfg);
    } else if cli.status {
        run_status();
    } else {
        run_login();
    }
}

fn run_login() {
    Shell::print_banner();

    let (_engine, auth, db_manager, acl_manager, cert_manager, audit_logger) = match Shell::init() {
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

    let mut shell = Shell::new(user, auth, db_manager, acl_manager, cert_manager, audit_logger);
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

    let (_engine, auth, db_manager, _acl, _cert, _audit) = match Shell::init() {
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

fn run_server(cfg: &config::Config) {
    Shell::print_banner();

    let (engine, auth, db_manager, _acl, _cert, _audit) = match Shell::init() {
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

    let data_dir = Shell::data_dir();
    let port = cfg.server.port;
    let use_tls = cfg.server.tls;

    println!(
        "  {} Starting gRPC{} server on port {}...",
        "→".truecolor(120, 80, 255),
        if use_tls { "+TLS" } else { "" },
        port.to_string().bold()
    );
    println!();

    let cfg = cfg.clone();
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    rt.block_on(async move {
        if let Err(e) = server::start(auth, db_manager, port, &data_dir, engine, use_tls, &cfg).await {
            eprintln!(
                "  {} Server error: {}",
                "✗".red().bold(),
                e.to_string().red()
            );
            process::exit(1);
        }
    });
}

fn run_self_check(
    user: Option<String>,
    password: Option<String>,
    port: u16,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    tls_ca: Option<String>,
) {
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

    let tls_config = match (&tls_cert, &tls_key, &tls_ca) {
        (Some(cert), Some(key), Some(ca)) => Some((cert.clone(), key.clone(), ca.clone())),
        (None, None, None) => None,
        _ => {
            eprintln!(
                "  {} --tls-cert, --tls-key, and --tls-ca must all be provided together",
                "✗".red().bold()
            );
            process::exit(1);
        }
    };

    Shell::print_banner();

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    rt.block_on(async move {
        let success = selfcheck::run(port, &user, &password, tls_config.as_ref()).await;
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
