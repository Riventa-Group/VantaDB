pub mod audit;
pub mod auth_interceptor;
pub mod lockout;
pub mod metrics;
pub mod rate_limit;
pub mod scheduler;
pub mod service;
pub mod service_auth;
pub mod service_db;
pub mod session;

use std::sync::Arc;
use std::path::Path;
use colored::*;
use tonic::transport::{Identity, Server, ServerTlsConfig};

use crate::auth::{AclManager, AuthManager, CertManager};
use crate::config::Config;
use crate::db::DatabaseManager;
use crate::storage::StorageEngine;
use audit::AuditLogger;
use auth_interceptor::AuthInterceptor;
use lockout::LockoutTracker;
use metrics::MetricsCollector;
use rate_limit::{GlobalRateLimiter, RateLimiter};
use scheduler::BackgroundScheduler;
use service::{VantaAuthServiceImpl, VantaDbServiceImpl};
use session::JwtSessionManager;

pub mod proto {
    tonic::include_proto!("vantadb");
}

pub async fn start(
    auth: AuthManager,
    db_manager: DatabaseManager,
    port: u16,
    data_dir: &Path,
    engine: StorageEngine,
    use_tls: bool,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr: std::net::SocketAddr = format!("0.0.0.0:{}", port).parse()?;

    // Bootstrap certificates and ACLs
    let engine = Arc::new(engine);
    let cert_manager = CertManager::bootstrap(data_dir, Arc::clone(&engine))?;
    let acl_manager = AclManager::new(Arc::clone(&engine))?;

    // Use config values for subsystem initialization
    let jwt_manager = Arc::new(JwtSessionManager::new(config.auth.jwt_ttl_hours));
    let auth_manager = Arc::new(auth);
    let db_manager = Arc::new(db_manager);
    let cert_manager = Arc::new(cert_manager);
    let acl_manager = Arc::new(acl_manager);
    let audit_logger = Arc::new(AuditLogger::new(Arc::clone(&engine))?);
    let metrics = Arc::new(MetricsCollector::new());
    let lockout_tracker = Arc::new(LockoutTracker::new(
        config.auth.lockout_max_attempts,
        config.auth.lockout_window_secs,
        config.auth.lockout_duration_secs,
    ));
    let global_limiter = Arc::new(GlobalRateLimiter::new(
        config.rate_limit.global_rps,
        config.rate_limit.global_rps,
    ));
    let ip_limiter = Arc::new(RateLimiter::new(
        config.rate_limit.per_ip_rps,
        config.rate_limit.per_ip_rps,
    ));

    let auth_service = VantaAuthServiceImpl {
        auth_manager: Arc::clone(&auth_manager),
        jwt_manager: Arc::clone(&jwt_manager),
        cert_manager: Arc::clone(&cert_manager),
        acl_manager: Arc::clone(&acl_manager),
        audit_logger: Arc::clone(&audit_logger),
        metrics: Arc::clone(&metrics),
        db_manager: Arc::clone(&db_manager),
        data_dir: data_dir.to_path_buf(),
        lockout_tracker: Arc::clone(&lockout_tracker),
        global_auth_limiter: Arc::clone(&global_limiter),
        ip_auth_limiter: Arc::clone(&ip_limiter),
    };

    let db_service = VantaDbServiceImpl {
        db_manager: Arc::clone(&db_manager),
        acl_manager: Arc::clone(&acl_manager),
        audit_logger: Arc::clone(&audit_logger),
        metrics: Arc::clone(&metrics),
        raft: None,
        forwarder: None,
    };

    let interceptor = AuthInterceptor {
        jwt_manager: Arc::clone(&jwt_manager),
    };

    // Start background scheduler with config intervals
    let _scheduler = BackgroundScheduler::start(
        Arc::clone(&db_manager),
        Arc::clone(&metrics),
        config.scheduler.reap_interval_secs,
        config.scheduler.gc_interval_secs,
        config.scheduler.compact_interval_secs,
    );

    let mut builder = Server::builder();

    if use_tls {
        let server_cert = cert_manager.server_cert_pem()?;
        let server_key = cert_manager.server_key_pem()?;
        let ca_cert = cert_manager.ca_cert_pem().to_string();

        let identity = Identity::from_pem(server_cert.as_bytes(), server_key.as_bytes());
        let client_ca = tonic::transport::Certificate::from_pem(ca_cert.as_bytes());

        let tls_config = ServerTlsConfig::new()
            .identity(identity)
            .client_ca_root(client_ca);

        builder = builder.tls_config(tls_config)?;

        println!(
            "  {} gRPC+TLS server listening on {}",
            "✓".green().bold(),
            addr.to_string().bold().truecolor(120, 200, 120)
        );
        println!(
            "  {} mTLS enabled (client certificates required)",
            "✓".green().bold(),
        );
    } else {
        println!(
            "  {} gRPC server listening on {} {}",
            "✓".green().bold(),
            addr.to_string().bold().truecolor(120, 200, 120),
            "(no TLS)".truecolor(255, 180, 50)
        );
    }

    println!(
        "  {} Ctrl+C to stop",
        "ℹ".truecolor(120, 80, 255)
    );
    println!();

    builder
        .add_service(proto::vanta_auth_server::VantaAuthServer::new(auth_service))
        .add_service(proto::vanta_db_server::VantaDbServer::with_interceptor(
            db_service,
            interceptor,
        ))
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    // ---- Graceful shutdown sequence ----
    println!();
    println!(
        "  {} {}",
        "→".truecolor(120, 80, 255),
        "Shutting down...".dimmed()
    );

    // Stop background scheduler
    _scheduler.stop();

    // Reap transactions, compact WALs, run final GC
    let (reaped, compacted, pruned) = tokio::task::spawn_blocking({
        let mgr = Arc::clone(&db_manager);
        move || mgr.shutdown()
    })
    .await
    .unwrap_or((0, 0, 0));

    println!(
        "  {} Flushed {} tables, reaped {} transactions, pruned {} versions",
        "✓".green().bold(),
        compacted,
        reaped,
        pruned
    );
    println!(
        "  {} {}",
        "←".truecolor(120, 80, 255),
        "Server stopped.".dimmed()
    );
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        )
        .expect("Failed to listen for SIGTERM");

        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await.expect("Failed to listen for ctrl+c");
    }
}
