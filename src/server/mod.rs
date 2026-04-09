pub mod auth_interceptor;
pub mod service;
pub mod session;

use std::sync::Arc;
use std::path::Path;
use colored::*;
use tonic::transport::{Identity, Server, ServerTlsConfig};

use crate::auth::{AuthManager, CertManager};
use crate::db::DatabaseManager;
use crate::storage::StorageEngine;
use auth_interceptor::AuthInterceptor;
use service::{VantaAuthServiceImpl, VantaDbServiceImpl};
use session::SessionStore;

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
) -> Result<(), Box<dyn std::error::Error>> {
    let addr: std::net::SocketAddr = format!("0.0.0.0:{}", port).parse()?;

    // Bootstrap certificates (needed for cert management RPCs even without TLS)
    let cert_manager = CertManager::bootstrap(data_dir, Arc::new(engine))?;

    let session_store = Arc::new(SessionStore::new(24));
    let auth_manager = Arc::new(auth);
    let db_manager = Arc::new(db_manager);
    let cert_manager = Arc::new(cert_manager);

    let auth_service = VantaAuthServiceImpl {
        auth_manager: Arc::clone(&auth_manager),
        session_store: Arc::clone(&session_store),
        cert_manager: Arc::clone(&cert_manager),
    };

    let db_service = VantaDbServiceImpl {
        db_manager: Arc::clone(&db_manager),
    };

    let interceptor = AuthInterceptor {
        session_store: Arc::clone(&session_store),
    };

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

    println!(
        "\n  {} {}",
        "←".truecolor(120, 80, 255),
        "Server stopped.".dimmed()
    );
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl+c");
}
