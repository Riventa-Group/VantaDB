pub mod auth_interceptor;
pub mod service;
pub mod session;

use std::sync::Arc;
use colored::*;
use tonic::transport::Server;

use crate::auth::AuthManager;
use crate::db::DatabaseManager;
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
) -> Result<(), Box<dyn std::error::Error>> {
    let addr: std::net::SocketAddr = format!("0.0.0.0:{}", port).parse()?;

    let session_store = Arc::new(SessionStore::new(24));
    let auth_manager = Arc::new(auth);
    let db_manager = Arc::new(db_manager);

    let auth_service = VantaAuthServiceImpl {
        auth_manager: Arc::clone(&auth_manager),
        session_store: Arc::clone(&session_store),
    };

    let db_service = VantaDbServiceImpl {
        db_manager: Arc::clone(&db_manager),
    };

    let interceptor = AuthInterceptor {
        session_store: Arc::clone(&session_store),
    };

    println!(
        "  {} gRPC server listening on {}",
        "✓".green().bold(),
        addr.to_string().bold().truecolor(120, 200, 120)
    );
    println!(
        "  {} Ctrl+C to stop",
        "ℹ".truecolor(120, 80, 255)
    );
    println!();

    Server::builder()
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
