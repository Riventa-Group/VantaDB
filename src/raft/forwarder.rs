use std::sync::Arc;
use parking_lot::RwLock;
use tonic::transport::Channel;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};

use crate::server::proto;
use proto::vanta_db_client::VantaDbClient;

/// Tracks the current leader address and provides forwarding capability.
/// Updated via openraft's metrics watcher.
pub struct LeaderForwarder {
    leader_addr: Arc<RwLock<Option<String>>>,
    client_port: u16,
}

impl LeaderForwarder {
    pub fn new(client_port: u16) -> Self {
        Self {
            leader_addr: Arc::new(RwLock::new(None)),
            client_port,
        }
    }

    /// Get a handle to the leader address for updating from Raft metrics.
    pub fn leader_addr_handle(&self) -> Arc<RwLock<Option<String>>> {
        Arc::clone(&self.leader_addr)
    }

    /// Get the current leader address, if known.
    pub fn current_leader(&self) -> Option<String> {
        self.leader_addr.read().clone()
    }

    /// Check if this node knows who the leader is.
    pub fn has_leader(&self) -> bool {
        self.leader_addr.read().is_some()
    }

    /// Create a gRPC client to the leader's client port.
    /// Returns None if leader is unknown.
    pub async fn leader_client(&self) -> Result<VantaDbClient<Channel>, Status> {
        let addr = self.leader_addr.read().clone()
            .ok_or_else(|| Status::unavailable("Cluster election in progress, retry"))?;

        // The addr is the Raft advertise address (raft port).
        // We need the client port. Parse the host and use client_port.
        let client_addr = if let Some(colon_pos) = addr.rfind(':') {
            let host = &addr[..colon_pos];
            format!("http://{}:{}", host, self.client_port)
        } else {
            format!("http://{}:{}", addr, self.client_port)
        };

        let channel = Channel::from_shared(client_addr.clone())
            .map_err(|e| Status::internal(format!("Invalid leader address: {}", e)))?
            .connect()
            .await
            .map_err(|e| Status::unavailable(format!("Cannot reach leader at {}: {}", client_addr, e)))?;

        Ok(VantaDbClient::new(channel))
    }
}

/// Forward a write request to the leader, preserving the auth token.
pub async fn forward_insert(
    forwarder: &LeaderForwarder,
    token: &str,
    req: proto::InsertRequest,
) -> Result<Response<proto::InsertResponse>, Status> {
    let mut client = forwarder.leader_client().await?;
    let mut request = Request::new(req);
    attach_token(&mut request, token);
    client.insert(request).await
}

pub async fn forward_status_req<F, Req>(
    forwarder: &LeaderForwarder,
    token: &str,
    req: Req,
    rpc_fn: F,
) -> Result<Response<proto::StatusResponse>, Status>
where
    F: FnOnce(VantaDbClient<Channel>, Request<Req>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Response<proto::StatusResponse>, Status>> + Send>>,
    Req: Send + 'static,
{
    let client = forwarder.leader_client().await?;
    let mut request = Request::new(req);
    attach_token(&mut request, token);
    rpc_fn(client, request).await
}

fn attach_token<T>(request: &mut Request<T>, token: &str) {
    if !token.is_empty() {
        if let Ok(val) = format!("Bearer {}", token).parse::<MetadataValue<_>>() {
            request.metadata_mut().insert("authorization", val);
        }
    }
}
