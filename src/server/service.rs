use std::path::PathBuf;
use std::sync::Arc;
use tonic::{Response, Status};

use crate::auth::{AclManager, AuthManager, CertManager};
use crate::db::{DatabaseManager, QueryOptions, VantaError};
use crate::raft::{LeaderForwarder, LeaseTracker, RaftOp, RaftResponse, VantaRaft};
use super::auth_interceptor::AuthContext;
use super::proto;
use super::audit::AuditLogger;
use super::lockout::LockoutTracker;
use super::metrics::MetricsCollector;
use super::rate_limit::{GlobalRateLimiter, RateLimiter};
use super::session::JwtSessionManager;

// ---- VantaAuth Service ---------------------------------------

pub struct VantaAuthServiceImpl {
    pub auth_manager: Arc<AuthManager>,
    pub jwt_manager: Arc<JwtSessionManager>,
    pub cert_manager: Arc<CertManager>,
    pub acl_manager: Arc<AclManager>,
    pub audit_logger: Arc<AuditLogger>,
    pub metrics: Arc<MetricsCollector>,
    pub db_manager: Arc<DatabaseManager>,
    pub data_dir: PathBuf,
    pub lockout_tracker: Arc<LockoutTracker>,
    pub global_auth_limiter: Arc<GlobalRateLimiter>,
    pub ip_auth_limiter: Arc<RateLimiter>,
}

// ---- VantaDb Service -----------------------------------------

pub struct VantaDbServiceImpl {
    pub db_manager: Arc<DatabaseManager>,
    pub acl_manager: Arc<AclManager>,
    pub audit_logger: Arc<AuditLogger>,
    pub metrics: Arc<MetricsCollector>,
    pub raft: Option<Arc<VantaRaft>>,
    pub forwarder: Option<Arc<LeaderForwarder>>,
    pub lease: Option<Arc<LeaseTracker>>,
}

/// Check if this node can serve a read with the given consistency.
/// Returns Ok(()) if the read can be served locally, Err(Status) if not.
pub fn check_read_permission(
    raft: &Option<Arc<VantaRaft>>,
    lease: &Option<Arc<LeaseTracker>>,
    consistency: crate::raft::ReadConsistency,
) -> Result<(), Status> {
    use crate::raft::ReadConsistency;

    match raft {
        None => Ok(()), // Single-node mode: always serve locally
        Some(r) => {
            let metrics = r.metrics().borrow().clone();
            let is_leader = metrics.current_leader == metrics.id.into();

            match consistency {
                ReadConsistency::Strong => {
                    if is_leader {
                        Ok(())
                    } else {
                        let leader_hint = metrics.current_leader
                            .map(|id| format!(" (leader_id={})", id))
                            .unwrap_or_default();
                        Err(Status::unavailable(format!(
                            "Strong read requires leader{}",
                            leader_hint
                        )))
                    }
                }
                ReadConsistency::Lease => {
                    if is_leader {
                        Ok(()) // Leader can always serve reads
                    } else if let Some(ref l) = lease {
                        if l.is_valid() {
                            Ok(()) // Lease is valid, serve locally
                        } else {
                            Err(Status::unavailable("Read lease expired, retry on leader"))
                        }
                    } else {
                        Err(Status::unavailable("No lease tracker available"))
                    }
                }
            }
        }
    }
}

/// Propose a write through Raft if clustered, or execute directly if single-node.
/// On followers, returns UNAVAILABLE with leader address hint for client retry.
pub async fn raft_propose_or_direct<F>(
    raft: &Option<Arc<VantaRaft>>,
    op: RaftOp,
    direct_fn: F,
) -> Result<RaftResponse, Status>
where
    F: FnOnce() -> Result<RaftResponse, VantaError> + Send + 'static,
{
    match raft {
        Some(r) => {
            let result = r.client_write(op).await;
            match result {
                Ok(resp) => Ok(resp.data),
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("ForwardToLeader") {
                        // Extract leader info if available and include in error
                        let metrics = r.metrics().borrow().clone();
                        let leader_hint = metrics.current_leader
                            .map(|id| format!(" (leader_id={})", id))
                            .unwrap_or_default();
                        Err(Status::unavailable(format!(
                            "Not leader, retry on another node{}",
                            leader_hint
                        )))
                    } else {
                        Err(Status::internal(format!("Raft error: {}", msg)))
                    }
                }
            }
        }
        None => {
            // Single-node mode: execute directly
            tokio::task::spawn_blocking(direct_fn)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .map_err(|e| e.to_grpc_status())
        }
    }
}

// ---- Shared helpers ------------------------------------------

pub fn vanta_err(e: VantaError) -> Status {
    e.to_grpc_status()
}

pub fn require_read(acl: &AclManager, ctx: &AuthContext, db: &str, col: Option<&str>) -> Result<(), Status> {
    let perm = acl.resolve(&ctx.username, &ctx.role, db, col);
    if perm.can_read() { Ok(()) } else { Err(Status::permission_denied("Read permission denied")) }
}

pub fn require_write(acl: &AclManager, ctx: &AuthContext, db: &str, col: Option<&str>) -> Result<(), Status> {
    let perm = acl.resolve(&ctx.username, &ctx.role, db, col);
    if perm.can_write() { Ok(()) } else { Err(Status::permission_denied("Write permission denied")) }
}

pub fn require_admin(acl: &AclManager, ctx: &AuthContext, db: &str, col: Option<&str>) -> Result<(), Status> {
    let perm = acl.resolve(&ctx.username, &ctx.role, db, col);
    if perm.can_admin() { Ok(()) } else { Err(Status::permission_denied("Admin permission denied")) }
}

pub fn ok_status() -> Result<Response<proto::StatusResponse>, Status> {
    Ok(Response::new(proto::StatusResponse { success: true, error: String::new() }))
}

pub fn err_status(msg: String) -> Result<Response<proto::StatusResponse>, Status> {
    Ok(Response::new(proto::StatusResponse { success: false, error: msg }))
}

pub fn docs_response(
    docs: Vec<serde_json::Value>,
    total: usize,
) -> Result<Response<proto::DocumentsResponse>, Status> {
    let documents_json: Result<Vec<String>, _> = docs.iter().map(serde_json::to_string).collect();
    let documents_json = documents_json.map_err(|e| Status::internal(e.to_string()))?;
    Ok(Response::new(proto::DocumentsResponse {
        documents_json,
        total_count: total as u64,
        error: String::new(),
    }))
}

pub fn to_query_options(
    pagination: Option<proto::PaginationOptions>,
    sort: Option<proto::SortOptions>,
) -> QueryOptions {
    let mut opts = QueryOptions::default();
    if let Some(p) = pagination {
        opts.page = p.page;
        opts.page_size = if p.page_size == 0 { 50 } else { p.page_size };
    }
    if let Some(s) = sort {
        if !s.field.is_empty() {
            opts.sort_field = Some(s.field);
            opts.sort_descending = s.descending;
        }
    }
    opts
}
