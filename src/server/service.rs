use std::path::PathBuf;
use std::sync::Arc;
use tonic::{Response, Status};

use crate::auth::{AclManager, AuthManager, CertManager};
use crate::db::{DatabaseManager, QueryOptions, VantaError};
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
