use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::auth::{AuthManager, CertManager, Role};
use crate::db::{CollectionSchema, DatabaseManager, QueryOptions, VantaError};
use super::auth_interceptor::{extract_auth, extract_auth_from_metadata};
use super::proto;
use super::lockout::LockoutTracker;
use super::rate_limit::{GlobalRateLimiter, RateLimiter};
use super::session::JwtSessionManager;

// ---- VantaAuth Service ---------------------------------------

pub struct VantaAuthServiceImpl {
    pub auth_manager: Arc<AuthManager>,
    pub jwt_manager: Arc<JwtSessionManager>,
    pub cert_manager: Arc<CertManager>,
    pub lockout_tracker: Arc<LockoutTracker>,
    pub global_auth_limiter: Arc<GlobalRateLimiter>,
    pub ip_auth_limiter: Arc<RateLimiter>,
}

#[tonic::async_trait]
impl proto::vanta_auth_server::VantaAuth for VantaAuthServiceImpl {
    async fn authenticate(
        &self,
        request: Request<proto::AuthRequest>,
    ) -> Result<Response<proto::AuthResponse>, Status> {
        // Rate limiting: global + per-IP
        if !self.global_auth_limiter.allow() {
            return Err(Status::resource_exhausted("Rate limit exceeded, try again later"));
        }

        let peer_ip = request.remote_addr()
            .map(|a| a.ip().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        if !self.ip_auth_limiter.allow(&peer_ip) {
            return Err(Status::resource_exhausted("Too many auth attempts from this address"));
        }

        let req = request.into_inner();
        let username = req.username;

        // Lockout check
        if let Some(remaining) = self.lockout_tracker.check(&username) {
            return Ok(Response::new(proto::AuthResponse {
                success: false,
                token: String::new(),
                role: String::new(),
                error: format!(
                    "Account locked, try again in {} seconds",
                    remaining.as_secs()
                ),
            }));
        }

        let auth = Arc::clone(&self.auth_manager);
        let user_clone = username.clone();
        let password = req.password;

        let result = tokio::task::spawn_blocking(move || {
            auth.authenticate(&user_clone, &password)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Ok(Some(user)) => {
                self.lockout_tracker.clear(&username);
                let token = self
                    .jwt_manager
                    .create_token(&user.username, &user.role);
                Ok(Response::new(proto::AuthResponse {
                    success: true,
                    token,
                    role: user.role.to_string(),
                    error: String::new(),
                }))
            }
            Ok(None) => {
                self.lockout_tracker.record_failure(&username);
                Ok(Response::new(proto::AuthResponse {
                    success: false,
                    token: String::new(),
                    role: String::new(),
                    error: "Invalid credentials".into(),
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn create_user(
        &self,
        request: Request<proto::CreateUserRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth_from_metadata(&request, &self.jwt_manager)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let req = request.into_inner();
        let role = Role::from_str(&req.role)
            .ok_or_else(|| Status::invalid_argument(format!("Invalid role '{}'", req.role)))?;

        let auth = Arc::clone(&self.auth_manager);
        let username = req.username;
        let password = req.password;
        let databases = req.databases;

        let result = tokio::task::spawn_blocking(move || {
            auth.create_user(&username, &password, role, databases)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Ok(Ok(())) => ok_status(),
            Ok(Err(e)) => err_status(e),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn delete_user(
        &self,
        request: Request<proto::DeleteUserRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth_from_metadata(&request, &self.jwt_manager)?;
        if !ctx.role.is_root() {
            return Err(Status::permission_denied("Root role required"));
        }

        let req = request.into_inner();
        let auth = Arc::clone(&self.auth_manager);
        let username = req.username;

        let result = tokio::task::spawn_blocking(move || auth.delete_user(&username))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Ok(Ok(())) => ok_status(),
            Ok(Err(e)) => err_status(e),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn set_password(
        &self,
        request: Request<proto::SetPasswordRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth_from_metadata(&request, &self.jwt_manager)?;
        let req = request.into_inner();

        if req.username != ctx.username && !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let auth = Arc::clone(&self.auth_manager);
        let username = req.username;
        let password = req.password;

        let result = tokio::task::spawn_blocking(move || auth.set_password(&username, &password))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Ok(Ok(())) => ok_status(),
            Ok(Err(e)) => err_status(e),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn list_users(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::ListUsersResponse>, Status> {
        let ctx = extract_auth_from_metadata(&request, &self.jwt_manager)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let auth = Arc::clone(&self.auth_manager);
        let usernames =
            tokio::task::spawn_blocking(move || auth.list_users())
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::ListUsersResponse { usernames }))
    }

    async fn get_user(
        &self,
        request: Request<proto::GetUserRequest>,
    ) -> Result<Response<proto::GetUserResponse>, Status> {
        let ctx = extract_auth_from_metadata(&request, &self.jwt_manager)?;
        let req = request.into_inner();

        if req.username != ctx.username && !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let auth = Arc::clone(&self.auth_manager);
        let username = req.username;

        let result = tokio::task::spawn_blocking(move || auth.get_user(&username))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Ok(Some(user)) => Ok(Response::new(proto::GetUserResponse {
                found: true,
                username: user.username,
                role: user.role.to_string(),
                created_at: user.created_at.to_rfc3339(),
                databases: user.databases,
            })),
            Ok(None) => Ok(Response::new(proto::GetUserResponse {
                found: false,
                username: String::new(),
                role: String::new(),
                created_at: String::new(),
                databases: vec![],
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    // ---- Certificate management ---------------------------------

    async fn issue_cert(
        &self,
        request: Request<proto::IssueCertRequest>,
    ) -> Result<Response<proto::IssueCertResponse>, Status> {
        let ctx = extract_auth_from_metadata(&request, &self.jwt_manager)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let req = request.into_inner();
        let cm = Arc::clone(&self.cert_manager);
        let username = req.username;

        let result = tokio::task::spawn_blocking(move || cm.issue_cert(&username))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Ok((cert_pem, key_pem, serial)) => {
                let ca_pem = self.cert_manager.ca_cert_pem().to_string();
                Ok(Response::new(proto::IssueCertResponse {
                    success: true,
                    cert_pem,
                    key_pem,
                    ca_pem,
                    serial,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(proto::IssueCertResponse {
                success: false,
                cert_pem: String::new(),
                key_pem: String::new(),
                ca_pem: String::new(),
                serial: String::new(),
                error: e.to_string(),
            })),
        }
    }

    async fn revoke_cert(
        &self,
        request: Request<proto::RevokeCertRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth_from_metadata(&request, &self.jwt_manager)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let req = request.into_inner();
        let cm = Arc::clone(&self.cert_manager);
        let serial = req.serial;

        let result = tokio::task::spawn_blocking(move || cm.revoke_cert(&serial))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Ok(true) => ok_status(),
            Ok(false) => err_status("Certificate not found".to_string()),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn list_certs(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::ListCertsResponse>, Status> {
        let ctx = extract_auth_from_metadata(&request, &self.jwt_manager)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let cm = Arc::clone(&self.cert_manager);
        let certs = tokio::task::spawn_blocking(move || cm.list_certs())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let cert_msgs: Vec<proto::CertInfoMsg> = certs
            .into_iter()
            .map(|c| proto::CertInfoMsg {
                serial: c.serial,
                username: c.username,
                issued_at: c.issued_at,
                revoked: c.revoked,
            })
            .collect();

        Ok(Response::new(proto::ListCertsResponse { certs: cert_msgs }))
    }

    async fn get_ca_cert(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::CaCertResponse>, Status> {
        // CA cert is public — no auth required
        let ca_pem = self.cert_manager.ca_cert_pem().to_string();
        Ok(Response::new(proto::CaCertResponse { ca_pem }))
    }
}

// ---- VantaDb Service -----------------------------------------

pub struct VantaDbServiceImpl {
    pub db_manager: Arc<DatabaseManager>,
}

/// Helper to convert VantaError to gRPC Status
fn vanta_err(e: VantaError) -> Status {
    e.to_grpc_status()
}

#[tonic::async_trait]
impl proto::vanta_db_server::VantaDb for VantaDbServiceImpl {
    // ---- Database ops ----------------------------------------

    async fn create_database(
        &self,
        request: Request<proto::DatabaseRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;

        tokio::task::spawn_blocking(move || mgr.create_database(&db))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        ok_status()
    }

    async fn drop_database(
        &self,
        request: Request<proto::DatabaseRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;

        tokio::task::spawn_blocking(move || mgr.drop_database(&db))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        ok_status()
    }

    async fn list_databases(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::ListDatabasesResponse>, Status> {
        let _ctx = extract_auth(&request)?;

        let mgr = Arc::clone(&self.db_manager);
        let databases =
            tokio::task::spawn_blocking(move || mgr.list_databases())
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::ListDatabasesResponse { databases }))
    }

    // ---- Collection ops --------------------------------------

    async fn create_collection(
        &self,
        request: Request<proto::CollectionRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        if !ctx.role.can_write() {
            return Err(Status::permission_denied("Write permission required"));
        }

        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        tokio::task::spawn_blocking(move || mgr.create_collection(&db, &col))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        ok_status()
    }

    async fn drop_collection(
        &self,
        request: Request<proto::CollectionRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        tokio::task::spawn_blocking(move || mgr.drop_collection(&db, &col))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        ok_status()
    }

    async fn list_collections(
        &self,
        request: Request<proto::DatabaseRequest>,
    ) -> Result<Response<proto::ListCollectionsResponse>, Status> {
        let _ctx = extract_auth(&request)?;

        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;

        let collections = tokio::task::spawn_blocking(move || mgr.list_collections(&db))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        Ok(Response::new(proto::ListCollectionsResponse { collections }))
    }

    // ---- Document CRUD ---------------------------------------

    async fn insert(
        &self,
        request: Request<proto::InsertRequest>,
    ) -> Result<Response<proto::InsertResponse>, Status> {
        let ctx = extract_auth(&request)?;
        if !ctx.role.can_write() {
            return Err(Status::permission_denied("Write permission required"));
        }

        let req = request.into_inner();
        let doc: serde_json::Value = serde_json::from_str(&req.document_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid JSON: {}", e)))?;

        if !doc.is_object() {
            return Err(Status::invalid_argument("Document must be a JSON object"));
        }

        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        let result = tokio::task::spawn_blocking(move || mgr.insert(&db, &col, doc))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Ok(id) => Ok(Response::new(proto::InsertResponse {
                success: true,
                id,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(proto::InsertResponse {
                success: false,
                id: String::new(),
                error: e.to_string(),
            })),
        }
    }

    async fn find_by_id(
        &self,
        request: Request<proto::FindByIdRequest>,
    ) -> Result<Response<proto::DocumentResponse>, Status> {
        let _ctx = extract_auth(&request)?;

        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;
        let id = req.id;

        let result = tokio::task::spawn_blocking(move || mgr.find_by_id(&db, &col, &id))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        match result {
            Some(doc) => {
                let json = serde_json::to_string(&doc)
                    .map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(proto::DocumentResponse {
                    found: true,
                    document_json: json,
                    error: String::new(),
                }))
            }
            None => Ok(Response::new(proto::DocumentResponse {
                found: false,
                document_json: String::new(),
                error: String::new(),
            })),
        }
    }

    async fn find_all(
        &self,
        request: Request<proto::FindAllRequest>,
    ) -> Result<Response<proto::DocumentsResponse>, Status> {
        let _ctx = extract_auth(&request)?;

        let req = request.into_inner();
        let opts = to_query_options(req.pagination, req.sort);
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        let (docs, total) = tokio::task::spawn_blocking(move || mgr.find_all_query(&db, &col, &opts))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        docs_response(docs, total)
    }

    async fn find_where(
        &self,
        request: Request<proto::FindWhereRequest>,
    ) -> Result<Response<proto::DocumentsResponse>, Status> {
        let _ctx = extract_auth(&request)?;

        let req = request.into_inner();
        let value: serde_json::Value = serde_json::from_str(&req.value_json)
            .unwrap_or(serde_json::Value::String(req.value_json.clone()));

        let opts = to_query_options(req.pagination, req.sort);
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;
        let field = req.field;

        let (docs, total) =
            tokio::task::spawn_blocking(move || {
                mgr.find_where_query(&db, &col, &field, &value, &opts)
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        docs_response(docs, total)
    }

    async fn delete_by_id(
        &self,
        request: Request<proto::DeleteByIdRequest>,
    ) -> Result<Response<proto::DeleteResponse>, Status> {
        let ctx = extract_auth(&request)?;
        if !ctx.role.can_write() {
            return Err(Status::permission_denied("Write permission required"));
        }

        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;
        let id = req.id;

        let result = tokio::task::spawn_blocking(move || mgr.delete_by_id(&db, &col, &id))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Ok(found) => Ok(Response::new(proto::DeleteResponse {
                success: true,
                found,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(proto::DeleteResponse {
                success: false,
                found: false,
                error: e.to_string(),
            })),
        }
    }

    async fn count(
        &self,
        request: Request<proto::CountRequest>,
    ) -> Result<Response<proto::CountResponse>, Status> {
        let _ctx = extract_auth(&request)?;

        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        let count = tokio::task::spawn_blocking(move || mgr.count(&db, &col))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        Ok(Response::new(proto::CountResponse {
            count: count as u64,
            error: String::new(),
        }))
    }

    // ---- Update ops ------------------------------------------

    async fn update(
        &self,
        request: Request<proto::UpdateRequest>,
    ) -> Result<Response<proto::UpdateResponse>, Status> {
        let ctx = extract_auth(&request)?;
        if !ctx.role.can_write() {
            return Err(Status::permission_denied("Write permission required"));
        }

        let req = request.into_inner();
        let patch: serde_json::Value = serde_json::from_str(&req.patch_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid JSON: {}", e)))?;

        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;
        let id = req.id;

        let result =
            tokio::task::spawn_blocking(move || mgr.update_by_id(&db, &col, &id, patch))
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Ok(found) => Ok(Response::new(proto::UpdateResponse {
                success: true,
                modified_count: if found { 1 } else { 0 },
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(proto::UpdateResponse {
                success: false,
                modified_count: 0,
                error: e.to_string(),
            })),
        }
    }

    async fn update_where(
        &self,
        request: Request<proto::UpdateWhereRequest>,
    ) -> Result<Response<proto::UpdateResponse>, Status> {
        let ctx = extract_auth(&request)?;
        if !ctx.role.can_write() {
            return Err(Status::permission_denied("Write permission required"));
        }

        let req = request.into_inner();
        let filter: serde_json::Value = serde_json::from_str(&req.filter_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid filter JSON: {}", e)))?;
        let patch: serde_json::Value = serde_json::from_str(&req.patch_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid patch JSON: {}", e)))?;

        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        let result =
            tokio::task::spawn_blocking(move || mgr.update_where(&db, &col, &filter, &patch))
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Ok(count) => Ok(Response::new(proto::UpdateResponse {
                success: true,
                modified_count: count,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(proto::UpdateResponse {
                success: false,
                modified_count: 0,
                error: e.to_string(),
            })),
        }
    }

    // ---- Rich query ------------------------------------------

    async fn query(
        &self,
        request: Request<proto::QueryRequest>,
    ) -> Result<Response<proto::DocumentsResponse>, Status> {
        let _ctx = extract_auth(&request)?;

        let req = request.into_inner();
        let filter: serde_json::Value = serde_json::from_str(&req.filter_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid filter JSON: {}", e)))?;

        let opts = to_query_options(req.pagination, req.sort);
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        let (docs, total) =
            tokio::task::spawn_blocking(move || mgr.query(&db, &col, &filter, &opts))
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .map_err(vanta_err)?;

        docs_response(docs, total)
    }

    // ---- Aggregation -----------------------------------------

    async fn aggregate(
        &self,
        request: Request<proto::AggregateRequest>,
    ) -> Result<Response<proto::AggregateResponse>, Status> {
        let _ctx = extract_auth(&request)?;

        let req = request.into_inner();
        let pipeline: Vec<serde_json::Value> = serde_json::from_str(&req.pipeline_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid pipeline JSON: {}", e)))?;

        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        let result =
            tokio::task::spawn_blocking(move || mgr.aggregate(&db, &col, &pipeline))
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Ok(results) => {
                let results_json: Result<Vec<String>, _> =
                    results.iter().map(serde_json::to_string).collect();
                let results_json =
                    results_json.map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(proto::AggregateResponse {
                    results_json,
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(proto::AggregateResponse {
                results_json: vec![],
                error: e.to_string(),
            })),
        }
    }

    // ---- Index ops -------------------------------------------

    async fn create_index(
        &self,
        request: Request<proto::IndexRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        if !ctx.role.can_write() {
            return Err(Status::permission_denied("Write permission required"));
        }

        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;
        let field = req.field;
        let unique = req.unique;

        tokio::task::spawn_blocking(move || mgr.create_index(&db, &col, &field, unique))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        ok_status()
    }

    async fn drop_index(
        &self,
        request: Request<proto::IndexRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;
        let field = req.field;

        tokio::task::spawn_blocking(move || mgr.drop_index(&db, &col, &field))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        ok_status()
    }

    async fn list_indexes(
        &self,
        request: Request<proto::ListIndexesRequest>,
    ) -> Result<Response<proto::ListIndexesResponse>, Status> {
        let _ctx = extract_auth(&request)?;

        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        let indexes = tokio::task::spawn_blocking(move || mgr.list_indexes(&db, &col))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        let infos: Vec<proto::IndexInfo> = indexes
            .iter()
            .map(|idx| proto::IndexInfo {
                field: idx.field.clone(),
                unique: idx.unique,
            })
            .collect();
        Ok(Response::new(proto::ListIndexesResponse { indexes: infos }))
    }

    // ---- Schema ops ------------------------------------------

    async fn set_schema(
        &self,
        request: Request<proto::SetSchemaRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let req = request.into_inner();
        let schema_val: serde_json::Value = serde_json::from_str(&req.schema_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid schema JSON: {}", e)))?;
        let schema = CollectionSchema::from_json(&schema_val)
            .map_err(|e| Status::invalid_argument(e))?;

        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        tokio::task::spawn_blocking(move || mgr.set_schema(&db, &col, &schema))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        ok_status()
    }

    async fn get_schema(
        &self,
        request: Request<proto::GetSchemaRequest>,
    ) -> Result<Response<proto::GetSchemaResponse>, Status> {
        let _ctx = extract_auth(&request)?;

        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        let result = tokio::task::spawn_blocking(move || mgr.get_schema(&db, &col))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        match result {
            Some(schema) => {
                let json = serde_json::to_string(&schema.to_json())
                    .map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(proto::GetSchemaResponse {
                    found: true,
                    schema_json: json,
                }))
            }
            None => Ok(Response::new(proto::GetSchemaResponse {
                found: false,
                schema_json: String::new(),
            })),
        }
    }

    async fn drop_schema(
        &self,
        request: Request<proto::GetSchemaRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        tokio::task::spawn_blocking(move || mgr.drop_schema(&db, &col))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        ok_status()
    }

    // ---- Transaction ops -------------------------------------

    async fn begin_tx(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::TxResponse>, Status> {
        let _ctx = extract_auth(&request)?;

        let mgr = Arc::clone(&self.db_manager);
        let tx_id =
            tokio::task::spawn_blocking(move || mgr.begin_transaction())
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::TxResponse { tx_id }))
    }

    async fn commit_tx(
        &self,
        request: Request<proto::TxRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        if !ctx.role.can_write() {
            return Err(Status::permission_denied("Write permission required"));
        }

        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let tx_id = req.tx_id;

        tokio::task::spawn_blocking(move || mgr.commit_transaction(&tx_id))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        ok_status()
    }

    async fn rollback_tx(
        &self,
        request: Request<proto::TxRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let _ctx = extract_auth(&request)?;

        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let tx_id = req.tx_id;

        tokio::task::spawn_blocking(move || mgr.rollback_transaction(&tx_id))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        ok_status()
    }

    async fn tx_insert(
        &self,
        request: Request<proto::TxInsertRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        if !ctx.role.can_write() {
            return Err(Status::permission_denied("Write permission required"));
        }

        let req = request.into_inner();
        let doc: serde_json::Value = serde_json::from_str(&req.document_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid JSON: {}", e)))?;

        let mgr = Arc::clone(&self.db_manager);
        let tx_id = req.tx_id;
        let db = req.database;
        let col = req.collection;

        tokio::task::spawn_blocking(move || mgr.tx_insert(&tx_id, db, col, doc))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        ok_status()
    }

    async fn tx_update(
        &self,
        request: Request<proto::TxUpdateRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        if !ctx.role.can_write() {
            return Err(Status::permission_denied("Write permission required"));
        }

        let req = request.into_inner();
        let patch: serde_json::Value = serde_json::from_str(&req.patch_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid JSON: {}", e)))?;

        let mgr = Arc::clone(&self.db_manager);
        let tx_id = req.tx_id;
        let db = req.database;
        let col = req.collection;
        let id = req.id;

        tokio::task::spawn_blocking(move || mgr.tx_update(&tx_id, db, col, id, patch))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        ok_status()
    }

    async fn tx_delete(
        &self,
        request: Request<proto::TxDeleteRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        if !ctx.role.can_write() {
            return Err(Status::permission_denied("Write permission required"));
        }

        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let tx_id = req.tx_id;
        let db = req.database;
        let col = req.collection;
        let id = req.id;

        tokio::task::spawn_blocking(move || mgr.tx_delete(&tx_id, db, col, id))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        ok_status()
    }
}

// ---- Helpers -------------------------------------------------

fn ok_status() -> Result<Response<proto::StatusResponse>, Status> {
    Ok(Response::new(proto::StatusResponse {
        success: true,
        error: String::new(),
    }))
}

fn err_status(msg: String) -> Result<Response<proto::StatusResponse>, Status> {
    Ok(Response::new(proto::StatusResponse {
        success: false,
        error: msg,
    }))
}

fn docs_response(
    docs: Vec<serde_json::Value>,
    total: usize,
) -> Result<Response<proto::DocumentsResponse>, Status> {
    let documents_json: Result<Vec<String>, _> =
        docs.iter().map(serde_json::to_string).collect();
    let documents_json =
        documents_json.map_err(|e| Status::internal(e.to_string()))?;
    Ok(Response::new(proto::DocumentsResponse {
        documents_json,
        total_count: total as u64,
        error: String::new(),
    }))
}

fn to_query_options(
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
