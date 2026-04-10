use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};

use crate::auth::{Permission, Role};
use super::auth_interceptor::extract_auth_from_metadata;
use super::metrics::record_op;
use super::proto;
use super::service::{VantaAuthServiceImpl, ok_status, err_status};

#[tonic::async_trait]
impl proto::vanta_auth_server::VantaAuth for VantaAuthServiceImpl {
    async fn authenticate(
        &self,
        request: Request<proto::AuthRequest>,
    ) -> Result<Response<proto::AuthResponse>, Status> {
        let start = Instant::now();
        self.metrics.inc("auth_attempts");
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

        if let Some(remaining) = self.lockout_tracker.check(&username) {
            return Ok(Response::new(proto::AuthResponse {
                success: false,
                token: String::new(),
                role: String::new(),
                error: format!("Account locked, try again in {} seconds", remaining.as_secs()),
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
                self.audit_logger.record(
                    &username, &user.role.to_string(), "authenticate",
                    None, None, "{}", true, None,
                );
                let token = self.jwt_manager.create_token(&user.username, &user.role);
                record_op(&self.metrics, "authenticate", start, true);
                Ok(Response::new(proto::AuthResponse {
                    success: true,
                    token,
                    role: user.role.to_string(),
                    error: String::new(),
                }))
            }
            Ok(None) => {
                self.lockout_tracker.record_failure(&username);
                self.metrics.inc("auth_failures");
                self.audit_logger.record(
                    &username, "", "authenticate",
                    None, None, "{}", false, Some("Invalid credentials"),
                );
                record_op(&self.metrics, "authenticate", start, false);
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
        let usernames = tokio::task::spawn_blocking(move || auth.list_users())
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
                    success: true, cert_pem, key_pem, ca_pem, serial, error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(proto::IssueCertResponse {
                success: false, cert_pem: String::new(), key_pem: String::new(),
                ca_pem: String::new(), serial: String::new(), error: e.to_string(),
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
                serial: c.serial, username: c.username, issued_at: c.issued_at, revoked: c.revoked,
            })
            .collect();

        Ok(Response::new(proto::ListCertsResponse { certs: cert_msgs }))
    }

    async fn get_ca_cert(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::CaCertResponse>, Status> {
        let ca_pem = self.cert_manager.ca_cert_pem().to_string();
        Ok(Response::new(proto::CaCertResponse { ca_pem }))
    }

    async fn set_acl(
        &self,
        request: Request<proto::SetAclRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth_from_metadata(&request, &self.jwt_manager)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let req = request.into_inner();
        let permission = Permission::from_str(&req.permission)
            .ok_or_else(|| Status::invalid_argument(format!("Invalid permission '{}'", req.permission)))?;

        let acl = Arc::clone(&self.acl_manager);
        let username = req.username;
        let database = req.database;
        let collection = req.collection;

        let result = tokio::task::spawn_blocking(move || {
            if collection.is_empty() {
                acl.set_database_permission(&username, &database, permission)
            } else {
                acl.set_collection_permission(&username, &database, &collection, permission)
            }
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        result.map_err(|e| Status::internal(e.to_string()))?;
        ok_status()
    }

    async fn get_acl(
        &self,
        request: Request<proto::GetAclRequest>,
    ) -> Result<Response<proto::GetAclResponse>, Status> {
        let ctx = extract_auth_from_metadata(&request, &self.jwt_manager)?;
        let req = request.into_inner();

        if req.username != ctx.username && !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let acl = Arc::clone(&self.acl_manager);
        let username = req.username;

        let result = tokio::task::spawn_blocking(move || acl.get_user_acl(&username))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Some(user_acl) => {
                let databases: Vec<proto::DatabaseAclMsg> = user_acl
                    .databases
                    .into_iter()
                    .map(|db| proto::DatabaseAclMsg {
                        database: db.database,
                        permission: db.permission.to_string(),
                        collections: db.collections.into_iter().map(|c| proto::CollectionAclMsg {
                            collection: c.collection,
                            permission: c.permission.to_string(),
                        }).collect(),
                    })
                    .collect();
                Ok(Response::new(proto::GetAclResponse { found: true, databases }))
            }
            None => Ok(Response::new(proto::GetAclResponse { found: false, databases: vec![] })),
        }
    }

    async fn delete_acl(
        &self,
        request: Request<proto::DeleteAclRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth_from_metadata(&request, &self.jwt_manager)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let req = request.into_inner();
        let acl = Arc::clone(&self.acl_manager);
        let username = req.username;

        tokio::task::spawn_blocking(move || acl.delete_user_acl(&username))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| Status::internal(e.to_string()))?;

        ok_status()
    }

    async fn query_audit_log(
        &self,
        request: Request<proto::AuditLogRequest>,
    ) -> Result<Response<proto::AuditLogResponse>, Status> {
        let ctx = extract_auth_from_metadata(&request, &self.jwt_manager)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let req = request.into_inner();
        let logger = Arc::clone(&self.audit_logger);

        let since = if req.since.is_empty() {
            None
        } else {
            Some(chrono::DateTime::parse_from_rfc3339(&req.since)
                .map_err(|e| Status::invalid_argument(format!("Invalid since timestamp: {}", e)))?
                .with_timezone(&chrono::Utc))
        };

        let filter = super::audit::AuditFilter {
            username: if req.username.is_empty() { None } else { Some(req.username) },
            operation: if req.operation.is_empty() { None } else { Some(req.operation) },
            database: if req.database.is_empty() { None } else { Some(req.database) },
            since,
            limit: req.limit as usize,
        };

        let events = tokio::task::spawn_blocking(move || logger.query(&filter))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let entries: Vec<proto::AuditEntry> = events
            .into_iter()
            .map(|e| proto::AuditEntry {
                id: e.id, timestamp: e.timestamp.to_rfc3339(), username: e.username,
                role: e.role, operation: e.operation,
                database: e.database.unwrap_or_default(),
                collection: e.collection.unwrap_or_default(),
                detail: e.detail, success: e.success, error: e.error.unwrap_or_default(),
            })
            .collect();

        Ok(Response::new(proto::AuditLogResponse { entries }))
    }

    async fn get_metrics(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::MetricsResponse>, Status> {
        let ctx = extract_auth_from_metadata(&request, &self.jwt_manager)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let counters: Vec<proto::MetricEntry> = self.metrics.all_counters()
            .into_iter().map(|(name, value)| proto::MetricEntry { name, value }).collect();
        let gauges: Vec<proto::MetricEntry> = self.metrics.all_gauges()
            .into_iter().map(|(name, value)| proto::MetricEntry { name, value }).collect();
        let latencies: Vec<proto::LatencyEntry> = self.metrics.all_latencies()
            .into_iter().map(|(op, p50, p95, p99)| proto::LatencyEntry {
                operation: op, p50_us: p50, p95_us: p95, p99_us: p99,
            }).collect();

        Ok(Response::new(proto::MetricsResponse { counters, gauges, latencies }))
    }

    async fn health_check(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::HealthResponse>, Status> {
        Ok(Response::new(proto::HealthResponse {
            status: "healthy".to_string(),
            uptime_seconds: self.metrics.uptime_seconds(),
            database_count: self.metrics.gauge("databases_count"),
            active_transactions: self.metrics.gauge("active_transactions"),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }))
    }

    async fn create_backup(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::BackupResponse>, Status> {
        let ctx = extract_auth_from_metadata(&request, &self.jwt_manager)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let mgr = Arc::clone(&self.db_manager);
        let data_dir = self.data_dir.clone();

        let result = tokio::task::spawn_blocking(move || {
            crate::db::backup::create_backup(&mgr, &data_dir)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Ok(info) => Ok(Response::new(proto::BackupResponse {
                success: true, path: info.path, size_bytes: info.size_bytes,
                database_count: info.database_count as u64, document_count: info.document_count as u64,
                timestamp: info.timestamp, error: String::new(),
            })),
            Err(e) => Ok(Response::new(proto::BackupResponse {
                success: false, path: String::new(), size_bytes: 0,
                database_count: 0, document_count: 0, timestamp: String::new(), error: e.to_string(),
            })),
        }
    }

    async fn list_backups(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::ListBackupsResponse>, Status> {
        let ctx = extract_auth_from_metadata(&request, &self.jwt_manager)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let data_dir = self.data_dir.clone();
        let backups = tokio::task::spawn_blocking(move || {
            crate::db::backup::list_backups(&data_dir)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let msgs: Vec<proto::BackupInfoMsg> = backups
            .into_iter()
            .map(|b| proto::BackupInfoMsg { path: b.path, size_bytes: b.size_bytes, timestamp: b.timestamp })
            .collect();

        Ok(Response::new(proto::ListBackupsResponse { backups: msgs }))
    }

    async fn restore_backup(
        &self,
        request: Request<proto::RestoreRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth_from_metadata(&request, &self.jwt_manager)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let path = req.path;

        tokio::task::spawn_blocking(move || {
            crate::db::backup::restore_backup(&mgr, &path)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(|e| Status::internal(e.to_string()))?;

        ok_status()
    }

    // ---- Cluster status -----------------------------------------

    async fn cluster_status(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::ClusterStatusResponse>, Status> {
        let ctx = extract_auth_from_metadata(&request, &self.jwt_manager)?;
        if !ctx.role.can_admin() {
            return Err(Status::permission_denied("Admin role required"));
        }

        // In single-node mode, return this node as the only member
        Ok(Response::new(proto::ClusterStatusResponse {
            nodes: vec![proto::ClusterNodeInfo {
                node_id: 0,
                addr: "localhost".to_string(),
                role: "leader".to_string(),
            }],
            leader_id: 0,
        }))
    }
}
