use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::db::{CollectionSchema, QueryOptions, VantaError};
use crate::raft::{RaftOp, RaftResponse};
use super::auth_interceptor::{extract_auth, AuthContext};
use super::metrics::record_op;
use super::proto;
use super::service::{VantaDbServiceImpl, ok_status, docs_response, to_query_options, require_read, require_write, require_admin, vanta_err, raft_propose_or_direct};

#[tonic::async_trait]
impl proto::vanta_db_server::VantaDb for VantaDbServiceImpl {
    type WatchCollectionStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<proto::WatchEvent, Status>> + Send>>;

    async fn create_database(
        &self,
        request: Request<proto::DatabaseRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let start = Instant::now();
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_admin(&self.acl_manager, &ctx, &req.database, None)?;

        let mgr = Arc::clone(&self.db_manager);
        let db = req.database.clone();

        raft_propose_or_direct(
            &self.raft,
            RaftOp::CreateDatabase { name: db.clone() },
            move || mgr.create_database(&db).map(|_| RaftResponse::Ok),
        ).await?;

        self.metrics.gauge_inc("databases_count");
        self.audit_logger.record(&ctx.username, &ctx.role.to_string(), "create_database", Some(&req.database), None, "{}", true, None);
        record_op(&self.metrics, "create_database", start, true);
        ok_status()
    }

    async fn drop_database(
        &self,
        request: Request<proto::DatabaseRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let start = Instant::now();
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_admin(&self.acl_manager, &ctx, &req.database, None)?;

        let mgr = Arc::clone(&self.db_manager);
        let db = req.database.clone();

        raft_propose_or_direct(
            &self.raft,
            RaftOp::DropDatabase { name: db.clone() },
            move || mgr.drop_database(&db).map(|_| RaftResponse::Ok),
        ).await?;

        self.metrics.gauge_dec("databases_count");
        self.audit_logger.record(&ctx.username, &ctx.role.to_string(), "drop_database", Some(&req.database), None, "{}", true, None);
        record_op(&self.metrics, "drop_database", start, true);
        ok_status()
    }

    async fn list_databases(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::ListDatabasesResponse>, Status> {
        let _ctx = extract_auth(&request)?;

        let mgr = Arc::clone(&self.db_manager);
        let databases = tokio::task::spawn_blocking(move || mgr.list_databases())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::ListDatabasesResponse { databases }))
    }

    async fn create_collection(
        &self,
        request: Request<proto::CollectionRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_write(&self.acl_manager, &ctx, &req.database, None)?;

        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        raft_propose_or_direct(
            &self.raft,
            RaftOp::CreateCollection { db: db.clone(), col: col.clone() },
            move || mgr.create_collection(&db, &col).map(|_| RaftResponse::Ok),
        ).await?;

        ok_status()
    }

    async fn drop_collection(
        &self,
        request: Request<proto::CollectionRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_admin(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;

        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        raft_propose_or_direct(
            &self.raft,
            RaftOp::DropCollection { db: db.clone(), col: col.clone() },
            move || mgr.drop_collection(&db, &col).map(|_| RaftResponse::Ok),
        ).await?;

        ok_status()
    }

    async fn list_collections(
        &self,
        request: Request<proto::DatabaseRequest>,
    ) -> Result<Response<proto::ListCollectionsResponse>, Status> {
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_read(&self.acl_manager, &ctx, &req.database, None)?;

        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;

        let collections = tokio::task::spawn_blocking(move || mgr.list_collections(&db))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        Ok(Response::new(proto::ListCollectionsResponse { collections }))
    }

    async fn insert(
        &self,
        request: Request<proto::InsertRequest>,
    ) -> Result<Response<proto::InsertResponse>, Status> {
        let start = Instant::now();
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_write(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let doc: serde_json::Value = serde_json::from_str(&req.document_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid JSON: {}", e)))?;

        if !doc.is_object() {
            return Err(Status::invalid_argument("Document must be a JSON object"));
        }

        let mgr = Arc::clone(&self.db_manager);
        let db = req.database.clone();
        let col = req.collection.clone();
        let doc_clone = doc.clone();

        let resp = raft_propose_or_direct(
            &self.raft,
            RaftOp::Insert { db: db.clone(), col: col.clone(), doc },
            move || mgr.insert(&db, &col, doc_clone).map(|id| RaftResponse::InsertOk { id }),
        ).await;

        let (success, id, error) = match resp {
            Ok(RaftResponse::InsertOk { id }) => (true, id, String::new()),
            Ok(RaftResponse::Error { message }) => (false, String::new(), message),
            Ok(_) => (true, String::new(), String::new()),
            Err(e) => (false, String::new(), e.message().to_string()),
        };

        if success {
            self.audit_logger.record(&ctx.username, &ctx.role.to_string(), "insert", Some(&req.database), Some(&req.collection), &format!("{{\"_id\":\"{}\"}}", id), true, None);
        } else {
            self.audit_logger.record(&ctx.username, &ctx.role.to_string(), "insert", Some(&req.database), Some(&req.collection), "{}", false, Some(&error));
        }
        record_op(&self.metrics, "insert", start, success);

        Ok(Response::new(proto::InsertResponse { success, id, error }))
    }

    async fn find_by_id(
        &self,
        request: Request<proto::FindByIdRequest>,
    ) -> Result<Response<proto::DocumentResponse>, Status> {
        let start = Instant::now();
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_read(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;
        let id = req.id;

        let result = tokio::task::spawn_blocking(move || mgr.find_by_id(&db, &col, &id))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        record_op(&self.metrics, "find_by_id", start, true);
        match result {
            Some(doc) => {
                let json = serde_json::to_string(&doc).map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(proto::DocumentResponse { found: true, document_json: json, error: String::new() }))
            }
            None => Ok(Response::new(proto::DocumentResponse { found: false, document_json: String::new(), error: String::new() })),
        }
    }

    async fn find_all(
        &self,
        request: Request<proto::FindAllRequest>,
    ) -> Result<Response<proto::DocumentsResponse>, Status> {
        let start = Instant::now();
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_read(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let opts = to_query_options(req.pagination, req.sort);
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        let (docs, total) = tokio::task::spawn_blocking(move || mgr.find_all_query(&db, &col, &opts))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        record_op(&self.metrics, "find_all", start, true);
        docs_response(docs, total)
    }

    async fn find_where(
        &self,
        request: Request<proto::FindWhereRequest>,
    ) -> Result<Response<proto::DocumentsResponse>, Status> {
        let start = Instant::now();
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_read(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let value: serde_json::Value = serde_json::from_str(&req.value_json)
            .unwrap_or(serde_json::Value::String(req.value_json.clone()));

        let opts = to_query_options(req.pagination, req.sort);
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;
        let field = req.field;

        let (docs, total) = tokio::task::spawn_blocking(move || {
            mgr.find_where_query(&db, &col, &field, &value, &opts)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(vanta_err)?;

        record_op(&self.metrics, "find_where", start, true);
        docs_response(docs, total)
    }

    async fn delete_by_id(
        &self,
        request: Request<proto::DeleteByIdRequest>,
    ) -> Result<Response<proto::DeleteResponse>, Status> {
        let start = Instant::now();
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_write(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;
        let id = req.id;

        let resp = raft_propose_or_direct(
            &self.raft,
            RaftOp::Delete { db: db.clone(), col: col.clone(), id: id.clone() },
            move || mgr.delete_by_id(&db, &col, &id).map(|_| RaftResponse::Ok),
        ).await;

        let success = resp.is_ok();
        record_op(&self.metrics, "delete_by_id", start, success);
        match resp {
            Ok(_) => Ok(Response::new(proto::DeleteResponse { success: true, found: true, error: String::new() })),
            Err(e) => Ok(Response::new(proto::DeleteResponse { success: false, found: false, error: e.message().to_string() })),
        }
    }

    async fn count(
        &self,
        request: Request<proto::CountRequest>,
    ) -> Result<Response<proto::CountResponse>, Status> {
        let start = Instant::now();
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_read(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        let count = tokio::task::spawn_blocking(move || mgr.count(&db, &col))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        record_op(&self.metrics, "count", start, true);
        Ok(Response::new(proto::CountResponse { count: count as u64, error: String::new() }))
    }

    async fn update(
        &self,
        request: Request<proto::UpdateRequest>,
    ) -> Result<Response<proto::UpdateResponse>, Status> {
        let start = Instant::now();
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_write(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let patch: serde_json::Value = serde_json::from_str(&req.patch_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid JSON: {}", e)))?;

        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;
        let id = req.id;
        let patch_clone = patch.clone();

        let resp = raft_propose_or_direct(
            &self.raft,
            RaftOp::Update { db: db.clone(), col: col.clone(), id: id.clone(), patch },
            move || mgr.update_by_id(&db, &col, &id, patch_clone).map(|found| {
                if found { RaftResponse::Ok } else { RaftResponse::Error { message: "Not found".to_string() } }
            }),
        ).await;

        let success = resp.is_ok();
        record_op(&self.metrics, "update", start, success);
        match resp {
            Ok(RaftResponse::Ok) => Ok(Response::new(proto::UpdateResponse { success: true, modified_count: 1, error: String::new() })),
            Ok(RaftResponse::Error { message }) => Ok(Response::new(proto::UpdateResponse { success: false, modified_count: 0, error: message })),
            Ok(_) => Ok(Response::new(proto::UpdateResponse { success: true, modified_count: 1, error: String::new() })),
            Err(e) => Ok(Response::new(proto::UpdateResponse { success: false, modified_count: 0, error: e.message().to_string() })),
        }
    }

    async fn update_where(
        &self,
        request: Request<proto::UpdateWhereRequest>,
    ) -> Result<Response<proto::UpdateResponse>, Status> {
        let start = Instant::now();
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_write(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let filter: serde_json::Value = serde_json::from_str(&req.filter_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid filter JSON: {}", e)))?;
        let patch: serde_json::Value = serde_json::from_str(&req.patch_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid patch JSON: {}", e)))?;

        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;
        let filter_clone = filter.clone();
        let patch_clone = patch.clone();

        let resp = raft_propose_or_direct(
            &self.raft,
            RaftOp::UpdateWhere { db: db.clone(), col: col.clone(), filter, patch },
            move || mgr.update_where(&db, &col, &filter_clone, &patch_clone).map(|_| RaftResponse::Ok),
        ).await;

        let success = resp.is_ok();
        record_op(&self.metrics, "update_where", start, success);
        match resp {
            Ok(_) => Ok(Response::new(proto::UpdateResponse { success: true, modified_count: 1, error: String::new() })),
            Err(e) => Ok(Response::new(proto::UpdateResponse { success: false, modified_count: 0, error: e.message().to_string() })),
        }
    }

    async fn query(
        &self,
        request: Request<proto::QueryRequest>,
    ) -> Result<Response<proto::DocumentsResponse>, Status> {
        let start = Instant::now();
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_read(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let filter: serde_json::Value = serde_json::from_str(&req.filter_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid filter JSON: {}", e)))?;

        let opts = to_query_options(req.pagination, req.sort);
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        let (docs, total) = tokio::task::spawn_blocking(move || mgr.query(&db, &col, &filter, &opts))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        record_op(&self.metrics, "query", start, true);
        docs_response(docs, total)
    }

    async fn aggregate(
        &self,
        request: Request<proto::AggregateRequest>,
    ) -> Result<Response<proto::AggregateResponse>, Status> {
        let start = Instant::now();
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_read(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let pipeline: Vec<serde_json::Value> = serde_json::from_str(&req.pipeline_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid pipeline JSON: {}", e)))?;

        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        let result = tokio::task::spawn_blocking(move || mgr.aggregate(&db, &col, &pipeline))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let success = result.is_ok();
        record_op(&self.metrics, "aggregate", start, success);
        match result {
            Ok(results) => {
                let results_json: Result<Vec<String>, _> = results.iter().map(serde_json::to_string).collect();
                let results_json = results_json.map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(proto::AggregateResponse { results_json, error: String::new() }))
            }
            Err(e) => Ok(Response::new(proto::AggregateResponse { results_json: vec![], error: e.to_string() })),
        }
    }

    async fn create_index(
        &self,
        request: Request<proto::IndexRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_write(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database; let col = req.collection; let field = req.field; let unique = req.unique;

        tokio::task::spawn_blocking(move || mgr.create_index(&db, &col, &field, unique))
            .await.map_err(|e| Status::internal(e.to_string()))?.map_err(vanta_err)?;
        ok_status()
    }

    async fn drop_index(
        &self,
        request: Request<proto::IndexRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_admin(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database; let col = req.collection; let field = req.field;

        tokio::task::spawn_blocking(move || mgr.drop_index(&db, &col, &field))
            .await.map_err(|e| Status::internal(e.to_string()))?.map_err(vanta_err)?;
        ok_status()
    }

    async fn list_indexes(
        &self,
        request: Request<proto::ListIndexesRequest>,
    ) -> Result<Response<proto::ListIndexesResponse>, Status> {
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_read(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database; let col = req.collection;

        let indexes = tokio::task::spawn_blocking(move || mgr.list_indexes(&db, &col))
            .await.map_err(|e| Status::internal(e.to_string()))?.map_err(vanta_err)?;

        let infos: Vec<proto::IndexInfo> = indexes.iter()
            .map(|idx| proto::IndexInfo { field: idx.field.clone(), unique: idx.unique }).collect();
        Ok(Response::new(proto::ListIndexesResponse { indexes: infos }))
    }

    async fn set_schema(
        &self,
        request: Request<proto::SetSchemaRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_admin(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let schema_val: serde_json::Value = serde_json::from_str(&req.schema_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid schema JSON: {}", e)))?;
        let schema = CollectionSchema::from_json(&schema_val).map_err(|e| Status::invalid_argument(e))?;
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database; let col = req.collection;

        tokio::task::spawn_blocking(move || mgr.set_schema(&db, &col, &schema))
            .await.map_err(|e| Status::internal(e.to_string()))?.map_err(vanta_err)?;
        ok_status()
    }

    async fn get_schema(
        &self,
        request: Request<proto::GetSchemaRequest>,
    ) -> Result<Response<proto::GetSchemaResponse>, Status> {
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_read(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database; let col = req.collection;

        let result = tokio::task::spawn_blocking(move || mgr.get_schema(&db, &col))
            .await.map_err(|e| Status::internal(e.to_string()))?.map_err(vanta_err)?;

        match result {
            Some(schema) => {
                let json = serde_json::to_string(&schema.to_json()).map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(proto::GetSchemaResponse { found: true, schema_json: json }))
            }
            None => Ok(Response::new(proto::GetSchemaResponse { found: false, schema_json: String::new() })),
        }
    }

    async fn drop_schema(
        &self,
        request: Request<proto::GetSchemaRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_admin(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let mgr = Arc::clone(&self.db_manager);
        let db = req.database; let col = req.collection;

        tokio::task::spawn_blocking(move || mgr.drop_schema(&db, &col))
            .await.map_err(|e| Status::internal(e.to_string()))?.map_err(vanta_err)?;
        ok_status()
    }

    async fn begin_tx(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::TxResponse>, Status> {
        let start = Instant::now();
        let _ctx = extract_auth(&request)?;
        let mgr = Arc::clone(&self.db_manager);
        let tx_id = tokio::task::spawn_blocking(move || mgr.begin_transaction())
            .await.map_err(|e| Status::internal(e.to_string()))?;
        self.metrics.gauge_inc("active_transactions");
        record_op(&self.metrics, "begin_tx", start, true);
        Ok(Response::new(proto::TxResponse { tx_id }))
    }

    async fn commit_tx(
        &self,
        request: Request<proto::TxRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let start = Instant::now();
        let ctx = extract_auth(&request)?;
        if !ctx.role.can_write() {
            return Err(Status::permission_denied("Write permission required"));
        }
        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let tx_id = req.tx_id;

        let result = tokio::task::spawn_blocking(move || mgr.commit_transaction(&tx_id))
            .await.map_err(|e| Status::internal(e.to_string()))?;
        self.metrics.gauge_dec("active_transactions");
        let success = result.is_ok();
        record_op(&self.metrics, "commit_tx", start, success);
        if success { self.metrics.inc("tx_commits"); }
        result.map_err(vanta_err)?;
        ok_status()
    }

    async fn rollback_tx(
        &self,
        request: Request<proto::TxRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let start = Instant::now();
        let _ctx = extract_auth(&request)?;
        let req = request.into_inner();
        let mgr = Arc::clone(&self.db_manager);
        let tx_id = req.tx_id;

        tokio::task::spawn_blocking(move || mgr.rollback_transaction(&tx_id))
            .await.map_err(|e| Status::internal(e.to_string()))?.map_err(vanta_err)?;
        self.metrics.gauge_dec("active_transactions");
        self.metrics.inc("tx_rollbacks");
        record_op(&self.metrics, "rollback_tx", start, true);
        ok_status()
    }

    async fn tx_insert(
        &self,
        request: Request<proto::TxInsertRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_write(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let doc: serde_json::Value = serde_json::from_str(&req.document_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid JSON: {}", e)))?;
        let mgr = Arc::clone(&self.db_manager);

        tokio::task::spawn_blocking(move || mgr.tx_insert(&req.tx_id, req.database, req.collection, doc))
            .await.map_err(|e| Status::internal(e.to_string()))?.map_err(vanta_err)?;
        ok_status()
    }

    async fn tx_update(
        &self,
        request: Request<proto::TxUpdateRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_write(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let patch: serde_json::Value = serde_json::from_str(&req.patch_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid JSON: {}", e)))?;
        let mgr = Arc::clone(&self.db_manager);

        tokio::task::spawn_blocking(move || mgr.tx_update(&req.tx_id, req.database, req.collection, req.id, patch))
            .await.map_err(|e| Status::internal(e.to_string()))?.map_err(vanta_err)?;
        ok_status()
    }

    async fn tx_delete(
        &self,
        request: Request<proto::TxDeleteRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_write(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let mgr = Arc::clone(&self.db_manager);

        tokio::task::spawn_blocking(move || mgr.tx_delete(&req.tx_id, req.database, req.collection, req.id))
            .await.map_err(|e| Status::internal(e.to_string()))?.map_err(vanta_err)?;
        ok_status()
    }

    async fn tx_find_by_id(
        &self,
        request: Request<proto::TxFindByIdRequest>,
    ) -> Result<Response<proto::DocumentResponse>, Status> {
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_read(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;
        let mgr = Arc::clone(&self.db_manager);

        let result = tokio::task::spawn_blocking(move || mgr.tx_find_by_id(&req.tx_id, &req.database, &req.collection, &req.id))
            .await.map_err(|e| Status::internal(e.to_string()))?.map_err(vanta_err)?;

        match result {
            Some(doc) => {
                let json = serde_json::to_string(&doc).map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(proto::DocumentResponse { found: true, document_json: json, error: String::new() }))
            }
            None => Ok(Response::new(proto::DocumentResponse { found: false, document_json: String::new(), error: String::new() })),
        }
    }

    // ---- Explain plan ------------------------------------------

    async fn explain(
        &self,
        request: Request<proto::QueryRequest>,
    ) -> Result<Response<proto::ExplainResponse>, Status> {
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_read(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;

        let filter: serde_json::Value = serde_json::from_str(&req.filter_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid filter JSON: {}", e)))?;

        let mgr = Arc::clone(&self.db_manager);
        let db = req.database;
        let col = req.collection;

        let explanation = tokio::task::spawn_blocking(move || mgr.explain(&db, &col, &filter))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(vanta_err)?;

        Ok(Response::new(proto::ExplainResponse {
            plan_type: explanation.plan_type,
            index_field: explanation.index_field.unwrap_or_default(),
            index_type: explanation.index_type.unwrap_or_default(),
            predicate: explanation.predicate.unwrap_or_default(),
            has_residual: explanation.has_residual,
            selectivity: explanation.selectivity,
            total_docs: explanation.total_docs as u64,
            estimated_scan: explanation.estimated_scan as u64,
        }))
    }

    // ---- Change feeds -------------------------------------------

    async fn watch_collection(
        &self,
        request: Request<proto::WatchRequest>,
    ) -> Result<Response<Self::WatchCollectionStream>, Status> {
        let ctx = extract_auth(&request)?;
        let req = request.into_inner();
        require_read(&self.acl_manager, &ctx, &req.database, Some(&req.collection))?;

        let database = req.database;
        let collection = req.collection;
        let since = req.since_sequence;

        let feed = Arc::clone(&self.db_manager.change_feed);
        let (tx, rx) = tokio::sync::mpsc::channel(256);

        let replay_events = feed.replay(&database, &collection, since);
        let mut broadcast_rx = feed.subscribe();

        let db_filter = database.clone();
        let col_filter = collection.clone();

        tokio::spawn(async move {
            for event in replay_events {
                let watch_event = proto::WatchEvent {
                    sequence: event.sequence,
                    operation: event.operation.to_string(),
                    doc_id: event.doc_id,
                    document_json: event.document
                        .map(|d| serde_json::to_string(&d).unwrap_or_default())
                        .unwrap_or_default(),
                    timestamp: event.timestamp.to_rfc3339(),
                };
                if tx.send(Ok(watch_event)).await.is_err() { return; }
            }

            loop {
                match broadcast_rx.recv().await {
                    Ok(event) => {
                        if event.database != db_filter || event.collection != col_filter { continue; }
                        let watch_event = proto::WatchEvent {
                            sequence: event.sequence,
                            operation: event.operation.to_string(),
                            doc_id: event.doc_id,
                            document_json: event.document
                                .map(|d| serde_json::to_string(&d).unwrap_or_default())
                                .unwrap_or_default(),
                            timestamp: event.timestamp.to_rfc3339(),
                        };
                        if tx.send(Ok(watch_event)).await.is_err() { return; }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream)))
    }
}
