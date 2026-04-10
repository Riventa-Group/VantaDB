use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tonic::{Request, Status};

use crate::proto;
use proto::vanta_auth_client::VantaAuthClient;
use proto::vanta_db_client::VantaDbClient;

/// Ergonomic VantaDB client with auto token management.
///
/// # Example
/// ```no_run
/// use vantadb_sdk::VantaClient;
///
/// #[tokio::main]
/// async fn main() {
///     let mut client = VantaClient::connect("http://localhost:5432").await.unwrap();
///     client.authenticate("root", "password").await.unwrap();
///     let id = client.insert("mydb", "users", serde_json::json!({"name": "Alice"})).await.unwrap();
///     println!("Inserted: {}", id);
/// }
/// ```
pub struct VantaClient {
    auth: VantaAuthClient<Channel>,
    db: VantaDbClient<Channel>,
    token: Option<String>,
}

impl VantaClient {
    /// Connect to a VantaDB server without TLS.
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let channel = Channel::from_shared(addr.to_string())?
            .connect()
            .await?;
        Ok(Self {
            auth: VantaAuthClient::new(channel.clone()),
            db: VantaDbClient::new(channel),
            token: None,
        })
    }

    /// Connect with mTLS (client certificate + CA verification).
    pub async fn connect_tls(
        addr: &str,
        ca_pem: &[u8],
        cert_pem: &[u8],
        key_pem: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let tls = ClientTlsConfig::new()
            .domain_name("localhost")
            .ca_certificate(Certificate::from_pem(ca_pem))
            .identity(Identity::from_pem(cert_pem, key_pem));

        let channel = Channel::from_shared(addr.to_string())?
            .tls_config(tls)?
            .connect()
            .await?;

        Ok(Self {
            auth: VantaAuthClient::new(channel.clone()),
            db: VantaDbClient::new(channel),
            token: None,
        })
    }

    /// Authenticate and store the JWT token for subsequent requests.
    pub async fn authenticate(&mut self, username: &str, password: &str) -> Result<String, Status> {
        let resp = self
            .auth
            .authenticate(proto::AuthRequest {
                username: username.to_string(),
                password: password.to_string(),
            })
            .await?
            .into_inner();

        if !resp.success {
            return Err(Status::unauthenticated(resp.error));
        }

        self.token = Some(resp.token.clone());
        Ok(resp.role)
    }

    // ---- Database operations --------------------------------

    pub async fn create_database(&mut self, name: &str) -> Result<(), Status> {
        self.db.create_database(self.db_req(proto::DatabaseRequest { database: name.to_string() })).await?.into_inner();
        Ok(())
    }

    pub async fn drop_database(&mut self, name: &str) -> Result<(), Status> {
        self.db.drop_database(self.db_req(proto::DatabaseRequest { database: name.to_string() })).await?.into_inner();
        Ok(())
    }

    pub async fn list_databases(&mut self) -> Result<Vec<String>, Status> {
        Ok(self.db.list_databases(self.db_req(proto::Empty {})).await?.into_inner().databases)
    }

    // ---- Collection operations ------------------------------

    pub async fn create_collection(&mut self, db: &str, col: &str) -> Result<(), Status> {
        self.db.create_collection(self.db_req(proto::CollectionRequest { database: db.to_string(), collection: col.to_string() })).await?.into_inner();
        Ok(())
    }

    pub async fn drop_collection(&mut self, db: &str, col: &str) -> Result<(), Status> {
        self.db.drop_collection(self.db_req(proto::CollectionRequest { database: db.to_string(), collection: col.to_string() })).await?.into_inner();
        Ok(())
    }

    pub async fn list_collections(&mut self, db: &str) -> Result<Vec<String>, Status> {
        Ok(self.db.list_collections(self.db_req(proto::DatabaseRequest { database: db.to_string() })).await?.into_inner().collections)
    }

    // ---- Document operations --------------------------------

    /// Insert a document. Returns the assigned document ID.
    pub async fn insert(
        &mut self,
        db: &str,
        collection: &str,
        document: serde_json::Value,
    ) -> Result<String, Status> {
        let req = self.db_req(proto::InsertRequest {
            database: db.to_string(),
            collection: collection.to_string(),
            document_json: serde_json::to_string(&document)
                .map_err(|e| Status::internal(e.to_string()))?,
        });
        let inner = self.db.insert(req).await?.into_inner();
        if inner.success {
            Ok(inner.id)
        } else {
            Err(Status::internal(inner.error))
        }
    }

    /// Find a document by ID.
    pub async fn find_by_id(
        &mut self,
        db: &str,
        collection: &str,
        id: &str,
    ) -> Result<Option<serde_json::Value>, Status> {
        let req = self.db_req(proto::FindByIdRequest {
            database: db.to_string(),
            collection: collection.to_string(),
            id: id.to_string(),
        });
        let inner = self.db.find_by_id(req).await?.into_inner();
        if inner.found {
            let doc = serde_json::from_str(&inner.document_json)
                .map_err(|e| Status::internal(e.to_string()))?;
            Ok(Some(doc))
        } else {
            Ok(None)
        }
    }

    /// Find all documents in a collection.
    pub async fn find_all(
        &mut self,
        db: &str,
        collection: &str,
    ) -> Result<Vec<serde_json::Value>, Status> {
        let req = self.db_req(proto::FindAllRequest {
            database: db.to_string(),
            collection: collection.to_string(),
            pagination: None,
            sort: None,
        });
        let inner = self.db.find_all(req).await?.into_inner();
        parse_doc_list(&inner.documents_json)
    }

    /// Query documents with a MongoDB-style filter.
    pub async fn query(
        &mut self,
        db: &str,
        collection: &str,
        filter: serde_json::Value,
    ) -> Result<Vec<serde_json::Value>, Status> {
        let req = self.db_req(proto::QueryRequest {
            database: db.to_string(),
            collection: collection.to_string(),
            filter_json: serde_json::to_string(&filter)
                .map_err(|e| Status::internal(e.to_string()))?,
            pagination: None,
            sort: None,
        });
        let inner = self.db.query(req).await?.into_inner();
        parse_doc_list(&inner.documents_json)
    }

    /// Delete a document by ID. Returns true if found and deleted.
    pub async fn delete(
        &mut self,
        db: &str,
        collection: &str,
        id: &str,
    ) -> Result<bool, Status> {
        let req = self.db_req(proto::DeleteByIdRequest {
            database: db.to_string(),
            collection: collection.to_string(),
            id: id.to_string(),
        });
        Ok(self.db.delete_by_id(req).await?.into_inner().found)
    }

    /// Update a document by ID. Returns true if found and updated.
    pub async fn update(
        &mut self,
        db: &str,
        collection: &str,
        id: &str,
        patch: serde_json::Value,
    ) -> Result<bool, Status> {
        let req = self.db_req(proto::UpdateRequest {
            database: db.to_string(),
            collection: collection.to_string(),
            id: id.to_string(),
            patch_json: serde_json::to_string(&patch)
                .map_err(|e| Status::internal(e.to_string()))?,
        });
        let inner = self.db.update(req).await?.into_inner();
        Ok(inner.modified_count > 0)
    }

    /// Count documents in a collection.
    pub async fn count(&mut self, db: &str, collection: &str) -> Result<u64, Status> {
        let req = self.db_req(proto::CountRequest {
            database: db.to_string(),
            collection: collection.to_string(),
        });
        Ok(self.db.count(req).await?.into_inner().count)
    }

    // ---- Watch (change feed) --------------------------------

    /// Subscribe to real-time changes on a collection.
    pub async fn watch(
        &mut self,
        db: &str,
        collection: &str,
    ) -> Result<tonic::Streaming<proto::WatchEvent>, Status> {
        let req = self.db_req(proto::WatchRequest {
            database: db.to_string(),
            collection: collection.to_string(),
            since_sequence: 0,
        });
        Ok(self.db.watch_collection(req).await?.into_inner())
    }

    // ---- Health check (no auth) -----------------------------

    pub async fn health(&mut self) -> Result<proto::HealthResponse, Status> {
        let resp = self
            .auth
            .health_check(proto::Empty {})
            .await?
            .into_inner();
        Ok(resp)
    }

    // ---- Internal helpers -----------------------------------

    fn db_req<T>(&self, inner: T) -> Request<T> {
        let mut req = Request::new(inner);
        if let Some(ref token) = self.token {
            let val: MetadataValue<_> = format!("Bearer {}", token).parse().unwrap();
            req.metadata_mut().insert("authorization", val);
        }
        req
    }
}

fn parse_doc_list(json_strings: &[String]) -> Result<Vec<serde_json::Value>, Status> {
    json_strings
        .iter()
        .map(|s| serde_json::from_str(s).map_err(|e| Status::internal(e.to_string())))
        .collect()
}
