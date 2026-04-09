use colored::*;
use std::time::Instant;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tonic::Request;

use crate::server::proto;
use proto::vanta_auth_client::VantaAuthClient;
use proto::vanta_db_client::VantaDbClient;

const TEST_DB: &str = "selfcheck_test_db";
const TEST_COL: &str = "test_items";

struct CheckRunner {
    auth: VantaAuthClient<Channel>,
    db: VantaDbClient<Channel>,
    token: String,
    passed: u32,
    failed: u32,
    total_time: std::time::Duration,
}

impl CheckRunner {
    async fn check<F, Fut>(&mut self, name: &str, f: F)
    where
        F: FnOnce(VantaAuthClient<Channel>, VantaDbClient<Channel>, String) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let start = Instant::now();
        let result = f(
            self.auth.clone(),
            self.db.clone(),
            self.token.clone(),
        )
        .await;
        let elapsed = start.elapsed();
        self.total_time += elapsed;

        match result {
            Ok(()) => {
                self.passed += 1;
                println!(
                    "  {} {:<50} {}",
                    "PASS".green().bold(),
                    name,
                    format!("{:.2}ms", elapsed.as_secs_f64() * 1000.0).dimmed()
                );
            }
            Err(e) => {
                self.failed += 1;
                println!(
                    "  {} {:<50} {}",
                    "FAIL".red().bold(),
                    name,
                    e.red()
                );
            }
        }
    }
}

fn auth_req<T>(token: &str, inner: T) -> Request<T> {
    let mut req = Request::new(inner);
    let val: MetadataValue<_> = format!("Bearer {}", token).parse().unwrap();
    req.metadata_mut().insert("authorization", val);
    req
}

pub async fn run(
    port: u16,
    username: &str,
    password: &str,
    tls_config: Option<&(String, String, String)>,
) -> bool {
    let (_scheme, addr) = if tls_config.is_some() {
        ("https", format!("https://localhost:{}", port))
    } else {
        ("http", format!("http://127.0.0.1:{}", port))
    };

    println!(
        "  {} Self-check targeting {} {}",
        "→".truecolor(120, 80, 255),
        addr.bold().truecolor(120, 200, 120),
        if tls_config.is_some() { "(mTLS)" } else { "(plain)" },
    );
    println!();

    // ── Connect ──────────────────────────────────

    let mut endpoint = Channel::from_shared(addr.clone()).unwrap();

    if let Some((cert_path, key_path, ca_path)) = tls_config {
        let ca_pem = std::fs::read(ca_path).expect("Failed to read CA cert");
        let cert_pem = std::fs::read(cert_path).expect("Failed to read client cert");
        let key_pem = std::fs::read(key_path).expect("Failed to read client key");

        let tls = ClientTlsConfig::new()
            .domain_name("localhost")
            .ca_certificate(Certificate::from_pem(ca_pem))
            .identity(Identity::from_pem(cert_pem, key_pem));

        endpoint = endpoint.tls_config(tls).expect("Failed to configure TLS");
    }

    let channel = match endpoint.connect().await {
        Ok(ch) => ch,
        Err(e) => {
            println!(
                "  {} Could not connect to {}: {}",
                "✗".red().bold(),
                addr,
                e.to_string().red()
            );
            println!(
                "  {} Make sure the server is running: {}",
                "ℹ".truecolor(120, 80, 255),
                "vantadb --serve".dimmed()
            );
            return false;
        }
    };

    let auth_client = VantaAuthClient::new(channel.clone());
    let db_client = VantaDbClient::new(channel);

    println!(
        "  {} Connected to gRPC server",
        "✓".green().bold()
    );
    println!();

    // ── Authenticate ─────────────────────────────

    println!(
        "  {}",
        "SELF-CHECK".bold().truecolor(120, 80, 255)
    );
    println!(
        "  {}",
        "─────────────────────────────────────────────────────────────────".truecolor(60, 60, 80)
    );
    println!();

    let token = {
        let mut ac = auth_client.clone();
        let resp = match ac
            .authenticate(proto::AuthRequest {
                username: username.to_string(),
                password: password.to_string(),
            })
            .await
        {
            Ok(r) => r.into_inner(),
            Err(e) => {
                println!(
                    "  {} Authentication RPC failed: {}",
                    "✗".red().bold(),
                    e.to_string().red()
                );
                return false;
            }
        };

        if !resp.success {
            println!(
                "  {} Authentication failed: {}",
                "✗".red().bold(),
                resp.error.red()
            );
            return false;
        }

        println!(
            "  {} Authenticated as {} ({})",
            "✓".green().bold(),
            username.bold().cyan(),
            resp.role.dimmed()
        );
        println!();

        resp.token
    };

    let mut runner = CheckRunner {
        auth: auth_client,
        db: db_client,
        token: token.clone(),
        passed: 0,
        failed: 0,
        total_time: std::time::Duration::ZERO,
    };

    // ═══════════════════════════════════════════════
    //  SECTION: Auth Service
    // ═══════════════════════════════════════════════

    section("Auth Service");

    runner.check("ListUsers", |mut auth, _, tok| async move {
        let resp = auth
            .list_users(auth_req(&tok, proto::Empty {}))
            .await
            .map_err(|e| e.to_string())?;
        let users = resp.into_inner().usernames;
        if users.is_empty() {
            return Err("Expected at least one user".into());
        }
        Ok(())
    }).await;

    runner.check("GetUser (self)", |mut auth, _, tok| async move {
        let resp = auth
            .get_user(auth_req(
                &tok,
                proto::GetUserRequest {
                    username: "root".into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        if !resp.into_inner().found {
            return Err("Root user not found".into());
        }
        Ok(())
    }).await;

    // ═══════════════════════════════════════════════
    //  SECTION: Database Operations
    // ═══════════════════════════════════════════════

    section("Database Operations");

    runner.check("CreateDatabase", |_, mut db, tok| async move {
        let resp = db
            .create_database(auth_req(&tok, proto::DatabaseRequest { database: TEST_DB.into() }))
            .await
            .map_err(|e| e.to_string())?;
        if !resp.into_inner().success {
            return Err("CreateDatabase failed".into());
        }
        Ok(())
    }).await;

    runner.check("ListDatabases", |_, mut db, tok| async move {
        let resp = db
            .list_databases(auth_req(&tok, proto::Empty {}))
            .await
            .map_err(|e| e.to_string())?;
        let dbs = resp.into_inner().databases;
        if !dbs.iter().any(|d| d == TEST_DB) {
            return Err(format!("'{}' not in database list", TEST_DB));
        }
        Ok(())
    }).await;

    // ═══════════════════════════════════════════════
    //  SECTION: Collection Operations
    // ═══════════════════════════════════════════════

    section("Collection Operations");

    runner.check("CreateCollection", |_, mut db, tok| async move {
        let resp = db
            .create_collection(auth_req(
                &tok,
                proto::CollectionRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        if !resp.into_inner().success {
            return Err("CreateCollection failed".into());
        }
        Ok(())
    }).await;

    runner.check("ListCollections", |_, mut db, tok| async move {
        let resp = db
            .list_collections(auth_req(&tok, proto::DatabaseRequest { database: TEST_DB.into() }))
            .await
            .map_err(|e| e.to_string())?;
        let cols = resp.into_inner().collections;
        if !cols.iter().any(|c| c == TEST_COL) {
            return Err(format!("'{}' not in collection list", TEST_COL));
        }
        Ok(())
    }).await;

    // ═══════════════════════════════════════════════
    //  SECTION: Document CRUD
    // ═══════════════════════════════════════════════

    section("Document CRUD");

    // Insert 3 docs, capture the first ID
    let mut inserted_id = String::new();

    {
        let mut db = runner.db.clone();
        let start = Instant::now();
        let resp = db
            .insert(auth_req(
                &token,
                proto::InsertRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    document_json: r#"{"name":"Alice","age":30,"city":"NYC"}"#.into(),
                },
            ))
            .await;
        let elapsed = start.elapsed();
        runner.total_time += elapsed;
        match resp {
            Ok(r) => {
                let inner = r.into_inner();
                if inner.success {
                    inserted_id = inner.id.clone();
                    runner.passed += 1;
                    println!(
                        "  {} {:<50} {}",
                        "PASS".green().bold(),
                        "Insert (doc 1)",
                        format!("{:.2}ms", elapsed.as_secs_f64() * 1000.0).dimmed()
                    );
                } else {
                    runner.failed += 1;
                    println!("  {} {:<50} {}", "FAIL".red().bold(), "Insert (doc 1)", inner.error.red());
                }
            }
            Err(e) => {
                runner.failed += 1;
                println!("  {} {:<50} {}", "FAIL".red().bold(), "Insert (doc 1)", e.to_string().red());
            }
        }
    }

    // Insert doc 2 & 3
    for (i, json) in [
        r#"{"name":"Bob","age":25,"city":"LA"}"#,
        r#"{"name":"Charlie","age":35,"city":"NYC"}"#,
    ]
    .iter()
    .enumerate()
    {
        let label = format!("Insert (doc {})", i + 2);
        let json = json.to_string();
        runner.check(&label, |_, mut db, tok| async move {
            let resp = db
                .insert(auth_req(
                    &tok,
                    proto::InsertRequest {
                        database: TEST_DB.into(),
                        collection: TEST_COL.into(),
                        document_json: json,
                    },
                ))
                .await
                .map_err(|e| e.to_string())?;
            if !resp.into_inner().success {
                return Err("Insert failed".into());
            }
            Ok(())
        }).await;
    }

    runner.check("Count", |_, mut db, tok| async move {
        let resp = db
            .count(auth_req(
                &tok,
                proto::CountRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let count = resp.into_inner().count;
        if count != 3 {
            return Err(format!("Expected 3 docs, got {}", count));
        }
        Ok(())
    }).await;

    let find_id = inserted_id.clone();
    runner.check("FindById", move |_, mut db, tok| async move {
        let resp = db
            .find_by_id(auth_req(
                &tok,
                proto::FindByIdRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    id: find_id,
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let inner = resp.into_inner();
        if !inner.found {
            return Err("Document not found".into());
        }
        if !inner.document_json.contains("Alice") {
            return Err("Document content mismatch".into());
        }
        Ok(())
    }).await;

    runner.check("FindAll", |_, mut db, tok| async move {
        let resp = db
            .find_all(auth_req(
                &tok,
                proto::FindAllRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    pagination: None,
                    sort: None,
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let docs = resp.into_inner().documents_json;
        if docs.len() != 3 {
            return Err(format!("Expected 3 docs, got {}", docs.len()));
        }
        Ok(())
    }).await;

    runner.check("FindAll (with sort)", |_, mut db, tok| async move {
        let resp = db
            .find_all(auth_req(
                &tok,
                proto::FindAllRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    pagination: None,
                    sort: Some(proto::SortOptions {
                        field: "age".into(),
                        descending: true,
                    }),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let docs = resp.into_inner().documents_json;
        if docs.is_empty() {
            return Err("No docs returned".into());
        }
        // First doc should be Charlie (age 35) since descending
        if !docs[0].contains("Charlie") {
            return Err("Sort order wrong: expected Charlie first".into());
        }
        Ok(())
    }).await;

    runner.check("FindAll (with pagination)", |_, mut db, tok| async move {
        let resp = db
            .find_all(auth_req(
                &tok,
                proto::FindAllRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    pagination: Some(proto::PaginationOptions {
                        page: 1,
                        page_size: 2,
                    }),
                    sort: None,
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let inner = resp.into_inner();
        if inner.documents_json.len() != 2 {
            return Err(format!("Expected 2 docs on page 1, got {}", inner.documents_json.len()));
        }
        if inner.total_count != 3 {
            return Err(format!("Expected total_count=3, got {}", inner.total_count));
        }
        Ok(())
    }).await;

    runner.check("FindWhere", |_, mut db, tok| async move {
        let resp = db
            .find_where(auth_req(
                &tok,
                proto::FindWhereRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    field: "city".into(),
                    value_json: r#""NYC""#.into(),
                    pagination: None,
                    sort: None,
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let docs = resp.into_inner().documents_json;
        if docs.len() != 2 {
            return Err(format!("Expected 2 NYC docs, got {}", docs.len()));
        }
        Ok(())
    }).await;

    // ═══════════════════════════════════════════════
    //  SECTION: Update Operations
    // ═══════════════════════════════════════════════

    section("Update Operations");

    let update_id = inserted_id.clone();
    runner.check("Update (by ID)", move |_, mut db, tok| async move {
        let resp = db
            .update(auth_req(
                &tok,
                proto::UpdateRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    id: update_id,
                    patch_json: r#"{"age":31}"#.into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let inner = resp.into_inner();
        if !inner.success || inner.modified_count != 1 {
            return Err("Update by ID failed".into());
        }
        Ok(())
    }).await;

    // Verify update
    let verify_id = inserted_id.clone();
    runner.check("Verify Update", move |_, mut db, tok| async move {
        let resp = db
            .find_by_id(auth_req(
                &tok,
                proto::FindByIdRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    id: verify_id,
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let doc = resp.into_inner().document_json;
        if !doc.contains("31") {
            return Err("Age was not updated to 31".into());
        }
        Ok(())
    }).await;

    runner.check("UpdateWhere", |_, mut db, tok| async move {
        let resp = db
            .update_where(auth_req(
                &tok,
                proto::UpdateWhereRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    filter_json: r#"{"city":{"$eq":"LA"}}"#.into(),
                    patch_json: r#"{"verified":true}"#.into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let inner = resp.into_inner();
        if !inner.success || inner.modified_count != 1 {
            return Err(format!("Expected 1 modified, got {}", inner.modified_count));
        }
        Ok(())
    }).await;

    // ═══════════════════════════════════════════════
    //  SECTION: Rich Query (Filter Engine)
    // ═══════════════════════════════════════════════

    section("Rich Query (Filter Engine)");

    runner.check("Query ($gt)", |_, mut db, tok| async move {
        let resp = db
            .query(auth_req(
                &tok,
                proto::QueryRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    filter_json: r#"{"age":{"$gt":28}}"#.into(),
                    pagination: None,
                    sort: None,
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let docs = resp.into_inner().documents_json;
        if docs.len() != 2 {
            return Err(format!("Expected 2 docs with age>28, got {}", docs.len()));
        }
        Ok(())
    }).await;

    runner.check("Query ($eq)", |_, mut db, tok| async move {
        let resp = db
            .query(auth_req(
                &tok,
                proto::QueryRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    filter_json: r#"{"name":{"$eq":"Bob"}}"#.into(),
                    pagination: None,
                    sort: None,
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let docs = resp.into_inner().documents_json;
        if docs.len() != 1 {
            return Err(format!("Expected 1 doc, got {}", docs.len()));
        }
        Ok(())
    }).await;

    runner.check("Query ($in)", |_, mut db, tok| async move {
        let resp = db
            .query(auth_req(
                &tok,
                proto::QueryRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    filter_json: r#"{"name":{"$in":["Alice","Bob"]}}"#.into(),
                    pagination: None,
                    sort: None,
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let docs = resp.into_inner().documents_json;
        if docs.len() != 2 {
            return Err(format!("Expected 2 docs for $in, got {}", docs.len()));
        }
        Ok(())
    }).await;

    runner.check("Query ($and)", |_, mut db, tok| async move {
        let resp = db
            .query(auth_req(
                &tok,
                proto::QueryRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    filter_json: r#"{"$and":[{"city":{"$eq":"NYC"}},{"age":{"$gt":32}}]}"#.into(),
                    pagination: None,
                    sort: None,
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let docs = resp.into_inner().documents_json;
        if docs.len() != 1 || !docs[0].contains("Charlie") {
            return Err("$and filter failed".into());
        }
        Ok(())
    }).await;

    runner.check("Query (with sort + pagination)", |_, mut db, tok| async move {
        let resp = db
            .query(auth_req(
                &tok,
                proto::QueryRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    filter_json: r#"{"age":{"$gte":25}}"#.into(),
                    pagination: Some(proto::PaginationOptions { page: 1, page_size: 2 }),
                    sort: Some(proto::SortOptions { field: "age".into(), descending: false }),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let inner = resp.into_inner();
        if inner.documents_json.len() != 2 {
            return Err(format!("Expected 2 docs, got {}", inner.documents_json.len()));
        }
        Ok(())
    }).await;

    // ═══════════════════════════════════════════════
    //  SECTION: Aggregation Pipeline
    // ═══════════════════════════════════════════════

    section("Aggregation Pipeline");

    runner.check("Aggregate ($group + $sum)", |_, mut db, tok| async move {
        let resp = db
            .aggregate(auth_req(
                &tok,
                proto::AggregateRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    pipeline_json: r#"[{"$group":{"_id":"$city","count":{"$sum":1}}}]"#.into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let results = resp.into_inner().results_json;
        if results.len() != 2 {
            return Err(format!("Expected 2 groups (NYC,LA), got {}", results.len()));
        }
        Ok(())
    }).await;

    runner.check("Aggregate ($group + $avg)", |_, mut db, tok| async move {
        let resp = db
            .aggregate(auth_req(
                &tok,
                proto::AggregateRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    pipeline_json: r#"[{"$group":{"_id":"$city","avg_age":{"$avg":"$age"}}}]"#.into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let results = resp.into_inner().results_json;
        if results.is_empty() {
            return Err("No aggregation results".into());
        }
        Ok(())
    }).await;

    runner.check("Aggregate ($match + $group)", |_, mut db, tok| async move {
        let resp = db
            .aggregate(auth_req(
                &tok,
                proto::AggregateRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    pipeline_json: r#"[{"$match":{"city":{"$eq":"NYC"}}},{"$group":{"_id":"$city","total":{"$sum":1}}}]"#.into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let results = resp.into_inner().results_json;
        if results.len() != 1 {
            return Err(format!("Expected 1 group after $match, got {}", results.len()));
        }
        Ok(())
    }).await;

    // ═══════════════════════════════════════════════
    //  SECTION: Index Management
    // ═══════════════════════════════════════════════

    section("Index Management");

    runner.check("CreateIndex", |_, mut db, tok| async move {
        let resp = db
            .create_index(auth_req(
                &tok,
                proto::IndexRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    field: "name".into(),
                    unique: false,
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        if !resp.into_inner().success {
            return Err("CreateIndex failed".into());
        }
        Ok(())
    }).await;

    runner.check("CreateIndex (unique)", |_, mut db, tok| async move {
        let resp = db
            .create_index(auth_req(
                &tok,
                proto::IndexRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    field: "age".into(),
                    unique: true,
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        if !resp.into_inner().success {
            return Err("CreateIndex (unique) failed".into());
        }
        Ok(())
    }).await;

    runner.check("ListIndexes", |_, mut db, tok| async move {
        let resp = db
            .list_indexes(auth_req(
                &tok,
                proto::ListIndexesRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let indexes = resp.into_inner().indexes;
        if indexes.len() != 2 {
            return Err(format!("Expected 2 indexes, got {}", indexes.len()));
        }
        let has_unique = indexes.iter().any(|i| i.field == "age" && i.unique);
        if !has_unique {
            return Err("Unique index on 'age' not found".into());
        }
        Ok(())
    }).await;

    runner.check("DropIndex", |_, mut db, tok| async move {
        let resp = db
            .drop_index(auth_req(
                &tok,
                proto::IndexRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    field: "age".into(),
                    unique: false,
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        if !resp.into_inner().success {
            return Err("DropIndex failed".into());
        }
        Ok(())
    }).await;

    runner.check("ListIndexes (after drop)", |_, mut db, tok| async move {
        let resp = db
            .list_indexes(auth_req(
                &tok,
                proto::ListIndexesRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let indexes = resp.into_inner().indexes;
        if indexes.len() != 1 {
            return Err(format!("Expected 1 index after drop, got {}", indexes.len()));
        }
        Ok(())
    }).await;

    // ═══════════════════════════════════════════════
    //  SECTION: Schema Validation
    // ═══════════════════════════════════════════════

    section("Schema Validation");

    runner.check("SetSchema", |_, mut db, tok| async move {
        let schema = r#"{"fields":[{"name":"name","type":"string","required":true},{"name":"age","type":"number","required":true},{"name":"city","type":"string","required":false}]}"#;
        let resp = db
            .set_schema(auth_req(
                &tok,
                proto::SetSchemaRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    schema_json: schema.into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        if !resp.into_inner().success {
            return Err("SetSchema failed".into());
        }
        Ok(())
    }).await;

    runner.check("GetSchema", |_, mut db, tok| async move {
        let resp = db
            .get_schema(auth_req(
                &tok,
                proto::GetSchemaRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let inner = resp.into_inner();
        if !inner.found {
            return Err("Schema not found".into());
        }
        if inner.schema_json.is_empty() {
            return Err("Schema JSON is empty".into());
        }
        Ok(())
    }).await;

    runner.check("DropSchema", |_, mut db, tok| async move {
        let resp = db
            .drop_schema(auth_req(
                &tok,
                proto::GetSchemaRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        if !resp.into_inner().success {
            return Err("DropSchema failed".into());
        }
        Ok(())
    }).await;

    runner.check("GetSchema (after drop)", |_, mut db, tok| async move {
        let resp = db
            .get_schema(auth_req(
                &tok,
                proto::GetSchemaRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        if resp.into_inner().found {
            return Err("Schema should be gone after drop".into());
        }
        Ok(())
    }).await;

    // ═══════════════════════════════════════════════
    //  SECTION: Transactions
    // ═══════════════════════════════════════════════

    section("Transactions");

    // Test commit flow
    runner.check("BeginTx + TxInsert + CommitTx", |_, mut db, tok| async move {
        // Begin
        let resp = db
            .begin_tx(auth_req(&tok, proto::Empty {}))
            .await
            .map_err(|e| e.to_string())?;
        let tx_id = resp.into_inner().tx_id;
        if tx_id.is_empty() {
            return Err("Empty tx_id".into());
        }

        // TxInsert
        let resp = db
            .tx_insert(auth_req(
                &tok,
                proto::TxInsertRequest {
                    tx_id: tx_id.clone(),
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    document_json: r#"{"name":"TxUser","age":99}"#.into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        if !resp.into_inner().success {
            return Err("TxInsert failed".into());
        }

        // Commit
        let resp = db
            .commit_tx(auth_req(&tok, proto::TxRequest { tx_id }))
            .await
            .map_err(|e| e.to_string())?;
        if !resp.into_inner().success {
            return Err("CommitTx failed".into());
        }
        Ok(())
    }).await;

    // Verify committed doc exists
    runner.check("Verify committed insert", |_, mut db, tok| async move {
        let resp = db
            .query(auth_req(
                &tok,
                proto::QueryRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    filter_json: r#"{"name":{"$eq":"TxUser"}}"#.into(),
                    pagination: None,
                    sort: None,
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let docs = resp.into_inner().documents_json;
        if docs.len() != 1 {
            return Err(format!("Expected 1 committed TxUser doc, got {}", docs.len()));
        }
        Ok(())
    }).await;

    // Test rollback flow
    runner.check("BeginTx + TxInsert + RollbackTx", |_, mut db, tok| async move {
        let resp = db
            .begin_tx(auth_req(&tok, proto::Empty {}))
            .await
            .map_err(|e| e.to_string())?;
        let tx_id = resp.into_inner().tx_id;

        let resp = db
            .tx_insert(auth_req(
                &tok,
                proto::TxInsertRequest {
                    tx_id: tx_id.clone(),
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    document_json: r#"{"name":"RollbackUser","age":0}"#.into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        if !resp.into_inner().success {
            return Err("TxInsert failed".into());
        }

        let resp = db
            .rollback_tx(auth_req(&tok, proto::TxRequest { tx_id }))
            .await
            .map_err(|e| e.to_string())?;
        if !resp.into_inner().success {
            return Err("RollbackTx failed".into());
        }
        Ok(())
    }).await;

    // Verify rollback didn't persist
    runner.check("Verify rollback discarded", |_, mut db, tok| async move {
        let resp = db
            .query(auth_req(
                &tok,
                proto::QueryRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    filter_json: r#"{"name":{"$eq":"RollbackUser"}}"#.into(),
                    pagination: None,
                    sort: None,
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let docs = resp.into_inner().documents_json;
        if !docs.is_empty() {
            return Err("RollbackUser should not exist after rollback".into());
        }
        Ok(())
    }).await;

    // TxUpdate + TxDelete in a commit
    runner.check("TxUpdate + TxDelete + CommitTx", |_, mut db, tok| async move {
        let resp = db
            .begin_tx(auth_req(&tok, proto::Empty {}))
            .await
            .map_err(|e| e.to_string())?;
        let tx_id = resp.into_inner().tx_id;

        // Find the TxUser doc to get its ID
        let resp = db
            .query(auth_req(
                &tok,
                proto::QueryRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    filter_json: r#"{"name":{"$eq":"TxUser"}}"#.into(),
                    pagination: None,
                    sort: None,
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let docs = resp.into_inner().documents_json;
        if docs.is_empty() {
            return Err("TxUser not found for TxUpdate test".into());
        }
        let doc: serde_json::Value = serde_json::from_str(&docs[0]).map_err(|e| e.to_string())?;
        let doc_id = doc["_id"].as_str().ok_or("No _id on TxUser doc")?.to_string();

        // TxUpdate
        let resp = db
            .tx_update(auth_req(
                &tok,
                proto::TxUpdateRequest {
                    tx_id: tx_id.clone(),
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    id: doc_id.clone(),
                    patch_json: r#"{"age":100}"#.into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        if !resp.into_inner().success {
            return Err("TxUpdate failed".into());
        }

        // TxDelete (delete the same doc)
        let resp = db
            .tx_delete(auth_req(
                &tok,
                proto::TxDeleteRequest {
                    tx_id: tx_id.clone(),
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    id: doc_id,
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        if !resp.into_inner().success {
            return Err("TxDelete failed".into());
        }

        // Commit
        let resp = db
            .commit_tx(auth_req(&tok, proto::TxRequest { tx_id }))
            .await
            .map_err(|e| e.to_string())?;
        if !resp.into_inner().success {
            return Err("CommitTx failed".into());
        }
        Ok(())
    }).await;

    // ═══════════════════════════════════════════════
    //  SECTION: Delete Operations
    // ═══════════════════════════════════════════════

    section("Delete Operations");

    let del_id = inserted_id.clone();
    runner.check("DeleteById", move |_, mut db, tok| async move {
        let resp = db
            .delete_by_id(auth_req(
                &tok,
                proto::DeleteByIdRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    id: del_id,
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let inner = resp.into_inner();
        if !inner.success || !inner.found {
            return Err("DeleteById failed or not found".into());
        }
        Ok(())
    }).await;

    runner.check("DeleteById (not found)", |_, mut db, tok| async move {
        let resp = db
            .delete_by_id(auth_req(
                &tok,
                proto::DeleteByIdRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                    id: "nonexistent-id-12345".into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        let inner = resp.into_inner();
        if inner.found {
            return Err("Should not have found nonexistent doc".into());
        }
        Ok(())
    }).await;

    // ═══════════════════════════════════════════════
    //  SECTION: Cleanup
    // ═══════════════════════════════════════════════

    section("Cleanup");

    runner.check("DropCollection", |_, mut db, tok| async move {
        let resp = db
            .drop_collection(auth_req(
                &tok,
                proto::CollectionRequest {
                    database: TEST_DB.into(),
                    collection: TEST_COL.into(),
                },
            ))
            .await
            .map_err(|e| e.to_string())?;
        if !resp.into_inner().success {
            return Err("DropCollection failed".into());
        }
        Ok(())
    }).await;

    runner.check("DropDatabase", |_, mut db, tok| async move {
        let resp = db
            .drop_database(auth_req(&tok, proto::DatabaseRequest { database: TEST_DB.into() }))
            .await
            .map_err(|e| e.to_string())?;
        if !resp.into_inner().success {
            return Err("DropDatabase failed".into());
        }
        Ok(())
    }).await;

    runner.check("Verify cleanup", |_, mut db, tok| async move {
        let resp = db
            .list_databases(auth_req(&tok, proto::Empty {}))
            .await
            .map_err(|e| e.to_string())?;
        let dbs = resp.into_inner().databases;
        if dbs.iter().any(|d| d == TEST_DB) {
            return Err("Test database still exists after cleanup".into());
        }
        Ok(())
    }).await;

    // ═══════════════════════════════════════════════
    //  REPORT
    // ═══════════════════════════════════════════════

    println!();
    println!(
        "  {}",
        "═════════════════════════════════════════════════════════════════".truecolor(60, 60, 80)
    );
    println!();

    let total = runner.passed + runner.failed;
    let all_passed = runner.failed == 0;

    if all_passed {
        println!(
            "  {} All {} checks passed in {:.2}ms",
            "✓".green().bold(),
            total.to_string().bold().green(),
            runner.total_time.as_secs_f64() * 1000.0
        );
    } else {
        println!(
            "  {} {}/{} checks passed, {} failed ({:.2}ms)",
            "✗".red().bold(),
            runner.passed.to_string().green(),
            total,
            runner.failed.to_string().red().bold(),
            runner.total_time.as_secs_f64() * 1000.0
        );
    }

    println!();

    all_passed
}

fn section(name: &str) {
    println!(
        "  {} {}",
        "▸".truecolor(120, 80, 255),
        name.bold().dimmed()
    );
}
