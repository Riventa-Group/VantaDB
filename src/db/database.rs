use ahash::RandomState;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

use crate::storage::StorageEngine;

use super::aggregation;
use super::error::VantaError;
use super::filter::matches_filter;
use super::index::{IndexDef, IndexManager, IndexType};
use super::planner::{self, QueryPlan};
use super::schema::CollectionSchema;
use super::transaction::{TransactionManager, TransactionOp};

const META_TABLE: &str = "_vanta_meta";

// ---- Query options ----------------------------------------

#[derive(Debug, Clone)]
pub struct QueryOptions {
    pub sort_field: Option<String>,
    pub sort_descending: bool,
    pub page: u32,
    pub page_size: u32,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            sort_field: None,
            sort_descending: false,
            page: 0,
            page_size: 50,
        }
    }
}

impl QueryOptions {
    pub fn apply(&self, mut docs: Vec<Value>) -> (Vec<Value>, usize) {
        if let Some(ref field) = self.sort_field {
            let desc = self.sort_descending;
            docs.sort_by(|a, b| {
                let ord = compare_json_values(a.get(field), b.get(field));
                if desc {
                    ord.reverse()
                } else {
                    ord
                }
            });
        }

        let total = docs.len();

        if self.page > 0 {
            let size = if self.page_size == 0 { 50 } else { self.page_size } as usize;
            let skip = ((self.page - 1) as usize) * size;
            docs = docs.into_iter().skip(skip).take(size).collect();
        }

        (docs, total)
    }
}

fn compare_json_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(va), Some(vb)) => {
            if let (Some(na), Some(nb)) = (va.as_f64(), vb.as_f64()) {
                return na.partial_cmp(&nb).unwrap_or(Ordering::Equal);
            }
            if let (Some(sa), Some(sb)) = (va.as_str(), vb.as_str()) {
                return sa.cmp(sb);
            }
            if let (Some(ba), Some(bb)) = (va.as_bool(), vb.as_bool()) {
                return ba.cmp(&bb);
            }
            va.to_string().cmp(&vb.to_string())
        }
    }
}

// ---- Database metadata ------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct DbMeta {
    name: String,
    collections: Vec<String>,
}

// ---- Database Manager -------------------------------------

pub struct DatabaseManager {
    base_path: PathBuf,
    meta_engine: StorageEngine,
    engines: DashMap<String, Arc<StorageEngine>, RandomState>,
    pub index_manager: IndexManager,
    pub tx_manager: TransactionManager,
    db_locks: DashMap<String, Arc<RwLock<()>>, RandomState>,
    global_ddl_lock: RwLock<()>,
}

impl DatabaseManager {
    pub fn new(base_path: &Path) -> Result<Self, VantaError> {
        let meta_engine = StorageEngine::open(&base_path.join("_meta"))?;
        if !meta_engine.table_exists(META_TABLE) {
            meta_engine.create_table(META_TABLE)?;
        }
        let manager = Self {
            base_path: base_path.to_path_buf(),
            meta_engine,
            engines: DashMap::with_hasher(RandomState::new()),
            index_manager: IndexManager::new(),
            tx_manager: TransactionManager::new(),
            db_locks: DashMap::with_hasher(RandomState::new()),
            global_ddl_lock: RwLock::new(()),
        };

        // Rebuild persisted indexes on startup (#22)
        manager.rebuild_indexes();

        Ok(manager)
    }

    /// Rebuild all persisted indexes from meta engine on startup.
    fn rebuild_indexes(&self) {
        let index_keys: Vec<String> = self
            .meta_engine
            .list_keys(META_TABLE)
            .into_iter()
            .filter(|k| k.starts_with("_idx:"))
            .collect();

        for key in index_keys {
            if let Some(data) = self.meta_engine.get(META_TABLE, &key) {
                if let Ok(def) = serde_json::from_slice::<IndexDef>(&data) {
                    // Parse "db:collection:field" from "_idx:db:col:field"
                    let parts: Vec<&str> = key.splitn(4, ':').collect();
                    if parts.len() >= 3 {
                        let db = parts[1];
                        let col = parts[2];
                        if let Ok(engine) = self.db_engine(db) {
                            let docs = Self::load_doc_pairs(&engine, col);
                            let idx_key = Self::idx_key(db, col);
                            self.index_manager.add_index(&idx_key, def, &docs);
                        }
                    }
                }
            }
        }
    }

    /// Load all documents from a collection as (id, Value) pairs.
    fn load_doc_pairs(engine: &StorageEngine, collection: &str) -> Vec<(String, Value)> {
        engine
            .list_keys(collection)
            .into_iter()
            .filter_map(|key| {
                let data = engine.get(collection, &key)?;
                let doc: Value = serde_json::from_slice(&data).ok()?;
                Some((key, doc))
            })
            .collect()
    }

    /// Get or open a cached StorageEngine for a database (#8).
    /// Engines are cached for the lifetime of the DatabaseManager and only
    /// evicted on drop_database.
    fn db_engine(&self, db_name: &str) -> Result<Arc<StorageEngine>, VantaError> {
        if let Some(engine) = self.engines.get(db_name) {
            return Ok(Arc::clone(engine.value()));
        }
        let engine = Arc::new(StorageEngine::open(&self.base_path.join(db_name))?);
        self.engines.insert(db_name.to_string(), Arc::clone(&engine));
        Ok(engine)
    }

    /// Acquire a shared (read) lock for DML operations on a database (#10).
    fn db_read_lock(&self, db: &str) -> Arc<RwLock<()>> {
        self.db_locks
            .entry(db.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .value()
            .clone()
    }

    fn idx_key(db: &str, collection: &str) -> String {
        format!("{}/{}", db, collection)
    }

    fn schema_meta_key(db: &str, collection: &str) -> String {
        format!("_schema:{}:{}", db, collection)
    }

    fn index_meta_key(db: &str, collection: &str, field: &str) -> String {
        format!("_idx:{}:{}:{}", db, collection, field)
    }

    fn require_db(&self, db: &str) -> Result<(), VantaError> {
        if self.meta_engine.get(META_TABLE, db).is_some() {
            Ok(())
        } else {
            Err(VantaError::NotFound {
                entity: "Database",
                name: db.to_string(),
            })
        }
    }

    // ---- Database ops ----------------------------------------

    pub fn create_database(&self, name: &str) -> Result<(), VantaError> {
        let _guard = self.global_ddl_lock.write();
        if self.meta_engine.get(META_TABLE, name).is_some() {
            return Err(VantaError::AlreadyExists {
                entity: "Database",
                name: name.to_string(),
            });
        }
        let meta = DbMeta {
            name: name.to_string(),
            collections: vec![],
        };
        let data = bincode::serialize(&meta)?;
        self.meta_engine.put(META_TABLE, name, &data)?;
        std::fs::create_dir_all(self.base_path.join(name))?;
        Ok(())
    }

    pub fn drop_database(&self, name: &str) -> Result<(), VantaError> {
        let _guard = self.global_ddl_lock.write();
        self.require_db(name)?;
        // Clean up indexes and schemas for all collections
        if let Ok(cols) = self.list_collections(name) {
            for col in &cols {
                let key = Self::idx_key(name, col);
                self.index_manager.drop_collection_indexes(&key);
                let schema_key = Self::schema_meta_key(name, col);
                let _ = self.meta_engine.delete(META_TABLE, &schema_key);
            }
        }
        self.meta_engine.delete(META_TABLE, name)?;
        // Evict cached engine
        self.engines.remove(name);
        self.db_locks.remove(name);
        let db_path = self.base_path.join(name);
        if db_path.exists() {
            std::fs::remove_dir_all(db_path)?;
        }
        Ok(())
    }

    pub fn list_databases(&self) -> Vec<String> {
        self.meta_engine
            .list_keys(META_TABLE)
            .into_iter()
            .filter(|k| !k.starts_with('_'))
            .collect()
    }

    pub fn database_exists(&self, name: &str) -> bool {
        self.meta_engine.get(META_TABLE, name).is_some()
    }

    // ---- Collection ops --------------------------------------

    pub fn create_collection(&self, db: &str, collection: &str) -> Result<(), VantaError> {
        let lock = self.db_read_lock(db);
        let _guard = lock.write(); // exclusive DDL lock on this database (#10)
        self.require_db(db)?;
        let engine = self.db_engine(db)?;
        if engine.table_exists(collection) {
            return Err(VantaError::AlreadyExists {
                entity: "Collection",
                name: format!("{}/{}", db, collection),
            });
        }
        engine.create_table(collection)?;
        self.update_meta_collections(db, |cols| cols.push(collection.to_string()))?;
        Ok(())
    }

    pub fn drop_collection(&self, db: &str, collection: &str) -> Result<(), VantaError> {
        let lock = self.db_read_lock(db);
        let _guard = lock.write(); // exclusive DDL lock on this database (#10)
        self.require_db(db)?;
        let engine = self.db_engine(db)?;
        if !engine.table_exists(collection) {
            return Err(VantaError::NotFound {
                entity: "Collection",
                name: format!("{}/{}", db, collection),
            });
        }
        engine.drop_table(collection)?;
        self.update_meta_collections(db, |cols| cols.retain(|c| c != collection))?;

        // Clean up indexes and schema
        let key = Self::idx_key(db, collection);
        self.index_manager.drop_collection_indexes(&key);
        let schema_key = Self::schema_meta_key(db, collection);
        let _ = self.meta_engine.delete(META_TABLE, &schema_key);

        Ok(())
    }

    pub fn list_collections(&self, db: &str) -> Result<Vec<String>, VantaError> {
        self.require_db(db)?;
        let engine = self.db_engine(db)?;
        Ok(engine
            .list_tables()
            .into_iter()
            .filter(|t| !t.starts_with('_'))
            .collect())
    }

    // ---- Document CRUD ---------------------------------------

    pub fn insert(
        &self,
        db: &str,
        collection: &str,
        document: Value,
    ) -> Result<String, VantaError> {
        let lock = self.db_read_lock(db);
        let _guard = lock.read(); // shared DML lock — DDL blocked while active (#10)
        self.require_db(db)?;
        let engine = self.db_engine(db)?;
        if !engine.table_exists(collection) {
            return Err(VantaError::NotFound {
                entity: "Collection",
                name: collection.to_string(),
            });
        }

        // Schema validation
        if let Some(schema) = self.get_schema_internal(db, collection) {
            if let Err(errors) = schema.validate(&document) {
                return Err(VantaError::ValidationFailed { errors });
            }
        }

        let id = if let Some(id_val) = document.get("_id") {
            id_val
                .as_str()
                .unwrap_or(&Uuid::new_v4().to_string())
                .to_string()
        } else {
            Uuid::new_v4().to_string()
        };

        let mut doc = document;
        if let Some(o) = doc.as_object_mut() {
            o.insert("_id".to_string(), Value::String(id.clone()));
        }

        let data = serde_json::to_vec(&doc)?;
        engine.put(collection, &id, &data)?;

        // Update indexes
        let idx_key = Self::idx_key(db, collection);
        self.index_manager.on_insert(&idx_key, &id, &doc);

        Ok(id)
    }

    pub fn find_by_id(
        &self,
        db: &str,
        collection: &str,
        id: &str,
    ) -> Result<Option<Value>, VantaError> {
        self.require_db(db)?;
        let engine = self.db_engine(db)?;
        match engine.get(collection, id) {
            Some(data) => {
                let doc: Value = serde_json::from_slice(&data)?;
                Ok(Some(doc))
            }
            None => Ok(None),
        }
    }

    pub fn find_all(&self, db: &str, collection: &str) -> Result<Vec<Value>, VantaError> {
        self.require_db(db)?;
        let engine = self.db_engine(db)?;
        let keys = engine.list_keys(collection);
        let mut docs = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(data) = engine.get(collection, &key) {
                if let Ok(doc) = serde_json::from_slice::<Value>(&data) {
                    docs.push(doc);
                }
            }
        }
        Ok(docs)
    }

    pub fn find_where(
        &self,
        db: &str,
        collection: &str,
        field: &str,
        value: &Value,
    ) -> Result<Vec<Value>, VantaError> {
        self.require_db(db)?;

        // Try index lookup first (#17)
        let idx_key = Self::idx_key(db, collection);
        if let Some(idx) = self.index_manager.get_index(&idx_key, field) {
            let engine = self.db_engine(db)?;
            let ids = idx.lookup_eq(value);
            let mut docs = Vec::with_capacity(ids.len());
            for id in &ids {
                if let Some(data) = engine.get(collection, id) {
                    if let Ok(doc) = serde_json::from_slice::<Value>(&data) {
                        // Double-check in case of stale index entry
                        if doc.get(field) == Some(value) {
                            docs.push(doc);
                        }
                    }
                }
            }
            return Ok(docs);
        }

        // Fallback: full scan
        let docs = self.find_all(db, collection)?;
        let filtered: Vec<Value> = docs
            .into_iter()
            .filter(|doc| doc.get(field) == Some(value))
            .collect();
        Ok(filtered)
    }

    pub fn find_all_query(
        &self,
        db: &str,
        collection: &str,
        opts: &QueryOptions,
    ) -> Result<(Vec<Value>, usize), VantaError> {
        let docs = self.find_all(db, collection)?;
        Ok(opts.apply(docs))
    }

    pub fn find_where_query(
        &self,
        db: &str,
        collection: &str,
        field: &str,
        value: &Value,
        opts: &QueryOptions,
    ) -> Result<(Vec<Value>, usize), VantaError> {
        let docs = self.find_where(db, collection, field, value)?;
        Ok(opts.apply(docs))
    }

    pub fn delete_by_id(
        &self,
        db: &str,
        collection: &str,
        id: &str,
    ) -> Result<bool, VantaError> {
        let lock = self.db_read_lock(db);
        let _guard = lock.read();
        self.require_db(db)?;
        let engine = self.db_engine(db)?;

        // Get old doc for index update
        let old_doc = engine
            .get(collection, id)
            .and_then(|data| serde_json::from_slice::<Value>(&data).ok());

        let deleted = engine.delete(collection, id)?;

        if deleted {
            if let Some(ref doc) = old_doc {
                let idx_key = Self::idx_key(db, collection);
                self.index_manager.on_delete(&idx_key, id, doc);
            }
        }

        Ok(deleted)
    }

    pub fn count(&self, db: &str, collection: &str) -> Result<usize, VantaError> {
        self.require_db(db)?;
        let engine = self.db_engine(db)?;
        Ok(engine.count(collection))
    }

    // ---- Update operations -----------------------------------

    pub fn update_by_id(
        &self,
        db: &str,
        collection: &str,
        id: &str,
        patch: Value,
    ) -> Result<bool, VantaError> {
        let lock = self.db_read_lock(db);
        let _guard = lock.read();
        self.require_db(db)?;
        let engine = self.db_engine(db)?;

        let old_data = match engine.get(collection, id) {
            Some(d) => d,
            None => return Ok(false),
        };
        let old_doc: Value = serde_json::from_slice(&old_data)?;

        let mut new_doc = old_doc.clone();
        apply_update(&mut new_doc, &patch)?;

        // Schema validation on the updated document
        if let Some(schema) = self.get_schema_internal(db, collection) {
            if let Err(errors) = schema.validate(&new_doc) {
                return Err(VantaError::ValidationFailed { errors });
            }
        }

        let data = serde_json::to_vec(&new_doc)?;
        engine.put(collection, id, &data)?;

        // Update indexes
        let idx_key = Self::idx_key(db, collection);
        self.index_manager
            .on_update(&idx_key, id, &old_doc, &new_doc);

        Ok(true)
    }

    pub fn update_where(
        &self,
        db: &str,
        collection: &str,
        filter: &Value,
        patch: &Value,
    ) -> Result<u64, VantaError> {
        let docs = self.find_all(db, collection)?;
        let engine = self.db_engine(db)?;
        let schema = self.get_schema_internal(db, collection);
        let idx_key = Self::idx_key(db, collection);
        let mut modified = 0u64;

        for doc in &docs {
            if !matches_filter(doc, filter) {
                continue;
            }
            let id = match doc.get("_id").and_then(|v| v.as_str()) {
                Some(id) => id.to_string(),
                None => continue,
            };

            let mut new_doc = doc.clone();
            if apply_update(&mut new_doc, patch).is_err() {
                continue;
            }

            if let Some(ref s) = schema {
                if s.validate(&new_doc).is_err() {
                    continue;
                }
            }

            let data = serde_json::to_vec(&new_doc)?;
            engine.put(collection, &id, &data)?;
            self.index_manager.on_update(&idx_key, &id, doc, &new_doc);
            modified += 1;
        }

        Ok(modified)
    }

    // ---- Rich query (filter) ---------------------------------

    pub fn query(
        &self,
        db: &str,
        collection: &str,
        filter: &Value,
        opts: &QueryOptions,
    ) -> Result<(Vec<Value>, usize), VantaError> {
        self.require_db(db)?;
        let engine = self.db_engine(db)?;

        let idx_key = Self::idx_key(db, collection);
        let indexes = self.index_manager.get_all_indexes(&idx_key);
        let plan = planner::plan_query(filter, &indexes);

        let filtered = match plan {
            QueryPlan::FullScan => {
                // Fallback: scan all documents
                let all = self.find_all(db, collection)?;
                all.into_iter()
                    .filter(|doc| matches_filter(doc, filter))
                    .collect()
            }
            QueryPlan::IndexScan {
                index,
                predicate,
                residual_filter,
            } => {
                // Use index to get candidate doc IDs
                let candidate_ids = planner::execute_index_scan(index.as_ref(), &predicate);
                let mut docs = Vec::with_capacity(candidate_ids.len());

                for id in &candidate_ids {
                    if let Some(data) = engine.get(collection, id) {
                        if let Ok(doc) = serde_json::from_slice::<Value>(&data) {
                            // Apply residual filter if any
                            let passes = match &residual_filter {
                                Some(rf) => matches_filter(&doc, rf),
                                None => true,
                            };
                            if passes {
                                docs.push(doc);
                            }
                        }
                    }
                }
                docs
            }
        };

        Ok(opts.apply(filtered))
    }

    // ---- Aggregation -----------------------------------------

    pub fn aggregate(
        &self,
        db: &str,
        collection: &str,
        pipeline: &[Value],
    ) -> Result<Vec<Value>, VantaError> {
        let docs = self.find_all(db, collection)?;

        let db_owned = db.to_string();
        let self_ref = &self;
        let resolver = move |foreign_col: &str| -> Vec<Value> {
            self_ref
                .find_all(&db_owned, foreign_col)
                .unwrap_or_default()
        };

        // Per-value index lookup closure for $lookup optimization (#19)
        let db_for_lookup = db.to_string();
        let lookup_index_fn =
            move |foreign_col: &str, field: &str, value: &Value| -> Option<Vec<String>> {
                let idx_key = Self::idx_key(&db_for_lookup, foreign_col);
                let idx = self.index_manager.get_index(&idx_key, field)?;
                Some(idx.lookup_eq(value))
            };

        aggregation::execute_pipeline_with_indexes(
            docs,
            pipeline,
            &resolver,
            &lookup_index_fn,
        )
        .map_err(|e| VantaError::Internal(e))
    }

    // ---- Index operations ------------------------------------

    pub fn create_index(
        &self,
        db: &str,
        collection: &str,
        field: &str,
        unique: bool,
    ) -> Result<(), VantaError> {
        self.create_index_typed(db, collection, field, unique, IndexType::BTree)
    }

    pub fn create_index_typed(
        &self,
        db: &str,
        collection: &str,
        field: &str,
        unique: bool,
        index_type: IndexType,
    ) -> Result<(), VantaError> {
        self.require_db(db)?;

        let key = Self::idx_key(db, collection);

        // Check if index already exists
        if self.index_manager.get_index(&key, field).is_some() {
            return Err(VantaError::AlreadyExists {
                entity: "Index",
                name: field.to_string(),
            });
        }

        // Load all docs to build the index
        let engine = self.db_engine(db)?;
        let doc_pairs = Self::load_doc_pairs(&engine, collection);

        let def = IndexDef {
            collection: key.clone(),
            field: field.to_string(),
            unique,
            index_type,
        };

        self.index_manager.add_index(&key, def.clone(), &doc_pairs);

        // Persist index definition
        let meta_key = Self::index_meta_key(db, collection, field);
        let data = serde_json::to_vec(&def)?;
        self.meta_engine.put(META_TABLE, &meta_key, &data)?;

        Ok(())
    }

    pub fn drop_index(
        &self,
        db: &str,
        collection: &str,
        field: &str,
    ) -> Result<(), VantaError> {
        let key = Self::idx_key(db, collection);
        if !self.index_manager.remove_index(&key, field) {
            return Err(VantaError::NotFound {
                entity: "Index",
                name: field.to_string(),
            });
        }
        let meta_key = Self::index_meta_key(db, collection, field);
        let _ = self.meta_engine.delete(META_TABLE, &meta_key);
        Ok(())
    }

    pub fn list_indexes(
        &self,
        db: &str,
        collection: &str,
    ) -> Result<Vec<IndexDef>, VantaError> {
        self.require_db(db)?;
        let key = Self::idx_key(db, collection);
        Ok(self.index_manager.list_indexes(&key))
    }

    // ---- Schema operations -----------------------------------

    pub fn set_schema(
        &self,
        db: &str,
        collection: &str,
        schema: &CollectionSchema,
    ) -> Result<(), VantaError> {
        self.require_db(db)?;
        let key = Self::schema_meta_key(db, collection);
        let data = serde_json::to_vec(schema)?;
        self.meta_engine.put(META_TABLE, &key, &data)?;
        Ok(())
    }

    pub fn get_schema(
        &self,
        db: &str,
        collection: &str,
    ) -> Result<Option<CollectionSchema>, VantaError> {
        self.require_db(db)?;
        Ok(self.get_schema_internal(db, collection))
    }

    pub fn drop_schema(&self, db: &str, collection: &str) -> Result<(), VantaError> {
        self.require_db(db)?;
        let key = Self::schema_meta_key(db, collection);
        self.meta_engine.delete(META_TABLE, &key)?;
        Ok(())
    }

    fn get_schema_internal(&self, db: &str, collection: &str) -> Option<CollectionSchema> {
        let key = Self::schema_meta_key(db, collection);
        let data = self.meta_engine.get(META_TABLE, &key)?;
        serde_json::from_slice(&data).ok()
    }

    // ---- Transaction operations ------------------------------

    pub fn begin_transaction(&self) -> String {
        self.tx_manager.begin()
    }

    pub fn tx_insert(
        &self,
        tx_id: &str,
        db: String,
        collection: String,
        document: Value,
    ) -> Result<(), VantaError> {
        self.tx_manager.add_op(
            tx_id,
            TransactionOp::Insert {
                db,
                collection,
                document,
            },
        )
    }

    pub fn tx_update(
        &self,
        tx_id: &str,
        db: String,
        collection: String,
        id: String,
        patch: Value,
    ) -> Result<(), VantaError> {
        self.tx_manager.add_op(
            tx_id,
            TransactionOp::Update {
                db,
                collection,
                id,
                patch,
            },
        )
    }

    pub fn tx_delete(
        &self,
        tx_id: &str,
        db: String,
        collection: String,
        id: String,
    ) -> Result<(), VantaError> {
        self.tx_manager.add_op(
            tx_id,
            TransactionOp::Delete {
                db,
                collection,
                id,
            },
        )
    }

    pub fn commit_transaction(&self, tx_id: &str) -> Result<(), VantaError> {
        let tx = self.tx_manager.take(tx_id)?;

        for op in tx.ops {
            match op {
                TransactionOp::Insert {
                    db,
                    collection,
                    document,
                } => {
                    self.insert(&db, &collection, document)?;
                }
                TransactionOp::Update {
                    db,
                    collection,
                    id,
                    patch,
                } => {
                    self.update_by_id(&db, &collection, &id, patch)?;
                }
                TransactionOp::Delete {
                    db,
                    collection,
                    id,
                } => {
                    self.delete_by_id(&db, &collection, &id)?;
                }
            }
        }
        Ok(())
    }

    pub fn rollback_transaction(&self, tx_id: &str) -> Result<(), VantaError> {
        self.tx_manager.rollback(tx_id)
    }

    // ---- Internal helpers ------------------------------------

    fn update_meta_collections<F: FnOnce(&mut Vec<String>)>(
        &self,
        db: &str,
        f: F,
    ) -> Result<(), VantaError> {
        if let Some(data) = self.meta_engine.get(META_TABLE, db) {
            let mut meta: DbMeta = bincode::deserialize(&data)?;
            f(&mut meta.collections);
            let new_data = bincode::serialize(&meta)?;
            self.meta_engine.put(META_TABLE, db, &new_data)?;
        }
        Ok(())
    }
}

// ---- Update patch application ----------------------------

/// Apply an update patch to a document.
///
/// Supports:
///   Plain object: treated as $set (merge fields)
///   {"$set": {"field": "value"}}   - set fields
///   {"$unset": ["field1", ...]}    - remove fields
///   {"$inc": {"field": 1}}         - increment numeric fields
///   {"$push": {"field": "value"}}  - push to array
///   {"$pull": {"field": "value"}}  - remove from array
pub fn apply_update(doc: &mut Value, patch: &Value) -> Result<(), VantaError> {
    let doc_obj = doc.as_object_mut().ok_or_else(|| VantaError::ValidationFailed {
        errors: vec!["Document must be an object".to_string()],
    })?;
    let patch_obj = patch.as_object().ok_or_else(|| VantaError::ValidationFailed {
        errors: vec!["Update patch must be an object".to_string()],
    })?;

    let has_operators = patch_obj.keys().any(|k| k.starts_with('$'));

    if !has_operators {
        // Plain object = $set shorthand
        for (k, v) in patch_obj {
            if k != "_id" {
                doc_obj.insert(k.clone(), v.clone());
            }
        }
        return Ok(());
    }

    for (op, val) in patch_obj {
        match op.as_str() {
            "$set" => {
                if let Some(set_obj) = val.as_object() {
                    for (k, v) in set_obj {
                        if k != "_id" {
                            doc_obj.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
            "$unset" => match val {
                Value::Array(arr) => {
                    for field in arr {
                        if let Some(s) = field.as_str() {
                            doc_obj.remove(s);
                        }
                    }
                }
                Value::Object(obj) => {
                    for k in obj.keys() {
                        doc_obj.remove(k);
                    }
                }
                _ => {}
            },
            "$inc" => {
                if let Some(inc_obj) = val.as_object() {
                    for (k, v) in inc_obj {
                        if let Some(inc_val) = v.as_f64() {
                            let current =
                                doc_obj.get(k).and_then(|v| v.as_f64()).unwrap_or(0.0);
                            doc_obj.insert(k.clone(), Value::from(current + inc_val));
                        }
                    }
                }
            }
            "$push" => {
                if let Some(push_obj) = val.as_object() {
                    for (k, v) in push_obj {
                        let arr = doc_obj
                            .entry(k.clone())
                            .or_insert(Value::Array(vec![]));
                        if let Some(arr) = arr.as_array_mut() {
                            arr.push(v.clone());
                        }
                    }
                }
            }
            "$pull" => {
                if let Some(pull_obj) = val.as_object() {
                    for (k, v) in pull_obj {
                        if let Some(arr) = doc_obj.get_mut(k).and_then(|a| a.as_array_mut())
                        {
                            arr.retain(|item| item != v);
                        }
                    }
                }
            }
            _ => {
                return Err(VantaError::ValidationFailed {
                    errors: vec![format!("Unknown update operator: {}", op)],
                })
            }
        }
    }

    Ok(())
}
