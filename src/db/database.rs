use ahash::RandomState;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::storage::{MVCCStore, StorageEngine};

use super::changefeed::ChangeFeed;
use super::error::VantaError;
use super::index::{IndexDef, IndexManager, IndexType};
use super::schema::CollectionSchema;
use super::transaction::TransactionManager;

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
    pub(crate) base_path: PathBuf,
    pub(crate) meta_engine: StorageEngine,
    pub(crate) engines: DashMap<String, Arc<MVCCStore>, RandomState>,
    pub index_manager: IndexManager,
    pub tx_manager: TransactionManager,
    pub change_feed: Arc<ChangeFeed>,
    pub(crate) db_locks: DashMap<String, Arc<RwLock<()>>, RandomState>,
    pub(crate) global_ddl_lock: RwLock<()>,
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
            change_feed: Arc::new(ChangeFeed::new()),
            db_locks: DashMap::with_hasher(RandomState::new()),
            global_ddl_lock: RwLock::new(()),
        };

        manager.rebuild_indexes();
        Ok(manager)
    }

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
                    let parts: Vec<&str> = key.splitn(4, ':').collect();
                    if parts.len() >= 3 {
                        let db = parts[1];
                        let col = parts[2];
                        if let Ok(store) = self.db_engine(db) {
                            let docs = Self::load_doc_pairs(&store, col);
                            let idx_key = Self::idx_key(db, col);
                            self.index_manager.add_index(&idx_key, def, &docs);
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn load_doc_pairs(store: &MVCCStore, collection: &str) -> Vec<(String, Value)> {
        store
            .list_keys_latest(collection)
            .into_iter()
            .filter_map(|key| {
                let data = store.get_latest(collection, &key)?;
                let doc: Value = serde_json::from_slice(&data).ok()?;
                Some((key, doc))
            })
            .collect()
    }

    pub(crate) fn db_engine(&self, db_name: &str) -> Result<Arc<MVCCStore>, VantaError> {
        if let Some(store) = self.engines.get(db_name) {
            return Ok(Arc::clone(store.value()));
        }
        let se = Arc::new(StorageEngine::open(&self.base_path.join(db_name))?);
        let mvcc = MVCCStore::new(se);
        for table in mvcc.list_tables() {
            mvcc.load_from_engine(&table);
        }
        let store = Arc::new(mvcc);
        self.engines.insert(db_name.to_string(), Arc::clone(&store));
        Ok(store)
    }

    pub(crate) fn db_read_lock(&self, db: &str) -> Arc<RwLock<()>> {
        self.db_locks
            .entry(db.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .value()
            .clone()
    }

    pub(crate) fn idx_key(db: &str, collection: &str) -> String {
        format!("{}/{}", db, collection)
    }

    pub(crate) fn schema_meta_key(db: &str, collection: &str) -> String {
        format!("_schema:{}:{}", db, collection)
    }

    fn index_meta_key(db: &str, collection: &str, field: &str) -> String {
        format!("_idx:{}:{}:{}", db, collection, field)
    }

    pub(crate) fn require_db(&self, db: &str) -> Result<(), VantaError> {
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
        if let Ok(cols) = self.list_collections(name) {
            for col in &cols {
                let key = Self::idx_key(name, col);
                self.index_manager.drop_collection_indexes(&key);
                let schema_key = Self::schema_meta_key(name, col);
                let _ = self.meta_engine.delete(META_TABLE, &schema_key);
            }
        }
        self.meta_engine.delete(META_TABLE, name)?;
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
        let _guard = lock.write();
        self.require_db(db)?;
        let store = self.db_engine(db)?;
        if store.table_exists(collection) {
            return Err(VantaError::AlreadyExists {
                entity: "Collection",
                name: format!("{}/{}", db, collection),
            });
        }
        store.create_table(collection)?;
        self.update_meta_collections(db, |cols| cols.push(collection.to_string()))?;
        Ok(())
    }

    pub fn drop_collection(&self, db: &str, collection: &str) -> Result<(), VantaError> {
        let lock = self.db_read_lock(db);
        let _guard = lock.write();
        self.require_db(db)?;
        let store = self.db_engine(db)?;
        if !store.table_exists(collection) {
            return Err(VantaError::NotFound {
                entity: "Collection",
                name: format!("{}/{}", db, collection),
            });
        }
        store.drop_table(collection)?;
        self.update_meta_collections(db, |cols| cols.retain(|c| c != collection))?;

        let key = Self::idx_key(db, collection);
        self.index_manager.drop_collection_indexes(&key);
        let schema_key = Self::schema_meta_key(db, collection);
        let _ = self.meta_engine.delete(META_TABLE, &schema_key);

        Ok(())
    }

    pub fn list_collections(&self, db: &str) -> Result<Vec<String>, VantaError> {
        self.require_db(db)?;
        let store = self.db_engine(db)?;
        Ok(store
            .list_tables()
            .into_iter()
            .filter(|t| !t.starts_with('_'))
            .collect())
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

        if self.index_manager.get_index(&key, field).is_some() {
            return Err(VantaError::AlreadyExists {
                entity: "Index",
                name: field.to_string(),
            });
        }

        let store = self.db_engine(db)?;
        let doc_pairs = Self::load_doc_pairs(&store, collection);

        let def = IndexDef {
            collection: key.clone(),
            field: field.to_string(),
            unique,
            index_type,
        };

        self.index_manager.add_index(&key, def.clone(), &doc_pairs);

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

    pub(crate) fn get_schema_internal(&self, db: &str, collection: &str) -> Option<CollectionSchema> {
        let key = Self::schema_meta_key(db, collection);
        let data = self.meta_engine.get(META_TABLE, &key)?;
        serde_json::from_slice(&data).ok()
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

    pub fn all_stores(&self) -> Vec<Arc<MVCCStore>> {
        self.engines.iter().map(|e| Arc::clone(e.value())).collect()
    }
}

// ---- Update patch application ----------------------------

pub fn apply_update(doc: &mut Value, patch: &Value) -> Result<(), VantaError> {
    let doc_obj = doc.as_object_mut().ok_or_else(|| VantaError::ValidationFailed {
        errors: vec!["Document must be an object".to_string()],
    })?;
    let patch_obj = patch.as_object().ok_or_else(|| VantaError::ValidationFailed {
        errors: vec!["Update patch must be an object".to_string()],
    })?;

    let has_operators = patch_obj.keys().any(|k| k.starts_with('$'));

    if !has_operators {
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
