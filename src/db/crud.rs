use serde_json::Value;
use uuid::Uuid;

use super::changefeed::ChangeOp;
use super::database::{apply_update, DatabaseManager, QueryOptions};
use super::error::VantaError;
use super::filter::matches_filter;

impl DatabaseManager {
    // ---- Document CRUD ---------------------------------------

    pub fn insert(
        &self,
        db: &str,
        collection: &str,
        document: Value,
    ) -> Result<String, VantaError> {
        let lock = self.db_read_lock(db);
        let _guard = lock.read();
        self.require_db(db)?;
        let store = self.db_engine(db)?;
        if !store.table_exists(collection) {
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
        store.put(collection, &id, &data, Uuid::new_v4())?;

        // Update indexes
        let idx_key = Self::idx_key(db, collection);
        self.index_manager.on_insert(&idx_key, &id, &doc);

        // Emit change event
        self.change_feed.emit(db, collection, ChangeOp::Insert, &id, Some(doc));

        Ok(id)
    }

    pub fn find_by_id(
        &self,
        db: &str,
        collection: &str,
        id: &str,
    ) -> Result<Option<Value>, VantaError> {
        self.require_db(db)?;
        let store = self.db_engine(db)?;
        match store.get_latest(collection, id) {
            Some(data) => {
                let doc: Value = serde_json::from_slice(&data)?;
                Ok(Some(doc))
            }
            None => Ok(None),
        }
    }

    pub fn find_all(&self, db: &str, collection: &str) -> Result<Vec<Value>, VantaError> {
        self.require_db(db)?;
        let store = self.db_engine(db)?;
        let keys = store.list_keys_latest(collection);
        let mut docs = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(data) = store.get_latest(collection, &key) {
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

        // Try index lookup first
        let idx_key = Self::idx_key(db, collection);
        if let Some(idx) = self.index_manager.get_index(&idx_key, field) {
            let store = self.db_engine(db)?;
            let ids = idx.lookup_eq(value);
            let mut docs = Vec::with_capacity(ids.len());
            for id in &ids {
                if let Some(data) = store.get_latest(collection, id) {
                    if let Ok(doc) = serde_json::from_slice::<Value>(&data) {
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
        let store = self.db_engine(db)?;

        let old_doc = store
            .get_latest(collection, id)
            .and_then(|data| serde_json::from_slice::<Value>(&data).ok());

        let result = store.delete(collection, id, Uuid::new_v4())?;

        if result.is_some() {
            if let Some(ref doc) = old_doc {
                let idx_key = Self::idx_key(db, collection);
                self.index_manager.on_delete(&idx_key, id, doc);
            }
            self.change_feed.emit(db, collection, ChangeOp::Delete, id, None);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn count(&self, db: &str, collection: &str) -> Result<usize, VantaError> {
        self.require_db(db)?;
        let store = self.db_engine(db)?;
        Ok(store.count_latest(collection))
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
        let store = self.db_engine(db)?;

        let old_data = match store.get_latest(collection, id) {
            Some(d) => d,
            None => return Ok(false),
        };
        let old_doc: Value = serde_json::from_slice(&old_data)?;

        let mut new_doc = old_doc.clone();
        apply_update(&mut new_doc, &patch)?;

        if let Some(schema) = self.get_schema_internal(db, collection) {
            if let Err(errors) = schema.validate(&new_doc) {
                return Err(VantaError::ValidationFailed { errors });
            }
        }

        let data = serde_json::to_vec(&new_doc)?;
        store.put(collection, id, &data, Uuid::new_v4())?;

        let idx_key = Self::idx_key(db, collection);
        self.index_manager
            .on_update(&idx_key, id, &old_doc, &new_doc);

        self.change_feed.emit(db, collection, ChangeOp::Update, id, Some(new_doc));

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
        let store = self.db_engine(db)?;
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
            store.put(collection, &id, &data, Uuid::new_v4())?;
            self.index_manager.on_update(&idx_key, &id, doc, &new_doc);
            self.change_feed.emit(db, collection, ChangeOp::Update, &id, Some(new_doc));
            modified += 1;
        }

        Ok(modified)
    }
}
