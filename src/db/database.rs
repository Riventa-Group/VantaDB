use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::storage::StorageEngine;

const META_TABLE: &str = "_vanta_meta";

#[derive(Debug, Serialize, Deserialize)]
struct DbMeta {
    name: String,
    collections: Vec<String>,
}

pub struct DatabaseManager {
    base_path: PathBuf,
    meta_engine: StorageEngine,
}

impl DatabaseManager {
    pub fn new(base_path: &Path) -> io::Result<Self> {
        let meta_engine = StorageEngine::open(&base_path.join("_meta"))?;
        if !meta_engine.table_exists(META_TABLE) {
            meta_engine.create_table(META_TABLE)?;
        }
        Ok(Self {
            base_path: base_path.to_path_buf(),
            meta_engine,
        })
    }

    fn db_engine(&self, db_name: &str) -> io::Result<StorageEngine> {
        StorageEngine::open(&self.base_path.join(db_name))
    }

    pub fn create_database(&self, name: &str) -> io::Result<Result<(), String>> {
        if self.meta_engine.get(META_TABLE, name).is_some() {
            return Ok(Err(format!("Database '{}' already exists", name)));
        }
        let meta = DbMeta {
            name: name.to_string(),
            collections: vec![],
        };
        let data = bincode::serialize(&meta)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.meta_engine.put(META_TABLE, name, &data)?;
        // Create the db directory
        std::fs::create_dir_all(self.base_path.join(name))?;
        Ok(Ok(()))
    }

    pub fn drop_database(&self, name: &str) -> io::Result<Result<(), String>> {
        if self.meta_engine.get(META_TABLE, name).is_none() {
            return Ok(Err(format!("Database '{}' not found", name)));
        }
        self.meta_engine.delete(META_TABLE, name)?;
        let db_path = self.base_path.join(name);
        if db_path.exists() {
            std::fs::remove_dir_all(db_path)?;
        }
        Ok(Ok(()))
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

    pub fn create_collection(&self, db: &str, collection: &str) -> io::Result<Result<(), String>> {
        if !self.database_exists(db) {
            return Ok(Err(format!("Database '{}' not found", db)));
        }
        let engine = self.db_engine(db)?;
        if engine.table_exists(collection) {
            return Ok(Err(format!("Collection '{}' already exists in '{}'", collection, db)));
        }
        engine.create_table(collection)?;

        // Update meta
        self.update_meta_collections(db, |cols| cols.push(collection.to_string()))?;
        Ok(Ok(()))
    }

    pub fn drop_collection(&self, db: &str, collection: &str) -> io::Result<Result<(), String>> {
        if !self.database_exists(db) {
            return Ok(Err(format!("Database '{}' not found", db)));
        }
        let engine = self.db_engine(db)?;
        if !engine.table_exists(collection) {
            return Ok(Err(format!("Collection '{}' not found in '{}'", collection, db)));
        }
        engine.drop_table(collection)?;
        self.update_meta_collections(db, |cols| cols.retain(|c| c != collection))?;
        Ok(Ok(()))
    }

    pub fn list_collections(&self, db: &str) -> io::Result<Result<Vec<String>, String>> {
        if !self.database_exists(db) {
            return Ok(Err(format!("Database '{}' not found", db)));
        }
        let engine = self.db_engine(db)?;
        Ok(Ok(engine
            .list_tables()
            .into_iter()
            .filter(|t| !t.starts_with('_'))
            .collect()))
    }

    pub fn insert(
        &self,
        db: &str,
        collection: &str,
        document: Value,
    ) -> io::Result<Result<String, String>> {
        if !self.database_exists(db) {
            return Ok(Err(format!("Database '{}' not found", db)));
        }
        let engine = self.db_engine(db)?;
        if !engine.table_exists(collection) {
            return Ok(Err(format!("Collection '{}' not found", collection)));
        }

        let id = if let Some(id_val) = document.get("_id") {
            id_val.as_str().unwrap_or(&Uuid::new_v4().to_string()).to_string()
        } else {
            Uuid::new_v4().to_string()
        };

        let mut doc = document;
        doc.as_object_mut().map(|o| {
            o.insert("_id".to_string(), Value::String(id.clone()));
        });

        let data = serde_json::to_vec(&doc)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        engine.put(collection, &id, &data)?;
        Ok(Ok(id))
    }

    pub fn find_by_id(
        &self,
        db: &str,
        collection: &str,
        id: &str,
    ) -> io::Result<Result<Option<Value>, String>> {
        if !self.database_exists(db) {
            return Ok(Err(format!("Database '{}' not found", db)));
        }
        let engine = self.db_engine(db)?;
        match engine.get(collection, id) {
            Some(data) => {
                let doc: Value = serde_json::from_slice(&data)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                Ok(Ok(Some(doc)))
            }
            None => Ok(Ok(None)),
        }
    }

    pub fn find_all(
        &self,
        db: &str,
        collection: &str,
    ) -> io::Result<Result<Vec<Value>, String>> {
        if !self.database_exists(db) {
            return Ok(Err(format!("Database '{}' not found", db)));
        }
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
        Ok(Ok(docs))
    }

    pub fn find_where(
        &self,
        db: &str,
        collection: &str,
        field: &str,
        value: &Value,
    ) -> io::Result<Result<Vec<Value>, String>> {
        let all = self.find_all(db, collection)?;
        match all {
            Ok(docs) => {
                let filtered: Vec<Value> = docs
                    .into_iter()
                    .filter(|doc| doc.get(field) == Some(value))
                    .collect();
                Ok(Ok(filtered))
            }
            Err(e) => Ok(Err(e)),
        }
    }

    pub fn delete_by_id(
        &self,
        db: &str,
        collection: &str,
        id: &str,
    ) -> io::Result<Result<bool, String>> {
        if !self.database_exists(db) {
            return Ok(Err(format!("Database '{}' not found", db)));
        }
        let engine = self.db_engine(db)?;
        let deleted = engine.delete(collection, id)?;
        Ok(Ok(deleted))
    }

    pub fn count(&self, db: &str, collection: &str) -> io::Result<Result<usize, String>> {
        if !self.database_exists(db) {
            return Ok(Err(format!("Database '{}' not found", db)));
        }
        let engine = self.db_engine(db)?;
        Ok(Ok(engine.count(collection)))
    }

    fn update_meta_collections<F: FnOnce(&mut Vec<String>)>(
        &self,
        db: &str,
        f: F,
    ) -> io::Result<()> {
        if let Some(data) = self.meta_engine.get(META_TABLE, db) {
            let mut meta: DbMeta = bincode::deserialize(&data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            f(&mut meta.collections);
            let new_data = bincode::serialize(&meta)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            self.meta_engine.put(META_TABLE, db, &new_data)?;
        }
        Ok(())
    }
}
