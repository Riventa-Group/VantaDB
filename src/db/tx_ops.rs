use serde_json::Value;
use uuid::Uuid;

use super::changefeed::ChangeOp;
use super::database::{apply_update, DatabaseManager};
use super::error::VantaError;
use super::transaction::{TransactionOp, WorkspaceEntry};

impl DatabaseManager {
    // ---- Transaction operations ------------------------------

    pub fn begin_transaction(&self) -> String {
        let version = self.engines.iter().next()
            .map(|e| e.value().clock().current())
            .unwrap_or(0);
        self.tx_manager.begin_at(version)
    }

    pub fn tx_insert(
        &self,
        tx_id: &str,
        db: String,
        collection: String,
        document: Value,
    ) -> Result<(), VantaError> {
        self.require_db(&db)?;
        let store = self.db_engine(&db)?;
        if !store.table_exists(&collection) {
            return Err(VantaError::NotFound {
                entity: "Collection",
                name: collection.clone(),
            });
        }

        if let Some(schema) = self.get_schema_internal(&db, &collection) {
            if let Err(errors) = schema.validate(&document) {
                return Err(VantaError::ValidationFailed { errors });
            }
        }

        let id = if let Some(id_val) = document.get("_id") {
            id_val.as_str().unwrap_or(&Uuid::new_v4().to_string()).to_string()
        } else {
            Uuid::new_v4().to_string()
        };

        let mut doc = document;
        if let Some(o) = doc.as_object_mut() {
            o.insert("_id".to_string(), Value::String(id.clone()));
        }

        self.tx_manager.acquire_lock(tx_id, &collection, &id)?;

        let data = serde_json::to_vec(&doc)?;
        self.tx_manager.workspace_put(
            tx_id,
            &collection,
            &id,
            WorkspaceEntry::Insert(data),
        )?;

        self.tx_manager.add_op(
            tx_id,
            TransactionOp::Insert { db, collection, document: doc },
        )?;

        Ok(())
    }

    pub fn tx_update(
        &self,
        tx_id: &str,
        db: String,
        collection: String,
        id: String,
        patch: Value,
    ) -> Result<(), VantaError> {
        self.require_db(&db)?;

        let current_doc = self.tx_find_by_id(tx_id, &db, &collection, &id)?
            .ok_or_else(|| VantaError::NotFound {
                entity: "Document",
                name: id.clone(),
            })?;

        let mut new_doc = current_doc;
        apply_update(&mut new_doc, &patch)?;

        if let Some(schema) = self.get_schema_internal(&db, &collection) {
            if let Err(errors) = schema.validate(&new_doc) {
                return Err(VantaError::ValidationFailed { errors });
            }
        }

        self.tx_manager.acquire_lock(tx_id, &collection, &id)?;

        let data = serde_json::to_vec(&new_doc)?;
        self.tx_manager.workspace_put(
            tx_id,
            &collection,
            &id,
            WorkspaceEntry::Update(data),
        )?;

        self.tx_manager.add_op(
            tx_id,
            TransactionOp::Update { db, collection, id, patch },
        )?;

        Ok(())
    }

    pub fn tx_delete(
        &self,
        tx_id: &str,
        db: String,
        collection: String,
        id: String,
    ) -> Result<(), VantaError> {
        self.require_db(&db)?;

        self.tx_manager.acquire_lock(tx_id, &collection, &id)?;

        self.tx_manager.workspace_put(
            tx_id,
            &collection,
            &id,
            WorkspaceEntry::Delete,
        )?;

        self.tx_manager.add_op(
            tx_id,
            TransactionOp::Delete { db, collection, id },
        )?;

        Ok(())
    }

    pub fn tx_find_by_id(
        &self,
        tx_id: &str,
        db: &str,
        collection: &str,
        id: &str,
    ) -> Result<Option<Value>, VantaError> {
        self.require_db(db)?;

        if let Some(entry) = self.tx_manager.workspace_get(tx_id, collection, id) {
            return match entry {
                WorkspaceEntry::Insert(data) | WorkspaceEntry::Update(data) => {
                    let doc: Value = serde_json::from_slice(&data)?;
                    Ok(Some(doc))
                }
                WorkspaceEntry::Delete => Ok(None),
            };
        }

        let snap_version = self.tx_manager.get_snapshot_version(tx_id)?;
        let store = self.db_engine(db)?;
        let snap = crate::storage::Snapshot { version: snap_version };

        self.tx_manager.record_read(tx_id, collection, id, snap_version)?;

        match store.get_at(collection, id, snap) {
            Some(data) => {
                let doc: Value = serde_json::from_slice(&data)?;
                Ok(Some(doc))
            }
            None => Ok(None),
        }
    }

    pub fn commit_transaction(&self, tx_id: &str) -> Result<(), VantaError> {
        let tx = self.tx_manager.take(tx_id)?;
        let commit_txn_id = tx.id;

        // Read-set validation (serializable snapshot isolation)
        for (collection, doc_id, version_read) in &tx.read_set {
            let db = tx.ops.iter().find_map(|op| match op {
                TransactionOp::Insert { db, .. }
                | TransactionOp::Update { db, .. }
                | TransactionOp::Delete { db, .. } => Some(db.as_str()),
            });
            if let Some(db) = db {
                if let Ok(store) = self.db_engine(db) {
                    if store.has_write_after(collection, doc_id, *version_read) {
                        self.tx_manager.finalize(&tx);
                        return Err(VantaError::TransactionConflict {
                            tx_id: tx_id.to_string(),
                            reason: format!(
                                "Read-set conflict: {}/{} was modified after snapshot",
                                collection, doc_id
                            ),
                        });
                    }
                }
            }
        }

        // Flush workspace to MVCCStore
        for op in &tx.ops {
            match op {
                TransactionOp::Insert { db, collection, document } => {
                    let store = self.db_engine(db)?;
                    let id = document.get("_id").and_then(|v| v.as_str()).unwrap_or("");
                    let data = serde_json::to_vec(document)?;
                    store.put(collection, id, &data, commit_txn_id)?;

                    let idx_key = Self::idx_key(db, collection);
                    self.index_manager.on_insert(&idx_key, id, document);
                    self.change_feed.emit(db, collection, ChangeOp::Insert, id, Some(document.clone()));
                }
                TransactionOp::Update { db, collection, id, patch: _ } => {
                    let store = self.db_engine(db)?;
                    let ws_key = (collection.clone(), id.clone());
                    if let Some(entry) = tx.workspace.get(&ws_key) {
                        if let WorkspaceEntry::Update(ref data) = entry {
                            let old_doc = store.get_latest(collection, id)
                                .and_then(|d| serde_json::from_slice::<Value>(&d).ok());
                            store.put(collection, id, data, commit_txn_id)?;
                            let new_doc: Value = serde_json::from_slice(data)?;
                            let idx_key = Self::idx_key(db, collection);
                            if let Some(ref old) = old_doc {
                                self.index_manager.on_update(&idx_key, id, old, &new_doc);
                            }
                            self.change_feed.emit(db, collection, ChangeOp::Update, id, Some(new_doc));
                        }
                    }
                }
                TransactionOp::Delete { db, collection, id } => {
                    let store = self.db_engine(db)?;
                    let old_doc = store.get_latest(collection, id)
                        .and_then(|d| serde_json::from_slice::<Value>(&d).ok());
                    store.delete(collection, id, commit_txn_id)?;
                    if let Some(ref doc) = old_doc {
                        let idx_key = Self::idx_key(db, collection);
                        self.index_manager.on_delete(&idx_key, id, doc);
                    }
                    self.change_feed.emit(db, collection, ChangeOp::Delete, id, None);
                }
            }
        }

        self.tx_manager.finalize(&tx);
        Ok(())
    }

    pub fn rollback_transaction(&self, tx_id: &str) -> Result<(), VantaError> {
        self.tx_manager.rollback(tx_id)
    }
}
