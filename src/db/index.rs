use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// Metadata for a single index (persisted).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub collection: String,
    pub field: String,
    pub unique: bool,
}

/// An in-memory B-tree index on a single field.
/// Maps value_key -> list of document IDs.
pub struct FieldIndex {
    pub def: IndexDef,
    tree: RwLock<BTreeMap<String, Vec<String>>>,
}

impl FieldIndex {
    pub fn new(def: IndexDef) -> Self {
        Self {
            def,
            tree: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn build_from(&self, docs: &[(String, Value)]) {
        let mut tree = self.tree.write();
        tree.clear();
        for (id, doc) in docs {
            if let Some(val) = resolve_field(doc, &self.def.field) {
                let key = value_to_index_key(val);
                tree.entry(key).or_default().push(id.clone());
            }
        }
    }

    pub fn insert(&self, id: &str, doc: &Value) {
        if let Some(val) = resolve_field(doc, &self.def.field) {
            let key = value_to_index_key(val);
            self.tree.write().entry(key).or_default().push(id.to_string());
        }
    }

    pub fn remove(&self, id: &str, doc: &Value) {
        if let Some(val) = resolve_field(doc, &self.def.field) {
            let key = value_to_index_key(val);
            let mut tree = self.tree.write();
            if let Some(ids) = tree.get_mut(&key) {
                ids.retain(|i| i != id);
                if ids.is_empty() {
                    tree.remove(&key);
                }
            }
        }
    }

    pub fn lookup_eq(&self, value: &Value) -> Vec<String> {
        let key = value_to_index_key(value);
        self.tree.read().get(&key).cloned().unwrap_or_default()
    }

    pub fn lookup_range(
        &self,
        start: Option<&Value>,
        end: Option<&Value>,
        include_start: bool,
        include_end: bool,
    ) -> Vec<String> {
        use std::ops::Bound;
        let tree = self.tree.read();
        let lo = match start {
            Some(v) => {
                let k = value_to_index_key(v);
                if include_start {
                    Bound::Included(k)
                } else {
                    Bound::Excluded(k)
                }
            }
            None => Bound::Unbounded,
        };
        let hi = match end {
            Some(v) => {
                let k = value_to_index_key(v);
                if include_end {
                    Bound::Included(k)
                } else {
                    Bound::Excluded(k)
                }
            }
            None => Bound::Unbounded,
        };
        tree.range((lo, hi))
            .flat_map(|(_, ids)| ids.clone())
            .collect()
    }

    pub fn check_unique(&self, value: &Value, exclude_id: Option<&str>) -> bool {
        let key = value_to_index_key(value);
        let tree = self.tree.read();
        match tree.get(&key) {
            None => true,
            Some(ids) => match exclude_id {
                Some(eid) => ids.iter().all(|id| id != eid),
                None => ids.is_empty(),
            },
        }
    }
}

fn resolve_field<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = doc;
    for part in path.split('.') {
        current = current.get(part)?;
    }
    Some(current)
}

fn value_to_index_key(v: &Value) -> String {
    match v {
        Value::Null => "0:null".into(),
        Value::Bool(b) => format!("1:{}", if *b { "1" } else { "0" }),
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                format!("2:{:+020.6}", f)
            } else {
                format!("2:{}", n)
            }
        }
        Value::String(s) => format!("3:{}", s),
        _ => format!("9:{}", v),
    }
}

/// Manages all indexes across all databases/collections.
pub struct IndexManager {
    /// key = "db/collection" -> field -> FieldIndex
    indexes: RwLock<HashMap<String, HashMap<String, Arc<FieldIndex>>>>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            indexes: RwLock::new(HashMap::new()),
        }
    }

    pub fn add_index(&self, key: &str, def: IndexDef, docs: &[(String, Value)]) -> Arc<FieldIndex> {
        let idx = Arc::new(FieldIndex::new(def.clone()));
        idx.build_from(docs);
        self.indexes
            .write()
            .entry(key.to_string())
            .or_default()
            .insert(def.field.clone(), Arc::clone(&idx));
        idx
    }

    pub fn remove_index(&self, key: &str, field: &str) -> bool {
        let mut indexes = self.indexes.write();
        if let Some(col) = indexes.get_mut(key) {
            col.remove(field).is_some()
        } else {
            false
        }
    }

    pub fn get_index(&self, key: &str, field: &str) -> Option<Arc<FieldIndex>> {
        self.indexes.read().get(key)?.get(field).cloned()
    }

    pub fn list_indexes(&self, key: &str) -> Vec<IndexDef> {
        self.indexes
            .read()
            .get(key)
            .map(|m| m.values().map(|idx| idx.def.clone()).collect())
            .unwrap_or_default()
    }

    pub fn on_insert(&self, key: &str, id: &str, doc: &Value) {
        if let Some(col) = self.indexes.read().get(key) {
            for idx in col.values() {
                idx.insert(id, doc);
            }
        }
    }

    pub fn on_delete(&self, key: &str, id: &str, doc: &Value) {
        if let Some(col) = self.indexes.read().get(key) {
            for idx in col.values() {
                idx.remove(id, doc);
            }
        }
    }

    pub fn on_update(&self, key: &str, id: &str, old_doc: &Value, new_doc: &Value) {
        if let Some(col) = self.indexes.read().get(key) {
            for idx in col.values() {
                idx.remove(id, old_doc);
                idx.insert(id, new_doc);
            }
        }
    }

    pub fn drop_collection_indexes(&self, key: &str) {
        self.indexes.write().remove(key);
    }
}
