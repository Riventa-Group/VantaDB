use ahash::RandomState;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

// ---- Index definitions ------------------------------------

/// The type of index backing structure.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum IndexType {
    BTree, // ordered, supports equality + range
    Hash,  // O(1) equality only ($eq, $in)
}

impl Default for IndexType {
    fn default() -> Self {
        IndexType::BTree
    }
}

/// Metadata for a single index (persisted).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub collection: String,
    pub field: String,
    pub unique: bool,
    #[serde(default)]
    pub index_type: IndexType,
}

// ---- Index trait ------------------------------------------

/// Common interface for all index types.
pub trait Index: Send + Sync {
    fn def(&self) -> &IndexDef;
    fn field(&self) -> &str;
    fn index_type(&self) -> &IndexType;

    /// Exact equality lookup: returns matching doc IDs.
    fn lookup_eq(&self, value: &Value) -> Vec<String>;

    /// Range lookup (BTree only). Hash indexes return empty.
    fn lookup_range(
        &self,
        start: Option<&Value>,
        end: Option<&Value>,
        include_start: bool,
        include_end: bool,
    ) -> Vec<String>;

    /// Check if an operator is supported by this index type.
    fn supports_op(&self, op: &str) -> bool;

    /// Check uniqueness constraint.
    fn check_unique(&self, value: &Value, exclude_id: Option<&str>) -> bool;

    /// Build from a set of documents.
    fn build_from(&self, docs: &[(String, Value)]);

    /// Insert a single document.
    fn insert(&self, id: &str, doc: &Value);

    /// Remove a single document.
    fn remove(&self, id: &str, doc: &Value);
}

// ---- B-tree index -----------------------------------------

/// An in-memory B-tree index on a single field.
pub struct BTreeIndex {
    pub definition: IndexDef,
    tree: RwLock<BTreeMap<String, Vec<String>>>,
}

impl BTreeIndex {
    pub fn new(def: IndexDef) -> Self {
        Self {
            definition: def,
            tree: RwLock::new(BTreeMap::new()),
        }
    }
}

impl Index for BTreeIndex {
    fn def(&self) -> &IndexDef {
        &self.definition
    }

    fn field(&self) -> &str {
        &self.definition.field
    }

    fn index_type(&self) -> &IndexType {
        &IndexType::BTree
    }

    fn supports_op(&self, op: &str) -> bool {
        matches!(
            op,
            "$eq" | "$gt" | "$gte" | "$lt" | "$lte" | "$in" | "$nin"
        )
    }

    fn lookup_eq(&self, value: &Value) -> Vec<String> {
        let key = value_to_index_key(value);
        self.tree.read().get(&key).cloned().unwrap_or_default()
    }

    fn lookup_range(
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

    fn check_unique(&self, value: &Value, exclude_id: Option<&str>) -> bool {
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

    fn build_from(&self, docs: &[(String, Value)]) {
        let mut tree = self.tree.write();
        tree.clear();
        for (id, doc) in docs {
            if let Some(val) = resolve_field(doc, &self.definition.field) {
                let key = value_to_index_key(val);
                tree.entry(key).or_default().push(id.clone());
            }
        }
    }

    fn insert(&self, id: &str, doc: &Value) {
        if let Some(val) = resolve_field(doc, &self.definition.field) {
            let key = value_to_index_key(val);
            self.tree
                .write()
                .entry(key)
                .or_default()
                .push(id.to_string());
        }
    }

    fn remove(&self, id: &str, doc: &Value) {
        if let Some(val) = resolve_field(doc, &self.definition.field) {
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
}

// ---- Hash index -------------------------------------------

/// An in-memory hash index for O(1) equality lookups.
pub struct HashIndex {
    pub definition: IndexDef,
    map: DashMap<String, Vec<String>, RandomState>,
}

impl HashIndex {
    pub fn new(def: IndexDef) -> Self {
        Self {
            definition: def,
            map: DashMap::with_hasher(RandomState::new()),
        }
    }
}

impl Index for HashIndex {
    fn def(&self) -> &IndexDef {
        &self.definition
    }

    fn field(&self) -> &str {
        &self.definition.field
    }

    fn index_type(&self) -> &IndexType {
        &IndexType::Hash
    }

    fn supports_op(&self, op: &str) -> bool {
        matches!(op, "$eq" | "$in" | "$nin")
    }

    fn lookup_eq(&self, value: &Value) -> Vec<String> {
        let key = value_to_index_key(value);
        self.map.get(&key).map(|v| v.clone()).unwrap_or_default()
    }

    fn lookup_range(
        &self,
        _start: Option<&Value>,
        _end: Option<&Value>,
        _include_start: bool,
        _include_end: bool,
    ) -> Vec<String> {
        // Hash indexes do not support range queries
        Vec::new()
    }

    fn check_unique(&self, value: &Value, exclude_id: Option<&str>) -> bool {
        let key = value_to_index_key(value);
        match self.map.get(&key) {
            None => true,
            Some(ids) => match exclude_id {
                Some(eid) => ids.iter().all(|id| id != eid),
                None => ids.is_empty(),
            },
        }
    }

    fn build_from(&self, docs: &[(String, Value)]) {
        self.map.clear();
        for (id, doc) in docs {
            if let Some(val) = resolve_field(doc, &self.definition.field) {
                let key = value_to_index_key(val);
                self.map.entry(key).or_default().push(id.clone());
            }
        }
    }

    fn insert(&self, id: &str, doc: &Value) {
        if let Some(val) = resolve_field(doc, &self.definition.field) {
            let key = value_to_index_key(val);
            self.map.entry(key).or_default().push(id.to_string());
        }
    }

    fn remove(&self, id: &str, doc: &Value) {
        if let Some(val) = resolve_field(doc, &self.definition.field) {
            let key = value_to_index_key(val);
            if let Some(mut ids) = self.map.get_mut(&key) {
                ids.retain(|i| i != id);
            }
            // Clean up empty entries
            self.map.remove_if(&value_to_index_key(val), |_, ids| ids.is_empty());
        }
    }
}

// ---- Helpers ----------------------------------------------

pub fn resolve_field<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = doc;
    for part in path.split('.') {
        current = current.get(part)?;
    }
    Some(current)
}

pub fn value_to_index_key(v: &Value) -> String {
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

/// Create an index implementation based on the IndexDef's type.
pub fn create_index(def: IndexDef) -> Arc<dyn Index> {
    match def.index_type {
        IndexType::BTree => Arc::new(BTreeIndex::new(def)),
        IndexType::Hash => Arc::new(HashIndex::new(def)),
    }
}

// ---- Index Manager ----------------------------------------

/// Manages all indexes across all databases/collections.
pub struct IndexManager {
    /// key = "db/collection" -> field -> dyn Index
    indexes: RwLock<HashMap<String, HashMap<String, Arc<dyn Index>>>>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            indexes: RwLock::new(HashMap::new()),
        }
    }

    pub fn add_index(
        &self,
        key: &str,
        def: IndexDef,
        docs: &[(String, Value)],
    ) -> Arc<dyn Index> {
        let idx = create_index(def.clone());
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

    pub fn get_index(&self, key: &str, field: &str) -> Option<Arc<dyn Index>> {
        self.indexes.read().get(key)?.get(field).cloned()
    }

    /// Get all indexes for a collection. Used by the query planner.
    pub fn get_all_indexes(&self, key: &str) -> Vec<Arc<dyn Index>> {
        self.indexes
            .read()
            .get(key)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn list_indexes(&self, key: &str) -> Vec<IndexDef> {
        self.indexes
            .read()
            .get(key)
            .map(|m| m.values().map(|idx| idx.def().clone()).collect())
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

// ---- Tests ------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_docs() -> Vec<(String, Value)> {
        vec![
            ("1".into(), json!({"_id": "1", "name": "Alice", "age": 30})),
            ("2".into(), json!({"_id": "2", "name": "Bob", "age": 25})),
            ("3".into(), json!({"_id": "3", "name": "Charlie", "age": 35})),
            ("4".into(), json!({"_id": "4", "name": "Alice", "age": 28})),
        ]
    }

    #[test]
    fn test_btree_index_eq_lookup() {
        let def = IndexDef {
            collection: "test/users".into(),
            field: "name".into(),
            unique: false,
            index_type: IndexType::BTree,
        };
        let idx = BTreeIndex::new(def);
        idx.build_from(&sample_docs());

        let results = idx.lookup_eq(&json!("Alice"));
        assert_eq!(results.len(), 2);
        assert!(results.contains(&"1".to_string()));
        assert!(results.contains(&"4".to_string()));

        let results = idx.lookup_eq(&json!("Bob"));
        assert_eq!(results, vec!["2".to_string()]);

        let results = idx.lookup_eq(&json!("Nobody"));
        assert!(results.is_empty());
    }

    #[test]
    fn test_btree_index_range_lookup() {
        let def = IndexDef {
            collection: "test/users".into(),
            field: "age".into(),
            unique: false,
            index_type: IndexType::BTree,
        };
        let idx = BTreeIndex::new(def);
        idx.build_from(&sample_docs());

        // age > 28 (should get 30 and 35)
        let results = idx.lookup_range(Some(&json!(28)), None, false, false);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_hash_index_eq_lookup() {
        let def = IndexDef {
            collection: "test/users".into(),
            field: "name".into(),
            unique: false,
            index_type: IndexType::Hash,
        };
        let idx = HashIndex::new(def);
        idx.build_from(&sample_docs());

        let results = idx.lookup_eq(&json!("Alice"));
        assert_eq!(results.len(), 2);

        // Hash index does not support range
        let results = idx.lookup_range(Some(&json!(0)), Some(&json!(100)), true, true);
        assert!(results.is_empty());
    }

    #[test]
    fn test_hash_index_supports_op() {
        let def = IndexDef {
            collection: "test/users".into(),
            field: "name".into(),
            unique: false,
            index_type: IndexType::Hash,
        };
        let idx = HashIndex::new(def);
        assert!(idx.supports_op("$eq"));
        assert!(idx.supports_op("$in"));
        assert!(!idx.supports_op("$gt"));
        assert!(!idx.supports_op("$lt"));
    }

    #[test]
    fn test_unique_constraint() {
        let def = IndexDef {
            collection: "test/users".into(),
            field: "name".into(),
            unique: true,
            index_type: IndexType::BTree,
        };
        let idx = BTreeIndex::new(def);
        idx.build_from(&sample_docs());

        // "Alice" exists with ids 1 and 4
        assert!(!idx.check_unique(&json!("Alice"), None));
        // Excluding id "1", "Alice" still exists (id "4")
        assert!(!idx.check_unique(&json!("Alice"), Some("1")));
        // "NewName" doesn't exist
        assert!(idx.check_unique(&json!("NewName"), None));
    }

    #[test]
    fn test_insert_and_remove() {
        let def = IndexDef {
            collection: "test/users".into(),
            field: "name".into(),
            unique: false,
            index_type: IndexType::BTree,
        };
        let idx = BTreeIndex::new(def);
        idx.build_from(&[]);

        let doc = json!({"_id": "5", "name": "Dave"});
        idx.insert("5", &doc);
        assert_eq!(idx.lookup_eq(&json!("Dave")), vec!["5".to_string()]);

        idx.remove("5", &doc);
        assert!(idx.lookup_eq(&json!("Dave")).is_empty());
    }

    #[test]
    fn test_index_manager_lifecycle() {
        let mgr = IndexManager::new();
        let def = IndexDef {
            collection: "test/users".into(),
            field: "name".into(),
            unique: false,
            index_type: IndexType::BTree,
        };

        mgr.add_index("test/users", def, &sample_docs());

        let indexes = mgr.get_all_indexes("test/users");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].field(), "name");

        let results = indexes[0].lookup_eq(&json!("Alice"));
        assert_eq!(results.len(), 2);

        // Test on_insert
        let new_doc = json!({"_id": "5", "name": "Alice"});
        mgr.on_insert("test/users", "5", &new_doc);
        let results = indexes[0].lookup_eq(&json!("Alice"));
        assert_eq!(results.len(), 3);

        // Test on_delete
        mgr.on_delete("test/users", "5", &new_doc);
        let results = indexes[0].lookup_eq(&json!("Alice"));
        assert_eq!(results.len(), 2);

        // Drop
        mgr.drop_collection_indexes("test/users");
        assert!(mgr.get_all_indexes("test/users").is_empty());
    }
}
