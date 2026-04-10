use ahash::RandomState;
use std::collections::HashMap;

use super::record::EntryPointer;

/// Trait for an in-memory index mapping document keys to their on-disk location.
///
/// Designed for single-threaded use on a monoio core thread.
pub trait KeyIndex {
    fn get(&self, key: &str) -> Option<EntryPointer>;
    fn insert(&mut self, key: String, ptr: EntryPointer) -> Option<EntryPointer>;
    fn remove(&mut self, key: &str) -> Option<EntryPointer>;
    fn contains(&self, key: &str) -> bool;
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (&str, &EntryPointer)> + '_>;
}

/// Hash-map-backed key index using ahash for fast hashing.
pub struct MemoryKeyIndex {
    map: HashMap<String, EntryPointer, RandomState>,
}

impl MemoryKeyIndex {
    pub fn new() -> Self {
        Self {
            map: HashMap::with_hasher(RandomState::new()),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity_and_hasher(capacity, RandomState::new()),
        }
    }
}

impl KeyIndex for MemoryKeyIndex {
    fn get(&self, key: &str) -> Option<EntryPointer> {
        self.map.get(key).copied()
    }

    fn insert(&mut self, key: String, ptr: EntryPointer) -> Option<EntryPointer> {
        self.map.insert(key, ptr)
    }

    fn remove(&mut self, key: &str) -> Option<EntryPointer> {
        self.map.remove(key)
    }

    fn contains(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (&str, &EntryPointer)> + '_> {
        Box::new(self.map.iter().map(|(k, v)| (k.as_str(), v)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ptr(segment_id: u32, offset: u64, value_len: u32, lsn: u64) -> EntryPointer {
        EntryPointer {
            lsn,
            offset,
            segment_id,
            value_len,
        }
    }

    #[test]
    fn test_insert_and_get() {
        let mut idx = MemoryKeyIndex::new();
        let p = ptr(1, 0, 100, 1);
        assert!(idx.insert("doc1".into(), p).is_none());
        assert_eq!(idx.get("doc1"), Some(p));
        assert_eq!(idx.get("missing"), None);
    }

    #[test]
    fn test_overwrite_returns_old() {
        let mut idx = MemoryKeyIndex::new();
        let p1 = ptr(1, 0, 100, 1);
        let p2 = ptr(2, 512, 200, 2);
        idx.insert("key".into(), p1);
        let old = idx.insert("key".into(), p2);
        assert_eq!(old, Some(p1));
        assert_eq!(idx.get("key"), Some(p2));
    }

    #[test]
    fn test_remove() {
        let mut idx = MemoryKeyIndex::new();
        let p = ptr(1, 0, 100, 1);
        idx.insert("doc".into(), p);
        assert_eq!(idx.remove("doc"), Some(p));
        assert!(!idx.contains("doc"));
        assert_eq!(idx.remove("doc"), None);
    }

    #[test]
    fn test_len_and_iter() {
        let mut idx = MemoryKeyIndex::new();
        idx.insert("a".into(), ptr(1, 0, 10, 1));
        idx.insert("b".into(), ptr(1, 100, 20, 2));
        idx.insert("c".into(), ptr(2, 0, 30, 3));
        assert_eq!(idx.len(), 3);
        assert!(!idx.is_empty());

        let mut keys: Vec<&str> = idx.iter().map(|(k, _)| k).collect();
        keys.sort();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_with_capacity() {
        let idx = MemoryKeyIndex::with_capacity(128);
        assert!(idx.is_empty());
        assert_eq!(idx.len(), 0);
    }
}
