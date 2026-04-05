use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// An immutable, lock-free snapshot of a table for maximum read throughput.
/// No locks, no atomics, no contention — just pure HashMap::get().
/// Used for point-in-time reads and read-heavy workloads.
pub struct ReadSnapshot {
    data: HashMap<String, Arc<[u8]>>,
}

impl ReadSnapshot {
    /// Zero-overhead read: returns a reference to the value without any cloning or locking.
    #[inline(always)]
    pub fn get(&self, key: &str) -> Option<&Arc<[u8]>> {
        self.data.get(key)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

/// Core storage engine using sharded concurrent HashMaps (DashMap) with
/// reference-counted values for zero-copy reads.
///
/// Architecture:
/// - Outer DashMap: table name → Arc<DashMap> (table handle)
/// - Inner DashMap: key → Arc<[u8]> (sharded, O(1), lock-free reads across shards)
/// - Arc<[u8]> values: reads return a refcount bump (~5ns) instead of memcpy (~50ns)
/// - Table handles: callers can grab an Arc<DashMap> once and skip the outer lookup
#[derive(Clone)]
pub struct StorageEngine {
    base_path: PathBuf,
    tables: Arc<DashMap<String, Arc<DashMap<String, Arc<[u8]>>>>>,
}

#[derive(Serialize, Deserialize)]
struct TableFile {
    entries: Vec<(String, Vec<u8>)>,
}

impl StorageEngine {
    pub fn open(base_path: &Path) -> io::Result<Self> {
        fs::create_dir_all(base_path)?;
        let engine = Self {
            base_path: base_path.to_path_buf(),
            tables: Arc::new(DashMap::new()),
        };
        engine.load_all()?;
        Ok(engine)
    }

    fn table_path(&self, table: &str) -> PathBuf {
        self.base_path.join(format!("{}.vdb", table))
    }

    fn load_all(&self) -> io::Result<()> {
        if self.base_path.exists() {
            for entry in fs::read_dir(&self.base_path)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("vdb") {
                    let table_name = path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("")
                        .to_string();
                    if let Ok(data) = fs::read(&path) {
                        if let Ok(table_file) = bincode::deserialize::<TableFile>(&data) {
                            let map = DashMap::with_capacity(table_file.entries.len());
                            for (k, v) in table_file.entries {
                                map.insert(k, Arc::from(v.into_boxed_slice()));
                            }
                            self.tables.insert(table_name, Arc::new(map));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn flush_table(&self, table: &str) -> io::Result<()> {
        if let Some(map_ref) = self.tables.get(table) {
            let entries: Vec<(String, Vec<u8>)> = map_ref
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().to_vec()))
                .collect();
            let table_file = TableFile { entries };
            let data = bincode::serialize(&table_file)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            fs::write(self.table_path(table), data)?;
        }
        Ok(())
    }

    fn get_or_create_table(&self, table: &str) -> Arc<DashMap<String, Arc<[u8]>>> {
        if let Some(t) = self.tables.get(table) {
            Arc::clone(t.value())
        } else {
            let map = Arc::new(DashMap::new());
            self.tables.insert(table.to_string(), Arc::clone(&map));
            map
        }
    }

    pub fn put(&self, table: &str, key: &str, value: &[u8]) -> io::Result<()> {
        let t = self.get_or_create_table(table);
        t.insert(key.to_string(), Arc::from(value));
        self.flush_table(table)
    }

    pub fn get(&self, table: &str, key: &str) -> Option<Vec<u8>> {
        let t = self.tables.get(table)?;
        t.get(key).map(|v| v.value().to_vec())
    }

    pub fn delete(&self, table: &str, key: &str) -> io::Result<bool> {
        let existed = self
            .tables
            .get(table)
            .map(|map| map.remove(key).is_some())
            .unwrap_or(false);
        if existed {
            self.flush_table(table)?;
        }
        Ok(existed)
    }

    pub fn list_keys(&self, table: &str) -> Vec<String> {
        self.tables
            .get(table)
            .map(|map| map.iter().map(|e| e.key().clone()).collect())
            .unwrap_or_default()
    }

    pub fn table_exists(&self, table: &str) -> bool {
        self.tables.contains_key(table)
    }

    pub fn create_table(&self, table: &str) -> io::Result<()> {
        if !self.tables.contains_key(table) {
            self.tables.insert(table.to_string(), Arc::new(DashMap::new()));
        }
        self.flush_table(table)
    }

    /// Create a table pre-allocated for `capacity` entries (avoids DashMap resizing).
    pub fn create_table_with_capacity(&self, table: &str, capacity: usize) -> io::Result<()> {
        if !self.tables.contains_key(table) {
            self.tables
                .insert(table.to_string(), Arc::new(DashMap::with_capacity(capacity)));
        }
        self.flush_table(table)
    }

    pub fn drop_table(&self, table: &str) -> io::Result<bool> {
        let existed = self.tables.remove(table).is_some();
        if existed {
            let path = self.table_path(table);
            if path.exists() {
                fs::remove_file(path)?;
            }
        }
        Ok(existed)
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.tables.iter().map(|e| e.key().clone()).collect()
    }

    pub fn count(&self, table: &str) -> usize {
        self.tables.get(table).map(|m| m.len()).unwrap_or(0)
    }

    // ── High-performance read/write API ─────────────────────────

    /// Create a lock-free read snapshot of a table.
    /// The snapshot is an immutable HashMap — perfect for read-heavy workloads
    /// where point-in-time consistency is acceptable.
    pub fn snapshot(&self, table: &str) -> Option<ReadSnapshot> {
        let t = self.tables.get(table)?;
        let mut data = HashMap::with_capacity(t.len());
        for entry in t.iter() {
            data.insert(entry.key().clone(), Arc::clone(entry.value()));
        }
        Some(ReadSnapshot { data })
    }

    /// Get a direct handle to a table's DashMap, bypassing the outer lookup
    /// on every subsequent operation. This is the key to maximum throughput.
    #[inline(always)]
    pub fn table_handle(&self, table: &str) -> Option<Arc<DashMap<String, Arc<[u8]>>>> {
        self.tables.get(table).map(|t| Arc::clone(t.value()))
    }

    /// Write to memory only (no disk flush).
    #[inline(always)]
    pub fn put_memory_only(&self, table: &str, key: &str, value: &[u8]) {
        let t = self.get_or_create_table(table);
        t.insert(key.to_string(), Arc::from(value));
    }

    /// Delete from memory only (no disk flush).
    #[inline(always)]
    pub fn delete_memory_only(&self, table: &str, key: &str) -> bool {
        self.tables
            .get(table)
            .map(|map| map.remove(key).is_some())
            .unwrap_or(false)
    }

    /// Batch write to memory then single flush.
    pub fn put_batch(&self, table: &str, entries: &[(String, Vec<u8>)]) -> io::Result<()> {
        let t = self.get_or_create_table(table);
        for (k, v) in entries {
            t.insert(k.clone(), Arc::from(v.as_slice()));
        }
        self.flush_table(table)
    }
}
