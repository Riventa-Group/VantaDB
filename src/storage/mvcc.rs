use ahash::RandomState;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

use super::StorageEngine;

// ---- Version Clock ------------------------------------------

/// Global monotonic version clock for MVCC.
/// Every committed write advances the clock, producing a unique version number.
pub struct VersionClock {
    current: AtomicU64,
}

impl VersionClock {
    pub fn new() -> Self {
        Self {
            current: AtomicU64::new(1),
        }
    }

    /// The latest committed version.
    pub fn current(&self) -> u64 {
        self.current.load(Ordering::Acquire)
    }

    /// Advance the clock and return the new version number.
    pub fn advance(&self) -> u64 {
        self.current.fetch_add(1, Ordering::AcqRel) + 1
    }
}

// ---- MVCC Value ---------------------------------------------

/// A single version of a value in the MVCC store.
/// Version chains are ordered newest-first for fast resolution.
#[derive(Clone)]
pub struct MVCCValue {
    pub data: Arc<[u8]>,
    /// Transaction version that created this version.
    pub created_at: u64,
    /// Transaction version that logically deleted this (None = live).
    pub deleted_at: Option<u64>,
    /// ID of the transaction that created this version.
    pub txn_id: Uuid,
}

// ---- Snapshot -----------------------------------------------

/// An MVCC snapshot — reads see exactly the state as of `version`.
#[derive(Debug, Clone, Copy)]
pub struct Snapshot {
    pub version: u64,
}

// ---- Active Snapshots Tracker -------------------------------

/// Tracks the set of active snapshot versions so GC knows what's still readable.
struct ActiveSnapshots {
    versions: Mutex<BTreeSet<u64>>,
}

impl ActiveSnapshots {
    fn new() -> Self {
        Self {
            versions: Mutex::new(BTreeSet::new()),
        }
    }

    fn register(&self, version: u64) {
        self.versions.lock().insert(version);
    }

    fn release(&self, version: u64) {
        self.versions.lock().remove(&version);
    }

    /// The oldest snapshot version still in use (None if no active snapshots).
    fn oldest(&self) -> Option<u64> {
        self.versions.lock().iter().next().copied()
    }
}

// ---- MVCC Store ---------------------------------------------

/// MVCC storage layer on top of StorageEngine.
///
/// Each key holds a version chain: `Vec<MVCCValue>` ordered newest-first.
/// Reads resolve the correct version by finding the latest `created_at <= snapshot_version`
/// where `deleted_at` is either None or `> snapshot_version`.
///
/// The underlying StorageEngine handles WAL persistence for the latest committed
/// state. The MVCC layer adds multi-version reads in memory.
pub struct MVCCStore {
    engine: Arc<StorageEngine>,
    /// (table, key) -> version chain (newest first)
    chains: DashMap<(String, String), Vec<MVCCValue>, RandomState>,
    /// Global version clock
    clock: Arc<VersionClock>,
    /// Tracks active snapshots for GC
    active_snapshots: Arc<ActiveSnapshots>,
    /// GC watermark — versions below this with a newer replacement can be pruned
    gc_watermark: AtomicU64,
}

impl MVCCStore {
    /// Create a new MVCC store wrapping an existing StorageEngine.
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self {
            engine,
            chains: DashMap::with_hasher(RandomState::new()),
            clock: Arc::new(VersionClock::new()),
            active_snapshots: Arc::new(ActiveSnapshots::new()),
            gc_watermark: AtomicU64::new(0),
        }
    }

    /// Access the version clock.
    pub fn clock(&self) -> &VersionClock {
        &self.clock
    }

    /// Access the underlying StorageEngine (for table DDL, compaction, etc.).
    pub fn engine(&self) -> &StorageEngine {
        &self.engine
    }

    /// Compact all tables in the underlying StorageEngine.
    /// Writes fresh .vdb snapshots and truncates WALs.
    pub fn compact_all(&self) -> io::Result<usize> {
        let tables = self.engine.list_tables();
        let mut compacted = 0;
        for table in &tables {
            self.engine.compact(table)?;
            compacted += 1;
        }
        Ok(compacted)
    }

    // ---- Delegated table operations ----------------------------

    pub fn table_exists(&self, table: &str) -> bool {
        self.engine.table_exists(table)
    }

    pub fn create_table(&self, table: &str) -> io::Result<()> {
        self.engine.create_table(table)
    }

    pub fn drop_table(&self, table: &str) -> io::Result<bool> {
        // Remove all MVCC chains for this table
        self.chains.retain(|(t, _), _| t != table);
        self.engine.drop_table(table)
    }

    pub fn list_tables(&self) -> Vec<String> {
        self.engine.list_tables()
    }

    // ---- Latest-version convenience methods --------------------

    /// List all live keys in a table at the latest version (no snapshot needed).
    pub fn list_keys_latest(&self, table: &str) -> Vec<String> {
        let version = self.clock.current();
        let prefix = table.to_string();
        let mut keys = Vec::new();
        for entry in self.chains.iter() {
            let (ref t, ref k) = *entry.key();
            if t == &prefix {
                if resolve_version(entry.value(), version).is_some() {
                    keys.push(k.clone());
                }
            }
        }
        keys
    }

    /// Check if a key has any version created after the given version.
    /// Used for read-set validation at commit time (serializable isolation).
    pub fn has_write_after(&self, table: &str, key: &str, after_version: u64) -> bool {
        let chain_key = (table.to_string(), key.to_string());
        if let Some(chain) = self.chains.get(&chain_key) {
            for mv in chain.iter() {
                if mv.created_at > after_version {
                    return true;
                }
            }
        }
        false
    }

    /// Count live keys in a table at the latest version.
    pub fn count_latest(&self, table: &str) -> usize {
        let version = self.clock.current();
        let prefix = table.to_string();
        let mut count = 0;
        for entry in self.chains.iter() {
            let (ref t, _) = *entry.key();
            if t == &prefix {
                if resolve_version(entry.value(), version).is_some() {
                    count += 1;
                }
            }
        }
        count
    }

    /// Take a consistent snapshot at the current version.
    pub fn snapshot(&self) -> Snapshot {
        let version = self.clock.current();
        self.active_snapshots.register(version);
        Snapshot { version }
    }

    /// Release a snapshot, allowing GC to reclaim versions below it.
    pub fn release_snapshot(&self, snap: Snapshot) {
        self.active_snapshots.release(snap.version);
    }

    // ---- Versioned writes ------------------------------------

    /// Write a new version of a key. Returns the version number assigned.
    /// Also persists the latest value to the underlying StorageEngine.
    pub fn put(
        &self,
        table: &str,
        key: &str,
        value: &[u8],
        txn_id: Uuid,
    ) -> io::Result<u64> {
        let version = self.clock.advance();
        let mv = MVCCValue {
            data: Arc::from(value),
            created_at: version,
            deleted_at: None,
            txn_id,
        };

        let chain_key = (table.to_string(), key.to_string());
        let mut chain = self.chains.entry(chain_key).or_insert_with(Vec::new);
        // Insert at front (newest first)
        chain.insert(0, mv);

        // Persist latest to storage engine
        self.engine.put(table, key, value)?;
        Ok(version)
    }

    /// Logically delete a key at a new version.
    /// The value remains readable by older snapshots until GC prunes it.
    pub fn delete(
        &self,
        table: &str,
        key: &str,
        txn_id: Uuid,
    ) -> io::Result<Option<u64>> {
        let version = self.clock.advance();
        let chain_key = (table.to_string(), key.to_string());

        let mut chain = match self.chains.get_mut(&chain_key) {
            Some(c) => c,
            None => return Ok(None), // key doesn't exist
        };

        // Mark the latest live version as deleted
        let mut found = false;
        for mv in chain.iter_mut() {
            if mv.deleted_at.is_none() {
                mv.deleted_at = Some(version);
                found = true;
                break;
            }
        }

        if !found {
            return Ok(None); // already deleted in all versions
        }

        // Add a tombstone version so snapshot reads know about the deletion
        chain.insert(
            0,
            MVCCValue {
                data: Arc::from(&[] as &[u8]),
                created_at: version,
                deleted_at: Some(version), // born deleted = tombstone
                txn_id,
            },
        );

        // Persist deletion to storage engine
        self.engine.delete(table, key)?;
        Ok(Some(version))
    }

    // ---- Versioned reads ------------------------------------

    /// Read a key at a specific snapshot version.
    /// Finds the latest version where `created_at <= snap.version`
    /// and (`deleted_at` is None or `deleted_at > snap.version`).
    pub fn get_at(&self, table: &str, key: &str, snap: Snapshot) -> Option<Arc<[u8]>> {
        let chain_key = (table.to_string(), key.to_string());
        let chain = self.chains.get(&chain_key)?;
        resolve_version(&chain, snap.version)
    }

    /// Read the latest committed version of a key (no snapshot isolation).
    pub fn get_latest(&self, table: &str, key: &str) -> Option<Arc<[u8]>> {
        let chain_key = (table.to_string(), key.to_string());
        let chain = self.chains.get(&chain_key)?;
        let version = self.clock.current();
        resolve_version(&chain, version)
    }

    /// List all live keys in a table at a specific snapshot version.
    pub fn list_keys_at(&self, table: &str, snap: Snapshot) -> Vec<String> {
        let prefix = table.to_string();
        let mut keys = Vec::new();
        for entry in self.chains.iter() {
            let (ref t, ref k) = *entry.key();
            if t == &prefix {
                if resolve_version(entry.value(), snap.version).is_some() {
                    keys.push(k.clone());
                }
            }
        }
        keys
    }

    /// Count live keys in a table at a specific snapshot version.
    pub fn count_at(&self, table: &str, snap: Snapshot) -> usize {
        let prefix = table.to_string();
        let mut count = 0;
        for entry in self.chains.iter() {
            let (ref t, _) = *entry.key();
            if t == &prefix {
                if resolve_version(entry.value(), snap.version).is_some() {
                    count += 1;
                }
            }
        }
        count
    }

    // ---- Bulk load from StorageEngine -----------------------

    /// Load all keys from a StorageEngine table into the MVCC store
    /// as version 0 entries (pre-existing data before MVCC was enabled).
    pub fn load_from_engine(&self, table: &str) {
        let version = self.clock.current();
        let keys = self.engine.list_keys(table);
        let bootstrap_txn = Uuid::nil();

        for key in keys {
            if let Some(data) = self.engine.get(table, &key) {
                let chain_key = (table.to_string(), key);
                let mv = MVCCValue {
                    data,
                    created_at: version,
                    deleted_at: None,
                    txn_id: bootstrap_txn,
                };
                self.chains
                    .entry(chain_key)
                    .or_insert_with(Vec::new)
                    .push(mv);
            }
        }
    }

    // ---- Garbage Collection ---------------------------------

    /// Run garbage collection: prune version chains of entries that no active
    /// snapshot can ever read.
    ///
    /// A version can be pruned if:
    /// 1. It has been superseded by a newer version, AND
    /// 2. Its `created_at` is below the oldest active snapshot version
    ///    (meaning no snapshot will ever resolve to it).
    ///
    /// Returns the number of versions pruned.
    pub fn gc(&self) -> usize {
        // If no active snapshots, use current + 1 so all versions are below
        // the watermark and only the latest live version is preserved.
        let oldest_snap = self
            .active_snapshots
            .oldest()
            .unwrap_or_else(|| self.clock.current() + 1);

        self.gc_watermark.store(oldest_snap, Ordering::Release);

        let mut pruned = 0;

        for mut entry in self.chains.iter_mut() {
            let chain = entry.value_mut();
            if chain.len() <= 1 {
                continue; // nothing to prune — keep at least the latest version
            }

            let before = chain.len();

            // Keep versions that might still be visible to some snapshot.
            // A version is needed if:
            //   - It's the latest live version (deleted_at is None), OR
            //   - created_at >= oldest_snap (a snapshot might read it), OR
            //   - It's the most recent version with created_at < oldest_snap
            //     (needed as the "base" for the oldest snapshot)
            let mut keep = vec![true; chain.len()];
            let mut found_base = false;

            // chain is newest-first, so iterate forward
            for i in 0..chain.len() {
                let mv = &chain[i];

                if mv.created_at >= oldest_snap {
                    // Active range — keep
                    continue;
                }

                // Below oldest_snap
                if !found_base && mv.deleted_at.map_or(true, |d| d >= oldest_snap) {
                    // This is the base version the oldest snapshot would read — keep it
                    found_base = true;
                    continue;
                }

                // Below oldest_snap and superseded — safe to prune
                if found_base {
                    keep[i] = false;
                }
            }

            let mut idx = 0;
            chain.retain(|_| {
                let k = keep[idx];
                idx += 1;
                k
            });

            pruned += before - chain.len();
        }

        // Remove empty chains
        self.chains.retain(|_, chain| !chain.is_empty());

        pruned
    }

    /// Get statistics about the MVCC store.
    pub fn stats(&self) -> MVCCStats {
        let mut total_versions = 0usize;
        let mut total_chains = 0usize;
        let mut live_keys = 0usize;

        for entry in self.chains.iter() {
            total_chains += 1;
            total_versions += entry.value().len();
            if entry
                .value()
                .first()
                .map_or(false, |v| v.deleted_at.is_none())
            {
                live_keys += 1;
            }
        }

        MVCCStats {
            total_chains,
            total_versions,
            live_keys,
            current_version: self.clock.current(),
            gc_watermark: self.gc_watermark.load(Ordering::Acquire),
            active_snapshots: self
                .active_snapshots
                .versions
                .lock()
                .len(),
        }
    }
}

/// Statistics about the MVCC store.
#[derive(Debug, Clone)]
pub struct MVCCStats {
    pub total_chains: usize,
    pub total_versions: usize,
    pub live_keys: usize,
    pub current_version: u64,
    pub gc_watermark: u64,
    pub active_snapshots: usize,
}

// ---- Version resolution -------------------------------------

/// Resolve the visible version from a chain at a given snapshot version.
/// Chain must be ordered newest-first.
///
/// Returns the data of the latest version where:
///   created_at <= snap_version AND (deleted_at is None OR deleted_at > snap_version)
fn resolve_version(chain: &[MVCCValue], snap_version: u64) -> Option<Arc<[u8]>> {
    for mv in chain {
        if mv.created_at > snap_version {
            continue; // created after our snapshot — invisible
        }
        // created_at <= snap_version
        match mv.deleted_at {
            None => return Some(Arc::clone(&mv.data)), // live version, visible
            Some(d) if d > snap_version => return Some(Arc::clone(&mv.data)), // deleted after our snapshot — still visible
            Some(_) => continue, // deleted before or at our snapshot — skip
        }
    }
    None // key doesn't exist at this snapshot version
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn test_store() -> (MVCCStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(StorageEngine::open(dir.path()).unwrap());
        engine.create_table("test").unwrap();
        (MVCCStore::new(engine), dir)
    }

    #[test]
    fn test_put_and_get_latest() {
        let (store, _dir) = test_store();
        let txn = Uuid::new_v4();
        store.put("test", "k1", b"hello", txn).unwrap();

        let val = store.get_latest("test", "k1").unwrap();
        assert_eq!(&*val, b"hello");
    }

    #[test]
    fn test_snapshot_isolation() {
        let (store, _dir) = test_store();
        let txn = Uuid::new_v4();

        // Write v1
        store.put("test", "k1", b"v1", txn).unwrap();
        let snap1 = store.snapshot();

        // Write v2
        store.put("test", "k1", b"v2", txn).unwrap();
        let snap2 = store.snapshot();

        // Write v3
        store.put("test", "k1", b"v3", txn).unwrap();

        // snap1 sees v1
        let val = store.get_at("test", "k1", snap1).unwrap();
        assert_eq!(&*val, b"v1");

        // snap2 sees v2
        let val = store.get_at("test", "k1", snap2).unwrap();
        assert_eq!(&*val, b"v2");

        // latest sees v3
        let val = store.get_latest("test", "k1").unwrap();
        assert_eq!(&*val, b"v3");

        store.release_snapshot(snap1);
        store.release_snapshot(snap2);
    }

    #[test]
    fn test_delete_visibility() {
        let (store, _dir) = test_store();
        let txn = Uuid::new_v4();

        store.put("test", "k1", b"alive", txn).unwrap();
        let snap_before = store.snapshot();

        store.delete("test", "k1", txn).unwrap();
        let snap_after = store.snapshot();

        // Before delete — still visible
        assert!(store.get_at("test", "k1", snap_before).is_some());

        // After delete — gone
        assert!(store.get_at("test", "k1", snap_after).is_none());

        store.release_snapshot(snap_before);
        store.release_snapshot(snap_after);
    }

    #[test]
    fn test_list_keys_at_snapshot() {
        let (store, _dir) = test_store();
        let txn = Uuid::new_v4();

        store.put("test", "a", b"1", txn).unwrap();
        store.put("test", "b", b"2", txn).unwrap();
        let snap1 = store.snapshot();

        store.put("test", "c", b"3", txn).unwrap();
        store.delete("test", "a", txn).unwrap();
        let snap2 = store.snapshot();

        let mut keys1 = store.list_keys_at("test", snap1);
        keys1.sort();
        assert_eq!(keys1, vec!["a", "b"]);

        let mut keys2 = store.list_keys_at("test", snap2);
        keys2.sort();
        assert_eq!(keys2, vec!["b", "c"]);

        store.release_snapshot(snap1);
        store.release_snapshot(snap2);
    }

    #[test]
    fn test_gc_prunes_old_versions() {
        let (store, _dir) = test_store();
        let txn = Uuid::new_v4();

        // Create 5 versions
        for i in 0..5 {
            store
                .put("test", "k1", format!("v{}", i).as_bytes(), txn)
                .unwrap();
        }

        let stats_before = store.stats();
        assert_eq!(stats_before.total_versions, 5);

        // No active snapshots — GC should prune all but latest
        let pruned = store.gc();
        assert_eq!(pruned, 4);

        let stats_after = store.stats();
        assert_eq!(stats_after.total_versions, 1);

        // Latest version still readable
        let val = store.get_latest("test", "k1").unwrap();
        assert_eq!(&*val, b"v4");
    }

    #[test]
    fn test_gc_preserves_snapshot_visible_versions() {
        let (store, _dir) = test_store();
        let txn = Uuid::new_v4();

        store.put("test", "k1", b"v0", txn).unwrap();
        let snap = store.snapshot();

        store.put("test", "k1", b"v1", txn).unwrap();
        store.put("test", "k1", b"v2", txn).unwrap();

        // All 3 versions are >= oldest active snapshot (snap.version),
        // so GC can't prune any of them — a future snapshot might need v1.
        let pruned = store.gc();
        assert_eq!(pruned, 0);

        // snap still sees v0
        let val = store.get_at("test", "k1", snap).unwrap();
        assert_eq!(&*val, b"v0");

        // latest sees v2
        let val = store.get_latest("test", "k1").unwrap();
        assert_eq!(&*val, b"v2");

        // Release snapshot, then GC can prune v0 and v1
        store.release_snapshot(snap);
        let pruned = store.gc();
        assert_eq!(pruned, 2);

        let stats = store.stats();
        assert_eq!(stats.total_versions, 1);
    }

    #[test]
    fn test_version_clock_advances() {
        let clock = VersionClock::new();
        assert_eq!(clock.current(), 1);
        assert_eq!(clock.advance(), 2);
        assert_eq!(clock.advance(), 3);
        assert_eq!(clock.current(), 3);
    }

    #[test]
    fn test_nonexistent_key() {
        let (store, _dir) = test_store();
        let snap = store.snapshot();
        assert!(store.get_at("test", "nope", snap).is_none());
        assert!(store.get_latest("test", "nope").is_none());
        store.release_snapshot(snap);
    }

    #[test]
    fn test_delete_nonexistent() {
        let (store, _dir) = test_store();
        let txn = Uuid::new_v4();
        let result = store.delete("test", "nope", txn).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_has_write_after() {
        let (store, _dir) = test_store();
        let txn = Uuid::new_v4();

        let v1 = store.put("test", "k1", b"v1", txn).unwrap();
        assert!(!store.has_write_after("test", "k1", v1));

        let v2 = store.put("test", "k1", b"v2", txn).unwrap();
        assert!(store.has_write_after("test", "k1", v1));
        assert!(!store.has_write_after("test", "k1", v2));

        // Non-existent key
        assert!(!store.has_write_after("test", "nope", 0));
    }
}
