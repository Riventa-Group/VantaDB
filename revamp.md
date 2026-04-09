# VantaDB Enterprise Overhaul Specification

**Date:** 2026-04-08
**Status:** Approved
**Scope:** Complete enterprise-grade rewrite addressing 31 identified issues across data durability, security, scalability, reliability, and distributed operation.

---

## Table of Contents

1. [Overview](#overview)
2. [Issue Tracking Matrix](#issue-tracking-matrix)
3. [Phase 1: Storage Engine & Data Integrity](#phase-1-storage-engine--data-integrity)
4. [Phase 2: Security & Zero-Trust](#phase-2-security--zero-trust)
5. [Phase 3: Operational Readiness](#phase-3-operational-readiness)
6. [Phase 4: Active-Active Multi-Leader Replication](#phase-4-active-active-multi-leader-replication)
7. [Phase Dependencies](#phase-dependencies)
8. [New Crate Dependencies](#new-crate-dependencies)

---

## Overview

VantaDB is a document database (JSON documents, MongoDB-style queries) with a CLI shell and gRPC server, built in Rust as a single binary. An enterprise audit identified 31 issues ranging from data-loss bugs to missing distributed systems capabilities.

This spec defines a 4-phase bottom-up rebuild:

| Phase | Focus | Issues Fixed | Parallel Tracks |
|-------|-------|-------------|-----------------|
| **1** | Storage & Data Integrity | #1-10, #16-18, #20-22, #30-31 (18 issues) | Storage engine, Transaction engine, Index/query planner |
| **2** | Security & Zero-Trust | #11-15 (5 issues) | Auth/crypto, gRPC transport, Audit subsystem |
| **3** | Operational Readiness | #23-28 (6 issues) | Observability, Config/CLI, Test suite |
| **4** | Distributed System | #29 (1 issue, largest scope) | Replication protocol, CRDT engine, Cluster membership |

The system will be non-functional between phases (feature-freeze rewrite). All 4 tracks within each phase can be developed in parallel by separate team members.

---

## Issue Tracking Matrix

| # | Area | Issue | Severity | Phase |
|---|------|-------|----------|-------|
| 1 | WAL | No fsync/fdatasync on WAL writes | Critical | 1 |
| 2 | WAL | Truncated WAL entries silently ignored | High | 1 |
| 3 | Compaction | Non-atomic snapshot writes | Critical | 1 |
| 4 | Compaction | WAL deleted before snapshot verified | High | 1 |
| 5 | Transactions | Transactions are not atomic | Critical | 1 |
| 6 | Transactions | No isolation between transactions | Critical | 1 |
| 7 | Transactions | Abandoned transactions leak memory | Medium | 1 |
| 8 | Storage | `db_engine()` opens new StorageEngine per call | Critical | 1 |
| 9 | Concurrency | `get_or_create_table` race condition | Medium | 1 |
| 10 | Concurrency | create_database/create_collection TOCTOU | Medium | 1 |
| 11 | Auth | Root user requires no password over gRPC | Critical | 2 |
| 12 | Sessions | Session tokens are UUIDv4, in-memory only | High | 2 |
| 13 | Auth | No rate limiting on authentication | High | 2 |
| 14 | Auth | No per-database authorization enforcement | Medium | 2 |
| 15 | gRPC | Server binds 0.0.0.0 with no TLS | High | 2 |
| 16 | Queries | All queries do full collection scans | Critical | 1 |
| 17 | Indexes | Indexes maintained but never used by queries | High | 1 |
| 18 | Memory | Entire dataset must fit in RAM | High | 1 |
| 19 | Aggregation | $lookup is O(n*m) nested loop join | Medium | 1 |
| 20 | Errors | `io::Result<Result<T, String>>` double-wrapping | Medium | 1 |
| 21 | Schema | Regex compiled on every validation | Low | 1 |
| 22 | Indexes | Indexes not rebuilt on restart | High | 1 |
| 23 | Tests | Zero unit tests | High | 3 |
| 24 | Tests | Self-check doesn't test error paths | Medium | 3 |
| 25 | Logging | No logging framework | High | 3 |
| 26 | Monitoring | No metrics or health endpoints | High | 3 |
| 27 | Backups | No backup/restore mechanism | High | 3 |
| 28 | Config | No configuration file | Medium | 3 |
| 29 | Replication | Single-node only | Medium | 4 |
| 30 | Data dir | Data path inconsistency | Low | 1 |
| 31 | Version | Version string mismatch | Low | 1 |

---

## Phase 1: Storage Engine & Data Integrity

### 1.1 MVCC Storage Engine

**Problem:** The current `StorageEngine` is a single-version in-memory KV store (`DashMap<String, Arc<[u8]>>`). It cannot support snapshots, isolation, or serializable transactions.

**Architecture:**

```
                    +-----------------------------+
                    |       DatabaseManager        |
                    |  (cached, one per database)  |
                    +-------------+---------------+
                                  |
                    +-------------v---------------+
                    |      TransactionManager      |
                    |  (serializable scheduler)    |
                    +-------------+---------------+
                                  |
          +-----------------------+-----------------------+
          |                       |                       |
   +------v------+       +-------v-------+       +-------v-------+
   |  IndexEngine |       |  MVCCStorage  |       |  WAL Manager  |
   |  (B-tree,    |       |  (versioned   |       |  (fsync,      |
   |   hash,      |       |   DashMap,    |       |   atomic,     |
   |   used by    |       |   snapshots)  |       |   checksum)   |
   |   queries)   |       +-------+-------+       +---------------+
   +--------------+               |
                    +-------------v---------------+
                    |     PageStore / Disk I/O     |
                    |  (atomic writes, temp+rename)|
                    +-----------------------------+
```

**Versioned value storage:**

Each value in the MVCC store is:

```rust
pub struct MVCCValue {
    pub data: Arc<[u8]>,
    pub created_at: u64,      // transaction version that created this
    pub deleted_at: Option<u64>, // transaction version that deleted this (None = live)
    pub txn_id: Uuid,         // creating transaction ID
}
```

Tables become `DashMap<String, Vec<MVCCValue>>` -- each key holds a version chain. Reads resolve the correct version by finding the latest `created_at <= snapshot_version` where `deleted_at` is either `None` or `> snapshot_version`.

**Fixes #8 -- Engine caching:**

`DatabaseManager` holds a persistent cache of open engines:

```rust
pub struct DatabaseManager {
    base_path: PathBuf,
    meta_engine: StorageEngine,
    engines: DashMap<String, Arc<StorageEngine>>,  // cached, not re-opened
    index_manager: IndexManager,
    tx_manager: TransactionManager,
    db_locks: DashMap<String, Arc<RwLock<()>>>,
    global_ddl_lock: RwLock<()>,
}
```

`db_engine()` becomes a cache lookup:

```rust
fn db_engine(&self, db_name: &str) -> io::Result<Arc<StorageEngine>> {
    if let Some(engine) = self.engines.get(db_name) {
        return Ok(Arc::clone(engine.value()));
    }
    let engine = Arc::new(StorageEngine::open(&self.base_path.join(db_name))?);
    self.engines.insert(db_name.to_string(), Arc::clone(&engine));
    Ok(engine)
}
```

Engines are evicted only on `drop_database`.

### 1.2 WAL Manager

**Fixes:** #1 (no fsync), #2 (silent corruption), #3 (non-atomic compaction), #4 (premature WAL deletion).

**WAL record format:**

```
+--------+----------+--------+----------+--------+-----------+
| op (1B)| key_len  | key    | val_len  | value  | crc32 (4B)|
|        | (4B LE)  | (var)  | (4B LE)  | (var)  |           |
+--------+----------+--------+----------+--------+-----------+
```

Every record ends with a CRC32C checksum covering all preceding bytes in that record. On replay, records with invalid checksums are treated as the end of valid data (torn write from crash). A `WARN`-level log is emitted for each skipped record.

**Fsync protocol:**

```rust
pub fn wal_append(&self, table: &str, entries: &[WalEntry]) -> io::Result<()> {
    let writer = self.get_wal_writer(table)?;
    let mut w = writer.lock();
    for entry in entries {
        entry.write_to(&mut *w)?;  // includes CRC32C
    }
    w.flush()?;
    // fdatasync: flush data to disk, not just OS buffer
    w.get_ref().sync_data()?;
    Ok(())
}
```

**Group commit optimization:**

Multiple concurrent transactions waiting to commit batch their WAL entries into a single fsync call:

```rust
pub struct GroupCommitBatcher {
    pending: Mutex<Vec<PendingCommit>>,
    notify: Condvar,
    max_wait: Duration,  // default 5ms, configurable
}
```

A background thread wakes every `max_wait` or when the batch reaches a size threshold, writes all pending entries, calls fsync once, then notifies all waiting transactions. This amortizes the ~1ms fsync cost across multiple transactions.

**Atomic compaction:**

```rust
pub fn compact(&self, table: &str) -> io::Result<()> {
    // 1. Flush WAL buffer
    if let Some(writer) = self.wal_writers.get(table) {
        let mut w = writer.lock();
        w.flush()?;
        w.get_ref().sync_data()?;
    }

    // 2. Write snapshot to temp file
    let snapshot_path = self.table_path(table);
    let tmp_path = snapshot_path.with_extension("vdb.tmp");

    if let Some(map_ref) = self.tables.get(table) {
        let entries: Vec<(String, Vec<u8>)> = map_ref
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().to_vec()))
            .collect();
        let table_file = TableFile { entries };
        let data = bincode::serialize(&table_file)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        fs::write(&tmp_path, data)?;

        // 3. Fsync the temp file
        let f = File::open(&tmp_path)?;
        f.sync_all()?;

        // 4. Atomic rename
        fs::rename(&tmp_path, &snapshot_path)?;

        // 5. Fsync the directory to ensure rename is durable
        let dir = File::open(self.base_path.as_path())?;
        dir.sync_all()?;
    }

    // 6. Only NOW safe to remove WAL
    self.wal_writers.remove(table);
    let wal_path = self.wal_path(table);
    if wal_path.exists() {
        let _ = fs::remove_file(wal_path);
    }
    Ok(())
}
```

### 1.3 Serializable Transaction Engine

**Fixes:** #5 (non-atomic transactions), #6 (no isolation), #7 (leaked transactions).

**Model:** Strict Two-Phase Locking (S2PL) with MVCC reads. Same approach as PostgreSQL's serializable isolation.

**Components:**

```rust
/// Global monotonic version clock
pub struct VersionClock {
    current: AtomicU64,
}

/// Per-row lock table
pub struct LockTable {
    locks: DashMap<(String, String), LockEntry>,  // (collection, doc_id) -> lock
}

pub struct LockEntry {
    owner_tx: Uuid,
    wait_queue: VecDeque<(Uuid, oneshot::Sender<()>)>,
}

/// Active transaction state
pub struct Transaction {
    pub id: Uuid,
    pub start_version: u64,
    pub workspace: HashMap<(String, String), WorkspaceEntry>,  // pending writes
    pub read_set: Vec<(String, String, u64)>,  // (collection, doc_id, version_read)
    pub held_locks: Vec<(String, String)>,
    pub started_at: Instant,
    pub timeout: Duration,
}

pub enum WorkspaceEntry {
    Insert(Vec<u8>),
    Update(Vec<u8>),
    Delete,
}
```

**Transaction lifecycle:**

1. **BEGIN**: Allocate `Transaction` with `start_version = version_clock.current()`. Start timeout timer.

2. **READ**: Check workspace first (read-your-own-writes). If not in workspace, resolve from MVCC storage: find the latest version where `created_at <= start_version` and (`deleted_at` is `None` or `deleted_at > start_version`). Record `(collection, doc_id, version_read)` in read set.

3. **WRITE**: Acquire exclusive lock on `(collection, doc_id)` via lock table.
   - If lock is free: acquire it, buffer the write in workspace.
   - If lock is held by another transaction: add to wait queue. Check for deadlock via wait-for graph cycle detection. If deadlock detected, abort the younger transaction. If no deadlock, block until lock is released or timeout.

4. **COMMIT**:
   a. **Validation**: For each entry in read set, check if the key was modified by any transaction that committed between `start_version` and `now`. If so, abort with `TransactionConflict` error. Client retries.
   b. **WAL write**: Write all workspace entries to WAL in a single group commit batch. Wait for fsync.
   c. **Apply**: Write all workspace entries to MVCC storage with `created_at = new_commit_version`.
   d. **Release locks**: Release all held locks, wake waiting transactions.
   e. **Advance version clock**: `version_clock.fetch_add(1)`.

5. **ROLLBACK**: Discard workspace, release all locks, remove from active transactions.

6. **TIMEOUT**: Background reaper thread scans active transactions every second. Transactions exceeding their timeout (default 30s, configurable) are forcibly aborted. Fixes #7.

**Deadlock detection:**

```rust
pub struct WaitForGraph {
    edges: DashMap<Uuid, HashSet<Uuid>>,  // tx_id -> set of tx_ids it waits for
}

impl WaitForGraph {
    /// Returns true if adding this edge would create a cycle
    pub fn would_deadlock(&self, waiter: Uuid, holder: Uuid) -> bool {
        // BFS/DFS from holder: can we reach waiter?
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(holder);
        while let Some(node) = queue.pop_front() {
            if node == waiter {
                return true;
            }
            if visited.insert(node) {
                if let Some(targets) = self.edges.get(&node) {
                    for &t in targets.value() {
                        queue.push_back(t);
                    }
                }
            }
        }
        false
    }
}
```

**MVCC garbage collection:**

A background thread runs every `gc_interval_seconds` (default 60):

1. Find the minimum `start_version` across all active transactions.
2. For each key's version chain, prune versions where `deleted_at < min_active_version` and there exists a newer version also below `min_active_version`.
3. This reclaims memory from old versions that no transaction can ever read.

### 1.4 Query-Integrated Index Engine

**Fixes:** #16 (full collection scans), #17 (unused indexes), #19 ($lookup O(n*m)), #22 (indexes not rebuilt on restart).

**Index types:**

```rust
pub enum IndexType {
    BTree,   // ordered, supports equality + range + prefix
    Hash,    // O(1) equality only ($eq, $in, $nin)
}

pub struct IndexDef {
    pub collection: String,
    pub field: String,
    pub index_type: IndexType,
    pub unique: bool,
}
```

**B-tree index (enhanced):**

```rust
pub struct BTreeIndex {
    pub def: IndexDef,
    // value_key -> Vec<(doc_id, created_version, deleted_version)>
    tree: RwLock<BTreeMap<String, Vec<IndexEntry>>>,
}

pub struct IndexEntry {
    pub doc_id: String,
    pub created_at: u64,
    pub deleted_at: Option<u64>,
}
```

Index entries carry MVCC version metadata. Lookups filter by version visibility, consistent with the storage layer.

**Hash index (new):**

```rust
pub struct HashIndex {
    pub def: IndexDef,
    map: DashMap<String, Vec<IndexEntry>, RandomState>,
}
```

Same version-aware entries, but backed by a hash map for O(1) equality.

**Query planner:**

```rust
pub fn plan_query(
    filter: &Value,
    available_indexes: &[Arc<dyn Index>],
) -> QueryPlan {
    let predicates = extract_predicates(filter);
    let mut candidates: Vec<IndexScanPlan> = Vec::new();

    for pred in &predicates {
        for idx in available_indexes {
            if idx.field() == pred.field && idx.supports_op(&pred.op) {
                candidates.push(IndexScanPlan {
                    index: Arc::clone(idx),
                    predicate: pred.clone(),
                    estimated_selectivity: estimate_selectivity(pred),
                });
            }
        }
    }

    if candidates.is_empty() {
        return QueryPlan::FullScan { filter: filter.clone() };
    }

    // Pick the most selective index
    candidates.sort_by(|a, b| a.estimated_selectivity.partial_cmp(&b.estimated_selectivity).unwrap());

    let primary = candidates.remove(0);
    let remaining_filter = remove_predicate(filter, &primary.predicate);

    QueryPlan::IndexScan {
        index: primary.index,
        predicate: primary.predicate,
        residual_filter: remaining_filter,
    }
}

pub enum QueryPlan {
    FullScan { filter: Value },
    IndexScan {
        index: Arc<dyn Index>,
        predicate: Predicate,
        residual_filter: Option<Value>,
    },
}
```

**Execution flow:**

1. Query planner analyzes filter, selects index (if available).
2. Index scan returns candidate doc IDs (version-filtered).
3. For each candidate, resolve full document from MVCC storage at the transaction's snapshot version.
4. Apply residual filter predicates in memory.
5. Apply sort, pagination, projection.

**$lookup optimization (fixes #19):**

When `$lookup` executes, it checks for an index on `foreignField` in the foreign collection. If found:

```rust
// With index: O(n * log(m))
for doc in source_docs {
    let local_val = doc.get(local_field);
    let matches = foreign_index.lookup_eq(local_val);  // index scan
    // ...
}

// Without index: O(n * m) -- current behavior, used as fallback
```

**Index rebuild on startup (fixes #22):**

```rust
impl DatabaseManager {
    pub fn new(base_path: &Path) -> io::Result<Self> {
        let meta_engine = StorageEngine::open(&base_path.join("_meta"))?;
        // ... existing setup ...

        // Load and rebuild all persisted indexes
        let index_keys = meta_engine.list_keys(META_TABLE)
            .into_iter()
            .filter(|k| k.starts_with("_idx:"));

        for key in index_keys {
            if let Some(data) = meta_engine.get(META_TABLE, &key) {
                if let Ok(def) = serde_json::from_slice::<IndexDef>(&data) {
                    let (db, collection) = parse_idx_key(&def.collection);
                    if let Ok(docs) = self.find_all_internal(&db, &collection) {
                        let doc_pairs = docs_to_pairs(&docs);
                        self.index_manager.add_index(&def.collection, def, &doc_pairs);
                    }
                }
            }
        }

        Ok(manager)
    }
}
```

### 1.5 Concurrency Fixes

**Fix #9 -- Atomic table creation:**

Replace check-then-insert with `DashMap::entry()`:

```rust
fn get_or_create_table(&self, table: &str) -> Arc<Table> {
    self.tables
        .entry(table.to_string())
        .or_insert_with(|| Arc::new(new_table(64)))
        .value()
        .clone()
}
```

**Fix #10 -- DDL locking:**

```rust
pub struct DatabaseManager {
    // ...
    db_locks: DashMap<String, Arc<RwLock<()>>>,
    global_ddl_lock: RwLock<()>,
}

impl DatabaseManager {
    pub fn create_collection(&self, db: &str, col: &str) -> Result<(), VantaError> {
        let lock = self.db_locks
            .entry(db.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone();
        let _guard = lock.write();  // exclusive DDL lock
        // ... create collection logic ...
    }

    pub fn insert(&self, db: &str, col: &str, doc: Value) -> Result<String, VantaError> {
        let lock = self.db_locks
            .entry(db.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone();
        let _guard = lock.read();  // shared DML lock
        // ... insert logic ...
    }

    pub fn create_database(&self, name: &str) -> Result<(), VantaError> {
        let _guard = self.global_ddl_lock.write();  // global exclusive
        // ... create database logic ...
    }
}
```

### 1.6 Error Type System

**Fix #20:**

Replace `io::Result<Result<T, String>>` with a structured error enum:

```rust
#[derive(Debug)]
pub enum VantaError {
    /// Entity not found (database, collection, document, user, index)
    NotFound {
        entity: &'static str,
        name: String,
    },
    /// Entity already exists
    AlreadyExists {
        entity: &'static str,
        name: String,
    },
    /// Schema or input validation failed
    ValidationFailed {
        errors: Vec<String>,
    },
    /// Insufficient permissions
    PermissionDenied {
        required: String,
    },
    /// Transaction aborted due to serialization conflict
    TransactionConflict {
        tx_id: String,
        reason: String,
    },
    /// Transaction deadlock detected
    Deadlock {
        tx_id: String,
    },
    /// Transaction timed out
    TransactionTimeout {
        tx_id: String,
    },
    /// Underlying I/O error
    Io(io::Error),
    /// Serialization/deserialization error
    Serialization(String),
    /// Unknown or internal error
    Internal(String),
}

impl VantaError {
    pub fn to_grpc_status(&self) -> tonic::Status {
        match self {
            VantaError::NotFound { .. } => Status::not_found(self.to_string()),
            VantaError::AlreadyExists { .. } => Status::already_exists(self.to_string()),
            VantaError::ValidationFailed { .. } => Status::invalid_argument(self.to_string()),
            VantaError::PermissionDenied { .. } => Status::permission_denied(self.to_string()),
            VantaError::TransactionConflict { .. } => Status::aborted(self.to_string()),
            VantaError::Deadlock { .. } => Status::aborted(self.to_string()),
            VantaError::TransactionTimeout { .. } => Status::deadline_exceeded(self.to_string()),
            VantaError::Io(_) => Status::internal(self.to_string()),
            VantaError::Serialization(_) => Status::internal(self.to_string()),
            VantaError::Internal(_) => Status::internal(self.to_string()),
        }
    }
}
```

All `DatabaseManager` methods change from `io::Result<Result<T, String>>` to `Result<T, VantaError>`.

### 1.7 Minor Fixes

**Fix #21 -- Regex caching:**

```rust
use std::cell::RefCell;
use std::collections::HashMap;

thread_local! {
    static REGEX_CACHE: RefCell<HashMap<String, regex::Regex>> = RefCell::new(HashMap::new());
}

fn cached_regex(pattern: &str) -> Option<regex::Regex> {
    REGEX_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(re) = cache.get(pattern) {
            return Some(re.clone());
        }
        if let Ok(re) = regex::Regex::new(pattern) {
            if cache.len() >= 64 {
                cache.clear();  // simple eviction
            }
            cache.insert(pattern.to_string(), re.clone());
            Some(re)
        } else {
            None
        }
    })
}
```

Used in `filter.rs` (match_operator `$regex`) and `schema.rs` (validate_field pattern check).

**Fix #31 -- Version string consistency:**

In `main.rs`:

```rust
const VERSION: &str = env!("CARGO_PKG_VERSION");
```

Replace all hardcoded version strings in `print_banner()`, `cmd_status()`, and the clap `#[command(version)]` with `VERSION`.

**Fix #30 -- Data directory standardization:**

```rust
pub fn data_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("VANTADB_DATA_DIR") {
        return PathBuf::from(dir);
    }
    dirs::data_dir()
        .unwrap_or_else(|| dirs::home_dir().unwrap_or_else(|| PathBuf::from(".")))
        .join("vantadb")
}
```

Resolves to `~/.local/share/vantadb/` on Linux, `~/Library/Application Support/vantadb/` on macOS. If the legacy `~/.vantadb/` exists and the new path doesn't, emit a warning:

```
WARNING: Legacy data directory ~/.vantadb/ detected.
VantaDB now uses ~/.local/share/vantadb/
To migrate: mv ~/.vantadb ~/.local/share/vantadb
```

---

## Phase 2: Security & Zero-Trust

### 2.1 Root Authentication Fix

**Fixes #11.**

Remove the passwordless root shortcut entirely. On first startup:

1. Generate a 32-character random root password using `rand::OsRng`.
2. Hash it with Argon2id (memory: 64MB, iterations: 3, parallelism: 4).
3. Store the hash in the auth engine.
4. Print the plaintext password once to stdout:

```
=== INITIAL ROOT PASSWORD ===
Root password: <generated-password>
Store this securely. It cannot be recovered.
=============================
```

5. All subsequent root logins (CLI and gRPC) require this password. The sudo-only check in `shell.rs` is removed.

**Password policy:**

```rust
pub fn validate_password(password: &str) -> Result<(), VantaError> {
    if password.len() < 12 {
        return Err(VantaError::ValidationFailed {
            errors: vec!["Password must be at least 12 characters".into()],
        });
    }
    Ok(())
}
```

Enforced on `create_user`, `set_password`, and root bootstrap.

**Account lockout:**

```rust
pub struct LockoutTracker {
    attempts: DashMap<String, Vec<Instant>>,  // username -> recent failure timestamps
    max_attempts: u32,       // default 5
    window: Duration,        // default 5 minutes
    lockout_duration: Duration,  // default 15 minutes
}

impl LockoutTracker {
    pub fn check_locked(&self, username: &str) -> bool { /* ... */ }
    pub fn record_failure(&self, username: &str) { /* ... */ }
    pub fn record_success(&self, username: &str) { /* clear attempts */ }
}
```

Lockout state is persisted in the auth engine so it survives restarts.

### 2.2 Rate Limiting

**Fixes #13.**

Token bucket rate limiter as a tonic interceptor:

```rust
pub struct RateLimiter {
    global_bucket: TokenBucket,          // 10 req/sec default
    per_ip_buckets: DashMap<IpAddr, TokenBucket>,  // 3 req/sec default
}

impl tonic::service::Interceptor for RateLimiter {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        let ip = extract_peer_ip(&req);
        if !self.global_bucket.try_acquire() {
            return Err(Status::resource_exhausted("Global rate limit exceeded"));
        }
        if let Some(ip) = ip {
            let bucket = self.per_ip_buckets
                .entry(ip)
                .or_insert_with(|| TokenBucket::new(3, Duration::from_secs(1)));
            if !bucket.try_acquire() {
                return Err(Status::resource_exhausted("Per-IP rate limit exceeded"));
            }
        }
        Ok(req)
    }
}
```

Applied only to the `VantaAuth` service (specifically the `Authenticate` RPC). Data RPCs are already gated by valid tokens.

### 2.3 JWT Sessions

**Fixes #12.**

Replace UUID session tokens with signed JWTs.

**JWT structure:**

```rust
#[derive(Serialize, Deserialize)]
pub struct AccessClaims {
    pub sub: String,                    // username
    pub role: String,                   // global role
    pub db_acls: HashMap<String, DatabaseAcl>,  // per-database permissions
    pub iat: i64,                       // issued at (unix timestamp)
    pub exp: i64,                       // expires at
    pub jti: String,                    // unique token ID (for revocation)
}

#[derive(Serialize, Deserialize)]
pub struct RefreshClaims {
    pub sub: String,
    pub jti: String,
    pub exp: i64,                       // 24h TTL
}
```

**Signing:** HMAC-SHA256 using a server-side secret key. The key is generated on first startup and stored encrypted in the auth engine.

**Token lifecycle:**

1. Client calls `Authenticate` with username/password.
2. Server returns `{ access_token: "<JWT>", refresh_token: "<opaque>", expires_in: 3600 }`.
3. Client includes `Authorization: Bearer <JWT>` on every RPC.
4. Auth interceptor verifies JWT signature and expiry. Checks `jti` against revocation set.
5. When access token nears expiry, client calls `RefreshToken` with the refresh token.
6. Server validates refresh token, issues new access JWT.

**Revocation:**

```rust
pub struct TokenRevocationStore {
    revoked: DashMap<String, DateTime<Utc>>,  // jti -> revoked_at
    engine: StorageEngine,                     // persisted
}
```

- `RevokeSession` RPC: revokes a specific token by Jti.
- `RevokeUserSessions` RPC: revokes all tokens for a user (admin-only).
- Cleanup: background thread prunes entries where `revoked_at + max_token_ttl` has passed (the token would have expired naturally).

**Key rotation:**

Admin command `rotate_signing_key`:
1. Generate new HMAC key.
2. Store new key, keep old key in a "previous keys" list.
3. Token verification tries current key first, falls back to previous keys.
4. After max token TTL has elapsed, remove old keys from the list.

**New proto RPCs:**

```protobuf
service VantaAuth {
  // ... existing RPCs ...
  rpc RefreshToken (RefreshTokenRequest) returns (AuthResponse);
  rpc RevokeSession (RevokeSessionRequest) returns (StatusResponse);
  rpc RevokeUserSessions (RevokeUserSessionsRequest) returns (StatusResponse);
  rpc RotateSigningKey (Empty) returns (StatusResponse);
}
```

### 2.4 mTLS Transport

**Fixes #15.**

All gRPC traffic encrypted via rustls (pure Rust, no OpenSSL dependency).

**Built-in CA:**

On first startup, VantaDB generates:
1. A self-signed CA certificate (Ed25519, 10-year validity).
2. A server certificate signed by the CA.
3. Both stored in the data directory under `certs/`.

**Admin commands for client certificate management:**

```
vantadb --issue-cert --username alice --output alice.pem
vantadb --revoke-cert --username alice
vantadb --list-certs
```

Client certificates embed the username in the Subject CN field and role in a custom SAN extension.

**Server configuration:**

```rust
let tls_config = ServerTlsConfig::new()
    .identity(Identity::from_pem(&server_cert, &server_key))
    .client_ca_root(Certificate::from_pem(&ca_cert))
    .client_auth_optional(config.require_mtls);  // require or optional
```

**Auth flow with mTLS:**

When a client presents a valid certificate:
1. The TLS layer validates the certificate chain against the CA.
2. The auth interceptor extracts the username from the certificate CN.
3. No password or JWT needed -- the certificate IS the authentication.
4. The interceptor still checks revocation (certificate serial number against revoked list).
5. Permissions are loaded from the user's ACL configuration.

**Fallback for non-mTLS:**

If `require_mtls = false`, clients without certificates fall back to password + JWT auth. The server still presents its TLS certificate (server-side TLS is always on). There is no unencrypted mode.

### 2.5 Per-Collection ACLs

**Fixes #14.**

**Data model:**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserPermissions {
    pub global_role: Role,
    pub database_acls: HashMap<String, DatabaseAcl>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseAcl {
    pub role: Option<Role>,   // overrides global_role for this database
    pub collection_acls: HashMap<String, CollectionAcl>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionAcl {
    pub can_read: bool,
    pub can_write: bool,
    pub can_admin: bool,
}
```

**Resolution logic:**

```rust
pub fn resolve_permission(
    user: &UserPermissions,
    database: &str,
    collection: &str,
    action: Action,  // Read, Write, Admin
) -> bool {
    // Root bypasses all checks
    if user.global_role == Role::Root {
        return true;
    }

    // Check collection-level ACL first (most specific)
    if let Some(db_acl) = user.database_acls.get(database) {
        if let Some(col_acl) = db_acl.collection_acls.get(collection) {
            return match action {
                Action::Read => col_acl.can_read,
                Action::Write => col_acl.can_write,
                Action::Admin => col_acl.can_admin,
            };
        }
        // Fall back to database-level role
        if let Some(ref role) = db_acl.role {
            return match action {
                Action::Read => true,
                Action::Write => role.can_write(),
                Action::Admin => role.can_admin(),
            };
        }
    }

    // Fall back to global role
    match action {
        Action::Read => true,
        Action::Write => user.global_role.can_write(),
        Action::Admin => user.global_role.can_admin(),
    }
}
```

**Enforcement:**

The gRPC auth interceptor embeds the full `UserPermissions` in the request extensions (replacing the current `AuthContext` with just username/role). Every service method calls `resolve_permission()` with the specific database, collection, and action.

**Management RPCs:**

```protobuf
rpc SetUserAcl (SetUserAclRequest) returns (StatusResponse);
rpc GetUserAcl (GetUserAclRequest) returns (GetUserAclResponse);
```

Admin-only. ACLs are stored in the auth engine and embedded in JWTs on token issuance.

### 2.6 Audit Log

**Data model:**

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditEntry {
    pub timestamp: DateTime<Utc>,
    pub event: AuditEvent,
    pub username: String,
    pub source_ip: String,
    pub database: Option<String>,
    pub collection: Option<String>,
    pub detail: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum AuditEvent {
    AuthSuccess,
    AuthFailure,
    TokenRefresh,
    TokenRevoke,
    UserCreate,
    UserDelete,
    PasswordChange,
    DatabaseCreate,
    DatabaseDrop,
    CollectionCreate,
    CollectionDrop,
    DocumentInsert,
    DocumentUpdate,
    DocumentDelete,
    SchemaChange,
    IndexChange,
    TransactionCommit,
    TransactionRollback,
    AclChange,
    CertIssue,
    CertRevoke,
}
```

**Storage:**

- Separate `StorageEngine` instance at `<data_dir>/_audit/`.
- Entries stored in daily tables (`audit_2026_04_08`).
- Background writer: a bounded channel (capacity 10,000) buffers entries. A dedicated thread drains the channel and writes to the audit engine. Hot paths (insert, query) never block on audit I/O.

**Retention:**

Background GC thread runs daily, drops tables older than `audit.retention_days` (default 90).

**Query RPC:**

```protobuf
service VantaAudit {
  rpc QueryAudit (AuditQueryRequest) returns (AuditQueryResponse);
}

message AuditQueryRequest {
  string start_time = 1;   // ISO 8601
  string end_time = 2;
  string username = 3;     // optional filter
  string event_type = 4;   // optional filter
  string database = 5;     // optional filter
  uint32 limit = 6;
}
```

Admin-only access.

---

## Phase 3: Operational Readiness

### 3.1 Structured Logging

**Fixes #25.**

**Crate:** `tracing` + `tracing-subscriber` with `EnvFilter`.

**Setup in main.rs:**

```rust
use tracing_subscriber::{fmt, EnvFilter, prelude::*};

fn init_logging(config: &Config) {
    let filter = EnvFilter::try_from_env("VANTADB_LOG")
        .unwrap_or_else(|_| EnvFilter::new(&config.logging.level));

    let subscriber = tracing_subscriber::registry().with(filter);

    match config.logging.format.as_str() {
        "json" => {
            subscriber.with(fmt::layer().json().with_target(true)).init();
        }
        _ => {
            subscriber.with(fmt::layer().with_ansi(atty::is(atty::Stream::Stdout))).init();
        }
    }
}
```

**Instrumentation points:**

- `#[tracing::instrument]` on all `DatabaseManager` public methods.
- `tracing::info!` for lifecycle events (server start/stop, database create/drop, compaction).
- `tracing::warn!` for degraded states (WAL corruption detected, transaction timeout, lockout triggered).
- `tracing::error!` for failures (I/O errors, unrecoverable states).
- `tracing::debug!` for per-operation detail (document insert, query execution plan).
- `tracing::trace!` for internal detail (lock acquisition, MVCC version resolution, GC).

**Request tracing:**

Each gRPC request gets a span with a unique request ID:

```rust
async fn insert(&self, request: Request<proto::InsertRequest>) -> Result<Response<proto::InsertResponse>, Status> {
    let req_id = Uuid::new_v4();
    let span = tracing::info_span!("insert", req_id = %req_id, db = %req.database, col = %req.collection);
    let _enter = span.enter();
    // ... all tracing within this scope includes req_id
}
```

### 3.2 Metrics & Health Endpoints

**Fixes #26.**

**Crates:** `metrics` + `metrics-exporter-prometheus`.

**Setup:**

```rust
use metrics_exporter_prometheus::PrometheusBuilder;

fn init_metrics(port: u16) {
    PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], port))
        .install()
        .expect("Failed to install metrics exporter");
}
```

**Metrics registered:**

| Metric | Type | Labels |
|--------|------|--------|
| `vantadb_requests_total` | Counter | `method`, `status` |
| `vantadb_request_duration_seconds` | Histogram | `method` |
| `vantadb_active_transactions` | Gauge | |
| `vantadb_transaction_commits_total` | Counter | |
| `vantadb_transaction_aborts_total` | Counter | `reason` (conflict/timeout/deadlock) |
| `vantadb_documents_total` | Gauge | `database`, `collection` |
| `vantadb_index_lookups_total` | Counter | `index`, `type` (scan/hit) |
| `vantadb_wal_fsync_duration_seconds` | Histogram | |
| `vantadb_compaction_duration_seconds` | Histogram | |
| `vantadb_memory_rss_bytes` | Gauge | |
| `vantadb_mvcc_versions_total` | Gauge | |
| `vantadb_connections_active` | Gauge | |
| `vantadb_audit_entries_total` | Counter | `event` |
| `vantadb_replication_lag_seconds` | Gauge | `peer` (Phase 4, registered now) |

**Health check:**

Implement gRPC health check protocol (`grpc.health.v1.Health`):

```rust
pub struct HealthService {
    db_manager: Arc<DatabaseManager>,
}

#[tonic::async_trait]
impl health_server::Health for HealthService {
    async fn check(&self, _req: Request<HealthCheckRequest>) -> Result<Response<HealthCheckResponse>, Status> {
        // Check that we can read from the meta engine
        let status = if self.db_manager.is_healthy() {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        };
        Ok(Response::new(HealthCheckResponse { status: status.into() }))
    }
}
```

Exposed on the same gRPC port. Usable by Kubernetes liveness/readiness probes.

### 3.3 Configuration System

**Fixes #28.**

**Resolution order (last wins):** Defaults -> Config file -> Environment variables -> CLI flags.

**Config file:** TOML format.

```toml
[server]
grpc_port = 5432
metrics_port = 9090
data_dir = "/var/lib/vantadb"

[tls]
require_mtls = true
cert_file = "/etc/vantadb/server.crt"
key_file = "/etc/vantadb/server.key"
ca_file = "/etc/vantadb/ca.crt"

[auth]
jwt_ttl_hours = 1
refresh_ttl_hours = 24
lockout_attempts = 5
lockout_window_minutes = 5
lockout_duration_minutes = 15
rate_limit_global_per_second = 10
rate_limit_per_ip_per_second = 3
min_password_length = 12

[storage]
wal_fsync = true
compaction_interval_seconds = 300
group_commit_max_wait_ms = 5
transaction_timeout_seconds = 30
gc_interval_seconds = 60

[logging]
level = "info"
format = "json"

[audit]
enabled = true
retention_days = 90

[replication]
mode = "standalone"
seeds = []
replication_port = 5433
default_read_consistency = "local"
```

**Rust config struct:**

```rust
#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub tls: TlsConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub audit: AuditConfig,
    #[serde(default)]
    pub replication: ReplicationConfig,
}

impl Config {
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let mut config = Config::default();

        // Layer 1: config file
        let config_path = std::env::var("VANTADB_CONFIG")
            .unwrap_or_else(|_| "/etc/vantadb/config.toml".into());
        if Path::new(&config_path).exists() {
            let content = fs::read_to_string(&config_path)?;
            config = toml::from_str(&content)?;
        }

        // Layer 2: environment overrides
        config.apply_env_overrides();

        // Layer 3: CLI flags applied by caller
        Ok(config)
    }

    fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("VANTADB_GRPC_PORT") {
            if let Ok(port) = v.parse() { self.server.grpc_port = port; }
        }
        if let Ok(v) = std::env::var("VANTADB_DATA_DIR") {
            self.server.data_dir = v;
        }
        if let Ok(v) = std::env::var("VANTADB_LOG") {
            self.logging.level = v;
        }
        // ... etc for all config values
    }
}
```

Every config value has a sensible default. The system starts with zero configuration.

### 3.4 Backup & Restore

**Fixes #27.**

**Online backup:**

```protobuf
service VantaBackup {
  rpc CreateBackup (BackupRequest) returns (stream BackupChunk);
  rpc RestoreBackup (stream BackupChunk) returns (StatusResponse);
}

message BackupRequest {
  bool include_audit = 1;
}

message BackupChunk {
  bytes data = 1;          // length-prefixed protobuf messages
  uint64 sequence = 2;
  string chunk_type = 3;   // "header", "database", "collection", "document", "index", "schema", "user", "footer"
}
```

**Backup flow:**

1. Acquire a consistent MVCC snapshot at the current version.
2. Stream header (version, timestamp, node ID).
3. For each database: stream database metadata, then each collection's documents, indexes, schemas.
4. Stream user data and ACLs.
5. Stream footer with checksum of all preceding chunks.
6. Does not block writes -- reads from the MVCC snapshot.

**CLI:**

```bash
vantadb --backup --output /path/to/backup.vdbak
vantadb --restore --input /path/to/backup.vdbak
vantadb --restore --input /path/to/backup.vdbak --point-in-time "2026-04-08T15:30:00Z"
```

**Point-in-time recovery:**

WAL files are retained (not truncated on compaction) up to `wal_retention_hours` (default 24). Restore replays the base snapshot plus WAL entries up to the specified timestamp.

### 3.5 Test Suite

**Fixes #23, #24.**

**Unit tests** (`#[cfg(test)]` in each module):

| Module | Test Coverage |
|--------|--------------|
| `storage::engine` | WAL write/replay, crash recovery (partial WAL), atomic compaction (crash mid-write), fsync verification, group commit batching |
| `storage::mvcc` | Version chain resolution, snapshot reads, GC correctness |
| `db::filter` | Every operator ($eq, $gt, $lt, $gte, $lte, $in, $nin, $exists, $regex, $contains, $starts_with, $ends_with, $and, $or, $not), null handling, missing fields, type mismatches, nested dot paths |
| `db::schema` | Every field type, required fields, strict mode, min/max, min/max_length, pattern, enum values |
| `db::aggregation` | Every stage ($match, $group, $sort, $limit, $skip, $project, $count, $unwind, $lookup), empty inputs, type coercion, nested pipelines |
| `db::transaction` | Serializable isolation (concurrent conflicting writes abort), deadlock detection, timeout enforcement, read-your-own-writes, phantom prevention, read set validation |
| `db::index` | B-tree and hash correctness, equality and range lookups, uniqueness enforcement, MVCC version filtering, rebuild from documents |
| `db::query_planner` | Index selection, selectivity estimation, fallback to full scan |
| `auth::manager` | Password hashing/verification, lockout logic, root bootstrap, ACL resolution at all levels |
| `auth::jwt` | Token signing/verification, expiry, refresh flow, revocation, key rotation |
| `server::rate_limiter` | Token bucket refill, per-IP isolation, burst handling |

**Integration tests** (`tests/` directory):

| Test File | Coverage |
|-----------|----------|
| `tests/grpc_crud.rs` | Full gRPC round-trip for every RPC (database/collection/document CRUD) |
| `tests/grpc_auth.rs` | Login, JWT refresh, token revocation, mTLS auth, cert issuance |
| `tests/grpc_permissions.rs` | Every RPC with every role, ACL overrides, permission denied scenarios |
| `tests/transactions.rs` | Concurrent transaction scenarios (10+ tokio tasks), conflict detection, deadlock resolution |
| `tests/backup_restore.rs` | Full backup/restore round-trip, point-in-time recovery |
| `tests/crash_recovery.rs` | Simulate crash (kill process mid-write), restart, verify data integrity |
| `tests/aggregation.rs` | Complex multi-stage pipelines via gRPC |
| `tests/query_performance.rs` | Verify index scans are used (not full scans) when indexes exist |

**Property-based tests** (using `proptest`):

| Test | Strategy |
|------|----------|
| Filter correctness | Generate random filter expressions and documents. Verify `matches_filter` agrees with brute-force evaluation. |
| Transaction serializability | Generate random transaction interleavings. Verify committed results are equivalent to some serial execution order. |
| Schema validation | Generate random document shapes and schemas. Verify validation results are consistent. |
| Index consistency | Random insert/update/delete sequences. Verify index contents match a full collection scan at every step. |

**CI:**

`cargo test` runs all tests. No external dependencies. Integration tests use in-process gRPC channels (`tonic::transport::Channel::from_static`), temp directories for storage, and spawn the server in-process.

---

## Phase 4: Active-Active Multi-Leader Replication

### 4.1 Replication Architecture

**Fixes #29.**

Every node in the cluster is a full read-write leader. No primary/secondary distinction.

```
       +----------+        +----------+        +----------+
       |  Node A  |<======>|  Node B  |<======>|  Node C  |
       | (leader) |        | (leader) |        | (leader) |
       +----+-----+        +----+-----+        +----+-----+
            |                    |                    |
       Local MVCC           Local MVCC           Local MVCC
       Storage              Storage              Storage
```

**Consistency model:** Eventual consistency with causal ordering. Writes are immediately visible on the accepting node and propagate asynchronously to peers.

### 4.2 Hybrid Logical Clocks (HLC)

Causal ordering via hybrid logical clocks (Lamport + physical time):

```rust
pub struct HLC {
    /// Physical time component (milliseconds since epoch)
    physical: AtomicU64,
    /// Logical counter for events at the same physical time
    logical: AtomicU32,
    /// Node ID for tie-breaking
    node_id: u64,
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct Timestamp {
    pub physical: u64,
    pub logical: u32,
    pub node_id: u64,
}

impl HLC {
    /// Generate a new timestamp for a local event
    pub fn now(&self) -> Timestamp {
        let wall = system_time_ms();
        let physical = self.physical.load(Ordering::Acquire);
        if wall > physical {
            self.physical.store(wall, Ordering::Release);
            self.logical.store(0, Ordering::Release);
            Timestamp { physical: wall, logical: 0, node_id: self.node_id }
        } else {
            let l = self.logical.fetch_add(1, Ordering::AcqRel);
            Timestamp { physical, logical: l + 1, node_id: self.node_id }
        }
    }

    /// Update clock on receiving a remote timestamp
    pub fn receive(&self, remote: Timestamp) -> Timestamp {
        let wall = system_time_ms();
        let local_physical = self.physical.load(Ordering::Acquire);
        let max_physical = wall.max(local_physical).max(remote.physical);

        if max_physical == wall && max_physical > local_physical && max_physical > remote.physical {
            self.physical.store(max_physical, Ordering::Release);
            self.logical.store(0, Ordering::Release);
        } else if max_physical == local_physical && max_physical == remote.physical {
            let l = self.logical.load(Ordering::Acquire).max(remote.logical) + 1;
            self.logical.store(l, Ordering::Release);
        } else if max_physical == remote.physical {
            self.physical.store(max_physical, Ordering::Release);
            self.logical.store(remote.logical + 1, Ordering::Release);
        } else {
            let l = self.logical.fetch_add(1, Ordering::AcqRel);
            self.logical.store(l + 1, Ordering::Release);
        }

        self.physical.store(max_physical, Ordering::Release);
        Timestamp {
            physical: max_physical,
            logical: self.logical.load(Ordering::Acquire),
            node_id: self.node_id,
        }
    }
}
```

Every committed transaction gets an HLC timestamp. Replication entries are ordered by HLC.

### 4.3 Replication Log

Each node maintains an ordered log of committed transaction effects:

```rust
pub struct ReplicationEntry {
    pub sequence: u64,           // per-node monotonic sequence number
    pub hlc: Timestamp,          // hybrid logical clock timestamp
    pub source_node: u64,        // originating node ID
    pub operations: Vec<ReplicationOp>,
}

pub enum ReplicationOp {
    Insert {
        database: String,
        collection: String,
        doc_id: String,
        data: Vec<u8>,
        field_timestamps: HashMap<String, Timestamp>,  // per-field HLC for LWW merge
    },
    Update {
        database: String,
        collection: String,
        doc_id: String,
        fields_changed: HashMap<String, (Value, Timestamp)>,  // field -> (new_value, hlc)
    },
    Delete {
        database: String,
        collection: String,
        doc_id: String,
        hlc: Timestamp,
    },
    CreateDatabase { name: String },
    DropDatabase { name: String },
    CreateCollection { database: String, collection: String },
    DropCollection { database: String, collection: String },
    SchemaChange { database: String, collection: String, schema: Option<String> },
    IndexChange { database: String, collection: String, field: String, action: String },
}
```

The replication log is stored on disk (separate from the WAL) and retained for `replication_log_retention_hours` (default 72h).

**gRPC replication service:**

```protobuf
service VantaReplication {
    rpc StreamEntries (StreamRequest) returns (stream ReplicationEntry);
    rpc PushEntries (stream ReplicationEntry) returns (AckResponse);
    rpc GetSequenceVector (Empty) returns (SequenceVectorResponse);
    rpc RequestVote (VoteRequest) returns (VoteResponse);        // for DDL quorum
    rpc Heartbeat (HeartbeatRequest) returns (HeartbeatResponse); // SWIM protocol
}
```

### 4.4 Conflict Resolution (CRDTs)

Three strategies, selectable per-collection:

**Strategy 1: Last-Writer-Wins (LWW) -- Default**

Each document field carries an HLC timestamp. On conflict, higher HLC wins:

```rust
pub fn merge_lww(
    local: &Document,
    remote: &Document,
    local_timestamps: &HashMap<String, Timestamp>,
    remote_timestamps: &HashMap<String, Timestamp>,
) -> Document {
    let mut merged = Map::new();
    let all_fields: HashSet<&str> = local.keys()
        .chain(remote.keys())
        .map(|k| k.as_str())
        .collect();

    for field in all_fields {
        let local_ts = local_timestamps.get(field);
        let remote_ts = remote_timestamps.get(field);
        match (local.get(field), remote.get(field), local_ts, remote_ts) {
            (Some(lv), Some(rv), Some(lt), Some(rt)) => {
                if rt > lt {
                    merged.insert(field.to_string(), rv.clone());
                } else {
                    merged.insert(field.to_string(), lv.clone());
                }
            }
            (Some(lv), None, _, _) => { merged.insert(field.to_string(), lv.clone()); }
            (None, Some(rv), _, _) => { merged.insert(field.to_string(), rv.clone()); }
            _ => {}
        }
    }
    Value::Object(merged)
}
```

Concurrent writes to different fields merge cleanly. Same-field conflicts resolved by HLC.

**Strategy 2: CRDT-native (opt-in per field)**

Schema annotation enables CRDT semantics:

```json
{
  "fields": [
    {"name": "view_count", "type": "number", "crdt": "counter"},
    {"name": "tags", "type": "array", "crdt": "or-set"},
    {"name": "title", "type": "string"}
  ]
}
```

CRDT implementations:

```rust
/// G-Counter: grow-only counter, per-node increments
pub struct GCounter {
    counts: HashMap<u64, f64>,  // node_id -> count
}

impl GCounter {
    pub fn increment(&mut self, node_id: u64, amount: f64) {
        *self.counts.entry(node_id).or_insert(0.0) += amount;
    }

    pub fn merge(&mut self, other: &GCounter) {
        for (&node, &count) in &other.counts {
            let entry = self.counts.entry(node).or_insert(0.0);
            *entry = entry.max(count);
        }
    }

    pub fn value(&self) -> f64 {
        self.counts.values().sum()
    }
}

/// OR-Set: observed-remove set
pub struct ORSet {
    elements: HashMap<String, HashSet<(u64, Timestamp)>>,  // value -> set of (node_id, add_timestamp)
    tombstones: HashMap<String, HashSet<(u64, Timestamp)>>,
}

impl ORSet {
    pub fn add(&mut self, value: String, node_id: u64, ts: Timestamp) {
        self.elements.entry(value).or_default().insert((node_id, ts));
    }

    pub fn remove(&mut self, value: &str, observed: HashSet<(u64, Timestamp)>) {
        self.tombstones.entry(value.to_string()).or_default().extend(observed);
    }

    pub fn merge(&mut self, other: &ORSet) {
        for (val, tags) in &other.elements {
            self.elements.entry(val.clone()).or_default().extend(tags);
        }
        for (val, tags) in &other.tombstones {
            self.tombstones.entry(val.clone()).or_default().extend(tags);
        }
    }

    pub fn value(&self) -> Vec<String> {
        self.elements.iter()
            .filter(|(val, tags)| {
                let tombs = self.tombstones.get(*val);
                tags.iter().any(|tag| {
                    tombs.map_or(true, |t| !t.contains(tag))
                })
            })
            .map(|(val, _)| val.clone())
            .collect()
    }
}
```

Fields without a `crdt` annotation default to LWW-Register.

**Strategy 3: Application-level resolution**

For unresolvable conflicts, store both versions:

```rust
pub struct ConflictedDocument {
    pub doc_id: String,
    pub versions: Vec<ConflictVersion>,
}

pub struct ConflictVersion {
    pub data: Value,
    pub source_node: u64,
    pub hlc: Timestamp,
}
```

New RPCs:

```protobuf
rpc ListConflicts (ListConflictsRequest) returns (ListConflictsResponse);
rpc ResolveConflict (ResolveConflictRequest) returns (StatusResponse);
```

The application reads conflicted documents, merges them according to its business logic, and writes the resolved version back.

### 4.5 Cluster Membership (SWIM Protocol)

**No external coordinator.** Membership is managed via the SWIM protocol:

```rust
pub struct SwimMember {
    pub node_id: u64,
    pub address: SocketAddr,
    pub state: MemberState,
    pub incarnation: u64,         // monotonic, incremented on state change
    pub last_heartbeat: Instant,
}

pub enum MemberState {
    Alive,
    Suspicious,  // missed heartbeat, being probed
    Dead,        // confirmed unreachable
    Left,        // graceful departure
}

pub struct SwimProtocol {
    members: RwLock<HashMap<u64, SwimMember>>,
    self_node: u64,
    probe_interval: Duration,     // default 1 second
    suspicion_timeout: Duration,  // default 5 seconds
    dead_timeout: Duration,       // default 15 seconds
}
```

**Protocol flow:**

1. Every `probe_interval`, select a random member and send a direct ping.
2. If no ack within `suspicion_timeout / 3`, select K random members (default K=3) and ask them to indirect-ping the target.
3. If still no ack after `suspicion_timeout`, mark the member as `Suspicious`.
4. If no ack after `dead_timeout`, mark as `Dead`. Broadcast state change via piggybacked gossip.
5. Membership changes (join, leave, suspicion, death) piggyback on all protocol messages to propagate quickly.

**Node join:**

```
New node starts
  -> contacts seed node(s) from config
  -> receives full membership list
  -> begins replication log sync from all peers
  -> once caught up, announces itself to cluster
  -> begins accepting client traffic
```

**Anti-entropy:**

Each node maintains a sequence vector: `HashMap<u64, u64>` mapping `node_id -> latest_sequence_received`.

Periodically (every 10s), nodes exchange sequence vectors with a random peer. Missing entries are requested and replayed. This handles:
- Network partitions (entries replayed on reconnect)
- Late joiners (full sync from peers)
- Message loss (detected and repaired)

### 4.6 Partition Tolerance

**During partition:**
- Both sides continue accepting reads and writes (AP in CAP).
- DML operations proceed locally.
- DDL operations require quorum (see below).

**On partition heal:**
1. Nodes exchange replication logs for the partition duration.
2. CRDT merge / LWW resolution runs on all divergent documents.
3. Cluster converges to consistent state.

**DDL quorum requirement:**

DDL operations (create/drop database, create/drop collection, schema changes, index changes) require acknowledgment from a majority of nodes:

```rust
pub async fn quorum_ddl<F, Fut>(
    &self,
    operation: F,
) -> Result<(), VantaError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<(), VantaError>>,
{
    let total_nodes = self.membership.alive_count();
    let quorum = total_nodes / 2 + 1;

    // Propose to peers
    let mut acks = 1;  // self
    for peer in self.membership.alive_peers() {
        match peer.propose_ddl(&self.pending_ddl).await {
            Ok(()) => acks += 1,
            Err(e) => tracing::warn!("DDL proposal rejected by {}: {}", peer.node_id, e),
        }
    }

    if acks >= quorum {
        operation().await?;
        // Broadcast commit to peers
        self.broadcast_ddl_commit().await;
        Ok(())
    } else {
        Err(VantaError::Internal(format!(
            "DDL quorum not reached: got {}/{} (need {})",
            acks, total_nodes, quorum
        )))
    }
}
```

If a minority partition receives DDL, it rejects with a clear error. DML continues unaffected.

### 4.7 Tunable Read Consistency

Configurable per-request via gRPC metadata:

```rust
pub enum ReadConsistency {
    /// Read from local node. Fastest. May return stale data.
    Local,
    /// Read from majority of nodes. Guaranteed to return latest committed value.
    Quorum,
    /// Linearizable read. Contacts leader of the quorum. Highest latency.
    Linearizable,
}
```

Default: `Local` (configured in `[replication]` section of config).

Client override via gRPC metadata key `x-read-consistency`:

```rust
fn extract_read_consistency<T>(request: &Request<T>) -> ReadConsistency {
    request.metadata()
        .get("x-read-consistency")
        .and_then(|v| v.to_str().ok())
        .map(|s| match s {
            "quorum" => ReadConsistency::Quorum,
            "linearizable" => ReadConsistency::Linearizable,
            _ => ReadConsistency::Local,
        })
        .unwrap_or(ReadConsistency::Local)
}
```

**Quorum read implementation:**

1. Send read request to `N/2 + 1` nodes (including self).
2. Collect responses.
3. Return the value with the highest HLC timestamp.

**Linearizable read implementation:**

1. Send a "read barrier" to the quorum.
2. Each node flushes its pending replication queue.
3. Once quorum confirms, read from local (now guaranteed up-to-date).

---

## Phase Dependencies

```
Phase 1 (Storage & Data Integrity)
    |
    +---> Phase 2 (Security & Zero-Trust)
    |         |
    |         +---> Phase 3 (Operational Readiness)
    |                   |
    |                   +---> Phase 4 (Distributed System)
    |                               |
    +-------------------------------+
```

- Phase 2 depends on Phase 1: JWT and ACLs integrate with the new transaction/storage layer.
- Phase 3 depends on Phase 1: metrics instrument the new engine; tests validate it.
- Phase 4 depends on all previous phases: replication replicates MVCC versions, uses mTLS transport, instruments with metrics.
- Within each phase, the parallel tracks listed in the overview can be developed concurrently by different team members.

---

## New Crate Dependencies

| Crate | Phase | Purpose |
|-------|-------|---------|
| `crc32c` | 1 | WAL record checksums |
| `jsonwebtoken` | 2 | JWT signing/verification |
| `rustls` + `rustls-pemfile` | 2 | mTLS transport |
| `rcgen` | 2 | Built-in CA certificate generation |
| `governor` | 2 | Token bucket rate limiting |
| `tracing` + `tracing-subscriber` | 3 | Structured logging |
| `metrics` + `metrics-exporter-prometheus` | 3 | Prometheus metrics |
| `toml` | 3 | Config file parsing |
| `proptest` | 3 | Property-based testing |
| `tonic-health` | 3 | gRPC health check protocol |
| `prost` (already present) | 4 | Replication protocol messages |
