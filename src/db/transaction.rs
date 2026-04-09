use ahash::RandomState;
use dashmap::DashMap;
use parking_lot::Mutex;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

use super::error::VantaError;

// ---- Transaction Operations ---------------------------------

/// A single operation within a transaction.
#[derive(Debug, Clone)]
pub enum TransactionOp {
    Insert {
        db: String,
        collection: String,
        document: Value,
    },
    Update {
        db: String,
        collection: String,
        id: String,
        patch: Value,
    },
    Delete {
        db: String,
        collection: String,
        id: String,
    },
}

// ---- Workspace Entry ----------------------------------------

/// Buffered write in a transaction's workspace (read-your-own-writes).
#[derive(Debug, Clone)]
pub enum WorkspaceEntry {
    Insert(Vec<u8>),
    Update(Vec<u8>),
    Delete,
}

// ---- Lock Table ---------------------------------------------

/// Row-level lock: (collection, doc_id) -> LockEntry
struct LockEntry {
    owner_tx: Uuid,
    wait_queue: VecDeque<(Uuid, tokio::sync::oneshot::Sender<()>)>,
}

/// Per-row lock table for S2PL.
/// Transactions acquire exclusive locks on keys they write.
pub struct LockTable {
    locks: DashMap<(String, String), LockEntry, RandomState>,
}

impl LockTable {
    pub fn new() -> Self {
        Self {
            locks: DashMap::with_hasher(RandomState::new()),
        }
    }

    /// Try to acquire an exclusive lock on (collection, doc_id) for tx.
    /// Returns Ok(()) if acquired, or the Uuid of the current holder if blocked.
    pub fn try_acquire(&self, collection: &str, doc_id: &str, tx_id: Uuid) -> Result<(), Uuid> {
        let key = (collection.to_string(), doc_id.to_string());
        match self.locks.entry(key) {
            dashmap::mapref::entry::Entry::Vacant(e) => {
                e.insert(LockEntry {
                    owner_tx: tx_id,
                    wait_queue: VecDeque::new(),
                });
                Ok(())
            }
            dashmap::mapref::entry::Entry::Occupied(e) => {
                let entry = e.get();
                if entry.owner_tx == tx_id {
                    Ok(()) // already held by this tx
                } else {
                    Err(entry.owner_tx)
                }
            }
        }
    }

    /// Release all locks held by a transaction, waking the next waiter.
    pub fn release_all(&self, tx_id: Uuid) {
        let mut to_remove = Vec::new();
        let mut to_transfer = Vec::new();

        for entry in self.locks.iter() {
            if entry.value().owner_tx == tx_id {
                to_remove.push(entry.key().clone());
            }
        }

        for key in to_remove {
            if let Some(mut entry) = self.locks.get_mut(&key) {
                if entry.owner_tx != tx_id {
                    continue;
                }
                // Transfer lock to next waiter
                if let Some((next_tx, sender)) = entry.wait_queue.pop_front() {
                    entry.owner_tx = next_tx;
                    to_transfer.push(sender);
                } else {
                    drop(entry);
                    self.locks.remove(&key);
                }
            }
        }

        // Wake waiters outside the lock
        for sender in to_transfer {
            let _ = sender.send(());
        }
    }
}

// ---- Wait-For Graph -----------------------------------------

/// Deadlock detection via wait-for graph cycle detection.
pub struct WaitForGraph {
    edges: Mutex<HashMap<Uuid, HashSet<Uuid>>>,
}

impl WaitForGraph {
    pub fn new() -> Self {
        Self {
            edges: Mutex::new(HashMap::new()),
        }
    }

    /// Add an edge: `waiter` is waiting for `holder`.
    pub fn add_edge(&self, waiter: Uuid, holder: Uuid) {
        self.edges
            .lock()
            .entry(waiter)
            .or_insert_with(HashSet::new)
            .insert(holder);
    }

    /// Remove all edges from a transaction (when it commits, aborts, or acquires its lock).
    pub fn remove_node(&self, tx_id: Uuid) {
        let mut edges = self.edges.lock();
        edges.remove(&tx_id);
        // Also remove as a target from other nodes
        for targets in edges.values_mut() {
            targets.remove(&tx_id);
        }
    }

    /// Check if adding waiter -> holder would create a cycle (deadlock).
    /// Uses BFS from holder to see if we can reach waiter.
    pub fn would_deadlock(&self, waiter: Uuid, holder: Uuid) -> bool {
        let edges = self.edges.lock();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(holder);

        while let Some(node) = queue.pop_front() {
            if node == waiter {
                return true;
            }
            if visited.insert(node) {
                if let Some(targets) = edges.get(&node) {
                    for &t in targets {
                        queue.push_back(t);
                    }
                }
            }
        }
        false
    }
}

// ---- Transaction State --------------------------------------

/// An active transaction with S2PL + MVCC.
pub struct Transaction {
    pub id: Uuid,
    pub id_str: String,
    /// MVCC snapshot version at BEGIN time.
    pub start_version: u64,
    /// Buffered operations (applied on commit).
    pub ops: Vec<TransactionOp>,
    /// Workspace for read-your-own-writes: (collection, doc_id) -> entry.
    pub workspace: HashMap<(String, String), WorkspaceEntry>,
    /// Read set for validation: (collection, doc_id, version_read).
    pub read_set: Vec<(String, String, u64)>,
    /// Keys locked by this transaction.
    pub held_locks: Vec<(String, String)>,
    /// When this transaction started.
    pub started_at: Instant,
    /// Maximum lifetime before forced abort.
    pub timeout: Duration,
}

impl Transaction {
    pub fn new(start_version: u64, timeout: Duration) -> Self {
        let id = Uuid::new_v4();
        Self {
            id,
            id_str: id.to_string(),
            start_version,
            ops: Vec::new(),
            workspace: HashMap::new(),
            read_set: Vec::new(),
            held_locks: Vec::new(),
            started_at: Instant::now(),
            timeout,
        }
    }

    /// Check if this transaction has exceeded its timeout.
    pub fn is_expired(&self) -> bool {
        self.started_at.elapsed() > self.timeout
    }
}

// ---- Transaction Manager ------------------------------------

/// Serializable transaction manager with S2PL locking, MVCC reads,
/// deadlock detection, and timeout-based reaping.
///
/// Fixes:
/// - #5: Transactions are now atomic — all ops committed via WAL in one batch
/// - #6: Snapshot isolation via MVCC + S2PL write locks
/// - #7: Timeout reaper forcibly aborts abandoned transactions
pub struct TransactionManager {
    active: DashMap<String, Transaction, RandomState>,
    lock_table: Arc<LockTable>,
    wait_graph: Arc<WaitForGraph>,
    default_timeout: Duration,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            active: DashMap::with_hasher(RandomState::new()),
            lock_table: Arc::new(LockTable::new()),
            wait_graph: Arc::new(WaitForGraph::new()),
            default_timeout: Duration::from_secs(30),
        }
    }

    /// Begin a new transaction at the given MVCC snapshot version.
    pub fn begin_at(&self, start_version: u64) -> String {
        let tx = Transaction::new(start_version, self.default_timeout);
        let id = tx.id_str.clone();
        self.active.insert(id.clone(), tx);
        id
    }

    /// Begin a transaction (legacy — uses version 0, for backward compat).
    pub fn begin(&self) -> String {
        self.begin_at(0)
    }

    /// Buffer an operation in a transaction.
    pub fn add_op(&self, tx_id: &str, op: TransactionOp) -> Result<(), VantaError> {
        let mut tx = self.active.get_mut(tx_id).ok_or_else(|| VantaError::NotFound {
            entity: "Transaction",
            name: tx_id.to_string(),
        })?;

        if tx.is_expired() {
            let uuid = tx.id;
            drop(tx);
            self.abort_internal(tx_id, uuid);
            return Err(VantaError::TransactionTimeout {
                tx_id: tx_id.to_string(),
            });
        }

        tx.ops.push(op);
        Ok(())
    }

    /// Record a key in the transaction's workspace (for read-your-own-writes).
    pub fn workspace_put(
        &self,
        tx_id: &str,
        collection: &str,
        doc_id: &str,
        entry: WorkspaceEntry,
    ) -> Result<(), VantaError> {
        let mut tx = self.active.get_mut(tx_id).ok_or_else(|| VantaError::NotFound {
            entity: "Transaction",
            name: tx_id.to_string(),
        })?;
        tx.workspace
            .insert((collection.to_string(), doc_id.to_string()), entry);
        Ok(())
    }

    /// Check workspace for a read-your-own-writes hit.
    pub fn workspace_get(
        &self,
        tx_id: &str,
        collection: &str,
        doc_id: &str,
    ) -> Option<WorkspaceEntry> {
        let tx = self.active.get(tx_id)?;
        tx.workspace
            .get(&(collection.to_string(), doc_id.to_string()))
            .cloned()
    }

    /// Record a read in the transaction's read set (for commit-time validation).
    pub fn record_read(
        &self,
        tx_id: &str,
        collection: &str,
        doc_id: &str,
        version_read: u64,
    ) -> Result<(), VantaError> {
        let mut tx = self.active.get_mut(tx_id).ok_or_else(|| VantaError::NotFound {
            entity: "Transaction",
            name: tx_id.to_string(),
        })?;
        tx.read_set
            .push((collection.to_string(), doc_id.to_string(), version_read));
        Ok(())
    }

    /// Try to acquire a write lock on (collection, doc_id).
    /// Returns Err(Deadlock) if acquiring would cause a deadlock.
    pub fn acquire_lock(
        &self,
        tx_id: &str,
        collection: &str,
        doc_id: &str,
    ) -> Result<(), VantaError> {
        let tx = self.active.get(tx_id).ok_or_else(|| VantaError::NotFound {
            entity: "Transaction",
            name: tx_id.to_string(),
        })?;
        let uuid = tx.id;

        if tx.is_expired() {
            drop(tx);
            self.abort_internal(tx_id, uuid);
            return Err(VantaError::TransactionTimeout {
                tx_id: tx_id.to_string(),
            });
        }
        drop(tx);

        match self.lock_table.try_acquire(collection, doc_id, uuid) {
            Ok(()) => {
                // Record that we hold this lock
                if let Some(mut tx) = self.active.get_mut(tx_id) {
                    tx.held_locks
                        .push((collection.to_string(), doc_id.to_string()));
                }
                // Remove any stale wait-graph edges for this tx
                self.wait_graph.remove_node(uuid);
                Ok(())
            }
            Err(holder_uuid) => {
                // Check for deadlock: would adding waiter->holder create a cycle?
                if self.wait_graph.would_deadlock(uuid, holder_uuid) {
                    return Err(VantaError::Deadlock {
                        tx_id: tx_id.to_string(),
                    });
                }
                // Record the wait edge for future deadlock detection
                self.wait_graph.add_edge(uuid, holder_uuid);
                // Fail-immediate: client retries
                Err(VantaError::TransactionConflict {
                    tx_id: tx_id.to_string(),
                    reason: "Lock held by another transaction".to_string(),
                })
            }
        }
    }

    /// Get the snapshot version for a transaction.
    pub fn get_snapshot_version(&self, tx_id: &str) -> Result<u64, VantaError> {
        self.active
            .get(tx_id)
            .map(|tx| tx.start_version)
            .ok_or_else(|| VantaError::NotFound {
                entity: "Transaction",
                name: tx_id.to_string(),
            })
    }

    /// Take a transaction out for commit (consumes it from active set).
    /// The caller is responsible for:
    /// 1. Validating the read set
    /// 2. Writing to WAL
    /// 3. Applying to MVCC store
    /// 4. Releasing locks
    pub fn take(&self, tx_id: &str) -> Result<Transaction, VantaError> {
        let (_, tx) = self.active.remove(tx_id).ok_or_else(|| VantaError::NotFound {
            entity: "Transaction",
            name: tx_id.to_string(),
        })?;

        if tx.is_expired() {
            self.lock_table.release_all(tx.id);
            self.wait_graph.remove_node(tx.id);
            return Err(VantaError::TransactionTimeout {
                tx_id: tx_id.to_string(),
            });
        }

        Ok(tx)
    }

    /// Rollback a transaction: discard workspace, release locks.
    pub fn rollback(&self, tx_id: &str) -> Result<(), VantaError> {
        let (_, tx) = self.active.remove(tx_id).ok_or_else(|| VantaError::NotFound {
            entity: "Transaction",
            name: tx_id.to_string(),
        })?;
        self.lock_table.release_all(tx.id);
        self.wait_graph.remove_node(tx.id);
        Ok(())
    }

    /// Release locks and clean up wait graph for a committed/aborted transaction.
    pub fn finalize(&self, tx: &Transaction) {
        self.lock_table.release_all(tx.id);
        self.wait_graph.remove_node(tx.id);
    }

    /// Internal abort: release locks and remove from active set.
    fn abort_internal(&self, tx_id: &str, uuid: Uuid) {
        self.active.remove(tx_id);
        self.lock_table.release_all(uuid);
        self.wait_graph.remove_node(uuid);
    }

    /// Reap expired transactions (#7).
    /// Scans all active transactions and forcibly aborts any that have
    /// exceeded their timeout. Returns the number of transactions reaped.
    pub fn reap_expired(&self) -> usize {
        let mut expired = Vec::new();
        for entry in self.active.iter() {
            if entry.value().is_expired() {
                expired.push((entry.key().clone(), entry.value().id));
            }
        }
        let count = expired.len();
        for (tx_id, uuid) in expired {
            self.abort_internal(&tx_id, uuid);
        }
        count
    }

    /// Get the number of active transactions.
    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    /// List active transaction IDs (for diagnostics).
    pub fn active_ids(&self) -> Vec<String> {
        self.active.iter().map(|e| e.key().clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_begin_and_take() {
        let tm = TransactionManager::new();
        let id = tm.begin_at(42);
        assert_eq!(tm.active_count(), 1);

        let tx = tm.take(&id).unwrap();
        assert_eq!(tx.start_version, 42);
        assert_eq!(tm.active_count(), 0);
    }

    #[test]
    fn test_add_op_to_expired_tx() {
        let tm = TransactionManager::new();
        // Create a tx with 0 timeout (immediately expired)
        let tx = Transaction::new(1, Duration::from_millis(0));
        let id = tx.id_str.clone();
        tm.active.insert(id.clone(), tx);

        std::thread::sleep(Duration::from_millis(1));
        let result = tm.add_op(
            &id,
            TransactionOp::Insert {
                db: "db".into(),
                collection: "col".into(),
                document: Value::Null,
            },
        );
        assert!(matches!(result, Err(VantaError::TransactionTimeout { .. })));
        assert_eq!(tm.active_count(), 0); // auto-aborted
    }

    #[test]
    fn test_rollback() {
        let tm = TransactionManager::new();
        let id = tm.begin_at(1);
        assert_eq!(tm.active_count(), 1);
        tm.rollback(&id).unwrap();
        assert_eq!(tm.active_count(), 0);
    }

    #[test]
    fn test_rollback_nonexistent() {
        let tm = TransactionManager::new();
        let result = tm.rollback("nonexistent");
        assert!(matches!(result, Err(VantaError::NotFound { .. })));
    }

    #[test]
    fn test_lock_acquire_and_release() {
        let lt = LockTable::new();
        let tx1 = Uuid::new_v4();
        let tx2 = Uuid::new_v4();

        assert!(lt.try_acquire("col", "doc1", tx1).is_ok());
        // Same tx can re-acquire
        assert!(lt.try_acquire("col", "doc1", tx1).is_ok());
        // Different tx blocked
        assert!(lt.try_acquire("col", "doc1", tx2).is_err());

        lt.release_all(tx1);
        // Now tx2 can acquire
        assert!(lt.try_acquire("col", "doc1", tx2).is_ok());
    }

    #[test]
    fn test_deadlock_detection() {
        let wfg = WaitForGraph::new();
        let tx1 = Uuid::new_v4();
        let tx2 = Uuid::new_v4();
        let tx3 = Uuid::new_v4();

        // tx1 -> tx2
        wfg.add_edge(tx1, tx2);
        // tx2 -> tx3
        wfg.add_edge(tx2, tx3);
        // tx3 -> tx1 would create cycle: tx1 -> tx2 -> tx3 -> tx1
        assert!(wfg.would_deadlock(tx3, tx1));
        // tx3 -> tx2 would also create cycle: tx2 -> tx3 -> tx2
        assert!(wfg.would_deadlock(tx3, tx2));
    }

    #[test]
    fn test_deadlock_detection_no_cycle() {
        let wfg = WaitForGraph::new();
        let tx1 = Uuid::new_v4();
        let tx2 = Uuid::new_v4();

        // No edges — no deadlock
        assert!(!wfg.would_deadlock(tx1, tx2));

        wfg.add_edge(tx1, tx2);
        // tx2 -> tx1 would create cycle: tx1 -> tx2 -> tx1
        assert!(wfg.would_deadlock(tx2, tx1));
    }

    #[test]
    fn test_reap_expired() {
        let tm = TransactionManager::new();

        // Normal tx
        let _id1 = tm.begin_at(1);

        // Expired tx
        let tx = Transaction::new(2, Duration::from_millis(0));
        let id2 = tx.id_str.clone();
        tm.active.insert(id2, tx);

        std::thread::sleep(Duration::from_millis(1));
        let reaped = tm.reap_expired();
        assert_eq!(reaped, 1);
        assert_eq!(tm.active_count(), 1); // only the normal tx remains
    }

    #[test]
    fn test_workspace_read_your_own_writes() {
        let tm = TransactionManager::new();
        let id = tm.begin_at(1);

        // No workspace entry yet
        assert!(tm.workspace_get(&id, "col", "doc1").is_none());

        // Write to workspace
        tm.workspace_put(&id, "col", "doc1", WorkspaceEntry::Insert(b"data".to_vec()))
            .unwrap();

        // Read from workspace
        let entry = tm.workspace_get(&id, "col", "doc1").unwrap();
        assert!(matches!(entry, WorkspaceEntry::Insert(ref d) if d == b"data"));

        tm.rollback(&id).unwrap();
    }

    #[test]
    fn test_lock_and_deadlock_integration() {
        let tm = TransactionManager::new();
        let id1 = tm.begin_at(1);
        let id2 = tm.begin_at(2);

        // tx1 locks doc1
        tm.acquire_lock(&id1, "col", "doc1").unwrap();

        // tx2 tries to lock doc1 — blocked (returns conflict)
        let result = tm.acquire_lock(&id2, "col", "doc1");
        assert!(matches!(
            result,
            Err(VantaError::TransactionConflict { .. })
        ));

        // tx2 locks doc2
        tm.acquire_lock(&id2, "col", "doc2").unwrap();

        // Clean up
        tm.rollback(&id1).unwrap();
        tm.rollback(&id2).unwrap();
    }

    #[test]
    fn test_deadlock_via_wait_graph_edges() {
        let tm = TransactionManager::new();
        let id1 = tm.begin_at(1);
        let id2 = tm.begin_at(2);

        // tx1 holds lock on A
        tm.acquire_lock(&id1, "col", "A").unwrap();
        // tx2 holds lock on B
        tm.acquire_lock(&id2, "col", "B").unwrap();

        // tx1 tries B → blocked, edge tx1->tx2 added to wait graph
        let r1 = tm.acquire_lock(&id1, "col", "B");
        assert!(matches!(r1, Err(VantaError::TransactionConflict { .. })));

        // tx2 tries A → would create cycle tx2->tx1->tx2 → deadlock!
        let r2 = tm.acquire_lock(&id2, "col", "A");
        assert!(matches!(r2, Err(VantaError::Deadlock { .. })));

        tm.rollback(&id1).unwrap();
        tm.rollback(&id2).unwrap();
    }

    #[test]
    fn test_no_deadlock_after_rollback() {
        let tm = TransactionManager::new();
        let id1 = tm.begin_at(1);
        let id2 = tm.begin_at(2);

        // tx1 holds A
        tm.acquire_lock(&id1, "col", "A").unwrap();

        // tx2 tries A → conflict, edge added
        let r = tm.acquire_lock(&id2, "col", "A");
        assert!(matches!(r, Err(VantaError::TransactionConflict { .. })));

        // tx1 rolls back → releases lock and clears wait graph
        tm.rollback(&id1).unwrap();

        // tx2 can now acquire A (no deadlock, no conflict)
        tm.acquire_lock(&id2, "col", "A").unwrap();

        tm.rollback(&id2).unwrap();
    }
}
