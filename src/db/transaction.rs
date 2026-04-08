use parking_lot::Mutex;
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

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

/// An in-progress transaction that buffers operations.
#[derive(Debug)]
pub struct Transaction {
    pub id: String,
    pub ops: Vec<TransactionOp>,
}

impl Transaction {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            ops: Vec::new(),
        }
    }
}

/// Manages active transactions.
pub struct TransactionManager {
    active: Mutex<HashMap<String, Transaction>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            active: Mutex::new(HashMap::new()),
        }
    }

    pub fn begin(&self) -> String {
        let tx = Transaction::new();
        let id = tx.id.clone();
        self.active.lock().insert(id.clone(), tx);
        id
    }

    pub fn add_op(&self, tx_id: &str, op: TransactionOp) -> Result<(), String> {
        self.active
            .lock()
            .get_mut(tx_id)
            .map(|tx| tx.ops.push(op))
            .ok_or_else(|| format!("Transaction '{}' not found", tx_id))
    }

    pub fn take(&self, tx_id: &str) -> Result<Transaction, String> {
        self.active
            .lock()
            .remove(tx_id)
            .ok_or_else(|| format!("Transaction '{}' not found", tx_id))
    }

    pub fn rollback(&self, tx_id: &str) -> Result<(), String> {
        self.active
            .lock()
            .remove(tx_id)
            .map(|_| ())
            .ok_or_else(|| format!("Transaction '{}' not found", tx_id))
    }
}
