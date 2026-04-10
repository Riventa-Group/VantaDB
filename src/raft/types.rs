use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::Cursor;

/// Application-specific Raft operation.
/// Every mutating operation is represented as a variant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftOp {
    Insert { db: String, col: String, doc: Value },
    Update { db: String, col: String, id: String, patch: Value },
    Delete { db: String, col: String, id: String },
    UpdateWhere { db: String, col: String, filter: Value, patch: Value },
    CreateDatabase { name: String },
    DropDatabase { name: String },
    CreateCollection { db: String, col: String },
    DropCollection { db: String, col: String },
    CreateIndex { db: String, col: String, field: String, unique: bool },
    DropIndex { db: String, col: String, field: String },
    SetSchema { db: String, col: String, schema: Value },
    DropSchema { db: String, col: String },
}

/// Response from applying a RaftOp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftResponse {
    Ok,
    InsertOk { id: String },
    Error { message: String },
}

openraft::declare_raft_types!(
    pub VantaRaftConfig:
        D = RaftOp,
        R = RaftResponse,
        NodeId = u64,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<VantaRaftConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

pub type VantaRaft = openraft::Raft<VantaRaftConfig>;
