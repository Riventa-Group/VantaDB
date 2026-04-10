use std::io::Cursor;
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{Entry, EntryPayload, LogId, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership};
use parking_lot::Mutex;

use crate::db::DatabaseManager;
use super::types::{RaftOp, RaftResponse, VantaRaftConfig};

fn io_err(e: impl std::fmt::Display) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::write_logs(openraft::AnyError::new(&std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        ))),
    }
}

/// Raft state machine that applies committed operations to DatabaseManager.
pub struct VantaStateMachine {
    db_manager: Arc<DatabaseManager>,
    last_applied: Mutex<Option<LogId<u64>>>,
    last_membership: Mutex<StoredMembership<u64, openraft::BasicNode>>,
    data_dir: std::path::PathBuf,
}

impl VantaStateMachine {
    pub fn new(db_manager: Arc<DatabaseManager>, data_dir: std::path::PathBuf) -> Self {
        Self {
            db_manager,
            last_applied: Mutex::new(None),
            last_membership: Mutex::new(StoredMembership::default()),
            data_dir,
        }
    }

    fn apply_op(&self, op: &RaftOp) -> RaftResponse {
        match op {
            RaftOp::Insert { db, col, doc } => {
                match self.db_manager.insert(db, col, doc.clone()) {
                    Ok(id) => RaftResponse::InsertOk { id },
                    Err(e) => RaftResponse::Error { message: e.to_string() },
                }
            }
            RaftOp::Update { db, col, id, patch } => {
                match self.db_manager.update_by_id(db, col, id, patch.clone()) {
                    Ok(_) => RaftResponse::Ok,
                    Err(e) => RaftResponse::Error { message: e.to_string() },
                }
            }
            RaftOp::Delete { db, col, id } => {
                match self.db_manager.delete_by_id(db, col, id) {
                    Ok(_) => RaftResponse::Ok,
                    Err(e) => RaftResponse::Error { message: e.to_string() },
                }
            }
            RaftOp::UpdateWhere { db, col, filter, patch } => {
                match self.db_manager.update_where(db, col, filter, patch) {
                    Ok(_) => RaftResponse::Ok,
                    Err(e) => RaftResponse::Error { message: e.to_string() },
                }
            }
            RaftOp::CreateDatabase { name } => {
                match self.db_manager.create_database(name) {
                    Ok(_) => RaftResponse::Ok,
                    Err(e) => RaftResponse::Error { message: e.to_string() },
                }
            }
            RaftOp::DropDatabase { name } => {
                match self.db_manager.drop_database(name) {
                    Ok(_) => RaftResponse::Ok,
                    Err(e) => RaftResponse::Error { message: e.to_string() },
                }
            }
            RaftOp::CreateCollection { db, col } => {
                match self.db_manager.create_collection(db, col) {
                    Ok(_) => RaftResponse::Ok,
                    Err(e) => RaftResponse::Error { message: e.to_string() },
                }
            }
            RaftOp::DropCollection { db, col } => {
                match self.db_manager.drop_collection(db, col) {
                    Ok(_) => RaftResponse::Ok,
                    Err(e) => RaftResponse::Error { message: e.to_string() },
                }
            }
            RaftOp::CreateIndex { db, col, field, unique } => {
                match self.db_manager.create_index(db, col, field, *unique) {
                    Ok(_) => RaftResponse::Ok,
                    Err(e) => RaftResponse::Error { message: e.to_string() },
                }
            }
            RaftOp::DropIndex { db, col, field } => {
                match self.db_manager.drop_index(db, col, field) {
                    Ok(_) => RaftResponse::Ok,
                    Err(e) => RaftResponse::Error { message: e.to_string() },
                }
            }
            RaftOp::SetSchema { db, col, schema } => {
                match crate::db::CollectionSchema::from_json(schema) {
                    Ok(s) => match self.db_manager.set_schema(db, col, &s) {
                        Ok(_) => RaftResponse::Ok,
                        Err(e) => RaftResponse::Error { message: e.to_string() },
                    },
                    Err(e) => RaftResponse::Error { message: e },
                }
            }
            RaftOp::DropSchema { db, col } => {
                match self.db_manager.drop_schema(db, col) {
                    Ok(_) => RaftResponse::Ok,
                    Err(e) => RaftResponse::Error { message: e.to_string() },
                }
            }
        }
    }
}

/// Snapshot builder that serializes all data using the backup format.
pub struct VantaSnapshotBuilder {
    db_manager: Arc<DatabaseManager>,
    data_dir: std::path::PathBuf,
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, openraft::BasicNode>,
}

impl RaftSnapshotBuilder<VantaRaftConfig> for VantaSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<VantaRaftConfig>, StorageError<u64>> {
        let backup_result = crate::db::backup::create_backup(&self.db_manager, &self.data_dir)
            .map_err(|e| io_err(e))?;

        let data = std::fs::read(&backup_result.path).map_err(|e| io_err(e))?;

        let snapshot_id = format!("snap-{}", backup_result.timestamp);

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<VantaRaftConfig> for VantaStateMachine {
    type SnapshotBuilder = VantaSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (Option<LogId<u64>>, StoredMembership<u64, openraft::BasicNode>),
        StorageError<u64>,
    > {
        let last_applied = *self.last_applied.lock();
        let membership = self.last_membership.lock().clone();
        Ok((last_applied, membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<VantaRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();

        for entry in entries {
            *self.last_applied.lock() = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(RaftResponse::Ok);
                }
                EntryPayload::Normal(op) => {
                    let resp = self.apply_op(&op);
                    responses.push(resp);
                }
                EntryPayload::Membership(membership) => {
                    *self.last_membership.lock() =
                        StoredMembership::new(Some(entry.log_id), membership);
                    responses.push(RaftResponse::Ok);
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        VantaSnapshotBuilder {
            db_manager: Arc::clone(&self.db_manager),
            data_dir: self.data_dir.clone(),
            last_applied: *self.last_applied.lock(),
            last_membership: self.last_membership.lock().clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        let snap_path = self.data_dir.join("_raft_snapshot.vbak");

        std::fs::write(&snap_path, &data).map_err(|e| io_err(e))?;

        crate::db::backup::restore_backup(
            &self.db_manager,
            snap_path.to_str().unwrap_or(""),
        )
        .map_err(|e| io_err(e))?;

        *self.last_applied.lock() = meta.last_log_id;
        *self.last_membership.lock() = meta.last_membership.clone();

        let _ = std::fs::remove_file(&snap_path);

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<VantaRaftConfig>>, StorageError<u64>> {
        let mut builder = self.get_snapshot_builder().await;
        match builder.build_snapshot().await {
            Ok(snap) => Ok(Some(snap)),
            Err(_) => Ok(None),
        }
    }
}
