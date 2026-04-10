use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::{Entry, LogId, LogState, RaftLogReader, StorageError, StorageIOError, Vote};
use parking_lot::Mutex;

use crate::storage::StorageEngine;
use super::types::VantaRaftConfig;

const LOG_TABLE: &str = "_raft_log";
const META_TABLE: &str = "_raft_meta";

fn io_err(e: impl std::fmt::Display) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::write_logs(openraft::AnyError::new(&std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        ))),
    }
}

/// Raft log storage backed by VantaDB's StorageEngine (WAL + DashMap).
pub struct VantaLogStore {
    engine: Arc<StorageEngine>,
    last_purged: Mutex<Option<LogId<u64>>>,
}

impl VantaLogStore {
    pub fn new(engine: Arc<StorageEngine>) -> std::io::Result<Self> {
        if !engine.table_exists(LOG_TABLE) {
            engine.create_table(LOG_TABLE)?;
        }
        if !engine.table_exists(META_TABLE) {
            engine.create_table(META_TABLE)?;
        }

        let last_purged = engine
            .get(META_TABLE, "last_purged")
            .and_then(|data| bincode::deserialize(&data).ok());

        Ok(Self {
            engine,
            last_purged: Mutex::new(last_purged),
        })
    }

    fn log_key(index: u64) -> String {
        format!("{:020}", index)
    }
}

impl RaftLogReader<VantaRaftConfig> for VantaLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<VantaRaftConfig>>, StorageError<u64>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&v) => v,
            std::ops::Bound::Excluded(&v) => v + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&v) => v + 1,
            std::ops::Bound::Excluded(&v) => v,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        let mut entries = Vec::new();
        for index in start..end {
            let key = Self::log_key(index);
            if let Some(data) = self.engine.get(LOG_TABLE, &key) {
                if let Ok(entry) = bincode::deserialize::<Entry<VantaRaftConfig>>(&data) {
                    entries.push(entry);
                }
            } else if index > start {
                // Hit a gap — stop
                break;
            }
        }

        Ok(entries)
    }
}

impl RaftLogStorage<VantaRaftConfig> for VantaLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<VantaRaftConfig>, StorageError<u64>> {
        let last_purged = *self.last_purged.lock();

        let mut keys = self.engine.list_keys(LOG_TABLE);
        keys.sort();

        let last_log_id = if let Some(last_key) = keys.last() {
            self.engine
                .get(LOG_TABLE, last_key)
                .and_then(|d| bincode::deserialize::<Entry<VantaRaftConfig>>(&d).ok())
                .map(|e| e.log_id)
        } else {
            last_purged
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        VantaLogStore {
            engine: Arc::clone(&self.engine),
            last_purged: Mutex::new(*self.last_purged.lock()),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let data = bincode::serialize(vote).map_err(|e| io_err(e))?;
        self.engine.put(META_TABLE, "vote", &data).map_err(|e| io_err(e))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self
            .engine
            .get(META_TABLE, "vote")
            .and_then(|data| bincode::deserialize(&data).ok()))
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<VantaRaftConfig>) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<VantaRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        for entry in entries {
            let key = Self::log_key(entry.log_id.index);
            let data = bincode::serialize(&entry).map_err(|e| io_err(e))?;
            self.engine.put(LOG_TABLE, &key, &data).map_err(|e| io_err(e))?;
        }

        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let keys: Vec<String> = self
            .engine
            .list_keys(LOG_TABLE)
            .into_iter()
            .filter(|k| {
                k.parse::<u64>().map_or(false, |idx| idx >= log_id.index)
            })
            .collect();

        for key in keys {
            let _ = self.engine.delete(LOG_TABLE, &key);
        }

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let keys: Vec<String> = self
            .engine
            .list_keys(LOG_TABLE)
            .into_iter()
            .filter(|k| {
                k.parse::<u64>().map_or(false, |idx| idx <= log_id.index)
            })
            .collect();

        for key in keys {
            let _ = self.engine.delete(LOG_TABLE, &key);
        }

        *self.last_purged.lock() = Some(log_id);
        let data = bincode::serialize(&log_id).map_err(|e| io_err(e))?;
        self.engine.put(META_TABLE, "last_purged", &data).map_err(|e| io_err(e))?;

        Ok(())
    }
}
