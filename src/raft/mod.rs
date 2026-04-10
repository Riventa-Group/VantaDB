pub mod state_machine;
pub mod storage;
pub mod types;

pub use state_machine::VantaStateMachine;
pub use storage::VantaLogStore;
pub use types::{RaftOp, RaftResponse, VantaRaft, VantaRaftConfig};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DatabaseManager;
    use crate::storage::StorageEngine;
    use openraft::{BasicNode, Config, Raft};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_single_node_raft_write() {
        let dir = TempDir::new().unwrap();
        let data_path = dir.path().join("data");
        std::fs::create_dir_all(&data_path).unwrap();

        // Create storage engine for Raft log
        let raft_engine = Arc::new(StorageEngine::open(&dir.path().join("raft")).unwrap());
        let log_store = VantaLogStore::new(Arc::clone(&raft_engine)).unwrap();

        // Create DatabaseManager
        let db_manager = Arc::new(DatabaseManager::new(&data_path).unwrap());
        let state_machine = VantaStateMachine::new(Arc::clone(&db_manager), dir.path().to_path_buf());

        // Create Raft config
        let config = Arc::new(
            Config {
                heartbeat_interval: 500,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                ..Default::default()
            }
        );

        let node_id = 1u64;

        // Create Raft instance
        let raft = Raft::new(node_id, config, DummyNetwork, log_store, state_machine)
            .await
            .expect("Failed to create Raft instance");

        // Initialize as single-node cluster
        let mut members = BTreeMap::new();
        members.insert(node_id, BasicNode::default());
        raft.initialize(members)
            .await
            .expect("Failed to initialize Raft");

        // Wait for leader election
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Propose a write: create database
        let resp = raft
            .client_write(RaftOp::CreateDatabase {
                name: "test_raft_db".to_string(),
            })
            .await;

        assert!(resp.is_ok(), "Raft write failed: {:?}", resp.err());

        // Verify the database was created via DatabaseManager
        let dbs = db_manager.list_databases();
        assert!(dbs.contains(&"test_raft_db".to_string()), "Database not found after Raft write");

        // Shutdown raft
        let _ = raft.shutdown().await;
    }

    /// Dummy network for single-node testing — no actual communication needed.
    struct DummyNetwork;

    impl openraft::RaftNetworkFactory<VantaRaftConfig> for DummyNetwork {
        type Network = DummyNetworkConn;

        async fn new_client(
            &mut self,
            _target: u64,
            _node: &BasicNode,
        ) -> Self::Network {
            DummyNetworkConn
        }
    }

    struct DummyNetworkConn;

    impl openraft::RaftNetwork<VantaRaftConfig> for DummyNetworkConn {
        async fn append_entries(
            &mut self,
            _rpc: openraft::raft::AppendEntriesRequest<VantaRaftConfig>,
            _option: openraft::network::RPCOption,
        ) -> Result<
            openraft::raft::AppendEntriesResponse<u64>,
            openraft::error::RPCError<u64, BasicNode, openraft::error::RaftError<u64>>,
        > {
            Err(openraft::error::RPCError::Unreachable(openraft::error::Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::Other,
                "dummy network",
            ))))
        }

        async fn install_snapshot(
            &mut self,
            _rpc: openraft::raft::InstallSnapshotRequest<VantaRaftConfig>,
            _option: openraft::network::RPCOption,
        ) -> Result<
            openraft::raft::InstallSnapshotResponse<u64>,
            openraft::error::RPCError<u64, BasicNode, openraft::error::RaftError<u64, openraft::error::InstallSnapshotError>>,
        > {
            Err(openraft::error::RPCError::Unreachable(openraft::error::Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::Other,
                "dummy network",
            ))))
        }

        async fn vote(
            &mut self,
            _rpc: openraft::raft::VoteRequest<u64>,
            _option: openraft::network::RPCOption,
        ) -> Result<
            openraft::raft::VoteResponse<u64>,
            openraft::error::RPCError<u64, BasicNode, openraft::error::RaftError<u64>>,
        > {
            Err(openraft::error::RPCError::Unreachable(openraft::error::Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::Other,
                "dummy network",
            ))))
        }
    }
}
