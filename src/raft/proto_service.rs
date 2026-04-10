use std::collections::BTreeMap;
use std::sync::Arc;
use std::path::PathBuf;
use tonic::{Request, Response, Status};

use openraft::BasicNode;
use crate::server::proto;
use crate::server::proto::vanta_raft_server::VantaRaft as VantaRaftTrait;
use super::membership::ClusterMembership;
use super::types::{VantaRaft, VantaRaftConfig};

/// gRPC service handler for inter-node Raft communication.
pub struct VantaRaftService {
    raft: Arc<VantaRaft>,
    data_dir: PathBuf,
}

impl VantaRaftService {
    pub fn new(raft: Arc<VantaRaft>, data_dir: PathBuf) -> Self {
        Self { raft, data_dir }
    }
}

#[tonic::async_trait]
impl VantaRaftTrait for VantaRaftService {
    async fn append_entries(
        &self,
        request: Request<proto::RaftMessage>,
    ) -> Result<Response<proto::RaftMessage>, Status> {
        let data = request.into_inner().data;
        let rpc: openraft::raft::AppendEntriesRequest<VantaRaftConfig> =
            bincode::deserialize(&data)
                .map_err(|e| Status::internal(format!("Deserialize error: {}", e)))?;

        let resp = self
            .raft
            .append_entries(rpc)
            .await
            .map_err(|e| Status::internal(format!("Raft error: {}", e)))?;

        let resp_data = bincode::serialize(&resp)
            .map_err(|e| Status::internal(format!("Serialize error: {}", e)))?;

        Ok(Response::new(proto::RaftMessage { data: resp_data }))
    }

    async fn install_snapshot(
        &self,
        request: Request<proto::RaftMessage>,
    ) -> Result<Response<proto::RaftMessage>, Status> {
        let data = request.into_inner().data;
        let rpc: openraft::raft::InstallSnapshotRequest<VantaRaftConfig> =
            bincode::deserialize(&data)
                .map_err(|e| Status::internal(format!("Deserialize error: {}", e)))?;

        let resp = self
            .raft
            .install_snapshot(rpc)
            .await
            .map_err(|e| Status::internal(format!("Raft error: {}", e)))?;

        let resp_data = bincode::serialize(&resp)
            .map_err(|e| Status::internal(format!("Serialize error: {}", e)))?;

        Ok(Response::new(proto::RaftMessage { data: resp_data }))
    }

    async fn vote(
        &self,
        request: Request<proto::RaftMessage>,
    ) -> Result<Response<proto::RaftMessage>, Status> {
        let data = request.into_inner().data;
        let rpc: openraft::raft::VoteRequest<u64> =
            bincode::deserialize(&data)
                .map_err(|e| Status::internal(format!("Deserialize error: {}", e)))?;

        let resp = self
            .raft
            .vote(rpc)
            .await
            .map_err(|e| Status::internal(format!("Raft error: {}", e)))?;

        let resp_data = bincode::serialize(&resp)
            .map_err(|e| Status::internal(format!("Serialize error: {}", e)))?;

        Ok(Response::new(proto::RaftMessage { data: resp_data }))
    }

    async fn join_cluster(
        &self,
        request: Request<proto::JoinRequest>,
    ) -> Result<Response<proto::JoinResponse>, Status> {
        let req = request.into_inner();
        let node_id = req.node_id;
        let addr = req.advertise_addr;

        // Get current membership and add the new node
        let metrics = self.raft.metrics().borrow().clone();

        // Add node as learner first, then promote to voter
        let node = BasicNode { addr: addr.clone() };
        self.raft
            .add_learner(node_id, node, true)
            .await
            .map_err(|e| Status::internal(format!("Add learner failed: {}", e)))?;

        // Get current voter ids and add the new node
        let mut voter_ids: BTreeMap<u64, BasicNode> = metrics
            .membership_config
            .membership()
            .get_joint_config()
            .iter()
            .flat_map(|set| set.iter())
            .filter_map(|&id| {
                metrics.membership_config.membership().get_node(&id)
                    .map(|n| (id, n.clone()))
            })
            .collect();

        voter_ids.insert(node_id, BasicNode { addr: addr.clone() });

        self.raft
            .change_membership(voter_ids.keys().copied().collect::<std::collections::BTreeSet<_>>(), false)
            .await
            .map_err(|e| Status::internal(format!("Change membership failed: {}", e)))?;

        let leader_id = metrics.current_leader.unwrap_or(0);
        let leader_addr = metrics.membership_config.membership()
            .get_node(&leader_id)
            .map(|n| n.addr.clone())
            .unwrap_or_default();

        Ok(Response::new(proto::JoinResponse {
            success: true,
            error: String::new(),
            leader_id,
            leader_addr,
        }))
    }

    async fn leave_cluster(
        &self,
        request: Request<proto::LeaveRequest>,
    ) -> Result<Response<proto::StatusResponse>, Status> {
        let req = request.into_inner();
        let node_id = req.node_id;

        let metrics = self.raft.metrics().borrow().clone();

        // Remove node from voters
        let voter_ids: std::collections::BTreeSet<u64> = metrics
            .membership_config
            .membership()
            .get_joint_config()
            .iter()
            .flat_map(|set| set.iter())
            .copied()
            .filter(|&id| id != node_id)
            .collect();

        if voter_ids.is_empty() {
            return Err(Status::failed_precondition("Cannot remove the last node"));
        }

        self.raft
            .change_membership(voter_ids, false)
            .await
            .map_err(|e| Status::internal(format!("Change membership failed: {}", e)))?;

        // Delete cluster.json on the departing node's data dir
        let _ = ClusterMembership::delete(&self.data_dir);

        Ok(Response::new(proto::StatusResponse {
            success: true,
            error: String::new(),
        }))
    }
}
