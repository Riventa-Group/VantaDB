use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::server::proto;
use crate::server::proto::vanta_raft_server::VantaRaft as VantaRaftTrait;
use super::types::{VantaRaft, VantaRaftConfig};

/// gRPC service handler for inter-node Raft communication.
/// Receives Raft protocol messages from peers and forwards them to the local Raft instance.
pub struct VantaRaftService {
    raft: Arc<VantaRaft>,
}

impl VantaRaftService {
    pub fn new(raft: Arc<VantaRaft>) -> Self {
        Self { raft }
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
}
