use openraft::error::{RPCError, RaftError, InstallSnapshotError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};
use tonic::transport::Channel;

use crate::server::proto;
use proto::vanta_raft_client::VantaRaftClient;
use super::types::VantaRaftConfig;

/// Creates gRPC connections to peer Raft nodes.
pub struct VantaNetworkFactory;

impl RaftNetworkFactory<VantaRaftConfig> for VantaNetworkFactory {
    type Network = VantaNetworkConn;

    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> Self::Network {
        VantaNetworkConn {
            addr: node.addr.clone(),
            client: None,
        }
    }
}

/// A gRPC connection to a single peer Raft node.
pub struct VantaNetworkConn {
    addr: String,
    client: Option<VantaRaftClient<Channel>>,
}

impl VantaNetworkConn {
    async fn ensure_client(&mut self) -> Result<&mut VantaRaftClient<Channel>, std::io::Error> {
        if self.client.is_none() {
            let addr = if self.addr.starts_with("http") {
                self.addr.clone()
            } else {
                format!("http://{}", self.addr)
            };
            let channel = Channel::from_shared(addr)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
                .connect()
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e))?;
            self.client = Some(VantaRaftClient::new(channel));
        }
        Ok(self.client.as_mut().unwrap())
    }
}

fn unreachable_err<E: std::error::Error>(e: impl std::fmt::Display) -> RPCError<u64, BasicNode, E> {
    RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
        std::io::ErrorKind::Other,
        e.to_string(),
    )))
}

impl RaftNetwork<VantaRaftConfig> for VantaNetworkConn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<VantaRaftConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64>>,
    > {
        let data = bincode::serialize(&rpc).map_err(|e| unreachable_err::<RaftError<u64>>(e))?;
        let client = self.ensure_client().await.map_err(|e| unreachable_err::<RaftError<u64>>(e))?;

        let resp = client
            .append_entries(proto::RaftMessage { data })
            .await
            .map_err(|e| unreachable_err::<RaftError<u64>>(e))?;

        let result: AppendEntriesResponse<u64> =
            bincode::deserialize(&resp.into_inner().data)
                .map_err(|e| unreachable_err::<RaftError<u64>>(e))?;
        Ok(result)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<VantaRaftConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let data = bincode::serialize(&rpc)
            .map_err(|e| unreachable_err::<RaftError<u64, InstallSnapshotError>>(e))?;
        let client = self
            .ensure_client()
            .await
            .map_err(|e| unreachable_err::<RaftError<u64, InstallSnapshotError>>(e))?;

        let resp = client
            .install_snapshot(proto::RaftMessage { data })
            .await
            .map_err(|e| unreachable_err::<RaftError<u64, InstallSnapshotError>>(e))?;

        let result: InstallSnapshotResponse<u64> =
            bincode::deserialize(&resp.into_inner().data)
                .map_err(|e| unreachable_err::<RaftError<u64, InstallSnapshotError>>(e))?;
        Ok(result)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<
        VoteResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64>>,
    > {
        let data = bincode::serialize(&rpc).map_err(|e| unreachable_err::<RaftError<u64>>(e))?;
        let client = self.ensure_client().await.map_err(|e| unreachable_err::<RaftError<u64>>(e))?;

        let resp = client
            .vote(proto::RaftMessage { data })
            .await
            .map_err(|e| unreachable_err::<RaftError<u64>>(e))?;

        let result: VoteResponse<u64> =
            bincode::deserialize(&resp.into_inner().data)
                .map_err(|e| unreachable_err::<RaftError<u64>>(e))?;
        Ok(result)
    }
}
