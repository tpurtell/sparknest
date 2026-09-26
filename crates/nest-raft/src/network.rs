//! openraft network over `nest-rpc` (service RAFT).

use crate::{Raft, TypeConfig};
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use nest_rpc::{Handler, Rpc, RpcError, service};
use nest_types::NodeId;
use openraft::BasicNode;
use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
enum RaftReq {
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<u64>),
    Snapshot(InstallSnapshotRequest<TypeConfig>),
}

#[derive(Serialize, Deserialize)]
enum RaftResp {
    Append(Result<AppendEntriesResponse<u64>, RaftError<u64>>),
    Vote(Result<VoteResponse<u64>, RaftError<u64>>),
    Snapshot(Result<InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>>),
}

/// Serves incoming Raft RPCs into the local Raft instance.
pub struct RaftService {
    pub raft: Raft,
}

impl Handler for RaftService {
    fn call(&self, _peer: NodeId, body: Bytes) -> BoxFuture<'static, Result<Bytes, String>> {
        let raft = self.raft.clone();
        async move {
            let req: RaftReq = nest_rpc::decode(&body).map_err(|e| e.to_string())?;
            let resp = match req {
                RaftReq::Append(r) => RaftResp::Append(raft.append_entries(r).await),
                RaftReq::Vote(r) => RaftResp::Vote(raft.vote(r).await),
                RaftReq::Snapshot(r) => RaftResp::Snapshot(raft.install_snapshot(r).await),
            };
            nest_rpc::encode(&resp)
                .map(Bytes::from)
                .map_err(|e| e.to_string())
        }
        .boxed()
    }
}

pub fn register(rpc: &Rpc, raft: Raft) {
    rpc.register(service::RAFT, std::sync::Arc::new(RaftService { raft }));
}

pub struct NetworkFactory {
    pub rpc: Rpc,
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Network;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Network {
        if let Ok(addr) = node.addr.parse() {
            self.rpc.set_peer(NodeId(target), addr);
        }
        Network {
            rpc: self.rpc.clone(),
            target,
        }
    }
}

pub struct Network {
    rpc: Rpc,
    target: u64,
}

impl Network {
    async fn send<E: std::error::Error>(
        &self,
        req: RaftReq,
        option: &RPCOption,
    ) -> Result<RaftResp, RPCError<u64, BasicNode, RaftError<u64, E>>> {
        let body = nest_rpc::encode(&req).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let timeout = option.hard_ttl().max(Duration::from_millis(50));
        match self
            .rpc
            .call(NodeId(self.target), service::RAFT, body.into(), timeout)
            .await
        {
            Ok(b) => nest_rpc::decode(&b).map_err(|e| RPCError::Network(NetworkError::new(&e))),
            Err(e @ RpcError::Unreachable(..)) => Err(RPCError::Unreachable(Unreachable::new(&e))),
            Err(e) => Err(RPCError::Network(NetworkError::new(&e))),
        }
    }
}

fn unexpected<E: std::error::Error>() -> RPCError<u64, BasicNode, RaftError<u64, E>> {
    RPCError::Network(NetworkError::new(&std::io::Error::other(
        "mismatched raft response",
    )))
}

impl RaftNetwork<TypeConfig> for Network {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        match self.send(RaftReq::Append(rpc), &option).await? {
            RaftResp::Append(r) => {
                r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            _ => Err(unexpected()),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        match self.send(RaftReq::Vote(rpc), &option).await? {
            RaftResp::Vote(r) => {
                r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            _ => Err(unexpected()),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        match self.send(RaftReq::Snapshot(rpc), &option).await? {
            RaftResp::Snapshot(r) => {
                r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            _ => Err(unexpected()),
        }
    }
}
