//! openraft network over `nest-rpc` (service RAFT), openraft 0.10 (v2).
//!
//! Snapshots are `meta.sqlite` copies on disk; they travel in chunks and
//! are reassembled into a file under the receiver's snapshot directory
//! before `install_full_snapshot`.

use crate::state_machine::SnapshotFile;
use crate::{Raft, TypeConfig, Vote};
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use nest_rpc::{Handler, Rpc, RpcError, service};
use nest_types::NodeId;
use openraft::alias::{SnapshotMetaOf, SnapshotOf};
use openraft::error::{
    NetworkError, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetworkFactory, v2::RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{BasicNode, OptionalSend};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Snapshot bytes per RPC.
const SNAPSHOT_CHUNK: usize = 4 << 20;

#[derive(Serialize, Deserialize)]
enum RaftReq {
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<TypeConfig>),
    PreVote(VoteRequest<TypeConfig>),
    SnapshotChunk(Chunk),
}

/// One piece of a snapshot file in transit.
#[derive(Serialize, Deserialize)]
struct Chunk {
    transfer: u64,
    vote: Vote,
    meta: SnapshotMetaOf<TypeConfig>,
    offset: u64,
    data: Vec<u8>,
    done: bool,
}

#[derive(Serialize, Deserialize)]
enum RaftResp {
    Append(Result<AppendEntriesResponse<TypeConfig>, RaftError<TypeConfig>>),
    Vote(Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>>),
    /// `None` until the last chunk has been installed.
    SnapshotChunk(Result<Option<SnapshotResponse<TypeConfig>>, String>),
}

/// Serves incoming Raft RPCs into the local Raft instance.
pub struct RaftService {
    raft: Raft,
    snap_dir: PathBuf,
    incoming: Mutex<HashMap<(NodeId, u64), PathBuf>>,
}

impl RaftService {
    async fn snapshot_chunk(
        self: Arc<Self>,
        peer: NodeId,
        chunk: Chunk,
    ) -> Result<Option<SnapshotResponse<TypeConfig>>, String> {
        let Chunk {
            transfer,
            vote,
            meta,
            offset,
            data,
            done,
        } = chunk;
        let path = {
            let mut m = self.incoming.lock();
            if offset == 0 {
                let p = self
                    .snap_dir
                    .join(format!("incoming-{}-{transfer:016x}.sqlite", peer.0));
                m.insert((peer, transfer), p);
            }
            m.get(&(peer, transfer))
                .cloned()
                .ok_or("snapshot chunk for an unknown transfer")?
        };
        let p = path.clone();
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(offset == 0)
                .open(&p)?;
            f.write_all_at(&data, offset)?;
            if done {
                f.sync_all()?;
            }
            Ok(())
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        if !done {
            return Ok(None);
        }
        self.incoming.lock().remove(&(peer, transfer));
        let snapshot = SnapshotOf::<TypeConfig, SnapshotFile> {
            meta,
            snapshot: SnapshotFile(path),
        };
        self.raft
            .install_full_snapshot(vote, snapshot)
            .await
            .map(Some)
            .map_err(|e| e.to_string())
    }
}

struct RaftHandler(Arc<RaftService>);

impl Handler for RaftHandler {
    fn call(&self, peer: NodeId, body: Bytes) -> BoxFuture<'static, Result<Bytes, String>> {
        let me = self.0.clone();
        async move {
            let req: RaftReq = nest_rpc::decode(&body).map_err(|e| e.to_string())?;
            let resp = match req {
                RaftReq::Append(r) => RaftResp::Append(me.raft.append_entries(r).await),
                RaftReq::Vote(r) => RaftResp::Vote(me.raft.vote(r).await),
                RaftReq::PreVote(r) => RaftResp::Vote(me.raft.pre_vote(r).await),
                RaftReq::SnapshotChunk(c) => {
                    RaftResp::SnapshotChunk(me.snapshot_chunk(peer, c).await)
                }
            };
            nest_rpc::encode(&resp)
                .map(Bytes::from)
                .map_err(|e| e.to_string())
        }
        .boxed()
    }
}

pub fn register(rpc: &Rpc, raft: Raft, snap_dir: PathBuf) {
    rpc.register(
        service::RAFT,
        Arc::new(RaftHandler(Arc::new(RaftService {
            raft,
            snap_dir,
            incoming: Mutex::new(HashMap::new()),
        }))),
    );
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

fn net_err(e: &(impl std::error::Error + 'static)) -> RPCError<TypeConfig> {
    RPCError::Network(NetworkError::new(e))
}

impl Network {
    async fn send(
        &self,
        req: &RaftReq,
        timeout: Duration,
    ) -> Result<RaftResp, RPCError<TypeConfig>> {
        let body = nest_rpc::encode(req).map_err(|e| net_err(&e))?;
        match self
            .rpc
            .call(NodeId(self.target), service::RAFT, body.into(), timeout)
            .await
        {
            Ok(b) => nest_rpc::decode(&b).map_err(|e| net_err(&e)),
            Err(e @ RpcError::Unreachable(..)) => Err(RPCError::Unreachable(Unreachable::new(&e))),
            Err(e) => Err(net_err(&e)),
        }
    }

    async fn vote_like(
        &self,
        req: RaftReq,
        option: &RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        match self.send(&req, ttl(option)).await? {
            RaftResp::Vote(r) => r.map_err(|e| net_err(&e)),
            _ => Err(unexpected()),
        }
    }
}

fn ttl(option: &RPCOption) -> Duration {
    option.hard_ttl().max(Duration::from_millis(50))
}

fn unexpected() -> RPCError<TypeConfig> {
    net_err(&std::io::Error::other("mismatched raft response"))
}

impl RaftNetworkV2<TypeConfig> for Network {
    type SnapshotData = SnapshotFile;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        match self.send(&RaftReq::Append(rpc), ttl(&option)).await? {
            RaftResp::Append(r) => r.map_err(|e| net_err(&e)),
            _ => Err(unexpected()),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.vote_like(RaftReq::Vote(rpc), &option).await
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.vote_like(RaftReq::PreVote(rpc), &option).await
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: SnapshotOf<TypeConfig, SnapshotFile>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let transfer: u64 = rand::random();
        let path = snapshot.snapshot.0.clone();
        let meta = snapshot.meta.clone();
        let send_all = async {
            let len = tokio::fs::metadata(&path)
                .await
                .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?
                .len();
            let mut offset = 0u64;
            loop {
                let p = path.clone();
                let data = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<u8>> {
                    use std::os::unix::fs::FileExt;
                    let f = std::fs::File::open(&p)?;
                    let n = ((len - offset) as usize).min(SNAPSHOT_CHUNK);
                    let mut buf = vec![0u8; n];
                    f.read_exact_at(&mut buf, offset)?;
                    Ok(buf)
                })
                .await
                .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?
                .map_err(|e| StreamingError::Network(NetworkError::new(&e)))?;
                let n = data.len() as u64;
                let done = offset + n >= len;
                let req = RaftReq::SnapshotChunk(Chunk {
                    transfer,
                    vote,
                    meta: meta.clone(),
                    offset,
                    data,
                    done,
                });
                let resp = self
                    .send(&req, ttl(&option).max(Duration::from_secs(30)))
                    .await
                    .map_err(|e| match e {
                        RPCError::Unreachable(u) => StreamingError::Unreachable(u),
                        RPCError::Network(n) => StreamingError::Network(n),
                        RPCError::Timeout(t) => StreamingError::Timeout(t),
                        other => StreamingError::Network(NetworkError::new(&other)),
                    })?;
                match resp {
                    RaftResp::SnapshotChunk(Ok(Some(r))) => return Ok(r),
                    RaftResp::SnapshotChunk(Ok(None)) if !done => offset += n,
                    RaftResp::SnapshotChunk(Err(e)) => {
                        return Err(StreamingError::Network(NetworkError::new(
                            &std::io::Error::other(e),
                        )));
                    }
                    _ => {
                        return Err(StreamingError::Network(NetworkError::new(
                            &std::io::Error::other("mismatched snapshot response"),
                        )));
                    }
                }
            }
        };
        tokio::select! {
            r = send_all => r,
            closed = cancel => Err(StreamingError::Closed(closed)),
        }
    }
}
