//! [`MetaNode`]: the replicated metadata service as the daemon sees it.

use crate::log_store::LogStore;
use crate::network::{self, NetworkFactory};
use crate::state_machine::{EffectHandler, StateMachine};
use crate::{Raft, Request, Response, TypeConfig};
use anyhow::Context;
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use nest_meta::{Command, Reply};
use nest_rpc::{Handler, Rpc, service};
use nest_types::{NestError, NodeId};
use openraft::error::{ClientWriteError, RaftError};
use openraft::{BasicNode, ChangeMembers, SnapshotPolicy};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct MetaNodeConfig {
    pub node: NodeId,
    /// Holds `raft.sqlite`, `meta.sqlite` and `snapshots/`.
    pub dir: PathBuf,
    pub cluster: String,
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_max_ms: u64,
    /// Build a snapshot after this many applied entries.
    pub snapshot_every: u64,
    /// How long a proposal keeps retrying through elections and partitions
    /// before reporting [`NestError::NoQuorum`].
    pub propose_deadline: Duration,
}

impl MetaNodeConfig {
    pub fn new(node: NodeId, dir: PathBuf, cluster: impl Into<String>) -> Self {
        MetaNodeConfig {
            node,
            dir,
            cluster: cluster.into(),
            heartbeat_ms: 100,
            election_min_ms: 500,
            election_max_ms: 1000,
            snapshot_every: 50_000,
            propose_deadline: Duration::from_secs(15),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum MetaReq {
    Propose(Request),
    ReadIndex,
}

#[derive(Serialize, Deserialize)]
enum MetaResp {
    Written {
        index: u64,
        resp: Response,
    },
    ReadIndex(u64),
    NotLeader(Option<u64>),
    /// Definitely not appended: the leader has lost contact with a quorum.
    NoQuorum,
    Failed(String),
}

pub struct MetaNode {
    cfg: MetaNodeConfig,
    raft: Raft,
    rpc: Rpc,
    meta_path: PathBuf,
    /// Identifies this process for request deduplication.
    client: u64,
    /// Commit index recorded by the previous run.
    startup_committed: u64,
    seq: AtomicU64,
}

struct MetaService {
    raft: Raft,
    /// A leader that has not heard from a quorum for this long refuses new
    /// writes instead of appending entries that may commit much later.
    lease_ms: u64,
}

impl Handler for MetaService {
    fn call(&self, _peer: NodeId, body: Bytes) -> BoxFuture<'static, Result<Bytes, String>> {
        let raft = self.raft.clone();
        let lease_ms = self.lease_ms;
        async move {
            let req: MetaReq = nest_rpc::decode(&body).map_err(|e| e.to_string())?;
            let stale_leader = {
                let m = raft.metrics();
                let m = m.borrow();
                m.state == openraft::ServerState::Leader
                    && m.millis_since_quorum_ack.is_none_or(|ms| ms > lease_ms)
            };
            let resp = match req {
                MetaReq::Propose(_) if stale_leader => MetaResp::NoQuorum,
                MetaReq::Propose(r) => match raft.client_write(r).await {
                    Ok(w) => MetaResp::Written {
                        index: w.log_id.index,
                        resp: w.data,
                    },
                    Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                        MetaResp::NotLeader(f.leader_id)
                    }
                    Err(e) => MetaResp::Failed(e.to_string()),
                },
                MetaReq::ReadIndex => match raft.get_read_log_id().await {
                    Ok((read, _)) => MetaResp::ReadIndex(read.map(|l| l.index).unwrap_or(0)),
                    Err(RaftError::APIError(
                        openraft::error::CheckIsLeaderError::ForwardToLeader(f),
                    )) => MetaResp::NotLeader(f.leader_id),
                    Err(e) => MetaResp::Failed(e.to_string()),
                },
            };
            nest_rpc::encode(&resp)
                .map(Bytes::from)
                .map_err(|e| e.to_string())
        }
        .boxed()
    }
}

impl MetaNode {
    /// Open local state and start Raft. Handlers for the RAFT and META
    /// services are registered on `rpc`.
    pub async fn start(
        cfg: MetaNodeConfig,
        rpc: Rpc,
        handler: Arc<dyn EffectHandler>,
    ) -> anyhow::Result<Arc<MetaNode>> {
        std::fs::create_dir_all(&cfg.dir)
            .with_context(|| format!("creating {}", cfg.dir.display()))?;
        let log = LogStore::open(&cfg.dir.join("raft.sqlite")).context("opening raft log")?;
        let startup_committed = log.persisted_committed()?.unwrap_or(0);
        let sm = StateMachine::open(&cfg.dir, handler).context("opening state machine")?;
        let meta_path = sm.meta_path().to_path_buf();
        let config = openraft::Config {
            cluster_name: cfg.cluster.clone(),
            heartbeat_interval: cfg.heartbeat_ms,
            election_timeout_min: cfg.election_min_ms,
            election_timeout_max: cfg.election_max_ms,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(cfg.snapshot_every),
            max_in_snapshot_log_to_keep: 1000,
            ..Default::default()
        }
        .validate()?;
        let raft = Raft::new(
            cfg.node.0,
            Arc::new(config),
            NetworkFactory { rpc: rpc.clone() },
            log,
            sm,
        )
        .await?;
        network::register(&rpc, raft.clone());
        rpc.register(
            service::META,
            Arc::new(MetaService {
                raft: raft.clone(),
                lease_ms: cfg.election_max_ms,
            }),
        );
        Ok(Arc::new(MetaNode {
            cfg,
            raft,
            rpc,
            meta_path,
            client: rand::random(),
            startup_committed,
            seq: AtomicU64::new(1),
        }))
    }

    pub fn id(&self) -> NodeId {
        self.cfg.node
    }

    pub fn raft(&self) -> &Raft {
        &self.raft
    }

    pub fn rpc(&self) -> &Rpc {
        &self.rpc
    }

    /// A read-only connection to the local materialized metadata.
    pub fn open_reader(&self) -> rusqlite::Result<rusqlite::Connection> {
        nest_meta::open_read(&self.meta_path)
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow().current_leader.map(NodeId)
    }

    pub fn applied_index(&self) -> u64 {
        self.raft
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0)
    }

    /// Wait until everything committed before the previous shutdown has been
    /// re-applied locally. The local database may lag the durable log after
    /// a power loss; nothing may be reconciled against it before this.
    pub async fn wait_startup_replay(&self) -> Result<(), NestError> {
        if self.startup_committed == 0 {
            return Ok(());
        }
        self.wait_applied(self.startup_committed).await
    }

    /// Initialize a brand-new cluster with `members` (voters). Does nothing
    /// if this node already has Raft state. Returns whether it initialized.
    pub async fn bootstrap(&self, members: BTreeMap<u64, BasicNode>) -> anyhow::Result<bool> {
        if self.raft.is_initialized().await? {
            return Ok(false);
        }
        for (id, n) in &members {
            if let Ok(a) = n.addr.parse() {
                self.rpc.set_peer(NodeId(*id), a);
            }
        }
        match self.raft.initialize(members).await {
            Ok(()) => Ok(true),
            Err(RaftError::APIError(openraft::error::InitializeError::NotAllowed(_))) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn learn_addr(&self, target: NodeId) {
        if self.rpc.peer_addr(target).is_some() {
            return;
        }
        let m = self.raft.metrics().borrow().membership_config.clone();
        if let Some(n) = m
            .nodes()
            .find(|(id, _)| **id == target.0)
            .map(|(_, n)| n.clone())
            && let Ok(a) = n.addr.parse()
        {
            self.rpc.set_peer(target, a);
        }
    }

    async fn call_leader(&self, req: &MetaReq) -> Result<(NodeId, MetaResp), NestError> {
        let deadline = Instant::now() + self.cfg.propose_deadline;
        let mut hint = self.leader();
        let body: Bytes = nest_rpc::encode(req)
            .map_err(|e| NestError::Io(e.to_string()))?
            .into();
        // Set when an attempt may have reached a leader without us learning
        // the outcome. Retrying is safe (requests are deduplicated), but if
        // the deadline passes the outcome is unknown, not "not applied".
        let mut uncertain = false;
        loop {
            let target = hint.unwrap_or(self.cfg.node);
            self.learn_addr(target);
            let r = self
                .rpc
                .call(target, service::META, body.clone(), Duration::from_secs(5))
                .await;
            let pause = match r {
                Ok(b) => match nest_rpc::decode::<MetaResp>(&b) {
                    Ok(MetaResp::NotLeader(Some(l))) if l != target.0 => {
                        hint = Some(NodeId(l));
                        Duration::ZERO
                    }
                    Ok(MetaResp::NotLeader(_)) | Ok(MetaResp::NoQuorum) => {
                        hint = self.leader().filter(|l| Some(*l) != hint);
                        Duration::from_millis(50)
                    }
                    Ok(MetaResp::Failed(e)) => {
                        // client_write failed inside the leader, e.g. it
                        // stepped down mid-request: may or may not commit.
                        tracing::debug!(error = %e, "proposal failed on leader; retrying");
                        uncertain = true;
                        hint = None;
                        Duration::from_millis(50)
                    }
                    Ok(resp) => return Ok((target, resp)),
                    Err(e) => return Err(NestError::Io(e.to_string())),
                },
                Err(e) => {
                    if !e.not_delivered() {
                        uncertain = true;
                    }
                    hint = self.leader().filter(|l| Some(*l) != hint);
                    Duration::from_millis(50)
                }
            };
            if Instant::now() + pause > deadline {
                return Err(if uncertain {
                    NestError::Unavailable("metadata write outcome unknown".into())
                } else {
                    NestError::NoQuorum
                });
            }
            tokio::time::sleep(pause).await;
        }
    }

    /// Wait until the local state machine has applied `index`.
    pub async fn wait_applied(&self, index: u64) -> Result<(), NestError> {
        self.raft
            .wait(Some(self.cfg.propose_deadline))
            .applied_index_at_least(Some(index), "sparknest read-your-writes")
            .await
            .map(|_| ())
            .map_err(|_| NestError::Unavailable("committed but not yet applied locally".into()))
    }

    /// Replicate `cmd` and return its outcome once it is also applied on
    /// this node (so local reads observe it). Retries through leader
    /// changes; a retried request is applied at most once.
    pub async fn propose(&self, cmd: Command) -> Result<Reply, NestError> {
        let req = Request {
            client: self.client,
            seq: self.seq.fetch_add(1, Ordering::Relaxed),
            cmd,
        };
        match self.call_leader(&MetaReq::Propose(req)).await? {
            (_, MetaResp::Written { index, resp }) => {
                self.wait_applied(index).await?;
                resp.0
            }
            _ => Err(NestError::Io("unexpected meta response".into())),
        }
    }

    /// Linearizable read barrier: after this returns, the local database
    /// reflects every write committed before the call started.
    pub async fn barrier(&self) -> Result<(), NestError> {
        match self.call_leader(&MetaReq::ReadIndex).await? {
            (_, MetaResp::ReadIndex(index)) => self.wait_applied(index).await,
            _ => Err(NestError::Io("unexpected meta response".into())),
        }
    }

    /// Add a node as learner, then optionally promote it to voter.
    pub async fn add_node(&self, id: NodeId, addr: String, voter: bool) -> anyhow::Result<()> {
        if let Ok(a) = addr.parse() {
            self.rpc.set_peer(id, a);
        }
        self.raft
            .add_learner(id.0, BasicNode { addr }, true)
            .await?;
        if voter {
            self.raft
                .change_membership(ChangeMembers::AddVoterIds(BTreeSet::from([id.0])), false)
                .await?;
        }
        Ok(())
    }

    pub async fn remove_node(&self, id: NodeId) -> anyhow::Result<()> {
        self.raft
            .change_membership(ChangeMembers::RemoveVoters(BTreeSet::from([id.0])), false)
            .await?;
        self.raft
            .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([id.0])), false)
            .await?;
        Ok(())
    }

    pub async fn shutdown(&self) {
        let _ = self.raft.shutdown().await;
    }
}

#[allow(dead_code)]
fn _assert_send(n: &MetaNode) -> impl Send + '_ {
    n.propose(Command::Batch(vec![]))
}

pub type Membership = openraft::Membership<u64, BasicNode>;
pub type Metrics = openraft::RaftMetrics<u64, BasicNode>;
pub type TC = TypeConfig;
