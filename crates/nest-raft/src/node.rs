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
use openraft::Instant as _;
use openraft::async_runtime::WatchReceiver;
use openraft::error::{ClientWriteError, RaftError};
use openraft::{BasicNode, ChangeMembers, ReadPolicy, SnapshotPolicy};
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
    /// Joining a running cluster with empty state: its incarnation, until
    /// the snapshot it sends brings it along (ADR-026).
    pub join_incarnation: Option<String>,
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
            join_incarnation: None,
        }
    }
}

#[derive(Serialize, Deserialize)]
enum MetaReq {
    Propose(Request),
    ReadIndex,
    /// Make the log durable up to this entry (once it is here).
    Sync(crate::LogId),
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
    Synced,
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
    /// The re-found this node's metadata descends from (ADR-026).
    incarnation: Option<String>,
    seq: AtomicU64,
}

struct MetaService {
    raft: Raft,
    log: LogStore,
    /// How long a Sync waits for the entry to arrive.
    sync_wait: Duration,
    /// A leader that has not heard from a quorum for this long refuses new
    /// writes instead of appending entries that may commit much later.
    lease_ms: u64,
}

impl Handler for MetaService {
    fn call(&self, _peer: NodeId, body: Bytes) -> BoxFuture<'static, Result<Bytes, String>> {
        let raft = self.raft.clone();
        let lease_ms = self.lease_ms;
        let log = self.log.clone();
        let sync_wait = self.sync_wait;
        async move {
            let req: MetaReq = nest_rpc::decode(&body).map_err(|e| e.to_string())?;
            let stale_leader = {
                let m = raft.metrics();
                let m = m.borrow_watched();
                // A sole voter is its own quorum (and its ack time is not
                // refreshed while idle).
                let others = m
                    .membership_config
                    .membership()
                    .voter_ids()
                    .any(|v| v != m.id);
                m.state == openraft::ServerState::Leader
                    && others
                    && m.last_quorum_acked
                        .is_none_or(|t| t.into_inner().elapsed() > Duration::from_millis(lease_ms))
            };
            let resp = match req {
                MetaReq::Propose(_) if stale_leader => MetaResp::NoQuorum,
                MetaReq::Propose(r) => match raft.client_write(r).await {
                    Ok(w) => MetaResp::Written {
                        index: w.log_id.index(),
                        resp: w.data,
                    },
                    Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                        MetaResp::NotLeader(f.leader_id)
                    }
                    Err(e) => MetaResp::Failed(e.to_string()),
                },
                MetaReq::ReadIndex => match raft.get_read_log_id(ReadPolicy::ReadIndex).await {
                    Ok((read, _)) => MetaResp::ReadIndex(read.index()),
                    Err(RaftError::APIError(
                        openraft::error::LinearizableReadError::ForwardToLeader(f),
                    )) => MetaResp::NotLeader(f.leader_id),
                    Err(e) => MetaResp::Failed(e.to_string()),
                },
                MetaReq::Sync(want) => match sync_to(&raft, &log, want, sync_wait).await {
                    Ok(()) => MetaResp::Synced,
                    Err(e) => MetaResp::Failed(e),
                },
            };
            nest_rpc::encode(&resp)
                .map(Bytes::from)
                .map_err(|e| e.to_string())
        }
        .boxed()
    }
}

/// Wait until the local log holds exactly `want`, then fsync the log.
async fn sync_to(
    raft: &Raft,
    log: &LogStore,
    want: crate::LogId,
    wait: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + wait;
    loop {
        let log2 = log.clone();
        let here = tokio::task::spawn_blocking(move || log2.log_id_at(want.index()))
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        if here == Some(want) {
            return log.sync().await.map_err(|e| e.to_string());
        }
        // Not here yet (lagging) or superseded locally by the leader soon:
        // wait for more log, then look again.
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err("entry did not arrive in time".into());
        }
        let _ = raft
            .wait(Some(left.min(Duration::from_millis(100))))
            .log_index_at_least(Some(want.index()), "sync barrier")
            .await;
        if here.is_some() {
            // Present but different: give the leader a moment to repair it.
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
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
        let log_handle = log.clone();
        let startup_committed = log.persisted_committed()?.unwrap_or(0);
        let sm = StateMachine::open(&cfg.dir, handler).context("opening state machine")?;
        let meta_path = sm.meta_path().to_path_buf();
        let incarnation = nest_meta::open_read(&meta_path)
            .ok()
            .and_then(|c| crate::seed::refound_of(&c))
            .or_else(|| cfg.join_incarnation.clone());
        let config = openraft::Config {
            cluster_name: cfg.cluster.clone(),
            heartbeat_interval: cfg.heartbeat_ms,
            election_timeout_min: cfg.election_min_ms,
            election_timeout_max: cfg.election_max_ms,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(cfg.snapshot_every),
            max_in_snapshot_log_to_keep: 1000,
            // A host that lost its log tail (wiped, or rolled back by a
            // power loss under relaxed durability, ADR-026) rejoins and is
            // repaired instead of stopping the leader.
            allow_log_reversion: Some(true),
            ..Default::default()
        }
        .validate()?;
        let snap_dir = sm.snap_dir().to_path_buf();
        let raft = Raft::new(
            cfg.node.0,
            Arc::new(config),
            NetworkFactory {
                rpc: rpc.clone(),
                incarnation: incarnation.clone(),
            },
            log,
            sm,
        )
        .await?;
        network::register(&rpc, raft.clone(), snap_dir, incarnation.clone());
        rpc.register(
            service::META,
            Arc::new(MetaService {
                raft: raft.clone(),
                log: log_handle,
                sync_wait: cfg.propose_deadline,
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
            incarnation,
            seq: AtomicU64::new(1),
        }))
    }

    pub fn id(&self) -> NodeId {
        self.cfg.node
    }

    /// The re-found this node's metadata descends from, if any.
    pub fn incarnation(&self) -> Option<String> {
        self.incarnation.clone()
    }

    /// Right after founding a group from a seed: snapshot and drop the log,
    /// so hosts that join later receive the seeded state as a snapshot
    /// rather than replaying a log that does not contain it.
    pub async fn compact_now(&self) -> anyhow::Result<()> {
        let last = self
            .metrics()
            .last_applied
            .ok_or_else(|| anyhow::anyhow!("nothing applied yet"))?;
        self.raft.trigger().snapshot().await?;
        // Entries may apply meanwhile: any snapshot at or past `last` will do.
        let m = self
            .raft
            .wait(Some(Duration::from_secs(30)))
            .metrics(
                |m| m.snapshot.is_some_and(|s| s.index() >= last.index()),
                "re-found snapshot",
            )
            .await?;
        let upto = m.snapshot.map(|s| s.index()).unwrap_or(last.index());
        self.raft.trigger().purge_log(upto).await?;
        self.raft
            .wait(Some(Duration::from_secs(30)))
            .metrics(
                |m| m.purged.is_some_and(|p| p.index() >= upto),
                "re-found purge",
            )
            .await?;
        Ok(())
    }

    /// Make every host in `voters` a voter (adding learners first). Returns
    /// once the change commits; absent hosts catch up when they return.
    pub async fn expand_to(&self, voters: BTreeMap<u64, BasicNode>) -> anyhow::Result<()> {
        let have: BTreeSet<u64> = self
            .metrics()
            .membership_config
            .membership()
            .nodes()
            .map(|(id, _)| *id)
            .collect();
        for (id, n) in &voters {
            if let Ok(a) = n.addr.parse() {
                self.rpc.set_peer(NodeId(*id), a);
            }
            if !have.contains(id) {
                self.raft.add_learner(*id, n.clone(), false).await?;
            }
        }
        self.raft
            .change_membership(
                ChangeMembers::AddVoterIds(voters.keys().copied().collect()),
                false,
            )
            .await?;
        Ok(())
    }

    pub fn cluster(&self) -> &str {
        &self.cfg.cluster
    }

    pub fn raft(&self) -> &Raft {
        &self.raft
    }

    pub fn rpc(&self) -> &Rpc {
        &self.rpc
    }

    /// Write a consistent copy of the local metadata to `dest` (VACUUM INTO)
    /// and return the log index it reflects. The copy is a complete
    /// namespace at that point, usable offline (`sparknestd export`).
    pub fn snapshot_to(&self, dest: &std::path::Path) -> rusqlite::Result<u64> {
        let c = self.open_reader()?;
        c.execute("VACUUM INTO ?1", rusqlite::params![dest.to_string_lossy()])?;
        let copy = rusqlite::Connection::open_with_flags(
            dest,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let last: Option<Vec<u8>> = copy
            .query_row("SELECT v FROM sm_state WHERE k = 'last_applied'", [], |r| {
                r.get(0)
            })
            .ok();
        let idx = last
            .and_then(|b| postcard::from_bytes::<Option<crate::LogId>>(&b).ok())
            .flatten()
            .map(|l| l.index())
            .unwrap_or(0);
        Ok(idx)
    }

    /// A read-only connection to the local materialized metadata.
    pub fn open_reader(&self) -> rusqlite::Result<rusqlite::Connection> {
        nest_meta::open_read(&self.meta_path)
    }

    /// A snapshot of this node's Raft metrics.
    pub fn metrics(&self) -> Metrics {
        self.raft.metrics().borrow_watched().clone()
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.raft
            .metrics()
            .borrow_watched()
            .current_leader
            .map(NodeId)
    }

    pub fn applied_index(&self) -> u64 {
        self.raft
            .metrics()
            .borrow_watched()
            .last_applied
            .map(|l| l.index())
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
        let m = self
            .raft
            .metrics()
            .borrow_watched()
            .membership_config
            .clone();
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

    /// Durability barrier (ADR-026): return once a majority of voters have
    /// fsynced their Raft log up to everything this node has applied. Behind
    /// an application's fsync, so metadata it depends on survives a full
    /// power outage even though commits are not fsynced by default.
    pub async fn sync_barrier(&self) -> Result<(), NestError> {
        let m = self.metrics();
        let Some(want) = m.last_applied else {
            return Ok(());
        };
        let voters: Vec<u64> = m.membership_config.membership().voter_ids().collect();
        let need = voters.len() / 2 + 1;
        let body: Bytes = nest_rpc::encode(&MetaReq::Sync(want))
            .map_err(|e| NestError::Io(e.to_string()))?
            .into();
        let mut calls = futures::stream::FuturesUnordered::new();
        for v in voters {
            let (rpc, body) = (self.rpc.clone(), body.clone());
            self.learn_addr(NodeId(v));
            let timeout = self.cfg.propose_deadline;
            calls.push(async move {
                rpc.call(NodeId(v), service::META, body, timeout)
                    .await
                    .ok()
                    .and_then(|b| nest_rpc::decode::<MetaResp>(&b).ok())
            });
        }
        use futures::StreamExt;
        let mut acks = 0;
        while let Some(r) = calls.next().await {
            if matches!(r, Some(MetaResp::Synced)) {
                acks += 1;
                if acks >= need {
                    return Ok(());
                }
            }
        }
        Err(NestError::NoQuorum)
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
pub type Metrics = openraft::RaftMetrics<TypeConfig>;
pub type TC = TypeConfig;
