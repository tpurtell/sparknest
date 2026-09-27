//! Before Raft starts: versions, crashes and re-founding (ADR-026).
//!
//! Every daemon serves HELLO from the moment it binds, saying which build
//! and Raft format it runs, whether it is still recovering or running, and
//! which *incarnation* of the cluster its metadata belongs to (the re-found
//! it descends from). A host decides how to start:
//!
//! - **Normal**: its Raft state is in this build's format and it did not
//!   crash (or it crashed but a healthy cluster is running: it rejoins and
//!   fscks after catching up);
//! - **Join**: a live cluster runs a different incarnation or this host's
//!   Raft state is in an older format: it sets its Raft state aside and
//!   joins empty (the leader sends a snapshot);
//! - **Refound**: no live cluster, and a majority of members are here
//!   recovering (a full outage, or everyone upgraded): the most advanced
//!   metadata among them seeds a new Raft group.
//!
//! Plans are agreed in two steps so differing views cannot produce two
//! groups: the coordinator (lowest id it sees recovering) proposes, every
//! host accepts at most one plan, and the coordinator commits only once a
//! majority of members accepted. Hosts that do not answer HELLO (an older
//! build still running Raft, or down) never count, so an accidental upgrade
//! of a minority waits instead of taking the cluster.

use crate::config::Config;
use anyhow::Context;
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use nest_raft::MetaNode;
use nest_raft::log_store::{Found, LogStore};
use nest_rpc::{Handler, Rpc, service};
use nest_types::NodeId;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SEED_CHUNK: u64 = 4 << 20;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    Recovering {
        dirty: bool,
        /// Raft format of the state on disk (`None`: no Raft state).
        disk_format: Option<u32>,
        /// Applied (term, index) of the metadata on disk.
        applied: Option<(u64, u64)>,
    },
    Running {
        leader: Option<u64>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plan {
    pub id: String,
    pub coordinator: u64,
    /// Whose metadata seeds the new group (it also founds the group).
    pub seed: u64,
    pub participants: Vec<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeedInfo {
    pub plan: String,
    pub sha256: String,
    pub len: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Info {
    pub version: String,
    pub raft_format: u32,
    pub phase: Phase,
    pub incarnation: Option<String>,
    pub accepted: Option<Plan>,
    pub committed: bool,
    pub seed: Option<SeedInfo>,
}

#[derive(Serialize, Deserialize)]
enum HelloReq {
    Info,
    Propose(Plan),
    Commit(String),
    SeedChunk { plan: String, offset: u64 },
}

#[derive(Serialize, Deserialize)]
enum HelloResp {
    Info(Box<Info>),
    Ok,
    Refused(String),
    Chunk(Vec<u8>),
}

struct State {
    phase: Phase,
    incarnation: Option<String>,
    accepted: Option<(Plan, Instant)>,
    committed: bool,
    seed: Option<(SeedInfo, PathBuf)>,
    meta: Option<Arc<MetaNode>>,
}

/// The HELLO service and this host's recovery state.
#[derive(Clone)]
pub struct Hello {
    st: Arc<Mutex<State>>,
}

impl Hello {
    pub fn register(rpc: &Rpc, phase: Phase, incarnation: Option<String>) -> Hello {
        let h = Hello {
            st: Arc::new(Mutex::new(State {
                phase,
                incarnation,
                accepted: None,
                committed: false,
                seed: None,
                meta: None,
            })),
        };
        rpc.register(service::HELLO, Arc::new(h.clone()));
        h
    }

    /// Raft is up: report the leader from now on.
    pub fn running(&self, meta: Arc<MetaNode>, incarnation: Option<String>) {
        let mut st = self.st.lock();
        st.meta = Some(meta);
        st.incarnation = incarnation;
        st.phase = Phase::Running { leader: None };
    }

    fn info(&self) -> Info {
        let st = self.st.lock();
        let phase = match (&st.phase, &st.meta) {
            (Phase::Running { .. }, Some(m)) => Phase::Running {
                leader: m.leader().map(|l| l.0),
            },
            (p, _) => p.clone(),
        };
        Info {
            version: env!("CARGO_PKG_VERSION").to_string(),
            raft_format: nest_raft::RAFT_FORMAT,
            phase,
            incarnation: st.incarnation.clone(),
            accepted: st.accepted.as_ref().map(|(p, _)| p.clone()),
            committed: st.committed,
            seed: st.seed.as_ref().map(|(s, _)| s.clone()),
        }
    }
}

impl Handler for Hello {
    fn call(&self, _peer: NodeId, body: Bytes) -> BoxFuture<'static, Result<Bytes, String>> {
        let me = self.clone();
        async move {
            let req: HelloReq = nest_rpc::decode(&body).map_err(|e| e.to_string())?;
            let resp = match req {
                HelloReq::Info => HelloResp::Info(Box::new(me.info())),
                HelloReq::Propose(plan) => {
                    let mut st = me.st.lock();
                    let stale = st.accepted.as_ref().is_some_and(|(_, at)| {
                        !st.committed && at.elapsed() > Duration::from_secs(60)
                    });
                    if stale {
                        st.accepted = None;
                    }
                    match &st.accepted {
                        _ if !matches!(st.phase, Phase::Recovering { .. }) => {
                            HelloResp::Refused("already running".into())
                        }
                        Some((p, _)) if p.id != plan.id => {
                            HelloResp::Refused(format!("accepted plan {}", p.id))
                        }
                        _ => {
                            st.accepted = Some((plan, Instant::now()));
                            HelloResp::Ok
                        }
                    }
                }
                HelloReq::Commit(id) => {
                    let mut st = me.st.lock();
                    if st.accepted.as_ref().is_some_and(|(p, _)| p.id == id) {
                        st.committed = true;
                        HelloResp::Ok
                    } else {
                        HelloResp::Refused("not accepted".into())
                    }
                }
                HelloReq::SeedChunk { plan, offset } => {
                    let path = me
                        .st
                        .lock()
                        .seed
                        .as_ref()
                        .filter(|(s, _)| s.plan == plan)
                        .map(|(_, p)| p.clone());
                    match path {
                        None => HelloResp::Refused("no seed for that plan".into()),
                        Some(p) => match read_chunk(&p, offset).await {
                            Ok(b) => HelloResp::Chunk(b),
                            Err(e) => HelloResp::Refused(e.to_string()),
                        },
                    }
                }
            };
            nest_rpc::encode(&resp)
                .map(Bytes::from)
                .map_err(|e| e.to_string())
        }
        .boxed()
    }
}

async fn read_chunk(p: &Path, offset: u64) -> std::io::Result<Vec<u8>> {
    let p = p.to_path_buf();
    tokio::task::spawn_blocking(move || {
        use std::os::unix::fs::FileExt;
        let f = std::fs::File::open(&p)?;
        let len = f.metadata()?.len();
        let n = len.saturating_sub(offset).min(SEED_CHUNK) as usize;
        let mut buf = vec![0u8; n];
        f.read_exact_at(&mut buf, offset)?;
        Ok(buf)
    })
    .await
    .map_err(std::io::Error::other)?
}

async fn ask(rpc: &Rpc, peer: u64, req: &HelloReq, timeout: Duration) -> Option<HelloResp> {
    let body = nest_rpc::encode(req).ok()?;
    let b = rpc
        .call(NodeId(peer), service::HELLO, body.into(), timeout)
        .await
        .ok()?;
    nest_rpc::decode(&b).ok()
}

async fn infos(rpc: &Rpc, peers: &[u64]) -> BTreeMap<u64, Info> {
    let calls = peers.iter().map(|&p| async move {
        match ask(rpc, p, &HelloReq::Info, Duration::from_secs(1)).await {
            Some(HelloResp::Info(i)) => Some((p, *i)),
            _ => None,
        }
    });
    futures::future::join_all(calls)
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// What this host holds, read before Raft starts.
#[derive(Clone, Debug)]
pub struct Local {
    pub dirty: bool,
    pub disk: Found,
    pub applied: Option<(u64, u64)>,
    pub incarnation: Option<String>,
}

impl Local {
    pub fn read(dir: &Path, dirty: bool) -> anyhow::Result<Local> {
        let disk = LogStore::probe(&dir.join("raft.sqlite")).context("inspecting raft.sqlite")?;
        let meta = dir.join("meta.sqlite");
        let applied = nest_raft::seed::applied_of(&meta);
        let incarnation = rusqlite::Connection::open_with_flags(
            &meta,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .ok()
        .and_then(|c| nest_raft::seed::refound_of(&c));
        Ok(Local {
            dirty,
            disk,
            applied,
            incarnation,
        })
    }

    pub fn phase(&self) -> Phase {
        Phase::Recovering {
            dirty: self.dirty,
            disk_format: match self.disk {
                Found::Empty => None,
                Found::Current => Some(nest_raft::RAFT_FORMAT),
                Found::Other(v) => Some(v),
            },
            applied: self.applied,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Normal,
    /// Join a running cluster afresh, adopting its incarnation.
    Join(Option<String>),
    Refound(Plan),
}

/// A live cluster: some peer runs Raft with a leader that confirms itself.
fn live(infos: &BTreeMap<u64, Info>) -> Option<&Info> {
    infos.values().find(|i| match i.phase {
        Phase::Running { leader: Some(l) } => infos
            .get(&l)
            .is_some_and(|li| li.phase == Phase::Running { leader: Some(l) }),
        _ => false,
    })
}

/// Decide how to start (see the module docs). Waits as long as it takes
/// for a majority when a re-found is needed.
pub async fn decide(
    cfg: &Config,
    rpc: &Rpc,
    hello: &Hello,
    local: &Local,
    grace: Duration,
) -> anyhow::Result<Outcome> {
    let me = cfg.node.id.0;
    let voters: Vec<u64> = cfg
        .cluster
        .members
        .iter()
        .filter(|m| m.voter)
        .map(|m| m.id.0)
        .collect();
    let peers: Vec<u64> = cfg
        .cluster
        .members
        .iter()
        .map(|m| m.id.0)
        .filter(|id| *id != me)
        .collect();
    let fresh = local.disk == Found::Empty && local.applied.is_none();
    let settled = local.disk == Found::Current && !local.dirty;
    if fresh || peers.is_empty() {
        return Ok(Outcome::Normal);
    }
    if settled {
        // One quick look: are the others running a later incarnation?
        let infos = infos(rpc, &peers).await;
        return Ok(match live(&infos) {
            Some(i) if i.incarnation != local.incarnation => {
                tracing::warn!(
                    ours = ?local.incarnation,
                    theirs = ?i.incarnation,
                    "the cluster was re-founded while this host was away: joining it afresh"
                );
                Outcome::Join(i.incarnation.clone())
            }
            _ => Outcome::Normal,
        });
    }
    let need = voters.len() / 2 + 1;
    let mut quorum_since: Option<Instant> = None;
    let mut last_log = Instant::now() - Duration::from_secs(60);
    loop {
        let infos = infos(rpc, &peers).await;
        if let Some(i) = live(&infos) {
            let incarnation_ok = i.incarnation == local.incarnation;
            return Ok(match local.disk {
                Found::Current if incarnation_ok => Outcome::Normal,
                _ => {
                    tracing::warn!(
                        disk = ?local.disk,
                        "a cluster is running: setting this host's Raft state aside and joining it"
                    );
                    Outcome::Join(i.incarnation.clone())
                }
            });
        }
        // A plan committed by a coordinator, including us.
        {
            let st = hello.st.lock();
            if st.committed
                && let Some((p, _)) = &st.accepted
            {
                return Ok(Outcome::Refound(p.clone()));
            }
        }
        let mut here: Vec<(u64, Option<(u64, u64)>)> = infos
            .iter()
            .filter_map(|(id, i)| match &i.phase {
                Phase::Recovering { applied, .. } if i.raft_format == nest_raft::RAFT_FORMAT => {
                    Some((*id, *applied))
                }
                _ => None,
            })
            .collect();
        here.push((me, local.applied));
        here.sort();
        if here.len() >= need {
            let since = *quorum_since.get_or_insert_with(Instant::now);
            let coordinator = here[0].0;
            let everyone = here.len() >= voters.len();
            if coordinator == me && (everyone || since.elapsed() >= grace) {
                // Most advanced metadata; lowest id on ties.
                let seed = here
                    .iter()
                    .max_by(|a, b| a.1.cmp(&b.1).then(b.0.cmp(&a.0)))
                    .map(|x| x.0)
                    .unwrap_or(me);
                let plan = Plan {
                    id: format!("{:016x}", rand::random::<u64>()),
                    coordinator: me,
                    seed,
                    participants: here.iter().map(|x| x.0).collect(),
                };
                if let Some(p) = propose(rpc, hello, &plan, need).await {
                    return Ok(Outcome::Refound(p));
                }
            }
        } else {
            quorum_since = None;
        }
        if last_log.elapsed() > Duration::from_secs(10) {
            tracing::warn!(
                ready = here.len(),
                need,
                "recovery: waiting for a majority of members before re-founding the cluster"
            );
            crate::sdnotify::status(&format!(
                "recovering: {} of {need} members needed are here; waiting",
                here.len()
            ));
            last_log = Instant::now();
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Two-step agreement: accepted by a majority, then committed.
async fn propose(rpc: &Rpc, hello: &Hello, plan: &Plan, need: usize) -> Option<Plan> {
    {
        let mut st = hello.st.lock();
        match &st.accepted {
            Some((p, _)) if p.id != plan.id => return None,
            _ => st.accepted = Some((plan.clone(), Instant::now())),
        }
    }
    let others: Vec<u64> = plan
        .participants
        .iter()
        .copied()
        .filter(|p| *p != plan.coordinator)
        .collect();
    let acks = futures::future::join_all(others.iter().map(|&p| async move {
        matches!(
            ask(
                rpc,
                p,
                &HelloReq::Propose(plan.clone()),
                Duration::from_secs(2)
            )
            .await,
            Some(HelloResp::Ok)
        )
    }))
    .await;
    let accepted = 1 + acks.iter().filter(|a| **a).count();
    if accepted < need {
        hello.st.lock().accepted = None;
        return None;
    }
    hello.st.lock().committed = true;
    for &p in &others {
        let _ = ask(
            rpc,
            p,
            &HelloReq::Commit(plan.id.clone()),
            Duration::from_secs(2),
        )
        .await;
    }
    tracing::warn!(plan = %plan.id, seed = plan.seed, participants = ?plan.participants, "re-founding the cluster");
    Some(plan.clone())
}

/// Carry out a committed plan up to installing the seed; the caller then
/// starts Raft (the seed host founds the group).
pub async fn execute(cfg: &Config, rpc: &Rpc, hello: &Hello, plan: &Plan) -> anyhow::Result<()> {
    let dir = &cfg.node.state_dir;
    let me = cfg.node.id.0;
    let work = dir.join("refound");
    std::fs::create_dir_all(&work)?;
    let seed_path = work.join(format!("seed-{}.sqlite", plan.id));
    if plan.seed == me {
        let meta = dir.join("meta.sqlite");
        let (m, s, id) = (meta.clone(), seed_path.clone(), plan.id.clone());
        let sha =
            tokio::task::spawn_blocking(move || nest_raft::seed::build(&m, &s, &id)).await??;
        let len = std::fs::metadata(&seed_path)?.len();
        hello.st.lock().seed = Some((
            SeedInfo {
                plan: plan.id.clone(),
                sha256: sha,
                len,
            },
            seed_path.clone(),
        ));
    } else {
        fetch_seed(rpc, plan, &seed_path).await?;
    }
    let (d, s, id) = (dir.clone(), seed_path.clone(), plan.id.clone());
    let aside =
        tokio::task::spawn_blocking(move || nest_raft::seed::install(&d, &s, &id)).await??;
    tracing::warn!(aside = %aside.display(), plan = %plan.id, "installed the re-found seed; previous Raft state kept aside");
    Ok(())
}

async fn fetch_seed(rpc: &Rpc, plan: &Plan, to: &Path) -> anyhow::Result<()> {
    // Wait for the seed host to build it.
    let info = loop {
        if let Some(HelloResp::Info(i)) =
            ask(rpc, plan.seed, &HelloReq::Info, Duration::from_secs(2)).await
            && let Some(s) = i.seed
            && s.plan == plan.id
        {
            break s;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let mut buf = Vec::with_capacity(info.len as usize);
    while (buf.len() as u64) < info.len {
        match ask(
            rpc,
            plan.seed,
            &HelloReq::SeedChunk {
                plan: plan.id.clone(),
                offset: buf.len() as u64,
            },
            Duration::from_secs(30),
        )
        .await
        {
            Some(HelloResp::Chunk(b)) if !b.is_empty() => buf.extend_from_slice(&b),
            _ => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
    let to2 = to.to_path_buf();
    tokio::task::spawn_blocking(move || std::fs::write(&to2, &buf)).await??;
    let got = nest_raft::seed::sha256_file(to)?;
    anyhow::ensure!(
        got == info.sha256,
        "seed checksum mismatch ({got} != {}): not installing",
        info.sha256
    );
    Ok(())
}
