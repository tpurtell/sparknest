//! Push loads (ADR-063), the sparknestd side.
//!
//! Three roles, any of which a host may play in one group:
//! - **hub**: the coordinator's host. Takes the plan on the engine's
//!   socket, resolves and checks it, gives each file to one copy, tells
//!   every rank host which sources to expect and every source its files,
//!   then reports progress and the end.
//! - **source**: a host whose store holds assigned files. Reads each file
//!   once, in offset order, packs every run into the destination rank's
//!   open slot, and sends a slot whenever it fills and a credit allows.
//! - **rank host**: where a rank's engine joined with its ring. Hands out a
//!   credit per free ring slot, round robin over the sources owing it;
//!   writes each arriving slot into the ring slot its credit named, checks
//!   it, and tells the engine.
//!
//! Slots travel between hosts as `PushMsg::Slot` over the control RPC (a
//! direct call on the same host). RDMA writes straight into the ring
//! replace that where both ends have a fabric link.

use crate::vfs::{PushRead, Vfs};
use futures::FutureExt;
use futures::future::BoxFuture;
use nest_push::client::Conn;
use nest_push::plan::{Entry, FileEntries, Pattern, Plan, RunWalk};
use nest_push::proto::{Event, Forecast, Missing, RankSpec, Request, StreamForecast};
use nest_push::slot;
use nest_types::{FileId, Generation, NestError, NestResult, NodeId, StoreId};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Bytes read from a file at once.
const READ_CHUNK: u64 = 16 << 20;
/// Reads a source keeps in flight ahead of packing (`READ_CHUNK` each).
const READ_AHEAD: usize = 4;
/// Gaps between a file's runs shorter than this are read through.
const GAP: u64 = 1 << 20;
/// A group, or a source waiting for a credit, with no progress this long
/// fails.
const STALL: Duration = Duration::from_secs(30);
/// A group quiet this long when its coordinator leaves gets the stall's
/// cause in its reason.
const STALL_NOTE: Duration = Duration::from_secs(5);
/// How long `Start` waits for every rank to join.
const JOIN_WAIT: Duration = Duration::from_secs(60);
/// RPC deadline between hosts.
const RPC: Duration = Duration::from_secs(30);
/// Rank hosts report delivered bytes to the hub this often (bytes).
const REPORT_EVERY: u64 = 256 << 20;

/// Between sparknestds, on `service::PUSH`. Append-only.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum PushMsg {
    /// Hub → rank host: rank `rank` of `group` will join here.
    Expect {
        group: u64,
        token: u64,
        hub: NodeId,
        rank: u32,
        arena: u64,
    },
    /// Rank host → hub: the rank joined; its ring's slots are this big.
    Joined {
        group: u64,
        rank: u32,
        slot_bytes: u32,
    },
    /// Hub → rank host: the sources that will send this rank slots.
    Sources {
        group: u64,
        rank: u32,
        sources: Vec<Src>,
    },
    /// Hub → source host: stream these files from store `source.store`.
    Stream {
        group: u64,
        hub: NodeId,
        source: Src,
        patterns: Vec<Pattern>,
        files: Vec<SourceFile>,
        /// Per rank: its host and slot size.
        ranks: Vec<(NodeId, u32)>,
    },
    /// Rank host → source host: these ring slots of `rank` may be filled
    /// by the stream reading `store`, each by RDMA into its target when it
    /// has one, else by `Slot`.
    Credit {
        group: u64,
        store: StoreId,
        rank: u32,
        grants: Vec<Grant>,
    },
    /// Source → rank host: a packed slot (`slot_bytes` long) for ring slot
    /// `slot` of `rank`.
    Slot {
        group: u64,
        rank: u32,
        source: Src,
        slot: u32,
        /// The grant this fills (`Grant::seq`).
        seq: u64,
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
    },
    /// Source → hub: finished. `slots` sent per rank; files it could not
    /// read, by plan index.
    SourceDone {
        group: u64,
        source: Src,
        slots: Vec<u64>,
        failed: Vec<(u32, String)>,
    },
    /// Rank host → hub: payload bytes delivered so far (progress), and
    /// whether that rank has everything.
    Delivered {
        group: u64,
        rank: u32,
        bytes: u64,
        complete: bool,
    },
    /// Source → hub, every `push_record::TICK` and once at the end: payload
    /// bytes sent to each rank so far, and slots by RDMA and by RPC (for the
    /// load's record).
    Sent {
        group: u64,
        source: Src,
        bytes: Vec<u64>,
        rdma: u64,
        /// By a fallback: a TCP lane or the control connection.
        rpc: u64,
        /// To a rank on this host: no network.
        local: u64,
    },
    /// Hub → rank host: everything for `rank` is `slots` slots in all.
    RankDone { group: u64, rank: u32, slots: u64 },
    /// Any → any: the group ended (cancelled or failed).
    End { group: u64, reason: String },
    /// Rank host → hub, every few seconds while a rank is live: its ring
    /// (free slots, granted ones, slots filled and released) and when the
    /// engine last released a slot, so a stall names its cause.
    RankState {
        group: u64,
        rank: u32,
        ring: RingState,
    },
}

/// A rank's ring as its host sees it.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RingState {
    pub slots: u32,
    pub free: u32,
    pub granted: u32,
    pub filled: u64,
    pub released: u64,
    /// Milliseconds after the rank joined that the engine last released a
    /// slot; `None` before its first.
    pub last_release_ms: Option<u64>,
    /// Milliseconds after the rank joined, as of this report.
    pub at_ms: u64,
}

/// A source stream (ADR-063: one per store): the store it reads, and the
/// host that reads it: the store's own host, or for an archive one of its
/// gateways, where it runs beside that host's own stream as another disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct Src {
    pub store: StoreId,
    pub host: NodeId,
}

impl Src {
    /// A host's own store.
    fn host(n: NodeId) -> Src {
        Src {
            store: n.live_store(),
            host: n,
        }
    }
}

/// One ring slot a source may fill.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Grant {
    slot: u32,
    /// Which grant of the rank this is: a ring slot is granted again once
    /// filled, so a late duplicate of an earlier fill is told apart.
    seq: u64,
    /// Where to RDMA-write it (the ring is registered with the rank
    /// host's fabric).
    target: Option<nest_fabric::push::PushTarget>,
}

/// A file a source streams, with the entries that read it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SourceFile {
    pub index: u32,
    pub file: FileId,
    pub generation: Generation,
    pub entries: Vec<Entry>,
}

fn invalid(m: impl Into<String>) -> NestError {
    NestError::Invalid(m.into())
}

// ------------------------------------------------------------------ state

/// What an engine's socket is told; its writer thread sends these in order.
type EngineTx = tokio::sync::mpsc::UnboundedSender<Event>;

/// A group as the hub holds it.
struct HubGroup {
    ranks: Vec<RankSpec>,
    rank_hosts: Vec<NodeId>,
    files: Vec<String>,
    patterns: Vec<Pattern>,
    entries: Vec<Entry>,
    engine: EngineTx,
    /// Rank → its ring's slot size, once joined.
    joined: HashMap<u32, u32>,
    joined_notify: Arc<tokio::sync::Notify>,
    /// Sources still streaming, and every source used.
    streaming: Vec<Src>,
    sources: Vec<Src>,
    /// Slots per rank, summed over the sources that finished.
    slots: Vec<u64>,
    failed: Vec<(u32, String)>,
    total: u64,
    delivered: Vec<u64>,
    complete: Vec<bool>,
    progress_at: tokio::time::Instant,
    started: bool,
    /// When streaming started, and each source's last report (bytes per
    /// rank) for the record.
    t0: tokio::time::Instant,
    last_sent: HashMap<Src, Vec<u64>>,
    /// Each rank's ring as its host last reported it.
    rings: HashMap<u32, RingState>,
    /// The sources still owing each rank slots.
    owing: Vec<Vec<Src>>,
}

impl HubGroup {
    /// Why a started group has stopped moving, from its ranks' rings. With
    /// no slot free, a ring waits either on its engine (slots filled and
    /// not released) or on sources (slots granted and not filled).
    fn stall_reason(&self) -> Option<String> {
        let mut why: Vec<String> = Vec::new();
        for (rank, ring) in &self.rings {
            if ring.slots == 0 || ring.free > 0 {
                continue;
            }
            let host = self
                .ranks
                .get(*rank as usize)
                .map_or("?", |r| r.host.as_str());
            let held = ring.slots.saturating_sub(ring.granted);
            why.push(if held > 0 {
                let last = ring
                    .last_release_ms
                    .map_or("never released a slot".to_string(), |t| {
                        format!("last release {:.1} s after joining", t as f64 / 1000.0)
                    });
                format!(
                    "rank {rank} ({host}) stopped releasing ({held} of {} slots filled and held, {last}; {} filled, {} released)",
                    ring.slots, ring.filled, ring.released
                )
            } else {
                format!(
                    "rank {rank} ({host}) waits on sources (all {} slots granted, none filled; {} filled, {} released)",
                    ring.slots, ring.filled, ring.released
                )
            });
        }
        why.sort();
        (!why.is_empty()).then(|| why.join("; "))
    }
}

/// A rank this host receives for.
struct RankSlot {
    hub: NodeId,
    token: u64,
    arena: u64,
    ring: Option<Arc<Ring>>,
    engine: Option<EngineTx>,
    filled: u64,
    expected: Option<u64>,
    bytes: u64,
    reported: u64,
    /// Ring slots no credit names.
    free: Vec<u32>,
    /// Ring slots granted and not yet filled: to which source, which grant.
    granted: HashMap<u32, (Src, u64)>,
    /// Those of `granted` given as a source's floor, outside the host's
    /// budget (`push-grant-floor`).
    floor: std::collections::HashSet<u32>,
    /// The floor and the host's incast budget, read once at Join: fixed
    /// for the group (a setting changed mid-load applies to the next).
    floor_n: usize,
    budget: u64,
    /// The next grant's `seq`.
    next_seq: u64,
    /// The ring registered with this host's fabric (RDMA delivery).
    rdma: Option<Arc<nest_fabric::push::PushRing>>,
    /// This rank's token prefix: landing tokens are `id << 12 | slot`.
    token_id: u32,
    /// Sources owing this rank slots.
    sources: Vec<Src>,
    next: usize,
    done_sent: bool,
    /// When the rank joined, its releases and when the last came.
    joined_at: Option<tokio::time::Instant>,
    released: u64,
    last_release: Option<tokio::time::Instant>,
}

impl RankSlot {
    fn ring_state(&self) -> RingState {
        let since = |t: tokio::time::Instant| {
            self.joined_at
                .map_or(0, |j| t.saturating_duration_since(j).as_millis() as u64)
        };
        RingState {
            slots: self.ring.as_ref().map_or(0, |r| r.slots),
            free: self.free.len() as u32,
            granted: self.granted.len() as u32,
            filled: self.filled,
            released: self.released,
            last_release_ms: self.last_release.map(since),
            at_ms: since(tokio::time::Instant::now()),
        }
    }
}

/// A source stream's grants per rank, and its wake-up.
struct SourceState {
    credits: Vec<VecDeque<Grant>>,
    notify: Arc<tokio::sync::Notify>,
    cancelled: bool,
}

/// The engine's ring, mapped into this process: `slots` of `slot_bytes`.
struct Ring {
    map: Arc<nest_fabric::verbs::SharedMap>,
    slots: u32,
    slot_bytes: u32,
}

impl Ring {
    fn map(fd: std::os::fd::OwnedFd, slots: u32, slot_bytes: u32) -> NestResult<Ring> {
        let len = (slots as usize)
            .checked_mul(slot_bytes as usize)
            .filter(|l| *l > 0 && slot_bytes as usize > slot::FOOTER + slot::FRAG)
            .ok_or_else(|| invalid("bad ring geometry"))?;
        let map = nest_fabric::verbs::SharedMap::map(fd, len)
            .map_err(|e| invalid(format!("mapping the ring: {e}")))?;
        Ok(Ring {
            map: Arc::new(map),
            slots,
            slot_bytes,
        })
    }

    /// Copy a received slot into ring slot `i`, which this host owns: a
    /// credit named it and it is not handed to the engine until checked.
    fn write_slot(&self, i: u32, bytes: &[u8]) -> NestResult<()> {
        if i >= self.slots || bytes.len() != self.slot_bytes as usize {
            return Err(invalid("slot does not fit the ring"));
        }
        self.map
            .write_owned(i as usize * self.slot_bytes as usize, bytes);
        Ok(())
    }

    /// Ring slot `i`, owned as for `write_slot`.
    fn slot(&self, i: u32) -> &[u8] {
        self.map.read_owned(
            i as usize * self.slot_bytes as usize,
            self.slot_bytes as usize,
        )
    }
}

impl State {
    /// Return a departing rank's outstanding grants to the budget.
    fn release_grants(&mut self, r: &RankSlot) {
        let b = r.ring.as_ref().map_or(0, |x| x.slot_bytes as u64);
        let budgeted = r.granted.len() - r.floor.len();
        self.granted_bytes = self.granted_bytes.saturating_sub(b * budgeted as u64);
    }
}

#[derive(Default)]
struct State {
    hub: HashMap<u64, HubGroup>,
    ranks: HashMap<(u64, u32), RankSlot>,
    /// Streams on this host, by group and the store each reads.
    sources: HashMap<(u64, StoreId), SourceState>,
    /// Credits that came before their stream's `Stream` (rank hosts start
    /// granting as soon as they learn their sources): (group, store, rank).
    early: HashMap<(u64, StoreId, u32), Vec<Grant>>,
    /// Loads this host was the hub of, for status (`push_record`).
    history: crate::push_record::History,
    /// Landing tokens' rank ids → the rank they belong to.
    tokens: HashMap<u32, (u64, u32)>,
    next_token: u32,
    /// Bytes granted to sources and not yet landed, over every rank on this
    /// host: kept within the incast budget, as reads are (ADR-032). Six
    /// sources writing a whole ring at once overran the switch's buffers
    /// (RC retries exhausted, lanes dropped).
    granted_bytes: u64,
}

/// What this host's push service has done, for status and tests.
#[derive(Clone, Copy, Debug, Default)]
pub struct PushStats {
    /// Slots this host sent by RDMA and by RPC (as a source).
    pub rdma_slots: u64,
    pub rpc_slots: u64,
}

/// The push service of one host.
pub struct Push {
    vfs: std::sync::Weak<Vfs>,
    state: Mutex<State>,
    next_group: AtomicU64,
    /// The fabric's landing sink (it holds a Weak).
    landings: std::sync::OnceLock<Arc<dyn nest_fabric::push::PushSink>>,
    rdma_slots: AtomicU64,
    rpc_slots: AtomicU64,
    /// Tests: report this many RDMA writes that landed as failed, as when
    /// a completion is lost to a lane failure (the slot then also goes by
    /// RPC).
    lose_acks: AtomicU64,
}

/// A file resolved at start: its current sealed generation, pinned for the
/// group (a later write makes its source fail it as stale).
struct Resolved {
    file: FileId,
    generation: Generation,
    size: u64,
    /// The copies to read it from: those of the nearest kind there is
    /// (fabric hosts, else archives, else off-grid hosts; ADR-063).
    holders: Vec<Src>,
}

/// Give each file to one holder so the holders finish together: largest
/// files first, each to the holder whose share would be done soonest at its
/// measured disk read rate (`rates`, bytes/s; a holder with no rate counts at
/// the median, and with no rates at all every holder counts the same, which
/// balances bytes). A host's rate is its own store's; an archive has none.
/// This host's own copies win ties (ADR-066).
fn assign(files: &[Resolved], me: NodeId, rates: &HashMap<StoreId, u64>) -> Vec<Src> {
    let mut known: Vec<u64> = rates.values().copied().filter(|b| *b > 0).collect();
    known.sort_unstable();
    let median = known.get(known.len() / 2).copied().unwrap_or(1);
    // Each of a host's disks streams at its own rate (ADR-073).
    let rate = |s: &Src| {
        rates
            .get(&s.store)
            .copied()
            .filter(|b| *b > 0)
            .unwrap_or(median) as f64
    };
    let mut order: Vec<usize> = (0..files.len()).collect();
    order.sort_by_key(|i| std::cmp::Reverse(files[*i].size));
    let mut load: HashMap<Src, u64> = HashMap::new();
    let mut out = vec![Src::host(me); files.len()];
    for i in order {
        let f = &files[i];
        let done = |s: &Src| (load.get(s).copied().unwrap_or(0) + f.size) as f64 / rate(s);
        let pick = *f
            .holders
            .iter()
            .min_by(|a, b| {
                done(a)
                    .total_cmp(&done(b))
                    .then((a.host != me).cmp(&(b.host != me)))
            })
            .expect("resolved files have holders");
        *load.entry(pick).or_default() += f.size;
        out[i] = pick;
    }
    out
}

impl Push {
    pub fn new(vfs: &Arc<Vfs>) -> Arc<Push> {
        let p = Arc::new(Push {
            vfs: Arc::downgrade(vfs),
            state: Mutex::new(State::default()),
            // Below 2^53, so ids stay exact as JSON numbers in the web UI;
            // random high bits keep hubs' ids apart.
            next_group: AtomicU64::new((rand::random::<u64>() >> 28) << 16),
            landings: std::sync::OnceLock::new(),
            rdma_slots: AtomicU64::new(0),
            rpc_slots: AtomicU64::new(0),
            lose_acks: AtomicU64::new(0),
        });
        vfs.rpc().register(
            nest_rpc::service::PUSH,
            Arc::new(PushService(Arc::downgrade(&p))),
        );
        // Slots that cannot go by RDMA travel on TCP lanes, off the control
        // connection (ADR-067).
        if let Some(l) = vfs.lanes() {
            let sink: Arc<dyn nest_fabric::tcp::MessageSink> =
                Arc::new(PushService(Arc::downgrade(&p)));
            l.set_message_sink(nest_rpc::service::PUSH, Arc::downgrade(&sink));
            // The lanes hold a Weak; this keeps the sink alive with the
            // process (one per host).
            std::mem::forget(sink);
        }
        let w = Arc::downgrade(&p);
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let Some(p) = w.upgrade() else { return };
                    p.sweep();
                }
            });
        }
        p
    }

    /// Tests: treat the next `n` RDMA slot writes from this host as failed
    /// although they landed (each is then sent again by RPC).
    pub fn lose_rdma_acks(&self, n: u64) {
        self.lose_acks.store(n, Ordering::Relaxed);
    }

    pub fn stats(&self) -> PushStats {
        PushStats {
            rdma_slots: self.rdma_slots.load(Ordering::Relaxed),
            rpc_slots: self.rpc_slots.load(Ordering::Relaxed),
        }
    }

    fn landings(self: &Arc<Self>) -> std::sync::Weak<dyn nest_fabric::push::PushSink> {
        let s = self.landings.get_or_init(|| {
            Arc::new(Landings(
                Arc::downgrade(self),
                tokio::runtime::Handle::current(),
            ))
        });
        Arc::downgrade(s)
    }

    fn vfs(&self) -> NestResult<Arc<Vfs>> {
        self.vfs
            .upgrade()
            .ok_or_else(|| NestError::Unavailable("shutting down".into()))
    }

    fn me(&self) -> NestResult<NodeId> {
        Ok(self.vfs()?.me())
    }

    /// Groups this host takes part in, for status: (group, role).
    pub fn groups(&self) -> Vec<(u64, &'static str)> {
        let st = self.state.lock();
        let mut v: Vec<(u64, &'static str)> = st.hub.keys().map(|g| (*g, "hub")).collect();
        v.extend(st.sources.keys().map(|(g, _)| (*g, "source")));
        v.extend(st.ranks.keys().map(|(g, _)| (*g, "rank")));
        v.sort_unstable();
        v.dedup();
        v
    }

    // ----------------------------------------------------------- transport

    /// Deliver `m` to `to`: a direct call when it is this host.
    fn send(self: &Arc<Self>, to: NodeId, m: PushMsg) -> BoxFuture<'static, NestResult<()>> {
        let me = self.clone();
        async move {
            let here = me.me()?;
            if to == here {
                return me.on_msg(here, m).await;
            }
            let v = me.vfs()?;
            let r: Result<Result<(), String>, _> =
                nest_rpc::typed::call(v.rpc(), to, nest_rpc::service::PUSH, &m, RPC).await;
            match r {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(NestError::Unavailable(e)),
                Err(e) => Err(NestError::Unavailable(e.to_string())),
            }
        }
        .boxed()
    }

    /// Deliver a slot (bulk): on a TCP lane that takes messages, else
    /// on the control connection. A lane send that fails before reaching
    /// the peer falls back; one with an unknown outcome is safe to send
    /// again (a duplicate fill is dropped by its grant's `seq`).
    async fn send_bulk(self: &Arc<Self>, to: NodeId, m: PushMsg) -> NestResult<()> {
        let here = self.me()?;
        if to != here
            && let Some(l) = self.vfs()?.lanes().filter(|l| l.takes_messages(to))
        {
            let body = nest_rpc::encode(&m).map_err(|e| NestError::Io(e.to_string()))?;
            match l.message(to, nest_rpc::service::PUSH, body.into()).await {
                Ok(()) => return Ok(()),
                Err(nest_fabric::WriteError::Failed(e)) if !is_lane_failure(&e) => {
                    return Err(e);
                }
                Err(e) => {
                    tracing::debug!(error = ?e, %to, "push slot over a TCP lane failed; using the control connection")
                }
            }
        }
        self.send(to, m).await
    }

    /// A message from another host, or this one.
    fn on_msg(self: &Arc<Self>, _from: NodeId, m: PushMsg) -> BoxFuture<'static, NestResult<()>> {
        let me = self.clone();
        async move {
            match m {
                PushMsg::Expect {
                    group,
                    token,
                    hub,
                    rank,
                    arena,
                } => me.expect(group, token, hub, rank, arena),
                PushMsg::Joined {
                    group,
                    rank,
                    slot_bytes,
                } => me.joined(group, rank, slot_bytes),
                PushMsg::Sources {
                    group,
                    rank,
                    sources,
                } => {
                    me.rank_sources(group, rank, sources);
                    Ok(())
                }
                PushMsg::Stream {
                    group,
                    hub,
                    source,
                    patterns,
                    files,
                    ranks,
                } => {
                    me.stream(group, hub, source, patterns, files, ranks);
                    Ok(())
                }
                PushMsg::Credit {
                    group,
                    store,
                    rank,
                    grants,
                } => {
                    me.credit(group, store, rank, grants);
                    Ok(())
                }
                PushMsg::Slot {
                    group,
                    rank,
                    source,
                    slot,
                    seq,
                    bytes,
                } => me.slot_rpc(group, rank, source, slot, seq, &bytes),
                PushMsg::SourceDone {
                    group,
                    source,
                    slots,
                    failed,
                } => {
                    me.source_done(group, source, slots, failed).await;
                    Ok(())
                }
                PushMsg::Delivered {
                    group,
                    rank,
                    bytes,
                    complete,
                } => {
                    me.delivered(group, rank, bytes, complete);
                    Ok(())
                }
                PushMsg::RankState { group, rank, ring } => {
                    me.rank_state(group, rank, ring);
                    Ok(())
                }
                PushMsg::Sent {
                    group,
                    source,
                    bytes,
                    rdma,
                    rpc,
                    local,
                } => {
                    me.sent(group, source, &bytes, rdma, rpc, local);
                    Ok(())
                }
                PushMsg::RankDone { group, rank, slots } => {
                    me.rank_done(group, rank, slots);
                    Ok(())
                }
                PushMsg::End { group, reason } => {
                    // To the hub, a rank's end fails the group: every host
                    // in it hears, so sources stop and other ranks are told.
                    if me.state.lock().hub.contains_key(&group) {
                        me.fail(group, reason, Vec::new()).await;
                    } else {
                        me.end_here(group, &reason);
                    }
                    Ok(())
                }
            }
        }
        .boxed()
    }

    // ---------------------------------------------------------------- hub

    /// Open a group, and tell every rank host to expect its ranks before
    /// answering: ranks may join as soon as the coordinator hands them the
    /// group and token, before or after `Start` (ADR-063).
    async fn create(
        self: &Arc<Self>,
        ranks: Vec<RankSpec>,
        files: Vec<String>,
        engine: EngineTx,
    ) -> NestResult<(u64, u64)> {
        if ranks.is_empty() {
            return Err(invalid("no ranks"));
        }
        if ranks.iter().enumerate().any(|(i, r)| r.rank as usize != i) {
            return Err(invalid("ranks must be numbered 0.. in order"));
        }
        let rank_hosts = {
            let c = self.vfs()?.cluster()?;
            ranks
                .iter()
                .map(|r| {
                    c["hosts"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .find(|h| h["name"] == r.host.as_str())
                        .and_then(|h| h["node"].as_u64())
                        .map(NodeId)
                        .ok_or_else(|| invalid(format!("no host named {}", r.host)))
                })
                .collect::<NestResult<Vec<_>>>()?
        };
        let group = self.next_group.fetch_add(1, Ordering::Relaxed);
        let token = rand::random::<u64>();
        let n = ranks.len();
        self.state.lock().hub.insert(
            group,
            HubGroup {
                ranks,
                rank_hosts,
                files,
                patterns: Vec::new(),
                entries: Vec::new(),
                engine,
                joined: HashMap::new(),
                joined_notify: Arc::new(tokio::sync::Notify::new()),
                streaming: Vec::new(),
                sources: Vec::new(),
                slots: vec![0; n],
                failed: Vec::new(),
                total: 0,
                delivered: vec![0; n],
                complete: vec![false; n],
                progress_at: tokio::time::Instant::now(),
                started: false,
                t0: tokio::time::Instant::now(),
                last_sent: HashMap::new(),
                rings: HashMap::new(),
                owing: Vec::new(),
            },
        );
        let (me, expect) = {
            let st = self.state.lock();
            let g = &st.hub[&group];
            (
                self.me()?,
                g.rank_hosts
                    .iter()
                    .zip(&g.ranks)
                    .map(|(h, r)| (*h, r.rank, r.arena_bytes))
                    .collect::<Vec<_>>(),
            )
        };
        for (host, rank, arena) in expect {
            let sent = self
                .send(
                    host,
                    PushMsg::Expect {
                        group,
                        token,
                        hub: me,
                        rank,
                        arena,
                    },
                )
                .await;
            if let Err(e) = sent {
                self.fail(group, format!("rank {rank}'s host: {e}"), Vec::new())
                    .await;
                return Err(e);
            }
        }
        Ok((group, token))
    }

    fn add_plan(&self, group: u64, patterns: Vec<Pattern>, entries: Vec<Entry>) -> NestResult<()> {
        let mut st = self.state.lock();
        let g = st
            .hub
            .get_mut(&group)
            .ok_or_else(|| invalid("no such group"))?;
        if g.started {
            return Err(invalid("already started"));
        }
        g.patterns.extend(patterns);
        g.entries.extend(entries);
        Ok(())
    }

    fn resolve(&self, files: &[String]) -> NestResult<Vec<Resolved>> {
        let v = self.vfs()?;
        files
            .iter()
            .map(|p| {
                let id = v
                    .resolve_path(p)?
                    .ok_or_else(|| invalid(format!("{p}: not found")))?;
                let (a, _) = v.copies(id)?;
                if a.kind != nest_types::FileKind::Regular {
                    return Err(invalid(format!("{p}: not a regular file")));
                }
                if !a.sealed || a.gen_state != nest_types::GenState::Stable {
                    return Err(invalid(format!("{p}: not sealed")));
                }
                let copies = v.push_holders(&a)?;
                let Some(nearest) = copies.iter().map(|c| c.2).min() else {
                    return Err(invalid(format!("{p}: no live copy")));
                };
                let holders = copies
                    .iter()
                    .filter(|c| c.2 == nearest)
                    .map(|&(store, host, _)| Src { store, host })
                    .collect();
                Ok(Resolved {
                    file: id,
                    generation: a.generation,
                    size: a.size,
                    holders,
                })
            })
            .collect()
    }

    /// `Start`: check the plan, tell rank hosts what to expect, wait for
    /// every rank to join, then start the sources.
    async fn start(self: &Arc<Self>, group: u64) -> NestResult<()> {
        let me = self.me()?;
        let (plan, ranks, rank_hosts, notify) = {
            let mut st = self.state.lock();
            let g = st
                .hub
                .get_mut(&group)
                .ok_or_else(|| invalid("no such group"))?;
            if g.started {
                return Err(invalid("already started"));
            }
            g.started = true;
            let plan = Plan {
                files: g.files.clone(),
                patterns: std::mem::take(&mut g.patterns),
                entries: std::mem::take(&mut g.entries),
            };
            (
                plan,
                g.ranks.clone(),
                g.rank_hosts.clone(),
                g.joined_notify.clone(),
            )
        };
        let resolved = {
            let me2 = self.clone();
            let files = plan.files.clone();
            tokio::task::spawn_blocking(move || me2.resolve(&files))
                .await
                .map_err(|e| NestError::Io(e.to_string()))??
        };
        let sizes: Vec<u64> = resolved.iter().map(|r| r.size).collect();
        let arenas: Vec<u64> = ranks.iter().map(|r| r.arena_bytes).collect();
        let totals = plan
            .check(&sizes, &arenas)
            .map_err(|e| invalid(format!("plan: {e}")))?;
        let rates = self
            .vfs()
            .map(|v| v.balancer().store_rates())
            .unwrap_or_default();
        let holder = assign(&resolved, me, &rates);

        // Rank hosts were told to expect their ranks at Create; wait for
        // every rank's Join (some may have joined already).
        let deadline = tokio::time::Instant::now() + JOIN_WAIT;
        let slot_bytes = loop {
            let n = notify.notified();
            {
                let st = self.state.lock();
                let g = st.hub.get(&group).ok_or_else(|| invalid("group ended"))?;
                if g.joined.len() == ranks.len() {
                    break (0..ranks.len() as u32)
                        .map(|r| g.joined[&r])
                        .collect::<Vec<u32>>();
                }
            }
            if tokio::time::timeout_at(deadline, n).await.is_err() {
                return Err(NestError::Unavailable(
                    "not every rank joined in time".into(),
                ));
            }
        };

        // Each source's files, with the entries that read them.
        let mut per_file: Vec<Vec<Entry>> = vec![Vec::new(); plan.files.len()];
        for e in plan.entries {
            per_file[e.file as usize].push(e);
        }
        let mut by_source: BTreeMap<Src, Vec<SourceFile>> = BTreeMap::new();
        for (i, entries) in per_file.into_iter().enumerate() {
            if entries.is_empty() {
                continue;
            }
            let r = &resolved[i];
            by_source.entry(holder[i]).or_default().push(SourceFile {
                index: i as u32,
                file: r.file,
                generation: r.generation,
                entries,
            });
        }
        // Which sources owe each rank anything.
        let mut owing: Vec<Vec<Src>> = vec![Vec::new(); ranks.len()];
        for (src, files) in &by_source {
            let mut rs: Vec<u32> = files
                .iter()
                .flat_map(|f| f.entries.iter().map(|e| e.rank))
                .collect();
            rs.sort_unstable();
            rs.dedup();
            for r in rs {
                owing[r as usize].push(*src);
            }
        }
        if let Some(g) = self.state.lock().hub.get_mut(&group) {
            g.owing = owing.clone();
        }
        let forecast = Forecast {
            streams: by_source
                .iter()
                .map(|(n, fs)| StreamForecast {
                    store: self.src_name(*n),
                    bytes: fs.iter().map(|f| totals.file_bytes[f.index as usize]).sum(),
                    files: fs.len() as u32,
                    transport: if n.host == me { "local" } else { "rpc" }.into(),
                    bytes_per_s: 0,
                })
                .collect(),
            rank_bytes: totals.rank_bytes.clone(),
            millis: 0,
        };
        {
            let mut st = self.state.lock();
            let g = st
                .hub
                .get_mut(&group)
                .ok_or_else(|| invalid("group ended"))?;
            g.total = totals.rank_bytes.iter().sum();
            g.streaming = by_source.keys().copied().collect();
            g.sources = g.streaming.clone();
            g.progress_at = tokio::time::Instant::now();
            g.t0 = tokio::time::Instant::now();
            // Planned goes out before any source is told to stream: once
            // one is, Progress (or Done, for an empty plan) can follow.
            let _ = g.engine.send(Event::Planned(forecast));
        }
        {
            let names_s: Vec<String> = by_source.keys().map(|n| self.src_name(*n)).collect();
            let names_r: Vec<String> = rank_hosts.iter().map(|n| self.name_of(*n)).collect();
            let started = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as u64);
            let rec = crate::push_record::Record::new(
                group,
                self.name_of(me),
                started,
                names_s,
                names_r,
                totals.rank_bytes.iter().sum(),
            );
            self.state.lock().history.push(rec);
        }
        for (r, host) in rank_hosts.iter().enumerate() {
            self.send(
                *host,
                PushMsg::Sources {
                    group,
                    rank: r as u32,
                    sources: owing[r].clone(),
                },
            )
            .await?;
        }
        let rank_info: Vec<(NodeId, u32)> = rank_hosts.iter().copied().zip(slot_bytes).collect();
        for (src, files) in by_source {
            self.send(
                src.host,
                PushMsg::Stream {
                    group,
                    hub: me,
                    source: src,
                    patterns: plan.patterns.clone(),
                    files,
                    ranks: rank_info.clone(),
                },
            )
            .await?;
        }
        // A plan with nothing to read is done at once.
        if self
            .state
            .lock()
            .hub
            .get(&group)
            .is_some_and(|g| g.streaming.is_empty())
        {
            self.finish(group).await;
        }
        Ok(())
    }

    fn name_of(&self, n: NodeId) -> String {
        self.vfs()
            .and_then(|v| v.cluster())
            .ok()
            .and_then(|c| {
                c["hosts"]
                    .as_array()?
                    .iter()
                    .find(|h| h["node"].as_u64() == Some(n.0))
                    .and_then(|h| h["name"].as_str().map(str::to_string))
            })
            .unwrap_or_else(|| format!("node{}", n.0))
    }

    /// A source's name in records: its host's, or its archive's.
    fn src_name(&self, s: Src) -> String {
        if s.store == s.host.live_store() {
            return self.name_of(s.host);
        }
        if s.store.is_disk() {
            let k = s.store.0 & 0xff;
            return self
                .vfs()
                .ok()
                .and_then(|v| {
                    v.data()
                        .with_reader(nest_meta::query::stores)
                        .ok()?
                        .into_iter()
                        .find(|r| r.id == s.store)
                        .map(|r| r.name)
                })
                .unwrap_or_else(|| format!("{}:disk{k}", self.name_of(s.host)));
        }
        self.vfs()
            .ok()
            .and_then(|v| v.data().archive_row(s.store))
            .map_or_else(|| format!("store{}", s.store.0), |(name, _)| name)
    }

    /// Milliseconds since the group started streaming, and the index of
    /// `source` among its sources.
    fn rec_at(g: &HubGroup, source: Src) -> (u64, Option<usize>) {
        (
            g.t0.elapsed().as_millis() as u64,
            g.sources.iter().position(|s| *s == source),
        )
    }

    /// A source's report: into the load's record.
    fn sent(&self, group: u64, source: Src, bytes: &[u64], rdma: u64, rpc: u64, local: u64) {
        let mut st = self.state.lock();
        let State { hub, history, .. } = &mut *st;
        let Some(g) = hub.get_mut(&group) else { return };
        let (at, Some(i)) = Self::rec_at(g, source) else {
            return;
        };
        let last = g.last_sent.entry(source).or_default();
        if let Some(r) = history.get_mut(group) {
            r.report(i, at, bytes, last);
            r.rdma_slots[i] = r.rdma_slots[i].max(rdma);
            r.rpc_slots[i] = r.rpc_slots[i].max(rpc);
            r.local_slots[i] = r.local_slots[i].max(local);
        }
    }

    /// End the load's record.
    fn record_end(&self, group: u64, outcome: crate::push_record::Outcome, reason: Option<String>) {
        let mut st = self.state.lock();
        let at = st
            .hub
            .get(&group)
            .map(|g| g.t0.elapsed().as_millis() as u64);
        if let Some(r) = st.history.get_mut(group)
            && r.outcome == crate::push_record::Outcome::Running
        {
            if let Some(at) = at {
                r.elapsed_ms = r.elapsed_ms.max(at);
            }
            r.outcome = outcome;
            r.reason = reason;
        }
    }

    /// This hub's recent loads, newest last; `bins` only for `group`.
    pub fn history(&self, group: Option<u64>) -> Vec<crate::push_record::Record> {
        let mut st = self.state.lock();
        let State { hub, history, .. } = &mut *st;
        // Running loads: their elapsed time is now.
        for (id, g) in hub.iter() {
            if let Some(r) = history.get_mut(*id)
                && r.outcome == crate::push_record::Outcome::Running
            {
                r.elapsed_ms = r.elapsed_ms.max(g.t0.elapsed().as_millis() as u64);
            }
        }
        history
            .all()
            .map(|r| {
                if Some(r.group) == group {
                    r.clone()
                } else {
                    r.summary()
                }
            })
            .collect()
    }

    fn joined(&self, group: u64, rank: u32, slot_bytes: u32) -> NestResult<()> {
        let mut st = self.state.lock();
        let g = st
            .hub
            .get_mut(&group)
            .ok_or_else(|| invalid("no such group"))?;
        g.joined.insert(rank, slot_bytes);
        g.joined_notify.notify_waiters();
        Ok(())
    }

    /// A rank host's report: progress to the coordinator, and `Done` once
    /// every rank has everything.
    fn delivered(&self, group: u64, rank: u32, bytes: u64, complete: bool) {
        let mut st = self.state.lock();
        let Some(g) = st.hub.get_mut(&group) else {
            return;
        };
        if let Some(d) = g.delivered.get_mut(rank as usize) {
            *d = (*d).max(bytes);
        }
        let mut done_at = None;
        if complete && let Some(c) = g.complete.get_mut(rank as usize) {
            *c = true;
            done_at = Some(g.t0.elapsed().as_millis() as u64);
        }
        if let Some(at) = done_at
            && let Some(r) = st.history.get_mut(group)
            && let Some(d) = r.rank_done_ms.get_mut(rank as usize)
        {
            *d = Some(at);
        }
        let Some(g) = st.hub.get_mut(&group) else {
            return;
        };
        g.progress_at = tokio::time::Instant::now();
        let _ = g.engine.send(Event::Progress {
            done: g.delivered.iter().sum(),
            total: g.total,
        });
        if g.complete.iter().all(|c| *c) {
            let _ = g.engine.send(Event::Done);
            drop(st);
            self.record_end(group, crate::push_record::Outcome::Done, None);
            self.state.lock().hub.remove(&group);
        }
    }

    async fn source_done(
        self: &Arc<Self>,
        group: u64,
        source: Src,
        slots: Vec<u64>,
        failed: Vec<(u32, String)>,
    ) {
        tracing::trace!(group, ?source, ?slots, ?failed, "push source done");
        let all_done = {
            let mut st = self.state.lock();
            let State { hub, history, .. } = &mut *st;
            let Some(g) = hub.get_mut(&group) else {
                return;
            };
            let (at, i) = Self::rec_at(g, source);
            if let (Some(i), Some(r)) = (i, history.get_mut(group)) {
                r.source_done_ms[i] = Some(at);
            }
            g.streaming.retain(|s| *s != source);
            for (i, n) in slots.iter().enumerate() {
                if let Some(t) = g.slots.get_mut(i) {
                    *t += n;
                }
            }
            g.failed.extend(failed);
            g.progress_at = tokio::time::Instant::now();
            // Ranks that this source owed: their hosts must stop granting
            // it slots (it will fill none), or a finished source collects
            // every free ring slot and the others starve.
            let mut tell = Vec::new();
            for (r, owing) in g.owing.iter_mut().enumerate() {
                if owing.contains(&source) {
                    owing.retain(|s| *s != source);
                    tell.push((g.rank_hosts[r], r as u32, owing.clone()));
                }
            }
            (g.streaming.is_empty(), tell)
        };
        let (all_done, tell) = all_done;
        if all_done {
            self.finish(group).await;
            return;
        }
        for (host, rank, sources) in tell {
            let _ = self
                .send(
                    host,
                    PushMsg::Sources {
                        group,
                        rank,
                        sources,
                    },
                )
                .await;
        }
    }

    /// Every source reported: fail the group if any file failed (phase 1:
    /// the engine reloads the whole group), else tell each rank host how
    /// many slots its rank gets in all.
    async fn finish(self: &Arc<Self>, group: u64) {
        let (failed, files, slots, hosts) = {
            let st = self.state.lock();
            let Some(g) = st.hub.get(&group) else {
                return;
            };
            (
                g.failed.clone(),
                g.files.clone(),
                g.slots.clone(),
                g.rank_hosts.clone(),
            )
        };
        if let Some((_, why)) = failed.first() {
            let n = hosts.len() as u32;
            let missing = failed
                .iter()
                .filter_map(|(i, _)| files.get(*i as usize))
                .map(|f| Missing {
                    file: f.clone(),
                    ranks: (0..n).collect(),
                })
                .collect();
            self.fail(group, why.clone(), missing).await;
            return;
        }
        for (r, host) in hosts.iter().enumerate() {
            if let Err(e) = self
                .send(
                    *host,
                    PushMsg::RankDone {
                        group,
                        rank: r as u32,
                        slots: slots[r],
                    },
                )
                .await
            {
                self.fail(group, format!("telling rank {r}'s host: {e}"), Vec::new())
                    .await;
                return;
            }
        }
    }

    /// End the group everywhere with `reason`.
    async fn fail(self: &Arc<Self>, group: u64, reason: String, missing: Vec<Missing>) {
        tracing::info!(group, %reason, "push group failed; telling its hosts");
        self.record_end(
            group,
            crate::push_record::Outcome::Failed,
            Some(reason.clone()),
        );
        let hosts: Vec<NodeId> = {
            let mut st = self.state.lock();
            let Some(g) = st.hub.remove(&group) else {
                return;
            };
            let _ = g.engine.send(Event::Failed {
                reason: reason.clone(),
                missing,
            });
            let mut h = g.rank_hosts.clone();
            h.extend(g.sources.iter().map(|s| s.host));
            h.sort_unstable();
            h.dedup();
            h
        };
        for h in hosts {
            let sent = self
                .send(
                    h,
                    PushMsg::End {
                        group,
                        reason: reason.clone(),
                    },
                )
                .await;
            if let Err(e) = sent {
                tracing::warn!(group, host = %h, error = %e, "push End not delivered");
            }
        }
    }

    /// Fail groups that made no progress for `STALL`; report this host's
    /// live ranks' rings to their hubs.
    fn sweep(self: &Arc<Self>) {
        let stale: Vec<(u64, String)> = {
            let st = self.state.lock();
            st.hub
                .iter()
                .filter(|(_, g)| g.started && g.progress_at.elapsed() > STALL)
                .map(|(id, g)| {
                    let why = g.stall_reason().map_or(String::new(), |w| format!(": {w}"));
                    (*id, format!("no progress for {} s{why}", STALL.as_secs()))
                })
                .collect()
        };
        for (g, why) in stale {
            let me = self.clone();
            tokio::spawn(async move { me.fail(g, why, Vec::new()).await });
        }
        let live: Vec<(NodeId, u64, u32, RingState)> = {
            let st = self.state.lock();
            st.ranks
                .iter()
                .filter(|(_, r)| r.ring.is_some() && !r.done_sent)
                .map(|((g, k), r)| (r.hub, *g, *k, r.ring_state()))
                .collect()
        };
        for (hub, group, rank, ring) in live {
            let me = self.clone();
            tokio::spawn(async move {
                let _ = me.send(hub, PushMsg::RankState { group, rank, ring }).await;
            });
        }
    }

    /// A rank host's report of its rank's ring (hub).
    fn rank_state(&self, group: u64, rank: u32, ring: RingState) {
        let mut st = self.state.lock();
        if let Some(g) = st.hub.get_mut(&group) {
            g.rings.insert(rank, ring.clone());
        }
        if let Some(r) = st.history.get_mut(group) {
            let n = r.ranks.len();
            r.rings.resize(n, None);
            if let Some(x) = r.rings.get_mut(rank as usize) {
                *x = Some(ring);
            }
        }
    }

    // ---------------------------------------------------------- rank host

    fn expect(&self, group: u64, token: u64, hub: NodeId, rank: u32, arena: u64) -> NestResult<()> {
        self.state.lock().ranks.insert(
            (group, rank),
            RankSlot {
                hub,
                token,
                arena,
                ring: None,
                engine: None,
                filled: 0,
                expected: None,
                bytes: 0,
                reported: 0,
                free: Vec::new(),
                granted: HashMap::new(),
                floor: Default::default(),
                floor_n: 1,
                budget: 64 << 20,
                next_seq: 0,
                rdma: None,
                token_id: 0,
                sources: Vec::new(),
                next: 0,
                done_sent: false,
                joined_at: None,
                released: 0,
                last_release: None,
            },
        );
        Ok(())
    }

    /// `Join` from a rank's engine on this host.
    async fn join(
        self: &Arc<Self>,
        group: u64,
        token: u64,
        rank: u32,
        ring: Ring,
        engine: EngineTx,
    ) -> NestResult<()> {
        let (budget, floor_n) = (self.budget(), self.floor());
        let (hub, slot_bytes) = {
            let mut st = self.state.lock();
            let r = st
                .ranks
                .get_mut(&(group, rank))
                .ok_or_else(|| invalid("this host does not expect that rank"))?;
            if r.token != token {
                return Err(invalid("bad token"));
            }
            if r.ring.is_some() {
                return Err(invalid("rank already joined"));
            }
            r.free = (0..ring.slots).rev().collect();
            (r.budget, r.floor_n) = (budget, floor_n);
            let b = ring.slot_bytes;
            // With a fabric, sources' NICs write straight into the ring;
            // a ring the NIC cannot register is filled over RPC instead.
            if ring.slots <= 1 << 12
                && let Ok(v) = self.vfs()
                && let Some(f) = v.fabric()
            {
                f.set_push_sink(self.landings());
                match f.register_push_ring(ring.map.clone()) {
                    Ok(reg) => r.rdma = Some(Arc::new(reg)),
                    Err(e) => {
                        tracing::info!(error = %e, "push ring not registered with the NIC; slots come over RPC")
                    }
                }
            }
            r.ring = Some(Arc::new(ring));
            r.engine = Some(engine);
            r.joined_at = Some(tokio::time::Instant::now());
            (r.hub, b)
        };
        {
            let mut st = self.state.lock();
            st.next_token = (st.next_token + 1) % (1 << 19);
            let id = st.next_token;
            st.tokens.insert(id, (group, rank));
            if let Some(r) = st.ranks.get_mut(&(group, rank)) {
                r.token_id = id;
            }
        }
        self.send(
            hub,
            PushMsg::Joined {
                group,
                rank,
                slot_bytes,
            },
        )
        .await
    }

    /// The sources owing this rank, at Start and again whenever one
    /// finishes: grants held by a source no longer listed come back (it
    /// will fill none of them).
    fn rank_sources(self: &Arc<Self>, group: u64, rank: u32, sources: Vec<Src>) {
        {
            let mut st = self.state.lock();
            let Some(r) = st.ranks.get_mut(&(group, rank)) else {
                return;
            };
            let back: Vec<u32> = r
                .granted
                .iter()
                .filter(|(_, (s, _))| !sources.contains(s))
                .map(|(slot, _)| *slot)
                .collect();
            let mut budgeted = 0;
            for slot in &back {
                r.granted.remove(slot);
                if !r.floor.remove(slot) {
                    budgeted += 1;
                }
                r.free.push(*slot);
            }
            r.sources = sources;
            let b = r.ring.as_ref().map_or(0, |x| x.slot_bytes as u64);
            st.granted_bytes = st.granted_bytes.saturating_sub(b * budgeted);
        }
        self.grant(group, rank);
    }

    /// Hand free ring slots to sources as grants: first each owing source's
    /// floor (`push-grant-floor` grants of this rank held at once, outside
    /// the budget), then, while this host's grants outstanding stay within
    /// the incast budget, each to the owing source holding fewest.
    ///
    /// Fewest held, not round robin: a slow source (an archive disk, an
    /// off-grid host over TCP) fills its grants slowly, and round robin
    /// kept handing it its turn until it held nearly the whole budget and
    /// every fast source waited on one grant each. Now it holds at most an
    /// even share, and streams at its own pace beside the others.
    ///
    /// The floor is what keeps two ranks on one host live: sources stream
    /// in file order, so a source blocked on rank B's credit holds rank A's
    /// grants unfilled; with one budget for both ranks, A's parked grants
    /// could take all of it and B never get one (group 120235912134667).
    fn grant(self: &Arc<Self>, group: u64, rank: u32) {
        let mut out: Vec<(Src, Vec<Grant>)> = Vec::new();
        {
            let mut st = self.state.lock();
            let mut granted = st.granted_bytes;
            let Some(r) = st.ranks.get_mut(&(group, rank)) else {
                return;
            };
            let (budget, floor_n) = (r.budget, r.floor_n);
            if r.sources.is_empty() {
                return;
            }
            let slot_bytes = r.ring.as_ref().map_or(0, |x| x.slot_bytes) as u64;
            // Each owing source's floor first, then the budget's round robin.
            let mut held: HashMap<Src, usize> = HashMap::new();
            for (s, _) in r.granted.values() {
                *held.entry(*s).or_default() += 1;
            }
            let mut floor: VecDeque<Src> = r
                .sources
                .iter()
                .flat_map(|s| {
                    let need = floor_n.saturating_sub(held.get(s).copied().unwrap_or(0));
                    std::iter::repeat_n(*s, need)
                })
                .collect();
            loop {
                let (s, is_floor) = if let Some(s) = floor.pop_front() {
                    (s, true)
                } else if granted + slot_bytes <= budget {
                    let s = fewest(&r.sources, &held, r.next);
                    r.next = r.next.wrapping_add(1);
                    (s, false)
                } else {
                    break;
                };
                let Some(slot) = r.free.pop() else { break };
                let seq = r.next_seq;
                r.next_seq += 1;
                r.granted.insert(slot, (s, seq));
                *held.entry(s).or_default() += 1;
                if is_floor {
                    r.floor.insert(slot);
                }
                let target = r
                    .rdma
                    .as_ref()
                    .map(|ring| ring.target(slot as u64 * slot_bytes, r.token_id << 12 | slot));
                let g = Grant { slot, seq, target };
                match out.iter_mut().find(|(n, _)| *n == s) {
                    Some((_, v)) => v.push(g),
                    None => out.push((s, vec![g])),
                }
                if !is_floor {
                    granted += slot_bytes;
                }
            }
            st.granted_bytes = granted;
        }
        for (src, grants) in out {
            let me = self.clone();
            tokio::spawn(async move {
                if let Err(e) = me
                    .send(
                        src.host,
                        PushMsg::Credit {
                            group,
                            store: src.store,
                            rank,
                            grants,
                        },
                    )
                    .await
                {
                    me.end_rank(group, rank, format!("credit to a source: {e}"));
                }
            });
        }
    }

    /// A slot sent by RPC for ring slot `slot`: written into the ring here.
    /// A source sends by RPC when its RDMA write reported failure, but the
    /// write may have landed all the same (the ack was lost, then the lane
    /// failed): a fill for a grant already filled is a duplicate and is
    /// dropped, never written over the slot's next fill.
    fn slot_rpc(
        self: &Arc<Self>,
        group: u64,
        rank: u32,
        source: Src,
        slot: u32,
        seq: u64,
        bytes: &[u8],
    ) -> NestResult<()> {
        {
            let st = self.state.lock();
            let r = st
                .ranks
                .get(&(group, rank))
                .ok_or_else(|| invalid("no such rank here"))?;
            if r.granted.get(&slot) != Some(&(source, seq)) {
                if seq < r.next_seq {
                    tracing::debug!(group, rank, slot, seq, "push slot already landed by RDMA");
                    return Ok(());
                }
                return Err(invalid("a slot without a credit"));
            }
            r.ring
                .as_ref()
                .ok_or_else(|| invalid("rank not joined"))?
                .write_slot(slot, bytes)?;
        }
        self.landed(group, rank, slot)
    }

    /// Ring slot `slot` of `rank` holds a slot (written by RPC above, or by
    /// a source's NIC): check it, then tell the engine.
    fn landed(self: &Arc<Self>, group: u64, rank: u32, slot: u32) -> NestResult<()> {
        let (hub, delivered, complete, report) = {
            let mut st = self.state.lock();
            let r = st
                .ranks
                .get_mut(&(group, rank))
                .ok_or_else(|| invalid("no such rank here"))?;
            let ring = r.ring.clone().ok_or_else(|| invalid("rank not joined"))?;
            if r.granted.remove(&slot).is_none() {
                return Err(invalid("a slot without a credit"));
            }
            // The slot is no longer in flight: its budget is free (a floor
            // grant never took any).
            if !r.floor.remove(&slot) {
                let b = ring.slot_bytes as u64;
                st.granted_bytes = st.granted_bytes.saturating_sub(b);
            }
            let r = st.ranks.get_mut(&(group, rank)).expect("present");
            let p = match slot::parse(ring.slot(slot), r.arena) {
                Ok(p) => p,
                Err(e) => {
                    r.free.push(slot);
                    return Err(invalid(format!("bad slot: {e}")));
                }
            };
            r.filled += 1;
            r.bytes += p.payload as u64;
            tracing::trace!(group, rank, slot, filled = r.filled, expected = ?r.expected, engine = r.engine.is_some(), "push filled");
            if let Some(e) = &r.engine {
                let _ = e.send(Event::Filled {
                    slot,
                    len: ring.slot_bytes,
                });
            }
            let complete = r.expected == Some(r.filled) && !r.done_sent;
            if complete {
                r.done_sent = true;
                if let Some(e) = &r.engine {
                    let _ = e.send(Event::Done);
                }
            }
            let report = complete || r.bytes - r.reported >= REPORT_EVERY;
            if report {
                r.reported = r.bytes;
            }
            (r.hub, r.bytes, complete, report)
        };
        if report {
            self.report(group, rank, hub, delivered, complete);
        }
        // Budget freed: every rank here waiting for it may get grants now.
        self.grant_waiting();
        Ok(())
    }

    /// Grant to every rank on this host that has free ring slots.
    fn grant_waiting(self: &Arc<Self>) {
        let ranks: Vec<(u64, u32)> = {
            let st = self.state.lock();
            st.ranks
                .iter()
                .filter(|(_, r)| !r.free.is_empty() && !r.sources.is_empty())
                .map(|(k, _)| *k)
                .collect()
        };
        for (g, r) in ranks {
            self.grant(g, r);
        }
    }

    /// The incast budget: what this host lets sources have in flight at
    /// once toward its rings: `push-incast-mib`, else the fabric's read
    /// budget, or 64 MiB without a fabric.
    fn budget(&self) -> u64 {
        let Ok(v) = self.vfs() else { return 64 << 20 };
        match v.setting(nest_types::Setting::PushIncastMib) {
            0 => v.fabric().map_or(64 << 20, |f| f.incast_bytes()),
            mib => mib << 20,
        }
    }

    /// Grants of a rank each owing source always holds (`push-grant-floor`,
    /// at least 1).
    fn floor(&self) -> usize {
        self.vfs().map_or(1, |v| {
            v.setting(nest_types::Setting::PushGrantFloor).max(1) as usize
        })
    }

    fn report(self: &Arc<Self>, group: u64, rank: u32, hub: NodeId, bytes: u64, complete: bool) {
        let me = self.clone();
        tokio::spawn(async move {
            let _ = me
                .send(
                    hub,
                    PushMsg::Delivered {
                        group,
                        rank,
                        bytes,
                        complete,
                    },
                )
                .await;
        });
    }

    /// The engine released ring slot `slot`: free for a new credit.
    fn release(self: &Arc<Self>, group: u64, rank: u32, slot: u32) {
        {
            let mut st = self.state.lock();
            let Some(r) = st.ranks.get_mut(&(group, rank)) else {
                return;
            };
            if r.ring.as_ref().is_some_and(|ring| slot < ring.slots) && !r.free.contains(&slot) {
                r.free.push(slot);
                r.released += 1;
                r.last_release = Some(tokio::time::Instant::now());
            }
        }
        self.grant(group, rank);
    }

    fn rank_done(self: &Arc<Self>, group: u64, rank: u32, slots: u64) {
        tracing::trace!(group, rank, slots, "push rank done");
        let report = {
            let mut st = self.state.lock();
            let Some(r) = st.ranks.get_mut(&(group, rank)) else {
                return;
            };
            r.expected = Some(slots);
            // Sources are done: no more credits needed.
            r.sources.clear();
            if r.filled == slots && !r.done_sent {
                r.done_sent = true;
                if let Some(e) = &r.engine {
                    let _ = e.send(Event::Done);
                }
                Some((r.hub, r.bytes))
            } else {
                None
            }
        };
        if let Some((hub, bytes)) = report {
            self.report(group, rank, hub, bytes, true);
        }
    }

    fn end_rank(&self, group: u64, rank: u32, reason: String) {
        let mut st = self.state.lock();
        let removed = st.ranks.remove(&(group, rank));
        if let Some(r) = &removed {
            st.release_grants(r);
        }
        drop(st);
        if let Some(r) = removed
            && let Some(e) = r.engine
            && !r.done_sent
        {
            let _ = e.send(Event::Failed {
                reason,
                missing: Vec::new(),
            });
        }
    }

    /// `End` for `group`: drop this host's part of it.
    fn end_here(&self, group: u64, reason: &str) {
        let mut st = self.state.lock();
        let mut failed_ranks = Vec::new();
        let ranks: Vec<(u64, u32)> = st
            .ranks
            .keys()
            .filter(|(g, _)| *g == group)
            .copied()
            .collect();
        for k in ranks {
            let removed = st.ranks.remove(&k);
            if let Some(r) = &removed {
                st.release_grants(r);
            }
            if let Some(r) = removed
                && let Some(e) = r.engine
                && !r.done_sent
            {
                failed_ranks.push(k.1);
                let _ = e.send(Event::Failed {
                    reason: reason.to_string(),
                    missing: Vec::new(),
                });
            }
        }
        tracing::info!(group, %reason, ranks = ?failed_ranks, "push group ended here; ranks told");
        for ((g, _), s) in st.sources.iter_mut() {
            if *g == group {
                s.cancelled = true;
                s.notify.notify_waiters();
            }
        }
        st.early.retain(|(g, _, _), _| *g != group);
        if let Some(g) = st.hub.remove(&group) {
            let _ = g.engine.send(Event::Failed {
                reason: reason.to_string(),
                missing: Vec::new(),
            });
        }
    }

    // -------------------------------------------------------------- source

    fn stream(
        self: &Arc<Self>,
        group: u64,
        hub: NodeId,
        source: Src,
        patterns: Vec<Pattern>,
        files: Vec<SourceFile>,
        ranks: Vec<(NodeId, u32)>,
    ) {
        let notify = Arc::new(tokio::sync::Notify::new());
        {
            let mut st = self.state.lock();
            let mut credits: Vec<VecDeque<Grant>> = vec![VecDeque::new(); ranks.len()];
            for (r, c) in credits.iter_mut().enumerate() {
                *c = st
                    .early
                    .remove(&(group, source.store, r as u32))
                    .unwrap_or_default()
                    .into();
            }
            st.sources.insert(
                (group, source.store),
                SourceState {
                    credits,
                    notify: notify.clone(),
                    cancelled: false,
                },
            );
        }
        let me = self.clone();
        tokio::spawn(async move {
            let src = source;
            // Staging for RDMA writes, sized to the largest ring slot: one
            // per send in flight, and one per rank being packed.
            let slot_max = ranks.iter().map(|(_, b)| *b as usize).max().unwrap_or(0);
            let staged = in_flight(slot_max) + ranks.len();
            let pool = me
                .vfs()
                .ok()
                .and_then(|v| v.fabric())
                .and_then(|f| f.push_pool(staged as u32, slot_max).ok())
                .map(Arc::new);
            let counters = Arc::new(Counters::new(ranks.len()));
            let mut s = Stream {
                push: me.clone(),
                group,
                me: src,
                ranks,
                notify,
                open: Vec::new(),
                sent: Vec::new(),
                sends: tokio::task::JoinSet::new(),
                pool,
                counters: counters.clone(),
            };
            // What it has sent, to the hub every tick (the load's record).
            let ticker = {
                let (me2, c) = (me.clone(), counters.clone());
                tokio::spawn(async move {
                    let mut t = tokio::time::interval(crate::push_record::TICK);
                    loop {
                        t.tick().await;
                        let _ = me2.send(hub, c.msg(group, src)).await;
                    }
                })
            };
            let failed = s.run(&patterns, files).await;
            ticker.abort();
            let _ = me.send(hub, counters.msg(group, src)).await;
            let slots = std::mem::take(&mut s.sent);
            me.state.lock().sources.remove(&(group, src.store));
            let _ = me
                .send(
                    hub,
                    PushMsg::SourceDone {
                        group,
                        source: src,
                        slots,
                        failed,
                    },
                )
                .await;
        });
    }

    fn credit(&self, group: u64, store: StoreId, rank: u32, grants: Vec<Grant>) {
        let mut st = self.state.lock();
        match st.sources.get_mut(&(group, store)) {
            Some(s) => {
                if let Some(c) = s.credits.get_mut(rank as usize) {
                    c.extend(grants);
                    s.notify.notify_waiters();
                }
            }
            None => st
                .early
                .entry((group, store, rank))
                .or_default()
                .extend(grants),
        }
    }

    /// Wait for a grant for `rank` and take it; None if cancelled or none
    /// came within `STALL`.
    async fn take_credit(
        &self,
        group: u64,
        store: StoreId,
        rank: u32,
        notify: &tokio::sync::Notify,
    ) -> Option<Grant> {
        loop {
            let n = notify.notified();
            {
                let mut st = self.state.lock();
                let s = st.sources.get_mut(&(group, store))?;
                if s.cancelled {
                    return None;
                }
                if let Some(g) = s.credits[rank as usize].pop_front() {
                    return Some(g);
                }
            }
            if tokio::time::timeout(STALL, n).await.is_err() {
                return None;
            }
        }
    }

    // ----------------------------------------------------- engine sockets

    /// Serve one engine connection until it closes (its own threads: the
    /// socket is blocking).
    pub fn serve_engine(self: &Arc<Self>, conn: Conn) {
        let me = self.clone();
        let rt = tokio::runtime::Handle::current();
        std::thread::Builder::new()
            .name("push-engine".into())
            .spawn(move || me.engine_loop(conn, rt))
            .expect("thread");
    }

    fn engine_loop(self: Arc<Self>, conn: Conn, rt: tokio::runtime::Handle) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
        let conn = Arc::new(conn);
        let out = conn.clone();
        let writer = std::thread::Builder::new()
            .name("push-events".into())
            .spawn(move || {
                while let Some(e) = rx.blocking_recv() {
                    if out.send_event(&e).is_err() {
                        return;
                    }
                }
            })
            .expect("thread");
        let mut owns_group: Option<u64> = None;
        let mut owns_rank: Option<(u64, u32)> = None;
        while let Ok(Some((req, fd))) = conn.next_request() {
            let fresh = owns_group.is_none() && owns_rank.is_none();
            let ev = match req {
                Request::Create {
                    version: _,
                    ranks,
                    files,
                } if fresh => match rt.block_on(self.create(ranks, files, tx.clone())) {
                    Ok((group, token)) => {
                        owns_group = Some(group);
                        Some(Event::Created { group, token })
                    }
                    Err(e) => Some(Event::Refused(e.to_string())),
                },
                Request::Patterns(p) => owns_group
                    .ok_or_else(|| invalid("no group"))
                    .and_then(|g| self.add_plan(g, p, Vec::new()))
                    .err()
                    .map(|e| Event::Refused(e.to_string())),
                Request::Entries(e) => owns_group
                    .ok_or_else(|| invalid("no group"))
                    .and_then(|g| self.add_plan(g, Vec::new(), e))
                    .err()
                    .map(|e| Event::Refused(e.to_string())),
                Request::Start => match owns_group {
                    None => Some(Event::Refused("no group".into())),
                    Some(g) => {
                        let (me, tx2) = (self.clone(), tx.clone());
                        rt.spawn(async move {
                            if let Err(e) = me.start(g).await {
                                let _ = tx2.send(Event::Refused(e.to_string()));
                                me.fail(g, e.to_string(), Vec::new()).await;
                            }
                        });
                        None
                    }
                },
                Request::Join {
                    version: _,
                    group,
                    token,
                    rank,
                    slots,
                    slot_bytes,
                } if fresh => {
                    let joined = fd
                        .ok_or_else(|| invalid("Join needs the ring's fd"))
                        .and_then(|fd| Ring::map(fd, slots, slot_bytes))
                        .and_then(|ring| {
                            rt.block_on(self.join(group, token, rank, ring, tx.clone()))
                        });
                    match joined {
                        Ok(()) => {
                            owns_rank = Some((group, rank));
                            Some(Event::Joined)
                        }
                        Err(e) => Some(Event::Refused(e.to_string())),
                    }
                }
                Request::Release { slot } => {
                    if let Some((g, r)) = owns_rank {
                        let me = self.clone();
                        rt.spawn(async move { me.release(g, r, slot) });
                    }
                    None
                }
                _ => Some(Event::Refused("unexpected request".into())),
            };
            if let Some(e) = ev {
                let _ = tx.send(e);
            }
        }
        // The engine closed: a coordinator cancels its group, a rank that
        // had not finished fails it.
        if let Some(g) = owns_group {
            let me = self.clone();
            // A coordinator that gives up on a stalled load: say why it stalled.
            let why = {
                let st = self.state.lock();
                st.hub
                    .get(&g)
                    .filter(|h| h.started && h.progress_at.elapsed() > STALL_NOTE)
                    .and_then(|h| {
                        h.stall_reason().map(|w| {
                            format!(
                                "; no progress for {} s: {w}",
                                h.progress_at.elapsed().as_secs()
                            )
                        })
                    })
                    .unwrap_or_default()
            };
            rt.spawn(async move {
                me.fail(
                    g,
                    format!("the coordinator closed its socket{why}"),
                    Vec::new(),
                )
                .await
            });
        }
        if let Some((g, r)) = owns_rank {
            let unfinished = {
                let mut st = self.state.lock();
                let rs = st.ranks.remove(&(g, r));
                if let Some(rs) = &rs {
                    st.release_grants(rs);
                }
                rs.and_then(|rs| (!rs.done_sent).then_some(rs.hub))
            };
            if let Some(hub) = unfinished {
                let me = self.clone();
                rt.spawn(async move {
                    let _ = me
                        .send(
                            hub,
                            PushMsg::End {
                                group: g,
                                reason: format!("rank {r} closed its socket"),
                            },
                        )
                        .await;
                });
            }
        }
        drop(tx);
        let _ = writer.join();
    }
}

/// One source's pass over its files for a group.
/// A source's counters for one group, shared with its sends and ticker.
#[derive(Default)]
struct Counters {
    bytes: Vec<AtomicU64>,
    rdma: AtomicU64,
    rpc: AtomicU64,
    /// Slots to a rank on this host: no network.
    local: AtomicU64,
}

impl Counters {
    fn new(ranks: usize) -> Counters {
        Counters {
            bytes: (0..ranks).map(|_| AtomicU64::new(0)).collect(),
            ..Default::default()
        }
    }

    fn msg(&self, group: u64, source: Src) -> PushMsg {
        PushMsg::Sent {
            group,
            source,
            bytes: self
                .bytes
                .iter()
                .map(|b| b.load(Ordering::Relaxed))
                .collect(),
            rdma: self.rdma.load(Ordering::Relaxed),
            rpc: self.rpc.load(Ordering::Relaxed),
            local: self.local.load(Ordering::Relaxed),
        }
    }
}

struct Stream {
    push: Arc<Push>,
    group: u64,
    /// The store this stream reads, and this host.
    me: Src,
    /// Per rank: its host and slot size.
    ranks: Vec<(NodeId, u32)>,
    notify: Arc<tokio::sync::Notify>,
    /// Per rank: the slot being packed.
    open: Vec<Option<slot::Packer<SlotBuf>>>,
    /// Slots sent per rank (counted when handed to a send).
    sent: Vec<u64>,
    /// Sends in flight (bounded by `in_flight`).
    sends: tokio::task::JoinSet<Result<(), String>>,
    /// Registered staging for RDMA sends, when this host has a fabric.
    pool: Option<Arc<nest_fabric::push::PushPool>>,
    counters: Arc<Counters>,
}

/// Slot sends a source keeps in flight, at most: 8 capped a source near
/// 2.5 GB/s (each 1 MiB write waits a round trip) while its disk reads
/// several times that. Ranks' grants bound what reaches each port.
const IN_FLIGHT: usize = 32;
/// Registered staging a source keeps per group, at most.
const STAGING_BYTES: usize = 64 << 20;

/// Of `sources` (not empty), the one holding fewest grants; ties go to
/// the first from `from` on, so equal sources take turns.
fn fewest(sources: &[Src], held: &HashMap<Src, usize>, from: usize) -> Src {
    let n = sources.len();
    (0..n)
        .map(|k| sources[(from + k) % n])
        .min_by_key(|s| held.get(s).copied().unwrap_or(0))
        .expect("sources is not empty")
}

/// Sends in flight for slots of `slot_bytes`.
fn in_flight(slot_bytes: usize) -> usize {
    (STAGING_BYTES / slot_bytes.max(1)).clamp(2, IN_FLIGHT)
}

/// A file's reads, in order: its entries' ranges merged across small
/// gaps, cut into `READ_CHUNK`s.
fn reads_of(index: &FileEntries) -> Vec<(u64, u64)> {
    let mut merged: Vec<(u64, u64)> = Vec::new();
    for (a, b) in index.ranges() {
        match merged.last_mut() {
            Some(last) if a <= last.1.saturating_add(GAP) => last.1 = last.1.max(b),
            _ => merged.push((a, b)),
        }
    }
    merged
        .iter()
        .flat_map(|&(a, b)| {
            (a..b)
                .step_by(READ_CHUNK as usize)
                .map(move |at| (at, (at + READ_CHUNK).min(b)))
        })
        .collect()
}

/// A slot's memory while it is packed: registered staging when it is
/// bound for another host (packed in place, and the NIC sends it from
/// there), else plain.
enum SlotBuf {
    Staged(nest_fabric::push::PushSlot),
    Plain(Vec<u8>),
}

impl AsRef<[u8]> for SlotBuf {
    fn as_ref(&self) -> &[u8] {
        match self {
            SlotBuf::Staged(s) => s.as_ref(),
            SlotBuf::Plain(v) => v,
        }
    }
}

impl AsMut<[u8]> for SlotBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        match self {
            SlotBuf::Staged(s) => s.as_mut(),
            SlotBuf::Plain(v) => v,
        }
    }
}

impl Stream {
    /// Stream every file; the files that failed, by plan index.
    async fn run(&mut self, patterns: &[Pattern], files: Vec<SourceFile>) -> Vec<(u32, String)> {
        let n = self.ranks.len();
        self.open = (0..n).map(|_| None).collect();
        self.sent = vec![0; n];
        let mut failed: Vec<(u32, String)> = Vec::new();
        let vfs = match self.push.vfs() {
            Ok(v) => v,
            Err(e) => return files.iter().map(|f| (f.index, e.to_string())).collect(),
        };
        // Each pattern's extent, checked once (plans hold millions of
        // entries over a few patterns).
        let extents: Vec<Option<nest_push::plan::Extent>> =
            patterns.iter().map(|p| p.check().ok()).collect();
        // Each file's entries by offset: each read visits only those that
        // read it.
        let index: Vec<FileEntries> = files
            .iter()
            .map(|f| FileEntries::new(&f.entries, &extents))
            .collect();
        // Every read of every file, in order, with `READ_AHEAD` in flight
        // while earlier ones are packed, across file boundaries: the disk
        // never waits for the packing, nor for the next file's open.
        let reads: Vec<(usize, u64, u64)> = index
            .iter()
            .enumerate()
            .flat_map(|(i, x)| reads_of(x).into_iter().map(move |(a, b)| (i, a, b)))
            .collect();
        // Each file is opened by its first read, and closed after its last.
        type Opened = Arc<std::sync::OnceLock<Result<crate::vfs::PushObject, String>>>;
        let mut objs: Vec<Opened> = files.iter().map(|_| Opened::default()).collect();
        let mut ahead: VecDeque<tokio::task::JoinHandle<Result<PushRead, String>>> =
            VecDeque::new();
        let mut next = 0;
        for (k, &(i, at, end)) in reads.iter().enumerate() {
            while next < reads.len() && ahead.len() < READ_AHEAD {
                let (fi, a, b) = reads[next];
                let (v, obj, f) = (vfs.clone(), objs[fi].clone(), &files[fi]);
                let (file, generation, store) = (f.file, f.generation, self.me.store);
                ahead.push_back(tokio::task::spawn_blocking(move || {
                    let o = obj
                        .get_or_init(|| {
                            v.push_open_from(store, file, generation)
                                .map_err(|e| e.to_string())
                        })
                        .as_ref()
                        .map_err(|e| e.clone())?;
                    o.read(a, (b - a) as usize)
                        .map_err(|e| format!("reading: {e}"))
                }));
                next += 1;
            }
            let read = ahead
                .pop_front()
                .expect("a read in flight")
                .await
                .map_err(|e| e.to_string())
                .and_then(|r| r);
            if reads.get(k + 1).is_none_or(|r| r.0 != i) {
                objs[i] = Opened::default();
            }
            // The rest of a file that failed is skipped.
            if failed.last().is_some_and(|f| f.0 == files[i].index) {
                continue;
            }
            let packed = match read {
                Ok(buf) => self.pack(patterns, &index[i], at, end, &buf).await,
                Err(e) => Err(e),
            };
            if let Err(e) = packed {
                failed.push((files[i].index, e));
                if self.cancelled() {
                    return failed;
                }
            }
        }
        for r in 0..n as u32 {
            if let Err(e) = self.flush(r).await {
                failed.push((u32::MAX, e));
                break;
            }
        }
        while let Some(r) = self.sends.join_next().await {
            if let Err(e) = r.map_err(|e| e.to_string()).and_then(|x| x) {
                failed.push((u32::MAX, e));
            }
        }
        failed
    }

    fn cancelled(&self) -> bool {
        let st = self.push.state.lock();
        st.sources
            .get(&(self.group, self.me.store))
            .is_none_or(|s| s.cancelled)
    }

    /// Pack one read's bytes (`buf`, file range `[at, end)`) into the
    /// slots of the ranks whose entries read it.
    async fn pack(
        &mut self,
        patterns: &[Pattern],
        index: &FileEntries,
        at: u64,
        end: u64,
        buf: &[u8],
    ) -> Result<(), String> {
        for e in index.overlapping(at, end) {
            let p = &patterns[e.pattern as usize];
            for (s, d, n) in RunWalk::new(p, e, at, end) {
                let off = (s - at) as usize;
                self.put(e.rank, d, &buf[off..off + n as usize]).await?;
            }
        }
        Ok(())
    }

    /// Pack `data` (for arena offset `dst` of rank `r`), sending slots as
    /// they fill.
    async fn put(&mut self, r: u32, mut dst: u64, mut data: &[u8]) -> Result<(), String> {
        let ri = r as usize;
        while !data.is_empty() {
            if self.open[ri].is_none() {
                let (host, len) = (self.ranks[ri].0, self.ranks[ri].1 as usize);
                let buf = match &self.pool {
                    Some(p) if host != self.me.host && len <= p.slot_bytes() => {
                        SlotBuf::Staged(p.acquire(len).await)
                    }
                    _ => SlotBuf::Plain(vec![0u8; len]),
                };
                self.open[ri] = Some(slot::Packer::new(buf));
            }
            let s = self.open[ri].as_mut().expect("opened above");
            let took = s.push(dst, data);
            if took == 0 {
                self.flush(r).await?;
                continue;
            }
            data = &data[took..];
            dst += took as u64;
        }
        Ok(())
    }

    /// Send rank `r`'s open slot once a grant allows: by RDMA into the
    /// granted ring slot when possible, else by RPC.
    async fn flush(&mut self, r: u32) -> Result<(), String> {
        let ri = r as usize;
        let Some(s) = self.open[ri].take() else {
            return Ok(());
        };
        let (buf, payload, _) = s.finish();
        let grant = self
            .push
            .take_credit(self.group, self.me.store, r, &self.notify)
            .await
            .ok_or("cancelled, or no credit came")?;
        let limit = in_flight(
            self.ranks
                .iter()
                .map(|(_, b)| *b as usize)
                .max()
                .unwrap_or(0),
        );
        while self.sends.len() >= limit {
            if let Some(res) = self.sends.join_next().await {
                res.map_err(|e| e.to_string())??;
            }
        }
        let (push, group, me, to) = (self.push.clone(), self.group, self.me, self.ranks[ri].0);
        let c = self.counters.clone();
        c.bytes[ri].fetch_add(payload as u64, Ordering::Relaxed);
        self.sends.spawn(async move {
            let bytes = match buf {
                SlotBuf::Plain(v) => v,
                SlotBuf::Staged(slot) => {
                    match (&grant.target, push.vfs().ok().and_then(|v| v.fabric())) {
                        (Some(t), Some(f)) => {
                            // Tests: a write that lands but is taken as
                            // lost, and so is sent again by RPC.
                            let lost = push
                                .lose_acks
                                .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                                    n.checked_sub(1)
                                })
                                .is_ok();
                            let copy = lost.then(|| slot.as_ref().to_vec());
                            match f.push_write(to, slot, t).await {
                                Ok(()) if !lost => {
                                    push.rdma_slots.fetch_add(1, Ordering::Relaxed);
                                    c.rdma.fetch_add(1, Ordering::Relaxed);
                                    return Ok(());
                                }
                                Ok(()) => copy.expect("copied above"),
                                // Never landed: the slot is still ours to fill.
                                Err((e, bytes)) => {
                                    tracing::debug!(error = %e, "push write over RDMA failed; using RPC");
                                    if bytes.is_empty() {
                                        return Err(format!("sending a slot: {e}"));
                                    }
                                    bytes
                                }
                            }
                        }
                        _ => slot.as_ref().to_vec(),
                    }
                }
            };
            if to == me.host {
                // This host's own rank: handed over in memory.
                c.local.fetch_add(1, Ordering::Relaxed);
            } else {
                push.rpc_slots.fetch_add(1, Ordering::Relaxed);
                c.rpc.fetch_add(1, Ordering::Relaxed);
            }
            push.send_bulk(
                to,
                PushMsg::Slot {
                    group,
                    rank: r,
                    source: me,
                    slot: grant.slot,
                    seq: grant.seq,
                    bytes,
                },
            )
            .await
            .map_err(|e| format!("sending a slot: {e}"))
        });
        self.sent[ri] += 1;
        Ok(())
    }
}

/// RDMA landings, from the fabric's completion thread (a plain thread:
/// the runtime to hand work to is captured here).
pub(crate) struct Landings(std::sync::Weak<Push>, tokio::runtime::Handle);

impl nest_fabric::push::PushSink for Landings {
    fn landed(&self, token: u32, _len: u32) {
        let Some(p) = self.0.upgrade() else { return };
        let Some((group, rank)) = p.state.lock().tokens.get(&(token >> 12)).copied() else {
            tracing::debug!(token, "push landing for an unknown rank");
            return;
        };
        tracing::trace!(token, group, rank, "push landed");
        // Off the completion thread: checking takes locks and may send.
        self.1.spawn(async move {
            if let Err(e) = p.landed(group, rank, token & 0xfff) {
                tracing::warn!(error = %e, group, rank, "bad push slot");
            }
        });
    }
}

/// The `service::PUSH` handler.
struct PushService(std::sync::Weak<Push>);

impl nest_rpc::Handler for PushService {
    fn call(
        &self,
        peer: NodeId,
        body: bytes::Bytes,
    ) -> BoxFuture<'static, Result<bytes::Bytes, String>> {
        let p = self.0.clone();
        async move {
            let m: PushMsg = nest_rpc::decode(&body).map_err(|e| e.to_string())?;
            let Some(p) = p.upgrade() else {
                return Err("shutting down".to_string());
            };
            let r: Result<(), String> = p.on_msg(peer, m).await.map_err(|e| e.to_string());
            nest_rpc::encode(&r)
                .map(bytes::Bytes::from)
                .map_err(|e| e.to_string())
        }
        .boxed()
    }
}

impl nest_fabric::tcp::MessageSink for PushService {
    fn message(&self, peer: NodeId, data: Vec<u8>) -> BoxFuture<'static, NestResult<()>> {
        let p = self.0.clone();
        async move {
            let m: PushMsg = nest_rpc::decode(&data).map_err(|e| invalid(e.to_string()))?;
            let p = p
                .upgrade()
                .ok_or_else(|| NestError::Unavailable("shutting down".into()))?;
            p.on_msg(peer, m).await
        }
        .boxed()
    }
}

/// Listen for engines on `path` (`@name`: abstract) until the process
/// ends. Only this process's uid and root may connect (ADR-063 §8).
pub fn listen(push: Arc<Push>, path: &str) -> std::io::Result<()> {
    let l = nest_push::client::Listener::bind(path)?;
    let uid = nix::unistd::getuid().as_raw();
    let rt = tokio::runtime::Handle::current();
    std::thread::Builder::new()
        .name("push-listen".into())
        .spawn(move || {
            let _rt = rt.enter();
            loop {
                let conn = match l.accept() {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(error = %e, "push socket accept failed");
                        return;
                    }
                };
                match conn.peer() {
                    Ok((_, u, _)) if u == uid || u == 0 => push.serve_engine(conn),
                    Ok((pid, u, _)) => tracing::warn!(
                        pid,
                        uid = u,
                        "push socket: refused a connection from another user"
                    ),
                    Err(_) => {}
                }
            }
        })?;
    Ok(())
}

/// A lane error that says nothing about the message itself (the lane or
/// its peer went away), rather than the receiver's answer.
fn is_lane_failure(e: &NestError) -> bool {
    matches!(e, NestError::Io(m) if m.contains("outcome unknown"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files(n: usize, size: u64, holders: &[u64]) -> Vec<Resolved> {
        (0..n)
            .map(|i| Resolved {
                file: FileId(i as u64 + 1),
                generation: Generation(1),
                size,
                holders: holders.iter().map(|h| Src::host(NodeId(*h))).collect(),
            })
            .collect()
    }

    fn bytes(files: &[Resolved], to: &[Src]) -> HashMap<u64, u64> {
        let mut b = HashMap::new();
        for (f, s) in files.iter().zip(to) {
            *b.entry(s.store.0).or_default() += f.size;
        }
        b
    }

    #[test]
    fn without_rates_bytes_balance() {
        let f = files(12, 10, &[1, 2, 3]);
        let b = bytes(&f, &assign(&f, NodeId(1), &HashMap::new()));
        assert_eq!(b, HashMap::from([(1, 40), (2, 40), (3, 40)]));
    }

    #[test]
    fn faster_disks_carry_more_and_finish_together() {
        // Two disks twice as fast as the other four (rhea and moa against
        // the 5.6 GB/s Sparks).
        let rates: HashMap<StoreId, u64> = [(1, 6), (2, 6), (3, 6), (4, 6), (5, 12), (6, 12)]
            .into_iter()
            .map(|(n, r)| (StoreId(n), r))
            .collect();
        let f = files(80, 1, &[1, 2, 3, 4, 5, 6]);
        let b = bytes(&f, &assign(&f, NodeId(1), &rates));
        for n in 1..=4 {
            assert_eq!(b[&n], 10, "{b:?}");
        }
        assert_eq!(b[&5], 20, "{b:?}");
        assert_eq!(b[&6], 20, "{b:?}");
    }

    #[test]
    fn unknown_rates_count_at_the_median_and_own_copy_wins_ties() {
        let rates = HashMap::from([(StoreId(2), 10), (StoreId(3), 0)]);
        let f = files(1, 5, &[2, 1, 3]);
        assert_eq!(assign(&f, NodeId(1), &rates), vec![Src::host(NodeId(1))]);
    }

    #[test]
    fn a_source_slow_to_fill_its_grants_gets_no_more_until_others_catch_up() {
        let [slow, a, b] = [1, 2, 3].map(|n| Src::host(NodeId(n)));
        let sources = [slow, a, b];
        let mut held = HashMap::from([(slow, 0), (a, 0), (b, 0)]);
        // Grant 30 while only the fast sources fill theirs: the slow one
        // stops at an even share, however long it holds them.
        for k in 0..30 {
            let s = fewest(&sources, &held, k);
            *held.get_mut(&s).unwrap() += 1;
            if s != slow {
                *held.get_mut(&s).unwrap() -= 1;
            }
        }
        assert_eq!(held[&slow], 1, "{held:?}");
        // Equal sources take turns.
        let held = HashMap::new();
        let turns: Vec<Src> = (0..3).map(|k| fewest(&sources, &held, k)).collect();
        assert_eq!(turns, sources);
    }

    #[test]
    fn an_archive_is_a_stream_of_its_own_beside_its_gateways_store() {
        // Host 1 holds half the files on its own store; the other half are
        // only in an archive host 1 is the gateway of: two streams.
        let archive = Src {
            store: StoreId(crate::local::ARCHIVE_STORE_BASE),
            host: NodeId(1),
        };
        let mut f = files(4, 10, &[1]);
        for r in &mut f[2..] {
            r.holders = vec![archive];
        }
        let to = assign(&f, NodeId(1), &HashMap::from([(StoreId(1), 10)]));
        assert_eq!(to[..2], [Src::host(NodeId(1)); 2]);
        assert_eq!(to[2..], [archive; 2]);
    }

    #[test]
    fn a_hosts_disks_are_streams_of_their_own_at_their_own_rates() {
        // Host 1 has its live store and two disks, each twice a Spark's
        // speed; hosts 2 and 3 one disk each. Every file is everywhere.
        let d = |k| Src {
            store: NodeId(1).disk_store(k),
            host: NodeId(1),
        };
        let mut f = files(70, 1, &[1, 2, 3]);
        for r in &mut f {
            r.holders.extend([d(1), d(2)]);
        }
        let rates = HashMap::from([
            (StoreId(1), 10),
            (NodeId(1).disk_store(1), 20),
            (NodeId(1).disk_store(2), 20),
            (StoreId(2), 10),
            (StoreId(3), 10),
        ]);
        let b = bytes(&f, &assign(&f, NodeId(2), &rates));
        assert_eq!(b[&1], 10, "{b:?}");
        assert_eq!(b[&NodeId(1).disk_store(1).0], 20, "{b:?}");
        assert_eq!(b[&NodeId(1).disk_store(2).0], 20, "{b:?}");
        assert_eq!(b[&2], 10, "{b:?}");
        assert_eq!(b[&3], 10, "{b:?}");
    }
}
