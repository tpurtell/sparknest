//! The fabric engine: per-device resources, per-peer links, the read
//! protocol, and completion handling.
//!
//! Read protocol (all on RC queue pairs; TCP only sets links up):
//! ```text
//! client                                            server
//!   take bytes from the in-flight budget
//!   acquire landing slot (smallest tier that fits; remote-writable)
//!   SEND ReadReq{slot, file, gen, off, len, addr, rkey}  ->
//!                                        the object: open_now (cached) or open
//!                                        (checked off-thread, once in a while)
//!                                        queue until a staging slot is free
//!                                        io_uring read into it, on this thread
//!                 <- RDMA WRITE_WITH_IMM(staging -> addr), imm = slot id
//!                    (the length is the completion's byte count)
//!                 <- or SEND ReadErr{slot, code}
//!   complete the waiter for (device, slot)
//! ```
//! Each lane's receive ring absorbs requests, error replies and
//! write-with-imm notifications; a per-lane window keeps a node's requests
//! plus its responses within the peer's ring. Links are made only between
//! hosts speaking the same `PROTOCOL` (ADR-032); otherwise reads use TCP.

use crate::pool::{Slot, Tiers};
use crate::rail::Rail;
use crate::sys::{self, nf_wc};
use crate::verbs::{Context, Cq, Qp, QpInfo, Region};
use futures::future::BoxFuture;
use nest_types::{FileId, Generation, NestError, NodeId};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{Semaphore, oneshot};

#[derive(Clone, Debug)]
pub struct FabricConfig {
    /// Largest single read; also the slot size of the largest tier.
    pub chunk: usize,
    /// Landing slots per device (registered for remote write) of `chunk`.
    pub client_slots: u32,
    /// Staging slots per device of `chunk`.
    pub server_slots: u32,
    /// Smaller slot tiers for small reads (page faults, lookup rows, kernel
    /// requests), below `chunk`.
    pub small_tiers: Vec<Tier>,
    /// Outstanding requests per lane (per direction).
    pub window: u32,
    /// Bytes of remote reads this host has outstanding at once, over all
    /// peers: the incast cap (0: 2 ms of this host's links, at least
    /// 64 MiB). Many hosts answering one reader at the same moment overflow
    /// switch buffers, and every dropped packet then costs a retransmit
    /// timeout (the fabric is lossy without PFC). Counted in bytes: a page
    /// read costs a thousandth of a chunk.
    pub inflight_bytes: u64,
    /// Optional device/netdev/address filter.
    pub devices: Vec<String>,
}

/// A slot tier: its size, and landing and staging slots per device.
#[derive(Clone, Copy, Debug)]
pub struct Tier {
    pub size: usize,
    pub landing: u32,
    pub staging: u32,
}

impl Default for FabricConfig {
    fn default() -> Self {
        FabricConfig {
            chunk: 4 << 20,
            client_slots: 128,
            server_slots: 64,
            small_tiers: vec![
                Tier {
                    size: 4 << 10,
                    landing: 2048,
                    staging: 1024,
                },
                Tier {
                    size: 128 << 10,
                    landing: 512,
                    staging: 256,
                },
            ],
            window: 128,
            inflight_bytes: 0,
            devices: Vec::new(),
        }
    }
}

/// The read protocol links speak; hosts pair only with the same one.
/// 2: imm carries the whole slot id, the length is the completion's count.
const PROTOCOL: u32 = 2;

/// An object opened for serving, and what must stay held while a read of
/// it is in flight (the data layer's in-flight count, which fencing and
/// eviction wait on).
pub struct ServeFile {
    pub file: Arc<std::fs::File>,
    pub guard: Box<dyn Send>,
}

/// Supplies the objects this node serves. Implemented by the data service
/// so generation fencing and serving rules stay in one place; the fabric
/// reads them itself, through io_uring on its completion thread.
pub trait ReadSource: Send + Sync + 'static {
    /// The object holding `generation` of `file` if it is already known to
    /// be servable: no queries, no I/O (runs on the completion thread).
    /// `None`: check it with `open`.
    fn open_now(
        &self,
        file: FileId,
        generation: Generation,
    ) -> Option<Result<ServeFile, NestError>>;

    /// Check that this host may serve `generation` of `file`, and open it.
    fn open(
        &self,
        file: FileId,
        generation: Generation,
    ) -> BoxFuture<'static, Result<ServeFile, NestError>>;
}

/// A completed read: the bytes live in a landing slot until dropped.
pub struct ReadBuf {
    slot: Slot,
    len: usize,
}

impl ReadBuf {
    pub fn as_slice(&self) -> &[u8] {
        self.slot.as_slice(self.len)
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

// ---------------------------------------------------------------- wire

const MSG_SIZE: usize = 64;
const RING_SLOT: usize = 128;
const KIND_READ: u8 = 1;
const KIND_ERR: u8 = 2;

/// The largest read a `Reservation` makes (a FUSE request is at most
/// 128 KiB); larger reads use `read`.
const RESERVE_MAX: usize = 128 << 10;
/// Serving: reads in flight in one device's io_uring.
const SERVE_DEPTH: usize = 256;
/// A read the reader still waits for after this is failed (and its lane
/// retired), as `read` does.
const READ_TIMEOUT: Duration = Duration::from_secs(30);
/// Unit of the in-flight byte budget.
const BUDGET_UNIT: usize = 4 << 10;

fn err_code(e: &NestError) -> u8 {
    match e {
        NestError::Stale => 1,
        NestError::Unavailable(_) => 2,
        NestError::NotFound => 3,
        _ => 4,
    }
}

fn err_from(code: u8) -> NestError {
    match code {
        1 => NestError::Stale,
        2 => NestError::Unavailable("peer cannot serve that range".into()),
        3 => NestError::NotFound,
        _ => NestError::Io("remote read failed".into()),
    }
}

#[derive(Clone, Copy, Debug)]
struct ReadReq {
    slot: u32,
    file: u64,
    generation: u64,
    offset: u64,
    len: u32,
    addr: u64,
    rkey: u32,
}

impl ReadReq {
    fn encode(&self, out: &mut [u8; MSG_SIZE]) {
        out[0] = KIND_READ;
        out[4..8].copy_from_slice(&self.slot.to_le_bytes());
        out[8..16].copy_from_slice(&self.file.to_le_bytes());
        out[16..24].copy_from_slice(&self.generation.to_le_bytes());
        out[24..32].copy_from_slice(&self.offset.to_le_bytes());
        out[32..36].copy_from_slice(&self.len.to_le_bytes());
        out[36..40].copy_from_slice(&self.rkey.to_le_bytes());
        out[40..48].copy_from_slice(&self.addr.to_le_bytes());
    }
    fn decode(b: &[u8]) -> ReadReq {
        let u32_at = |i: usize| u32::from_le_bytes(b[i..i + 4].try_into().unwrap());
        let u64_at = |i: usize| u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        ReadReq {
            slot: u32_at(4),
            file: u64_at(8),
            generation: u64_at(16),
            offset: u64_at(24),
            len: u32_at(32),
            rkey: u32_at(36),
            addr: u64_at(40),
        }
    }
}

// ---------------------------------------------------------------- wr ids

const WR_RING: u64 = 1;
const WR_SEND: u64 = 2;
const WR_WRITE: u64 = 3;

fn wr(kind: u64, lane: u32, idx: u32) -> u64 {
    (kind << 56) | ((lane as u64) << 32) | idx as u64
}

fn wr_parts(id: u64) -> (u64, u32, u32) {
    (id >> 56, ((id >> 32) & 0xff_ffff) as u32, id as u32)
}

// ---------------------------------------------------------------- devices

struct Device {
    id: u32,
    ctx: Arc<Context>,
    cq: Arc<Cq>,
    landing: Tiers,
    staging: Tiers,
    mtu: u32,
    /// Serving: the io_uring reads go through, and requests waiting for a
    /// slot or ring room. Only the device's poller thread submits.
    serve: Mutex<ServeRing>,
    /// Requests whose object was checked off-thread, back for the poller.
    resolved: Mutex<Vec<Resolved>>,
    /// Wakes the poller: io_uring completions (registered) and `resolved`.
    wake: std::fs::File,
}

impl Device {
    fn wake(&self) {
        use std::io::Write;
        let _ = (&self.wake).write(&1u64.to_ne_bytes());
    }

    fn clear_wake(&self) {
        use std::io::Read;
        let mut b = [0u8; 8];
        let _ = (&self.wake).read(&mut b);
    }
}

struct Resolved {
    lane: Arc<Lane>,
    req: ReadReq,
    file: Result<ServeFile, NestError>,
    at: std::time::Instant,
}

struct Waiting {
    lane: Arc<Lane>,
    req: ReadReq,
    file: ServeFile,
    at: std::time::Instant,
}

/// A served read in the ring.
struct ServeOp {
    lane: Arc<Lane>,
    req: ReadReq,
    slot: Slot,
    file: ServeFile,
    done: usize,
    want: usize,
    at: std::time::Instant,
    submitted: std::time::Instant,
}

struct ServeRing {
    ring: io_uring::IoUring,
    ops: Vec<Option<ServeOp>>,
    free: Vec<usize>,
    waiting: std::collections::VecDeque<Waiting>,
}

impl ServeRing {
    fn new(wake: &std::fs::File) -> std::io::Result<ServeRing> {
        use std::os::fd::AsRawFd;
        let ring = io_uring::IoUring::new(SERVE_DEPTH as u32)?;
        ring.submitter().register_eventfd(wake.as_raw_fd())?;
        Ok(ServeRing {
            ring,
            ops: (0..SERVE_DEPTH).map(|_| None).collect(),
            free: (0..SERVE_DEPTH).rev().collect(),
            waiting: Default::default(),
        })
    }

    /// Queue the (rest of the) read of op `idx`. Returns false if the
    /// submission queue is full (it is sized for every op, so never).
    fn push(&mut self, idx: usize) -> bool {
        use std::os::fd::AsRawFd;
        let Some(op) = self.ops[idx].as_ref() else {
            return false;
        };
        let e = io_uring::opcode::Read::new(
            io_uring::types::Fd(op.file.file.as_raw_fd()),
            op.slot.ptr().wrapping_add(op.done),
            (op.want - op.done) as u32,
        )
        .offset(op.req.offset + op.done as u64)
        .build()
        .user_data(idx as u64);
        // SAFETY: the buffer is op's staging slot and the fd its open
        // object, both owned by `ops[idx]` until this read's completion is
        // reaped (and leaked with the ring if it never is, see Drop).
        unsafe { self.ring.submission().push(&e) }.is_ok()
    }
}

impl Drop for ServeRing {
    fn drop(&mut self) {
        // Reads still in the kernel may write into their slots: never
        // return those to the pool.
        for op in self.ops.drain(..).flatten() {
            std::mem::forget(op.slot);
        }
    }
}

/// One queue pair to one peer on one rail pair.
struct Lane {
    id: u32,
    dev: Arc<Device>,
    qp: Qp,
    local: Rail,
    ring: Region,
    /// Our outstanding requests on this lane.
    window: Arc<Semaphore>,
    peer: NodeId,
    dead: AtomicBool,
}

/// A read in flight. The pending table owns its landing slot until the
/// peer answers: a reader that gives up (its future dropped, e.g. readahead
/// cancelling a chunk) must not return the slot to the pool while the peer
/// may still write into it, or the next request given that slot would take
/// this request's answer (its bytes and length) as its own.
struct Waiter {
    lane: u32,
    slot: Slot,
    tx: Notify,
    since: std::time::Instant,
}

type Answer = Result<(usize, Slot), NestError>;

/// How the reader learns the answer: a task waiting (`read`), or a
/// callback run on the completion thread (`Reservation::send`: no thread
/// waits at all).
enum Notify {
    Task(oneshot::Sender<Answer>),
    Call(Box<dyn FnOnce(Answer) + Send>),
}

impl Notify {
    fn send(self, a: Answer) {
        match self {
            Notify::Task(tx) => {
                let _ = tx.send(a);
            }
            Notify::Call(f) => f(a),
        }
    }
}

/// What a `Reservation`'s reader is handed: the bytes, or why not.
pub type ReadDone = Box<dyn FnOnce(Result<&[u8], NestError>) + Send>;

/// Everything a small read needs, taken without waiting: a link lane,
/// budget, a landing slot and window room. `send` it, or drop it to give
/// them back.
pub struct Reservation {
    fabric: Arc<Fabric>,
    lane: Arc<Lane>,
    slot: Slot,
    budget: tokio::sync::OwnedSemaphorePermit,
    window: tokio::sync::OwnedSemaphorePermit,
    len: usize,
}

impl Waiter {
    /// The peer wrote `len` bytes: hand the slot to the reader, or free it
    /// if the reader has gone.
    fn complete(self, len: usize) {
        self.tx.send(Ok((len, self.slot)));
    }

    /// The peer will not write: the slot is free again.
    fn fail(self, e: NestError) {
        self.tx.send(Err(e));
    }
}

struct Link {
    lanes: Vec<Arc<Lane>>,
    next: AtomicU32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LaneOffer {
    rail: Rail,
    qp: QpInfo,
}

#[derive(Debug, Serialize, Deserialize)]
enum FabricReq {
    /// Here are my rails with a fresh QP on each; pair by subnet. (Protocol
    /// 1: refused now.)
    Connect { lanes: Vec<LaneOffer> },
    /// The same, speaking read protocol `protocol`.
    ConnectV {
        lanes: Vec<LaneOffer>,
        protocol: u32,
    },
}

#[derive(Debug, Serialize, Deserialize)]
enum FabricResp {
    /// For each paired offer (by index): the peer's QP.
    Accepted {
        pairs: Vec<(u32, QpInfo)>,
    },
    Refused(String),
}

pub struct Fabric {
    cfg: FabricConfig,
    rails: Vec<Rail>,
    devices: Vec<Arc<Device>>,
    rpc: nest_rpc::Rpc,
    lanes: Mutex<HashMap<u32, Arc<Lane>>>,
    next_lane: AtomicU32,
    links: Mutex<HashMap<NodeId, Arc<Link>>>,
    connecting: Mutex<HashMap<NodeId, Arc<tokio::sync::Mutex<()>>>>,
    /// Waiters by (device, landing slot), with the lane the request used.
    pending: Mutex<HashMap<(u32, u32), Waiter>>,
    /// Staging slots held until their write-with-imm completes.
    in_flight: Mutex<HashMap<u64, Slot>>,
    source: Mutex<Option<Weak<dyn ReadSource>>>,
    /// Where read latencies are recorded (set by the host's data layer).
    io: std::sync::OnceLock<Arc<crate::iostats::IoStats>>,
    rt: tokio::runtime::Handle,
    stop: Arc<AtomicBool>,
    /// The incast cap, in `BUDGET_UNIT`s of bytes.
    inflight: Arc<Semaphore>,
    /// This host's links, bytes per second.
    link_bps: u64,
    pub stats: Stats,
}

#[derive(Default)]
pub struct Stats {
    pub reads: AtomicU64,
    pub read_bytes: AtomicU64,
    pub served: AtomicU64,
    pub served_bytes: AtomicU64,
    pub errors: AtomicU64,
    /// Cumulative nanoseconds, for where a remote read's time goes: waiting
    /// for a landing slot, for lane window room, and request to answer.
    pub read_slot_wait_ns: AtomicU64,
    pub read_window_wait_ns: AtomicU64,
    pub read_rtt_ns: AtomicU64,
    /// Serving: waiting for a staging slot, and reading the bytes.
    pub serve_slot_wait_ns: AtomicU64,
    pub serve_read_ns: AtomicU64,
    /// Reads served through the io_uring path (all of them now).
    pub served_fast: AtomicU64,
    /// Reads made through a `Reservation` (answered by callback).
    pub read_now: AtomicU64,
}

impl Fabric {
    /// Open every discovered rail's device and start its poller. Returns
    /// `None` when this node has no usable RoCE rail.
    pub fn start(
        me: NodeId,
        cfg: FabricConfig,
        rpc: nest_rpc::Rpc,
    ) -> std::io::Result<Option<Arc<Fabric>>> {
        let rails = crate::rail::discover(&cfg.devices);
        if rails.is_empty() {
            return Ok(None);
        }
        let mut devices: Vec<Arc<Device>> = Vec::new();
        for r in &rails {
            if devices.iter().any(|d| d.ctx.name == r.ibdev) {
                continue;
            }
            let ctx = Context::open(&r.ibdev)?;
            let (_, mtu) = ctx.port(r.port)?;
            // Every lane's sends and receives complete here: sized for a
            // few dozen peers at the full window.
            let cq = Cq::new(&ctx, 65536, true)?;
            let small = cfg.small_tiers.iter().filter(|t| t.size < cfg.chunk);
            let landing = Tiers::new(
                &ctx,
                &small
                    .clone()
                    .map(|t| (t.size, t.landing))
                    .chain([(cfg.chunk, cfg.client_slots)])
                    .collect::<Vec<_>>(),
                true,
            )?;
            let staging = Tiers::new(
                &ctx,
                &small
                    .map(|t| (t.size, t.staging))
                    .chain([(cfg.chunk, cfg.server_slots)])
                    .collect::<Vec<_>>(),
                false,
            )?;
            // SAFETY: eventfd returns a new descriptor or -1.
            let efd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
            if efd < 0 {
                return Err(std::io::Error::last_os_error());
            }
            // SAFETY: a fresh descriptor, owned from here on.
            let wake = std::fs::File::from(unsafe {
                <std::os::fd::OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(efd)
            });
            let serve = Mutex::new(ServeRing::new(&wake)?);
            devices.push(Arc::new(Device {
                id: devices.len() as u32,
                ctx,
                cq,
                landing,
                staging,
                mtu,
                serve,
                resolved: Mutex::new(Vec::new()),
                wake,
            }));
        }
        let _ = me;
        let link = link_bytes_per_s(&rails);
        let budget = match cfg.inflight_bytes {
            0 => (link / 500).max(64 << 20),
            b => b.max(cfg.chunk as u64),
        };
        tracing::info!(
            link_gbit = link * 8 / 1_000_000_000,
            inflight_mib = budget >> 20,
            "fabric read budget"
        );
        let fabric = Arc::new(Fabric {
            cfg,
            rails,
            devices,
            rpc: rpc.clone(),
            lanes: Mutex::new(HashMap::new()),
            next_lane: AtomicU32::new(1),
            links: Mutex::new(HashMap::new()),
            connecting: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
            source: Mutex::new(None),
            rt: tokio::runtime::Handle::current(),
            stop: Arc::new(AtomicBool::new(false)),
            inflight: Arc::new(Semaphore::new((budget / BUDGET_UNIT as u64) as usize)),
            link_bps: link,
            stats: Stats::default(),
            io: std::sync::OnceLock::new(),
        });
        for d in &fabric.devices {
            let (weak, dev, stop) = (Arc::downgrade(&fabric), d.clone(), fabric.stop.clone());
            std::thread::Builder::new()
                .name(format!("nf-poll-{}", d.ctx.name))
                .spawn(move || poller(weak, dev, stop))?;
        }
        {
            let weak = Arc::downgrade(&fabric);
            fabric.rt.spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let Some(f) = weak.upgrade() else { return };
                    f.expire();
                }
            });
        }
        let weak = Arc::downgrade(&fabric);
        rpc.register(
            nest_rpc::service::FABRIC,
            Arc::new(move |peer: NodeId, body: bytes::Bytes| {
                let weak = weak.clone();
                Box::pin(async move {
                    let f = weak.upgrade().ok_or("fabric stopped")?;
                    let req: FabricReq = nest_rpc::decode(&body).map_err(|e| e.to_string())?;
                    let resp = f.accept(peer, req);
                    nest_rpc::encode(&resp)
                        .map(bytes::Bytes::from)
                        .map_err(|e| e.to_string())
                }) as BoxFuture<'static, Result<bytes::Bytes, String>>
            }),
        );
        tracing::info!(rails = ?fabric.rails.iter().map(|r| format!("{}/{}", r.ibdev, r.addr)).collect::<Vec<_>>(), "RDMA fabric up");
        Ok(Some(fabric))
    }

    pub fn rails(&self) -> &[Rail] {
        &self.rails
    }

    pub fn set_source(&self, source: Weak<dyn ReadSource>) {
        *self.source.lock() = Some(source);
    }

    /// Record read latencies into `io` (reads made and served).
    pub fn set_io_stats(&self, io: Arc<crate::iostats::IoStats>) {
        let _ = self.io.set(io);
    }

    fn record(&self, kind: crate::iostats::Kind, bytes: usize, took: Duration) {
        if let Some(io) = self.io.get() {
            io.record(kind, bytes as u64, took);
        }
    }

    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::SeqCst);
        for l in self.lanes.lock().values() {
            l.qp.set_error();
        }
    }

    pub fn chunk(&self) -> usize {
        self.cfg.chunk
    }

    /// This host's RDMA links, bytes per second (the rails' ports summed).
    pub fn link_bytes_per_s(&self) -> u64 {
        self.link_bps
    }

    /// Chunk-sized landing slots free on the tightest device: speculative
    /// readahead only uses spare slots so demanded reads always find one.
    pub fn spare_landing(&self) -> usize {
        self.devices
            .iter()
            .map(|d| d.landing.big().available())
            .min()
            .unwrap_or(0)
    }

    /// Chunk-sized landing slots per device.
    pub fn landing_slots(&self) -> usize {
        self.cfg.client_slots as usize
    }

    /// Whether a working link to `peer` exists or can be tried.
    pub fn has_link(&self, peer: NodeId) -> bool {
        self.links
            .lock()
            .get(&peer)
            .is_some_and(|l| l.lanes.iter().all(|x| !x.dead.load(Ordering::SeqCst)))
    }

    // ------------------------------------------------------------ lanes

    fn new_lane(&self, rail: &Rail, peer: NodeId) -> std::io::Result<Arc<Lane>> {
        let dev = self
            .devices
            .iter()
            .find(|d| d.ctx.name == rail.ibdev)
            .expect("rail device opened")
            .clone();
        let depth = self.cfg.window * 2 + 8;
        let qp = Qp::new(&dev.ctx, &dev.cq, &dev.cq, depth * 2, depth, rail.port)?;
        let ring = Region::new(&dev.ctx, depth as usize * RING_SLOT, false)?;
        let id = self.next_lane.fetch_add(1, Ordering::Relaxed);
        let lane = Arc::new(Lane {
            id,
            dev,
            qp,
            local: rail.clone(),
            ring,
            window: Arc::new(Semaphore::new(self.cfg.window as usize)),
            peer,
            dead: AtomicBool::new(false),
        });
        for i in 0..depth {
            self.post_ring(&lane, i)?;
        }
        self.lanes.lock().insert(id, lane.clone());
        Ok(lane)
    }

    fn post_ring(&self, lane: &Lane, i: u32) -> std::io::Result<()> {
        // SAFETY: ring slot i lies in the lane's registered ring and is not
        // in use until this receive completes.
        unsafe {
            lane.qp.post_recv(
                wr(WR_RING, lane.id, i),
                lane.ring.ptr_at(i as usize * RING_SLOT),
                RING_SLOT as u32,
                lane.ring.lkey(),
            )
        }
    }

    fn qp_info(&self, lane: &Lane, psn: u32) -> QpInfo {
        QpInfo {
            qpn: lane.qp.num(),
            psn,
            gid: lane.local.gid,
            mtu: lane.dev.mtu,
        }
    }

    /// Server side of link setup: pair offered rails with ours by subnet.
    fn accept(&self, peer: NodeId, req: FabricReq) -> FabricResp {
        let lanes = match req {
            FabricReq::ConnectV { lanes, protocol } if protocol == PROTOCOL => lanes,
            FabricReq::ConnectV { protocol, .. } => {
                return FabricResp::Refused(format!(
                    "read protocol {protocol} (this host speaks {PROTOCOL})"
                ));
            }
            FabricReq::Connect { .. } => {
                return FabricResp::Refused(format!(
                    "read protocol 1 (this host speaks {PROTOCOL})"
                ));
            }
        };
        let mut pairs = Vec::new();
        let mut made = Vec::new();
        for (i, offer) in lanes.iter().enumerate() {
            let Some(rail) = self.rails.iter().find(|r| r.same_subnet(&offer.rail)) else {
                continue;
            };
            if made.iter().any(|(r, _): &(Rail, Arc<Lane>)| r == rail) {
                continue;
            }
            let lane = match self.new_lane(rail, peer) {
                Ok(l) => l,
                Err(e) => return FabricResp::Refused(e.to_string()),
            };
            let psn = rand::random::<u32>() & 0xff_ffff;
            let mtu = lane.dev.mtu.min(offer.qp.mtu);
            if let Err(e) = lane.qp.connect(rail.gid_index, psn, &offer.qp, mtu) {
                return FabricResp::Refused(e.to_string());
            }
            pairs.push((i as u32, self.qp_info(&lane, psn)));
            made.push((rail.clone(), lane));
        }
        if made.is_empty() {
            return FabricResp::Refused("no rail shares a subnet".into());
        }
        let link = Arc::new(Link {
            lanes: made.into_iter().map(|(_, l)| l).collect(),
            next: AtomicU32::new(0),
        });
        if let Some(old) = self.links.lock().insert(peer, link) {
            self.retire(&old);
        }
        tracing::info!(%peer, lanes = pairs.len(), "RDMA link accepted");
        FabricResp::Accepted { pairs }
    }

    fn retire(&self, link: &Link) {
        for l in &link.lanes {
            l.dead.store(true, Ordering::SeqCst);
            l.qp.set_error();
        }
    }

    async fn link(&self, peer: NodeId) -> Result<Arc<Link>, NestError> {
        if let Some(l) = self.links.lock().get(&peer).cloned()
            && l.lanes.iter().all(|x| !x.dead.load(Ordering::SeqCst))
        {
            return Ok(l);
        }
        let gate = self.connecting.lock().entry(peer).or_default().clone();
        let _g = gate.lock().await;
        if let Some(l) = self.links.lock().get(&peer).cloned()
            && l.lanes.iter().all(|x| !x.dead.load(Ordering::SeqCst))
        {
            return Ok(l);
        }
        let mut offers = Vec::new();
        let mut lanes = Vec::new();
        let mut psns = Vec::new();
        for r in &self.rails {
            let lane = self
                .new_lane(r, peer)
                .map_err(|e| NestError::Io(e.to_string()))?;
            let psn = rand::random::<u32>() & 0xff_ffff;
            offers.push(LaneOffer {
                rail: r.clone(),
                qp: self.qp_info(&lane, psn),
            });
            lanes.push(lane);
            psns.push(psn);
        }
        let body = nest_rpc::encode(&FabricReq::ConnectV {
            lanes: offers,
            protocol: PROTOCOL,
        })
        .map_err(|e| NestError::Io(e.to_string()))?;
        let resp = self
            .rpc
            .call(
                peer,
                nest_rpc::service::FABRIC,
                body.into(),
                Duration::from_secs(5),
            )
            .await
            .map_err(|e| NestError::Unavailable(e.to_string()))?;
        let resp: FabricResp = nest_rpc::decode(&resp).map_err(|e| NestError::Io(e.to_string()))?;
        let pairs = match resp {
            FabricResp::Accepted { pairs } => pairs,
            FabricResp::Refused(why) => {
                for l in &lanes {
                    self.lanes.lock().remove(&l.id);
                }
                return Err(NestError::Unavailable(format!(
                    "peer refused RDMA link: {why}"
                )));
            }
        };
        let mut used = Vec::new();
        for (i, remote) in pairs {
            let lane = lanes[i as usize].clone();
            let mtu = lane.dev.mtu.min(remote.mtu);
            lane.qp
                .connect(lane.local.gid_index, psns[i as usize], &remote, mtu)
                .map_err(|e| NestError::Io(e.to_string()))?;
            used.push(lane);
        }
        for l in &lanes {
            if !used.iter().any(|u| u.id == l.id) {
                self.lanes.lock().remove(&l.id);
            }
        }
        tracing::info!(%peer, lanes = used.len(), "RDMA link established");
        let link = Arc::new(Link {
            lanes: used,
            next: AtomicU32::new(0),
        });
        if let Some(old) = self.links.lock().insert(peer, link.clone()) {
            self.retire(&old);
        }
        Ok(link)
    }

    // ------------------------------------------------------------ client

    /// Read `len` bytes (≤ chunk) of exactly `generation` from `peer`.
    pub async fn read(
        &self,
        peer: NodeId,
        file: FileId,
        generation: Generation,
        offset: u64,
        len: usize,
    ) -> Result<ReadBuf, NestError> {
        assert!(len <= self.cfg.chunk);
        let link = self.link(peer).await?;
        let lane = link.lanes
            [link.next.fetch_add(1, Ordering::Relaxed) as usize % link.lanes.len()]
        .clone();
        let t0 = std::time::Instant::now();
        // The incast cap, in bytes: held until the answer (or failure).
        let _cap = self
            .inflight
            .acquire_many(len.div_ceil(BUDGET_UNIT).max(1) as u32)
            .await
            .map_err(|_| NestError::Unavailable("fabric stopped".into()))?;
        let slot = lane.dev.landing.acquire(len).await;
        let t1 = std::time::Instant::now();
        let _permit = lane
            .window
            .acquire()
            .await
            .map_err(|_| NestError::Unavailable("lane closed".into()))?;
        let t2 = std::time::Instant::now();
        self.stats
            .read_slot_wait_ns
            .fetch_add(t1.duration_since(t0).as_nanos() as u64, Ordering::Relaxed);
        self.stats
            .read_window_wait_ns
            .fetch_add(t2.duration_since(t1).as_nanos() as u64, Ordering::Relaxed);
        if lane.dead.load(Ordering::SeqCst) {
            return Err(NestError::Unavailable("RDMA link failed".into()));
        }
        let (tx, rx) = oneshot::channel();
        let key = (lane.dev.id, slot.id());
        let mut msg = [0u8; MSG_SIZE];
        ReadReq {
            slot: slot.id(),
            file: file.0,
            generation: generation.0,
            offset,
            len: len as u32,
            addr: slot.addr(),
            rkey: slot.rkey(),
        }
        .encode(&mut msg);
        // From here the slot belongs to the pending table until the answer.
        self.pending.lock().insert(
            key,
            Waiter {
                lane: lane.id,
                slot,
                tx: Notify::Task(tx),
                since: std::time::Instant::now(),
            },
        );
        // SAFETY: inline send copies the message at post time.
        let posted = unsafe {
            lane.qp.post_send(
                wr(WR_SEND, lane.id, 0),
                msg.as_mut_ptr(),
                MSG_SIZE as u32,
                0,
                true,
            )
        };
        if let Err(e) = posted {
            // Never sent: nothing will write the slot.
            drop(self.pending.lock().remove(&key));
            self.fail_lane(&lane);
            return Err(NestError::Unavailable(e.to_string()));
        }
        let r = tokio::time::timeout(Duration::from_secs(30), rx).await;
        self.stats
            .read_rtt_ns
            .fetch_add(t2.elapsed().as_nanos() as u64, Ordering::Relaxed);
        let r = match r {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err(NestError::Unavailable("RDMA link failed".into())),
            Err(_) => {
                // The slot may still be written later: never reuse it.
                // Retiring the link fences the NIC.
                if let Some(w) = self.pending.lock().remove(&key) {
                    std::mem::forget(w.slot);
                }
                self.fail_lane(&lane);
                return Err(NestError::Unavailable("RDMA read timed out".into()));
            }
        };
        match r {
            Ok((n, slot)) => {
                self.stats.reads.fetch_add(1, Ordering::Relaxed);
                self.stats.read_bytes.fetch_add(n as u64, Ordering::Relaxed);
                self.record(crate::iostats::Kind::Fabric, len, t0.elapsed());
                Ok(ReadBuf { slot, len: n })
            }
            Err(e) => Err(e),
        }
    }

    /// Take what a small read needs without waiting (see `Reservation`);
    /// `None` when any of it is not free right now, or no link is up yet:
    /// use `read`.
    pub fn reserve(self: &Arc<Self>, peer: NodeId, len: usize) -> Option<Reservation> {
        if len > RESERVE_MAX {
            return None;
        }
        let link = self
            .links
            .lock()
            .get(&peer)
            .cloned()
            .filter(|l| l.lanes.iter().all(|x| !x.dead.load(Ordering::SeqCst)))?;
        let lane = link.lanes
            [link.next.fetch_add(1, Ordering::Relaxed) as usize % link.lanes.len()]
        .clone();
        let budget = self
            .inflight
            .clone()
            .try_acquire_many_owned(len.div_ceil(BUDGET_UNIT).max(1) as u32)
            .ok()?;
        let slot = lane.dev.landing.try_acquire(len)?;
        let window = lane.window.clone().try_acquire_owned().ok()?;
        Some(Reservation {
            fabric: self.clone(),
            lane,
            slot,
            budget,
            window,
            len,
        })
    }

    /// Fail reads made with `Reservation::send` that have waited past
    /// `READ_TIMEOUT` (their lane is retired, as `read` does on a timeout).
    fn expire(&self) {
        let now = std::time::Instant::now();
        let old: Vec<(u32, u32)> = self
            .pending
            .lock()
            .iter()
            .filter(|(_, w)| {
                matches!(w.tx, Notify::Call(_)) && now.duration_since(w.since) > READ_TIMEOUT
            })
            .map(|(k, _)| *k)
            .collect();
        for k in old {
            let w = self.pending.lock().remove(&k);
            if let Some(w) = w {
                let lane = self.lanes.lock().get(&w.lane).cloned();
                let Waiter { slot, tx, .. } = w;
                std::mem::forget(slot);
                if let Some(l) = lane {
                    self.fail_lane(&l);
                }
                tx.send(Err(NestError::Unavailable("RDMA read timed out".into())));
            }
        }
    }

    /// Take a lane out of service: the QP enters the error state (so the
    /// NIC stops touching its buffers) and requests sent on it fail.
    fn fail_lane(&self, lane: &Lane) {
        if !lane.dead.swap(true, Ordering::SeqCst) {
            tracing::warn!(peer = %lane.peer, rail = %lane.local.addr, "RDMA lane failed");
            self.stats.errors.fetch_add(1, Ordering::Relaxed);
            lane.qp.set_error();
            let failed: Vec<Waiter> = {
                let mut p = self.pending.lock();
                let keys: Vec<_> = p
                    .iter()
                    .filter(|(_, w)| w.lane == lane.id)
                    .map(|(k, _)| *k)
                    .collect();
                keys.into_iter().filter_map(|k| p.remove(&k)).collect()
            };
            // Answered outside the lock: a callback may reply to FUSE.
            for w in failed {
                // As on a timeout: a write may still land in the slot.
                let Waiter { slot, tx, .. } = w;
                std::mem::forget(slot);
                tx.send(Err(NestError::Unavailable("RDMA link failed".into())));
            }
        }
    }

    // ------------------------------------------------------------ completions

    fn on_completion(self: &Arc<Self>, dev: &Arc<Device>, wc: &nf_wc) {
        let (kind, lane_id, idx) = wr_parts(wc.wr_id);
        let lane = self.lanes.lock().get(&lane_id).cloned();
        if wc.status != 0 {
            if let Some(l) = &lane {
                if !l.dead.load(Ordering::SeqCst) {
                    tracing::warn!(status = wc.status, kind, peer = %l.peer, "RDMA completion error");
                }
                self.fail_lane(l);
            }
            if kind == WR_WRITE {
                self.in_flight.lock().remove(&wc.wr_id);
            }
            return;
        }
        match (kind, wc.opcode) {
            (WR_RING, sys::NF_OP_RECV_IMM) => {
                let slot = wc.imm;
                let len = wc.byte_len as usize;
                let w = self.pending.lock().remove(&(dev.id, slot));
                if let Some(w) = w {
                    w.complete(len);
                }
                if let Some(l) = lane {
                    let _ = self.post_ring(&l, idx);
                }
            }
            (WR_RING, sys::NF_OP_RECV) => {
                let Some(l) = lane else { return };
                // SAFETY: the receive completed; the ring slot is ours until
                // we repost it below.
                let msg = unsafe { l.ring.slice(idx as usize * RING_SLOT, wc.byte_len as usize) }
                    .to_vec();
                let _ = self.post_ring(&l, idx);
                match msg.first().copied() {
                    Some(KIND_READ) if msg.len() >= 48 => self.serve(l, ReadReq::decode(&msg)),
                    Some(KIND_ERR) if msg.len() >= 8 => {
                        let slot = u32::from_le_bytes(msg[4..8].try_into().unwrap());
                        let w = self.pending.lock().remove(&(dev.id, slot));
                        if let Some(w) = w {
                            w.fail(err_from(msg[1]));
                        }
                    }
                    _ => tracing::warn!(peer = %l.peer, "unrecognized fabric message"),
                }
            }
            (WR_WRITE, _) => {
                self.in_flight.lock().remove(&wc.wr_id);
            }
            _ => {}
        }
    }

    // ------------------------------------------------------------ server

    /// A read request arrived (on this device's poller thread). Every
    /// served read takes one path: the object (already open, or checked
    /// off-thread first), a queue until a staging slot is free, an io_uring
    /// read into the slot, and the RDMA write when it completes, all on
    /// this thread with no hand-offs; the disk sees many reads at once.
    fn serve(self: &Arc<Self>, lane: Arc<Lane>, req: ReadReq) {
        let at = std::time::Instant::now();
        let Some(src) = self.source.lock().as_ref().and_then(|w| w.upgrade()) else {
            return self.answer(
                &lane,
                &req,
                Err(NestError::Unavailable("not serving yet".into())),
            );
        };
        let (file, generation) = (FileId(req.file), Generation(req.generation));
        match src.open_now(file, generation) {
            Some(Ok(f)) => {
                lane.dev.serve.lock().waiting.push_back(Waiting {
                    lane: lane.clone(),
                    req,
                    file: f,
                    at,
                });
                self.pump(&lane.dev);
            }
            Some(Err(e)) => self.answer(&lane, &req, Err(e)),
            None => {
                self.rt.spawn(async move {
                    let f = src.open(file, generation).await;
                    let dev = lane.dev.clone();
                    dev.resolved.lock().push(Resolved {
                        lane,
                        req,
                        file: f,
                        at,
                    });
                    dev.wake();
                });
            }
        }
    }

    /// Serving work for the poller: requests checked off-thread, finished
    /// reads, and waiting requests that can start. Returns whether anything
    /// happened.
    fn serve_work(&self, dev: &Arc<Device>) -> bool {
        let resolved = std::mem::take(&mut *dev.resolved.lock());
        let mut busy = !resolved.is_empty();
        for r in resolved {
            match r.file {
                Ok(f) => dev.serve.lock().waiting.push_back(Waiting {
                    lane: r.lane,
                    req: r.req,
                    file: f,
                    at: r.at,
                }),
                Err(e) => self.answer(&r.lane, &r.req, Err(e)),
            }
        }
        busy |= self.reap(dev);
        self.pump(dev);
        busy
    }

    /// Start waiting reads that have a staging slot and ring room.
    fn pump(&self, dev: &Device) {
        let mut g = dev.serve.lock();
        let s = &mut *g;
        let mut started = false;
        let mut i = 0;
        while i < s.waiting.len() && !s.free.is_empty() {
            let len = (s.waiting[i].req.len as usize).min(self.cfg.chunk);
            let Some(slot) = dev.staging.try_acquire(len) else {
                i += 1;
                continue;
            };
            let w = s.waiting.remove(i).expect("in range");
            let idx = s.free.pop().expect("checked");
            s.ops[idx] = Some(ServeOp {
                lane: w.lane,
                req: w.req,
                slot,
                file: w.file,
                done: 0,
                want: len,
                at: w.at,
                submitted: std::time::Instant::now(),
            });
            started |= s.push(idx);
        }
        if started {
            let _ = s.ring.submit();
        }
    }

    /// Finished io_uring reads: continue short ones, answer the rest.
    fn reap(&self, dev: &Device) -> bool {
        let mut finished = Vec::new();
        {
            let mut g = dev.serve.lock();
            let s = &mut *g;
            let cqes: Vec<(u64, i32)> = s
                .ring
                .completion()
                .map(|c| (c.user_data(), c.result()))
                .collect();
            if cqes.is_empty() {
                return false;
            }
            let mut again = false;
            for (ud, res) in cqes {
                let idx = ud as usize;
                let Some(op) = s.ops.get_mut(idx).and_then(|o| o.as_mut()) else {
                    continue;
                };
                if res == -libc::EINTR || res == -libc::EAGAIN {
                    again |= s.push(idx);
                    continue;
                }
                if res > 0 {
                    op.done += res as usize;
                    // A short read mid-file is not the end: read the rest
                    // (taking it for EOF once truncated a reader's file).
                    if op.done < op.want {
                        again |= s.push(idx);
                        continue;
                    }
                }
                let op = s.ops[idx].take().expect("present");
                s.free.push(idx);
                let r = if res < 0 {
                    Err(NestError::Io(
                        std::io::Error::from_raw_os_error(-res).to_string(),
                    ))
                } else {
                    Ok(op.done)
                };
                finished.push((op, r));
            }
            if again {
                let _ = s.ring.submit();
            }
        }
        for (op, r) in finished {
            let ServeOp {
                lane,
                req,
                slot,
                file,
                at,
                submitted,
                ..
            } = op;
            self.stats.serve_slot_wait_ns.fetch_add(
                submitted.duration_since(at).as_nanos() as u64,
                Ordering::Relaxed,
            );
            self.stats
                .serve_read_ns
                .fetch_add(submitted.elapsed().as_nanos() as u64, Ordering::Relaxed);
            self.stats.served_fast.fetch_add(1, Ordering::Relaxed);
            self.record(crate::iostats::Kind::Served, req.len as usize, at.elapsed());
            self.answer(&lane, &req, r.map(|n| (slot, n)));
            drop(file);
        }
        true
    }

    /// Send a read's answer: the bytes (an RDMA write into the client's
    /// landing slot, with the length in the immediate) or an error message.
    fn answer(&self, lane: &Arc<Lane>, req: &ReadReq, result: Result<(Slot, usize), NestError>) {
        match result {
            Ok((slot, n)) => {
                let id = wr(WR_WRITE, lane.id, slot.id());
                let (ptr, lkey) = (slot.ptr(), slot.lkey());
                self.in_flight.lock().insert(id, slot);
                let imm = req.slot;
                // SAFETY: the staging slot stays alive in `in_flight`
                // until this write completes; the remote range is the
                // landing slot the client named for this request.
                let r = unsafe {
                    lane.qp
                        .post_write_imm(id, ptr, n as u32, lkey, req.addr, req.rkey, imm)
                };
                if r.is_err() {
                    self.in_flight.lock().remove(&id);
                    self.fail_lane(lane);
                } else {
                    self.stats.served.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .served_bytes
                        .fetch_add(n as u64, Ordering::Relaxed);
                }
            }
            Err(e) => {
                let mut msg = [0u8; MSG_SIZE];
                msg[0] = KIND_ERR;
                msg[1] = err_code(&e);
                msg[4..8].copy_from_slice(&req.slot.to_le_bytes());
                // SAFETY: inline send copies the message at post time.
                if unsafe {
                    lane.qp
                        .post_send(wr(WR_SEND, lane.id, 0), msg.as_mut_ptr(), 8, 0, true)
                }
                .is_err()
                {
                    self.fail_lane(lane);
                }
            }
        }
    }
}

impl Reservation {
    /// Ask for `len` bytes of `generation` of `file` at `offset`. `done`
    /// runs on the completion thread when the answer arrives (or the lane
    /// fails, or `READ_TIMEOUT` passes): no thread waits for it.
    pub fn send(self, file: FileId, generation: Generation, offset: u64, done: ReadDone) {
        let Reservation {
            fabric,
            lane,
            slot,
            budget,
            window,
            len,
        } = self;
        let t0 = std::time::Instant::now();
        let key = (lane.dev.id, slot.id());
        let mut msg = [0u8; MSG_SIZE];
        ReadReq {
            slot: slot.id(),
            file: file.0,
            generation: generation.0,
            offset,
            len: len as u32,
            addr: slot.addr(),
            rkey: slot.rkey(),
        }
        .encode(&mut msg);
        let f = fabric.clone();
        let call = Box::new(move |a: Answer| {
            let _held = (budget, window);
            match a {
                Ok((n, slot)) => {
                    f.stats.reads.fetch_add(1, Ordering::Relaxed);
                    f.stats.read_bytes.fetch_add(n as u64, Ordering::Relaxed);
                    f.stats.read_now.fetch_add(1, Ordering::Relaxed);
                    f.stats
                        .read_rtt_ns
                        .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    f.record(crate::iostats::Kind::Fabric, len, t0.elapsed());
                    done(Ok(slot.as_slice(n)));
                }
                Err(e) => done(Err(e)),
            }
        });
        fabric.pending.lock().insert(
            key,
            Waiter {
                lane: lane.id,
                slot,
                tx: Notify::Call(call),
                since: t0,
            },
        );
        // SAFETY: inline send copies the message at post time.
        let posted = unsafe {
            lane.qp.post_send(
                wr(WR_SEND, lane.id, 0),
                msg.as_mut_ptr(),
                MSG_SIZE as u32,
                0,
                true,
            )
        };
        if let Err(e) = posted {
            let w = fabric.pending.lock().remove(&key);
            fabric.fail_lane(&lane);
            if let Some(w) = w {
                // Never sent: nothing will write the slot.
                w.fail(NestError::Unavailable(e.to_string()));
            }
        }
    }
}

/// Completion loop for one device: spin briefly, then sleep on the
/// completion channel and the serving ring's eventfd.
fn poller(weak: Weak<Fabric>, dev: Arc<Device>, stop: Arc<AtomicBool>) {
    use std::os::fd::AsRawFd;
    let mut wcs = [nf_wc::default(); 32];
    let fd = dev.cq.fd();
    let mut idle_spins = 0u32;
    let mut armed = false;
    while !stop.load(Ordering::Relaxed) {
        let n = match dev.cq.poll(&mut wcs) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(error = %e, "polling CQ failed");
                return;
            }
        };
        let Some(f) = weak.upgrade() else { return };
        for wc in &wcs[..n] {
            f.on_completion(&dev, wc);
        }
        let served = f.serve_work(&dev);
        drop(f);
        if n > 0 || served {
            idle_spins = 0;
            continue;
        }
        idle_spins += 1;
        if idle_spins < 2000 {
            std::hint::spin_loop();
            continue;
        }
        if !armed {
            // Arm, then poll once more before sleeping: a completion that
            // arrived between the last poll and arming raises no event.
            if dev.cq.arm().is_err() {
                return;
            }
            armed = true;
            continue;
        }
        let mut pfd = [
            libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: dev.wake.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        // SAFETY: valid pollfds for the duration of the call.
        let r = unsafe { libc::poll(pfd.as_mut_ptr(), 2, 50) };
        if r > 0 {
            if pfd[0].revents != 0 {
                let _ = dev.cq.take_event();
                armed = false;
            }
            if pfd[1].revents != 0 {
                dev.clear_wake();
            }
        }
        idle_spins = 0;
    }
}

/// The rails' links in bytes per second: each device port once (a port
/// carrying two subnets is two rails but one link).
fn link_bytes_per_s(rails: &[Rail]) -> u64 {
    let mut seen = std::collections::HashSet::new();
    rails
        .iter()
        .filter(|r| seen.insert((r.ibdev.clone(), r.port)))
        .map(|r| r.rate_gbps as u64 * 1_000_000_000 / 8)
        .sum()
}
