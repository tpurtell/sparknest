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
//! Write protocol (3, ADR-057): the owner pulls the bytes.
//! ```text
//! writer                                            owner
//!   take an outgoing slot (registered remote-readable), copy the bytes in
//!   SEND WriteReq{req, file, epoch, writer, off|append, len, addr, rkey} ->
//!                                        queue until a write staging slot
//!                                        is free (never reserved per writer)
//!                 <- RDMA READ(addr, rkey -> staging slot)
//!                                        WriteSink::write: the data layer's
//!                                        owner checks, append offset, pwrite
//!                 <- SEND WriteDone{req, n | code}
//!   complete the waiter for (device, req); the slot is free again
//! ```
//! A write is applied at most once and never resent: when its answer is
//! lost (the lane failed, or `write_timeout` passed after the request went
//! out) the writer reports an error with the outcome unknown, and its
//! outgoing slot is never reused (the owner may still read it until the
//! lane's QP is in the error state). Only failures before the request was
//! posted, or the owner's "not accepting", are `WriteError::Unsent` (safe
//! to send another way).
//!
//! Each lane's receive ring absorbs requests, error replies and
//! write-with-imm notifications; a per-lane window keeps a node's requests
//! plus its responses within the peer's ring. A link speaks the lower of
//! the two hosts' protocols, at least `MIN_PROTOCOL` (ADR-032): hosts on 2
//! pair for reads, and writes to them go over TCP. Otherwise reads use TCP.

use crate::pool::{Slot, Tiers};
use crate::rail::Rail;
use crate::sys::{self, nf_wc};
use crate::verbs::{Context, Cq, Qp, QpInfo, Region};
use futures::future::BoxFuture;
use nest_types::{Epoch, FileId, Generation, NestError, NodeId};
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
    /// How long a dead lane (failed, or replaced by a new link) stays
    /// registered before its QP and ring are released: long enough for the
    /// NIC to flush its outstanding work into the completion queue.
    pub lane_grace: Duration,
    /// After a failed link setup to a peer, how long until the next attempt;
    /// doubled on each further failure up to `link_retry_max`. Meanwhile
    /// reads to that peer fail at once (callers fall back to TCP).
    pub link_retry: Duration,
    pub link_retry_max: Duration,
    /// Remote writes (ADR-057), any order: per tier, `landing` is the
    /// writer's outgoing slots (registered remote-readable) and `staging`
    /// the owner's slots it pulls into. The largest size is the largest
    /// write sent over RDMA; larger ones go over TCP.
    pub write_tiers: Vec<Tier>,
    /// A remote write still unanswered after this fails with its outcome
    /// unknown, and its lane is retired.
    pub write_timeout: Duration,
    /// The TOS byte this host's RDMA packets carry (ADR-084): DSCP 18 in
    /// its top bits, a class of its own below the engine's DSCP 26, so a
    /// fabric tuned for it (`docs/LOSSLESS.md`) neither drops it nor lets
    /// it hold up the engine's traffic. 0 sends it unmarked.
    pub traffic_class: u8,
    /// The protocol this host offers; tests set 2 to stand in for an
    /// older host.
    #[doc(hidden)]
    pub protocol: u32,
}

/// DSCP 18 (AF21) as a TOS byte: SparkNest's own fabric class (ADR-084).
pub const TRAFFIC_CLASS: u8 = 18 << 2;

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
            lane_grace: Duration::from_secs(10),
            link_retry: Duration::from_secs(1),
            link_retry_max: Duration::from_secs(30),
            write_tiers: vec![
                Tier {
                    size: 64 << 10,
                    landing: 128,
                    staging: 64,
                },
                Tier {
                    size: 1 << 20,
                    landing: 32,
                    staging: 32,
                },
            ],
            write_timeout: WRITE_TIMEOUT,
            traffic_class: TRAFFIC_CLASS,
            protocol: PROTOCOL,
        }
    }
}

/// A refused dial's reason when both ends dialled at once (see `accept`).
const SIMULTANEOUS: &str = "simultaneous dial";

/// The protocol links speak: the lower of the two hosts'.
/// 2: imm carries the whole slot id, the length is the completion's count.
/// 3: remote writes pulled by the owner (`WriteReq`/`WriteDone`).
pub const PROTOCOL: u32 = 3;
/// The oldest protocol a link is made with.
const MIN_PROTOCOL: u32 = 2;
/// The first protocol with remote writes.
const WRITE_PROTOCOL: u32 = 3;

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

/// A write to apply as owner: where, in which ownership epoch, by which
/// writer handle. The fabric carries it; what an epoch means is the data
/// layer's business.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteOp {
    pub file: FileId,
    pub epoch: Epoch,
    pub writer: (NodeId, u64),
    /// Ignored when `append`: the owner writes at the end.
    pub offset: u64,
    pub append: bool,
}

/// Applies writes other hosts send (over RDMA or TCP lanes), with exactly
/// the checks a write over the control connection gets. Implemented by the
/// data service.
pub trait WriteSink: Send + Sync + 'static {
    /// Apply `data` as `op` says; the bytes written. Pulled bytes stay in
    /// the fabric's staging slot (no copy) until `data` is dropped.
    fn write(&self, op: WriteOp, data: bytes::Bytes) -> BoxFuture<'static, Result<u32, NestError>>;
}

/// A write staging slot holding pulled bytes, owned by the `Bytes` handed
/// to the sink; dropping it frees the slot for the next queued write.
struct WriteBuf {
    slot: Option<Slot>,
    len: usize,
    dev: Arc<Device>,
}

impl AsRef<[u8]> for WriteBuf {
    fn as_ref(&self) -> &[u8] {
        self.slot.as_ref().map_or(&[], |s| s.as_slice(self.len))
    }
}

impl Drop for WriteBuf {
    fn drop(&mut self) {
        drop(self.slot.take());
        // The poller starts queued pulls when a slot frees.
        self.dev.wake();
    }
}

/// Why a remote write failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteError {
    /// Nothing was applied and nothing will be: it never left this host,
    /// or the owner did not take it (no link, an older peer, not
    /// accepting writes). Safe to send another way.
    Unsent(NestError),
    /// The owner answered with this error, or the outcome is unknown (the
    /// answer was lost): never resend it.
    Failed(NestError),
}

impl WriteError {
    pub fn into_inner(self) -> NestError {
        match self {
            WriteError::Unsent(e) | WriteError::Failed(e) => e,
        }
    }
}

// ---------------------------------------------------------------- wire

const MSG_SIZE: usize = 64;
const RING_SLOT: usize = 128;
const KIND_READ: u8 = 1;
const KIND_ERR: u8 = 2;
/// Protocol 3: a write for the owner to pull, and its answer.
const KIND_WRITE: u8 = 3;
const KIND_WRITE_DONE: u8 = 4;
const WRITE_DONE_SIZE: usize = 16;
/// `WriteDone` code: the owner did not take the write (no sink yet, too
/// large for its slots); nothing was applied.
const NOT_ACCEPTING: u8 = 255;

/// The largest read a `Reservation` makes (a FUSE request is at most
/// 128 KiB); larger reads use `read`.
const RESERVE_MAX: usize = 128 << 10;
/// Serving: reads in flight in one device's io_uring.
const SERVE_DEPTH: usize = 256;
/// A read the reader still waits for after this is failed (and its lane
/// retired), as `read` does.
const READ_TIMEOUT: Duration = Duration::from_secs(30);
/// A write whose answer has not come after this is failed with its outcome
/// unknown (and its lane retired).
const WRITE_TIMEOUT: Duration = READ_TIMEOUT;
/// A completion thread keeps spinning this long after its last work.
const HOT: Duration = Duration::from_millis(10);
/// Unit of the in-flight byte budget.
const BUDGET_UNIT: usize = 4 << 10;

/// Error codes on the wire (append only: older peers read unknown codes
/// as I/O errors).
fn err_code(e: &NestError) -> u8 {
    match e {
        NestError::Stale => 1,
        NestError::Unavailable(_) => 2,
        NestError::NotFound => 3,
        NestError::NoSpace => 5,
        NestError::NoQuorum => 6,
        NestError::Busy(_) => 7,
        NestError::Invalid(_) => 8,
        _ => 4,
    }
}

fn err_from(code: u8) -> NestError {
    match code {
        1 => NestError::Stale,
        2 => NestError::Unavailable("peer cannot serve that range".into()),
        3 => NestError::NotFound,
        5 => NestError::NoSpace,
        6 => NestError::NoQuorum,
        7 => NestError::Busy("owner busy".into()),
        8 => NestError::Invalid("owner refused the write".into()),
        _ => NestError::Io("remote operation failed".into()),
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

/// A write for the owner to pull (64 bytes, sent inline).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WriteReq {
    /// The writer's outgoing slot id: names the request in `WriteDone`.
    req: u32,
    file: u64,
    epoch: u64,
    offset: u64,
    len: u32,
    rkey: u32,
    addr: u64,
    writer: u64,
    handle: u64,
    append: bool,
}

impl WriteReq {
    fn encode(&self, out: &mut [u8; MSG_SIZE]) {
        out[0] = KIND_WRITE;
        out[1] = self.append as u8;
        out[4..8].copy_from_slice(&self.req.to_le_bytes());
        out[8..16].copy_from_slice(&self.file.to_le_bytes());
        out[16..24].copy_from_slice(&self.epoch.to_le_bytes());
        out[24..32].copy_from_slice(&self.offset.to_le_bytes());
        out[32..36].copy_from_slice(&self.len.to_le_bytes());
        out[36..40].copy_from_slice(&self.rkey.to_le_bytes());
        out[40..48].copy_from_slice(&self.addr.to_le_bytes());
        out[48..56].copy_from_slice(&self.writer.to_le_bytes());
        out[56..64].copy_from_slice(&self.handle.to_le_bytes());
    }
    fn decode(b: &[u8]) -> WriteReq {
        let u32_at = |i: usize| u32::from_le_bytes(b[i..i + 4].try_into().unwrap());
        let u64_at = |i: usize| u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        WriteReq {
            append: b[1] & 1 != 0,
            req: u32_at(4),
            file: u64_at(8),
            epoch: u64_at(16),
            offset: u64_at(24),
            len: u32_at(32),
            rkey: u32_at(36),
            addr: u64_at(40),
            writer: u64_at(48),
            handle: u64_at(56),
        }
    }
    fn op(&self) -> WriteOp {
        WriteOp {
            file: FileId(self.file),
            epoch: Epoch(self.epoch),
            writer: (NodeId(self.writer), self.handle),
            offset: self.offset,
            append: self.append,
        }
    }
}

/// `WriteDone`: code 0 and the bytes written, or an error code.
fn write_done(req: u32, r: Result<u32, u8>) -> [u8; WRITE_DONE_SIZE] {
    let mut m = [0u8; WRITE_DONE_SIZE];
    m[0] = KIND_WRITE_DONE;
    m[4..8].copy_from_slice(&req.to_le_bytes());
    match r {
        Ok(n) => m[8..12].copy_from_slice(&n.to_le_bytes()),
        Err(code) => m[1] = code,
    }
    m
}

// ---------------------------------------------------------------- wr ids

const WR_RING: u64 = 1;
const WR_SEND: u64 = 2;
const WR_WRITE: u64 = 3;
/// The owner's RDMA READ of a writer's buffer.
const WR_PULL: u64 = 4;

pub(crate) fn wr(kind: u64, lane: u32, idx: u32) -> u64 {
    (kind << 56) | ((lane as u64) << 32) | idx as u64
}

fn wr_parts(id: u64) -> (u64, u32, u32) {
    (id >> 56, ((id >> 32) & 0xff_ffff) as u32, id as u32)
}

// ---------------------------------------------------------------- devices

pub(crate) struct Device {
    pub(crate) id: u32,
    pub(crate) ctx: Arc<Context>,
    /// The port as /sys/class/infiniband names it (ADR-051).
    info: crate::counters::PortInfo,
    counters: crate::counters::PortCounters,
    cq: Arc<Cq>,
    landing: Tiers,
    staging: Tiers,
    /// Writes we send: peers RDMA-READ these.
    outgoing: Tiers,
    /// Writes we receive are pulled into these.
    write_staging: Tiers,
    /// Writes waiting for a write staging slot, oldest first (and whether
    /// each has been counted as waiting).
    write_waiting: Mutex<std::collections::VecDeque<(Arc<Lane>, WriteReq, bool)>>,
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
pub(crate) struct Lane {
    pub(crate) id: u32,
    pub(crate) dev: Arc<Device>,
    pub(crate) qp: Qp,
    pub(crate) local: Rail,
    ring: Region,
    /// Our outstanding requests on this lane.
    window: Arc<Semaphore>,
    pub(crate) peer: NodeId,
    pub(crate) dead: AtomicBool,
    /// When the lane died; the expiry sweep releases it after `lane_grace`.
    dead_at: Mutex<Option<std::time::Instant>>,
    /// Path MTU agreed with the peer, bytes (for packet estimates).
    mtu: AtomicU32,
}

impl Lane {
    /// Marks the lane dead; returns whether it was alive.
    fn kill(&self) -> bool {
        let was_alive = !self.dead.swap(true, Ordering::SeqCst);
        if was_alive {
            *self.dead_at.lock() = Some(std::time::Instant::now());
        }
        was_alive
    }
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

pub(crate) struct Link {
    pub(crate) lanes: Vec<Arc<Lane>>,
    pub(crate) next: AtomicU32,
    /// The protocol both ends speak.
    protocol: u32,
}

/// A write of ours awaiting its `WriteDone`. Owns the outgoing slot, which
/// the owner may read until it answers.
struct WriteWaiter {
    lane: u32,
    slot: Slot,
    len: usize,
    since: std::time::Instant,
    tx: oneshot::Sender<Result<u32, WriteError>>,
}

/// A pull in flight: the staging slot the writer's bytes land in.
struct Pull {
    lane: Arc<Lane>,
    req: WriteReq,
    slot: Slot,
}

fn unknown_outcome(peer: NodeId, why: &str) -> WriteError {
    WriteError::Failed(NestError::Io(format!(
        "remote write to {peer}: {why}; outcome unknown"
    )))
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
    /// This host.
    me: NodeId,
    cfg: FabricConfig,
    rails: Vec<Rail>,
    pub(crate) devices: Vec<Arc<Device>>,
    rpc: nest_rpc::Rpc,
    lanes: Mutex<HashMap<u32, Arc<Lane>>>,
    next_lane: AtomicU32,
    links: Mutex<HashMap<NodeId, Arc<Link>>>,
    connecting: Mutex<HashMap<NodeId, Arc<tokio::sync::Mutex<()>>>>,
    /// Peers whose last link setup failed: when to try again, and the wait
    /// after the next failure. Without it every read re-dialled at once (a
    /// peer that could not pair took ~5,000 setups a minute).
    retry: Mutex<HashMap<NodeId, (std::time::Instant, Duration)>>,
    /// Link setups started (dialled), successful or not.
    attempts: AtomicU64,
    /// Waiters by (device, landing slot), with the lane the request used.
    pending: Mutex<HashMap<(u32, u32), Waiter>>,
    /// Staging slots held until their write-with-imm completes.
    /// With the bytes written, counted when the write completes.
    in_flight: Mutex<HashMap<u64, (Slot, u32)>>,
    source: Mutex<Option<Weak<dyn ReadSource>>>,
    /// Our writes awaiting answers, by (device, outgoing slot).
    writes: Mutex<HashMap<(u32, u32), WriteWaiter>>,
    /// Pulls in flight, by work request id.
    pulls: Mutex<HashMap<u64, Pull>>,
    sink: Mutex<Option<Weak<dyn WriteSink>>>,
    /// Where read latencies are recorded (set by the host's data layer).
    io: std::sync::OnceLock<Arc<crate::iostats::IoStats>>,
    rt: tokio::runtime::Handle,
    stop: Arc<AtomicBool>,
    /// The incast cap, in `BUDGET_UNIT`s of bytes.
    inflight: Arc<Semaphore>,
    /// This host's links, bytes per second.
    link_bps: u64,
    /// The incast budget in bytes (`inflight`'s size), for push loads.
    budget_bytes: u64,
    /// Push loads (ADR-063): writes into engines' rings.
    pub(crate) push: crate::push::PushState,
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
    /// Remote writes this host made (answered, applied), and their bytes.
    pub writes: AtomicU64,
    pub write_bytes: AtomicU64,
    /// Writes other hosts sent here, pulled and applied by the sink.
    pub pulled: AtomicU64,
    pub pulled_bytes: AtomicU64,
    /// Pulled writes that had to wait for a staging slot.
    pub pull_waits: AtomicU64,
    /// Push writes into engines' rings (ADR-063), sent from this host.
    pub push_writes: AtomicU64,
    pub push_bytes: AtomicU64,
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
            let outgoing = Tiers::with_access(
                &ctx,
                &cfg.write_tiers
                    .iter()
                    .map(|t| (t.size, t.landing))
                    .collect::<Vec<_>>(),
                crate::verbs::Access::RemoteRead,
            )?;
            let write_staging = Tiers::with_access(
                &ctx,
                &cfg.write_tiers
                    .iter()
                    .map(|t| (t.size, t.staging))
                    .collect::<Vec<_>>(),
                crate::verbs::Access::Local,
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
            let info = crate::counters::PortInfo {
                device: r.ibdev.clone(),
                port: r.port,
                netdev: r.netdev.clone(),
                addrs: rails
                    .iter()
                    .filter(|x| x.ibdev == r.ibdev)
                    .map(|x| x.addr.to_string())
                    .collect(),
                path_mtu: crate::counters::mtu_bytes(mtu),
            };
            devices.push(Arc::new(Device {
                id: devices.len() as u32,
                info,
                counters: Default::default(),
                ctx,
                cq,
                landing,
                staging,
                outgoing,
                write_staging,
                write_waiting: Mutex::new(Default::default()),
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
            me,
            cfg,
            rails,
            devices,
            rpc: rpc.clone(),
            lanes: Mutex::new(HashMap::new()),
            next_lane: AtomicU32::new(1),
            links: Mutex::new(HashMap::new()),
            connecting: Mutex::new(HashMap::new()),
            retry: Mutex::new(HashMap::new()),
            attempts: AtomicU64::new(0),
            pending: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
            source: Mutex::new(None),
            writes: Mutex::new(HashMap::new()),
            pulls: Mutex::new(HashMap::new()),
            sink: Mutex::new(None),
            push: Default::default(),
            rt: tokio::runtime::Handle::current(),
            stop: Arc::new(AtomicBool::new(false)),
            inflight: Arc::new(Semaphore::new((budget / BUDGET_UNIT as u64) as usize)),
            link_bps: link,
            budget_bytes: budget,
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
        tracing::info!(
            rails = ?fabric.rails.iter().map(|r| format!("{}/{}", r.ibdev, r.addr)).collect::<Vec<_>>(),
            traffic_class = fabric.cfg.traffic_class,
            dscp = fabric.cfg.traffic_class >> 2,
            "RDMA fabric up"
        );
        Ok(Some(fabric))
    }

    pub fn rails(&self) -> &[Rail] {
        &self.rails
    }

    pub fn set_source(&self, source: Weak<dyn ReadSource>) {
        *self.source.lock() = Some(source);
    }

    /// Apply writes other hosts send here through `sink`.
    pub fn set_sink(&self, sink: Weak<dyn WriteSink>) {
        *self.sink.lock() = Some(sink);
    }

    /// The largest write `write` sends (0: none).
    pub fn max_write(&self) -> usize {
        if self.cfg.protocol < WRITE_PROTOCOL {
            return 0;
        }
        self.devices
            .iter()
            .map(|d| d.outgoing.max_size().min(d.write_staging.max_size()))
            .min()
            .unwrap_or(0)
    }

    /// The protocol the live link to `peer` speaks (`None`: no live link).
    pub fn link_protocol(&self, peer: NodeId) -> Option<u32> {
        self.live_link(peer).map(|l| l.protocol)
    }

    /// Whether writes to `peer` can go over RDMA now: a live link on which
    /// both ends speak the write protocol.
    pub fn can_write(&self, peer: NodeId) -> bool {
        self.link_protocol(peer)
            .is_some_and(|p| p >= WRITE_PROTOCOL)
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

    /// Bytes this host lets peers have in flight toward it at once: the
    /// incast cap (ADR-032), which push grants also stay within (ADR-063).
    pub fn incast_bytes(&self) -> u64 {
        self.budget_bytes
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

    /// The ports this host's RDMA traffic uses (ADR-051).
    pub fn ports(&self) -> Vec<crate::counters::PortInfo> {
        self.devices.iter().map(|d| d.info.clone()).collect()
    }

    /// Their counters now, in the order of `ports`. Atomic loads only.
    pub fn port_counters(&self, out: &mut Vec<crate::counters::CounterValues>) {
        out.clear();
        out.extend(self.devices.iter().map(|d| d.counters.load()));
    }

    /// Chunk-sized landing slots per device.
    pub fn landing_slots(&self) -> usize {
        self.cfg.client_slots as usize
    }

    /// Whether link setup to `peer` failed, or a link to it died, within
    /// the current retry backoff: reads to it fail at once until then
    /// (ADR-057: callers use the standby lane meanwhile).
    pub fn link_failed(&self, peer: NodeId) -> bool {
        self.live_link(peer).is_none()
            && self
                .retry
                .lock()
                .get(&peer)
                .is_some_and(|(at, _)| std::time::Instant::now() < *at)
    }

    /// Start (or lengthen) the retry backoff for `peer`; returns the wait.
    /// The wait doubles while failures follow each other (a link that pairs
    /// and dies again counts), and starts afresh after a long quiet spell.
    fn back_off(&self, peer: NodeId) -> Duration {
        let mut retry = self.retry.lock();
        let now = std::time::Instant::now();
        let wait = match retry.get(&peer) {
            Some((at, w)) if now.saturating_duration_since(*at) < 2 * self.cfg.link_retry_max => *w,
            _ => self.cfg.link_retry,
        }
        .min(self.cfg.link_retry_max);
        retry.insert(
            peer,
            (
                std::time::Instant::now() + wait,
                (wait * 2).min(self.cfg.link_retry_max),
            ),
        );
        wait
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
        let mtu = AtomicU32::new(crate::counters::mtu_bytes(dev.mtu));
        let lane = Arc::new(Lane {
            id,
            dev,
            qp,
            local: rail.clone(),
            ring,
            window: Arc::new(Semaphore::new(self.cfg.window as usize)),
            peer,
            dead: AtomicBool::new(false),
            dead_at: Mutex::new(None),
            mtu,
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
        // Both ends dialling each other at once (an all-to-all start: push
        // loads) would make two links per pair; each end keeps the one it
        // installs last and retires the other's partner lanes, so the peer
        // writes into QPs in the error state until its retries run out. The
        // lower id's dial wins: while we are dialling a higher peer, its
        // dial is refused, and it uses the link our dial gives it.
        if self.me < peer && self.dialling(peer) {
            return FabricResp::Refused(SIMULTANEOUS.into());
        }
        let mine = self.cfg.protocol;
        // A dialler offers its own protocol. One newer than ours is
        // refused with "read protocol …" before anything is made, so it
        // offers `MIN_PROTOCOL` next (hosts on 2 refuse every other
        // protocol in exactly that way).
        let (lanes, protocol) = match req {
            FabricReq::ConnectV { lanes, protocol }
                if (MIN_PROTOCOL..=mine).contains(&protocol) =>
            {
                (lanes, protocol)
            }
            FabricReq::ConnectV { protocol, .. } => {
                return FabricResp::Refused(format!(
                    "read protocol {protocol} (this host speaks {mine})"
                ));
            }
            FabricReq::Connect { .. } => {
                return FabricResp::Refused(format!("read protocol 1 (this host speaks {mine})"));
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
                Err(e) => {
                    self.retire_lanes(&made.into_iter().map(|(_, l)| l).collect::<Vec<_>>());
                    return FabricResp::Refused(e.to_string());
                }
            };
            let psn = rand::random::<u32>() & 0xff_ffff;
            let mtu = lane.dev.mtu.min(offer.qp.mtu);
            lane.mtu
                .store(crate::counters::mtu_bytes(mtu), Ordering::Relaxed);
            if let Err(e) = lane.qp.connect(
                rail.current_gid_index(),
                psn,
                &offer.qp,
                mtu,
                self.cfg.traffic_class,
            ) {
                made.push((rail.clone(), lane));
                self.retire_lanes(&made.into_iter().map(|(_, l)| l).collect::<Vec<_>>());
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
            protocol,
        });
        if let Some(old) = self.links.lock().insert(peer, link) {
            self.retire(&old);
        }
        tracing::info!(%peer, lanes = pairs.len(), protocol, "RDMA link accepted");
        FabricResp::Accepted { pairs }
    }

    fn retire(&self, link: &Link) {
        self.retire_lanes(&link.lanes);
    }

    /// Takes lanes out of service without failing their requests; the
    /// expiry sweep releases them once nothing refers to them.
    fn retire_lanes(&self, lanes: &[Arc<Lane>]) {
        for l in lanes {
            l.kill();
            l.qp.set_error();
        }
    }

    /// Releases dead lanes that no link uses and nothing else holds, once
    /// their grace period has passed. Without this every replaced or failed
    /// link kept its QP and registered ring for the life of the process.
    fn reap_lanes(&self) -> usize {
        let now = std::time::Instant::now();
        let live: std::collections::HashSet<u32> = self
            .links
            .lock()
            .values()
            .flat_map(|link| link.lanes.iter().map(|l| l.id))
            .collect();
        let mut released = Vec::new();
        self.lanes.lock().retain(|id, lane| {
            let expired = !live.contains(id)
                && lane
                    .dead_at
                    .lock()
                    .is_some_and(|at| now.duration_since(at) >= self.cfg.lane_grace)
                && Arc::strong_count(lane) == 1;
            if expired {
                released.push(lane.clone());
            }
            !expired
        });
        // Dropped outside the lock: destroying a QP is a syscall.
        let count = released.len();
        drop(released);
        count
    }

    /// Link setups this host has started, for tests and diagnostics.
    pub fn link_attempts(&self) -> u64 {
        self.attempts.load(Ordering::Relaxed)
    }

    /// Registered lanes, live and not yet released.
    pub fn lane_count(&self) -> usize {
        self.lanes.lock().len()
    }

    /// Retires the link to `peer` as a replacement would, then releases
    /// every lane whose grace has passed. For tests and diagnostics.
    #[doc(hidden)]
    pub fn drop_link(&self, peer: NodeId) -> usize {
        if let Some(old) = self.links.lock().remove(&peer) {
            self.retire(&old);
        }
        self.reap_lanes()
    }

    pub(crate) async fn link(&self, peer: NodeId) -> Result<Arc<Link>, NestError> {
        if let Some(l) = self.live_link(peer) {
            return Ok(l);
        }
        let gate = self.connecting.lock().entry(peer).or_default().clone();
        let _g = gate.lock().await;
        if let Some(l) = self.live_link(peer) {
            return Ok(l);
        }
        if let Some((at, _)) = self.retry.lock().get(&peer)
            && std::time::Instant::now() < *at
        {
            return Err(NestError::Unavailable(format!(
                "RDMA link to {peer} failed recently; retrying later"
            )));
        }
        self.attempts.fetch_add(1, Ordering::Relaxed);
        let r = match self.connect(peer).await {
            // The peer was dialling us at the same moment and its id is
            // lower: its link reaches us through `accept`. Wait for it.
            Err(NestError::Unavailable(why)) if why.contains(SIMULTANEOUS) => {
                drop(_g);
                let until = std::time::Instant::now() + Duration::from_secs(5);
                loop {
                    if let Some(l) = self.live_link(peer) {
                        break Ok(l);
                    }
                    if std::time::Instant::now() >= until {
                        break Err(NestError::Unavailable(format!(
                            "{peer} was dialling us, but its link did not arrive"
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
            r => r,
        };
        match r {
            Ok(link) => {
                // Keep the next wait: a link that dies soon after pairing
                // (a dead switch: pairing rides the control connection)
                // backs off longer each time.
                if let Some((at, _)) = self.retry.lock().get_mut(&peer) {
                    *at = std::time::Instant::now();
                }
                Ok(link)
            }
            Err(e) => {
                let wait = self.back_off(peer);
                tracing::warn!(%peer, error = %e, retry_in = ?wait, "RDMA link setup failed");
                Err(e)
            }
        }
    }

    /// Whether this host is setting up a link to `peer` right now.
    fn dialling(&self, peer: NodeId) -> bool {
        self.connecting
            .lock()
            .get(&peer)
            .is_some_and(|g| g.try_lock().is_err())
    }

    fn live_link(&self, peer: NodeId) -> Option<Arc<Link>> {
        self.links
            .lock()
            .get(&peer)
            .filter(|l| l.lanes.iter().all(|x| !x.dead.load(Ordering::SeqCst)))
            .cloned()
    }

    async fn connect(&self, peer: NodeId) -> Result<Arc<Link>, NestError> {
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
        let mut protocol = self.cfg.protocol;
        let pairs = loop {
            let body = nest_rpc::encode(&FabricReq::ConnectV {
                lanes: offers.clone(),
                protocol,
            })
            .map_err(|e| NestError::Io(e.to_string()))?;
            let resp = match self
                .rpc
                .call(
                    peer,
                    nest_rpc::service::FABRIC,
                    body.into(),
                    Duration::from_secs(5),
                )
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    // The peer may have connected its side: let the sweep release ours.
                    self.retire_lanes(&lanes);
                    return Err(NestError::Unavailable(e.to_string()));
                }
            };
            match nest_rpc::decode(&resp) {
                Ok(FabricResp::Accepted { pairs }) => break pairs,
                // An older peer refuses before making anything and our QPs
                // are still unconnected: offer them again on its terms.
                Ok(FabricResp::Refused(why))
                    if protocol > MIN_PROTOCOL && why.starts_with("read protocol") =>
                {
                    tracing::info!(%peer, why, "peer speaks an older fabric protocol");
                    protocol = peer_protocol(&why)
                        .filter(|p| (MIN_PROTOCOL..protocol).contains(p))
                        .unwrap_or(MIN_PROTOCOL);
                }
                Ok(FabricResp::Refused(why)) => {
                    for l in &lanes {
                        self.lanes.lock().remove(&l.id);
                    }
                    return Err(NestError::Unavailable(format!(
                        "peer refused RDMA link: {why}"
                    )));
                }
                Err(e) => {
                    self.retire_lanes(&lanes);
                    return Err(NestError::Io(e.to_string()));
                }
            }
        };
        let mut used = Vec::new();
        for (i, remote) in pairs {
            let lane = lanes[i as usize].clone();
            let mtu = lane.dev.mtu.min(remote.mtu);
            lane.mtu
                .store(crate::counters::mtu_bytes(mtu), Ordering::Relaxed);
            // Looked up now, not at start: a link flap can renumber it.
            if let Err(e) = lane.qp.connect(
                lane.local.current_gid_index(),
                psns[i as usize],
                &remote,
                mtu,
                self.cfg.traffic_class,
            ) {
                self.retire_lanes(&lanes);
                return Err(NestError::Io(e.to_string()));
            }
            used.push(lane);
        }
        for l in &lanes {
            if !used.iter().any(|u| u.id == l.id) {
                self.lanes.lock().remove(&l.id);
            }
        }
        tracing::info!(%peer, lanes = used.len(), protocol, "RDMA link established");
        let link = Arc::new(Link {
            lanes: used,
            next: AtomicU32::new(0),
            protocol,
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
        lane.dev.counters.sent(MSG_SIZE as u64, 0);
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

    /// Send `data` (at most `max_write` bytes) to `peer`, the file's owner,
    /// to be written as `op` says: the owner pulls the bytes from our
    /// outgoing slot with an RDMA READ and answers with the count written.
    /// `WriteError::Failed` means the owner's answer (an error), or that the
    /// outcome is unknown: never send such a write again (an append would
    /// land twice).
    pub async fn write(&self, peer: NodeId, op: WriteOp, data: &[u8]) -> Result<u32, WriteError> {
        if data.len() > self.max_write() {
            return Err(WriteError::Unsent(NestError::Invalid(format!(
                "{} bytes is larger than an RDMA write",
                data.len()
            ))));
        }
        let link = self.link(peer).await.map_err(WriteError::Unsent)?;
        if link.protocol < WRITE_PROTOCOL {
            return Err(WriteError::Unsent(NestError::Unavailable(format!(
                "{peer} speaks fabric protocol {}, without writes",
                link.protocol
            ))));
        }
        let lane = link.lanes
            [link.next.fetch_add(1, Ordering::Relaxed) as usize % link.lanes.len()]
        .clone();
        let t0 = std::time::Instant::now();
        let mut slot = lane.dev.outgoing.acquire(data.len()).await;
        slot.as_mut_slice(data.len()).copy_from_slice(data);
        let _permit = lane
            .window
            .acquire()
            .await
            .map_err(|_| WriteError::Unsent(NestError::Unavailable("lane closed".into())))?;
        if lane.dead.load(Ordering::SeqCst) {
            return Err(WriteError::Unsent(NestError::Unavailable(
                "RDMA link failed".into(),
            )));
        }
        let key = (lane.dev.id, slot.id());
        let mut msg = [0u8; MSG_SIZE];
        WriteReq {
            req: slot.id(),
            file: op.file.0,
            epoch: op.epoch.0,
            offset: op.offset,
            len: data.len() as u32,
            rkey: slot.rkey(),
            addr: slot.addr(),
            writer: op.writer.0.0,
            handle: op.writer.1,
            append: op.append,
        }
        .encode(&mut msg);
        let (tx, rx) = oneshot::channel();
        // From here the slot belongs to the table until the answer: the
        // owner may read it until then.
        self.writes.lock().insert(
            key,
            WriteWaiter {
                lane: lane.id,
                slot,
                len: data.len(),
                since: t0,
                tx,
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
            // Never sent: nothing will read the slot.
            drop(self.writes.lock().remove(&key));
            self.fail_lane(&lane);
            return Err(WriteError::Unsent(NestError::Unavailable(e.to_string())));
        }
        lane.dev.counters.sent(MSG_SIZE as u64, 0);
        match tokio::time::timeout(self.cfg.write_timeout, rx).await {
            Ok(Ok(r)) => {
                if let Ok(n) = &r {
                    self.stats.writes.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .write_bytes
                        .fetch_add(*n as u64, Ordering::Relaxed);
                }
                r
            }
            Ok(Err(_)) => Err(unknown_outcome(peer, "the RDMA link failed")),
            Err(_) => {
                // The owner may still read the slot: never reuse it, and
                // retire the lane so the NIC stops.
                if let Some(w) = self.writes.lock().remove(&key) {
                    std::mem::forget(w.slot);
                }
                self.fail_lane(&lane);
                Err(unknown_outcome(peer, "no answer"))
            }
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
    /// `READ_TIMEOUT` (their lane is retired, as `read` does on a timeout),
    /// and release writes whose writer gave up waiting past `write_timeout`
    /// the same way.
    fn expire(&self) {
        self.reap_lanes();
        let now = std::time::Instant::now();
        let old: Vec<WriteWaiter> = {
            let mut w = self.writes.lock();
            let keys: Vec<_> = w
                .iter()
                .filter(|(_, x)| now.duration_since(x.since) > self.cfg.write_timeout)
                .map(|(k, _)| *k)
                .collect();
            keys.into_iter().filter_map(|k| w.remove(&k)).collect()
        };
        for w in old {
            let lane = self.lanes.lock().get(&w.lane).cloned();
            let WriteWaiter { slot, tx, .. } = w;
            std::mem::forget(slot);
            if let Some(l) = &lane {
                self.fail_lane(l);
            }
            let peer = lane.map_or(NodeId(0), |l| l.peer);
            let _ = tx.send(Err(unknown_outcome(peer, "no answer")));
        }
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
    pub(crate) fn fail_lane(&self, lane: &Lane) {
        if lane.kill() {
            // A link that died is retried after the backoff, like one that
            // failed to set up: re-pairing at once over a dead switch would
            // succeed (setup rides the control connection, which may be on
            // its LAN standby) and fail every read again (ADR-057).
            // The other lanes of the link die with it: one backoff for all.
            let wait = if self.link_failed(lane.peer) {
                None
            } else {
                Some(self.back_off(lane.peer))
            };
            tracing::warn!(peer = %lane.peer, rail = %lane.local.addr, retry_in = ?wait, "RDMA lane failed");
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
            let writes: Vec<WriteWaiter> = {
                let mut p = self.writes.lock();
                let keys: Vec<_> = p
                    .iter()
                    .filter(|(_, w)| w.lane == lane.id)
                    .map(|(k, _)| *k)
                    .collect();
                keys.into_iter().filter_map(|k| p.remove(&k)).collect()
            };
            for w in writes {
                // The owner may have read and applied it, or may still be
                // reading the slot until its QP errors: unknown, and the
                // slot is never reused.
                let WriteWaiter { slot, tx, .. } = w;
                std::mem::forget(slot);
                let _ = tx.send(Err(unknown_outcome(lane.peer, "the RDMA link failed")));
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
            if kind == WR_PULL {
                // The NIC is done with the slot (a failed or flushed read).
                drop(self.pulls.lock().remove(&wc.wr_id));
            }
            if kind == crate::push::WR_PUSH {
                self.push_written(wc.wr_id, false);
            }
            return;
        }
        let mtu = lane
            .as_ref()
            .map_or(dev.info.path_mtu, |l| l.mtu.load(Ordering::Relaxed));
        match (kind, wc.opcode) {
            (WR_RING, sys::NF_OP_RECV_IMM) if wc.imm & crate::push::IMM_PUSH != 0 => {
                dev.counters.received(wc.byte_len as u64, mtu);
                self.push_landed(wc.imm & !crate::push::IMM_PUSH, wc.byte_len);
                if let Some(l) = lane {
                    let _ = self.post_ring(&l, idx);
                }
            }
            (WR_RING, sys::NF_OP_RECV_IMM) => {
                let slot = wc.imm;
                let len = wc.byte_len as usize;
                dev.counters.received(len as u64, mtu);
                let w = self.pending.lock().remove(&(dev.id, slot));
                if let Some(w) = w {
                    w.complete(len);
                }
                if let Some(l) = lane {
                    let _ = self.post_ring(&l, idx);
                }
            }
            (WR_RING, sys::NF_OP_RECV) => {
                dev.counters.received(wc.byte_len as u64, mtu);
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
                    Some(KIND_WRITE) if msg.len() >= MSG_SIZE => {
                        self.accept_write(l, WriteReq::decode(&msg))
                    }
                    Some(KIND_WRITE_DONE) if msg.len() >= WRITE_DONE_SIZE => {
                        let req = u32::from_le_bytes(msg[4..8].try_into().unwrap());
                        let n = u32::from_le_bytes(msg[8..12].try_into().unwrap());
                        let w = self.writes.lock().remove(&(dev.id, req));
                        if let Some(w) = w {
                            // The owner's read of our slot left this port.
                            dev.counters.sent(w.len as u64, mtu);
                            let WriteWaiter { slot, tx, .. } = w;
                            drop(slot);
                            let r = match msg[1] {
                                0 => Ok(n),
                                NOT_ACCEPTING => Err(WriteError::Unsent(NestError::Unavailable(
                                    format!("{} does not take RDMA writes", l.peer),
                                ))),
                                code => Err(WriteError::Failed(err_from(code))),
                            };
                            let _ = tx.send(r);
                        }
                    }
                    _ => tracing::warn!(peer = %l.peer, "unrecognized fabric message"),
                }
            }
            (WR_WRITE, _) => {
                if let Some((_, n)) = self.in_flight.lock().remove(&wc.wr_id) {
                    dev.counters.sent(n as u64, mtu);
                }
            }
            (crate::push::WR_PUSH, _) => {
                if let Some(n) = self.push_written(wc.wr_id, true) {
                    dev.counters.sent(n as u64, mtu);
                }
            }
            (WR_PULL, _) => {
                let p = self.pulls.lock().remove(&wc.wr_id);
                if let Some(p) = p {
                    let len = p.req.len as usize;
                    dev.counters.received(len as u64, mtu);
                    let buf = WriteBuf {
                        slot: Some(p.slot),
                        len,
                        dev: dev.clone(),
                    };
                    self.apply_write(p.lane, p.req, bytes::Bytes::from_owner(buf));
                }
            }
            _ => {}
        }
    }

    // ------------------------------------------------------------ owner

    fn sink(&self) -> Option<Arc<dyn WriteSink>> {
        self.sink.lock().as_ref().and_then(|w| w.upgrade())
    }

    /// A write request arrived (on the poller thread): queue it for a
    /// staging slot to pull into, or refuse it untouched.
    fn accept_write(self: &Arc<Self>, lane: Arc<Lane>, req: WriteReq) {
        if self.sink().is_none() || req.len as usize > lane.dev.write_staging.max_size() {
            return self.send_write_done(&lane, req.req, Err(NOT_ACCEPTING));
        }
        if req.len == 0 {
            return self.apply_write(lane, req, bytes::Bytes::new());
        }
        let dev = lane.dev.clone();
        dev.write_waiting.lock().push_back((lane, req, false));
        self.pump_writes(&dev);
    }

    /// Start pulls for queued writes that can get a staging slot. Writes
    /// wait here when every slot is taken (nothing is reserved per writer);
    /// a freed slot wakes the poller, which calls this again.
    fn pump_writes(&self, dev: &Arc<Device>) -> bool {
        let mut start = Vec::new();
        {
            let mut q = dev.write_waiting.lock();
            let mut i = 0;
            while i < q.len() {
                if q[i].0.dead.load(Ordering::SeqCst) {
                    // Its writer learns from its own side of the lane.
                    q.remove(i);
                    continue;
                }
                match dev.write_staging.try_acquire(q[i].1.len as usize) {
                    Some(slot) => {
                        let (lane, req, _) = q.remove(i).expect("in range");
                        start.push((lane, req, slot));
                    }
                    None => {
                        if !q[i].2 {
                            q[i].2 = true;
                            self.stats.pull_waits.fetch_add(1, Ordering::Relaxed);
                        }
                        i += 1;
                    }
                }
            }
        }
        let started = !start.is_empty();
        for (lane, req, slot) in start {
            let id = wr(WR_PULL, lane.id, slot.id());
            let (ptr, lkey) = (slot.ptr(), slot.lkey());
            self.pulls.lock().insert(
                id,
                Pull {
                    lane: lane.clone(),
                    req,
                    slot,
                },
            );
            // SAFETY: the staging slot stays in `pulls` until this read's
            // completion (success or flush); the remote range is the
            // outgoing slot the writer named, which it keeps until it hears
            // back or its lane is retired.
            let r = unsafe {
                lane.qp
                    .post_read(id, ptr, req.len, lkey, req.addr, req.rkey)
            };
            if r.is_err() {
                // Never posted: the NIC will not touch the slot.
                drop(self.pulls.lock().remove(&id));
                self.fail_lane(&lane);
            }
        }
        started
    }

    /// Hand pulled bytes to the sink off the poller thread, and answer.
    fn apply_write(self: &Arc<Self>, lane: Arc<Lane>, req: WriteReq, data: bytes::Bytes) {
        let Some(sink) = self.sink() else {
            return self.send_write_done(&lane, req.req, Err(NOT_ACCEPTING));
        };
        let fut = sink.write(req.op(), data);
        let f = self.clone();
        self.rt.spawn(async move {
            let r = fut.await;
            if let Ok(n) = &r {
                f.stats.pulled.fetch_add(1, Ordering::Relaxed);
                f.stats.pulled_bytes.fetch_add(*n as u64, Ordering::Relaxed);
            }
            f.send_write_done(&lane, req.req, r.map_err(|e| err_code(&e)));
        });
    }

    fn send_write_done(&self, lane: &Arc<Lane>, req: u32, r: Result<u32, u8>) {
        let mut msg = write_done(req, r);
        // SAFETY: inline send copies the message at post time.
        if unsafe {
            lane.qp.post_send(
                wr(WR_SEND, lane.id, 0),
                msg.as_mut_ptr(),
                WRITE_DONE_SIZE as u32,
                0,
                true,
            )
        }
        .is_err()
        {
            self.fail_lane(lane);
        } else {
            lane.dev.counters.sent(WRITE_DONE_SIZE as u64, 0);
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
        busy |= self.pump_writes(dev);
        busy
    }

    /// Whether this device has reads in flight: served reads in its ring or
    /// waiting, or reads of ours awaiting answers on it.
    fn outstanding(&self, dev: &Device) -> bool {
        {
            let s = dev.serve.lock();
            if s.free.len() < SERVE_DEPTH || !s.waiting.is_empty() {
                return true;
            }
        }
        !dev.resolved.lock().is_empty()
            || !dev.write_waiting.lock().is_empty()
            || self.pending.lock().keys().any(|(d, _)| *d == dev.id)
            || self.writes.lock().keys().any(|(d, _)| *d == dev.id)
            || self.pulls.lock().values().any(|p| p.lane.dev.id == dev.id)
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
                self.in_flight.lock().insert(id, (slot, n as u32));
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
                } else {
                    lane.dev.counters.sent(8, 0);
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
        if posted.is_ok() {
            lane.dev.counters.sent(MSG_SIZE as u64, 0);
        }
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
    let mut last_work = std::time::Instant::now();
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
            last_work = std::time::Instant::now();
            continue;
        }
        idle_spins += 1;
        if idle_spins < 2000 {
            std::hint::spin_loop();
            continue;
        }
        idle_spins = 0;
        // Stay awake while hot: with work outstanding (reads in the ring,
        // answers awaited) or within `HOT` of the last. A sleeping thread on
        // the Sparks' deep idle states took hundreds of microseconds to
        // wake, several times per read: a lone reader's 4 KiB row took
        // ~790 us against ~150 us with the threads awake.
        if last_work.elapsed() < HOT || weak.upgrade().is_some_and(|f| f.outstanding(&dev)) {
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

/// The protocol a refusal says the peer speaks ("… (this host speaks N)",
/// as every host since protocol 2 words it).
fn peer_protocol(why: &str) -> Option<u32> {
    let rest = &why[why.find("speaks ")? + "speaks ".len()..];
    rest[..rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len())]
        .parse()
        .ok()
}

/// The rails' links in bytes per second: each physical port once.
fn link_bytes_per_s(rails: &[Rail]) -> u64 {
    crate::rail::link_gbps(rails) * 1_000_000_000 / 8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_requests_fill_64_bytes_and_round_trip() {
        let r = WriteReq {
            req: 0x0102_0304,
            file: u64::MAX - 1,
            epoch: 77,
            offset: 1 << 40,
            len: 1 << 20,
            rkey: 0xdead_beef,
            addr: 0x7fff_0000_1000,
            writer: 6,
            handle: 0x1234_5678_9abc,
            append: true,
        };
        let mut b = [0u8; MSG_SIZE];
        r.encode(&mut b);
        assert_eq!(b[0], KIND_WRITE);
        assert_eq!(WriteReq::decode(&b), r);
        let plain = WriteReq { append: false, ..r };
        plain.encode(&mut b);
        assert_eq!(WriteReq::decode(&b), plain);
        let d = write_done(9, Ok(4096));
        assert_eq!((d[0], d[1]), (KIND_WRITE_DONE, 0));
        assert_eq!(u32::from_le_bytes(d[8..12].try_into().unwrap()), 4096);
        assert_eq!(write_done(9, Err(5))[1], 5);
    }

    #[test]
    fn error_codes_round_trip() {
        for e in [
            NestError::Stale,
            NestError::NotFound,
            NestError::NoSpace,
            NestError::NoQuorum,
        ] {
            assert_eq!(err_from(err_code(&e)), e);
        }
        assert!(matches!(
            err_from(err_code(&NestError::Busy("x".into()))),
            NestError::Busy(_)
        ));
        assert!(matches!(
            err_from(err_code(&NestError::Exists)),
            NestError::Io(_)
        ));
    }

    #[test]
    fn refusals_name_the_peers_protocol() {
        assert_eq!(
            peer_protocol("read protocol 3 (this host speaks 2)"),
            Some(2)
        );
        assert_eq!(
            peer_protocol("read protocol 1 (this host speaks 17)"),
            Some(17)
        );
        assert_eq!(peer_protocol("no rail shares a subnet"), None);
    }
}
