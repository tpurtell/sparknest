//! The filesystem as seen by a mount on this node, independent of FUSE.
//!
//! Every operation takes and returns plain values and `NestResult`, so the
//! full POSIX-facing behaviour can be tested across nodes without mounting.
//! `nest-fuse` is a thin adapter over this. The same object also serves
//! other nodes' data requests (see [`crate::remote`]).
//!
//! Content lifecycle (PROPOSAL §6, ADR-011, ADR-015):
//! - Opening never changes ownership. The first mutation (write, truncate,
//!   O_TRUNC) of a STABLE file makes one node the owner of a new generation;
//!   every other copy is invalidated in the same commit. The owner is this
//!   node if it holds the content (or the mutation discards it), otherwise
//!   a node that does, and this node writes through it.
//! - Before the owner applies the first mutation it fences: every node
//!   acknowledges that it applied the grant and finished older reads, or
//!   its read lease runs out.
//! - Writer handles anywhere participate in the epoch. When the last one
//!   leaves, ownership lingers briefly and then the generation is finalized.
//! - Reads go to a local copy when this node may serve one, else to the
//!   owner (while owned) or a node holding a live copy; every remote read
//!   names the exact generation and is refused if it is no longer current.
//! - Unsealed files are always served through the daemon (direct I/O);
//!   sealed files may use kernel passthrough or the page cache.

use crate::DataNode;
use crate::remote::{self, DataReq, DataResp, DataService, Writer};
use nest_meta::{Command, Effect, LockKind, RenameFlags, Reply, query};
use nest_store::ObjectKey;
use nest_types::{
    DirEntry, Epoch, FileAttr, FileId, FileKind, GenState, Generation, NestError, NestResult,
    NodeId, Setting, Timestamp,
};
use parking_lot::Mutex;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::watch;

#[derive(Clone, Debug)]
pub struct VfsConfig {
    /// How long ownership survives the last writer leaving before the
    /// generation is finalized.
    pub finalize_linger: Duration,
    /// How long reads wait for the node to catch up after start.
    pub catch_up_wait: Duration,
    /// Timeout for data requests to other nodes.
    pub rpc_timeout: Duration,
    /// When a sealed file with a local copy is handed to the kernel.
    pub passthrough: Passthrough,
}

/// When a sealed file with a local copy is served by the kernel directly
/// (fastest for one reader, but only ever from this host's disk).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Passthrough {
    /// Whenever this host has a copy.
    #[default]
    Always,
    /// Only when this host holds the only live copy; otherwise the daemon
    /// serves reads and can spread them over every copy.
    Sole,
    /// Never: the daemon serves every read.
    Never,
}

impl Default for VfsConfig {
    fn default() -> Self {
        VfsConfig {
            finalize_linger: Duration::from_millis(250),
            catch_up_wait: Duration::from_secs(10),
            rpc_timeout: Duration::from_secs(10),
            passthrough: Passthrough::Always,
        }
    }
}

/// How the frontend should serve an open file.
#[derive(Debug)]
pub enum OpenMode {
    /// Sealed file with a local complete copy: hand this file to the kernel.
    Passthrough(std::fs::File),
    /// Sealed file served by the daemon; the kernel may cache pages forever.
    Cached,
    /// Unsealed: every read and write goes through the daemon.
    Direct,
}

/// An object a push source reads (ADR-063): direct I/O where the store
/// allows it, so a load does not fill the page cache (on the Sparks that
/// memory is the GPU's), else buffered.
pub struct PushObject {
    buffered: Arc<std::fs::File>,
    direct: Option<std::fs::File>,
    size: u64,
}

/// Bytes a push source read: the aligned buffer a direct read filled
/// (the wanted bytes start `lo` into it), or a plain one. Packing reads
/// them in place, so a direct read costs no copy out.
pub enum PushRead {
    Direct {
        buf: nest_store::uring::AlignedBuf,
        lo: usize,
        len: usize,
    },
    Buffered(Vec<u8>),
}

impl std::ops::Deref for PushRead {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            PushRead::Direct { buf, lo, len } => &buf.as_slice()[*lo..*lo + *len],
            PushRead::Buffered(b) => b,
        }
    }
}

impl PushObject {
    /// Read `[off, off+len)` (blocking). Direct reads are aligned outward;
    /// the result covers exactly the bytes asked for.
    pub fn read(&self, off: u64, len: usize) -> std::io::Result<PushRead> {
        use std::os::unix::fs::FileExt;
        if let Some(f) = &self.direct {
            let a = nest_store::uring::ALIGN as u64;
            let start = off / a * a;
            let end = (off + len as u64).div_ceil(a) * a;
            let mut buf = nest_store::uring::AlignedBuf::new((end - start) as usize);
            let mut got = 0usize;
            let want = (self.size.min(end) - start) as usize;
            while got < want {
                match f.read_at(&mut buf.as_mut_slice()[got..], start + got as u64) {
                    Ok(0) => break,
                    Ok(n) => got += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    // A filesystem that took the open but not the read.
                    Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
                        return self.read_buffered(off, len);
                    }
                    Err(e) => return Err(e),
                }
            }
            let lo = (off - start) as usize;
            if got < lo + len {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "object shorter than its size",
                ));
            }
            return Ok(PushRead::Direct { buf, lo, len });
        }
        self.read_buffered(off, len)
    }

    fn read_buffered(&self, off: u64, len: usize) -> std::io::Result<PushRead> {
        use std::os::unix::fs::FileExt;
        let mut b = vec![0u8; len];
        self.buffered.read_exact_at(&mut b, off)?;
        Ok(PushRead::Buffered(b))
    }

    pub fn is_direct(&self) -> bool {
        self.direct.is_some()
    }
}

#[cfg(test)]
mod push_object_tests {
    use super::*;
    use std::os::unix::fs::OpenOptionsExt;

    /// Direct reads at unaligned offsets and lengths give the same bytes
    /// as buffered ones (where the filesystem takes O_DIRECT).
    #[test]
    fn direct_reads_match_buffered() {
        let dir =
            tempfile::tempdir_in(std::env::var("NEST_DISK_DIR").unwrap_or_else(|_| ".".into()))
                .unwrap();
        let p = dir.path().join("obj");
        let size = (3 << 20) + 12345;
        let data: Vec<u8> = (0..size).map(|i| (i * 31 % 253) as u8).collect();
        std::fs::write(&p, &data).unwrap();
        let direct = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(&p)
            .ok();
        let o = PushObject {
            buffered: Arc::new(std::fs::File::open(&p).unwrap()),
            direct,
            size: size as u64,
        };
        eprintln!("direct: {}", o.is_direct());
        for (off, len) in [
            (0, 4096),
            (1, 100),
            (4095, 2),
            (1 << 20, 1 << 20),
            (size as u64 - 7, 7),
            (12345, 2 << 20),
        ] {
            assert_eq!(
                &*o.read(off, len).unwrap(),
                &data[off as usize..off as usize + len],
                "{off}+{len}"
            );
        }
        assert!(o.read(size as u64 - 1, 2).is_err());
    }
}

/// A live copy of a file's current generation (`Vfs::copies`).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Copy {
    /// The store's name: a host's name for its live store.
    pub store: String,
    pub kind: CopyKind,
}

/// What holds a copy, in the order reads prefer them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CopyKind {
    /// This host's own store.
    Local,
    /// Another host on the RDMA fabric.
    Fabric,
    /// An off-grid host, reached over TCP lanes (ADR-047).
    Offgrid,
    /// An archive store, read through its gateways (ADR-055).
    Archive,
}

/// Open-flag bits the VFS cares about (Linux values).
pub mod oflags {
    pub const ACCMODE: i32 = libc::O_ACCMODE;
    pub const WRONLY: i32 = libc::O_WRONLY;
    pub const RDWR: i32 = libc::O_RDWR;
    pub const TRUNC: i32 = libc::O_TRUNC;
    pub const EXCL: i32 = libc::O_EXCL;
    pub const APPEND: i32 = libc::O_APPEND;
}

/// A blocking callback run during fencing to drop kernel page caches for a
/// file (registered by the FUSE frontend).
pub type FenceHook = Arc<dyn Fn(FileId) + Send + Sync>;

struct Handle {
    file: FileId,
    writable: bool,
    append: bool,
    /// Cached local object for reads, revalidated against the generation.
    reader: Mutex<Option<(ObjectKey, Arc<std::fs::File>)>>,
    /// Lock owners that took locks through this handle.
    lock_owners: Mutex<HashSet<u64>>,
    /// A remote owner this handle wrote through, to leave on close.
    remote: Mutex<Option<(NodeId, Epoch)>>,
    /// Bytes read through this handle not yet folded into usage: local,
    /// remote, archive.
    read_bytes: [AtomicU64; 3],
    /// The copies reads of a generation go to, decided on its first read.
    route: Mutex<Option<(Generation, Option<Route>)>>,
    /// The file's attributes for fast-path reads of a STABLE generation,
    /// trusted for `ATTR_TTL`: every read names its exact generation, so a
    /// rewrite in between fails as Stale and the slow path refreshes.
    stable_attr: Mutex<Option<(FileAttr, std::time::Instant)>>,
    /// Where reads of a STABLE generation come from on this host, decided
    /// with metadata queries and kept until any copy anywhere changes
    /// (`Vfs::replica_epoch`) or `ATTR_TTL` passes. Without it every
    /// remote read queried SQLite to learn there is no copy here; 64
    /// FUSE queues doing that serialised on SQLite's page-cache mutex.
    local: Mutex<Option<LocalPick>>,
}

/// A handle's remembered answer to `own_read_source`.
struct LocalPick {
    generation: Generation,
    epoch: u64,
    at: std::time::Instant,
    /// The store read from, or none: every read goes to another host.
    src: Option<(nest_types::StoreId, Arc<nest_store::ObjectStore>)>,
}

/// A generation readable from several copies.
#[derive(Clone)]
struct Route {
    /// Other fabric hosts holding a live copy.
    sources: Vec<crate::balance::Source>,
}

impl Handle {
    fn count(&self, src: crate::usage::Source, n: u64) {
        self.read_bytes[src as usize].fetch_add(n, Ordering::Relaxed);
    }

    fn take_counts(&self) -> [u64; 3] {
        [0, 1, 2].map(|i| self.read_bytes[i].swap(0, Ordering::Relaxed))
    }
}

/// This node's state for a file it owns.
struct Owned {
    key: ObjectKey,
    epoch: Epoch,
    file: Arc<std::fs::File>,
    participants: Mutex<HashSet<Writer>>,
    /// Writes hold this shared while they run; finalize takes it exclusive
    /// and flips it to closed, so no write can land in a finalized object.
    open: tokio::sync::RwLock<bool>,
    /// Bumped on every mutation; a lingering finalize aborts if it moved.
    activity: AtomicU64,
    /// Held from choosing an append's offset (the end) until its bytes are
    /// written, so concurrent appends from any writers never overlap.
    append: Mutex<()>,
    /// True once revocation completed; mutations wait for it.
    fenced: watch::Receiver<bool>,
}

pub struct Vfs {
    /// Bytes read through sparknest from this host's own disk (tracked
    /// reads; passthrough bypasses us), for live rates.
    local_read_bytes: AtomicU64,
    d: Arc<DataNode>,
    cfg: VfsConfig,
    weak: Weak<Vfs>,
    readers: Mutex<Vec<Connection>>,
    handles: Mutex<HashMap<u64, Arc<Handle>>>,
    next_fh: AtomicU64,
    owned: Mutex<HashMap<FileId, Arc<Owned>>>,
    usage: Arc<crate::usage::Usage>,
    balancer: Arc<crate::balance::Balancer>,
    /// Read latencies by kind and size, over recent windows.
    io: Arc<nest_fabric::iostats::IoStats>,
    /// This host's measured disk read rate (bytes/s; 0 until measured).
    disk_bps: Arc<AtomicU64>,
    /// Revocation state per (file, epoch) this node was granted.
    fences: Mutex<HashMap<(FileId, Epoch), watch::Receiver<bool>>>,
    /// Local reads in progress per file (fencing waits for them).
    inflight: Mutex<HashMap<FileId, u32>>,
    /// Objects this host serves to others, kept open: fully re-checked
    /// (metadata, live replica) when idle `SERVE_TTL` or open
    /// `SERVE_MAX_AGE`, and in between only against the lease and fences;
    /// closed as soon as their copy here is invalidated. STABLE
    /// generations only: their bytes never change.
    serve_files: Mutex<HashMap<ObjectKey, ServeEntry>>,
    /// Bumped whenever a copy of any file is added, removed or replaced
    /// (after the change is committed): handles re-decide their
    /// `LocalPick` on the next read.
    replica_epoch: AtomicU64,
    /// Metadata queries this VFS ran (tests check the read path runs none).
    queries: AtomicU64,
    /// Transfers into each archive store at once (a single disk or share
    /// gains nothing from more; dozens of queued offloads once pinned
    /// hundreds of threads on a 150 MB/s disk).
    archive_slots: Mutex<HashMap<nest_types::StoreId, Arc<tokio::sync::Semaphore>>>,
    /// Transfers into each of this host's stores (`DISK_TRANSFERS`).
    disk_slots: Mutex<HashMap<nest_types::StoreId, Arc<tokio::sync::Semaphore>>>,
    inflight_done: tokio::sync::Notify,
    fence_hooks: Mutex<Vec<FenceHook>>,
    fabric: Mutex<Option<Arc<nest_fabric::Fabric>>>,
    /// TCP lanes to and from off-grid hosts (ADR-048), and the standby
    /// lanes between fabric hosts (ADR-057).
    lanes: Mutex<Option<Arc<nest_fabric::tcp::Lanes>>>,
    /// The fabric and lanes as one transport (built on first use).
    standby_bulk: Mutex<Option<Arc<crate::bulk::Standby>>>,
    /// Cluster-wide settings as last read from the metadata (ADR-056).
    settings: Mutex<(std::time::Instant, HashMap<String, u64>)>,
}

/// How long a read of the settings stays fresh: a change applies on every
/// host within this.
/// How long a copy keeps trying when no source can serve it right now.
const COPY_RECOVERY_WAIT: Duration = Duration::from_secs(120);
const SETTINGS_TTL: Duration = Duration::from_secs(1);

fn io(e: std::io::Error) -> NestError {
    NestError::from_io(&e)
}

fn sql(e: rusqlite::Error) -> NestError {
    NestError::Io(format!("metadata: {e}"))
}

/// Decrements the in-flight read count for a file when dropped.
struct Inflight<'a> {
    vfs: &'a Vfs,
    file: FileId,
}

impl Drop for Inflight<'_> {
    fn drop(&mut self) {
        let mut m = self.vfs.inflight.lock();
        if let Some(n) = m.get_mut(&self.file) {
            *n -= 1;
            if *n == 0 {
                m.remove(&self.file);
            }
        }
        drop(m);
        self.vfs.inflight_done.notify_waiters();
    }
}

/// What `Vfs::remove_tree` removed (or, dry run, would remove).
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct RemoveReport {
    pub files: u64,
    pub dirs: u64,
    pub errors: Vec<String>,
}

/// A directory entry with its attributes (`Vfs::readdir_plus`).
#[derive(Clone, Debug)]
pub struct DirEntryPlus {
    /// Cookie to continue the listing after this entry.
    pub offset: u64,
    pub name: Vec<u8>,
    pub attr: FileAttr,
    /// False when another host owns the file: the size here may be stale,
    /// so the kernel must not cache it.
    pub fresh: bool,
}

impl DirEntryPlus {
    fn new(offset: u64, name: &[u8], attr: FileAttr) -> Self {
        DirEntryPlus {
            offset,
            name: name.to_vec(),
            attr,
            fresh: true,
        }
    }
}

fn pread_into(f: &std::fs::File, offset: u64, buf: &mut [u8]) -> NestResult<usize> {
    let mut done = 0;
    while done < buf.len() {
        match f.read_at(&mut buf[done..], offset + done as u64) {
            Ok(0) => break,
            Ok(n) => done += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(io(e)),
        }
    }
    Ok(done)
}

fn pread(f: &std::fs::File, offset: u64, len: u32) -> NestResult<Vec<u8>> {
    let mut buf = vec![0u8; len as usize];
    let mut done = 0;
    while done < buf.len() {
        match f.read_at(&mut buf[done..], offset + done as u64) {
            Ok(0) => break,
            Ok(n) => done += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(io(e)),
        }
    }
    buf.truncate(done);
    Ok(buf)
}

impl Vfs {
    /// Create the VFS, register it as this node's DATA service and as an
    /// observer of ownership and session effects.
    pub fn new(d: Arc<DataNode>, cfg: VfsConfig) -> Arc<Vfs> {
        let vfs = Arc::new_cyclic(|weak| Vfs {
            local_read_bytes: AtomicU64::new(0),
            d: d.clone(),
            cfg,
            weak: weak.clone(),
            readers: Mutex::new(Vec::new()),
            handles: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            owned: Mutex::new(HashMap::new()),
            usage: Arc::new(crate::usage::Usage::open(d.store().root())),
            balancer: Arc::new(crate::balance::Balancer::default()),
            io: Arc::default(),
            disk_bps: Arc::new(AtomicU64::new(
                crate::diskprobe::load(d.store().root()).map_or(0, |b| b.bytes_per_s),
            )),
            fences: Mutex::new(HashMap::new()),
            inflight: Mutex::new(HashMap::new()),
            serve_files: Mutex::new(HashMap::new()),
            replica_epoch: AtomicU64::new(0),
            queries: AtomicU64::new(0),
            archive_slots: Mutex::new(HashMap::new()),
            disk_slots: Mutex::new(HashMap::new()),
            inflight_done: tokio::sync::Notify::new(),
            fence_hooks: Mutex::new(Vec::new()),
            fabric: Mutex::new(None),
            lanes: Mutex::new(None),
            standby_bulk: Mutex::new(None),
            settings: Mutex::new((std::time::Instant::now() - SETTINGS_TTL, HashMap::new())),
        });
        d.meta().rpc().register(
            nest_rpc::service::DATA,
            Arc::new(DataService {
                vfs: Arc::downgrade(&vfs),
            }),
        );
        let weak = Arc::downgrade(&vfs);
        d.add_tap(Arc::new(move |index, e| {
            if let Some(v) = weak.upgrade() {
                v.on_effect(index, e);
            }
        }));
        // Ownerships granted before this VFS existed (startup) are finalized
        // by the data node; nothing to adopt here.
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(fold_usage(Arc::downgrade(&vfs)));
            tokio::spawn(roll_io_stats(Arc::downgrade(&vfs.io)));
            tokio::spawn(sweep_serve_files(Arc::downgrade(&vfs)));
            tokio::spawn(probe_disk(Arc::downgrade(&vfs)));
        }
        vfs
    }

    /// This host's file usage (ADR-028).
    pub fn usage(&self) -> &Arc<crate::usage::Usage> {
        &self.usage
    }

    /// What this host has learned about its read sources (ADR-030).
    pub fn io_report(&self) -> Vec<crate::balance::SourceReport> {
        self.balancer.report()
    }

    pub fn balancer(&self) -> &Arc<crate::balance::Balancer> {
        &self.balancer
    }

    /// This host's measured disk read rate, bytes/s (0 until measured).
    pub fn disk_read_bps(&self) -> u64 {
        self.disk_bps.load(Ordering::Relaxed)
    }

    /// Each of this host's extra disks with its measured read rate
    /// (bytes/s, 0 until measured; ADR-073), kept beside its objects.
    pub fn disk_store_rates(&self) -> Vec<(nest_types::StoreId, u64)> {
        self.d
            .disks()
            .iter()
            .map(|d| {
                let b = crate::diskprobe::load(d.store.root()).map_or(0, |b| b.bytes_per_s);
                (d.id, b)
            })
            .collect()
    }

    /// Set the measured disk read rate (tests: in-process nodes share one
    /// disk, so their measured rates are alike).
    pub fn set_disk_read_bps(&self, bps: u64) {
        self.disk_bps.store(bps, Ordering::Relaxed);
    }

    /// Members that do not vote: off-grid hosts, on the LAN only (ADR-047).
    pub fn offgrid_hosts(&self) -> std::collections::HashSet<NodeId> {
        let m = self.d.meta().metrics().membership_config;
        let voters: std::collections::HashSet<u64> = m.membership().voter_ids().collect();
        m.nodes()
            .map(|(id, _)| *id)
            .filter(|id| !voters.contains(id))
            .map(NodeId)
            .collect()
    }

    /// This host's RDMA link rate, bytes/s (0 without a fabric).
    pub fn link_bps(&self) -> u64 {
        self.fabric().map_or(0, |f| f.link_bytes_per_s())
    }

    /// The other fabric hosts a STABLE generation can be read from over
    /// RDMA (`None`: none, or no fabric).
    fn route_for(&self, a: &FileAttr) -> Option<Route> {
        use crate::balance::Source;
        if a.gen_state != GenState::Stable || self.fabric().is_none() {
            return None;
        }
        let me = self.me();
        let offgrid = self.offgrid_hosts();
        let sources: Vec<Source> = self
            .q(|c| query::replicas(c, a.id))
            .ok()?
            .into_iter()
            .filter(|r| {
                r.generation == a.generation
                    && r.state == nest_types::ReplicaState::Live
                    && r.store.host().is_some_and(|h| h != me)
            })
            .filter_map(|r| r.store.host())
            // Off-grid hosts are not on the fabric: never part of a spread.
            .filter(|n| !offgrid.contains(n))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(Source::Peer)
            .collect();
        (!sources.is_empty()).then_some(Route { sources })
    }

    /// The handle's route for `a`'s generation, deciding it on first use.
    fn route(&self, h: &Handle, a: &FileAttr) -> Option<Route> {
        let mut r = h.route.lock();
        match r.as_ref() {
            Some((g, route)) if *g == a.generation => route.clone(),
            _ => {
                let route = self.route_for(a);
                *r = Some((a.generation, route.clone()));
                route
            }
        }
    }

    /// Move a handle's byte counters into the usage table's pending batch.
    fn fold_handle(&self, h: &Handle) {
        let [l, r, a] = h.take_counts();
        self.usage.add_bytes(h.file, l, r, a);
    }

    /// Where `src` sits for usage accounting.
    /// Count bytes a handle read, by origin (usage) and host-wide (local).
    fn count_read(&self, h: &Handle, src: crate::usage::Source, n: u64) {
        if src == crate::usage::Source::Local {
            self.local_read_bytes.fetch_add(n, Ordering::Relaxed);
        }
        h.count(src, n);
    }

    /// A read with no copy here, ready to send without waiting:
    /// the frontend starts it and replies from `done`, which runs on the
    /// fabric's completion thread (no thread waits for the answer, and no
    /// trip through the runtime). `None`: use `read`.
    pub fn prepare_remote(&self, fh: u64, offset: u64, size: u32) -> Option<ScatteredRead> {
        let h = self.handle(fh).ok()?;
        let a = self.fast_attr(&h)?;
        if a.gen_state != GenState::Stable || self.read_source(&h, &a).is_some() {
            return None;
        }
        let fab = self.fabric()?;
        let route = self.route(&h, &a)?;
        let peers: Vec<crate::balance::Source> = route
            .sources
            .iter()
            .copied()
            .filter(|s| matches!(s, crate::balance::Source::Peer(_)))
            .collect();
        if peers.is_empty() {
            return None;
        }
        let len = a.size.saturating_sub(offset).min(size as u64) as usize;
        let ticket = self
            .balancer
            .pick_affine(&peers, region_key(h.file, offset));
        let crate::balance::Source::Peer(node) = ticket.source() else {
            return None;
        };
        let reservation = fab.reserve(node, len.max(1))?;
        Some(ScatteredRead {
            vfs: self.arc(),
            h,
            generation: a.generation,
            offset,
            reservation,
            ticket,
        })
    }

    /// Attributes for a fast-path read: cached on the handle for STABLE
    /// generations (no query per page fault), else read.
    fn fast_attr(&self, h: &Handle) -> Option<FileAttr> {
        if let Some((a, at)) = h.stable_attr.lock().as_ref()
            && at.elapsed() < ATTR_TTL
        {
            return Some(a.clone());
        }
        let a = self.raw_attr(h.file).ok()?;
        if a.gen_state == GenState::Stable {
            *h.stable_attr.lock() = Some((a.clone(), std::time::Instant::now()));
        }
        Some(a)
    }

    /// Read latencies over the last 10 s, 1 min, 10 min.
    pub fn io_windows(&self) -> Vec<nest_fabric::iostats::Window> {
        self.io.windows()
    }

    /// Bytes read from this host's own disk through sparknest so far.
    pub fn local_read_bytes(&self) -> u64 {
        self.local_read_bytes.load(Ordering::Relaxed)
    }

    fn source_kind(&self, src: &Arc<nest_store::ObjectStore>) -> crate::usage::Source {
        if Arc::ptr_eq(src, self.d.store()) {
            crate::usage::Source::Local
        } else {
            crate::usage::Source::Archive
        }
    }

    pub fn data(&self) -> &Arc<DataNode> {
        &self.d
    }

    /// Use `fabric` for remote reads, and serve its requests from here.
    pub fn attach_fabric(self: &Arc<Self>, fabric: Arc<nest_fabric::Fabric>) {
        let src: Arc<dyn nest_fabric::ReadSource> = self.clone();
        fabric.set_source(Arc::downgrade(&src));
        let sink: Arc<dyn nest_fabric::WriteSink> = self.clone();
        fabric.set_sink(Arc::downgrade(&sink));
        fabric.set_io_stats(self.io.clone());
        // The fabric holds Weaks; keep the trait objects alive with us.
        std::mem::forget(src);
        std::mem::forget(sink);
        *self.fabric.lock() = Some(fabric);
    }

    pub fn fabric(&self) -> Option<Arc<nest_fabric::Fabric>> {
        self.fabric.lock().clone()
    }

    /// Use `lanes` for reads to and from off-grid hosts, and serve their
    /// requests from here.
    pub fn attach_lanes(self: &Arc<Self>, lanes: Arc<nest_fabric::tcp::Lanes>) {
        let src: Arc<dyn nest_fabric::ReadSource> = self.clone();
        lanes.set_source(Arc::downgrade(&src));
        let sink: Arc<dyn nest_fabric::WriteSink> = self.clone();
        lanes.set_sink(Arc::downgrade(&sink));
        // The lanes hold Weaks; keep the trait objects alive with us.
        std::mem::forget(src);
        std::mem::forget(sink);
        *self.lanes.lock() = Some(lanes);
    }

    pub fn lanes(&self) -> Option<Arc<nest_fabric::tcp::Lanes>> {
        self.lanes.lock().clone()
    }

    /// Replace the transports this host sends with (serving is unchanged),
    /// returning the old ones: benchmarks compare paths with it.
    #[doc(hidden)]
    pub fn swap_transports(
        &self,
        fabric: Option<Arc<nest_fabric::Fabric>>,
        lanes: Option<Arc<nest_fabric::tcp::Lanes>>,
    ) -> (
        Option<Arc<nest_fabric::Fabric>>,
        Option<Arc<nest_fabric::tcp::Lanes>>,
    ) {
        (
            std::mem::replace(&mut *self.fabric.lock(), fabric),
            std::mem::replace(&mut *self.lanes.lock(), lanes),
        )
    }

    /// How to fetch whole chunks from `peer`: TCP lanes when either end is
    /// off the fabric, else RDMA with the standby lane behind it (ADR-057).
    /// `None`: one request at a time over the control connection.
    fn bulk_to(&self, peer: NodeId) -> Option<Arc<dyn crate::bulk::Bulk>> {
        let fabric = self.fabric();
        let lanes = self.lanes();
        if (fabric.is_none() || self.offgrid_hosts().contains(&peer))
            && let Some(l) = &lanes
            && l.connected(peer) > 0
        {
            return Some(l.clone());
        }
        let fabric = fabric?;
        Some(match self.standby() {
            Some(s) => s,
            None => fabric,
        })
    }

    /// RDMA with every peer's standby lane behind it, between fabric hosts
    /// (ADR-057); `None` without both.
    fn standby(&self) -> Option<Arc<crate::bulk::Standby>> {
        let (fabric, lanes) = (self.fabric()?, self.lanes()?);
        let mut cached = self.standby_bulk.lock();
        if let Some(s) = cached.as_ref()
            && Arc::ptr_eq(&s.fabric, &fabric)
            && Arc::ptr_eq(&s.lanes, &lanes)
        {
            return Some(s.clone());
        }
        let s = Arc::new(crate::bulk::Standby { fabric, lanes });
        *cached = Some(s.clone());
        Some(s)
    }

    /// Register a blocking page-cache invalidation run during fencing.
    pub fn add_fence_hook(&self, hook: FenceHook) {
        self.fence_hooks.lock().push(hook);
    }

    fn arc(&self) -> Arc<Vfs> {
        self.weak.upgrade().expect("Vfs used after drop")
    }

    /// Run a closure with a pooled read-only metadata connection.
    pub(crate) fn q<T>(&self, f: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> NestResult<T> {
        self.queries.fetch_add(1, Ordering::Relaxed);
        let c = self.readers.lock().pop();
        let c = match c {
            Some(c) => c,
            None => self.d.meta().open_reader().map_err(sql)?,
        };
        let r = f(&c).map_err(sql);
        let mut pool = self.readers.lock();
        if pool.len() < 32 {
            pool.push(c);
        }
        r
    }

    pub(crate) fn raw_attr(&self, file: FileId) -> NestResult<FileAttr> {
        self.q(|c| query::getattr(c, file))?
            .ok_or(NestError::NotFound)
    }

    pub(crate) async fn propose(&self, cmd: Command) -> NestResult<Reply> {
        self.d.meta().propose(cmd).await
    }

    pub(crate) fn me(&self) -> NodeId {
        self.d.id()
    }

    pub(crate) fn rpc(&self) -> &nest_rpc::Rpc {
        self.d.meta().rpc()
    }

    async fn call(&self, peer: NodeId, req: &DataReq) -> NestResult<DataResp> {
        remote::call(self.rpc(), peer, req, self.cfg.rpc_timeout).await
    }

    // ------------------------------------------------------------ effects

    fn on_effect(&self, index: u64, e: &Effect) {
        if matches!(
            e,
            Effect::ReplicaInvalidated { .. }
                | Effect::ReplicaPublished { .. }
                | Effect::Finalized { .. }
                | Effect::OwnershipGranted { .. }
                | Effect::FileDeleted { .. }
        ) {
            self.replica_epoch.fetch_add(1, Ordering::SeqCst);
        }
        match e {
            // Deleted, evicted or superseded by a new owner's generation:
            // close it now so its space comes back with the last read still
            // holding it (ADR-057), not at the next sweep.
            Effect::ReplicaInvalidated {
                file,
                generation,
                store,
            } if store.host() == Some(self.me()) => {
                let key = ObjectKey::new(*file, *generation);
                let mut m = self.serve_files.lock();
                if m.get(&key).is_some_and(|e| e.store == *store) {
                    m.remove(&key);
                }
            }
            Effect::OwnershipGranted {
                file,
                owner,
                generation,
                epoch,
                from_gen,
            } if *owner == self.me() => {
                let (tx, rx) = watch::channel(false);
                self.fences.lock().insert((*file, *epoch), rx);
                let me = self.arc();
                let (file, generation, epoch) = (*file, *generation, *epoch);
                // A brand-new file has no older content anyone could read.
                let new_file = generation == Generation(1) && from_gen.is_none();
                let granted = Instant::now();
                tokio::spawn(async move {
                    if !new_file {
                        me.fence_cluster(file, generation, index, granted).await;
                    }
                    let _ = tx.send(true);
                    me.adopt_ownership(file, generation, epoch).await;
                });
            }
            Effect::SessionExpired { node, .. } => {
                // That incarnation's handles are gone: drop its writers.
                let owned: Vec<(FileId, Arc<Owned>)> = self
                    .owned
                    .lock()
                    .iter()
                    .map(|(f, o)| (*f, o.clone()))
                    .collect();
                let me = self.arc();
                for (file, o) in owned {
                    let removed = {
                        let mut p = o.participants.lock();
                        let before = p.len();
                        p.retain(|(n, _)| n != node);
                        before != p.len()
                    };
                    if removed {
                        me.schedule_finalize(file);
                    }
                }
            }
            _ => {}
        }
    }

    /// Revocation (ADR-015): every other node acknowledges that it applied
    /// the grant at `index` and has no older read of `file` in progress, or
    /// we wait out its read lease.
    async fn fence_cluster(
        &self,
        file: FileId,
        generation: Generation,
        index: u64,
        granted: Instant,
    ) {
        let members: Vec<NodeId> = {
            let m = self.d.meta().metrics().membership_config;
            m.nodes()
                .map(|(id, _)| NodeId(*id))
                .filter(|n| *n != self.me())
                .collect()
        };
        let lease = self.d.lease();
        let req = DataReq::Fence {
            file,
            generation,
            index,
        };
        let acks = futures::future::join_all(members.iter().map(|n| {
            let req = &req;
            async move { remote::call(self.rpc(), *n, req, lease).await.is_ok() }
        }))
        .await;
        self.fence_local(file).await;
        if acks.iter().any(|ok| !ok) {
            // Unreachable nodes stop serving when their lease lapses.
            let until = granted + lease + Duration::from_millis(50);
            tokio::time::sleep_until(until.into()).await;
        }
    }

    /// Wait for this node's in-flight reads of `file` and drop page caches.
    async fn fence_local(&self, file: FileId) {
        loop {
            let notified = self.inflight_done.notified();
            if !self.inflight.lock().contains_key(&file) {
                break;
            }
            let _ = tokio::time::timeout(Duration::from_millis(50), notified).await;
        }
        let hooks = self.fence_hooks.lock().clone();
        if !hooks.is_empty() {
            let _ = tokio::task::spawn_blocking(move || {
                for h in hooks {
                    h(file);
                }
            })
            .await;
        }
    }

    /// Create the owner state for a grant (whoever proposed it) and settle it
    /// if no writer turns up.
    async fn adopt_ownership(&self, file: FileId, generation: Generation, epoch: Epoch) {
        match self.owned_state(file, generation, epoch).await {
            Ok(_) => self.schedule_finalize(file),
            Err(e) => tracing::debug!(%file, error = %e, "ownership ended before adoption"),
        }
    }

    // ------------------------------------------------------------ attributes

    /// Attributes as a caller on this node should see them: while a file is
    /// owned, size and mtime come from the owner's working object.
    pub async fn getattr(&self, file: FileId) -> NestResult<FileAttr> {
        let mut a = self.raw_attr(file)?;
        self.overlay_owned(&mut a).await;
        Ok(a)
    }

    /// `getattr` answered from this host's own state, without waiting:
    /// `None` when another host owns the file, whose live size takes a call
    /// (use `getattr`). Lets FUSE answer on the thread the request arrived on.
    pub fn getattr_now(&self, file: FileId) -> NestResult<Option<FileAttr>> {
        let mut a = self.raw_attr(file)?;
        Ok(self.overlay_local(&mut a).then_some(a))
    }

    /// `lookup` answered from this host's own state; `None` as for
    /// `getattr_now`.
    pub fn lookup_now(&self, parent: FileId, name: &[u8]) -> NestResult<Option<FileAttr>> {
        let mut a = self
            .q(|c| query::lookup_attr(c, parent, name))?
            .ok_or(NestError::NotFound)?;
        Ok(self.overlay_local(&mut a).then_some(a))
    }

    /// Overlay an owned file's live size and mtime when this host owns it.
    /// False when another host does: only it knows them.
    fn overlay_local(&self, a: &mut FileAttr) -> bool {
        let (GenState::Owned, Some(owner)) = (a.gen_state, a.owner) else {
            return true;
        };
        if owner != self.me() {
            return false;
        }
        if let Ok((size, mtime)) = self.d.store().stat(ObjectKey::new(a.id, a.generation)) {
            a.size = size;
            a.mtime = mtime;
        }
        true
    }

    async fn overlay_owned(&self, a: &mut FileAttr) {
        if self.overlay_local(a) {
            return;
        }
        let Some(owner) = a.owner else {
            return;
        };
        let r = remote::call(
            self.rpc(),
            owner,
            &DataReq::Stat { file: a.id },
            Duration::from_secs(1),
        )
        .await;
        if let Ok(DataResp::Stat {
            generation,
            size,
            mtime,
        }) = r
            && generation == a.generation
        {
            a.size = size;
            a.mtime = mtime;
        }
    }

    pub async fn lookup(&self, parent: FileId, name: &[u8]) -> NestResult<FileAttr> {
        let mut a = self
            .q(|c| query::lookup_attr(c, parent, name))?
            .ok_or(NestError::NotFound)?;
        self.overlay_owned(&mut a).await;
        Ok(a)
    }

    pub fn readlink(&self, file: FileId) -> NestResult<Vec<u8>> {
        self.q(|c| query::readlink(c, file))?
            .ok_or(NestError::Invalid("not a symlink".into()))
    }

    /// Directory entries after `offset`. Offsets 1 and 2 are "." and "..";
    /// entry offsets are stable cookies (+2), valid across concurrent changes.
    pub fn readdir(
        &self,
        dir: FileId,
        offset: u64,
        limit: usize,
    ) -> NestResult<Vec<(u64, DirEntry)>> {
        let a = self.raw_attr(dir)?;
        if a.kind != FileKind::Directory {
            return Err(NestError::NotDir);
        }
        let mut out = Vec::new();
        if offset < 1 {
            out.push((
                1,
                DirEntry {
                    name: b".".to_vec(),
                    id: dir,
                    kind: FileKind::Directory,
                },
            ));
        }
        if offset < 2 {
            let parent = self
                .q(|c| query::dir_parent(c, dir))?
                .unwrap_or(FileId::ROOT);
            out.push((
                2,
                DirEntry {
                    name: b"..".to_vec(),
                    id: parent,
                    kind: FileKind::Directory,
                },
            ));
        }
        let after = offset.saturating_sub(2);
        for (cookie, e) in self.q(|c| query::readdir(c, dir, after, limit))? {
            out.push((cookie + 2, e));
        }
        Ok(out)
    }

    /// `readdir` with each entry's attributes (READDIRPLUS).
    pub fn readdir_plus(
        &self,
        dir: FileId,
        offset: u64,
        limit: usize,
    ) -> NestResult<Vec<DirEntryPlus>> {
        let a = self.raw_attr(dir)?;
        if a.kind != FileKind::Directory {
            return Err(NestError::NotDir);
        }
        let mut out = Vec::new();
        if offset < 2 {
            let parent = self
                .q(|c| query::dir_parent(c, dir))?
                .unwrap_or(FileId::ROOT);
            let pa = self.raw_attr(parent)?;
            if offset < 1 {
                out.push(DirEntryPlus::new(1, b".", a));
            }
            out.push(DirEntryPlus::new(2, b"..", pa));
        }
        let after = offset.saturating_sub(2);
        for (cookie, name, mut e) in self.q(|c| query::readdir_attrs(c, dir, after, limit))? {
            let fresh = self.overlay_local(&mut e);
            out.push(DirEntryPlus {
                offset: cookie + 2,
                name,
                attr: e,
                fresh,
            });
        }
        Ok(out)
    }

    // ------------------------------------------------------------ namespace

    fn attr_reply(r: Reply) -> NestResult<FileAttr> {
        match r {
            Reply::Attr(a) => Ok(a),
            other => Err(NestError::Io(format!("unexpected reply {other:?}"))),
        }
    }

    pub async fn mkdir(&self, parent: FileId, name: &[u8], perm: u32) -> NestResult<FileAttr> {
        Self::attr_reply(
            self.propose(Command::Mkdir {
                parent,
                name: name.to_vec(),
                perm,
                now: Timestamp::now(),
            })
            .await?,
        )
    }

    pub async fn symlink(
        &self,
        parent: FileId,
        name: &[u8],
        target: &[u8],
    ) -> NestResult<FileAttr> {
        Self::attr_reply(
            self.propose(Command::Symlink {
                parent,
                name: name.to_vec(),
                target: target.to_vec(),
                now: Timestamp::now(),
            })
            .await?,
        )
    }

    pub async fn link(&self, file: FileId, parent: FileId, name: &[u8]) -> NestResult<FileAttr> {
        let mut a = Self::attr_reply(
            self.propose(Command::Link {
                file,
                parent,
                name: name.to_vec(),
                now: Timestamp::now(),
            })
            .await?,
        )?;
        self.overlay_owned(&mut a).await;
        Ok(a)
    }

    pub async fn unlink(&self, parent: FileId, name: &[u8]) -> NestResult<()> {
        self.propose(Command::Unlink {
            parent,
            name: name.to_vec(),
            now: Timestamp::now(),
        })
        .await?;
        Ok(())
    }

    pub async fn rmdir(&self, parent: FileId, name: &[u8]) -> NestResult<()> {
        self.propose(Command::Rmdir {
            parent,
            name: name.to_vec(),
            now: Timestamp::now(),
        })
        .await?;
        Ok(())
    }

    /// Remove `name` from `parent`, a whole tree with `recursive`, many
    /// entries per Raft commit (children before their directory). One
    /// POSIX unlink per file costs a commit each; this costs one per
    /// `REMOVE_BATCH` entries. With `dry_run` nothing changes and the report
    /// counts what would go.
    pub async fn remove_tree(
        &self,
        parent: FileId,
        name: &[u8],
        recursive: bool,
        dry_run: bool,
    ) -> NestResult<RemoveReport> {
        const REMOVE_BATCH: usize = 512;
        let root = self
            .q(|c| query::lookup(c, parent, name))?
            .ok_or(NestError::NotFound)?;
        let root_kind = self.raw_attr(root)?.kind;
        let root_name = String::from_utf8_lossy(name).into_owned();
        // Post-order: every directory after everything inside it.
        let mut order: Vec<(FileId, Vec<u8>, bool, String)> = Vec::new();
        if root_kind == FileKind::Directory {
            if !recursive {
                return Err(NestError::Invalid(format!(
                    "{root_name} is a directory (use recursive)"
                )));
            }
            // (dir, its parent, its name, its path, children listed?)
            let mut stack = vec![(root, parent, name.to_vec(), root_name.clone(), false)];
            while let Some((dir, dparent, dname, dpath, listed)) = stack.pop() {
                if listed {
                    order.push((dparent, dname, true, dpath));
                    continue;
                }
                stack.push((dir, dparent, dname, dpath.clone(), true));
                let mut after = 0u64;
                loop {
                    let page = self.q(|c| query::readdir(c, dir, after, 1024))?;
                    if page.is_empty() {
                        break;
                    }
                    for (cookie, e) in page {
                        after = cookie;
                        let path = format!("{dpath}/{}", String::from_utf8_lossy(&e.name));
                        if e.kind == FileKind::Directory {
                            stack.push((e.id, dir, e.name, path, false));
                        } else {
                            order.push((dir, e.name, false, path));
                        }
                    }
                }
            }
        } else {
            order.push((parent, name.to_vec(), false, root_name));
        }
        let mut report = RemoveReport::default();
        if dry_run {
            for (_, _, is_dir, _) in &order {
                if *is_dir {
                    report.dirs += 1;
                } else {
                    report.files += 1;
                }
            }
            return Ok(report);
        }
        for chunk in order.chunks(REMOVE_BATCH) {
            let now = Timestamp::now();
            let cmds = chunk
                .iter()
                .map(|(p, n, is_dir, _)| {
                    if *is_dir {
                        Command::Rmdir {
                            parent: *p,
                            name: n.clone(),
                            now,
                        }
                    } else {
                        Command::Unlink {
                            parent: *p,
                            name: n.clone(),
                            now,
                        }
                    }
                })
                .collect();
            let results = match self.propose(Command::Batch(cmds)).await? {
                Reply::Batch(r) => r,
                other => return Err(NestError::Io(format!("unexpected {other:?}"))),
            };
            for ((_, _, is_dir, path), r) in chunk.iter().zip(results) {
                match r {
                    Ok(_) if *is_dir => report.dirs += 1,
                    Ok(_) => report.files += 1,
                    Err(e) => report.errors.push(format!("{path}: {e}")),
                }
            }
        }
        Ok(report)
    }

    pub async fn rename(
        &self,
        parent: FileId,
        name: &[u8],
        new_parent: FileId,
        new_name: &[u8],
        flags: RenameFlags,
    ) -> NestResult<()> {
        self.propose(Command::Rename {
            parent,
            name: name.to_vec(),
            new_parent,
            new_name: new_name.to_vec(),
            flags,
            now: Timestamp::now(),
        })
        .await?;
        Ok(())
    }

    /// Change permissions, times, and/or size. A size change is a content
    /// mutation and goes through ownership.
    pub async fn setattr(
        &self,
        file: FileId,
        perm: Option<u32>,
        size: Option<u64>,
        atime: Option<Timestamp>,
        mtime: Option<Timestamp>,
        fh: Option<u64>,
    ) -> NestResult<FileAttr> {
        if let Some(size) = size {
            self.truncate(file, size, fh).await?;
        }
        if perm.is_some() || atime.is_some() || mtime.is_some() {
            self.propose(Command::SetAttr {
                file,
                perm,
                atime,
                mtime,
                now: Timestamp::now(),
            })
            .await?;
        }
        if let Some(mtime) = mtime {
            // While this node owns the file its mtime comes from the working
            // object: keep that in step so the change survives finalize.
            let a = self.raw_attr(file)?;
            if a.gen_state == GenState::Owned && a.owner == Some(self.me()) {
                let key = ObjectKey::new(file, a.generation);
                let f = self.d.store().open_write(key).map_err(io)?;
                f.set_times(std::fs::FileTimes::new().set_modified(mtime.as_system_time()))
                    .map_err(io)?;
            }
        }
        self.getattr(file).await
    }

    pub async fn seal(&self, file: FileId, sealed: bool) -> NestResult<FileAttr> {
        Self::attr_reply(
            self.propose(Command::Seal {
                file,
                sealed,
                now: Timestamp::now(),
            })
            .await?,
        )
    }

    // ------------------------------------------------------------ ownership

    /// Owner state for `(file, generation, epoch)` on this node, creating it
    /// from the prepared working object if needed.
    async fn owned_state(
        &self,
        file: FileId,
        generation: Generation,
        epoch: Epoch,
    ) -> NestResult<Arc<Owned>> {
        let existing = self.owned.lock().get(&file).cloned();
        if let Some(o) = existing
            && o.epoch == epoch
        {
            return Ok(o);
        }
        let key = ObjectKey::new(file, generation);
        tokio::time::timeout(Duration::from_secs(10), self.d.wait_ready(key))
            .await
            .map_err(|_| NestError::Unavailable("working object not ready".into()))?;
        let store = self.d.store().clone();
        let f = tokio::task::spawn_blocking(move || store.open_write(key))
            .await
            .expect("blocking task")
            .map_err(io)?;
        let fenced = self
            .fences
            .lock()
            .get(&(file, epoch))
            .cloned()
            // No grant observed by this process (e.g. restarted): fencing
            // already happened before the restart or startup settles it.
            .unwrap_or_else(|| watch::channel(true).1);
        let o = Arc::new(Owned {
            key,
            epoch,
            file: Arc::new(f),
            participants: Mutex::new(HashSet::new()),
            open: tokio::sync::RwLock::new(true),
            activity: AtomicU64::new(0),
            append: Mutex::new(()),
            fenced,
        });
        let mut owned = self.owned.lock();
        let entry = owned.entry(file).or_insert_with(|| o.clone());
        if entry.epoch != epoch {
            *entry = o;
        }
        Ok(entry.clone())
    }

    /// Where mutations of `file` must go right now, taking ownership if the
    /// file is STABLE. `discard` means the old content is not needed
    /// (O_TRUNC, truncate to zero), so this node can own it without a copy.
    async fn route_mutation(
        &self,
        file: FileId,
        discard: bool,
    ) -> NestResult<(NodeId, Generation, Epoch)> {
        for _ in 0..16 {
            let a = self.raw_attr(file)?;
            match a.kind {
                FileKind::Regular => {}
                FileKind::Directory => return Err(NestError::IsDir),
                FileKind::Symlink => return Err(NestError::Invalid("not a regular file".into())),
            }
            if let (GenState::Owned, Some(owner)) = (a.gen_state, a.owner) {
                return Ok((owner, a.generation, a.epoch));
            }
            if a.sealed {
                return Err(NestError::NotPermitted("file is sealed".into()));
            }
            let owner = match self.choose_owner(&a, discard).await {
                Ok(o) => o,
                // Another host was granted ownership between our two
                // metadata reads (its grant retires the copies we looked
                // for): read the attributes again.
                Err(NestError::Unavailable(_)) if self.attr_moved(&a)? => continue,
                Err(e) => return Err(e),
            };
            let r = self
                .propose(Command::AcquireOwner {
                    file,
                    node: owner,
                    expect_gen: a.generation,
                    truncate: discard && owner == self.me(),
                    now: Timestamp::now(),
                })
                .await;
            match r {
                Ok(_) | Err(NestError::Stale) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(NestError::Busy("ownership kept changing".into()))
    }

    /// Whether `file`'s generation or its state changed since `a` was read.
    fn attr_moved(&self, a: &FileAttr) -> NestResult<bool> {
        let now = self.raw_attr(a.id)?;
        Ok(now.generation != a.generation || now.gen_state != a.gen_state)
    }

    /// Prefer this node (it holds the content, or the content is being
    /// discarded); otherwise a reachable node holding a live copy, so a large
    /// file is never moved just to edit it.
    async fn choose_owner(&self, a: &FileAttr, discard: bool) -> NestResult<NodeId> {
        let me = self.me();
        if discard || a.size == 0 || self.d.servable(ObjectKey::new(a.id, a.generation)) {
            return Ok(me);
        }
        // Held only on one of this host's disks: that copy becomes the
        // working object once it is in the live store (ADR-073).
        if self.holds_live(a) && self.pull_to_live(a).await.is_ok() {
            return Ok(me);
        }
        for h in self.holders(a)? {
            if self.call(h, &DataReq::Ping).await.is_ok() {
                return Ok(h);
            }
        }
        Err(NestError::Unavailable(
            "no reachable node holds this file".into(),
        ))
    }

    /// Nodes holding a live copy of the current generation, this node last,
    /// rotated by file id to spread load.
    /// The hosts this node would read `file` from, in order. For tests and
    /// diagnostics.
    #[doc(hidden)]
    pub fn read_order(&self, file: FileId) -> NestResult<Vec<NodeId>> {
        self.holders(&self.raw_attr(file)?)
    }

    /// The file at namespace path `path`, following symlinks (a snapshot
    /// entry resolves to its blob).
    pub fn resolve_path(&self, path: &str) -> NestResult<Option<FileId>> {
        self.q(|c| query::resolve_follow(c, path.as_bytes()))
    }

    /// Open `generation` of `file` for a push source to read (ADR-063): the
    /// same checks as serving a read, and the object stays open for later
    /// reads of it. Blocking.
    pub fn push_open(&self, file: FileId, generation: Generation) -> NestResult<PushObject> {
        let buffered = self.serve_file(file, generation)?;
        // The same object, direct, where the store's filesystem allows it.
        let direct = self.raw_attr(file).ok().and_then(|a| {
            let src = self.local_source(&a)?;
            src.open_read_direct(ObjectKey::new(file, generation)).ok()
        });
        Ok(PushObject {
            buffered,
            direct,
            size: self.raw_attr(file)?.size,
        })
    }

    /// Where a push load can read `a`'s generation (ADR-063): each live
    /// copy as (store, the host that reads it, how far: 0 a fabric host's
    /// store or this host's, 1 an archive, 2 an off-grid host's). An
    /// archive is read by this host when it is a gateway, else by its
    /// first gateway. This host's own copy counts only if it can serve it.
    pub fn push_holders(&self, a: &FileAttr) -> NestResult<Vec<(nest_types::StoreId, NodeId, u8)>> {
        let me = self.me();
        let offgrid = self.offgrid_hosts();
        let replicas = self.q(|c| query::replicas(c, a.id))?;
        let mut out = Vec::new();
        for r in replicas
            .iter()
            .filter(|r| r.generation == a.generation && r.state == nest_types::ReplicaState::Live)
        {
            if crate::is_archive(r.store) {
                let Some((_, cfg)) = self.d.archive_row(r.store) else {
                    continue;
                };
                let host = if cfg.gateways.contains(&me) {
                    Some(me)
                } else {
                    cfg.gateways.first().copied()
                };
                if let Some(h) = host {
                    out.push((r.store, h, 1));
                }
                continue;
            }
            let Some(host) = r.store.host() else { continue };
            if host == me
                && self
                    .d
                    .servable_local(ObjectKey::new(a.id, a.generation))
                    .is_none_or(|(s, _)| s != r.store)
            {
                continue;
            }
            out.push((r.store, host, if offgrid.contains(&host) { 2 } else { 0 }));
        }
        Ok(out)
    }

    /// Open `generation` of `file` from store `store` of this host (its own,
    /// or an archive it is a gateway of) for a push source; as `push_open`.
    pub fn push_open_from(
        &self,
        store: nest_types::StoreId,
        file: FileId,
        generation: Generation,
    ) -> NestResult<PushObject> {
        if store == self.me().live_store() {
            return self.push_open(file, generation);
        }
        if store.is_disk() {
            let a = self.raw_attr(file)?;
            if a.generation != generation {
                return Err(NestError::Stale);
            }
            let key = ObjectKey::new(file, generation);
            let st = match self.d.servable_local(key) {
                Some((s, st)) if s == store => st,
                _ => {
                    return Err(NestError::Unavailable(
                        "that disk does not serve this copy here".into(),
                    ));
                }
            };
            let buffered = Arc::new(st.open_read(key).map_err(io)?);
            let direct = st.open_read_direct(key).ok();
            return Ok(PushObject {
                buffered,
                direct,
                size: a.size,
            });
        }
        let a = self.raw_attr(file)?;
        if a.generation != generation {
            return Err(NestError::Stale);
        }
        let key = ObjectKey::new(file, generation);
        let arch = self.d.servable_archive(store, key).ok_or_else(|| {
            NestError::Unavailable("that archive does not serve this copy here".into())
        })?;
        let buffered = Arc::new(arch.store.open_read(key).map_err(io)?);
        let direct = arch.store.open_read_direct(key).ok();
        Ok(PushObject {
            buffered,
            direct,
            size: a.size,
        })
    }

    /// Whether this host's own store holds a live copy of `a`'s generation.
    pub fn holds_live(&self, a: &FileAttr) -> bool {
        self.d
            .servable_local(ObjectKey::new(a.id, a.generation))
            .is_some()
    }

    /// Where `file`'s current generation can be read from: every live copy,
    /// named, with what kind of store holds it. Read-only; for the mount's
    /// placement attributes.
    pub fn copies(&self, file: FileId) -> NestResult<(FileAttr, Vec<Copy>)> {
        let a = self.raw_attr(file)?;
        let (replicas, stores) = self.q(|c| Ok((query::replicas(c, a.id)?, query::stores(c)?)))?;
        let offgrid = self.offgrid_hosts();
        let mut out: Vec<Copy> = replicas
            .into_iter()
            .filter(|r| r.generation == a.generation && r.state == nest_types::ReplicaState::Live)
            .filter_map(|r| {
                let s = stores.iter().find(|s| s.id == r.store)?;
                let host = r.store.host();
                let kind = if host.is_none() {
                    CopyKind::Archive
                } else if host == Some(self.me()) {
                    CopyKind::Local
                } else if host.is_some_and(|n| offgrid.contains(&n)) {
                    CopyKind::Offgrid
                } else {
                    CopyKind::Fabric
                };
                Some(Copy {
                    store: s.name.clone(),
                    kind,
                })
            })
            .collect();
        out.sort_by(|x, y| (x.kind, &x.store).cmp(&(y.kind, &y.store)));
        Ok((a, out))
    }

    /// The cluster as metadata knows it, without asking any host: members
    /// (this host marked, off-grid ones flagged), the leader, and archive
    /// stores with their gateways. For the mount root's attribute.
    pub fn cluster(&self) -> NestResult<serde_json::Value> {
        let stores = self.q(query::stores)?;
        let m = self.d.meta().metrics().membership_config;
        let offgrid = self.offgrid_hosts();
        let name = |n: NodeId| {
            stores
                .iter()
                .find(|s| s.id == n.live_store())
                .map_or_else(|| format!("node{}", n.0), |s| s.name.clone())
        };
        let hosts: Vec<_> = m
            .nodes()
            .map(|(id, _)| {
                let n = NodeId(*id);
                serde_json::json!({
                    "name": name(n),
                    "node": n.0,
                    "offgrid": offgrid.contains(&n),
                    "me": n == self.me(),
                })
            })
            .collect();
        let archives: Vec<_> = stores
            .iter()
            .filter(|s| crate::is_archive(s.id))
            .map(|s| {
                let gateways: Vec<String> =
                    serde_json::from_str::<crate::local::ArchiveConfig>(&s.config)
                        .map(|c| c.gateways.into_iter().map(name).collect())
                        .unwrap_or_default();
                serde_json::json!({"name": s.name, "gateways": gateways})
            })
            .collect();
        Ok(serde_json::json!({
            "me": name(self.me()),
            "leader": self.d.meta().leader().map(name),
            "hosts": hosts,
            "archives": archives,
        }))
    }

    fn holders(&self, a: &FileAttr) -> NestResult<Vec<NodeId>> {
        let replicas: Vec<_> = self
            .q(|c| query::replicas(c, a.id))?
            .into_iter()
            .filter(|r| r.generation == a.generation && r.state == nest_types::ReplicaState::Live)
            .collect();
        let mut nodes: Vec<NodeId> = replicas
            .iter()
            .filter_map(|r| r.store.host())
            .filter(|n| *n != self.me())
            .collect();
        // A host with copies on two of its disks is one holder.
        nodes.sort();
        nodes.dedup();
        if !nodes.is_empty() {
            let k = (a.id.0 as usize) % nodes.len();
            nodes.rotate_left(k);
        }
        // Off-grid holders answer over TCP: after every fabric holder (a
        // stable sort keeps the rotation within each group).
        let offgrid = self.offgrid_hosts();
        nodes.sort_by_key(|n| offgrid.contains(n));
        // Archive copies are read through their gateways, after live copies.
        for r in replicas.iter().filter(|r| crate::is_archive(r.store)) {
            if let Some((_, cfg)) = self.d.archive_row(r.store) {
                for g in cfg.gateways {
                    if g != self.me() && !nodes.contains(&g) {
                        nodes.push(g);
                    }
                }
            }
        }
        Ok(nodes)
    }

    /// Finalize after the linger unless someone writes or joins meanwhile.
    fn schedule_finalize(&self, file: FileId) {
        let Some(o) = self.owned.lock().get(&file).cloned() else {
            return;
        };
        if !o.participants.lock().is_empty() {
            return;
        }
        let seen = o.activity.load(Ordering::SeqCst);
        let me = self.arc();
        tokio::spawn(async move {
            tokio::time::sleep(me.cfg.finalize_linger).await;
            let idle = |o: &Owned| {
                o.participants.lock().is_empty() && o.activity.load(Ordering::SeqCst) == seen
            };
            if !idle(&o) {
                return;
            }
            let mut open = o.open.write().await;
            if !*open || !idle(&o) {
                return;
            }
            *open = false;
            let drop_state = |me: &Vfs| {
                let mut owned = me.owned.lock();
                if owned.get(&file).is_some_and(|x| Arc::ptr_eq(x, &o)) {
                    owned.remove(&file);
                }
                me.fences.lock().remove(&(file, o.epoch));
            };
            // A stable file must be durable on its holder (ADR-026).
            let f = o.file.clone();
            if let Err(e) = tokio::task::spawn_blocking(move || f.sync_data())
                .await
                .expect("blocking task")
            {
                tracing::error!(key = ?o.key, error = %e, "fdatasync before finalize failed");
                *open = true;
                return;
            }
            let (size, mtime) = match me.d.store().stat(o.key) {
                Ok(s) => s,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    drop_state(&me); // deleted while lingering
                    return;
                }
                Err(e) => {
                    tracing::error!(key = ?o.key, error = %e, "stat before finalize failed");
                    *open = true;
                    return;
                }
            };
            let r = me
                .propose(Command::Finalize {
                    file,
                    epoch: o.epoch,
                    size,
                    mtime,
                    now: Timestamp::now(),
                })
                .await;
            drop_state(&me);
            if let Err(e) = r
                && !matches!(e, NestError::NotFound | NestError::Stale)
            {
                tracing::warn!(%file, error = %e, "finalize failed; startup reconciliation will settle it");
            }
        });
    }

    // ------------------------------------------------------------ owner side

    /// Apply a mutation as owner. `writer` joins the epoch.
    async fn owner_mutate(
        &self,
        file: FileId,
        epoch: Epoch,
        writer: Option<Writer>,
        op: OwnerOp,
    ) -> NestResult<DataResp> {
        let a = self.raw_attr(file)?;
        if a.gen_state != GenState::Owned || a.owner != Some(self.me()) || a.epoch != epoch {
            return Err(NestError::Stale);
        }
        let o = self.owned_state(file, a.generation, epoch).await?;
        let mut fenced = o.fenced.clone();
        fenced
            .wait_for(|f| *f)
            .await
            .map_err(|_| NestError::Unavailable("fence abandoned".into()))?;
        if let Some(w) = writer {
            o.participants.lock().insert(w);
        }
        let guard = o.open.read().await;
        if !*guard {
            return Err(NestError::Stale); // finalized under us
        }
        if let OwnerOp::Write { data, .. } = &op {
            self.d.store().reserve_room(data.len() as u64).map_err(io)?;
        }
        o.activity.fetch_add(1, Ordering::SeqCst);
        let f = o.file.clone();
        let o2 = o.clone();
        let resp = tokio::task::spawn_blocking(move || -> std::io::Result<DataResp> {
            match op {
                OwnerOp::Write {
                    append: true, data, ..
                } => {
                    // The owner chooses an append's offset: the end, now.
                    let _end = o2.append.lock();
                    f.write_all_at(&data, f.metadata()?.len())?;
                    Ok(DataResp::Written(data.len() as u32))
                }
                OwnerOp::Write { offset, data, .. } => {
                    f.write_all_at(&data, offset)?;
                    Ok(DataResp::Written(data.len() as u32))
                }
                OwnerOp::Truncate(size) => {
                    f.set_len(size)?;
                    Ok(DataResp::Done)
                }
                OwnerOp::Fsync { data_only } => {
                    if data_only {
                        f.sync_data()?;
                    } else {
                        f.sync_all()?;
                    }
                    Ok(DataResp::Done)
                }
            }
        })
        .await
        .expect("blocking task")
        .map_err(io)?;
        drop(guard);
        if writer.is_none() && matches!(resp, DataResp::Done) && o.participants.lock().is_empty() {
            // truncate(2) without a handle: nothing will close it.
            self.schedule_finalize(file);
        }
        Ok(resp)
    }

    /// A write from another host, however it arrived (the control
    /// connection, an RDMA pull, a TCP lane): one path, one set of checks.
    async fn owner_write(&self, op: nest_fabric::WriteOp, data: bytes::Bytes) -> NestResult<u32> {
        let resp = self
            .owner_mutate(
                op.file,
                op.epoch,
                Some(op.writer),
                OwnerOp::Write {
                    offset: op.offset,
                    append: op.append,
                    data,
                },
            )
            .await?;
        match resp {
            DataResp::Written(n) => Ok(n),
            other => Err(NestError::Io(format!("unexpected response {other:?}"))),
        }
    }

    async fn owner_leave(&self, file: FileId, epoch: Epoch, writer: Writer) {
        let o = self.owned.lock().get(&file).cloned();
        if let Some(o) = o
            && o.epoch == epoch
            && o.participants.lock().remove(&writer)
        {
            self.schedule_finalize(file);
        }
    }

    async fn owner_fsync(&self, file: FileId, epoch: Epoch, data_only: bool) -> NestResult<()> {
        self.owner_mutate(file, epoch, None, OwnerOp::Fsync { data_only })
            .await?;
        let o = self
            .owned
            .lock()
            .get(&file)
            .cloned()
            .ok_or(NestError::Stale)?;
        let (size, mtime) = self.d.store().stat(o.key).map_err(io)?;
        match self
            .propose(Command::SyncOwned {
                file,
                epoch,
                size,
                mtime,
            })
            .await
        {
            Ok(_) | Err(NestError::Stale) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Serve another node's data request.
    pub(crate) async fn serve(&self, _peer: NodeId, req: DataReq) -> DataResp {
        let r = match req {
            DataReq::Read {
                file,
                generation,
                offset,
                len,
            } => self
                .serve_read(file, generation, offset, len)
                .await
                .map(DataResp::Data),
            DataReq::Write {
                file,
                epoch,
                writer,
                offset,
                append,
                data,
            } => self
                .owner_write(
                    nest_fabric::WriteOp {
                        file,
                        epoch,
                        writer,
                        offset,
                        append,
                    },
                    data.into(),
                )
                .await
                .map(DataResp::Written),
            DataReq::Truncate {
                file,
                epoch,
                writer,
                size,
            } => {
                self.owner_mutate(file, epoch, writer, OwnerOp::Truncate(size))
                    .await
            }
            DataReq::Leave {
                file,
                epoch,
                writer,
            } => {
                self.owner_leave(file, epoch, writer).await;
                Ok(DataResp::Done)
            }
            DataReq::Fsync {
                file,
                epoch,
                data_only,
            } => self
                .owner_fsync(file, epoch, data_only)
                .await
                .map(|_| DataResp::Done),
            DataReq::Stat { file } => self.serve_stat(file),
            DataReq::Fence {
                file,
                generation: _,
                index,
            } => {
                let applied =
                    tokio::time::timeout(self.d.lease(), self.d.meta().wait_applied(index)).await;
                match applied {
                    Ok(Ok(())) => {
                        self.fence_local(file).await;
                        Ok(DataResp::Done)
                    }
                    _ => Err(NestError::Unavailable("not caught up".into())),
                }
            }
            DataReq::Ping => Ok(DataResp::Done),
        };
        r.unwrap_or_else(DataResp::Err)
    }

    fn serve_stat(&self, file: FileId) -> NestResult<DataResp> {
        let a = self.raw_attr(file)?;
        if a.gen_state != GenState::Owned || a.owner != Some(self.me()) {
            return Err(NestError::Stale);
        }
        let (size, mtime) = self
            .d
            .store()
            .stat(ObjectKey::new(file, a.generation))
            .map_err(io)?;
        Ok(DataResp::Stat {
            generation: a.generation,
            size,
            mtime,
        })
    }

    fn track(&self, file: FileId) -> Inflight<'_> {
        *self.inflight.lock().entry(file).or_default() += 1;
        Inflight { vfs: self, file }
    }

    /// The object holding exactly `generation` of `file`, if this node may
    /// serve it right now (the rule every remote read goes through).
    /// Where this node can read `a`'s current generation locally: its live
    /// store (owned here, or a servable live copy) or an archive store it is
    /// a gateway for.
    fn local_source(&self, a: &FileAttr) -> Option<Arc<nest_store::ObjectStore>> {
        self.local_store(a).map(|(_, st)| st)
    }

    /// `local_source`, with the store's id.
    fn local_store(
        &self,
        a: &FileAttr,
    ) -> Option<(nest_types::StoreId, Arc<nest_store::ObjectStore>)> {
        let key = ObjectKey::new(a.id, a.generation);
        match (a.gen_state, a.owner) {
            (GenState::Owned, Some(o)) if o == self.me() => {
                Some((self.me().live_store(), self.d.store().clone()))
            }
            (GenState::Stable, _) => {
                // The live store, or one of this host's disks (ADR-073).
                self.queries.fetch_add(1, Ordering::Relaxed);
                if let Some(local) = self.d.servable_local(key) {
                    return Some(local);
                }
                let replicas = self.q(|c| query::replicas(c, a.id)).ok()?;
                replicas
                    .into_iter()
                    .filter(|r| r.generation == a.generation && crate::is_archive(r.store))
                    .find_map(|r| {
                        let arch = self.d.servable_archive(r.store, key)?;
                        Some((r.store, arch.store.clone()))
                    })
            }
            _ => None,
        }
    }

    /// Where this node reads `a` for itself: its live store, or a local
    /// archive copy only when no other node holds a live one. A live copy
    /// over the fabric (5–15 GB/s) beats an archive disk (a SATA SSD or a
    /// NAS share); the archive stays the fallback when those holders fail.
    fn own_read_source(&self, a: &FileAttr) -> Option<Arc<nest_store::ObjectStore>> {
        self.own_read_store(a).map(|(_, st)| st)
    }

    fn own_read_store(
        &self,
        a: &FileAttr,
    ) -> Option<(nest_types::StoreId, Arc<nest_store::ObjectStore>)> {
        let (id, src) = self.local_store(a)?;
        if self.is_own_disk(&src) || !self.live_elsewhere(a) {
            Some((id, src))
        } else {
            None
        }
    }

    /// `own_read_source` for a read through `h`, without metadata queries
    /// while nothing about any copy changed: the handle remembers the
    /// answer per generation (`LocalPick`), and a remembered copy is
    /// re-checked in memory (lease, fences, its disk still mounted) on
    /// every read.
    fn read_source(&self, h: &Handle, a: &FileAttr) -> Option<Arc<nest_store::ObjectStore>> {
        if a.gen_state != GenState::Stable {
            return self.own_read_source(a);
        }
        let key = ObjectKey::new(a.id, a.generation);
        let epoch = self.replica_epoch.load(Ordering::SeqCst);
        let remembered = h
            .local
            .lock()
            .as_ref()
            .filter(|p| {
                p.generation == a.generation && p.epoch == epoch && p.at.elapsed() < ATTR_TTL
            })
            .map(|p| p.src.clone());
        match remembered {
            Some(None) => return None,
            Some(Some((id, st))) if self.still_local(id, key) => return Some(st),
            // Unmounted, fenced or the lease lapsed since: decide again.
            _ => {}
        }
        // The epoch is read before the queries: a change committed while
        // they run leaves this answer tagged with the older epoch.
        let src = self.own_read_store(a);
        *h.local.lock() = Some(LocalPick {
            generation: a.generation,
            epoch,
            at: std::time::Instant::now(),
            src: src.clone(),
        });
        src.map(|(_, st)| st)
    }

    /// A copy found servable may still be served: in memory only.
    fn still_local(&self, store: nest_types::StoreId, key: ObjectKey) -> bool {
        self.d.may_serve_now(store, key)
            && (store == self.me().live_store()
                || (store.is_disk() && self.d.disk(store).is_some())
                || (crate::is_archive(store) && self.d.archive(store).is_some()))
    }

    /// Metadata queries this VFS has run so far.
    pub fn metadata_queries(&self) -> u64 {
        self.queries.load(Ordering::Relaxed)
    }

    /// Whether `src` is this host's live store or one of its disks (not an
    /// archive it is a gateway for).
    fn is_own_disk(&self, src: &Arc<nest_store::ObjectStore>) -> bool {
        Arc::ptr_eq(src, self.d.store())
            || self.d.disks().iter().any(|d| Arc::ptr_eq(src, &d.store))
    }

    /// Whether another node holds a live (non-archive) copy of `a`'s generation.
    fn live_elsewhere(&self, a: &FileAttr) -> bool {
        let me = self.me();
        self.q(|c| query::replicas(c, a.id))
            .map(|rs| {
                rs.iter().any(|r| {
                    r.generation == a.generation
                        && r.state == nest_types::ReplicaState::Live
                        && r.store.host().is_some_and(|h| h != me)
                })
            })
            .unwrap_or(false)
    }

    /// The open object to serve `generation` of `file` from, cached for
    /// STABLE generations (a remote read used to cost two queries, a stat
    /// and an open per request).
    fn serve_file(&self, file: FileId, generation: Generation) -> NestResult<Arc<std::fs::File>> {
        let key = ObjectKey::new(file, generation);
        if let Some(f) = self.cached_serve_file(key) {
            return Ok(f);
        }
        let a = self.raw_attr(file)?;
        let f = Arc::new(self.servable_object(file, generation)?);
        if a.gen_state == GenState::Stable
            && let Some((store, _)) = self.d.servable_local(key)
        {
            let mut m = self.serve_files.lock();
            m.retain(|_, e| e.live());
            if m.len() >= SERVE_FILES_MAX {
                m.clear();
            }
            let now = std::time::Instant::now();
            m.insert(
                key,
                ServeEntry {
                    file: f.clone(),
                    store,
                    opened: now,
                    used: now,
                },
            );
        }
        Ok(f)
    }

    /// Close served objects not used within `SERVE_TTL`, or open longer
    /// than `SERVE_MAX_AGE`. An open file keeps its disk space after it is
    /// deleted: a spread import's staging copies (served to their new hosts,
    /// then removed here) once filled raptor's disk because entries were
    /// only swept past `SERVE_FILES_MAX`. Fencing closes them at once
    /// (`on_effect`); this catches anything else.
    fn sweep_serve_files(&self) {
        self.serve_files.lock().retain(|_, e| e.live());
    }

    /// Objects kept open for serving (for tests and diagnostics).
    pub fn serving_open(&self) -> usize {
        self.serve_files.lock().len()
    }

    fn cached_serve_file(&self, key: ObjectKey) -> Option<Arc<std::fs::File>> {
        let (f, store) = match self.serve_files.lock().get_mut(&key) {
            Some(e) if e.live() => {
                e.used = std::time::Instant::now();
                (e.file.clone(), e.store)
            }
            _ => return None,
        };
        self.d.may_serve_now(store, key).then_some(f)
    }

    fn servable_object(&self, file: FileId, generation: Generation) -> NestResult<std::fs::File> {
        let a = self.raw_attr(file)?;
        if a.generation != generation {
            return Err(NestError::Stale);
        }
        let key = ObjectKey::new(file, generation);
        let Some(src) = self.local_source(&a) else {
            return Err(NestError::Unavailable(if self.d.lease_valid() {
                "no copy of that generation here".into()
            } else {
                "not serving right now (this host could not confirm with the cluster in time)"
                    .into()
            }));
        };
        src.open_read(key).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                tracing::warn!(
                    file = file.0,
                    generation = generation.0,
                    "asked for a copy that vanished since the check"
                );
                NestError::Stale // invalidated since the check
            } else {
                io(e)
            }
        })
    }

    /// Read bytes of exactly `generation` from this node, if it may serve it.
    async fn serve_read(
        &self,
        file: FileId,
        generation: Generation,
        offset: u64,
        len: u32,
    ) -> NestResult<Vec<u8>> {
        let _t = self.track(file);
        let f = self.servable_object(file, generation)?;
        tokio::task::spawn_blocking(move || pread(&f, offset, len))
            .await
            .expect("blocking task")
    }

    // ------------------------------------------------------------ open/close

    fn new_handle(&self, file: FileId, flags: i32) -> u64 {
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        let acc = flags & oflags::ACCMODE;
        let h = Arc::new(Handle {
            file,
            writable: acc == oflags::WRONLY || acc == oflags::RDWR,
            append: flags & oflags::APPEND != 0,
            reader: Mutex::new(None),
            lock_owners: Mutex::new(HashSet::new()),
            remote: Mutex::new(None),
            read_bytes: Default::default(),
            route: Mutex::new(None),
            stable_attr: Mutex::new(None),
            local: Mutex::new(None),
        });
        self.handles.lock().insert(fh, h);
        self.d.handle_opened(file);
        fh
    }

    fn handle(&self, fh: u64) -> NestResult<Arc<Handle>> {
        self.handles
            .lock()
            .get(&fh)
            .cloned()
            .ok_or(NestError::Invalid("bad file handle".into()))
    }

    /// `pinned`: the file is marked always passthrough (ADR-053).
    fn choose_mode(&self, a: &FileAttr, writable: bool, pinned: bool) -> OpenMode {
        if a.sealed && !writable && a.gen_state == GenState::Stable {
            let key = ObjectKey::new(a.id, a.generation);
            let allowed = pinned
                || match self.cfg.passthrough {
                    // The daemon reads a local copy from local disk anyway
                    // (ADR-065): the kernel does it best.
                    Passthrough::Always | Passthrough::Sole => true,
                    Passthrough::Never => false,
                };
            if allowed
                && let Some((_, st)) = self.d.servable_local(key)
                && let Ok(f) = st.open_read(key)
            {
                return OpenMode::Passthrough(f);
            }
            return OpenMode::Cached;
        }
        OpenMode::Direct
    }

    pub async fn open(&self, file: FileId, flags: i32) -> NestResult<(u64, OpenMode)> {
        let a = self.raw_attr(file)?;
        match a.kind {
            FileKind::Regular => {}
            FileKind::Directory => return Err(NestError::IsDir),
            FileKind::Symlink => return Err(NestError::Invalid("open of a symlink".into())),
        }
        if a.nlink == 0 {
            return Err(NestError::NotFound);
        }
        let acc = flags & oflags::ACCMODE;
        let writable = acc == oflags::WRONLY || acc == oflags::RDWR;
        if writable && a.sealed {
            return Err(NestError::NotPermitted("file is sealed".into()));
        }
        self.d.wait_caught_up(self.cfg.catch_up_wait).await;
        self.usage.record_open(file);
        let mut pinned = false;
        if !writable && let Ok((_, always)) = self.q(|c| query::read_hints(c, file)) {
            pinned = always;
        }
        let mode = self.choose_mode(&a, writable, pinned);
        let fh = self.new_handle(file, flags);
        if writable && flags & oflags::TRUNC != 0 {
            let r = self.truncate(file, 0, Some(fh)).await;
            if let Err(e) = r {
                self.release(fh, None).await;
                return Err(e);
            }
        }
        Ok((fh, mode))
    }

    pub async fn create(
        &self,
        parent: FileId,
        name: &[u8],
        perm: u32,
        flags: i32,
    ) -> NestResult<(FileAttr, u64, OpenMode)> {
        let r = self
            .propose(Command::Create {
                parent,
                name: name.to_vec(),
                perm,
                node: self.me(),
                exclusive: flags & oflags::EXCL != 0,
                now: Timestamp::now(),
            })
            .await?;
        let Reply::Created(cr) = r else {
            return Err(NestError::Io(format!("unexpected reply {r:?}")));
        };
        if !cr.created {
            let (fh, mode) = self.open(cr.attr.id, flags).await?;
            return Ok((self.getattr(cr.attr.id).await?, fh, mode));
        }
        let fh = self.new_handle(cr.attr.id, flags);
        let o = self
            .owned_state(cr.attr.id, cr.attr.generation, cr.attr.epoch)
            .await?;
        o.participants.lock().insert((self.me(), fh));
        Ok((cr.attr, fh, OpenMode::Direct))
    }

    /// Close a handle. `lock_owner` is set for flock-style releases.
    pub async fn release(&self, fh: u64, lock_owner: Option<u64>) {
        let Some(h) = self.handles.lock().remove(&fh) else {
            return;
        };
        self.fold_handle(&h);
        let owners: Vec<u64> = h.lock_owners.lock().drain().chain(lock_owner).collect();
        for owner in owners {
            self.release_locks(h.file, owner).await;
        }
        let me = (self.me(), fh);
        let local = self
            .owned
            .lock()
            .get(&h.file)
            .map(|o| o.participants.lock().remove(&me))
            .unwrap_or(false);
        if local {
            self.schedule_finalize(h.file);
        }
        let remote = *h.remote.lock();
        if let Some((owner, epoch)) = remote {
            let _ = self
                .call(
                    owner,
                    &DataReq::Leave {
                        file: h.file,
                        epoch,
                        writer: me,
                    },
                )
                .await;
        }
        self.d.handle_closed(h.file);
    }

    /// close(2) on some descriptor: POSIX drops the process's fcntl locks.
    pub async fn flush(&self, fh: u64, lock_owner: u64) -> NestResult<()> {
        let h = self.handle(fh)?;
        if h.lock_owners.lock().remove(&lock_owner) {
            self.release_locks(h.file, lock_owner).await;
        }
        Ok(())
    }

    /// An application's `fsync` (`data_only`: `fdatasync`). The owner's
    /// object gets the same call; then the metadata the file depends on is
    /// made durable on a majority (ADR-026). Stable content was already
    /// synced when it became stable.
    pub async fn fsync(&self, fh: u64, data_only: bool) -> NestResult<()> {
        let h = self.handle(fh)?;
        let a = self.raw_attr(h.file)?;
        match (a.gen_state, a.owner) {
            (GenState::Owned, Some(o)) if o == self.me() => {
                self.owner_fsync(h.file, a.epoch, data_only).await?
            }
            (GenState::Owned, Some(o)) => {
                match self
                    .call(
                        o,
                        &DataReq::Fsync {
                            file: h.file,
                            epoch: a.epoch,
                            data_only,
                        },
                    )
                    .await
                {
                    // Stale: finalized meanwhile, which syncs the object.
                    Ok(_) | Err(NestError::Stale) => {}
                    Err(e) => return Err(e),
                }
            }
            _ => {}
        }
        self.sync_metadata().await
    }

    /// Make every namespace change this node has seen durable on a majority
    /// (`fsync` on a directory, and the tail of a file `fsync`).
    pub async fn sync_metadata(&self) -> NestResult<()> {
        self.d.meta().sync_barrier().await
    }

    // ------------------------------------------------------------ data

    /// Read through a handle. A read that finds no copy it may serve (the
    /// lease lapsed, holders unreachable, the leader changing) retries
    /// until the cluster's read recovery wait is spent (ADR-056): a model
    /// load would rather stall through an election than fail.
    pub async fn read(&self, fh: u64, offset: u64, size: u32) -> NestResult<Vec<u8>> {
        let mut deadline = None;
        let mut pause = Duration::from_millis(50);
        loop {
            match self.read_once(fh, offset, size).await {
                Err(e @ NestError::Unavailable(_)) => {
                    let d = *deadline.get_or_insert_with(|| {
                        tokio::time::Instant::now()
                            + Duration::from_millis(self.setting(Setting::ReadRecoveryWait))
                    });
                    let now = tokio::time::Instant::now();
                    if now >= d {
                        return Err(e);
                    }
                    tracing::debug!(fh, error = %e, "read found no servable copy; retrying");
                    tokio::time::sleep(pause.min(d - now)).await;
                    pause = (pause * 2).min(Duration::from_millis(500));
                }
                r => return r,
            }
        }
    }

    /// The file an open handle reads, if it is still open.
    pub fn handle_file(&self, fh: u64) -> Option<FileId> {
        self.handle(fh).ok().map(|h| h.file)
    }

    /// A file's path from the mount root (any one of its names), for logs.
    pub fn path_string(&self, file: FileId) -> Option<String> {
        self.q(|c| query::path_of(c, file))
            .ok()
            .flatten()
            .map(|p| String::from_utf8_lossy(&p).into_owned())
    }

    /// The current value of a cluster-wide setting (its default unless set).
    pub fn setting(&self, s: Setting) -> u64 {
        let mut g = self.settings.lock();
        if g.0.elapsed() >= SETTINGS_TTL {
            if let Ok(rows) = self.q(query::settings) {
                g.1 = rows.into_iter().collect();
            }
            g.0 = std::time::Instant::now();
        }
        g.1.get(s.name())
            .copied()
            .unwrap_or_else(|| s.default_value())
    }

    async fn read_once(&self, fh: u64, offset: u64, size: u32) -> NestResult<Vec<u8>> {
        let h = self.handle(fh)?;
        let mut last = NestError::Unavailable("no copy reachable".into());
        for attempt in 0..6 {
            if attempt > 0 {
                // Our view may be behind the cluster: catch up and retry. A
                // node cut off from the leader gives up within a lease.
                if !self.refresh().await {
                    return Err(last);
                }
            }
            let a = self.raw_attr(h.file)?;
            let key = ObjectKey::new(h.file, a.generation);
            if let Some(src) = self.read_source(&h, &a) {
                let _t = self.track(h.file);
                let f = {
                    let mut r = h.reader.lock();
                    match r.as_ref() {
                        Some((k, f)) if *k == key => f.clone(),
                        _ => match src.open_read(key) {
                            Ok(f) => {
                                let f = Arc::new(f);
                                *r = Some((key, f.clone()));
                                f
                            }
                            // Invalidated since the check: the generation
                            // moved, so refresh and route again.
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                last = NestError::Stale;
                                continue;
                            }
                            Err(e) => return Err(io(e)),
                        },
                    }
                };
                let kind = self.source_kind(&src);
                let (r, took) = tokio::task::spawn_blocking(move || {
                    let t = std::time::Instant::now();
                    (pread(&f, offset, size), t.elapsed())
                })
                .await
                .expect("blocking task");
                if let Ok(b) = &r {
                    if kind == crate::usage::Source::Local {
                        self.io
                            .record(nest_fabric::iostats::Kind::Disk, size as u64, took);
                    }
                    self.count_read(&h, kind, b.len() as u64);
                }
                return r;
            }
            // No local copy: each read exactly as asked (the kernel reads
            // ahead, ADR-065), from whichever copy is least busy.
            if a.gen_state == GenState::Stable
                && let Some(route) = self.route(&h, &a)
                && let Some(fab) = self.fabric()
            {
                let peers: Vec<crate::balance::Source> = route
                    .sources
                    .iter()
                    .copied()
                    .filter(|s| matches!(s, crate::balance::Source::Peer(_)))
                    .collect();
                if !peers.is_empty() {
                    let ticket = self
                        .balancer
                        .pick_affine(&peers, region_key(h.file, offset));
                    let crate::balance::Source::Peer(node) = ticket.source() else {
                        unreachable!("peers only")
                    };
                    let _t = self.track(h.file);
                    match self
                        .fabric_exact(&fab, node, h.file, a.generation, offset, size)
                        .await
                    {
                        Ok(b) => {
                            ticket.finish(b.len() as u64);
                            self.count_read(&h, crate::usage::Source::Remote, b.len() as u64);
                            return Ok(b);
                        }
                        // Fall through to the one-holder-at-a-time path.
                        Err(e) => {
                            ticket.fail();
                            last = e;
                        }
                    }
                }
            }
            let sources = match (a.gen_state, a.owner) {
                (GenState::Owned, Some(o)) => vec![o],
                _ => self.holders(&a)?,
            };
            if sources.is_empty() {
                // Either the file really has no live copy, or its state moved
                // between our two metadata reads: refresh and look again.
                last = NestError::Unavailable("no live copy of this file is known".into());
                continue;
            }
            for s in sources {
                if let Some(bulk) = self.bulk_to(s) {
                    let r = bulk_exact(&bulk, s, h.file, a.generation, offset, size).await;
                    match r {
                        Ok(b) => {
                            self.count_read(&h, crate::usage::Source::Remote, b.len() as u64);
                            return Ok(b);
                        }
                        Err(NestError::Stale) => {
                            last = NestError::Stale;
                            break; // generation moved: refresh
                        }
                        Err(NestError::NotFound) => {
                            last = NestError::NotFound;
                            continue;
                        }
                        Err(e) => {
                            // Bulk trouble: fall back to the control
                            // connection for this read.
                            tracing::debug!(peer = %s, error = %e, "bulk read failed; using one request");
                        }
                    }
                }
                match self
                    .call(
                        s,
                        &DataReq::Read {
                            file: h.file,
                            generation: a.generation,
                            offset,
                            len: size,
                        },
                    )
                    .await
                {
                    Ok(DataResp::Data(b)) => {
                        self.count_read(&h, crate::usage::Source::Remote, b.len() as u64);
                        return Ok(b);
                    }
                    Ok(other) => last = NestError::Io(format!("unexpected response {other:?}")),
                    Err(NestError::Stale) => {
                        last = NestError::Stale;
                        break; // generation moved: refresh
                    }
                    Err(e) => last = e,
                }
            }
            // Every live holder failed: a local archive copy still serves.
            if !matches!(last, NestError::Stale)
                && let Some(src) = self.local_source(&a)
            {
                let f = src.open_read(key).map_err(io)?;
                let r = tokio::task::spawn_blocking(move || pread(&f, offset, size))
                    .await
                    .expect("blocking task");
                if let Ok(b) = &r {
                    self.count_read(&h, crate::usage::Source::Archive, b.len() as u64);
                }
                return r;
            }
        }
        Err(last)
    }

    /// One uncached fabric read of exactly `[offset, offset+size)`.
    async fn fabric_exact(
        &self,
        fab: &Arc<nest_fabric::Fabric>,
        source: NodeId,
        file: FileId,
        generation: Generation,
        offset: u64,
        size: u32,
    ) -> NestResult<Vec<u8>> {
        let mut out = Vec::with_capacity(size as usize);
        let chunk = fab.chunk();
        while out.len() < size as usize {
            let want = (size as usize - out.len()).min(chunk);
            let b = fab
                .read(source, file, generation, offset + out.len() as u64, want)
                .await?;
            out.extend_from_slice(b.as_slice());
            if b.len() < want {
                break;
            }
        }
        Ok(out)
    }

    /// Complete a read without waiting, if possible: a local copy (pread
    /// from the page cache). Returns `None`
    /// when the read needs I/O across the network or a state change.
    ///
    /// Frontends call this on the thread that received the request: on the
    /// Sparks every thread hand-off can cost a deep-idle wake (hundreds of
    /// microseconds), which dominated small-request latency.
    pub fn try_read_now(&self, fh: u64, offset: u64, size: u32) -> Option<NestResult<Vec<u8>>> {
        self.try_read_now_with(fh, offset, size, |f| pread(f, offset, size))
    }

    /// `try_read_now` into a caller's buffer (FUSE over io_uring reads
    /// straight into the ring entry the kernel copies from).
    pub fn try_read_into(&self, fh: u64, offset: u64, buf: &mut [u8]) -> Option<NestResult<usize>> {
        let size = buf.len() as u32;
        self.try_read_now_with(fh, offset, size, |f| pread_into(f, offset, buf))
    }

    fn try_read_now_with<R>(
        &self,
        fh: u64,
        offset: u64,
        size: u32,
        from_file: impl FnOnce(&std::fs::File) -> NestResult<R>,
    ) -> Option<NestResult<R>> {
        let h = self.handle(fh).ok()?;
        let a = self.fast_attr(&h)?;
        let key = ObjectKey::new(h.file, a.generation);
        // A local copy answers at once; remote reads take the slow path or
        // `prepare_remote`.
        if let Some(src) = self.read_source(&h, &a) {
            let _t = self.track(h.file);
            let f = {
                let mut r = h.reader.lock();
                match r.as_ref() {
                    Some((k, f)) if *k == key => f.clone(),
                    _ => {
                        let f = match src.open_read(key) {
                            Ok(f) => Arc::new(f),
                            // Invalidated since the check: the slow path
                            // refreshes and routes to the new owner.
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
                            Err(e) => return Some(Err(io(e))),
                        };
                        *r = Some((key, f.clone()));
                        f
                    }
                }
            };
            let t = std::time::Instant::now();
            let r = from_file(&f);
            if r.is_ok() {
                if self.source_kind(&src) == crate::usage::Source::Local {
                    self.io
                        .record(nest_fabric::iostats::Kind::Disk, size as u64, t.elapsed());
                }
                // The bytes a read at `offset` can return, without asking
                // the caller's result type for its length.
                let n = a.size.saturating_sub(offset).min(size as u64);
                self.count_read(&h, self.source_kind(&src), n);
            }
            return Some(r);
        }
        None
    }

    /// Complete a write without waiting, if this node already owns the file
    /// in an open, fenced epoch. Returns `None` otherwise.
    pub fn try_write_now(&self, fh: u64, offset: u64, data: &[u8]) -> Option<NestResult<u32>> {
        let h = self.handle(fh).ok()?;
        if !h.writable {
            return None;
        }
        let o = self.owned.lock().get(&h.file).cloned()?;
        if !*o.fenced.borrow() {
            return None;
        }
        let a = self.raw_attr(h.file).ok()?;
        if a.gen_state != GenState::Owned || a.owner != Some(self.me()) || a.epoch != o.epoch {
            return None;
        }
        let guard = o.open.try_read().ok()?;
        if !*guard {
            return None;
        }
        if let Err(e) = self.d.store().reserve_room(data.len() as u64) {
            return Some(Err(io(e)));
        }
        o.participants.lock().insert((self.me(), fh));
        o.activity.fetch_add(1, Ordering::SeqCst);
        let r = (|| {
            if h.append {
                let _end = o.append.lock();
                o.file.write_all_at(data, o.file.metadata()?.len())?;
            } else {
                o.file.write_all_at(data, offset)?;
            }
            Ok::<_, std::io::Error>(data.len() as u32)
        })();
        Some(r.map_err(io))
    }

    pub async fn write(&self, fh: u64, offset: u64, data: Vec<u8>) -> NestResult<u32> {
        let h = self.handle(fh)?;
        if !h.writable {
            return Err(NestError::Invalid("handle not open for writing".into()));
        }
        let op = OwnerOp::Write {
            offset,
            append: h.append,
            data: data.into(),
        };
        match self.mutate(&h, fh, false, op).await? {
            DataResp::Written(n) => Ok(n),
            other => Err(NestError::Io(format!("unexpected response {other:?}"))),
        }
    }

    async fn truncate(&self, file: FileId, size: u64, fh: Option<u64>) -> NestResult<()> {
        let h = match fh {
            Some(fh) => self.handles.lock().get(&fh).cloned(),
            None => None,
        };
        match (h, fh) {
            (Some(h), Some(fh)) => self
                .mutate(&h, fh, size == 0, OwnerOp::Truncate(size))
                .await
                .map(|_| ()),
            _ => {
                // truncate(2) by path: no writer joins the epoch.
                for _ in 0..8 {
                    let (owner, _, epoch) = self.route_mutation(file, size == 0).await?;
                    let r = if owner == self.me() {
                        self.owner_mutate(file, epoch, None, OwnerOp::Truncate(size))
                            .await
                    } else {
                        self.call(
                            owner,
                            &DataReq::Truncate {
                                file,
                                epoch,
                                writer: None,
                                size,
                            },
                        )
                        .await
                    };
                    match r {
                        Err(NestError::Stale) => continue,
                        other => return other.map(|_| ()),
                    }
                }
                Err(NestError::Busy("ownership kept changing".into()))
            }
        }
    }

    /// Route a mutation through a handle to the file's owner.
    async fn mutate(
        &self,
        h: &Handle,
        fh: u64,
        discard: bool,
        op: OwnerOp,
    ) -> NestResult<DataResp> {
        let writer = (self.me(), fh);
        for _ in 0..8 {
            let (owner, _, epoch) = self.route_mutation(h.file, discard).await?;
            let r = if owner == self.me() {
                self.owner_mutate(h.file, epoch, Some(writer), op.clone())
                    .await
            } else {
                let r = match &op {
                    OwnerOp::Write {
                        offset,
                        append,
                        data,
                    } => {
                        let op = nest_fabric::WriteOp {
                            file: h.file,
                            epoch,
                            writer,
                            offset: *offset,
                            append: *append,
                        };
                        self.remote_write(owner, op, data.clone())
                            .await
                            .map(DataResp::Written)
                    }
                    OwnerOp::Truncate(size) => {
                        self.call(
                            owner,
                            &DataReq::Truncate {
                                file: h.file,
                                epoch,
                                writer: Some(writer),
                                size: *size,
                            },
                        )
                        .await
                    }
                    OwnerOp::Fsync { data_only } => {
                        self.call(
                            owner,
                            &DataReq::Fsync {
                                file: h.file,
                                epoch,
                                data_only: *data_only,
                            },
                        )
                        .await
                    }
                };
                if r.is_ok() {
                    *h.remote.lock() = Some((owner, epoch));
                }
                r
            };
            match r {
                Err(NestError::Stale) => {
                    self.refresh().await;
                    continue;
                }
                other => return other,
            }
        }
        Err(NestError::Busy("ownership kept changing".into()))
    }

    /// Send a write to `owner` (ADR-057): over RDMA when a link to it speaks
    /// writes, else over a TCP lane that takes them, else in a request on
    /// the control connection. A transport is skipped only when the write
    /// never reached the owner (`WriteError::Unsent`). Once it may have,
    /// its error is final: a write whose answer was lost is EIO, never
    /// resent, since a resent append would land twice.
    async fn remote_write(
        &self,
        owner: NodeId,
        op: nest_fabric::WriteOp,
        data: bytes::Bytes,
    ) -> NestResult<u32> {
        use nest_fabric::WriteError;
        let fabric = self
            .fabric()
            .filter(|f| data.len() <= f.max_write() && !self.offgrid_hosts().contains(&owner));
        if let Some(f) = fabric {
            match f.write(owner, op, &data).await {
                Ok(n) => return Ok(n),
                Err(WriteError::Failed(e)) => return Err(e),
                Err(WriteError::Unsent(e)) => {
                    tracing::debug!(%owner, error = %e, "write not sent over RDMA");
                }
            }
        }
        if let Some(l) = self
            .lanes()
            .filter(|l| data.len() <= l.chunk() && l.writable(owner))
        {
            match l.write(owner, op, data.clone()).await {
                Ok(n) => return Ok(n),
                Err(WriteError::Failed(e)) => return Err(e),
                Err(WriteError::Unsent(e)) => {
                    tracing::debug!(%owner, error = %e, "write not sent over a TCP lane");
                }
            }
        }
        let req = DataReq::Write {
            file: op.file,
            epoch: op.epoch,
            writer: op.writer,
            offset: op.offset,
            append: op.append,
            data: data.to_vec(),
        };
        match self.call(owner, &req).await? {
            DataResp::Written(n) => Ok(n),
            other => Err(NestError::Io(format!("unexpected response {other:?}"))),
        }
    }

    /// Catch up with the leader, bounded by one lease. False if we could not.
    async fn refresh(&self) -> bool {
        matches!(
            tokio::time::timeout(self.d.lease(), self.d.meta().barrier()).await,
            Ok(Ok(()))
        )
    }

    // ------------------------------------------------------------ locks

    /// Set or clear an advisory lock. With `wait`, blocks until granted.
    #[allow(clippy::too_many_arguments)]
    pub async fn setlk(
        &self,
        fh: u64,
        owner: u64,
        start: u64,
        end: u64,
        kind: LockKind,
        pid: u32,
        wait: bool,
    ) -> NestResult<()> {
        let h = self.handle(fh)?;
        loop {
            let session = self
                .d
                .session()
                .ok_or(NestError::Unavailable("no session yet".into()))?;
            let notified = self.d.lock_notify.notified();
            let r = self
                .propose(Command::SetLock {
                    file: h.file,
                    session,
                    owner,
                    start,
                    end,
                    kind,
                    pid,
                })
                .await;
            match r {
                Ok(_) => {
                    if kind != LockKind::Unlock {
                        h.lock_owners.lock().insert(owner);
                    }
                    return Ok(());
                }
                Err(NestError::WouldBlock) if wait => {
                    let _ = tokio::time::timeout(Duration::from_millis(250), notified).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// The first conflicting lock, as (start, end, write, pid).
    pub fn getlk(
        &self,
        fh: u64,
        owner: u64,
        start: u64,
        end: u64,
        write: bool,
    ) -> NestResult<Option<(u64, u64, bool, u32)>> {
        let h = self.handle(fh)?;
        let session = self
            .d
            .session()
            .ok_or(NestError::Unavailable("no session yet".into()))?;
        Ok(self
            .q(|c| query::lock_conflict(c, h.file, session, owner, start, end, write))?
            .map(|l| (l.start, l.end, l.write, l.pid)))
    }

    async fn release_locks(&self, file: FileId, owner: u64) {
        let Some(session) = self.d.session() else {
            return;
        };
        if let Err(e) = self
            .propose(Command::ReleaseLocks {
                file,
                session,
                owner,
            })
            .await
        {
            tracing::warn!(%file, error = %e, "releasing locks failed; session expiry will");
        }
    }

    // ------------------------------------------------------------ placement

    /// Pull a complete copy of `file`'s current STABLE generation into this
    /// node's store and publish it (PROPOSAL §7 whole-file replication).
    /// Returns the bytes copied (0 if a live copy was already here).
    ///
    /// Staging is never served; the copy is accepted on expected length and
    /// completed I/O only (no checksums, ADR-008), made durable, moved into
    /// place, then published conditionally: if the generation moved in the
    /// meantime publication is refused and the copy deleted.
    pub async fn replicate_here(&self, file: FileId) -> NestResult<u64> {
        self.replicate_into(file, self.me().live_store()).await
    }

    /// Replicate into `target`: this node's live store, or an archive store
    /// this node is a (healthy) gateway for.
    pub async fn replicate_into(
        &self,
        file: FileId,
        target: nest_types::StoreId,
    ) -> NestResult<u64> {
        self.replicate_counting(file, target, &Arc::default()).await
    }

    /// `replicate_into`, adding each chunk's bytes to `copied` as it lands
    /// (for a job's progress; a retried source counts again from 0, so
    /// readers take it as a hint, not a total).
    pub async fn replicate_counting(
        &self,
        file: FileId,
        target: nest_types::StoreId,
        copied: &Arc<AtomicU64>,
    ) -> NestResult<u64> {
        let dest: Arc<nest_store::ObjectStore> = match self.d.store_for(target) {
            Some(st) => st,
            None => {
                return Err(NestError::Unavailable(format!(
                    "store {target} is not reachable through this node (not its disk or a gateway, or its marker is missing)"
                )));
            }
        };
        let a = self.raw_attr(file)?;
        if a.kind != FileKind::Regular {
            return Err(NestError::Invalid(
                "only regular files have replicas".into(),
            ));
        }
        if a.gen_state != GenState::Stable {
            return Err(NestError::Busy("file is being written".into()));
        }
        let key = ObjectKey::new(file, a.generation);
        let me_store = target;
        if self.q(|c| query::has_live_replica(c, file, a.generation, me_store))? {
            return Ok(0);
        }
        // Into the host's own stores, one at a time per disk: transfers
        // fill the disks side by side rather than each in turn (ADR-073).
        let _disk_slot = if target.host() == Some(self.me()) {
            let sem = self
                .disk_slots
                .lock()
                .entry(target)
                .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(DISK_TRANSFERS)))
                .clone();
            Some(
                sem.acquire_owned()
                    .await
                    .map_err(|_| NestError::Unavailable("shutting down".into()))?,
            )
        } else {
            None
        };
        let _slot = if crate::is_archive(target) {
            let sem = self
                .archive_slots
                .lock()
                .entry(target)
                .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(ARCHIVE_TRANSFERS)))
                .clone();
            Some(
                sem.acquire_owned()
                    .await
                    .map_err(|_| NestError::Unavailable("shutting down".into()))?,
            )
        } else {
            None
        };
        self.fetch_counting(&a, &dest, copied).await?;
        let r = self
            .propose(Command::PublishReplica {
                file,
                generation: a.generation,
                store: me_store,
            })
            .await;
        match r {
            Ok(_) => Ok(a.size),
            Err(e) => {
                let _ = dest.delete(key);
                Err(e)
            }
        }
    }

    /// Register a local file as this node's copy of `file`'s current
    /// generation by hard-linking it into the store: no bytes move. Only for
    /// callers that know the contents are identical, such as
    /// content-addressed Hugging Face blobs (the name is the hash). Checks
    /// kind, stability and size; publication is conditional on the
    /// generation. `Ok(false)` if this node already had a copy.
    pub async fn adopt_local(
        &self,
        file: FileId,
        src: &std::path::Path,
        size: u64,
    ) -> NestResult<bool> {
        let a = self.raw_attr(file)?;
        if a.kind != FileKind::Regular {
            return Err(NestError::Invalid("not a regular file".into()));
        }
        if a.gen_state != GenState::Stable {
            return Err(NestError::Busy("file is being written".into()));
        }
        if a.size != size {
            return Err(NestError::Invalid(format!(
                "{} bytes here but {} in the cluster; left alone",
                size, a.size
            )));
        }
        let me_store = self.me().live_store();
        if self.q(|c| query::has_live_replica(c, file, a.generation, me_store))? {
            return Ok(false);
        }
        let key = ObjectKey::new(file, a.generation);
        let store = self.d.store().clone();
        let src = src.to_path_buf();
        tokio::task::spawn_blocking(move || {
            // An unpublished leftover under this key is never served.
            let _ = store.delete(key);
            store.link_from(&src, key)
        })
        .await
        .map_err(|e| NestError::Io(e.to_string()))?
        .map_err(|e| NestError::Io(e.to_string()))?;
        match self
            .propose(Command::PublishReplica {
                file,
                generation: a.generation,
                store: me_store,
            })
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let _ = self.d.store().delete(key);
                Err(e)
            }
        }
    }

    /// Copy exactly `a`'s generation into `dest` (committed into its objects,
    /// not published anywhere): from a local copy if this node has one, else
    /// from a holder over RDMA/TCP. `Stale` if the generation moved.
    pub async fn fetch_into(
        &self,
        a: &FileAttr,
        dest: &Arc<nest_store::ObjectStore>,
    ) -> NestResult<()> {
        self.fetch_counting(a, dest, &Arc::default()).await
    }

    /// `fetch_into`, adding each chunk's bytes to `copied` as it lands. When
    /// no source can serve it right now (their leases lapsed while the
    /// cluster could not confirm, a leader changing), it tries again until
    /// the cluster's read recovery wait is spent, as reads do (ADR-056):
    /// a burst of traffic then delays a copy instead of skipping it. Copies
    /// wait at least `COPY_RECOVERY_WAIT`.
    async fn fetch_counting(
        &self,
        a: &FileAttr,
        dest: &Arc<nest_store::ObjectStore>,
        copied: &Arc<AtomicU64>,
    ) -> NestResult<()> {
        let mut deadline = None;
        let mut pause = Duration::from_millis(100);
        loop {
            match self.fetch_once(a, dest, copied).await {
                Err(e @ NestError::Unavailable(_)) => {
                    // Copies are not waited on by a reader: they ride out
                    // longer than reads (a flood can hold leases off for
                    // a minute or two).
                    let d = *deadline.get_or_insert_with(|| {
                        tokio::time::Instant::now()
                            + Duration::from_millis(self.setting(Setting::ReadRecoveryWait))
                                .max(COPY_RECOVERY_WAIT)
                    });
                    let now = tokio::time::Instant::now();
                    if now >= d {
                        return Err(e);
                    }
                    tracing::debug!(file = a.id.0, error = %e, "no source could serve a copy; retrying");
                    tokio::time::sleep(pause.min(d - now)).await;
                    pause = (pause * 2).min(Duration::from_secs(1));
                    // Holders may have changed meanwhile: plan again.
                    let now = self.raw_attr(a.id)?;
                    if now.generation != a.generation || now.gen_state != GenState::Stable {
                        return Err(NestError::Stale);
                    }
                }
                r => return r,
            }
        }
    }

    async fn fetch_once(
        &self,
        a: &FileAttr,
        dest: &Arc<nest_store::ObjectStore>,
        copied: &Arc<AtomicU64>,
    ) -> NestResult<()> {
        dest.reserve_room(a.size).map_err(io)?;
        let holders = self.holders(a)?;
        let mut sources: Vec<Option<NodeId>> = Vec::new();
        let local = self.local_source(a).filter(|l| !Arc::ptr_eq(l, dest));
        // A local live copy goes first; a local archive copy only after the
        // live holders (see `own_read_source`).
        let local_first = self.own_read_source(a).is_some();
        if local.is_some() && local_first {
            sources.push(None);
        }
        sources.extend(holders.into_iter().map(Some));
        if local.is_some() && !local_first {
            sources.push(None);
        }
        if sources.is_empty() {
            return Err(NestError::Unavailable("no live copy to copy from".into()));
        }
        let mut tried = Vec::new();
        let mut last = NestError::Unavailable("no holder reachable".into());
        for src in sources {
            copied.store(0, Ordering::Relaxed);
            let copied = match src {
                None => self.copy_local(a, dest, copied).await,
                Some(n) => self.copy_from(n, a, dest, copied).await,
            };
            let from = src.map_or_else(|| "local copy".to_string(), |n| format!("node {n}"));
            match copied {
                Ok(()) => return Ok(()),
                // Stale from one source is final only if the file really
                // moved on; otherwise that source lost its copy (evicted,
                // or behind on metadata) and the next one may still serve.
                Err(NestError::Stale) => {
                    let now = self.raw_attr(a.id)?;
                    if now.generation != a.generation || now.gen_state != GenState::Stable {
                        return Err(NestError::Stale);
                    }
                    tracing::warn!(file = a.id.0, generation = a.generation.0, %from,
                        "source no longer has this generation; trying the next");
                    tried.push(format!("{from}: stale"));
                    last = NestError::Unavailable(format!(
                        "no source could serve it ({})",
                        tried.join(", ")
                    ));
                }
                Err(e) => {
                    tried.push(format!("{from}: {e}"));
                    last = e;
                }
            }
        }
        tracing::warn!(file = a.id.0, generation = a.generation.0, tried = %tried.join("; "),
            "copy failed from every source");
        Err(last)
    }

    async fn copy_from(
        &self,
        src: NodeId,
        a: &FileAttr,
        dest: &Arc<nest_store::ObjectStore>,
        copied: &Arc<AtomicU64>,
    ) -> NestResult<()> {
        let key = ObjectKey::new(a.id, a.generation);
        let store = dest.clone();
        let staging = tokio::task::spawn_blocking(move || store.begin_staging(key))
            .await
            .expect("blocking task")
            .map_err(io)?;
        // Off the fabric (either end): TCP lanes, which need no slots. On
        // it: RDMA, or the standby lane while the RDMA link to `src` is
        // down (ADR-057).
        let lanes = self.lanes().filter(|l| l.connected(src) > 0).filter(|_| {
            self.fabric().is_none()
                || self.offgrid_hosts().contains(&src)
                || self.standby().is_some_and(|s| s.on_lane(src))
        });
        let fab = if lanes.is_some() { None } else { self.fabric() };
        let chunk: u64 = match (&fab, &lanes) {
            (Some(f), _) => f.chunk() as u64,
            (None, Some(l)) => l.chunk() as u64,
            (None, None) => 4 << 20,
        };
        let window = 16usize;
        // Chunks are fetched 16 at a time and written as they arrive by
        // direct I/O through io_uring (nest_store::uring): no thread waits
        // on the disk, and the store's window of writes in flight paces the
        // transfer at the device's speed.
        staging.presize(a.size).map_err(io)?;
        let staging = Arc::new(staging);
        let mut next = 0u64;
        let mut inflight = futures::stream::FuturesUnordered::new();
        use futures::StreamExt;
        let fetch = |off: u64| {
            let (fab, lanes, st) = (fab.clone(), lanes.clone(), staging.clone());
            let (file, generation, size) = (a.id, a.generation, a.size);
            async move {
                let len = (size - off).min(chunk) as usize;
                let buf = match (&fab, &lanes) {
                    (_, Some(l)) => match l.read(src, file, generation, off, len).await {
                        Ok(b) => nest_store::uring::AlignedBuf::copy_of(&b),
                        Err(NestError::Stale) => return Err(NestError::Stale),
                        Err(_) => nest_store::uring::AlignedBuf::copy_of(
                            &self
                                .tcp_read(src, file, generation, off, len as u32)
                                .await?,
                        ),
                    },
                    (Some(f), None) => match f.read(src, file, generation, off, len).await {
                        Ok(b) => nest_store::uring::AlignedBuf::copy_of(b.as_slice()),
                        Err(NestError::Stale) => return Err(NestError::Stale),
                        // The link died: the standby lane if there is one,
                        // else one request on the control connection.
                        Err(_) => match self.standby().filter(|s| s.on_lane(src)) {
                            Some(s) => nest_store::uring::AlignedBuf::copy_of(
                                &s.lanes.read(src, file, generation, off, len).await?,
                            ),
                            None => nest_store::uring::AlignedBuf::copy_of(
                                &self
                                    .tcp_read(src, file, generation, off, len as u32)
                                    .await?,
                            ),
                        },
                    },
                    (None, None) => nest_store::uring::AlignedBuf::copy_of(
                        &self
                            .tcp_read(src, file, generation, off, len as u32)
                            .await?,
                    ),
                };
                if buf.len() != len {
                    return Err(NestError::Io(format!(
                        "short read at {off}: {} of {len} bytes",
                        buf.len()
                    )));
                }
                st.write(buf, off).await.map_err(io)?;
                copied.fetch_add(len as u64, Ordering::Relaxed);
                Ok(())
            }
        };
        while next < a.size || !inflight.is_empty() {
            while inflight.len() < window && next < a.size {
                inflight.push(fetch(next));
                next += chunk;
            }
            if let Some(r) = inflight.next().await {
                r?;
            }
        }
        drop(inflight);
        staging.flushed().await.map_err(io)?;
        let st = Arc::try_unwrap(staging)
            .map_err(|_| NestError::Io("transfer still referenced".into()))?;
        let store = dest.clone();
        let size = a.size;
        tokio::task::spawn_blocking(move || {
            st.file().set_len(size)?;
            store.commit_staging(st)
        })
        .await
        .expect("blocking task")
        .map_err(io)?;
        Ok(())
    }

    /// Copy the local object into `dest` (e.g. live store to an archive this
    /// node is a gateway for): read in 4 MiB chunks, written directly
    /// through io_uring with the store's window in flight, the source's
    /// pages dropped as it goes. Not `copy_file_range`: between two
    /// filesystems (NVMe to the scratch disk) it quietly became a buffered
    /// copy and flooded the page cache.
    async fn copy_local(
        &self,
        a: &FileAttr,
        dest: &Arc<nest_store::ObjectStore>,
        copied: &Arc<AtomicU64>,
    ) -> NestResult<()> {
        let key = ObjectKey::new(a.id, a.generation);
        let src = self
            .local_source(a)
            .ok_or_else(|| NestError::Unavailable("the local copy is no longer servable".into()))?;
        let dest = dest.clone();
        let size = a.size;
        let copied = copied.clone();
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            use std::os::unix::fs::FileExt;
            const CHUNK: usize = 4 << 20;
            let from = src.open_read(key)?;
            let st = dest.begin_staging(key)?;
            st.presize(size)?;
            let mut off = 0u64;
            let mut since_drop = 0usize;
            while off < size {
                let want = ((size - off) as usize).min(CHUNK);
                let mut buf = nest_store::uring::AlignedBuf::new(want);
                let mut got = 0;
                while got < want {
                    let n = from.read_at(&mut buf.as_mut_slice()[got..], off + got as u64)?;
                    if n == 0 {
                        return Err(std::io::Error::other(format!(
                            "short copy: {} of {size} bytes",
                            off + got as u64
                        )));
                    }
                    got += n;
                }
                st.write_blocking(buf, off)?;
                off += want as u64;
                copied.fetch_add(want as u64, Ordering::Relaxed);
                // Read once: keep the host's page cache for others.
                since_drop += want;
                if since_drop >= 256 << 20 {
                    nest_store::drop_cached(&from);
                    since_drop = 0;
                }
            }
            nest_store::drop_cached(&from);
            st.flushed_blocking()?;
            st.file().set_len(size)?;
            dest.commit_staging(st).map(|_| ())
        })
        .await
        .expect("blocking task")
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                NestError::Stale
            } else {
                io(e)
            }
        })
    }

    async fn tcp_read(
        &self,
        src: NodeId,
        file: FileId,
        generation: Generation,
        offset: u64,
        len: u32,
    ) -> NestResult<Vec<u8>> {
        match self
            .call(
                src,
                &DataReq::Read {
                    file,
                    generation,
                    offset,
                    len,
                },
            )
            .await?
        {
            DataResp::Data(b) => Ok(b),
            other => Err(NestError::Io(format!("unexpected response {other:?}"))),
        }
    }

    /// Remove this node's copy of `file` (refused for the last live copy).
    pub async fn evict_here(&self, file: FileId) -> NestResult<bool> {
        self.evict_from(file, self.me().live_store()).await
    }

    /// Remove `store`'s copy of exactly `generation` (a plan step): fails
    /// with `NotFound` if the file moved on since it was planned.
    pub async fn evict_generation(
        &self,
        file: FileId,
        generation: Generation,
        store: nest_types::StoreId,
    ) -> NestResult<()> {
        self.propose(Command::RetireReplica {
            file,
            generation,
            store,
            allow_last: false,
        })
        .await
        .map(|_| ())
    }

    /// Remove `store`'s copy of `file` (refused for the last live copy).
    pub async fn evict_from(&self, file: FileId, store: nest_types::StoreId) -> NestResult<bool> {
        let a = self.raw_attr(file)?;
        // Never hold open an object about to be removed here.
        {
            let key = ObjectKey::new(file, a.generation);
            let mut m = self.serve_files.lock();
            if m.get(&key).is_some_and(|e| e.store == store) {
                m.remove(&key);
            }
        }
        let me_store = store;
        if !self.q(|c| query::has_live_replica(c, file, a.generation, me_store))? {
            return Ok(false);
        }
        self.propose(Command::RetireReplica {
            file,
            generation: a.generation,
            store: me_store,
            allow_last: false,
        })
        .await?;
        Ok(true)
    }

    /// Make this host's live store hold `a`'s generation when only one of
    /// its disks does, so the copy can become a working object (writes and
    /// ownership use the live store; ADR-073). The disk's copy then goes.
    async fn pull_to_live(&self, a: &FileAttr) -> NestResult<()> {
        let key = ObjectKey::new(a.id, a.generation);
        let Some((from, _)) = self.d.servable_local(key) else {
            return Err(NestError::Unavailable("no copy here".into()));
        };
        let live = self.me().live_store();
        if from == live {
            return Ok(());
        }
        self.replicate_into(a.id, live).await?;
        let _ = self.evict_generation(a.id, a.generation, from).await;
        Ok(())
    }

    /// Move this host's copy of `file` from one of its stores to another
    /// (its live store or a disk): copied on this host, never over the
    /// network, then removed from `from` (ADR-073).
    pub async fn move_between(
        &self,
        file: FileId,
        from: nest_types::StoreId,
        to: nest_types::StoreId,
    ) -> NestResult<u64> {
        if from.host() != Some(self.me()) || to.host() != Some(self.me()) {
            return Err(NestError::Invalid("both stores must be this host's".into()));
        }
        let a = self.raw_attr(file)?;
        if !self.q(|c| query::has_live_replica(c, file, a.generation, from))? {
            return Ok(0);
        }
        let n = self.replicate_into(file, to).await?;
        self.evict_generation(file, a.generation, from).await?;
        Ok(n)
    }

    // ------------------------------------------------------------ misc

    /// The mount's size and free space: the live store's disk and every
    /// usable extra disk together (ADR-073).
    pub fn statfs(&self) -> NestResult<(nest_store::Capacity, u64)> {
        let mut cap = self.d.store().capacity().map_err(io)?;
        for d in self.d.disks() {
            if let Ok(c) = d.store.capacity() {
                cap.total += c.total;
                cap.free += c.free;
            }
        }
        let t = self.q(query::totals)?;
        Ok((cap, t.files + t.dirs))
    }
}

/// Transfers into one archive store at once.
const ARCHIVE_TRANSFERS: usize = 2;

/// Transfers into one of a host's own stores at once (ADR-073): enough to
/// keep an NVMe busy, so a job spread over a host's disks fills them
/// together.
const DISK_TRANSFERS: usize = 8;

/// How long a handle trusts a STABLE generation's attributes on the fast
/// path.
const ATTR_TTL: std::time::Duration = std::time::Duration::from_secs(1);

/// How long an open served object is trusted before a full re-check.
/// A served object idle this long is closed.
const SERVE_TTL: std::time::Duration = std::time::Duration::from_secs(2);
/// A served object is re-checked against metadata (closed, then reopened
/// by the full `servable` check) at least this often, however busy.
const SERVE_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(30);
const SERVE_FILES_MAX: usize = 4096;

/// An object kept open for serving (see `Vfs::serve_files`).
struct ServeEntry {
    file: Arc<std::fs::File>,
    /// The store it was opened from (this host's live store or a disk).
    store: nest_types::StoreId,
    opened: std::time::Instant,
    used: std::time::Instant,
}

impl ServeEntry {
    fn live(&self) -> bool {
        self.used.elapsed() < SERVE_TTL && self.opened.elapsed() < SERVE_MAX_AGE
    }
}

impl nest_fabric::ReadSource for Vfs {
    /// Objects already open for serving: the lease and fences are checked,
    /// nothing else (the fabric's completion thread calls this).
    fn open_now(
        &self,
        file: FileId,
        generation: Generation,
    ) -> Option<Result<nest_fabric::ServeFile, NestError>> {
        let f = self.cached_serve_file(ObjectKey::new(file, generation))?;
        Some(Ok(nest_fabric::ServeFile {
            file: f,
            guard: Box::new(self.arc().track_owned(file)),
        }))
    }

    /// The full check (metadata, live replica) and open, off the fabric's
    /// thread; the object stays open for later reads.
    fn open(
        &self,
        file: FileId,
        generation: Generation,
    ) -> futures::future::BoxFuture<'static, Result<nest_fabric::ServeFile, NestError>> {
        let me = self.arc();
        Box::pin(async move {
            let guard = me.clone().track_owned(file);
            let me2 = me.clone();
            let f = tokio::task::spawn_blocking(move || me2.serve_file(file, generation))
                .await
                .map_err(|e| NestError::Io(e.to_string()))??;
            Ok(nest_fabric::ServeFile {
                file: f,
                guard: Box::new(guard),
            })
        })
    }
}

/// Writes other hosts send over RDMA or TCP lanes: applied exactly as a
/// `DataReq::Write` is.
impl nest_fabric::WriteSink for Vfs {
    fn write(
        &self,
        op: nest_fabric::WriteOp,
        data: bytes::Bytes,
    ) -> futures::future::BoxFuture<'static, Result<u32, NestError>> {
        let me = self.arc();
        Box::pin(async move { me.owner_write(op, data).await })
    }
}

/// `Inflight` for reads that outlive a borrow (served through the fabric's
/// ring).
struct InflightOwned {
    vfs: Arc<Vfs>,
    file: FileId,
}

impl Drop for InflightOwned {
    fn drop(&mut self) {
        drop(Inflight {
            vfs: &self.vfs,
            file: self.file,
        });
    }
}

impl Vfs {
    fn track_owned(self: Arc<Self>, file: FileId) -> InflightOwned {
        *self.inflight.lock().entry(file).or_default() += 1;
        InflightOwned { vfs: self, file }
    }
}

#[derive(Clone, Debug)]
enum OwnerOp {
    Write {
        offset: u64,
        append: bool,
        /// Shared, not copied: an RDMA write's bytes stay in the fabric's
        /// staging slot until applied.
        data: bytes::Bytes,
    },
    Truncate(u64),
    Fsync {
        data_only: bool,
    },
}

/// Every few seconds fold open handles' read counters into usage and write
/// the batch on a blocking thread (ADR-028).
async fn fold_usage(vfs: Weak<Vfs>) {
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let Some(v) = vfs.upgrade() else { return };
        let handles: Vec<Arc<Handle>> = v.handles.lock().values().cloned().collect();
        for h in &handles {
            v.fold_handle(h);
        }
        let usage = v.usage.clone();
        drop(v);
        if let Ok(Err(e)) = tokio::task::spawn_blocking(move || usage.flush()).await {
            tracing::warn!(error = %e, "writing usage statistics failed");
        }
    }
}

/// Measure the disk's read rate once the host is idle after boot, then
/// daily (ADR-030). The measurement reads a few GiB with direct I/O, so it
/// waits for a quiet minute rather than competing with real reads.
async fn probe_disk(vfs: Weak<Vfs>) {
    const DAY: u64 = 86_400;
    tokio::time::sleep(Duration::from_secs(60)).await;
    loop {
        let Some(v) = vfs.upgrade() else { return };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let last = crate::diskprobe::load(v.d.store().root()).map_or(0, |b| b.measured_at);
        let due = now.saturating_sub(last) >= DAY;
        if due && v.balancer.idle() && v.handles.lock().is_empty() {
            let store = v.d.store().clone();
            drop(v);
            if let Ok(Some(bps)) =
                tokio::task::spawn_blocking(move || crate::diskprobe::measure(&store)).await
            {
                let Some(v) = vfs.upgrade() else { return };
                let b = crate::diskprobe::DiskBandwidth {
                    bytes_per_s: bps,
                    measured_at: now,
                };
                crate::diskprobe::save(v.d.store().root(), &b);
                v.disk_bps.store(bps, Ordering::Relaxed);
                tracing::info!(bytes_per_s = bps, "disk read rate measured");
            }
            // Each extra disk the same way (ADR-073), one at a time.
            let disks = match vfs.upgrade() {
                Some(v) => v.d.disks(),
                None => return,
            };
            for d in disks {
                let st = d.store.clone();
                if let Ok(Some(bps)) =
                    tokio::task::spawn_blocking(move || crate::diskprobe::measure(&st)).await
                {
                    let b = crate::diskprobe::DiskBandwidth {
                        bytes_per_s: bps,
                        measured_at: now,
                    };
                    crate::diskprobe::save(d.store.root(), &b);
                    tracing::info!(disk = %d.name, bytes_per_s = bps, "disk read rate measured");
                }
            }
            tokio::time::sleep(Duration::from_secs(600)).await;
        } else {
            drop(v);
            tokio::time::sleep(Duration::from_secs(if due { 30 } else { 3600 })).await;
        }
    }
}

/// Snapshot read latencies once a second (windows are differences).
/// Close served objects nobody read lately (see `Vfs::sweep_serve_files`).
async fn sweep_serve_files(vfs: std::sync::Weak<Vfs>) {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tick.tick().await;
        let Some(v) = vfs.upgrade() else { return };
        v.sweep_serve_files();
    }
}

async fn roll_io_stats(io: std::sync::Weak<nest_fabric::iostats::IoStats>) {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tick.tick().await;
        let Some(io) = io.upgrade() else { return };
        io.roll();
    }
}

/// The rendezvous key of a scattered read: its file and 1 MiB region, so a
/// region is always asked of the same holder (ADR-034).
fn region_key(file: FileId, offset: u64) -> u64 {
    file.0.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (offset >> 20)
}

/// A scattered read from another host, reserved and ready to send (see
/// `Vfs::prepare_remote`).
pub struct ScatteredRead {
    vfs: Arc<Vfs>,
    h: Arc<Handle>,
    generation: Generation,
    offset: u64,
    reservation: nest_fabric::Reservation,
    ticket: crate::balance::Ticket,
}

impl ScatteredRead {
    /// Send it; `done` gets the bytes (or an error, after which the caller
    /// should retry through `Vfs::read`, which refreshes and reroutes).
    pub fn start(self, done: nest_fabric::ReadDone) {
        let ScatteredRead {
            vfs,
            h,
            generation,
            offset,
            reservation,
            ticket,
        } = self;
        let file = h.file;
        reservation.send(
            file,
            generation,
            offset,
            Box::new(move |r| {
                match &r {
                    Ok(b) => {
                        ticket.finish(b.len() as u64);
                        vfs.count_read(&h, crate::usage::Source::Remote, b.len() as u64);
                    }
                    Err(_) => {
                        ticket.fail();
                        *h.stable_attr.lock() = None;
                    }
                }
                done(r);
            }),
        );
    }
}

/// One uncached read of exactly `[offset, offset+size)` over `bulk`.
async fn bulk_exact(
    bulk: &Arc<dyn crate::bulk::Bulk>,
    source: NodeId,
    file: FileId,
    generation: Generation,
    offset: u64,
    size: u32,
) -> NestResult<Vec<u8>> {
    let mut out = Vec::with_capacity(size as usize);
    let chunk = bulk.chunk();
    while out.len() < size as usize {
        let want = (size as usize - out.len()).min(chunk);
        let b = bulk
            .clone()
            .fetch(source, file, generation, offset + out.len() as u64, want)
            .await?;
        out.extend_from_slice(&b);
        if b.len() < want {
            break;
        }
    }
    Ok(out)
}
