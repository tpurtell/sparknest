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
    NodeId, Timestamp,
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
    /// Largest readahead window, in fabric chunks.
    pub readahead_chunks: usize,
    /// When a sealed file with a local copy is handed to the kernel.
    pub passthrough: Passthrough,
    /// Read files with several copies from several of them (ADR-030).
    pub balance_reads: bool,
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
            readahead_chunks: 16,
            passthrough: Passthrough::Always,
            balance_reads: true,
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
    /// Sequential readahead state for remote reads over the fabric.
    readahead: tokio::sync::Mutex<Option<crate::readahead::Readahead>>,
    /// Bytes read through this handle not yet folded into usage: local,
    /// remote, archive.
    read_bytes: [AtomicU64; 3],
    /// How reads of a generation are served, decided on its first read:
    /// `Some(sources)` when spread over several copies.
    route: Mutex<Option<(Generation, Option<Route>)>>,
    /// Where this handle's previous direct read ended (u64::MAX: none).
    last_end: AtomicU64,
    /// The file's attributes for fast-path reads of a STABLE generation,
    /// trusted for `ATTR_TTL`: every read names its exact generation, so a
    /// rewrite in between fails as Stale and the slow path refreshes.
    stable_attr: Mutex<Option<(FileAttr, std::time::Instant)>>,
}

/// A generation readable from several copies.
#[derive(Clone)]
struct Route {
    sources: Vec<crate::balance::Source>,
    local: Option<Arc<nest_store::ObjectStore>>,
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
    /// Which files are read in scattered pieces (read directly).
    patterns: Arc<crate::pattern::Patterns>,
    /// Read latencies by kind and size, over recent windows.
    io: Arc<nest_fabric::iostats::IoStats>,
    /// This host's measured disk read rate (bytes/s; 0 until measured).
    disk_bps: Arc<AtomicU64>,
    /// Revocation state per (file, epoch) this node was granted.
    fences: Mutex<HashMap<(FileId, Epoch), watch::Receiver<bool>>>,
    /// Local reads in progress per file (fencing waits for them).
    inflight: Mutex<HashMap<FileId, u32>>,
    /// Objects this host serves to others, kept open: fully re-checked
    /// (metadata, live replica) at most every `SERVE_TTL`, and in between
    /// only against the lease and fences. STABLE generations only: their
    /// bytes never change.
    serve_files: Mutex<HashMap<ObjectKey, (Arc<std::fs::File>, std::time::Instant)>>,
    inflight_done: tokio::sync::Notify,
    fence_hooks: Mutex<Vec<FenceHook>>,
    fabric: Mutex<Option<Arc<nest_fabric::Fabric>>>,
}

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
            patterns: Arc::default(),
            io: Arc::default(),
            disk_bps: Arc::new(AtomicU64::new(
                crate::diskprobe::load(d.store().root()).map_or(0, |b| b.bytes_per_s),
            )),
            fences: Mutex::new(HashMap::new()),
            inflight: Mutex::new(HashMap::new()),
            serve_files: Mutex::new(HashMap::new()),
            inflight_done: tokio::sync::Notify::new(),
            fence_hooks: Mutex::new(Vec::new()),
            fabric: Mutex::new(None),
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
        // A verdict on how a file is read goes into its metadata, so other
        // hosts and restarts start from it (ADR-031).
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            let weak = Arc::downgrade(&vfs);
            vfs.patterns.set_on_change(Box::new(move |file, scattered| {
                let weak = weak.clone();
                rt.spawn(async move {
                    let Some(v) = weak.upgrade() else { return };
                    if let Err(e) = v.propose(Command::SetReadPattern { file, scattered }).await {
                        tracing::debug!(file = file.0, error = %e, "could not record the read pattern");
                    }
                });
            }));
        }
        // Ownerships granted before this VFS existed (startup) are finalized
        // by the data node; nothing to adopt here.
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(fold_usage(Arc::downgrade(&vfs)));
            tokio::spawn(roll_io_stats(Arc::downgrade(&vfs.io)));
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

    /// This host's RDMA link rate, bytes/s (0 without a fabric).
    pub fn link_bps(&self) -> u64 {
        self.fabric().map_or(0, |f| f.link_bytes_per_s())
    }

    /// The copies a STABLE generation can be read from, when there are
    /// several: this host's live copy and every other host's.
    fn route_for(&self, a: &FileAttr) -> Option<Route> {
        use crate::balance::Source;
        if !self.cfg.balance_reads || a.gen_state != GenState::Stable || self.fabric().is_none() {
            return None;
        }
        let key = ObjectKey::new(a.id, a.generation);
        let local = self.d.servable(key).then(|| self.d.store().clone());
        let me = self.me();
        let mut sources: Vec<Source> = self
            .q(|c| query::replicas(c, a.id))
            .ok()?
            .into_iter()
            .filter(|r| {
                r.generation == a.generation
                    && r.state == nest_types::ReplicaState::Live
                    && !crate::is_archive(r.store)
                    && NodeId(r.store.0) != me
            })
            .map(|r| Source::Peer(NodeId(r.store.0)))
            .collect();
        if local.is_some() {
            sources.insert(0, Source::Local);
        }
        (sources.len() >= 2).then_some(Route { sources, local })
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

    /// A direct read of a scattered file: note whether it continued the
    /// handle's previous one (streams switch back to readahead).
    fn note_direct(&self, h: &Handle, offset: u64, n: u64) {
        let seq = h.last_end.swap(offset + n, Ordering::Relaxed) == offset;
        self.patterns.direct(h.file, n, seq);
    }

    /// Readahead chunks dropped on this host, and bytes of them consumed.
    pub fn readahead_totals(&self) -> (u64, u64) {
        self.patterns.readahead_totals()
    }

    /// Read latencies and readahead waste over the last 10 s, 1 min, 10 min.
    pub fn io_windows(&self) -> Vec<nest_fabric::iostats::Window> {
        self.io.windows()
    }

    /// Files this host currently reads directly (scattered reads).
    pub fn scattered_files(&self) -> usize {
        self.patterns.random_files()
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
        fabric.set_io_stats(self.io.clone());
        // The fabric holds a Weak; keep the trait object alive with us.
        std::mem::forget(src);
        *self.fabric.lock() = Some(fabric);
    }

    pub fn fabric(&self) -> Option<Arc<nest_fabric::Fabric>> {
        self.fabric.lock().clone()
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

    fn rpc(&self) -> &nest_rpc::Rpc {
        self.d.meta().rpc()
    }

    async fn call(&self, peer: NodeId, req: &DataReq) -> NestResult<DataResp> {
        remote::call(self.rpc(), peer, req, self.cfg.rpc_timeout).await
    }

    // ------------------------------------------------------------ effects

    fn on_effect(&self, index: u64, e: &Effect) {
        match e {
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

    async fn overlay_owned(&self, a: &mut FileAttr) {
        let (GenState::Owned, Some(owner)) = (a.gen_state, a.owner) else {
            return;
        };
        if owner == self.me() {
            if let Ok((size, mtime)) = self.d.store().stat(ObjectKey::new(a.id, a.generation)) {
                a.size = size;
                a.mtime = mtime;
            }
            return;
        }
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
            let owner = self.choose_owner(&a, discard).await?;
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

    /// Prefer this node (it holds the content, or the content is being
    /// discarded); otherwise a reachable node holding a live copy, so a large
    /// file is never moved just to edit it.
    async fn choose_owner(&self, a: &FileAttr, discard: bool) -> NestResult<NodeId> {
        let me = self.me();
        if discard || a.size == 0 || self.d.servable(ObjectKey::new(a.id, a.generation)) {
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
    fn holders(&self, a: &FileAttr) -> NestResult<Vec<NodeId>> {
        let replicas: Vec<_> = self
            .q(|c| query::replicas(c, a.id))?
            .into_iter()
            .filter(|r| r.generation == a.generation && r.state == nest_types::ReplicaState::Live)
            .collect();
        let mut nodes: Vec<NodeId> = replicas
            .iter()
            .filter(|r| !crate::is_archive(r.store))
            .map(|r| NodeId(r.store.0))
            .filter(|n| *n != self.me())
            .collect();
        if !nodes.is_empty() {
            let k = (a.id.0 as usize) % nodes.len();
            nodes.rotate_left(k);
        }
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
        let resp = tokio::task::spawn_blocking(move || -> std::io::Result<DataResp> {
            match op {
                OwnerOp::Write {
                    offset,
                    append,
                    data,
                } => {
                    let off = if append { f.metadata()?.len() } else { offset };
                    f.write_all_at(&data, off)?;
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
            } => {
                self.owner_mutate(
                    file,
                    epoch,
                    Some(writer),
                    OwnerOp::Write {
                        offset,
                        append,
                        data,
                    },
                )
                .await
            }
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
        let key = ObjectKey::new(a.id, a.generation);
        match (a.gen_state, a.owner) {
            (GenState::Owned, Some(o)) if o == self.me() => Some(self.d.store().clone()),
            (GenState::Stable, _) => {
                if self.d.servable(key) {
                    return Some(self.d.store().clone());
                }
                let replicas = self.q(|c| query::replicas(c, a.id)).ok()?;
                replicas
                    .into_iter()
                    .filter(|r| r.generation == a.generation && crate::is_archive(r.store))
                    .find_map(|r| self.d.servable_archive(r.store, key))
                    .map(|arch| arch.store.clone())
            }
            _ => None,
        }
    }

    /// Where this node reads `a` for itself: its live store, or a local
    /// archive copy only when no other node holds a live one. A live copy
    /// over the fabric (5–15 GB/s) beats an archive disk (a SATA SSD or a
    /// NAS share); the archive stays the fallback when those holders fail.
    fn own_read_source(&self, a: &FileAttr) -> Option<Arc<nest_store::ObjectStore>> {
        let src = self.local_source(a)?;
        if Arc::ptr_eq(&src, self.d.store()) || !self.live_elsewhere(a) {
            Some(src)
        } else {
            None
        }
    }

    /// Whether another node holds a live (non-archive) copy of `a`'s generation.
    fn live_elsewhere(&self, a: &FileAttr) -> bool {
        let me = self.me();
        self.q(|c| query::replicas(c, a.id))
            .map(|rs| {
                rs.iter().any(|r| {
                    r.generation == a.generation
                        && r.state == nest_types::ReplicaState::Live
                        && !crate::is_archive(r.store)
                        && NodeId(r.store.0) != me
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
        if a.gen_state == GenState::Stable && self.d.servable(key) {
            let mut m = self.serve_files.lock();
            if m.len() >= SERVE_FILES_MAX {
                m.retain(|_, (_, at)| at.elapsed() < SERVE_TTL);
                if m.len() >= SERVE_FILES_MAX {
                    m.clear();
                }
            }
            m.insert(key, (f.clone(), std::time::Instant::now()));
        }
        Ok(f)
    }

    fn cached_serve_file(&self, key: ObjectKey) -> Option<Arc<std::fs::File>> {
        let f = match self.serve_files.lock().get(&key) {
            Some((f, at)) if at.elapsed() < SERVE_TTL => f.clone(),
            _ => return None,
        };
        self.d.may_serve_now(key).then_some(f)
    }

    fn servable_object(&self, file: FileId, generation: Generation) -> NestResult<std::fs::File> {
        let a = self.raw_attr(file)?;
        if a.generation != generation {
            return Err(NestError::Stale);
        }
        let key = ObjectKey::new(file, generation);
        let Some(src) = self.local_source(&a) else {
            return Err(NestError::Unavailable(
                "no copy of that generation here".into(),
            ));
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
            readahead: tokio::sync::Mutex::new(None),
            read_bytes: Default::default(),
            route: Mutex::new(None),
            last_end: AtomicU64::new(u64::MAX),
            stable_attr: Mutex::new(None),
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

    fn choose_mode(&self, a: &FileAttr, writable: bool) -> OpenMode {
        if a.sealed && !writable && a.gen_state == GenState::Stable {
            let key = ObjectKey::new(a.id, a.generation);
            let allowed = match self.cfg.passthrough {
                Passthrough::Always => true,
                // Other copies exist: let the daemon spread the reads,
                // unless they are scattered (the local disk answers those
                // fastest).
                Passthrough::Sole => !self.live_elsewhere(a) || self.patterns.is_random(a.id),
                Passthrough::Never => false,
            };
            if allowed
                && self.d.servable(key)
                && let Ok(f) = self.d.store().open_read(key)
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
        if !writable && let Ok(scattered) = self.q(|c| query::read_scattered(c, file)) {
            self.patterns.seed(file, scattered);
        }
        let mode = self.choose_mode(&a, writable);
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

    pub async fn read(&self, fh: u64, offset: u64, size: u32) -> NestResult<Vec<u8>> {
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
            // Read in scattered pieces: exactly what is asked, no readahead
            // (ADR-031).
            let scattered = self.patterns.is_random(h.file);
            // Several copies: spread chunks over them (ADR-030).
            if !scattered
                && let Some(route) = self.route(&h, &a)
                && let Some(fab) = self.fabric()
            {
                let _t = self.track(h.file);
                let mut ra = h.readahead.lock().await;
                if !ra.as_ref().is_some_and(|r| r.matches(h.file, a.generation)) {
                    *ra = Some(crate::readahead::Readahead::new(
                        &fab,
                        self.balancer.clone(),
                        self.patterns.clone(),
                        self.io.clone(),
                        h.file,
                        a.generation,
                        route.sources.clone(),
                        route.local.clone(),
                        self.cfg.readahead_chunks,
                        Some(a.size),
                    ));
                }
                let r = ra.as_mut().expect("just set");
                match r.read(&fab, offset, size).await {
                    Ok(b) => {
                        let (l, rem) = r.take_served();
                        self.count_read(&h, crate::usage::Source::Local, l);
                        self.count_read(&h, crate::usage::Source::Remote, rem);
                        return Ok(b);
                    }
                    Err(e) => {
                        // No copy could serve it: decide afresh.
                        *ra = None;
                        *h.route.lock() = None;
                        last = e;
                        continue;
                    }
                }
            }
            if let Some(src) = self.own_read_source(&a) {
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
                    if scattered {
                        self.note_direct(&h, offset, b.len() as u64);
                    }
                }
                return r;
            }
            // Scattered, no local copy: each read from whichever copy is
            // least busy, over every copy (no stripes, no cap).
            if scattered
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
                    let ticket = self.balancer.pick_least_busy(&peers);
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
                            self.note_direct(&h, offset, b.len() as u64);
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
                if let Some(fab) = self.fabric() {
                    let r = if a.gen_state == GenState::Stable && !scattered {
                        // Immutable generation: read ahead in whole chunks.
                        let mut ra = h.readahead.lock().await;
                        if !ra.as_ref().is_some_and(|r| r.matches(h.file, a.generation)) {
                            *ra = Some(crate::readahead::Readahead::new(
                                &fab,
                                self.balancer.clone(),
                                self.patterns.clone(),
                                self.io.clone(),
                                h.file,
                                a.generation,
                                vec![crate::balance::Source::Peer(s)],
                                None,
                                self.cfg.readahead_chunks,
                                Some(a.size),
                            ));
                        }
                        ra.as_mut()
                            .expect("just set")
                            .read(&fab, offset, size)
                            .await
                    } else {
                        // Being written by its owner: fetch exactly this
                        // range, keep nothing.
                        self.fabric_exact(&fab, s, h.file, a.generation, offset, size)
                            .await
                    };
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
                            // Fabric trouble: fall back to TCP for this read.
                            tracing::debug!(peer = %s, error = %e, "RDMA read failed; using TCP");
                            *h.readahead.lock().await = None;
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
    /// from the page cache) or bytes readahead already holds. Returns `None`
    /// when the read needs I/O across the network or a state change.
    ///
    /// Frontends call this on the thread that received the request: on the
    /// Sparks every thread hand-off can cost a deep-idle wake (hundreds of
    /// microseconds), which dominated small-request latency.
    pub fn try_read_now(&self, fh: u64, offset: u64, size: u32) -> Option<NestResult<Vec<u8>>> {
        self.try_read_now_with(fh, offset, size, |f| pread(f, offset, size), Ok)
    }

    /// `try_read_now` into a caller's buffer (FUSE over io_uring reads
    /// straight into the ring entry the kernel copies from).
    pub fn try_read_into(&self, fh: u64, offset: u64, buf: &mut [u8]) -> Option<NestResult<usize>> {
        let size = buf.len() as u32;
        // Exactly one of the two closures runs; both need the buffer.
        let local = std::cell::RefCell::new(buf);
        self.try_read_now_with(
            fh,
            offset,
            size,
            |f| pread_into(f, offset, &mut local.borrow_mut()),
            |v| {
                let mut b = local.borrow_mut();
                b[..v.len()].copy_from_slice(&v);
                Ok(v.len())
            },
        )
    }

    fn try_read_now_with<R>(
        &self,
        fh: u64,
        offset: u64,
        size: u32,
        from_file: impl FnOnce(&std::fs::File) -> NestResult<R>,
        from_readahead: impl FnOnce(Vec<u8>) -> NestResult<R>,
    ) -> Option<NestResult<R>> {
        let h = self.handle(fh).ok()?;
        let a = self.fast_attr(&h)?;
        let key = ObjectKey::new(h.file, a.generation);
        // Until the first read has decided how this generation is served,
        // the slow path decides; a spread read only serves ready chunks here.
        // Scattered reads go straight to a local copy (ADR-031).
        let scattered = self.patterns.is_random(h.file);
        let spread = if scattered {
            false
        } else if self.cfg.balance_reads && a.gen_state == GenState::Stable {
            match h.route.lock().as_ref() {
                Some((g, r)) if *g == a.generation => r.is_some(),
                _ => return None,
            }
        } else {
            false
        };
        if !spread && let Some(src) = self.own_read_source(&a) {
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
                if scattered {
                    self.note_direct(&h, offset, n);
                }
            }
            return Some(r);
        }
        // Scattered, no copy here: fetch exactly this range from the least
        // busy copy on this thread, parked until the answer arrives (a page
        // fault served without a trip through the runtime).
        if scattered && a.gen_state == GenState::Stable {
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
            let n = a.size.saturating_sub(offset).min(size as u64) as usize;
            let mut buf = vec![0u8; n];
            let ticket = self.balancer.pick_least_busy(&peers);
            let crate::balance::Source::Peer(node) = ticket.source() else {
                return None;
            };
            let _t = self.track(h.file);
            return match fab.read_now(node, h.file, a.generation, offset, &mut buf) {
                Some(Ok(got)) => {
                    ticket.finish(got as u64);
                    buf.truncate(got);
                    self.count_read(&h, crate::usage::Source::Remote, got as u64);
                    self.note_direct(&h, offset, got as u64);
                    Some(from_readahead(buf))
                }
                // Let the async path handle it (and its retries).
                Some(Err(_)) => {
                    ticket.fail();
                    *h.stable_attr.lock() = None;
                    None
                }
                None => None,
            };
        }
        // Only STABLE generations are cached ahead: their bytes never change.
        if a.gen_state != GenState::Stable || scattered {
            return None;
        }
        let fab = self.fabric()?;
        let mut ra = h.readahead.try_lock().ok()?;
        let r = ra.as_mut()?;
        if !r.matches(h.file, a.generation) {
            return None;
        }
        let out = r.try_ready(offset, size)?;
        r.advance(&fab, offset, size);
        let (l, rem) = r.take_served();
        self.count_read(&h, crate::usage::Source::Local, l);
        self.count_read(&h, crate::usage::Source::Remote, rem);
        Some(from_readahead(out))
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
            let off = if h.append {
                o.file.metadata()?.len()
            } else {
                offset
            };
            o.file.write_all_at(data, off)?;
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
            data,
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
                let req = match &op {
                    OwnerOp::Write {
                        offset,
                        append,
                        data,
                    } => DataReq::Write {
                        file: h.file,
                        epoch,
                        writer,
                        offset: *offset,
                        append: *append,
                        data: data.clone(),
                    },
                    OwnerOp::Truncate(size) => DataReq::Truncate {
                        file: h.file,
                        epoch,
                        writer: Some(writer),
                        size: *size,
                    },
                    OwnerOp::Fsync { data_only } => DataReq::Fsync {
                        file: h.file,
                        epoch,
                        data_only: *data_only,
                    },
                };
                let r = self.call(owner, &req).await;
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
        let dest: Arc<nest_store::ObjectStore> = if target == self.me().live_store() {
            self.d.store().clone()
        } else {
            match self.d.archive(target) {
                Some(a) => a.store.clone(),
                None => {
                    return Err(NestError::Unavailable(format!(
                        "store {target} is not reachable through this node (not a gateway, or its marker is missing)"
                    )));
                }
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
        self.fetch_into(&a, &dest).await?;
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
            let copied = match src {
                None => self.copy_local(a, dest).await,
                Some(n) => self.copy_from(n, a, dest).await,
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
    ) -> NestResult<()> {
        let key = ObjectKey::new(a.id, a.generation);
        let store = dest.clone();
        let staging = tokio::task::spawn_blocking(move || store.begin_staging(key))
            .await
            .expect("blocking task")
            .map_err(io)?;
        let staging = Arc::new(Mutex::new(Some(staging)));
        let fab = self.fabric();
        let chunk: u64 = fab.as_ref().map(|f| f.chunk() as u64).unwrap_or(4 << 20);
        let window = 16usize;
        let mut next = 0u64;
        let mut inflight = futures::stream::FuturesUnordered::new();
        use futures::StreamExt;
        let fetch = |off: u64| {
            let (fab, st) = (fab.clone(), staging.clone());
            let (file, generation, size) = (a.id, a.generation, a.size);
            async move {
                let len = (size - off).min(chunk) as usize;
                let bytes: Vec<u8> = match &fab {
                    Some(f) => match f.read(src, file, generation, off, len).await {
                        Ok(b) => b.as_slice().to_vec(),
                        Err(NestError::Stale) => return Err(NestError::Stale),
                        Err(_) => {
                            self.tcp_read(src, file, generation, off, len as u32)
                                .await?
                        }
                    },
                    None => {
                        self.tcp_read(src, file, generation, off, len as u32)
                            .await?
                    }
                };
                if bytes.len() != len {
                    return Err(NestError::Io(format!(
                        "short read at {off}: {} of {len} bytes",
                        bytes.len()
                    )));
                }
                tokio::task::spawn_blocking(move || {
                    let g = st.lock();
                    let f = g.as_ref().expect("staging open").file();
                    f.write_all_at(&bytes, off)
                })
                .await
                .expect("blocking task")
                .map_err(io)
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
        let st = staging.lock().take().expect("staging open");
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
    /// node is a gateway for) with `copy_file_range`, then commit.
    async fn copy_local(
        &self,
        a: &FileAttr,
        dest: &Arc<nest_store::ObjectStore>,
    ) -> NestResult<()> {
        let key = ObjectKey::new(a.id, a.generation);
        let src = self
            .local_source(a)
            .ok_or_else(|| NestError::Unavailable("the local copy is no longer servable".into()))?;
        let dest = dest.clone();
        let size = a.size;
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let from = src.open_read(key)?;
            let st = dest.begin_staging(key)?;
            use std::os::fd::AsRawFd;
            let (mut off_in, mut off_out): (libc::loff_t, libc::loff_t) = (0, 0);
            while (off_in as u64) < size {
                let want = ((size - off_in as u64) as usize).min(1 << 30);
                // SAFETY: both fds are open for the duration; offsets are
                // valid pointers to locals.
                let n = unsafe {
                    libc::copy_file_range(
                        from.as_raw_fd(),
                        &mut off_in,
                        st.file().as_raw_fd(),
                        &mut off_out,
                        want,
                        0,
                    )
                };
                if n < 0 {
                    let e = std::io::Error::last_os_error();
                    // Different filesystem types (ext4 to CIFS, ...) or no
                    // support: finish with an ordinary buffered copy.
                    if matches!(
                        e.raw_os_error(),
                        Some(libc::EXDEV | libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP)
                    ) {
                        use std::os::unix::fs::FileExt;
                        let mut buf = vec![0u8; 4 << 20];
                        while (off_in as u64) < size {
                            let want = ((size - off_in as u64) as usize).min(buf.len());
                            let got = from.read_at(&mut buf[..want], off_in as u64)?;
                            if got == 0 {
                                return Err(std::io::Error::other(format!(
                                    "short copy: {off_in} of {size} bytes"
                                )));
                            }
                            st.file().write_all_at(&buf[..got], off_out as u64)?;
                            off_in += got as libc::loff_t;
                            off_out += got as libc::loff_t;
                        }
                        break;
                    }
                    return Err(e);
                }
                if n == 0 {
                    return Err(std::io::Error::other(format!(
                        "short copy: {off_in} of {size} bytes"
                    )));
                }
            }
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

    // ------------------------------------------------------------ misc

    pub fn statfs(&self) -> NestResult<(nest_store::Capacity, u64)> {
        let cap = self.d.store().capacity().map_err(io)?;
        let t = self.q(query::totals)?;
        Ok((cap, t.files + t.dirs))
    }
}

/// How long a handle trusts a STABLE generation's attributes on the fast
/// path.
const ATTR_TTL: std::time::Duration = std::time::Duration::from_secs(1);

/// How long an open served object is trusted before a full re-check.
const SERVE_TTL: std::time::Duration = std::time::Duration::from_secs(2);
const SERVE_FILES_MAX: usize = 4096;

impl nest_fabric::ReadSource for Vfs {
    /// Small reads of objects already open for serving: one pread, on the
    /// fabric's completion thread, no queries (the first read of an object
    /// takes the async path and opens it).
    fn try_read_now(
        &self,
        file: FileId,
        generation: Generation,
        offset: u64,
        dst: &mut [u8],
    ) -> Option<Result<usize, NestError>> {
        let f = self.cached_serve_file(ObjectKey::new(file, generation))?;
        let _t = self.track(file);
        let mut done = 0;
        Some(loop {
            if done == dst.len() {
                break Ok(done);
            }
            match f.read_at(&mut dst[done..], offset + done as u64) {
                Ok(0) => break Ok(done),
                Ok(n) => done += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => break Err(io(e)),
            }
        })
    }

    fn read_into(
        &self,
        file: FileId,
        generation: Generation,
        offset: u64,
        len: usize,
        mut slot: nest_fabric::Slot,
    ) -> futures::future::BoxFuture<'static, (nest_fabric::Slot, Result<usize, NestError>)> {
        let me = self.arc();
        Box::pin(async move {
            let _t = me.track(file);
            let f = match me.serve_file(file, generation) {
                Ok(f) => f,
                Err(e) => return (slot, Err(e)),
            };
            tokio::task::spawn_blocking(move || {
                let buf = slot.as_mut_slice(len);
                let mut done = 0;
                let r = loop {
                    if done == buf.len() {
                        break Ok(done);
                    }
                    match f.read_at(&mut buf[done..], offset + done as u64) {
                        Ok(0) => break Ok(done),
                        Ok(n) => done += n,
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(e) => break Err(io(e)),
                    }
                };
                (slot, r)
            })
            .await
            .expect("blocking task")
        })
    }
}

#[derive(Clone, Debug)]
enum OwnerOp {
    Write {
        offset: u64,
        append: bool,
        data: Vec<u8>,
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
            tokio::time::sleep(Duration::from_secs(600)).await;
        } else {
            drop(v);
            tokio::time::sleep(Duration::from_secs(if due { 30 } else { 3600 })).await;
        }
    }
}

/// Snapshot read latencies once a second (windows are differences).
async fn roll_io_stats(io: std::sync::Weak<nest_fabric::iostats::IoStats>) {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tick.tick().await;
        let Some(io) = io.upgrade() else { return };
        io.roll();
    }
}
