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
}

impl Default for VfsConfig {
    fn default() -> Self {
        VfsConfig {
            finalize_linger: Duration::from_millis(250),
            catch_up_wait: Duration::from_secs(10),
            rpc_timeout: Duration::from_secs(10),
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
    d: Arc<DataNode>,
    cfg: VfsConfig,
    weak: Weak<Vfs>,
    readers: Mutex<Vec<Connection>>,
    handles: Mutex<HashMap<u64, Arc<Handle>>>,
    next_fh: AtomicU64,
    owned: Mutex<HashMap<FileId, Arc<Owned>>>,
    /// Revocation state per (file, epoch) this node was granted.
    fences: Mutex<HashMap<(FileId, Epoch), watch::Receiver<bool>>>,
    /// Local reads in progress per file (fencing waits for them).
    inflight: Mutex<HashMap<FileId, u32>>,
    inflight_done: tokio::sync::Notify,
    fence_hooks: Mutex<Vec<FenceHook>>,
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
            d: d.clone(),
            cfg,
            weak: weak.clone(),
            readers: Mutex::new(Vec::new()),
            handles: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            owned: Mutex::new(HashMap::new()),
            fences: Mutex::new(HashMap::new()),
            inflight: Mutex::new(HashMap::new()),
            inflight_done: tokio::sync::Notify::new(),
            fence_hooks: Mutex::new(Vec::new()),
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
        vfs
    }

    pub fn data(&self) -> &Arc<DataNode> {
        &self.d
    }

    /// Register a blocking page-cache invalidation run during fencing.
    pub fn add_fence_hook(&self, hook: FenceHook) {
        self.fence_hooks.lock().push(hook);
    }

    fn arc(&self) -> Arc<Vfs> {
        self.weak.upgrade().expect("Vfs used after drop")
    }

    /// Run a closure with a pooled read-only metadata connection.
    fn q<T>(&self, f: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> NestResult<T> {
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

    fn raw_attr(&self, file: FileId) -> NestResult<FileAttr> {
        self.q(|c| query::getattr(c, file))?
            .ok_or(NestError::NotFound)
    }

    async fn propose(&self, cmd: Command) -> NestResult<Reply> {
        self.d.meta().propose(cmd).await
    }

    fn me(&self) -> NodeId {
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
            let m = self
                .d
                .meta()
                .raft()
                .metrics()
                .borrow()
                .membership_config
                .clone();
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
        let mut nodes: Vec<NodeId> = self
            .q(|c| query::replicas(c, a.id))?
            .into_iter()
            .filter(|r| r.generation == a.generation && r.state == nest_types::ReplicaState::Live)
            .map(|r| NodeId(r.store.0))
            .filter(|n| *n != self.me())
            .collect();
        if !nodes.is_empty() {
            let k = (a.id.0 as usize) % nodes.len();
            nodes.rotate_left(k);
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
                OwnerOp::Fsync => {
                    f.sync_all()?;
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

    async fn owner_fsync(&self, file: FileId, epoch: Epoch) -> NestResult<()> {
        self.owner_mutate(file, epoch, None, OwnerOp::Fsync).await?;
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
            DataReq::Fsync { file, epoch } => {
                self.owner_fsync(file, epoch).await.map(|_| DataResp::Done)
            }
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

    /// Read bytes of exactly `generation` from this node, if it may serve it.
    async fn serve_read(
        &self,
        file: FileId,
        generation: Generation,
        offset: u64,
        len: u32,
    ) -> NestResult<Vec<u8>> {
        let _t = self.track(file);
        let a = self.raw_attr(file)?;
        if a.generation != generation {
            return Err(NestError::Stale);
        }
        let key = ObjectKey::new(file, generation);
        let ok = match (a.gen_state, a.owner) {
            (GenState::Owned, Some(o)) => o == self.me(),
            (GenState::Stable, _) => self.d.servable(key),
            _ => false,
        };
        if !ok {
            return Err(NestError::Unavailable(
                "no copy of that generation here".into(),
            ));
        }
        let f = self.d.store().open_read(key).map_err(io)?;
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
            if self.d.servable(key)
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

    pub async fn fsync(&self, fh: u64) -> NestResult<()> {
        let h = self.handle(fh)?;
        let a = self.raw_attr(h.file)?;
        match (a.gen_state, a.owner) {
            (GenState::Owned, Some(o)) if o == self.me() => self.owner_fsync(h.file, a.epoch).await,
            (GenState::Owned, Some(o)) => {
                match self
                    .call(
                        o,
                        &DataReq::Fsync {
                            file: h.file,
                            epoch: a.epoch,
                        },
                    )
                    .await
                {
                    Ok(_) | Err(NestError::Stale) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            // Stable content is already settled.
            _ => Ok(()),
        }
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
            let local = match (a.gen_state, a.owner) {
                (GenState::Owned, Some(o)) => o == self.me(),
                (GenState::Stable, _) => self.d.servable(key),
                _ => false,
            };
            if local {
                let _t = self.track(h.file);
                let f = {
                    let mut r = h.reader.lock();
                    match r.as_ref() {
                        Some((k, f)) if *k == key => f.clone(),
                        _ => {
                            let f = Arc::new(self.d.store().open_read(key).map_err(io)?);
                            *r = Some((key, f.clone()));
                            f
                        }
                    }
                };
                return tokio::task::spawn_blocking(move || pread(&f, offset, size))
                    .await
                    .expect("blocking task");
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
                    Ok(DataResp::Data(b)) => return Ok(b),
                    Ok(other) => last = NestError::Io(format!("unexpected response {other:?}")),
                    Err(NestError::Stale) => {
                        last = NestError::Stale;
                        break; // generation moved: refresh
                    }
                    Err(e) => last = e,
                }
            }
        }
        Err(last)
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
                    OwnerOp::Fsync => DataReq::Fsync {
                        file: h.file,
                        epoch,
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

    // ------------------------------------------------------------ misc

    pub fn statfs(&self) -> NestResult<(nest_store::Capacity, u64)> {
        let cap = self.d.store().capacity().map_err(io)?;
        let t = self.q(query::totals)?;
        Ok((cap, t.files + t.dirs))
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
    Fsync,
}
