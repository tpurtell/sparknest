//! The filesystem as seen by a mount on this node, independent of FUSE.
//!
//! Every operation takes and returns plain values and `NestResult`, so the
//! full POSIX-facing behaviour can be tested across nodes without mounting.
//! `nest-fuse` is a thin adapter over this.
//!
//! Content lifecycle (PROPOSAL §6, ADR-011):
//! - Opening never changes ownership. The first mutation (write, truncate,
//!   O_TRUNC) of a STABLE file makes this node the owner of a new
//!   generation; every other copy is invalidated in the same commit.
//! - Handles that created or wrote the file participate in the write epoch.
//!   When the last participant closes, ownership lingers briefly (so
//!   close/reopen/append patterns do not churn generations) and then the
//!   generation is finalized.
//! - Unsealed files are always served through the daemon (direct I/O);
//!   sealed files may use kernel passthrough or the page cache.

use crate::DataNode;
use nest_meta::{Command, LockKind, RenameFlags, Reply, query};
use nest_store::ObjectKey;
use nest_types::{
    DirEntry, Epoch, FileAttr, FileId, FileKind, GenState, NestError, NestResult, Timestamp,
};
use parking_lot::Mutex;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct VfsConfig {
    /// How long ownership survives the last writer closing before the
    /// generation is finalized.
    pub finalize_linger: Duration,
    /// How long reads wait for the node to catch up after start.
    pub catch_up_wait: Duration,
}

impl Default for VfsConfig {
    fn default() -> Self {
        VfsConfig {
            finalize_linger: Duration::from_millis(250),
            catch_up_wait: Duration::from_secs(10),
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

struct Handle {
    file: FileId,
    writable: bool,
    append: bool,
    /// Cached object for reads, revalidated against the current generation.
    reader: Mutex<Option<(ObjectKey, Arc<std::fs::File>)>>,
    /// Lock owners that took locks through this handle.
    lock_owners: Mutex<HashSet<u64>>,
}

/// This node's view of a file it owns.
struct Owned {
    key: ObjectKey,
    epoch: Epoch,
    file: Arc<std::fs::File>,
    participants: Mutex<HashSet<u64>>,
    /// Writes hold this shared while they run; finalize takes it exclusive
    /// and flips it to closed, so no write can land in a finalized object.
    open: tokio::sync::RwLock<bool>,
    /// Bumped on every write; a lingering finalize aborts if it moved.
    activity: AtomicU64,
}

pub struct Vfs {
    d: Arc<DataNode>,
    cfg: VfsConfig,
    readers: Mutex<Vec<Connection>>,
    handles: Mutex<HashMap<u64, Arc<Handle>>>,
    next_fh: AtomicU64,
    owned: Mutex<HashMap<FileId, Arc<Owned>>>,
}

fn io(e: std::io::Error) -> NestError {
    NestError::from_io(&e)
}

fn sql(e: rusqlite::Error) -> NestError {
    NestError::Io(format!("metadata: {e}"))
}

impl Vfs {
    pub fn new(d: Arc<DataNode>, cfg: VfsConfig) -> Arc<Vfs> {
        Arc::new(Vfs {
            d,
            cfg,
            readers: Mutex::new(Vec::new()),
            handles: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            owned: Mutex::new(HashMap::new()),
        })
    }

    pub fn data(&self) -> &Arc<DataNode> {
        &self.d
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

    async fn propose(&self, cmd: Command) -> NestResult<Reply> {
        self.d.meta().propose(cmd).await
    }

    fn me(&self) -> nest_types::NodeId {
        self.d.id()
    }

    // ------------------------------------------------------------ attributes

    /// Attributes as a local caller should see them: while this node owns a
    /// file, size and mtime come from the working object.
    pub fn getattr(&self, file: FileId) -> NestResult<FileAttr> {
        let mut a = self
            .q(|c| query::getattr(c, file))?
            .ok_or(NestError::NotFound)?;
        self.overlay_owned(&mut a);
        Ok(a)
    }

    fn overlay_owned(&self, a: &mut FileAttr) {
        if a.gen_state == GenState::Owned
            && a.owner == Some(self.me())
            && let Ok((size, mtime)) = self.d.store().stat(ObjectKey::new(a.id, a.generation))
        {
            // The working object carries the live size and mtime; explicit
            // time changes are applied to it too (see setattr).
            a.size = size;
            a.mtime = mtime;
        }
    }

    pub fn lookup(&self, parent: FileId, name: &[u8]) -> NestResult<FileAttr> {
        let mut a = self
            .q(|c| query::lookup_attr(c, parent, name))?
            .ok_or(NestError::NotFound)?;
        self.overlay_owned(&mut a);
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
        let a = self.getattr(dir)?;
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
        self.overlay_owned(&mut a);
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
        self: &Arc<Self>,
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
            // While this node owns the file, its size and mtime come from
            // the working object: keep the object's mtime in step so the
            // change survives reads and finalize.
            let a = self
                .q(|c| query::getattr(c, file))?
                .ok_or(NestError::NotFound)?;
            if a.gen_state == GenState::Owned && a.owner == Some(self.me()) {
                let key = ObjectKey::new(file, a.generation);
                let f = self.d.store().open_write(key).map_err(io)?;
                f.set_times(std::fs::FileTimes::new().set_modified(mtime.as_system_time()))
                    .map_err(io)?;
            }
        }
        self.getattr(file)
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

    /// Become (or confirm being) the owner of `file` and return the local
    /// working-object state. `truncate_to_zero` lets a node without the old
    /// content take ownership (O_TRUNC, truncate(0)).
    async fn ensure_owned(&self, file: FileId, truncate_to_zero: bool) -> NestResult<Arc<Owned>> {
        for _ in 0..16 {
            let existing = self.owned.lock().get(&file).cloned();
            if let Some(o) = existing
                && *o.open.read().await
            {
                return Ok(o);
            }
            let a = self
                .q(|c| query::getattr(c, file))?
                .ok_or(NestError::NotFound)?;
            if a.kind != FileKind::Regular {
                return Err(if a.kind == FileKind::Directory {
                    NestError::IsDir
                } else {
                    NestError::Invalid("not a regular file".into())
                });
            }
            match (a.gen_state, a.owner) {
                (GenState::Owned, Some(owner)) if owner == self.me() => {
                    let key = ObjectKey::new(file, a.generation);
                    self.d.wait_ready(key).await;
                    let store = self.d.store().clone();
                    let f = tokio::task::spawn_blocking(move || store.open_write(key))
                        .await
                        .expect("blocking task")
                        .map_err(io)?;
                    let o = Arc::new(Owned {
                        key,
                        epoch: a.epoch,
                        file: Arc::new(f),
                        participants: Mutex::new(HashSet::new()),
                        open: tokio::sync::RwLock::new(true),
                        activity: AtomicU64::new(0),
                    });
                    let mut owned = self.owned.lock();
                    // Another task may have raced us; keep the first.
                    let entry = owned.entry(file).or_insert_with(|| o.clone());
                    if entry.key != key {
                        *entry = o.clone();
                    }
                    return Ok(entry.clone());
                }
                (GenState::Owned, Some(_other)) => {
                    return Err(NestError::Unavailable(
                        "file is being written on another node".into(),
                    ));
                }
                _ => {
                    if a.sealed {
                        return Err(NestError::NotPermitted("file is sealed".into()));
                    }
                    let r = self
                        .propose(Command::AcquireOwner {
                            file,
                            node: self.me(),
                            expect_gen: a.generation,
                            truncate: truncate_to_zero,
                            now: Timestamp::now(),
                        })
                        .await;
                    match r {
                        Ok(_) | Err(NestError::Stale) => continue,
                        Err(NestError::Invalid(_)) => {
                            return Err(NestError::Unavailable(
                                "content is held by another node; remote writes arrive in M3"
                                    .into(),
                            ));
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        Err(NestError::Busy("ownership kept changing".into()))
    }

    fn participate(&self, o: &Owned, fh: Option<u64>) {
        if let Some(fh) = fh {
            o.participants.lock().insert(fh);
        }
    }

    /// Finalize after the linger unless someone writes or joins meanwhile.
    fn schedule_finalize(self: &Arc<Self>, file: FileId) {
        let Some(o) = self.owned.lock().get(&file).cloned() else {
            return;
        };
        if !o.participants.lock().is_empty() {
            return;
        }
        let seen = o.activity.load(Ordering::SeqCst);
        let me = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(me.cfg.finalize_linger).await;
            if !o.participants.lock().is_empty() || o.activity.load(Ordering::SeqCst) != seen {
                return;
            }
            let mut open = o.open.write().await;
            if !*open
                || !o.participants.lock().is_empty()
                || o.activity.load(Ordering::SeqCst) != seen
            {
                return;
            }
            *open = false;
            let drop_state = |me: &Vfs| {
                let mut owned = me.owned.lock();
                if owned.get(&file).is_some_and(|x| Arc::ptr_eq(x, &o)) {
                    owned.remove(&file);
                }
            };
            let (size, mtime) = match me.d.store().stat(o.key) {
                Ok(s) => s,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Deleted while lingering (temp files): nothing to settle.
                    drop_state(&me);
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
                && !matches!(e, NestError::NotFound)
            {
                tracing::warn!(%file, error = %e, "finalize failed; startup reconciliation will settle it");
            }
        });
    }

    async fn truncate(
        self: &Arc<Self>,
        file: FileId,
        size: u64,
        fh: Option<u64>,
    ) -> NestResult<()> {
        loop {
            let o = self.ensure_owned(file, size == 0).await?;
            let guard = o.open.read().await;
            if !*guard {
                continue; // finalized under us: start a new epoch
            }
            o.file.set_len(size).map_err(io)?;
            o.activity.fetch_add(1, Ordering::SeqCst);
            drop(guard);
            if fh.is_none_or(|fh| !self.handles.lock().contains_key(&fh)) {
                // truncate(2) without a handle: nothing will close it.
                self.schedule_finalize(file);
            }
            return Ok(());
        }
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

    pub async fn open(self: &Arc<Self>, file: FileId, flags: i32) -> NestResult<(u64, OpenMode)> {
        let a = self.getattr(file)?;
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
            let r = async {
                let o = self.ensure_owned(file, true).await?;
                self.participate(&o, Some(fh));
                let _g = o.open.read().await;
                o.file.set_len(0).map_err(io)?;
                o.activity.fetch_add(1, Ordering::SeqCst);
                Ok::<_, NestError>(())
            }
            .await;
            if let Err(e) = r {
                self.release(fh, None).await;
                return Err(e);
            }
        }
        Ok((fh, mode))
    }

    pub async fn create(
        self: &Arc<Self>,
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
            return Ok((self.getattr(cr.attr.id)?, fh, mode));
        }
        let fh = self.new_handle(cr.attr.id, flags);
        let o = self.ensure_owned(cr.attr.id, false).await?;
        self.participate(&o, Some(fh));
        Ok((cr.attr, fh, OpenMode::Direct))
    }

    /// Close a handle. `lock_owner` is set for flock-style releases.
    pub async fn release(self: &Arc<Self>, fh: u64, lock_owner: Option<u64>) {
        let Some(h) = self.handles.lock().remove(&fh) else {
            return;
        };
        let owners: Vec<u64> = h.lock_owners.lock().drain().chain(lock_owner).collect();
        for owner in owners {
            self.release_locks(h.file, owner).await;
        }
        let participated = self
            .owned
            .lock()
            .get(&h.file)
            .map(|o| o.participants.lock().remove(&fh))
            .unwrap_or(false);
        if participated {
            self.schedule_finalize(h.file);
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
        let Some(o) = self.owned.lock().get(&h.file).cloned() else {
            return Ok(());
        };
        let f = o.file.clone();
        tokio::task::spawn_blocking(move || f.sync_all())
            .await
            .expect("blocking task")
            .map_err(io)?;
        let (size, mtime) = self.d.store().stat(o.key).map_err(io)?;
        match self
            .propose(Command::SyncOwned {
                file: h.file,
                epoch: o.epoch,
                size,
                mtime,
            })
            .await
        {
            Ok(_) | Err(NestError::Stale) => Ok(()),
            Err(e) => Err(e),
        }
    }

    // ------------------------------------------------------------ data

    pub async fn read(&self, fh: u64, offset: u64, size: u32) -> NestResult<Vec<u8>> {
        let h = self.handle(fh)?;
        let a = self
            .q(|c| query::getattr(c, h.file))?
            .ok_or(NestError::NotFound)?;
        let key = ObjectKey::new(h.file, a.generation);
        let local = match (a.gen_state, a.owner) {
            (GenState::Owned, Some(o)) if o == self.me() => true,
            (GenState::Stable, _) => self.d.servable(key),
            _ => false,
        };
        if !local {
            return Err(NestError::Unavailable(
                "no local copy; remote reads arrive in M3".into(),
            ));
        }
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
        tokio::task::spawn_blocking(move || {
            let mut buf = vec![0u8; size as usize];
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
        })
        .await
        .expect("blocking task")
    }

    pub async fn write(&self, fh: u64, offset: u64, data: Vec<u8>) -> NestResult<u32> {
        let h = self.handle(fh)?;
        if !h.writable {
            return Err(NestError::Invalid("handle not open for writing".into()));
        }
        loop {
            let o = self.ensure_owned(h.file, false).await?;
            self.participate(&o, Some(fh));
            let guard = o.open.read().await;
            if !*guard {
                continue; // finalized under us: start a new epoch
            }
            o.activity.fetch_add(1, Ordering::SeqCst);
            let f = o.file.clone();
            let append = h.append;
            let n = tokio::task::spawn_blocking(move || -> std::io::Result<u32> {
                let off = if append { f.metadata()?.len() } else { offset };
                f.write_all_at(&data, off)?;
                Ok(data.len() as u32)
            })
            .await
            .expect("blocking task")
            .map_err(io)?;
            drop(guard);
            return Ok(n);
        }
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
