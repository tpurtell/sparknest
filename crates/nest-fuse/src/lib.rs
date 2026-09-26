//! FUSE frontend: a thin adapter from the kernel protocol (via `fuser`) to
//! [`nest_data::Vfs`].
//!
//! - Requests are dispatched onto the tokio runtime and answered from
//!   there, so slow operations (consensus, remote I/O) never block the
//!   `/dev/fuse` reader threads.
//! - Unsealed files are opened with `FOPEN_DIRECT_IO` (plus parallel direct
//!   writes); sealed files use kernel passthrough to the local object when
//!   possible, else the page cache with `FOPEN_KEEP_CACHE`.
//! - Kernel dentry/attribute/page caches are invalidated from applied
//!   metadata effects on a dedicated thread, never on the apply path or a
//!   request path (inval_entry can wait on locks an in-flight request holds).

use fuser::{
    BackingId, Config, Errno, FileAttr as FAttr, FileHandle, FileType, Filesystem, FopenFlags,
    Generation, INodeNo, InitFlags, KernelConfig, LockOwner, MountOption, Notifier, OpenFlags,
    RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyLock, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Request, SessionACL, TimeOrNow,
    WriteFlags,
};
use nest_data::{OpenMode, Vfs};
use nest_meta::{Effect, LockKind};
use nest_types::{FileAttr, FileId, FileKind, NestError, Timestamp};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

#[derive(Clone, Debug)]
pub struct MountConfig {
    pub mountpoint: PathBuf,
    pub allow_other: bool,
    /// Kernel entry/attribute cache TTL.
    pub ttl: Duration,
    /// `/dev/fuse` reader threads.
    pub threads: usize,
}

/// The value handed to fuser. Everything a spawned task needs lives in
/// `Inner`, shared by `Arc`.
struct Fs {
    inner: Arc<Inner>,
    rt: tokio::runtime::Handle,
}

struct Inner {
    vfs: Arc<Vfs>,
    ttl: Duration,
    uid: u32,
    gid: u32,
    /// Passthrough registrations, kept until release.
    backing: Mutex<HashMap<u64, BackingId>>,
    passthrough: AtomicBool,
    passthrough_warned: AtomicBool,
}

impl std::ops::Deref for Fs {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        &self.inner
    }
}

/// Extended attribute that exposes (and sets) the seal bit.
const SEAL_XATTR: &[u8] = b"user.sparknest.sealed";

fn errno(e: &NestError) -> Errno {
    Errno::from_i32(e.errno())
}

fn ino(f: FileId) -> INodeNo {
    INodeNo(f.0)
}

fn fid(i: INodeNo) -> FileId {
    FileId(i.0)
}

fn kind(k: FileKind) -> FileType {
    match k {
        FileKind::Regular => FileType::RegularFile,
        FileKind::Directory => FileType::Directory,
        FileKind::Symlink => FileType::Symlink,
    }
}

fn ts(t: TimeOrNow) -> Timestamp {
    match t {
        TimeOrNow::SpecificTime(t) => Timestamp::from_system_time(t),
        TimeOrNow::Now => Timestamp::now(),
    }
}

impl Inner {
    fn attr(&self, a: &FileAttr) -> FAttr {
        FAttr {
            ino: ino(a.id),
            size: a.size,
            blocks: a.size.div_ceil(512),
            atime: a.atime.as_system_time(),
            mtime: a.mtime.as_system_time(),
            ctime: a.ctime.as_system_time(),
            crtime: a.crtime.as_system_time(),
            kind: kind(a.kind),
            perm: (a.perm & 0o7777) as u16,
            nlink: a.nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 1 << 20,
            flags: 0,
        }
    }

    fn open_flags(&self, mode: &OpenMode) -> FopenFlags {
        match mode {
            OpenMode::Direct => {
                FopenFlags::FOPEN_DIRECT_IO | FopenFlags::FOPEN_PARALLEL_DIRECT_WRITES
            }
            OpenMode::Cached | OpenMode::Passthrough(_) => FopenFlags::FOPEN_KEEP_CACHE,
        }
    }

    /// Reply to open/create in the best mode available; falls back from
    /// passthrough to the page cache if the kernel refuses the backing file.
    fn register_backing(
        &self,
        fh: u64,
        file: &std::fs::File,
        open_backing: impl FnOnce(&std::fs::File) -> std::io::Result<BackingId>,
    ) -> Option<()> {
        if !self.passthrough.load(Ordering::Relaxed) {
            return None;
        }
        match open_backing(file) {
            Ok(id) => {
                self.backing.lock().insert(fh, id);
                Some(())
            }
            Err(e) => {
                if !self.passthrough_warned.swap(true, Ordering::Relaxed) {
                    tracing::warn!(error = %e, "FUSE passthrough unavailable (needs CAP_SYS_ADMIN); sealed files use the page cache");
                }
                self.passthrough.store(false, Ordering::Relaxed);
                None
            }
        }
    }
}

impl Filesystem for Fs {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        let this = &self.inner;
        let want = InitFlags::FUSE_POSIX_LOCKS
            | InitFlags::FUSE_FLOCK_LOCKS
            | InitFlags::FUSE_ATOMIC_O_TRUNC
            | InitFlags::FUSE_PARALLEL_DIROPS
            | InitFlags::FUSE_MAX_PAGES
            | InitFlags::FUSE_CACHE_SYMLINKS
            | InitFlags::FUSE_DIRECT_IO_ALLOW_MMAP
            | InitFlags::FUSE_BIG_WRITES
            | InitFlags::FUSE_ASYNC_READ;
        let have = config.capabilities();
        if let Err(missing) = config.add_capabilities(want & have) {
            tracing::warn!(?missing, "kernel lacks some FUSE capabilities");
        }
        let _ = config.set_max_write(1 << 20);
        let _ = config.set_max_readahead(4 << 20);
        let _ = config.set_max_background(128);
        if have.contains(InitFlags::FUSE_PASSTHROUGH)
            && config.add_capabilities(InitFlags::FUSE_PASSTHROUGH).is_ok()
            && config.set_max_stack_depth(1).is_ok()
        {
            this.passthrough.store(true, Ordering::Relaxed);
        }
        tracing::info!(
            abi = ?config.kernel_abi(),
            passthrough = this.passthrough.load(Ordering::Relaxed),
            "FUSE session initialized"
        );
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let inner = self.inner.clone();
        let name = name.as_bytes().to_vec();
        self.spawn(async move {
            match inner.vfs.lookup(fid(parent), &name).await {
                Ok(a) => reply.entry(&inner.ttl, &inner.attr(&a), Generation(0)),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn getattr(&self, _req: &Request, i: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let inner = self.inner.clone();
        self.spawn(async move {
            match inner.vfs.getattr(fid(i)).await {
                Ok(a) => reply.attr(&inner.ttl, &inner.attr(&a)),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn setattr(
        &self,
        _req: &Request,
        i: INodeNo,
        mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        // Ownership changes are accepted and ignored: every file is presented
        // as the mounting user (ADR-007).
        let inner = self.inner.clone();
        self.spawn(async move {
            match inner
                .vfs
                .setattr(
                    fid(i),
                    mode.map(|m| m & 0o7777),
                    size,
                    atime.map(ts),
                    mtime.map(ts),
                    fh.map(|f| f.0),
                )
                .await
            {
                Ok(a) => reply.attr(&inner.ttl, &inner.attr(&a)),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn readlink(&self, _req: &Request, i: INodeNo, reply: ReplyData) {
        match self.vfs.readlink(fid(i)) {
            Ok(t) => reply.data(&t),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn mknod(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        if mode & libc::S_IFMT != libc::S_IFREG {
            reply.error(Errno::EPERM);
            return;
        }
        let inner = self.inner.clone();
        let vfs = inner.vfs.clone();
        let name = name.as_bytes().to_vec();
        self.spawn(async move {
            match vfs
                .create(
                    fid(parent),
                    &name,
                    mode & !umask & 0o7777,
                    libc::O_WRONLY | libc::O_EXCL,
                )
                .await
            {
                Ok((a, fh, _)) => {
                    vfs.release(fh, None).await;
                    reply.entry(&inner.ttl, &inner.attr(&a), Generation(0));
                }
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let inner = self.inner.clone();
        let vfs = inner.vfs.clone();
        let name = name.as_bytes().to_vec();
        self.spawn(async move {
            match vfs.mkdir(fid(parent), &name, mode & !umask & 0o7777).await {
                Ok(a) => reply.entry(&inner.ttl, &inner.attr(&a), Generation(0)),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let vfs = self.vfs.clone();
        let name = name.as_bytes().to_vec();
        self.spawn(async move {
            match vfs.unlink(fid(parent), &name).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let vfs = self.vfs.clone();
        let name = name.as_bytes().to_vec();
        self.spawn(async move {
            match vfs.rmdir(fid(parent), &name).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn symlink(
        &self,
        _req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let inner = self.inner.clone();
        let vfs = inner.vfs.clone();
        let name = link_name.as_bytes().to_vec();
        let target = target.as_os_str().as_bytes().to_vec();
        self.spawn(async move {
            match vfs.symlink(fid(parent), &name, &target).await {
                Ok(a) => reply.entry(&inner.ttl, &inner.attr(&a), Generation(0)),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        if flags.contains(RenameFlags::RENAME_WHITEOUT) {
            reply.error(Errno::EINVAL);
            return;
        }
        let vfs = self.vfs.clone();
        let (name, newname) = (name.as_bytes().to_vec(), newname.as_bytes().to_vec());
        let f = nest_meta::RenameFlags {
            noreplace: flags.contains(RenameFlags::RENAME_NOREPLACE),
            exchange: flags.contains(RenameFlags::RENAME_EXCHANGE),
        };
        self.spawn(async move {
            match vfs
                .rename(fid(parent), &name, fid(newparent), &newname, f)
                .await
            {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn link(
        &self,
        _req: &Request,
        i: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let inner = self.inner.clone();
        let vfs = inner.vfs.clone();
        let name = newname.as_bytes().to_vec();
        self.spawn(async move {
            match vfs.link(fid(i), fid(newparent), &name).await {
                Ok(a) => reply.entry(&inner.ttl, &inner.attr(&a), Generation(0)),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn open(&self, _req: &Request, i: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let fs = self.inner.clone();
        self.spawn(async move {
            match fs.vfs.open(fid(i), flags.0).await {
                Ok((fh, mode)) => {
                    if let OpenMode::Passthrough(f) = &mode
                        && fs
                            .register_backing(fh, f, |f| reply.open_backing(f))
                            .is_some()
                    {
                        let id = fs.backing.lock();
                        let id = id.get(&fh).expect("just registered");
                        reply.opened_passthrough(FileHandle(fh), FopenFlags::FOPEN_KEEP_CACHE, id);
                        return;
                    }
                    let flags = match mode {
                        OpenMode::Passthrough(_) => FopenFlags::FOPEN_KEEP_CACHE,
                        ref m => fs.open_flags(m),
                    };
                    reply.opened(FileHandle(fh), flags);
                }
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let fs = self.inner.clone();
        let name = name.as_bytes().to_vec();
        self.spawn(async move {
            match fs
                .vfs
                .create(fid(parent), &name, mode & !umask & 0o7777, flags)
                .await
            {
                Ok((a, fh, mode)) => {
                    let fl = match mode {
                        OpenMode::Passthrough(_) => FopenFlags::FOPEN_KEEP_CACHE,
                        ref m => fs.open_flags(m),
                    };
                    reply.created(&fs.ttl, &fs.attr(&a), Generation(0), FileHandle(fh), fl);
                }
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn read(
        &self,
        _req: &Request,
        _i: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        // Local copies and readahead hits complete right here: every hand-off
        // to another thread can cost a deep-idle wake on the Sparks.
        let fast = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.vfs.try_read_now(fh.0, offset, size)
        }))
        .unwrap_or_else(|_| {
            tracing::error!("fast read path panicked; using the async path");
            None
        });
        match fast {
            Some(Ok(b)) => return reply.data(&b),
            Some(Err(e)) => return reply.error(errno(&e)),
            None => {}
        }
        let vfs = self.vfs.clone();
        self.spawn(async move {
            match vfs.read(fh.0, offset, size).await {
                Ok(b) => reply.data(&b),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn write(
        &self,
        _req: &Request,
        _i: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        // The owner writing its own open epoch completes right here.
        let fast = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.vfs.try_write_now(fh.0, offset, data)
        }))
        .unwrap_or_else(|_| {
            tracing::error!("fast write path panicked; using the async path");
            None
        });
        match fast {
            Some(Ok(n)) => return reply.written(n),
            Some(Err(e)) => return reply.error(errno(&e)),
            None => {}
        }
        let vfs = self.vfs.clone();
        let data = data.to_vec();
        self.spawn(async move {
            match vfs.write(fh.0, offset, data).await {
                Ok(n) => reply.written(n),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn flush(
        &self,
        _req: &Request,
        _i: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        tracing::debug!(fh = fh.0, owner = lock_owner.0, "flush");
        let vfs = self.vfs.clone();
        self.spawn(async move {
            match vfs.flush(fh.0, lock_owner.0).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn release(
        &self,
        _req: &Request,
        _i: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        tracing::debug!(
            fh = fh.0,
            ?lock_owner,
            flush = _flush,
            flags = _flags.0,
            "release"
        );
        self.backing.lock().remove(&fh.0);
        let vfs = self.vfs.clone();
        self.spawn(async move {
            vfs.release(fh.0, lock_owner.map(|o| o.0)).await;
            reply.ok();
        });
    }

    fn fsync(
        &self,
        _req: &Request,
        _i: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let vfs = self.vfs.clone();
        self.spawn(async move {
            match vfs.fsync(fh.0).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn opendir(&self, _req: &Request, i: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        // readdir past the end: validates that `i` is a directory, cheaply.
        match self.vfs.readdir(fid(i), u64::MAX, 0) {
            Ok(_) => reply.opened(FileHandle(0), FopenFlags::empty()),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        i: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        match self.vfs.readdir(fid(i), offset, 512) {
            Ok(entries) => {
                for (off, e) in entries {
                    if reply.add(ino(e.id), off, kind(e.kind), OsStr::from_bytes(&e.name)) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn fsyncdir(
        &self,
        _req: &Request,
        _i: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        // Namespace changes are durable once committed.
        reply.ok();
    }

    fn statfs(&self, _req: &Request, _i: INodeNo, reply: ReplyStatfs) {
        match self.vfs.statfs() {
            Ok((cap, files)) => {
                let bs = 4096u64;
                reply.statfs(
                    cap.total / bs,
                    cap.free / bs,
                    cap.free / bs,
                    files,
                    u32::MAX as u64,
                    bs as u32,
                    255,
                    bs as u32,
                )
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn getxattr(&self, _req: &Request, i: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        if name.as_bytes() != SEAL_XATTR {
            return reply.error(Errno::from_i32(libc::ENODATA));
        }
        let inner = self.inner.clone();
        self.spawn(async move {
            match inner.vfs.getattr(fid(i)).await {
                Ok(a) if a.kind == FileKind::Regular => {
                    let v: &[u8] = if a.sealed { b"1" } else { b"0" };
                    if size == 0 {
                        reply.size(v.len() as u32)
                    } else if (size as usize) < v.len() {
                        reply.error(Errno::from_i32(libc::ERANGE))
                    } else {
                        reply.data(v)
                    }
                }
                Ok(_) => reply.error(Errno::from_i32(libc::ENODATA)),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn listxattr(&self, _req: &Request, _i: INodeNo, size: u32, reply: ReplyXattr) {
        // Advertise the seal attribute; listing values is by getxattr.
        let list = [SEAL_XATTR, b"\0"].concat();
        if size == 0 {
            reply.size(list.len() as u32);
        } else if (size as usize) < list.len() {
            reply.error(Errno::from_i32(libc::ERANGE));
        } else {
            reply.data(&list);
        }
    }

    /// `setfattr -n user.sparknest.sealed -v 1 FILE` seals a file (enforced
    /// immutability; unlocks passthrough and page caching), `-v 0` unseals.
    fn setxattr(
        &self,
        _req: &Request,
        i: INodeNo,
        name: &OsStr,
        value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        if name.as_bytes() != SEAL_XATTR {
            return reply.error(Errno::from_i32(libc::ENOTSUP));
        }
        let sealed = match value {
            b"1" | b"true" | b"yes" => true,
            b"0" | b"false" | b"no" => false,
            _ => return reply.error(Errno::EINVAL),
        };
        let vfs = self.vfs.clone();
        self.spawn(async move {
            match vfs.seal(fid(i), sealed).await {
                Ok(_) => reply.ok(),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn removexattr(&self, _req: &Request, _i: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::from_i32(libc::ENODATA));
    }

    fn access(&self, _req: &Request, _i: INodeNo, _mask: fuser::AccessFlags, reply: ReplyEmpty) {
        // default_permissions: the kernel checks mode bits itself.
        reply.ok();
    }

    fn ioctl(
        &self,
        _req: &Request,
        _i: INodeNo,
        _fh: FileHandle,
        _flags: fuser::IoctlFlags,
        _cmd: u32,
        _in_data: &[u8],
        _out_size: u32,
        reply: fuser::ReplyIoctl,
    ) {
        reply.error(Errno::from_i32(libc::ENOTTY));
    }

    fn getlk(
        &self,
        _req: &Request,
        _i: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        _pid: u32,
        reply: ReplyLock,
    ) {
        match self
            .vfs
            .getlk(fh.0, lock_owner.0, start, end, typ == libc::F_WRLCK)
        {
            Ok(Some((s, e, w, pid))) => {
                reply.locked(s, e, if w { libc::F_WRLCK } else { libc::F_RDLCK }, pid)
            }
            Ok(None) => reply.locked(start, end, libc::F_UNLCK, 0),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn setlk(
        &self,
        _req: &Request,
        _i: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        tracing::debug!(
            fh = fh.0,
            owner = lock_owner.0,
            start,
            end,
            typ,
            sleep,
            "setlk"
        );
        let kind = match typ {
            libc::F_RDLCK => LockKind::Read,
            libc::F_WRLCK => LockKind::Write,
            _ => LockKind::Unlock,
        };
        let vfs = self.vfs.clone();
        self.spawn(async move {
            match vfs
                .setlk(fh.0, lock_owner.0, start, end, kind, pid, sleep)
                .await
            {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }
}

impl Fs {
    fn spawn<F>(&self, f: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.rt.spawn(f);
    }
}

/// A mounted filesystem. Dropping it unmounts.
pub struct Mounted {
    session: Option<fuser::BackgroundSession>,
    stop_notify: std::sync::mpsc::Sender<Inval>,
}

impl Mounted {
    pub fn unmount(mut self) {
        self.do_unmount();
    }

    fn do_unmount(&mut self) {
        let _ = self.stop_notify.send(Inval::Stop);
        if let Some(s) = self.session.take()
            && let Err(e) = s.umount_and_join()
        {
            tracing::warn!(error = %e, "unmount failed");
        }
    }
}

impl Drop for Mounted {
    fn drop(&mut self) {
        self.do_unmount();
    }
}

enum Inval {
    Entry(FileId, Vec<u8>),
    Attr(FileId),
    Data(FileId),
    Stop,
}

/// Mount `vfs` and start serving. Must be called from within a tokio
/// runtime (requests are dispatched onto it).
pub fn mount(vfs: Arc<Vfs>, cfg: &MountConfig) -> std::io::Result<Mounted> {
    let inner = Arc::new(Inner {
        vfs: vfs.clone(),
        ttl: cfg.ttl,
        uid: nix::unistd::getuid().as_raw(),
        gid: nix::unistd::getgid().as_raw(),
        backing: Mutex::new(HashMap::new()),
        passthrough: AtomicBool::new(false),
        passthrough_warned: AtomicBool::new(false),
    });
    let fs = Fs {
        inner,
        rt: tokio::runtime::Handle::current(),
    };
    let mut options = vec![
        MountOption::FSName("sparknest".into()),
        MountOption::Subtype("sparknest".into()),
        MountOption::DefaultPermissions,
        MountOption::NoAtime,
    ];
    if cfg.allow_other {
        options.push(MountOption::AutoUnmount);
    }
    let mut config = Config::default();
    config.mount_options = options;
    config.acl = if cfg.allow_other {
        SessionACL::All
    } else {
        SessionACL::Owner
    };
    config.n_threads = Some(cfg.threads.max(1));
    config.clone_fd = cfg.threads > 1;
    let session = fuser::Session::new(fs, &cfg.mountpoint, &config)?;
    let notifier = session.notifier();
    let session = session.spawn()?;

    // Fencing drops this node's page cache for a file synchronously before
    // acknowledging revocation (mmap of unsealed files uses the page cache).
    let fence_notifier = notifier.clone();
    vfs.add_fence_hook(Arc::new(move |file: FileId| {
        let _ = fence_notifier.inval_inode(ino(file), 0, 0);
    }));

    // Kernel cache invalidation runs on its own thread: inval_entry may
    // wait on a directory lock held by a request we are still answering.
    let (tx, rx) = std::sync::mpsc::channel::<Inval>();
    let tap_tx = std::sync::Mutex::new(tx.clone());
    vfs.data().add_tap(Arc::new(move |_index: u64, e: &Effect| {
        let msg = match e {
            Effect::EntryChanged { parent, name } => Inval::Entry(*parent, name.clone()),
            Effect::AttrChanged { file } => Inval::Attr(*file),
            Effect::OwnershipGranted { file, .. } | Effect::FileDeleted { file } => {
                Inval::Data(*file)
            }
            _ => return,
        };
        let _ = tap_tx.lock().map(|t| t.send(msg));
    }));
    std::thread::Builder::new()
        .name("sparknest-inval".into())
        .spawn(move || invalidator(notifier, rx))?;
    Ok(Mounted {
        session: Some(session),
        stop_notify: tx,
    })
}

fn invalidator(n: Notifier, rx: std::sync::mpsc::Receiver<Inval>) {
    while let Ok(m) = rx.recv() {
        // ENOENT means the kernel has nothing cached for it: fine.
        let _ = match m {
            Inval::Entry(p, name) => n.inval_entry(ino(p), OsStr::from_bytes(&name)),
            Inval::Attr(f) => n.inval_inode(ino(f), -1, 0),
            Inval::Data(f) => n.inval_inode(ino(f), 0, 0),
            Inval::Stop => return,
        };
    }
}
