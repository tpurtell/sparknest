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

mod uring;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};
pub use uring::kernel_enabled as io_uring_kernel_enabled;

#[derive(Clone, Debug)]
pub struct MountConfig {
    pub mountpoint: PathBuf,
    pub allow_other: bool,
    /// Kernel entry/attribute cache TTL.
    pub ttl: Duration,
    /// `/dev/fuse` reader threads.
    pub threads: usize,
    /// Use FUSE over io_uring when the kernel offers it (see `uring`).
    pub io_uring: bool,
}

/// The value handed to fuser. Everything a spawned task needs lives in
/// `Inner`, shared by `Arc`.
const MAX_WRITE: u32 = 1 << 20;
const MAX_READAHEAD: u32 = 4 << 20;

struct Fs {
    inner: Arc<Inner>,
    rt: tokio::runtime::Handle,
}

struct Inner {
    vfs: Arc<Vfs>,
    ttl: Duration,
    uid: u32,
    gid: u32,
    /// Inodes with open handles and how the kernel opened them (`InodeIo`).
    io: Mutex<HashMap<u64, InodeIo>>,
    passthrough: AtomicBool,
    passthrough_warned: AtomicBool,
    uring_wanted: bool,
    /// Negotiated at INIT: the kernel will use rings once they are registered.
    uring: AtomicBool,
    uring_stats: Arc<uring::Stats>,
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

/// An inode with open handles. The kernel keeps one I/O mode per inode
/// across all its open files (fs/fuse/iomode.c): every passthrough open of
/// it must name the same backing file, and while any file is open through
/// passthrough every other open must be too (and the reverse: an inode open
/// through the page cache refuses passthrough). A refused open is EIO for
/// the caller. So the first open decides, and later opens follow it until
/// the last handle is released.
struct InodeIo<B = BackingId> {
    /// The backing file all passthrough opens share; none when the inode is
    /// open through the daemon.
    backing: Option<B>,
    opens: u64,
}

impl<B> Default for InodeIo<B> {
    fn default() -> Self {
        InodeIo {
            backing: None,
            opens: 0,
        }
    }
}

/// The only flags the kernel accepts beside FOPEN_PASSTHROUGH are direct
/// I/O, parallel direct writes and no-flush (FOPEN_PASSTHROUGH_MASK); any
/// other (FOPEN_KEEP_CACHE) fails the open with EIO.
const PASSTHROUGH_OPEN: FopenFlags = FopenFlags::empty();

/// How to answer one open of an inode.
#[derive(Debug, PartialEq, Eq)]
enum Answer {
    /// The inode is open through passthrough: the same backing file.
    Share,
    /// First open, and the VFS offers the local file: register it.
    Register,
    /// Through the daemon, in the mode the VFS chose.
    Daemon,
    /// A writer while the inode is passed through (only sealed files are).
    Busy,
}

impl<B> InodeIo<B> {
    /// `offered`: the VFS handed over a local file for passthrough.
    fn answer(&self, offered: bool, passthrough: bool, writable: bool) -> Answer {
        if self.backing.is_some() {
            if writable {
                Answer::Busy
            } else {
                Answer::Share
            }
        } else if self.opens == 0 && offered && passthrough {
            Answer::Register
        } else {
            Answer::Daemon
        }
    }
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
            // Through the daemon (a sealed file keeps its pages).
            OpenMode::Cached | OpenMode::Passthrough(_) => FopenFlags::FOPEN_KEEP_CACHE,
        }
    }

    /// Answer an open of `ino` (VFS handle `fh`, VFS mode `mode`) in the mode
    /// the inode is already open in, or, for its first open, the best one:
    /// passthrough when the VFS offers the local file. The kernel accepts no
    /// flags beside FOPEN_PASSTHROUGH but direct I/O and no-flush, so a
    /// passthrough answer carries none.
    fn answer_open(&self, ino: u64, fh: u64, mode: OpenMode, writable: bool, reply: ReplyOpen) {
        let mut io = self.io.lock();
        let st = io.entry(ino).or_default();
        let offered = matches!(mode, OpenMode::Passthrough(_));
        match st.answer(offered, self.passthrough.load(Ordering::Relaxed), writable) {
            Answer::Share => {
                st.opens += 1;
                let id = st.backing.as_ref().expect("shared backing");
                return reply.opened_passthrough(FileHandle(fh), PASSTHROUGH_OPEN, id);
            }
            Answer::Busy => {
                if st.opens == 0 {
                    io.remove(&ino);
                }
                drop(io);
                let vfs = self.vfs.clone();
                tokio::spawn(async move { vfs.release(fh, None).await });
                return reply.error(Errno::ETXTBSY);
            }
            Answer::Register => {
                let OpenMode::Passthrough(f) = &mode else {
                    unreachable!("offered")
                };
                match reply.open_backing(f) {
                    Ok(id) => {
                        st.opens = 1;
                        let id = st.backing.insert(id);
                        return reply.opened_passthrough(FileHandle(fh), PASSTHROUGH_OPEN, id);
                    }
                    Err(e) => {
                        if !self.passthrough_warned.swap(true, Ordering::Relaxed) {
                            tracing::warn!(error = %e, "FUSE passthrough unavailable (needs CAP_SYS_ADMIN); sealed files use the page cache");
                        }
                        self.passthrough.store(false, Ordering::Relaxed);
                    }
                }
            }
            Answer::Daemon => {}
        }
        st.opens += 1;
        reply.opened(FileHandle(fh), self.open_flags(&mode));
    }

    /// A handle of `ino` is gone.
    fn closed(&self, ino: u64) {
        let mut io = self.io.lock();
        if let Some(st) = io.get_mut(&ino) {
            st.opens = st.opens.saturating_sub(1);
            if st.opens == 0 {
                // Dropping the backing id closes it in the kernel.
                io.remove(&ino);
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
        let _ = config.set_max_write(MAX_WRITE);
        let _ = config.set_max_readahead(MAX_READAHEAD);
        if this.uring_wanted {
            if have.contains(InitFlags::FUSE_OVER_IO_URING)
                && config
                    .add_capabilities(InitFlags::FUSE_OVER_IO_URING)
                    .is_ok()
            {
                this.uring.store(true, Ordering::Relaxed);
            } else if uring::kernel_enabled() == Some(false) {
                tracing::warn!(
                    "FUSE over io_uring is off in the kernel, so every request takes the /dev/fuse path; \
                     enable it with `echo 1 | sudo tee {}` (and persist it, e.g. with a tmpfiles.d entry), \
                     then restart sparknestd",
                    uring::PARAM
                );
            } else {
                tracing::info!("this kernel does not offer FUSE over io_uring; using /dev/fuse");
            }
        }
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
        let acc = flags.0 & libc::O_ACCMODE;
        let writable = acc == libc::O_WRONLY || acc == libc::O_RDWR;
        self.spawn(async move {
            match fs.vfs.open(fid(i), flags.0).await {
                Ok((fh, mode)) => fs.answer_open(i.0, fh, mode, writable, reply),
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
                    // An existing file (no O_EXCL) may be open through
                    // passthrough, which a create cannot answer with.
                    let passed_through = {
                        let mut io = fs.io.lock();
                        let st = io.entry(a.id.0).or_default();
                        if st.backing.is_none() {
                            st.opens += 1;
                        }
                        st.backing.is_some()
                    };
                    if passed_through {
                        fs.vfs.release(fh, None).await;
                        return reply.error(Errno::ETXTBSY);
                    }
                    reply.created(
                        &fs.ttl,
                        &fs.attr(&a),
                        Generation(0),
                        FileHandle(fh),
                        fs.open_flags(&mode),
                    );
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
        // A scattered file with no copy here (page faults on a lookup
        // table): send the fabric read and return; the fabric's completion
        // thread replies. On an error, retry the ordinary way.
        if let Some(r) = self.vfs.prepare_scattered(fh.0, offset, size) {
            let (vfs, rt) = (self.vfs.clone(), self.rt.clone());
            r.start(Box::new(move |res| match res {
                Ok(b) => reply.data(b),
                Err(_) => {
                    rt.spawn(async move {
                        match vfs.read(fh.0, offset, size).await {
                            Ok(b) => reply.data(&b),
                            Err(e) => reply.error(errno(&e)),
                        }
                    });
                }
            }));
            return;
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
        self.closed(_i.0);
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
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        let vfs = self.vfs.clone();
        self.spawn(async move {
            match vfs.fsync(fh.0, datasync).await {
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
        // Namespace changes commit without an fsync (ADR-026): make them
        // durable on a majority, as a directory fsync promises.
        let vfs = self.vfs.clone();
        self.spawn(async move {
            match vfs.sync_metadata().await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(&e)),
            }
        });
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
    uring: Arc<uring::Stats>,
    mountpoint: std::path::PathBuf,
}

/// How long unmounting waits for the FUSE threads before giving up on them
/// (the process exits anyway, which ends the connection).
const UNMOUNT_WAIT: Duration = Duration::from_secs(10);

/// Whether `mp` is still a sparknest mount, from /proc/self/mountinfo.
fn still_mounted(mp: &std::path::Path) -> bool {
    let Ok(info) = std::fs::read_to_string("/proc/self/mountinfo") else {
        return false;
    };
    let mp = mp.to_string_lossy();
    info.lines().any(|l| {
        let f: Vec<&str> = l.split(' ').collect();
        f.get(4) == Some(&mp.as_ref()) && l.contains(" - fuse.sparknest ")
    })
}

impl Mounted {
    /// FUSE over io_uring: (queues running, requests served through them).
    pub fn io_uring(&self) -> (usize, usize) {
        (
            self.uring.queues.load(Ordering::Relaxed),
            self.uring.requests.load(Ordering::Relaxed),
        )
    }

    pub fn unmount(mut self) {
        self.do_unmount();
    }

    /// Unmount and stop the FUSE threads, within `UNMOUNT_WAIT`. With
    /// auto_unmount, fuser's unmount only closes its socket to the
    /// fusermount3 helper and leaves the unmount to it; when the helper does
    /// not (seen on every trial restart), the threads wait on /dev/fuse
    /// forever and systemd kills the daemon after 90 s. So: lazily unmount
    /// ourselves if the mount is still there, and stop waiting after a while.
    fn do_unmount(&mut self) {
        let _ = self.stop_notify.send(Inval::Stop);
        let Some(s) = self.session.take() else { return };
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(s.umount_and_join());
        });
        let start = std::time::Instant::now();
        let mut detached = false;
        loop {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(Ok(())) => return,
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "unmount failed");
                    return;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            }
            if !detached && start.elapsed() > Duration::from_secs(1) {
                detached = true;
                if still_mounted(&self.mountpoint) {
                    tracing::info!(mountpoint = %self.mountpoint.display(), "still mounted: detaching it");
                    let _ = std::process::Command::new("fusermount3")
                        .arg("-uz")
                        .arg(&self.mountpoint)
                        .status();
                }
            }
            if start.elapsed() > UNMOUNT_WAIT {
                tracing::warn!("FUSE threads did not stop; exiting without them");
                return;
            }
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
    // A daemon that died without unmounting leaves a dead mount behind
    // ("Transport endpoint is not connected"); clear it so we can mount.
    if let Err(e) = std::fs::metadata(&cfg.mountpoint)
        && e.raw_os_error() == Some(libc::ENOTCONN)
    {
        tracing::warn!(mountpoint = %cfg.mountpoint.display(), "clearing a dead FUSE mount left by a previous daemon");
        let _ = std::process::Command::new("fusermount3")
            .arg("-uz")
            .arg(&cfg.mountpoint)
            .status();
    }
    let inner = Arc::new(Inner {
        vfs: vfs.clone(),
        ttl: cfg.ttl,
        uid: nix::unistd::getuid().as_raw(),
        gid: nix::unistd::getgid().as_raw(),
        io: Mutex::new(HashMap::new()),
        passthrough: AtomicBool::new(false),
        passthrough_warned: AtomicBool::new(false),
        uring_wanted: cfg.io_uring,
        uring: AtomicBool::new(false),
        uring_stats: Arc::new(uring::Stats::default()),
    });
    let fs = Fs {
        inner: inner.clone(),
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
    if inner.uring.load(Ordering::Relaxed) {
        let ring_fs = Fs {
            inner: inner.clone(),
            rt: tokio::runtime::Handle::current(),
        };
        let max_pages = (MAX_READAHEAD.max(MAX_WRITE) as usize).div_ceil(4096);
        match uring::start(
            session.ring_dispatcher(ring_fs),
            MAX_WRITE as usize,
            max_pages,
            inner.uring_stats.clone(),
            {
                let vfs = vfs.clone();
                Arc::new(move |fh, offset, buf: &mut [u8]| {
                    vfs.try_read_into(fh, offset, buf)
                        .map(|r| r.map_err(|e| e.errno()))
                })
            },
        ) {
            Ok(n) => tracing::info!(
                queues = n,
                "FUSE over io_uring: requests are served on the issuing CPU"
            ),
            Err(e) => tracing::warn!(error = %e, "FUSE over io_uring unavailable; using /dev/fuse"),
        }
    }
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
        uring: inner.uring_stats.clone(),
        mountpoint: cfg.mountpoint.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;

    /// fs/fuse/iomode.c: FOPEN_PASSTHROUGH_MASK.
    const KERNEL_PASSTHROUGH_MASK: FopenFlags = FopenFlags::FOPEN_PASSTHROUGH
        .union(FopenFlags::FOPEN_DIRECT_IO)
        .union(FopenFlags::FOPEN_PARALLEL_DIRECT_WRITES)
        .union(FopenFlags::FOPEN_NOFLUSH);

    #[test]
    fn passthrough_opens_carry_only_flags_the_kernel_accepts() {
        let sent = PASSTHROUGH_OPEN | FopenFlags::FOPEN_PASSTHROUGH;
        assert!(KERNEL_PASSTHROUGH_MASK.contains(sent), "{sent:?}");
    }

    /// Every open of an inode follows the first until the last closes: one
    /// backing file shared, never a second one, never a page-cache open
    /// beside a passthrough one (or the reverse).
    #[test]
    fn opens_of_one_inode_share_its_mode() {
        let mut st: InodeIo<u32> = InodeIo::default();
        // First reader: passthrough offered, registered.
        assert_eq!(st.answer(true, true, false), Answer::Register);
        st.backing = Some(7);
        st.opens = 1;
        // Three more ranks: the same backing, whatever the VFS offers now
        // (a second copy elsewhere makes it offer the daemon path).
        assert_eq!(st.answer(true, true, false), Answer::Share);
        assert_eq!(st.answer(false, true, false), Answer::Share);
        assert_eq!(st.answer(false, false, false), Answer::Share);
        // A writer is refused while it is passed through.
        assert_eq!(st.answer(false, true, true), Answer::Busy);

        // Opened through the daemon first: later opens never pass through.
        let mut st: InodeIo<u32> = InodeIo::default();
        assert_eq!(st.answer(false, true, false), Answer::Daemon);
        st.opens = 1;
        assert_eq!(st.answer(true, true, false), Answer::Daemon);
        // No passthrough available: the daemon path.
        let st: InodeIo<u32> = InodeIo::default();
        assert_eq!(st.answer(true, false, false), Answer::Daemon);
    }
}
