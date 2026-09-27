//! Transfer writes: direct I/O through io_uring (ADR-042).
//!
//! A transfer writes 4 KiB-aligned buffers to the object's file opened with
//! `O_DIRECT`, through one io_uring owned by a thread of this process. A
//! direct write bypasses the page cache, so its completion means the device
//! (or, on CIFS, the server) has the bytes: no dirty pages pile up to be
//! flushed later, no per-window fsync, and a transfer's pacing is simply how
//! many writes it keeps in flight (`Staging`'s window). The one sync left is
//! at commit, which makes the object durable before it is published.
//!
//! A short write is continued for the rest, never taken for complete. The
//! last write of a file is padded to the alignment and the file trimmed to
//! its size at commit. Without io_uring, writes run on the caller's thread.

use io_uring::{IoUring, opcode, types};
use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::{Arc, OnceLock};

const DEPTH: usize = 256;
const WAKE: u64 = u64::MAX;
/// Direct I/O alignment of buffers, offsets and lengths.
pub const ALIGN: usize = 4096;

/// A heap buffer aligned for direct I/O; `len` bytes of it are meant, its
/// capacity is `len` rounded up to `ALIGN` (the rest zero).
pub struct AlignedBuf {
    ptr: std::ptr::NonNull<u8>,
    cap: usize,
    len: usize,
}

// SAFETY: an owned heap allocation, like Vec<u8>.
unsafe impl Send for AlignedBuf {}

impl AlignedBuf {
    /// A zeroed buffer for `len` bytes.
    pub fn new(len: usize) -> AlignedBuf {
        let cap = len.max(1).div_ceil(ALIGN) * ALIGN;
        let layout = std::alloc::Layout::from_size_align(cap, ALIGN).expect("valid layout");
        // SAFETY: a non-zero size, a power-of-two alignment.
        let p = unsafe { std::alloc::alloc_zeroed(layout) };
        let ptr =
            std::ptr::NonNull::new(p).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        AlignedBuf { ptr, cap, len }
    }

    /// A buffer holding a copy of `data`.
    pub fn copy_of(data: &[u8]) -> AlignedBuf {
        let mut b = AlignedBuf::new(data.len());
        b.as_mut_slice().copy_from_slice(data);
        b
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Keep only the first `len` bytes meant (after a short read).
    pub fn truncate(&mut self, len: usize) {
        self.len = self.len.min(len);
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: `cap` initialized bytes (zeroed at allocation), len <= cap.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: as above, and `&mut self` is exclusive.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    /// The whole aligned extent a direct write covers (len rounded up).
    fn padded(&self) -> &[u8] {
        let n = self.len.div_ceil(ALIGN) * ALIGN;
        // SAFETY: n <= cap, all initialized.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), n) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        let layout = std::alloc::Layout::from_size_align(self.cap, ALIGN).expect("valid layout");
        // SAFETY: allocated with this layout in `new`.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), layout) }
    }
}

/// A finished write.
pub type Done = tokio::sync::oneshot::Receiver<io::Result<()>>;

struct Req {
    file: Arc<File>,
    off: u64,
    buf: AlignedBuf,
    done: tokio::sync::oneshot::Sender<io::Result<()>>,
}

struct Op {
    req: Req,
    written: usize,
}

pub struct WriteRing {
    tx: std::sync::mpsc::Sender<Req>,
    wake: File,
}

/// The process's ring, if io_uring is available.
pub fn ring() -> Option<&'static WriteRing> {
    static RING: OnceLock<Option<WriteRing>> = OnceLock::new();
    RING.get_or_init(|| match WriteRing::start() {
        Ok(r) => Some(r),
        Err(e) => {
            tracing::warn!(error = %e, "io_uring unavailable: transfer writes run on the caller's thread");
            None
        }
    })
    .as_ref()
}

impl WriteRing {
    fn start() -> io::Result<WriteRing> {
        let ring = IoUring::new(DEPTH as u32 + 1)?;
        // SAFETY: eventfd returns a new descriptor or -1.
        let efd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if efd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a fresh descriptor, owned from here on.
        let wake = File::from(unsafe { OwnedFd::from_raw_fd(efd) });
        let theirs = wake.try_clone()?;
        let (tx, rx) = std::sync::mpsc::channel::<Req>();
        std::thread::Builder::new()
            .name("nest-write-ring".into())
            .spawn(move || run(ring, rx, theirs))?;
        Ok(WriteRing { tx, wake })
    }

    /// Queue a direct write of `buf` (padded to the alignment) at `off`.
    pub fn submit(&self, file: Arc<File>, off: u64, buf: AlignedBuf) -> Done {
        let (done, rx) = tokio::sync::oneshot::channel();
        match self.tx.send(Req {
            file,
            off,
            buf,
            done,
        }) {
            Ok(()) => {
                use std::io::Write;
                let _ = (&self.wake).write(&1u64.to_ne_bytes());
            }
            Err(std::sync::mpsc::SendError(r)) => {
                let _ = r.done.send(Err(io::Error::other("write ring stopped")));
            }
        }
        rx
    }
}

/// Write on this thread (no io_uring): the whole padded extent.
pub fn write_now(file: &File, off: u64, buf: &AlignedBuf) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf.padded(), off)
}

fn run(mut ring: IoUring, rx: std::sync::mpsc::Receiver<Req>, wake: File) {
    let mut ops: Vec<Option<Op>> = (0..DEPTH).map(|_| None).collect();
    let mut free: Vec<usize> = (0..DEPTH).rev().collect();
    let mut queue: VecDeque<Req> = VecDeque::new();
    let mut armed = false;
    loop {
        while let Ok(r) = rx.try_recv() {
            queue.push_back(r);
        }
        while let Some(idx) = free.last().copied() {
            let Some(req) = queue.pop_front() else { break };
            free.pop();
            ops[idx] = Some(Op { req, written: 0 });
            if !push(&mut ring, &ops, idx) {
                // Cannot happen: the ring has room for every op.
                if let Some(op) = ops[idx].take() {
                    let _ = op.req.done.send(Err(io::Error::other("write ring full")));
                }
                free.push(idx);
            }
        }
        if !armed {
            let e = opcode::PollAdd::new(types::Fd(wake.as_raw_fd()), libc::POLLIN as u32)
                .build()
                .user_data(WAKE);
            // SAFETY: references only the eventfd, owned by this thread for
            // its whole life.
            if unsafe { ring.submission().push(&e) }.is_ok() {
                armed = true;
            }
        }
        if let Err(e) = ring.submit_and_wait(1)
            && e.raw_os_error() != Some(libc::EINTR)
        {
            tracing::error!(error = %e, "write ring failed");
            fail_all(&mut ops, &mut queue, &e);
            return;
        }
        let cqes: Vec<(u64, i32)> = ring
            .completion()
            .map(|c| (c.user_data(), c.result()))
            .collect();
        for (ud, res) in cqes {
            if ud == WAKE {
                armed = false;
                use std::io::Read;
                let mut b = [0u8; 8];
                let _ = (&wake).read(&mut b);
                continue;
            }
            let idx = ud as usize;
            let Some(op) = ops.get_mut(idx).and_then(|o| o.as_mut()) else {
                continue;
            };
            let want = op.req.buf.padded().len();
            if res == -libc::EINTR || res == -libc::EAGAIN {
                push(&mut ring, &ops, idx);
                continue;
            }
            if res > 0 {
                op.written += res as usize;
                if op.written < want {
                    // Short write: the rest, never taken for complete.
                    push(&mut ring, &ops, idx);
                    continue;
                }
            }
            let op = ops[idx].take().expect("present");
            free.push(idx);
            let r = if res < 0 {
                Err(io::Error::from_raw_os_error(-res))
            } else if op.written < want {
                Err(io::Error::new(io::ErrorKind::WriteZero, "wrote nothing"))
            } else {
                Ok(())
            };
            let _ = op.req.done.send(r);
        }
    }
}

/// Queue (the rest of) op `idx`'s write; false if the queue is full.
fn push(ring: &mut IoUring, ops: &[Option<Op>], idx: usize) -> bool {
    let Some(op) = ops[idx].as_ref() else {
        return false;
    };
    let rest = &op.req.buf.padded()[op.written..];
    let e = opcode::Write::new(
        types::Fd(op.req.file.as_raw_fd()),
        rest.as_ptr(),
        rest.len() as u32,
    )
    .offset(op.req.off + op.written as u64)
    .build()
    .user_data(idx as u64);
    // SAFETY: the buffer and file are owned by `ops[idx]`, cleared only once
    // this write's completion is reaped; the thread never exits with writes
    // in flight except through `fail_all`, which leaks their buffers.
    unsafe { ring.submission().push(&e) }.is_ok()
}

fn fail_all(ops: &mut [Option<Op>], queue: &mut VecDeque<Req>, e: &io::Error) {
    for op in ops.iter_mut().filter_map(|o| o.take()) {
        let Req { buf, done, .. } = op.req;
        // The kernel may still write from it: never free it.
        std::mem::forget(buf);
        let _ = done.send(Err(io::Error::other(e.to_string())));
    }
    for r in queue.drain(..) {
        let _ = r.done.send(Err(io::Error::other(e.to_string())));
    }
}
