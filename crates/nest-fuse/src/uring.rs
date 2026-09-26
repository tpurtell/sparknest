//! FUSE over io_uring (Linux 6.14+, `fuse.enable_uring=1`).
//!
//! Once INIT negotiates `FUSE_OVER_IO_URING` and every CPU's queue has a
//! registered entry, the kernel stops queueing ordinary requests on
//! /dev/fuse and completes a ring command on the issuing CPU's queue
//! instead. Each queue here is one thread pinned to its CPU with its own
//! ring: a request is answered on the core that asked, which is awake, and
//! a round trip costs no read/write syscall pair. FORGET, INTERRUPT and
//! notifications keep using /dev/fuse (fuser's session thread).
//!
//! Per entry the kernel writes `fuse_in_header` into `in_out`, the
//! operation's fixed header into `op_in` and everything else into the
//! payload buffer. Requests are reassembled into the classic contiguous
//! layout and dispatched through fuser (vendor/fuser/src/ring.rs); the reply
//! goes back into the same entry and is committed with
//! COMMIT_AND_FETCH, which also re-arms the entry. Replies made on other
//! threads (async operations) are handed to the queue thread through an
//! eventfd, so every ring command is issued by its queue's thread.
//!
//! If the kernel does not offer the feature, or any queue fails to register,
//! the ring never becomes ready and the kernel keeps using /dev/fuse.

use fuser::ring::{RingDispatcher, RingSink};
use io_uring::{IoUring, opcode, squeue::Entry128, types};
use parking_lot::Mutex;
use std::io::{self, IoSlice};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const IN_OUT: usize = 128;
const OP_IN: usize = 128;
/// `struct fuse_uring_req_header`: in_out, op_in, `fuse_uring_ent_in_out`.
const HEADER: usize = IN_OUT + OP_IN + 32;
const ENT_COMMIT_ID: usize = IN_OUT + OP_IN + 8;
const ENT_PAYLOAD_SZ: usize = IN_OUT + OP_IN + 16;
const IN_HEADER: usize = 40; // struct fuse_in_header
const OUT_HEADER: usize = 16; // struct fuse_out_header
const CMD_REGISTER: u32 = 1;
const CMD_COMMIT_AND_FETCH: u32 = 2;
/// Entries per queue: one being answered while the next request waits.
const DEPTH: usize = 2;
const EVENTFD_TAG: u64 = u64::MAX;
const _: () = assert!(std::mem::size_of::<Entry128>() == 128);

/// Entry failures are logged once per process, not once per entry.
static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Where the parameter lives, for the warning when it is off.
pub const PARAM: &str = "/sys/module/fuse/parameters/enable_uring";

/// Whether the kernel's module parameter is on (None: no such parameter).
pub fn kernel_enabled() -> Option<bool> {
    std::fs::read_to_string(PARAM)
        .ok()
        .map(|s| matches!(s.trim(), "Y" | "y" | "1"))
}

#[repr(C, align(64))]
struct Header([u8; HEADER]);

struct Bufs {
    header: Box<Header>,
    payload: Box<[u8]>,
    commit_id: u64,
}

/// Kernel-visible addresses of one entry's buffers. They never move: the
/// boxes are allocated once and live as long as the queue.
struct Iov([libc::iovec; 2]);
// SAFETY: the pointers refer to heap buffers owned by the same Queue.
unsafe impl Send for Iov {}
unsafe impl Sync for Iov {}

#[derive(Debug, Default)]
pub struct Stats {
    pub queues: AtomicUsize,
    pub requests: AtomicUsize,
}

struct Queue {
    qid: u16,
    entries: Vec<Mutex<Bufs>>,
    iovs: Vec<Iov>,
    /// Entries answered and waiting for COMMIT_AND_FETCH.
    ready: Mutex<Vec<usize>>,
    wake: OwnedFd,
    thread: std::sync::OnceLock<std::thread::ThreadId>,
}

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Queue({})", self.qid)
    }
}

impl RingSink for Queue {
    fn reply(&self, slot: u64, data: &[IoSlice<'_>]) -> io::Result<()> {
        let slot = slot as usize;
        {
            let mut e = self.entries[slot].lock();
            let Bufs {
                header, payload, ..
            } = &mut *e;
            let h = &mut header.0;
            // First OUT_HEADER bytes: fuse_out_header; the rest is payload.
            let mut head = [0u8; OUT_HEADER];
            let mut hn = 0;
            let mut pn = 0;
            let mut overflow = false;
            for s in data {
                let mut s: &[u8] = s;
                if hn < OUT_HEADER {
                    let k = (OUT_HEADER - hn).min(s.len());
                    head[hn..hn + k].copy_from_slice(&s[..k]);
                    hn += k;
                    s = &s[k..];
                }
                if pn + s.len() > payload.len() {
                    overflow = true;
                    break;
                }
                payload[pn..pn + s.len()].copy_from_slice(s);
                pn += s.len();
            }
            if overflow || hn < OUT_HEADER {
                // Cannot happen with the negotiated sizes; fail the request
                // rather than the ring.
                head[0..4].copy_from_slice(&(OUT_HEADER as u32).to_ne_bytes());
                head[4..8].copy_from_slice(&(-libc::EIO).to_ne_bytes());
                pn = 0;
            }
            h[..IN_OUT].fill(0);
            h[..OUT_HEADER].copy_from_slice(&head);
            h[ENT_PAYLOAD_SZ..ENT_PAYLOAD_SZ + 4].copy_from_slice(&(pn as u32).to_ne_bytes());
        }
        self.ready.lock().push(slot);
        if self.thread.get() != Some(&std::thread::current().id()) {
            let one = 1u64.to_ne_bytes();
            // SAFETY: writing 8 bytes to an eventfd we own.
            unsafe { libc::write(self.wake.as_raw_fd(), one.as_ptr().cast(), 8) };
        }
        Ok(())
    }
}

fn cmd_bytes(commit_id: u64, qid: u16) -> [u8; 80] {
    let mut c = [0u8; 80];
    c[8..16].copy_from_slice(&commit_id.to_ne_bytes());
    c[16..18].copy_from_slice(&qid.to_ne_bytes());
    c
}

/// A URING_CMD on /dev/fuse carrying the entry's two iovecs.
fn ring_cmd(fd: RawFd, op: u32, iov: &Iov, commit_id: u64, qid: u16, slot: usize) -> Entry128 {
    let e = opcode::UringCmd80::new(types::Fd(fd), op)
        .cmd(cmd_bytes(commit_id, qid))
        .addr(Some(iov.0.as_ptr() as u64))
        .build()
        .user_data(slot as u64);
    // The builder has no `len`; the kernel requires len = 2 (iovec count).
    // SAFETY: Entry128 is a repr(C) 128-byte io_uring_sqe; `len` is the
    // u32 at offset 24.
    let mut raw: [u8; 128] = unsafe { std::mem::transmute(e) };
    raw[24..28].copy_from_slice(&2u32.to_ne_bytes());
    unsafe { std::mem::transmute::<[u8; 128], Entry128>(raw) }
}

fn possible_cpus() -> usize {
    // "0-19" or "0-3,8-11": the kernel sizes its queue array by the highest.
    std::fs::read_to_string("/sys/devices/system/cpu/possible")
        .ok()
        .and_then(|s| {
            s.trim()
                .split(',')
                .filter_map(|r| r.rsplit('-').next()?.parse::<usize>().ok())
                .max()
        })
        .map(|m| m + 1)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()))
}

/// Payload buffer size: the kernel wants room for max(max_write,
/// max_pages * page size); max_pages is clamped to fs.fuse.max_pages_limit.
fn payload_size(max_write: usize, max_pages: usize) -> usize {
    let page = page_size();
    let limit = std::fs::read_to_string("/proc/sys/fs/fuse/max_pages_limit")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .unwrap_or(256);
    max_write.max(max_pages.min(limit) * page).max(8192)
}

fn page_size() -> usize {
    // SAFETY: sysconf has no preconditions.
    let p = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if p > 0 { p as usize } else { 4096 }
}

/// Start one queue per possible CPU. Returns once every queue has
/// registered its entries (or failed to). The rings then run until unmount.
pub fn start(
    dispatcher: RingDispatcher,
    max_write: usize,
    max_pages: usize,
    stats: Arc<Stats>,
) -> io::Result<usize> {
    let n = possible_cpus();
    let payload = payload_size(max_write, max_pages);
    let fd = dispatcher.dev_fd().as_raw_fd();
    let (tx, rx) = std::sync::mpsc::channel::<Result<(), String>>();
    for cpu in 0..n {
        let dispatcher = dispatcher.clone();
        let tx = tx.clone();
        let stats = stats.clone();
        std::thread::Builder::new()
            .name(format!("fuse-uring-{cpu}"))
            .spawn(move || run_queue(cpu, fd, payload, dispatcher, stats, tx))?;
    }
    drop(tx);
    let mut ok = 0;
    let mut first_err = None;
    for r in rx.iter().take(n) {
        match r {
            Ok(()) => ok += 1,
            Err(e) => {
                first_err.get_or_insert(e);
            }
        }
    }
    match first_err {
        None => Ok(ok),
        Some(e) => Err(io::Error::other(format!(
            "{ok} of {n} queues registered: {e}"
        ))),
    }
}

fn pin(cpu: usize) {
    // SAFETY: a zeroed cpu_set_t is valid; CPU_SET stays within its size.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if cpu < libc::CPU_SETSIZE as usize {
            libc::CPU_SET(cpu, &mut set);
            // An offline CPU cannot be pinned to; its queue still works.
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        }
    }
}

fn run_queue(
    cpu: usize,
    fd: RawFd,
    payload: usize,
    dispatcher: RingDispatcher,
    stats: Arc<Stats>,
    ready_tx: std::sync::mpsc::Sender<Result<(), String>>,
) {
    pin(cpu);
    let qid = cpu as u16;
    let setup = || -> io::Result<(IoUring<Entry128>, Arc<Queue>)> {
        let ring = IoUring::<Entry128>::builder().build(((DEPTH + 1) * 2) as u32)?;
        // SAFETY: eventfd returns a new descriptor or -1.
        let efd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
        if efd < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut entries = Vec::with_capacity(DEPTH);
        let mut iovs = Vec::with_capacity(DEPTH);
        for _ in 0..DEPTH {
            let mut header = Box::new(Header([0u8; HEADER]));
            let mut payload = vec![0u8; payload].into_boxed_slice();
            iovs.push(Iov([
                libc::iovec {
                    iov_base: header.0.as_mut_ptr().cast(),
                    iov_len: HEADER,
                },
                libc::iovec {
                    iov_base: payload.as_mut_ptr().cast(),
                    iov_len: payload.len(),
                },
            ]));
            entries.push(Mutex::new(Bufs {
                header,
                payload,
                commit_id: 0,
            }));
        }
        let q = Arc::new(Queue {
            qid,
            entries,
            iovs,
            ready: Mutex::new(Vec::new()),
            // SAFETY: efd was just created and is owned here.
            wake: unsafe { OwnedFd::from_raw_fd(efd) },
            thread: std::sync::OnceLock::new(),
        });
        let _ = q.thread.set(std::thread::current().id());
        Ok((ring, q))
    };
    let (mut ring, q) = match setup() {
        Ok(v) => v,
        Err(e) => {
            let _ = ready_tx.send(Err(format!("queue {qid}: {e}")));
            return;
        }
    };
    // Leaked: a read may still be pending on it when the ring closes.
    let wake_buf: &'static mut [u8; 8] = Box::leak(Box::new([0u8; 8]));
    let wake_ptr = wake_buf.as_mut_ptr();
    let arm_wake = |ring: &mut IoUring<Entry128>| {
        let e: Entry128 = opcode::Read::new(types::Fd(q.wake.as_raw_fd()), wake_ptr, 8)
            .build()
            .user_data(EVENTFD_TAG)
            .into();
        // SAFETY: the buffer is never freed.
        unsafe {
            ring.submission()
                .push(&e)
                .map_err(|_| io::Error::other("SQ full"))
        }
    };
    let registered = (|| -> io::Result<()> {
        for slot in 0..DEPTH {
            let e = ring_cmd(fd, CMD_REGISTER, &q.iovs[slot], 0, qid, slot);
            // SAFETY: the iovecs and buffers outlive the command.
            unsafe { ring.submission().push(&e) }.map_err(|_| io::Error::other("SQ full"))?;
        }
        arm_wake(&mut ring)?;
        ring.submit()?;
        Ok(())
    })();
    if let Err(e) = registered {
        let _ = ready_tx.send(Err(format!("queue {qid}: {e}")));
        return;
    }
    // Registration errors come back as completions; the first request (or
    // teardown) completes them later, so report success once submitted and
    // log any failure below.
    let _ = ready_tx.send(Ok(()));
    stats.queues.fetch_add(1, Ordering::Relaxed);

    let mut live = DEPTH;
    let mut req = Vec::with_capacity(IN_HEADER + OP_IN + payload);
    let mut done: Vec<(u64, i32)> = Vec::with_capacity(DEPTH + 1);
    while live > 0 {
        match ring.submit_and_wait(1) {
            Ok(_) => {}
            Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => {
                tracing::warn!(qid, error = %e, "FUSE io_uring queue stopped");
                break;
            }
        }
        done.clear();
        done.extend(ring.completion().map(|c| (c.user_data(), c.result())));
        for &(tag, res) in &done {
            if tag == EVENTFD_TAG {
                if let Err(e) = arm_wake(&mut ring) {
                    tracing::warn!(qid, error = %e, "cannot re-arm wakeup");
                }
                continue;
            }
            let slot = tag as usize;
            if res < 0 {
                live -= 1;
                match -res {
                    libc::ENOTCONN | libc::ENODEV | libc::ECANCELED => {}
                    e => {
                        if !WARNED.swap(true, Ordering::Relaxed) {
                            tracing::warn!(
                                qid,
                                error = %io::Error::from_raw_os_error(e),
                                "FUSE io_uring entry failed; requests fall back to /dev/fuse"
                            );
                        }
                    }
                }
                continue;
            }
            // A request is in the entry: rebuild the /dev/fuse layout.
            req.clear();
            let unique;
            {
                let mut guard = q.entries[slot].lock();
                let e = &mut *guard;
                let h = &e.header.0;
                let len = u32::from_ne_bytes(h[0..4].try_into().unwrap()) as usize;
                unique = u64::from_ne_bytes(h[8..16].try_into().unwrap());
                let commit_id =
                    u64::from_ne_bytes(h[ENT_COMMIT_ID..ENT_COMMIT_ID + 8].try_into().unwrap());
                let psz =
                    u32::from_ne_bytes(h[ENT_PAYLOAD_SZ..ENT_PAYLOAD_SZ + 4].try_into().unwrap())
                        as usize;
                let op_len = len.checked_sub(IN_HEADER + psz).filter(|n| *n <= OP_IN);
                e.commit_id = commit_id;
                if let Some(op_len) = op_len.filter(|_| psz <= e.payload.len()) {
                    req.extend_from_slice(&h[..IN_HEADER]);
                    req.extend_from_slice(&h[IN_OUT..IN_OUT + op_len]);
                    req.extend_from_slice(&e.payload[..psz]);
                }
            }
            stats.requests.fetch_add(1, Ordering::Relaxed);
            let sink: Arc<dyn RingSink> = q.clone();
            if req.is_empty() || !dispatcher.dispatch(&req, sink, slot as u64) {
                tracing::warn!(qid, unique, "malformed FUSE io_uring request");
                let mut out = [0u8; OUT_HEADER];
                out[0..4].copy_from_slice(&(OUT_HEADER as u32).to_ne_bytes());
                out[4..8].copy_from_slice(&(-libc::EIO).to_ne_bytes());
                out[8..16].copy_from_slice(&unique.to_ne_bytes());
                let _ = q.reply(slot as u64, &[IoSlice::new(&out)]);
            }
        }
        // Commit answered entries and fetch their next requests.
        let ready: Vec<usize> = std::mem::take(&mut *q.ready.lock());
        for slot in ready {
            let commit_id = q.entries[slot].lock().commit_id;
            let e = ring_cmd(
                fd,
                CMD_COMMIT_AND_FETCH,
                &q.iovs[slot],
                commit_id,
                qid,
                slot,
            );
            // SAFETY: as for registration.
            while unsafe { ring.submission().push(&e) }.is_err() {
                let _ = ring.submit();
            }
        }
    }
    stats.queues.fetch_sub(1, Ordering::Relaxed);
    if live > 0 {
        // Stopped abnormally: the kernel may still hold entries pointing
        // at these buffers, so they must never be freed.
        std::mem::forget(q.clone());
    }
    drop(ring);
    tracing::debug!(qid, "FUSE io_uring queue finished");
}
