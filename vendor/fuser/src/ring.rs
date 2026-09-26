//! sparknest patch: hooks for a FUSE-over-io_uring transport.
//!
//! The kernel hands requests to per-CPU io_uring entries instead of
//! /dev/fuse reads once the daemon negotiates `FUSE_OVER_IO_URING` and
//! registers entries for every queue. The transport itself lives in the
//! caller; this module lets it feed a request (reassembled as the classic
//! contiguous `fuse_in_header` + arguments buffer) through fuser's normal
//! decoding and `Filesystem` dispatch, with the reply routed back to the ring
//! entry the request came from.

use std::fmt;
use std::io;
use std::io::IoSlice;
use std::os::fd::AsFd;
use std::os::fd::BorrowedFd;
use std::sync::Arc;

use crate::Filesystem;
use crate::Session;
use crate::request::RequestWithSender;
use crate::session::FilesystemHolder;
use crate::session::SessionEventLoop;

/// Receives the reply for a request that arrived on a ring entry.
pub trait RingSink: Send + Sync + fmt::Debug {
    /// `data` is the reply exactly as it would be written to /dev/fuse:
    /// a `fuse_out_header` followed by the reply payload. Called once per
    /// request, from any thread.
    fn reply(&self, slot: u64, data: &[IoSlice<'_>]) -> io::Result<()>;
}

#[derive(Clone, Debug)]
pub(crate) struct RingTarget {
    pub(crate) sink: Arc<dyn RingSink>,
    pub(crate) slot: u64,
}

trait Dispatch: Send + Sync {
    fn dispatch(&self, request: &[u8], target: RingTarget) -> bool;
    fn fd(&self) -> BorrowedFd<'_>;
}

impl<FS: Filesystem> Dispatch for SessionEventLoop<FS> {
    fn dispatch(&self, request: &[u8], target: RingTarget) -> bool {
        match RequestWithSender::new(self.ch.sender().with_ring(target), request) {
            Some(req) => {
                req.dispatch(self);
                true
            }
            None => false,
        }
    }

    fn fd(&self) -> BorrowedFd<'_> {
        self.ch.as_fd()
    }
}

/// Dispatches ring requests into a filesystem instance.
#[derive(Clone)]
pub struct RingDispatcher(Arc<dyn Dispatch>);

impl fmt::Debug for RingDispatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RingDispatcher")
    }
}

impl RingDispatcher {
    /// Decode and dispatch one request. Returns false if it could not be
    /// parsed; the caller must then answer the entry itself.
    pub fn dispatch(&self, request: &[u8], sink: Arc<dyn RingSink>, slot: u64) -> bool {
        self.0.dispatch(request, RingTarget { sink, slot })
    }

    /// The session's /dev/fuse descriptor (the target of ring commands).
    pub fn dev_fd(&self) -> BorrowedFd<'_> {
        self.0.fd()
    }
}

impl<FS: Filesystem> Session<FS> {
    /// A dispatcher for ring requests into `fs`, which should share state
    /// with the session's own filesystem (e.g. a clone of a handle type).
    /// Call after `new` (INIT is complete) and before `spawn`.
    pub fn ring_dispatcher<F2: Filesystem>(&self, fs: F2) -> RingDispatcher {
        RingDispatcher(Arc::new(SessionEventLoop {
            thread_name: "fuse-ring".to_string(),
            ch: self.ch.clone(),
            filesystem: Arc::new(FilesystemHolder { fs: Some(fs) }),
            allowed: self.allowed,
            session_owner: self.session_owner,
        }))
    }
}
