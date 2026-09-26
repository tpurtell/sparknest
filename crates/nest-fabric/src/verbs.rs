//! Safe RAII wrappers over the shim. Ownership rules:
//! - `Context` outlives every CQ, QP and MR created from it (they hold an
//!   `Arc<Context>`).
//! - A registered buffer (`Region`) owns its memory and its MR; memory is
//!   never freed while registered.
//! - Work requests reference registered memory by address; callers
//!   guarantee a buffer is not reused before its completion arrives.

use crate::sys;
use std::ffi::CString;
use std::io;
use std::os::raw::c_void;
use std::sync::Arc;

fn last_err(what: &str) -> io::Error {
    let e = io::Error::last_os_error();
    io::Error::new(e.kind(), format!("{what}: {e}"))
}

pub struct Context {
    raw: *mut sys::nf_ctx,
    pub name: String,
}

// SAFETY: libibverbs contexts and PDs are thread-safe objects.
unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    pub fn open(ibdev: &str) -> io::Result<Arc<Context>> {
        let c = CString::new(ibdev).map_err(|_| io::Error::other("bad device name"))?;
        // SAFETY: valid NUL-terminated string; result checked for NULL.
        let raw = unsafe { sys::nf_open(c.as_ptr()) };
        if raw.is_null() {
            return Err(last_err(&format!("opening {ibdev}")));
        }
        Ok(Arc::new(Context {
            raw,
            name: ibdev.to_string(),
        }))
    }

    /// (state, active_mtu enum) of `port`.
    pub fn port(&self, port: u8) -> io::Result<(u32, u32)> {
        let (mut s, mut m) = (0u32, 0u32);
        // SAFETY: raw is live; out-pointers are valid.
        if unsafe { sys::nf_query_port(self.raw, port, &mut s, &mut m) } != 0 {
            return Err(last_err("query port"));
        }
        Ok((s, m))
    }

    pub fn max_qp_wr(&self) -> io::Result<u32> {
        // SAFETY: raw is live.
        let n = unsafe { sys::nf_max_qp_wr(self.raw) };
        if n < 0 {
            Err(last_err("query device"))
        } else {
            Ok(n as u32)
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        // SAFETY: every dependent object holds an Arc<Context>, so none remain.
        unsafe { sys::nf_close(self.raw) }
    }
}

/// Page-aligned memory registered with a context.
pub struct Region {
    ctx: Arc<Context>,
    mr: *mut sys::nf_mr,
    ptr: *mut u8,
    len: usize,
    layout: std::alloc::Layout,
}

// SAFETY: the region's memory is plain bytes; concurrent access to distinct
// slots is coordinated by the pool that owns it.
unsafe impl Send for Region {}
unsafe impl Sync for Region {}

impl Region {
    pub fn new(ctx: &Arc<Context>, len: usize, remote_write: bool) -> io::Result<Region> {
        let layout = std::alloc::Layout::from_size_align(len.max(4096), 4096)
            .map_err(|_| io::Error::other("bad region size"))?;
        // SAFETY: non-zero size layout.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(io::Error::other("allocating registered region"));
        }
        // SAFETY: ptr..ptr+len is owned by us and outlives the MR.
        let mr = unsafe { sys::nf_reg(ctx.raw, ptr as *mut c_void, len, remote_write as i32) };
        if mr.is_null() {
            let e = last_err("ibv_reg_mr (check `ulimit -l`)");
            // SAFETY: allocated above with this layout.
            unsafe { std::alloc::dealloc(ptr, layout) };
            return Err(e);
        }
        Ok(Region {
            ctx: ctx.clone(),
            mr,
            ptr,
            len,
            layout,
        })
    }

    pub fn lkey(&self) -> u32 {
        // SAFETY: mr is live.
        unsafe { sys::nf_lkey(self.mr) }
    }

    pub fn rkey(&self) -> u32 {
        // SAFETY: mr is live.
        unsafe { sys::nf_rkey(self.mr) }
    }

    pub fn addr(&self) -> u64 {
        self.ptr as u64
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Raw pointer to `offset` within the region.
    pub fn ptr_at(&self, offset: usize) -> *mut u8 {
        assert!(offset <= self.len);
        // SAFETY: bounds checked.
        unsafe { self.ptr.add(offset) }
    }

    /// A shared view of `[offset, offset+len)`.
    ///
    /// # Safety
    /// No DMA or other writer may be modifying that range for the lifetime
    /// of the returned slice.
    pub unsafe fn slice(&self, offset: usize, len: usize) -> &[u8] {
        assert!(offset + len <= self.len);
        // SAFETY: bounds checked; exclusivity guaranteed by the caller.
        unsafe { std::slice::from_raw_parts(self.ptr.add(offset), len) }
    }

    /// A mutable view of `[offset, offset+len)`.
    ///
    /// # Safety
    /// The caller has exclusive use of that range (no DMA, no other view).
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn slice_mut(&self, offset: usize, len: usize) -> &mut [u8] {
        assert!(offset + len <= self.len);
        // SAFETY: bounds checked; exclusivity guaranteed by the caller.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.add(offset), len) }
    }

    pub fn context(&self) -> &Arc<Context> {
        &self.ctx
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        // SAFETY: deregister before freeing; no work request may still
        // reference the region (owners drain their queues first).
        unsafe {
            sys::nf_dereg(self.mr);
            std::alloc::dealloc(self.ptr, self.layout);
        }
    }
}

pub struct Cq {
    _ctx: Arc<Context>,
    raw: *mut sys::nf_cq,
}

// SAFETY: CQ polling is serialized by its single poller thread; arming and
// event consumption are thread-safe in libibverbs.
unsafe impl Send for Cq {}
unsafe impl Sync for Cq {}

impl Cq {
    pub fn new(ctx: &Arc<Context>, depth: u32, with_channel: bool) -> io::Result<Arc<Cq>> {
        // SAFETY: ctx is live.
        let raw = unsafe { sys::nf_cq_create(ctx.raw, depth as i32, with_channel as i32) };
        if raw.is_null() {
            return Err(last_err("ibv_create_cq"));
        }
        Ok(Arc::new(Cq {
            _ctx: ctx.clone(),
            raw,
        }))
    }

    pub fn poll(&self, out: &mut [sys::nf_wc]) -> io::Result<usize> {
        // SAFETY: out is valid for out.len() entries; poll is serialized by
        // the owning poller.
        let n = unsafe { sys::nf_poll(self.raw, out.as_mut_ptr(), out.len().min(32) as i32) };
        if n < 0 {
            Err(last_err("ibv_poll_cq"))
        } else {
            Ok(n as usize)
        }
    }

    pub fn fd(&self) -> i32 {
        // SAFETY: raw is live.
        unsafe { sys::nf_cq_fd(self.raw) }
    }

    pub fn arm(&self) -> io::Result<()> {
        // SAFETY: raw is live.
        if unsafe { sys::nf_cq_arm(self.raw) } != 0 {
            Err(last_err("ibv_req_notify_cq"))
        } else {
            Ok(())
        }
    }

    pub fn take_event(&self) -> io::Result<()> {
        // SAFETY: raw is live and has a channel (caller checked fd >= 0).
        if unsafe { sys::nf_cq_take_event(self.raw) } != 0 {
            Err(last_err("ibv_get_cq_event"))
        } else {
            Ok(())
        }
    }
}

impl Drop for Cq {
    fn drop(&mut self) {
        // SAFETY: QPs using this CQ hold an Arc<Cq> and are destroyed first.
        unsafe { sys::nf_cq_destroy(self.raw) }
    }
}

pub struct Qp {
    _ctx: Arc<Context>,
    _scq: Arc<Cq>,
    _rcq: Arc<Cq>,
    raw: *mut sys::nf_qp,
    pub port: u8,
}

// SAFETY: posting to a QP is thread-safe in libibverbs.
unsafe impl Send for Qp {}
unsafe impl Sync for Qp {}

/// What the peer needs to connect to one of our QPs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QpInfo {
    pub qpn: u32,
    pub psn: u32,
    pub gid: [u8; 16],
    pub mtu: u32,
}

impl Qp {
    pub fn new(
        ctx: &Arc<Context>,
        scq: &Arc<Cq>,
        rcq: &Arc<Cq>,
        max_send: u32,
        max_recv: u32,
        port: u8,
    ) -> io::Result<Qp> {
        // SAFETY: all handles live.
        let raw = unsafe { sys::nf_qp_create(ctx.raw, scq.raw, rcq.raw, max_send, max_recv, port) };
        if raw.is_null() {
            return Err(last_err("ibv_create_qp"));
        }
        Ok(Qp {
            _ctx: ctx.clone(),
            _scq: scq.clone(),
            _rcq: rcq.clone(),
            raw,
            port,
        })
    }

    pub fn num(&self) -> u32 {
        // SAFETY: raw is live.
        unsafe { sys::nf_qp_num(self.raw) }
    }

    pub fn connect(
        &self,
        sgid_index: u32,
        local_psn: u32,
        remote: &QpInfo,
        mtu: u32,
    ) -> io::Result<()> {
        // SAFETY: raw live; gid is 16 bytes.
        let r = unsafe {
            sys::nf_qp_connect(
                self.raw,
                self.port,
                sgid_index as i32,
                local_psn,
                remote.gid.as_ptr(),
                remote.qpn,
                remote.psn,
                mtu,
            )
        };
        if r != 0 {
            Err(last_err("connecting QP (INIT->RTR->RTS)"))
        } else {
            Ok(())
        }
    }

    /// Force outstanding work to complete (with flush errors).
    pub fn set_error(&self) {
        // SAFETY: raw is live.
        unsafe { sys::nf_qp_error(self.raw) };
    }

    /// # Safety
    /// `[addr, addr+len)` must lie in a region registered on this QP's
    /// context with `lkey`, and stay untouched until the completion.
    pub unsafe fn post_recv(
        &self,
        wr_id: u64,
        addr: *mut u8,
        len: u32,
        lkey: u32,
    ) -> io::Result<()> {
        // SAFETY: forwarded caller contract.
        if unsafe { sys::nf_post_recv(self.raw, wr_id, addr as *mut c_void, len, lkey) } != 0 {
            return Err(last_err("ibv_post_recv"));
        }
        Ok(())
    }

    /// # Safety
    /// As for `post_recv`; with `inline_data` the bytes are copied at post
    /// time and the buffer may be reused immediately.
    pub unsafe fn post_send(
        &self,
        wr_id: u64,
        addr: *mut u8,
        len: u32,
        lkey: u32,
        inline_data: bool,
    ) -> io::Result<()> {
        // SAFETY: forwarded caller contract.
        if unsafe {
            sys::nf_post_send(
                self.raw,
                wr_id,
                addr as *mut c_void,
                len,
                lkey,
                inline_data as i32,
            )
        } != 0
        {
            return Err(last_err("ibv_post_send"));
        }
        Ok(())
    }

    /// # Safety
    /// Local range as for `post_recv`; the remote range must be a region the
    /// peer registered for remote write and is expecting this write.
    #[allow(clippy::too_many_arguments)]
    pub unsafe fn post_write_imm(
        &self,
        wr_id: u64,
        addr: *mut u8,
        len: u32,
        lkey: u32,
        raddr: u64,
        rkey: u32,
        imm: u32,
    ) -> io::Result<()> {
        // SAFETY: forwarded caller contract.
        if unsafe {
            sys::nf_post_write_imm(
                self.raw,
                wr_id,
                addr as *mut c_void,
                len,
                lkey,
                raddr,
                rkey,
                imm,
            )
        } != 0
        {
            return Err(last_err("ibv_post_send (write with imm)"));
        }
        Ok(())
    }
}

impl Drop for Qp {
    fn drop(&mut self) {
        // SAFETY: owners drain completions before dropping a QP.
        unsafe { sys::nf_qp_destroy(self.raw) }
    }
}
