//! Raw declarations for `csrc/nf_shim.c`. Everything unsafe lives here and
//! in `verbs.rs`.

#![allow(non_camel_case_types)]

use std::os::raw::{c_char, c_int, c_void};

#[repr(C)]
pub struct nf_ctx {
    _p: [u8; 0],
}
#[repr(C)]
pub struct nf_cq {
    _p: [u8; 0],
}
#[repr(C)]
pub struct nf_mr {
    _p: [u8; 0],
}
#[repr(C)]
pub struct nf_qp {
    _p: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct nf_wc {
    pub wr_id: u64,
    pub status: u32,
    pub opcode: u32,
    pub byte_len: u32,
    pub imm: u32,
    pub has_imm: u32,
    pub qp_num: u32,
}

pub const NF_OP_SEND: u32 = 1;
pub const NF_OP_RECV: u32 = 2;
pub const NF_OP_WRITE: u32 = 3;
pub const NF_OP_RECV_IMM: u32 = 4;

unsafe extern "C" {
    pub fn nf_open(ibdev: *const c_char) -> *mut nf_ctx;
    pub fn nf_close(c: *mut nf_ctx);
    pub fn nf_query_port(c: *mut nf_ctx, port: u8, state: *mut u32, active_mtu: *mut u32) -> c_int;
    pub fn nf_max_qp_wr(c: *mut nf_ctx) -> c_int;
    pub fn nf_reg(c: *mut nf_ctx, addr: *mut c_void, len: usize, remote_write: c_int)
    -> *mut nf_mr;
    pub fn nf_lkey(m: *const nf_mr) -> u32;
    pub fn nf_rkey(m: *const nf_mr) -> u32;
    pub fn nf_dereg(m: *mut nf_mr);
    pub fn nf_cq_create(c: *mut nf_ctx, depth: c_int, with_channel: c_int) -> *mut nf_cq;
    pub fn nf_cq_fd(q: *const nf_cq) -> c_int;
    pub fn nf_cq_arm(q: *mut nf_cq) -> c_int;
    pub fn nf_cq_take_event(q: *mut nf_cq) -> c_int;
    pub fn nf_cq_destroy(q: *mut nf_cq);
    pub fn nf_poll(q: *mut nf_cq, out: *mut nf_wc, max: c_int) -> c_int;
    pub fn nf_qp_create(
        c: *mut nf_ctx,
        scq: *mut nf_cq,
        rcq: *mut nf_cq,
        max_send: u32,
        max_recv: u32,
        port: u8,
    ) -> *mut nf_qp;
    pub fn nf_qp_num(q: *const nf_qp) -> u32;
    pub fn nf_qp_connect(
        q: *mut nf_qp,
        port: u8,
        sgid_index: c_int,
        local_psn: u32,
        remote_gid: *const u8,
        remote_qpn: u32,
        remote_psn: u32,
        mtu: u32,
    ) -> c_int;
    pub fn nf_qp_error(q: *mut nf_qp) -> c_int;
    pub fn nf_qp_destroy(q: *mut nf_qp);
    pub fn nf_post_recv(q: *mut nf_qp, wr_id: u64, addr: *mut c_void, len: u32, lkey: u32)
    -> c_int;
    pub fn nf_post_send(
        q: *mut nf_qp,
        wr_id: u64,
        addr: *mut c_void,
        len: u32,
        lkey: u32,
        inline_data: c_int,
    ) -> c_int;
    pub fn nf_post_write_imm(
        q: *mut nf_qp,
        wr_id: u64,
        addr: *mut c_void,
        len: u32,
        lkey: u32,
        raddr: u64,
        rkey: u32,
        imm: u32,
    ) -> c_int;
}
