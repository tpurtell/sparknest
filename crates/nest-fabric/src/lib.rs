//! The RDMA fabric (PROPOSAL §5, ADR-004, ADR-016).
//!
//! A small C shim (`csrc/nf_shim.c`) wraps libibverbs; `verbs` provides
//! RAII wrappers; `rail` discovers RoCE v2 rails. The link and read
//! protocol live in `link` and `engine`.

pub mod counters;
mod engine;
pub mod iostats;
mod pool;
pub mod push;
pub mod rail;
pub(crate) mod sys;
pub mod tcp;
pub use sys::nf_wc as Completion;
pub use sys::{
    NF_OP_READ as OP_READ, NF_OP_RECV as OP_RECV, NF_OP_RECV_IMM as OP_RECV_IMM,
    NF_OP_SEND as OP_SEND, NF_OP_WRITE as OP_WRITE,
};
pub mod verbs;

pub use engine::{
    Fabric, FabricConfig, PROTOCOL, ReadBuf, ReadDone, ReadSource, Reservation, ServeFile, Stats,
    TRAFFIC_CLASS, Tier, WriteError, WriteOp, WriteSink,
};
pub use pool::{Pool, Slot, Tiers};
pub use rail::{Rail, discover};
