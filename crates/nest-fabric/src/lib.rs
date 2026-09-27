//! The RDMA fabric (PROPOSAL §5, ADR-004, ADR-016).
//!
//! A small C shim (`csrc/nf_shim.c`) wraps libibverbs; `verbs` provides
//! RAII wrappers; `rail` discovers RoCE v2 rails. The link and read
//! protocol live in `link` and `engine`.

mod engine;
mod pool;
pub mod rail;
pub(crate) mod sys;
pub use sys::nf_wc as Completion;
pub use sys::{
    NF_OP_RECV as OP_RECV, NF_OP_RECV_IMM as OP_RECV_IMM, NF_OP_SEND as OP_SEND,
    NF_OP_WRITE as OP_WRITE,
};
pub mod verbs;

pub use engine::{Fabric, FabricConfig, ReadBuf, ReadSource, Stats, Tier};
pub use pool::{Pool, Slot, Tiers};
pub use rail::{Rail, discover};
