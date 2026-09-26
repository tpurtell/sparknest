//! The node-local data service.
//!
//! [`DataNode`] turns replicated facts into local actions and owns
//! everything a node does with file contents:
//!
//! - Effects from the state machine arrive synchronously
//!   ([`nest_raft::EffectHandler`]); object operations are queued on
//!   per-file-ordered workers and start immediately. An invalidated replica
//!   is fenced from reads at once and its deletion starts without any job
//!   engine, debounce or garbage-collection pass (PROPOSAL invariant 4).
//! - Startup and snapshot-install reconciliation make the object directory
//!   agree with committed metadata before anything is served.
//! - Sessions, orphan release, and (later milestones) ranged reads,
//!   owner-routed writes and whole-file transfers.

mod local;
mod session;
pub mod vfs;

pub use local::{DataNode, ReconcileReport};
pub use vfs::{OpenMode, Vfs, VfsConfig};
