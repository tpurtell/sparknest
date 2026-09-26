//! Replicated metadata: the namespace, file lifecycle, replicas, sessions and
//! stores, materialized in SQLite.
//!
//! Everything that changes replicated state is a [`Command`]. [`apply`] runs
//! one command inside the caller's transaction, deterministically: the same
//! sequence of commands produces byte-identical databases on every node.
//! Besides its reply, apply returns [`Effect`]s: facts about what changed that
//! node-local services act on (delete an invalidated replica, drop kernel
//! caches, release orphans). Effects are computed identically everywhere;
//! each node picks out the ones addressed to it.
//!
//! Read-side queries live in [`query`] and run on any connection.

mod apply;
mod command;
mod effect;
pub mod fsck;
pub mod query;
pub mod schema;

pub use apply::apply;
pub use command::{Command, CreateReply, LockKind, RenameFlags, Reply, SealPolicy, StoreClass};
pub use effect::Effect;
pub use schema::{open_memory, open_read, open_write};
