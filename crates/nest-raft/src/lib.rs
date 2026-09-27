//! Replicated metadata service: openraft over SQLite.
//!
//! - [`log_store`]: the Raft log, vote and purge state in `raft.sqlite`
//!   (WAL, synchronous=NORMAL; votes are fsynced, ADR-026).
//! - [`checkpoint`]: our own WAL checkpointing for both databases.
//! - [`state_machine`]: applies committed [`Request`]s to `meta.sqlite` via
//!   `nest-meta`, deduplicates retried requests, snapshots with
//!   `VACUUM INTO`, installs snapshots with the SQLite restore API, and
//!   hands every applied batch's effects to an [`EffectHandler`]
//!   synchronously, before the entries count as applied.
//! - [`network`]: openraft's network over `nest-rpc`.
//! - [`node`]: [`MetaNode`], the API the rest of the daemon uses: propose
//!   with leader forwarding and read-your-writes, read barriers, membership.

// openraft's trait signatures fix these large error types.
#![allow(clippy::result_large_err)]

pub mod checkpoint;
pub mod log_store;
pub mod network;
pub mod node;
pub mod state_machine;

pub use node::{MetaNode, MetaNodeConfig};
pub use state_machine::{EffectHandler, SmEvent};

use nest_meta::{Command, Reply};
use nest_types::NestError;
use serde::{Deserialize, Serialize};

/// A command with a client identity for exactly-once application across
/// retries: `(client, seq)` pairs are remembered by the state machine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    pub client: u64,
    pub seq: u64,
    pub cmd: Command,
}

impl std::fmt::Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "request {}:{}", self.client, self.seq)
    }
}

/// The state machine's answer to one log entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response(pub Result<Reply, NestError>);

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
);

pub type Raft = openraft::Raft<TypeConfig, state_machine::StateMachine>;
pub type LogId = openraft::alias::LogIdOf<TypeConfig>;
pub type Vote = openraft::alias::VoteOf<TypeConfig>;
pub type Entry = openraft::alias::EntryOf<TypeConfig>;

/// On-disk format of `raft.sqlite` and of the openraft values kept in
/// `meta.sqlite` (`sm_state`). 2 = openraft 0.10. Independent of the
/// metadata schema (`nest_meta::FORMAT_VERSION`, `SCHEMA_VERSION`).
pub const RAFT_FORMAT: u32 = 2;
