//! Replicated metadata service: openraft over SQLite.
//!
//! - [`log_store`]: the Raft log, vote and purge state in `raft.sqlite`
//!   (synchronous=FULL; the durability source).
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

/// The state machine's answer to one log entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response(pub Result<Reply, NestError>);

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        NodeId = u64,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = tokio::fs::File,
        AsyncRuntime = openraft::TokioRuntime,
);

pub type Raft = openraft::Raft<TypeConfig>;
pub type LogId = openraft::LogId<u64>;
pub type StorageError = openraft::StorageError<u64>;
