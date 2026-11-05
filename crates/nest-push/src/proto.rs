//! Messages between an engine and its local sparknestd on the push socket
//! (ADR-063 §8). Each SEQPACKET message is one postcard-encoded value;
//! `Join` carries the ring's fd as SCM_RIGHTS.
//!
//! Coordinator (on the hub):  Create → Created, Patterns…, Entries…, Start
//!                            → Planned, Progress…, Done | Failed
//! Each rank (on its host):   Join (+ fd) → Joined, Filled… / Release…,
//!                            Done | Failed
//!
//! Enums here are append-only: an older peer refuses a message it cannot
//! decode, and the engine falls back to reading through the mount.

use crate::plan::{Entry, Pattern};
use serde::{Deserialize, Serialize};

/// The default socket: abstract, so containers sharing the host's network
/// namespace reach it without a bind mount.
pub const SOCKET: &str = "@sparknest-push";
/// Largest message. A SEQPACKET message must fit the sender's socket
/// buffer, which an unprivileged process cannot raise past
/// `net.core.wmem_max` (often 212,992 bytes), so this stays below the
/// default. The client splits `Patterns` and `Entries` into batches under
/// it; one pattern larger than this (an index list of ~25,000 entries) is
/// refused.
pub const MAX_MSG: usize = 128 << 10;
/// Protocol version spoken on the socket.
pub const VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RankSpec {
    pub rank: u32,
    /// The host the rank runs on, by name (as `user.sparknest.cluster`
    /// lists them).
    pub host: String,
    pub arena_bytes: u64,
}

/// Engine → sparknestd.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Request {
    /// Open a group on this host (the hub). The socket that sent it owns
    /// the group: closing it cancels the load.
    Create {
        version: u32,
        ranks: Vec<RankSpec>,
        /// Namespace paths, as under the mount.
        files: Vec<String>,
    },
    /// Patterns, appended in order (ids are their positions).
    Patterns(Vec<Pattern>),
    /// Plan entries, appended.
    Entries(Vec<Entry>),
    /// Check the plan and start streaming once every rank has joined.
    Start,
    /// Join `group` as `rank`, with the ring's fd. The ring is
    /// `slots × slot_bytes`.
    Join {
        version: u32,
        group: u64,
        token: u64,
        rank: u32,
        slots: u32,
        slot_bytes: u32,
    },
    /// The engine is done with this slot (its copy out has completed).
    Release { slot: u32 },
}

/// sparknestd → engine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    Created {
        group: u64,
        token: u64,
    },
    /// The plan checked out; what the hub expects.
    Planned(Forecast),
    /// Bytes delivered to every rank so far: to the coordinator, and since
    /// ADR-087 to each waiting rank too, so its idle deadline sees the
    /// group moving. A rank skips it.
    Progress {
        done: u64,
        total: u64,
    },
    Joined,
    /// `len` bytes of `slot` hold a checked slot (see `slot::parse`).
    Filled {
        slot: u32,
        len: u32,
    },
    /// Everything for this rank (or, for the coordinator, every rank) has
    /// been delivered: every `Filled` came before this.
    Done,
    /// The group failed. Phase 1: the engine loads the whole group through
    /// the mount.
    Failed {
        reason: String,
        missing: Vec<Missing>,
    },
    /// A request was refused (bad plan, unknown group, older host).
    Refused(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Missing {
    pub file: String,
    pub ranks: Vec<u32>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Forecast {
    pub streams: Vec<StreamForecast>,
    pub rank_bytes: Vec<u64>,
    /// Milliseconds, from the slowest stream at its expected rate.
    pub millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamForecast {
    /// The store read (a host's name, or an archive's).
    pub store: String,
    pub bytes: u64,
    pub files: u32,
    /// "rdma", "tcp" or "local" (the slowest transport it uses).
    pub transport: String,
    pub bytes_per_s: u64,
}

pub fn encode<T: Serialize>(v: &T) -> Vec<u8> {
    postcard::to_allocvec(v).expect("encodable")
}

pub fn decode<'a, T: Deserialize<'a>>(b: &'a [u8]) -> Result<T, postcard::Error> {
    postcard::from_bytes(b)
}
