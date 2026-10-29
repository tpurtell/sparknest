//! Shared vocabulary for every sparknest crate.
//!
//! Identifiers are plain newtypes over integers so they are cheap to copy,
//! store in SQLite, and put on the wire. Nothing in here performs I/O.

use serde::{Deserialize, Serialize};
use std::fmt;

mod error;
pub use error::{NestError, NestResult};

macro_rules! id_type {
    ($(#[$m:meta])* $name:ident($inner:ty)) => {
        $(#[$m])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
        #[serde(transparent)]
        pub struct $name(pub $inner);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }
        impl From<$inner> for $name {
            fn from(v: $inner) -> Self { Self(v) }
        }
    };
}

id_type!(
    /// Stable identity of a logical file object (the sparknest "inode").
    /// Never reused. `FileId::ROOT` is the root directory.
    FileId(u64)
);
id_type!(
    /// Content generation of a regular file. Increments each time a new
    /// ownership epoch begins mutating the file.
    Generation(u64)
);
id_type!(
    /// Ownership epoch: fences obsolete owners and requests. Distinct from
    /// [`Generation`]; bumped on every ownership grant.
    Epoch(u64)
);
id_type!(
    /// Raft node identity. Also the identity of the node's live store.
    NodeId(u64)
);
id_type!(
    /// A registered store (a node's live object store or an archive folder).
    StoreId(u64)
);
id_type!(
    /// A client session (one per node daemon incarnation). Handles and
    /// locks belong to sessions and die with them.
    SessionId(u64)
);

impl FileId {
    pub const ROOT: FileId = FileId(1);
}

impl NodeId {
    /// Every node's live object store has the same id as the node.
    pub fn live_store(self) -> StoreId {
        StoreId(self.0)
    }

    /// The store id of this host's extra disk number `k` (1..=255, ADR-073).
    pub fn disk_store(self, k: u8) -> StoreId {
        StoreId(StoreId::DISK_BASE + (self.0 << 8) + k as u64)
    }
}

impl StoreId {
    /// First archive store id; lower ids are hosts' live stores (ADR-019).
    pub const ARCHIVE_BASE: u64 = 1 << 20;
    /// Store ids from here up are hosts' extra disks: `DISK_BASE + (node <<
    /// 8) + k` (ADR-073), so every host knows a copy's host from its id.
    pub const DISK_BASE: u64 = 1 << 40;

    /// An archive store (a folder reached through gateways).
    pub fn is_archive(self) -> bool {
        (Self::ARCHIVE_BASE..Self::DISK_BASE).contains(&self.0)
    }

    /// One of a host's extra disks.
    pub fn is_disk(self) -> bool {
        self.0 >= Self::DISK_BASE
    }

    /// The host holding this store's copies: a live store's node or a
    /// disk's host; `None` for an archive.
    pub fn host(self) -> Option<NodeId> {
        if self.0 < Self::ARCHIVE_BASE {
            Some(NodeId(self.0))
        } else if self.is_disk() {
            Some(NodeId((self.0 - Self::DISK_BASE) >> 8))
        } else {
            None
        }
    }
}

/// Nanoseconds since the Unix epoch. Timestamps are chosen by the proposer
/// of a command so that apply stays deterministic on every replica.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct Timestamp(pub i64);

impl Timestamp {
    pub fn now() -> Self {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Timestamp(d.as_nanos() as i64)
    }
    pub fn as_system_time(self) -> std::time::SystemTime {
        if self.0 >= 0 {
            std::time::UNIX_EPOCH + std::time::Duration::from_nanos(self.0 as u64)
        } else {
            std::time::UNIX_EPOCH - std::time::Duration::from_nanos(self.0.unsigned_abs())
        }
    }
    pub fn from_system_time(t: std::time::SystemTime) -> Self {
        match t.duration_since(std::time::UNIX_EPOCH) {
            Ok(d) => Timestamp(d.as_nanos() as i64),
            Err(e) => Timestamp(-(e.duration().as_nanos() as i64)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileKind {
    Regular,
    Directory,
    Symlink,
}

impl FileKind {
    pub fn as_i64(self) -> i64 {
        match self {
            FileKind::Regular => 1,
            FileKind::Directory => 2,
            FileKind::Symlink => 3,
        }
    }
    pub fn from_i64(v: i64) -> Option<Self> {
        Some(match v {
            1 => FileKind::Regular,
            2 => FileKind::Directory,
            3 => FileKind::Symlink,
            _ => return None,
        })
    }
}

/// Replicated lifecycle of a regular file's current generation
/// (PROPOSAL §6.1, ADR-011). Revocation and finalization are owner-local
/// phases; every other node routes to the owner in both, so they are not
/// replicated states.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GenState {
    /// Settled content; any LIVE replica may serve it.
    Stable,
    /// One owner is authoritative for the changing content.
    Owned,
}

impl GenState {
    pub fn as_i64(self) -> i64 {
        match self {
            GenState::Stable => 0,
            GenState::Owned => 1,
        }
    }
    pub fn from_i64(v: i64) -> Option<Self> {
        Some(match v {
            0 => GenState::Stable,
            1 => GenState::Owned,
            _ => return None,
        })
    }
}

/// State of one complete copy of one generation in one store.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaState {
    /// Transfer in progress; never served.
    Staging,
    /// Complete and eligible to serve reads of its generation.
    Live,
    /// Invalidated; the holder is deleting it.
    Invalid,
}

impl ReplicaState {
    pub fn as_i64(self) -> i64 {
        match self {
            ReplicaState::Staging => 0,
            ReplicaState::Live => 1,
            ReplicaState::Invalid => 2,
        }
    }
    pub fn from_i64(v: i64) -> Option<Self> {
        Some(match v {
            0 => ReplicaState::Staging,
            1 => ReplicaState::Live,
            2 => ReplicaState::Invalid,
            _ => return None,
        })
    }
}

/// Attributes of a file object as stored in replicated metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileAttr {
    pub id: FileId,
    pub kind: FileKind,
    /// Permission bits only (`0o7777` mask). Ownership is presented as the
    /// mounting user on every host (ADR-007).
    pub perm: u32,
    pub size: u64,
    pub nlink: u32,
    pub atime: Timestamp,
    pub mtime: Timestamp,
    pub ctime: Timestamp,
    pub crtime: Timestamp,
    /// Current generation (regular files); 0 for directories and symlinks.
    pub generation: Generation,
    pub gen_state: GenState,
    /// Owner node while `gen_state` is Owned/Revoking/Finalizing.
    pub owner: Option<NodeId>,
    pub epoch: Epoch,
    /// Enforced immutability (PROPOSAL §6.1).
    pub sealed: bool,
}

/// A directory entry as returned by readdir.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirEntry {
    pub name: Vec<u8>,
    pub id: FileId,
    pub kind: FileKind,
}

/// Maximum length of one path component, matching Linux NAME_MAX.
pub const NAME_MAX: usize = 255;

/// Validate a single path component for create/link/rename. Names are
/// arbitrary bytes like on Linux; only `/`, NUL, `.` and `..` are refused.
pub fn validate_name(name: &[u8]) -> NestResult<()> {
    if name.is_empty() || name == b"." || name == b".." || name.contains(&b'/') || name.contains(&0)
    {
        return Err(NestError::Invalid(format!(
            "invalid name {:?}",
            String::from_utf8_lossy(name)
        )));
    }
    if name.len() > NAME_MAX {
        return Err(NestError::NameTooLong);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert!(validate_name(b"config.json").is_ok());
        assert!(validate_name(b"\xff\xfe").is_ok());
        assert!(validate_name(b"").is_err());
        assert!(validate_name(b"..").is_err());
        assert!(validate_name(b"a/b").is_err());
        assert!(matches!(
            validate_name(&[b'x'; 256]),
            Err(NestError::NameTooLong)
        ));
    }

    #[test]
    fn enum_roundtrip() {
        for k in [FileKind::Regular, FileKind::Directory, FileKind::Symlink] {
            assert_eq!(FileKind::from_i64(k.as_i64()), Some(k));
        }
        for s in [GenState::Stable, GenState::Owned] {
            assert_eq!(GenState::from_i64(s.as_i64()), Some(s));
        }
    }

    #[test]
    fn timestamps() {
        let t = Timestamp(1_700_000_000_123_456_789);
        assert_eq!(Timestamp::from_system_time(t.as_system_time()), t);
        let n = Timestamp(-5_000_000_000);
        assert_eq!(Timestamp::from_system_time(n.as_system_time()), n);
    }
}

/// A cluster-wide tuning value, stored in the metadata and read live by
/// every host (ADR-056). Values are integers in the setting's unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Setting {
    /// How long a read keeps retrying through a leader change or a lost
    /// metadata quorum before it fails with EIO (milliseconds).
    ReadRecoveryWait,
    /// The host the leader hands leadership to once that host has been up,
    /// caught up and reachable for a while (ADR-057). A node id; 0 is none.
    PreferredLeader,
    /// Control traffic between fabric hosts on the LAN while it answers,
    /// the fabric the fallback (ADR-068). 1 on, 0 off.
    ControlOnLan,
    /// Push grants of a rank each owing source always holds, outside the
    /// host's incast budget (ADR-063). At least 1: below it, two ranks on
    /// one host can deadlock.
    PushGrantFloor,
    /// Push bytes a host lets sources have in flight toward its rings
    /// (MiB); 0 is the fabric's read budget.
    PushIncastMib,
}

impl Setting {
    pub const ALL: &'static [Setting] = &[
        Setting::ReadRecoveryWait,
        Setting::PreferredLeader,
        Setting::ControlOnLan,
        Setting::PushGrantFloor,
        Setting::PushIncastMib,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Setting::ReadRecoveryWait => "read-recovery-wait",
            Setting::PreferredLeader => "preferred-leader",
            Setting::ControlOnLan => "control-on-lan",
            Setting::PushGrantFloor => "push-grant-floor",
            Setting::PushIncastMib => "push-incast-mib",
        }
    }

    pub fn parse(name: &str) -> Option<Setting> {
        Self::ALL.iter().copied().find(|s| s.name() == name)
    }

    pub fn default_value(self) -> u64 {
        match self {
            Setting::ReadRecoveryWait => 5_000,
            Setting::PreferredLeader => 0,
            Setting::ControlOnLan => 1,
            Setting::PushGrantFloor => 1,
            Setting::PushIncastMib => 0,
        }
    }

    pub fn unit(self) -> &'static str {
        match self {
            Setting::ReadRecoveryWait => "ms",
            Setting::PreferredLeader => "node",
            Setting::ControlOnLan => "0/1",
            Setting::PushGrantFloor => "grants",
            Setting::PushIncastMib => "MiB",
        }
    }

    pub fn about(self) -> &'static str {
        match self {
            Setting::ReadRecoveryWait => {
                "how long reads retry through a leader change or lost quorum before EIO"
            }
            Setting::PreferredLeader => {
                "host the metadata leader moves to once it is settled (0: none)"
            }
            Setting::ControlOnLan => {
                "control traffic on the 10G LAN while it answers, the fabric the fallback (0: fabric first)"
            }
            Setting::PushGrantFloor => {
                "push grants per rank each sending source always holds, outside the incast budget"
            }
            Setting::PushIncastMib => {
                "push bytes in flight toward one host's rings (0: the fabric's read budget)"
            }
        }
    }

    /// Accepted range, so a typo cannot stall every read for an hour.
    pub fn range(self) -> (u64, u64) {
        match self {
            Setting::ReadRecoveryWait => (0, 120_000),
            Setting::PreferredLeader => (0, u32::MAX as u64),
            Setting::ControlOnLan => (0, 1),
            Setting::PushGrantFloor => (1, 64),
            Setting::PushIncastMib => (0, 4096),
        }
    }

    /// The value is a host (its node id): `nest config set` also takes the
    /// host's name.
    pub fn is_host(self) -> bool {
        matches!(self, Setting::PreferredLeader)
    }
}

/// Round trips of one kind of Raft request to one peer, as this host's Raft
/// network measured them (ADR-057). Times in microseconds.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RttStats {
    /// Requests that got an answer (including an error answer).
    pub ok: u64,
    /// Smoothed round trip (EWMA, weight 1/8 per sample).
    pub ewma_us: u64,
    pub last_us: u64,
    /// Slowest answer in the last 10 to 20 seconds.
    pub max_10s_us: u64,
    /// No answer within the request's budget.
    pub timeouts: u64,
    /// Not sent: no connection to the peer.
    pub unreachable: u64,
    /// Answered with an error (e.g. a peer that cannot decode the request).
    pub errors: u64,
}

/// One Raft peer as this host sees it (ADR-057). Replication progress is
/// known only on the leader.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerRaftStats {
    pub peer: NodeId,
    pub voter: bool,
    /// Empty append-entries (openraft's heartbeats).
    pub heartbeat: RttStats,
    /// Append-entries carrying entries.
    pub append: RttStats,
    /// Votes and pre-votes.
    pub vote: RttStats,
    /// Snapshot chunks.
    pub snapshot: RttStats,
    /// Leadership handoffs sent (ADR-057).
    pub transfer: RttStats,
    /// Leader only: the last log index known to be on the peer.
    pub matched: Option<u64>,
    /// Leader only: how many entries the peer is behind the leader's log.
    pub lag: Option<u64>,
    /// Leader only: milliseconds since the peer last acknowledged.
    pub last_ack_ms: Option<u64>,
}

/// How this host reaches one peer, for `nest net` (ADR-057).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerNet {
    pub peer: NodeId,
    /// Control requests use the LAN standby connection right now.
    pub control_on_standby: bool,
    /// Seconds since control last moved (or the standby was set up).
    pub control_since_s: u64,
    pub failovers: u64,
    pub returns: u64,
    /// The standby connection answers its probes.
    pub standby_up: bool,
    pub standby_rtt_us: Option<u64>,
    pub standby_addr: Option<String>,
    /// Why control last moved.
    pub reason: Option<String>,
    /// An RDMA link is up, and its protocol (3 also carries writes).
    pub rdma_link: bool,
    pub rdma_protocol: Option<u32>,
    /// Link setup failed lately: reads use the TCP lane meanwhile.
    pub rdma_backing_off: bool,
    /// TCP lanes open to the peer, and whether they carry writes.
    pub lanes: u32,
    pub lanes_writable: bool,
    /// Raft round trips to the peer as this host measures them.
    pub raft: Option<PeerRaftStats>,
}

/// One host's view of the network (`AdminReq::Net`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetReport {
    pub node: NodeId,
    pub leader: Option<NodeId>,
    pub term: u64,
    pub peers: Vec<PeerNet>,
    /// The NIC's loss and congestion counters per RDMA port, cumulative
    /// since the driver loaded, as `name=value` pairs (absent ones left out).
    pub nic: Vec<(String, Vec<(String, u64)>)>,
}
