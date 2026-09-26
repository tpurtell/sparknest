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
