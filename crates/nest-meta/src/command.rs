use nest_types::{Epoch, FileAttr, FileId, Generation, NodeId, SessionId, StoreId, Timestamp};
use serde::{Deserialize, Serialize};

/// A semantic change to replicated metadata. Commands carry every
/// nondeterministic input (timestamps, the proposing node) so that apply is a
/// pure function of (state, command).
///
/// **On-disk format:** commands are stored in the Raft log with postcard,
/// which encodes enum variants by position and struct fields in order.
/// Never reorder, remove or change variants or fields of this type,
/// [`Reply`], or `NestError`: append new variants at the end only, and bump
/// `nest_meta::FORMAT_VERSION` for anything else (ADR-018). The
/// `format_is_pinned` test fails on accidental changes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    // ---- namespace ------------------------------------------------------
    Mkdir {
        parent: FileId,
        name: Vec<u8>,
        perm: u32,
        now: Timestamp,
    },
    /// Create a regular file. The new file is OWNED by `node` at generation 1
    /// (local-only creation). With `exclusive == false` an existing regular
    /// file is returned instead of failing.
    Create {
        parent: FileId,
        name: Vec<u8>,
        perm: u32,
        node: NodeId,
        exclusive: bool,
        now: Timestamp,
    },
    Symlink {
        parent: FileId,
        name: Vec<u8>,
        target: Vec<u8>,
        now: Timestamp,
    },
    Link {
        file: FileId,
        parent: FileId,
        name: Vec<u8>,
        now: Timestamp,
    },
    Unlink {
        parent: FileId,
        name: Vec<u8>,
        now: Timestamp,
    },
    Rmdir {
        parent: FileId,
        name: Vec<u8>,
        now: Timestamp,
    },
    Rename {
        parent: FileId,
        name: Vec<u8>,
        new_parent: FileId,
        new_name: Vec<u8>,
        flags: RenameFlags,
        now: Timestamp,
    },
    /// Metadata-only attribute changes. Size changes are content mutations
    /// and go through ownership ([`Command::AcquireOwner`] with truncate, or
    /// the owner's [`Command::SyncOwned`]/[`Command::Finalize`]).
    SetAttr {
        file: FileId,
        perm: Option<u32>,
        atime: Option<Timestamp>,
        mtime: Option<Timestamp>,
        now: Timestamp,
    },

    // ---- lifecycle ------------------------------------------------------
    /// First mutation of a STABLE file: make `node` the single owner of a new
    /// generation. Every other replica of the old generation is invalidated
    /// (and its holder starts deleting it on apply). `truncate` means the
    /// new content starts empty, so the owner need not hold the old content.
    AcquireOwner {
        file: FileId,
        node: NodeId,
        expect_gen: Generation,
        truncate: bool,
        now: Timestamp,
    },
    /// Owner publishes current size/mtime of an OWNED file (fsync, periodic).
    SyncOwned {
        file: FileId,
        epoch: Epoch,
        size: u64,
        mtime: Timestamp,
    },
    /// Owner ends the write epoch: the working object becomes the only LIVE
    /// replica of the now-STABLE generation.
    Finalize {
        file: FileId,
        epoch: Epoch,
        size: u64,
        mtime: Timestamp,
        now: Timestamp,
    },
    /// Set or clear enforced immutability. Sealing an OWNED file is recorded
    /// and takes effect at finalize.
    Seal {
        file: FileId,
        sealed: bool,
        now: Timestamp,
    },

    /// Set a directory's automatic sealing policy (inherited by the tree
    /// below it unless overridden).
    SetSealPolicy {
        dir: FileId,
        policy: SealPolicy,
        now: Timestamp,
    },

    // ---- replicas -------------------------------------------------------
    /// A complete copy of `gen` now exists in `store`.
    PublishReplica {
        file: FileId,
        generation: Generation,
        store: StoreId,
    },
    /// Remove one copy (eviction). Refused for the last LIVE copy of a
    /// STABLE generation unless `allow_last`.
    RetireReplica {
        file: FileId,
        generation: Generation,
        store: StoreId,
        allow_last: bool,
    },

    // ---- sessions and orphans -------------------------------------------
    /// A daemon incarnation starts. Previous sessions of the same node are
    /// expired: their handles died with the old process.
    OpenSession {
        node: NodeId,
        now: Timestamp,
    },
    RenewSession {
        session: SessionId,
        now: Timestamp,
    },
    ExpireSession {
        session: SessionId,
    },
    /// `session` no longer holds these unlinked files open.
    ReleaseOrphans {
        session: SessionId,
        files: Vec<FileId>,
    },

    // ---- advisory locks -------------------------------------------------
    /// Set, change or clear (`kind == Unlock`) a byte-range lock held by
    /// `(session, owner)`. Fails with `WouldBlock` on conflict; nothing
    /// changes then. Ranges are inclusive; `end == u64::MAX` means EOF.
    SetLock {
        file: FileId,
        session: SessionId,
        owner: u64,
        start: u64,
        end: u64,
        kind: LockKind,
        pid: u32,
    },
    /// Drop every lock `(session, owner)` holds on `file` (close/flush).
    ReleaseLocks {
        file: FileId,
        session: SessionId,
        owner: u64,
    },

    // ---- stores ---------------------------------------------------------
    RegisterStore {
        name: String,
        class: StoreClass,
        /// Set for a node's live store (whose id is the node id).
        node: Option<NodeId>,
        config: String,
    },

    // ---- import -----------------------------------------------------------
    /// Reserve `count` consecutive file ids (for import, where the object
    /// must exist under its final id before the entry is committed).
    ReserveFileIds {
        count: u64,
    },
    /// Create a STABLE regular file (generation 1) whose complete content
    /// already exists in `node`'s store under the reserved id `file`.
    Import {
        parent: FileId,
        name: Vec<u8>,
        file: FileId,
        perm: u32,
        size: u64,
        mtime: Timestamp,
        node: NodeId,
        sealed: bool,
        now: Timestamp,
    },

    // ---- placement rules --------------------------------------------------
    /// Create or replace a rule. With `expect_revision`, fails with `Stale`
    /// unless the current revision matches (0 = must not exist).
    SetRule {
        name: String,
        spec: String,
        expect_revision: Option<u64>,
    },
    DeleteRule {
        name: String,
    },

    /// Apply several commands in one log entry. Each sub-command succeeds or
    /// fails on its own.
    Batch(Vec<Command>),

    // ---- appended after format 1 (ADR-018: append only) -------------------
    /// Catalog a completed backup whose manifest and objects are in `store`.
    RecordBackup {
        name: String,
        store: StoreId,
        selector: String,
        files: u64,
        bytes: u64,
        now: Timestamp,
    },
    DeleteBackup {
        id: u64,
    },
    /// Define (or redefine) a host group; `members` are host names.
    SetGroup {
        name: String,
        members: Vec<String>,
    },
    DeleteGroup {
        name: String,
    },
    /// Record how a regular file is read (ADR-031): `scattered` when readers
    /// take small pieces at random, so hosts read it directly instead of
    /// reading ahead. A hint learned by whichever host notices first.
    SetReadPattern {
        file: FileId,
        scattered: bool,
    },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenameFlags {
    pub noreplace: bool,
    pub exchange: bool,
}

/// When files in a tree become sealed automatically.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SealPolicy {
    /// Use the parent directory's policy (the default).
    Inherit,
    /// Never seal automatically.
    Off,
    /// Seal a regular file when it is renamed from `*.incomplete` to a name
    /// without that suffix: how huggingface_hub completes a download.
    RenameFromIncomplete,
    /// Seal every regular file when its write epoch finalizes.
    OnFinalize,
}

impl SealPolicy {
    pub fn as_bits(self) -> i64 {
        match self {
            SealPolicy::Inherit => 0,
            SealPolicy::Off => 1,
            SealPolicy::RenameFromIncomplete => 2,
            SealPolicy::OnFinalize => 3,
        }
    }
    pub fn from_bits(v: i64) -> SealPolicy {
        match v & 3 {
            1 => SealPolicy::Off,
            2 => SealPolicy::RenameFromIncomplete,
            3 => SealPolicy::OnFinalize,
            _ => SealPolicy::Inherit,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LockKind {
    Read,
    Write,
    Unlock,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreClass {
    Live,
    Archive,
}

impl StoreClass {
    pub fn as_i64(self) -> i64 {
        match self {
            StoreClass::Live => 0,
            StoreClass::Archive => 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateReply {
    pub attr: FileAttr,
    pub created: bool,
}

/// Successful outcome of a command.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reply {
    Done,
    Attr(FileAttr),
    Created(CreateReply),
    /// Ownership was granted to the proposer.
    Acquired {
        attr: FileAttr,
        /// The owner already holds the previous generation's content and
        /// converts it in place (`None`: start from an empty object).
        from_gen: Option<Generation>,
    },
    /// The file is already owned; route to that owner instead.
    AlreadyOwned {
        owner: NodeId,
        generation: Generation,
        epoch: Epoch,
    },
    Session(SessionId),
    Store(StoreId),
    Revision(u64),
    /// First of a reserved block of file ids.
    FileIds(FileId),
    Batch(Vec<Result<Reply, nest_types::NestError>>),
    /// Id of a newly cataloged backup (appended after format 1).
    Backup(u64),
}
