use nest_types::{Epoch, FileAttr, FileId, Generation, NodeId, SessionId, StoreId, Timestamp};
use serde::{Deserialize, Serialize};

/// A semantic change to replicated metadata. Commands carry every
/// nondeterministic input (timestamps, the proposing node) so that apply is a
/// pure function of (state, command).
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

    // ---- stores ---------------------------------------------------------
    RegisterStore {
        name: String,
        class: StoreClass,
        /// Set for a node's live store (whose id is the node id).
        node: Option<NodeId>,
        config: String,
    },

    /// Apply several commands in one log entry. Each sub-command succeeds or
    /// fails on its own.
    Batch(Vec<Command>),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenameFlags {
    pub noreplace: bool,
    pub exchange: bool,
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
    Batch(Vec<Result<Reply, nest_types::NestError>>),
}
