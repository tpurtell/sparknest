use nest_types::{Epoch, FileId, Generation, NodeId, SessionId, StoreId};
use serde::{Deserialize, Serialize};

/// A consequence of an applied command that node-local services act on.
/// Every node computes the same effects; each acts on those that concern it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Effect {
    /// This copy is no longer valid. Its holder must stop serving it and
    /// start deleting it immediately (PROPOSAL §4, invariant 4).
    ReplicaInvalidated {
        file: FileId,
        generation: Generation,
        store: StoreId,
    },
    /// `owner` now owns `gen` of `file`. Every node fences reads of older
    /// generations and acknowledges to the owner; the owner prepares its
    /// working object (converting `from_gen` in place, or creating it empty).
    OwnershipGranted {
        file: FileId,
        owner: NodeId,
        generation: Generation,
        epoch: Epoch,
        from_gen: Option<Generation>,
    },
    /// The owner's working object for an OWNED generation is no longer
    /// needed (the file was deleted while owned).
    WorkingObjectDeleted {
        file: FileId,
        generation: Generation,
        owner: NodeId,
    },
    /// Write epoch ended; `gen` is STABLE with one LIVE replica on `owner`.
    Finalized {
        file: FileId,
        generation: Generation,
        owner: NodeId,
    },
    /// The file lost its last name but may be open somewhere. Each listed
    /// session releases it once it holds no handle to it.
    Orphaned {
        file: FileId,
        sessions: Vec<SessionId>,
    },
    /// The file object no longer exists.
    FileDeleted {
        file: FileId,
    },
    /// Kernel dentry cache for (parent, name) is stale on every node.
    EntryChanged {
        parent: FileId,
        name: Vec<u8>,
    },
    /// Kernel attribute cache for the file is stale on every node.
    AttrChanged {
        file: FileId,
    },
    /// Locks on `file` were released; blocked lockers should retry.
    LocksReleased {
        file: FileId,
    },
    SessionOpened {
        session: SessionId,
        node: NodeId,
    },
    SessionExpired {
        session: SessionId,
        node: NodeId,
    },
}
