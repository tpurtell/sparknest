use nest_types::NodeId;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::Arc;

/// Reachability control used by tests to simulate partitions. A blocked
/// peer is unreachable in both directions from this node's point of view:
/// outgoing calls fail immediately and incoming requests are dropped.
#[derive(Clone, Default)]
pub struct Filter {
    blocked: Arc<RwLock<HashSet<NodeId>>>,
}

impl Filter {
    pub fn block(&self, peer: NodeId) {
        self.blocked.write().insert(peer);
    }
    pub fn unblock(&self, peer: NodeId) {
        self.blocked.write().remove(&peer);
    }
    pub fn unblock_all(&self) {
        self.blocked.write().clear();
    }
    pub fn is_blocked(&self, peer: NodeId) -> bool {
        self.blocked.read().contains(&peer)
    }
}
