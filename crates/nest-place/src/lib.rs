//! Placement (PROPOSAL §7): what should be where, and making it so.
//!
//! - [`selector`]: selectors resolve to manifests of concrete files and
//!   generations. Path selectors follow symlinks inside the namespace, which
//!   is exactly how a Hugging Face snapshot reaches its blobs (repo-local or
//!   the shared blob store); HF selectors narrow a repo to one revision.
//! - [`admin`]: the per-node ADMIN service: replication jobs with progress,
//!   eviction, node information.
//! - [`placer`]: the coordinator used by the management API: rules,
//!   reconcile, ad-hoc replicate/evict, cluster status.
//!
//! Rules are the only authorization to create copies; reading never does.

pub mod admin;
pub mod backup;
pub mod import;
pub mod logs;
pub mod placer;
pub mod plan;
pub mod selector;
pub mod space;
pub mod spec;

pub use placer::Placer;
pub use spec::{RuleSpec, Selector};
