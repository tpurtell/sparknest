//! Control-plane RPC between sparknest nodes.
//!
//! One TCP connection per peer pair and direction carries any number of
//! concurrent requests, each tagged with a service number and a request id.
//! Either end may send requests on a connection: a peer this node has no
//! address for (an off-grid host behind NAT) is reached over the connection
//! it opened, and pinned peers are kept connected (ADR-047).
//! Connections are mutually authenticated with an HMAC challenge-response
//! over the shared cluster secret (ADR-012). Payloads are opaque bytes;
//! [`typed`] helpers encode serde types with postcard.
//!
//! This carries Raft traffic, write forwarding and data-service control
//! messages. Bulk file data uses `nest-fabric`.
//!
//! A peer may also have a standby address (its 10G LAN address): a second,
//! idle connection is kept there and the peer's requests move to it when
//! the primary connection fails (ADR-057, see `path`).

pub mod auth;
mod conn;
mod filter;
mod path;
pub mod sockopt;

pub use conn::{Handler, Rpc, RpcConfig};
pub use filter::Filter;
pub use path::{PathState, StandbyConfig};

use nest_types::NodeId;
use serde::{Serialize, de::DeserializeOwned};

/// Service numbers. Each node registers a handler per service.
pub mod service {
    pub const RAFT: u8 = 1;
    pub const META: u8 = 2;
    pub const DATA: u8 = 3;
    pub const ADMIN: u8 = 4;
    pub const FABRIC: u8 = 5;
    /// Versions and recovery coordination before Raft starts (ADR-026).
    pub const HELLO: u8 = 6;
    /// Push loads (ADR-063).
    pub const PUSH: u8 = 7;
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum RpcError {
    #[error("peer {0} unreachable: {1}")]
    Unreachable(NodeId, String),
    /// The connection closed with the request in flight (the peer went
    /// away, or its requests moved to another path, ADR-057). It may or may
    /// not have run there; nothing resends it here.
    #[error("request to {0} lost in flight: {1}")]
    Lost(NodeId, String),
    #[error("request to {0} timed out")]
    Timeout(NodeId),
    #[error("remote error: {0}")]
    Remote(String),
    #[error("no handler for service {0}")]
    NoService(u8),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("encoding error: {0}")]
    Codec(String),
}

impl RpcError {
    /// True when the request certainly did not execute on the peer.
    pub fn not_delivered(&self) -> bool {
        matches!(self, RpcError::Unreachable(..) | RpcError::NoService(_))
    }
}

pub fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>, RpcError> {
    postcard::to_stdvec(v).map_err(|e| RpcError::Codec(e.to_string()))
}

pub fn decode<T: DeserializeOwned>(b: &[u8]) -> Result<T, RpcError> {
    postcard::from_bytes(b).map_err(|e| RpcError::Codec(e.to_string()))
}

/// Typed call helpers.
pub mod typed {
    use super::*;
    use std::time::Duration;

    pub async fn call<Req: Serialize, Resp: DeserializeOwned>(
        rpc: &Rpc,
        peer: NodeId,
        service: u8,
        req: &Req,
        timeout: Duration,
    ) -> Result<Resp, RpcError> {
        let body = encode(req)?;
        let resp = rpc.call(peer, service, body.into(), timeout).await?;
        decode(&resp)
    }
}
