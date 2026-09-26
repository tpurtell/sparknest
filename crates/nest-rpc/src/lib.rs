//! Control-plane RPC between sparknest nodes.
//!
//! One TCP connection per peer pair and direction carries any number of
//! concurrent requests, each tagged with a service number and a request id.
//! Connections are mutually authenticated with an HMAC challenge-response
//! over the shared cluster secret (ADR-012). Payloads are opaque bytes;
//! [`typed`] helpers encode serde types with postcard.
//!
//! This carries Raft traffic, write forwarding and data-service control
//! messages. Bulk file data uses `nest-fabric`.

mod auth;
mod conn;
mod filter;

pub use conn::{Handler, Rpc, RpcConfig};
pub use filter::Filter;

use nest_types::NodeId;
use serde::{Serialize, de::DeserializeOwned};

/// Service numbers. Each node registers a handler per service.
pub mod service {
    pub const RAFT: u8 = 1;
    pub const META: u8 = 2;
    pub const DATA: u8 = 3;
    pub const ADMIN: u8 = 4;
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum RpcError {
    #[error("peer {0} unreachable: {1}")]
    Unreachable(NodeId, String),
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
