//! Data-plane RPC between nodes (service DATA over `nest-rpc`).
//!
//! In M3 bulk bytes travel in these messages over TCP; M4 moves ranged
//! reads onto the RDMA fabric behind the same calls.

use crate::vfs::Vfs;
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use nest_rpc::{Handler, RpcError, service};
use nest_types::{Epoch, FileId, Generation, NestError, NodeId, Timestamp};
use serde::{Deserialize, Serialize};
use std::sync::Weak;
use std::time::Duration;

/// A writer handle anywhere in the cluster.
pub type Writer = (NodeId, u64);

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum DataReq {
    /// Bytes of exactly this generation, or `Stale`.
    Read {
        file: FileId,
        generation: Generation,
        offset: u64,
        len: u32,
    },
    /// Owner-routed write in the given ownership epoch.
    Write {
        file: FileId,
        epoch: Epoch,
        writer: Writer,
        offset: u64,
        append: bool,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    },
    Truncate {
        file: FileId,
        epoch: Epoch,
        writer: Option<Writer>,
        size: u64,
    },
    /// A remote writer handle closed.
    Leave {
        file: FileId,
        epoch: Epoch,
        writer: Writer,
    },
    Fsync {
        file: FileId,
        epoch: Epoch,
    },
    /// Live size/mtime of a file this node owns.
    Stat {
        file: FileId,
    },
    /// Revocation: acknowledge once log `index` is applied here and no read
    /// of an older generation of `file` is still running.
    Fence {
        file: FileId,
        generation: Generation,
        index: u64,
    },
    Ping,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum DataResp {
    Data(#[serde(with = "serde_bytes")] Vec<u8>),
    Written(u32),
    Stat {
        generation: Generation,
        size: u64,
        mtime: Timestamp,
    },
    Done,
    Err(NestError),
}

pub(crate) struct DataService {
    pub(crate) vfs: Weak<Vfs>,
}

impl Handler for DataService {
    fn call(&self, peer: NodeId, body: Bytes) -> BoxFuture<'static, Result<Bytes, String>> {
        let vfs = self.vfs.clone();
        async move {
            let req: DataReq = nest_rpc::decode(&body).map_err(|e| e.to_string())?;
            let Some(vfs) = vfs.upgrade() else {
                return Err("shutting down".to_string());
            };
            let resp = vfs.serve(peer, req).await;
            nest_rpc::encode(&resp)
                .map(Bytes::from)
                .map_err(|e| e.to_string())
        }
        .boxed()
    }
}

/// Map transport failures onto filesystem errors.
pub(crate) fn rpc_err(e: RpcError) -> NestError {
    match e {
        RpcError::Unreachable(n, why) => {
            NestError::Unavailable(format!("node {n} unreachable: {why}"))
        }
        RpcError::Timeout(n) => NestError::Unavailable(format!("node {n} timed out")),
        other => NestError::Io(other.to_string()),
    }
}

pub(crate) async fn call(
    rpc: &nest_rpc::Rpc,
    peer: NodeId,
    req: &DataReq,
    timeout: Duration,
) -> Result<DataResp, NestError> {
    let body = nest_rpc::encode(req).map_err(|e| NestError::Io(e.to_string()))?;
    let resp = rpc
        .call(peer, service::DATA, body.into(), timeout)
        .await
        .map_err(rpc_err)?;
    match nest_rpc::decode::<DataResp>(&resp).map_err(|e| NestError::Io(e.to_string()))? {
        DataResp::Err(e) => Err(e),
        ok => Ok(ok),
    }
}
