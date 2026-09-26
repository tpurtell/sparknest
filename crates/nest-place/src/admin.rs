//! Per-node ADMIN service: work that must run on a particular node.

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use nest_data::Vfs;
use nest_rpc::{Handler, Rpc, service};
use nest_types::{FileId, NestError, NodeId};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node: NodeId,
    pub name: String,
    pub mountpoint: Option<String>,
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub objects: u64,
    pub object_bytes: u64,
    pub rails: Vec<String>,
    pub serving: bool,
    pub leader: Option<NodeId>,
    pub applied: u64,
    pub version: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobProgress {
    pub total_files: u64,
    pub done_files: u64,
    pub total_bytes: u64,
    pub done_bytes: u64,
    pub failed: Vec<(FileId, String)>,
    pub finished: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum AdminReq {
    Info,
    /// Replicate these files here: (file, expected size) pairs.
    StartReplicate {
        job: u64,
        files: Vec<(FileId, u64)>,
        parallel: usize,
    },
    JobStatus {
        job: u64,
    },
    Evict {
        files: Vec<FileId>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum AdminResp {
    Info(NodeInfo),
    Started,
    Job(JobProgress),
    Evicted {
        removed: u64,
        refused: Vec<(FileId, String)>,
    },
    Err(String),
}

pub struct Admin {
    vfs: Arc<Vfs>,
    name: String,
    mountpoint: Option<String>,
    jobs: Mutex<HashMap<u64, Arc<Mutex<JobProgress>>>>,
}

impl Admin {
    pub fn register(
        rpc: &Rpc,
        vfs: Arc<Vfs>,
        name: String,
        mountpoint: Option<String>,
    ) -> Arc<Admin> {
        let a = Arc::new(Admin {
            vfs,
            name,
            mountpoint,
            jobs: Mutex::new(HashMap::new()),
        });
        rpc.register(service::ADMIN, Arc::new(AdminService(Arc::downgrade(&a))));
        a
    }

    pub fn info(&self) -> NodeInfo {
        let d = self.vfs.data();
        let (cap, _) = self
            .vfs
            .statfs()
            .map(|(c, n)| (Some(c), n))
            .unwrap_or((None, 0));
        let objs = d.store().scan().unwrap_or_default();
        NodeInfo {
            node: d.id(),
            name: self.name.clone(),
            mountpoint: self.mountpoint.clone(),
            total_bytes: cap.map(|c| c.total).unwrap_or(0),
            free_bytes: cap.map(|c| c.free).unwrap_or(0),
            objects: objs.len() as u64,
            object_bytes: objs.iter().map(|o| o.size).sum(),
            rails: self
                .vfs
                .fabric()
                .map(|f| {
                    f.rails()
                        .iter()
                        .map(|r| format!("{}/{}", r.ibdev, r.addr))
                        .collect()
                })
                .unwrap_or_default(),
            serving: d.lease_valid(),
            leader: d.meta().leader(),
            applied: d.meta().applied_index(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    fn start_replicate(self: &Arc<Self>, job: u64, files: Vec<(FileId, u64)>, parallel: usize) {
        let progress = Arc::new(Mutex::new(JobProgress {
            total_files: files.len() as u64,
            total_bytes: files.iter().map(|(_, s)| s).sum(),
            ..Default::default()
        }));
        {
            let mut jobs = self.jobs.lock();
            if jobs.len() > 256 {
                jobs.retain(|_, p| !p.lock().finished);
            }
            jobs.insert(job, progress.clone());
        }
        let vfs = self.vfs.clone();
        tokio::spawn(async move {
            futures::stream::iter(files)
                .map(|(f, size)| {
                    let vfs = vfs.clone();
                    async move { (f, size, vfs.replicate_here(f).await) }
                })
                .buffer_unordered(parallel.max(1))
                .for_each(|(f, size, r)| {
                    let mut p = progress.lock();
                    p.done_files += 1;
                    p.done_bytes += size;
                    if let Err(e) = r {
                        p.failed.push((f, e.to_string()));
                    }
                    futures::future::ready(())
                })
                .await;
            progress.lock().finished = true;
        });
    }

    async fn evict(&self, files: Vec<FileId>) -> AdminResp {
        let mut removed = 0;
        let mut refused = Vec::new();
        for f in files {
            match self.vfs.evict_here(f).await {
                Ok(true) => removed += 1,
                Ok(false) => {}
                Err(e) => refused.push((f, e.to_string())),
            }
        }
        AdminResp::Evicted { removed, refused }
    }
}

struct AdminService(std::sync::Weak<Admin>);

impl Handler for AdminService {
    fn call(&self, _peer: NodeId, body: Bytes) -> BoxFuture<'static, Result<Bytes, String>> {
        let a = self.0.clone();
        async move {
            let a = a.upgrade().ok_or("shutting down")?;
            let req: AdminReq = nest_rpc::decode(&body).map_err(|e| e.to_string())?;
            let resp = match req {
                AdminReq::Info => AdminResp::Info(a.info()),
                AdminReq::StartReplicate {
                    job,
                    files,
                    parallel,
                } => {
                    a.start_replicate(job, files, parallel);
                    AdminResp::Started
                }
                AdminReq::JobStatus { job } => match a.jobs.lock().get(&job) {
                    Some(p) => AdminResp::Job(p.lock().clone()),
                    None => AdminResp::Err(format!("no job {job}")),
                },
                AdminReq::Evict { files } => a.evict(files).await,
            };
            nest_rpc::encode(&resp)
                .map(Bytes::from)
                .map_err(|e| e.to_string())
        }
        .boxed()
    }
}

pub(crate) async fn call(
    rpc: &Rpc,
    node: NodeId,
    req: &AdminReq,
    timeout: Duration,
) -> Result<AdminResp, NestError> {
    let body = nest_rpc::encode(req).map_err(|e| NestError::Io(e.to_string()))?;
    let b = rpc
        .call(node, service::ADMIN, body.into(), timeout)
        .await
        .map_err(|e| NestError::Unavailable(e.to_string()))?;
    match nest_rpc::decode::<AdminResp>(&b).map_err(|e| NestError::Io(e.to_string()))? {
        AdminResp::Err(e) => Err(NestError::Io(e)),
        r => Ok(r),
    }
}
