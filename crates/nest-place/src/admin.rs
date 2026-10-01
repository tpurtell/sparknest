//! Per-node ADMIN service: work that must run on a particular node.

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use nest_data::Vfs;
use nest_rpc::{Handler, Rpc, service};
use nest_types::{FileId, NestError, NodeId, StoreId};
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
    /// Cumulative bytes this host read from others / served to others over
    /// the fabric (rates come from differences between polls).
    #[serde(default)]
    pub fabric_read_bytes: u64,
    #[serde(default)]
    pub fabric_served_bytes: u64,
    /// Of the reads served, those answered on the completion thread.
    #[serde(default)]
    pub fabric_served_fast: u64,
    /// Of the reads made, those made on the requesting FUSE thread.
    #[serde(default)]
    pub fabric_read_now: u64,
    /// Bytes this host read from its own disk through sparknest (tracked
    /// reads; rates come from differences between polls).
    #[serde(default)]
    pub local_read_bytes: u64,
    /// Files this host reads directly because reads are scattered (ADR-031).
    #[serde(default)]
    pub scattered_files: u64,
    /// Readahead chunks this host fetched and dropped, and bytes of them
    /// readers used: the rest was wasted reading (ADR-031).
    #[serde(default)]
    pub readahead_dropped_bytes: u64,
    #[serde(default)]
    pub readahead_used_bytes: u64,
    /// Read latencies by kind and size, and readahead waste, over the last
    /// 10 s, 1 min and 10 min.
    #[serde(default)]
    pub io_windows: Vec<nest_data::iostats::Window>,
    /// What this host has learned about the sources it reads from
    /// (ADR-030): latency, in flight, recent rate.
    #[serde(default)]
    pub io: Vec<nest_data::balance::SourceReport>,
    /// Measured disk read rate and RDMA link rate, bytes/s (0: unknown).
    #[serde(default)]
    pub disk_read_bps: u64,
    #[serde(default)]
    pub link_bps: u64,
    /// Fabric timing totals (ns) and counts, for diagnosis: reads issued
    /// here (slot wait, window wait, round trip) and served here (slot
    /// wait, read).
    #[serde(default)]
    pub fabric_timing: [u64; 7],
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreHealth {
    pub healthy: bool,
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub objects: u64,
    pub object_bytes: u64,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobProgress {
    pub total_files: u64,
    pub done_files: u64,
    pub total_bytes: u64,
    pub done_bytes: u64,
    pub failed: Vec<(FileId, String)>,
    pub finished: bool,
    /// Cancelled: files not yet started are skipped (counted in neither
    /// done nor failed); files already copying finish.
    #[serde(default)]
    pub cancelled: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum AdminReq {
    Info,
    /// Replicate these files here: (file, expected size) pairs.
    StartReplicate {
        job: u64,
        files: Vec<(FileId, u64)>,
        parallel: usize,
        /// Store to copy into (this node's live store or an archive it
        /// gateways).
        target: StoreId,
    },
    JobStatus {
        job: u64,
    },
    Evict {
        files: Vec<FileId>,
        store: StoreId,
    },
    /// Prepare an archive store root on this gateway (writes the marker).
    InitStore {
        store: StoreId,
        name: String,
        path: String,
    },
    /// Health and capacity of an archive store as seen from this gateway.
    StoreHealth {
        store: StoreId,
    },
    /// Run on the leader: change Raft membership.
    Membership(MembershipChange),
    /// Run on a gateway of `store`: back up the tree at `root`.
    BackupCreate {
        job: u64,
        name: String,
        root: String,
        store: StoreId,
    },
    BackupRestore {
        job: u64,
        id: u64,
        store: StoreId,
        dst: String,
    },
    BackupDelete {
        id: u64,
        store: StoreId,
    },
    MetaSnapshot {
        store: StoreId,
    },
    /// Evict exact generations (plan steps); moved files are refused.
    EvictExact {
        files: Vec<(FileId, nest_types::Generation)>,
        store: StoreId,
    },
    /// Recent log lines kept in memory on this node.
    Logs(crate::logs::LogQuery),
    /// Stop starting new files for this node-side job.
    CancelJob {
        job: u64,
    },
    /// This host's per-file usage since a time (Unix ms).
    Usage {
        since_ms: u64,
    },
    /// Drop this host's clean page cache (benchmarks), through the fixed
    /// setuid helper; nothing about it comes from the request.
    DropCaches,
    /// The jobs people started on this node (imports, replicates, plans),
    /// so any node's pages can list them.
    Jobs,
    /// Cancel one of this node's own jobs (see `Jobs`).
    CancelOwnJob {
        job: u64,
    },
    /// Download a Hugging Face repo to this node (`Placer::hf_download`),
    /// through its mount's `hub`.
    HfDownload {
        repo: String,
        kind: String,
        revision: Option<String>,
        hub: String,
        hf: Option<String>,
    },
    /// Fetch these files (name, size) of a Hugging Face repo at commit
    /// `sha` into this node's hub: its share of a download
    /// (`Placer::hf_fetch_here`). Answers `JobStarted` with a fetch id.
    HfFetch {
        repo: String,
        kind: String,
        sha: String,
        files: Vec<(String, u64)>,
        hub: String,
    },
    /// Progress of an `HfFetch`; `cancel` stops it.
    HfFetchStatus {
        id: u64,
        cancel: bool,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MembershipChange {
    /// Add (or re-add) a node, as voter or learner.
    Add {
        node: NodeId,
        addr: String,
        voter: bool,
    },
    /// Remove a node entirely (it stops voting and receiving the log).
    Remove { node: NodeId },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum AdminResp {
    Info(Box<NodeInfo>),
    Health(StoreHealth),
    Started,
    Job(JobProgress),
    Evicted {
        removed: u64,
        refused: Vec<(FileId, String)>,
    },
    Removed(u64),
    Path(String),
    Err(String),
    Logs(Vec<crate::logs::LogLine>),
    Usage(Vec<nest_data::usage::FileUsage>),
    Dropped(Result<String, String>),
    Jobs(Vec<crate::placer::ClusterJob>),
    JobStarted(u64),
    HfFetch(crate::hfimport::FetchProgress),
}

/// Installed by `sparknest-drop-page-cache --install` (root, setuid, mode
/// 4750, the installing user's group). A fixed path: the daemon never runs
/// anything a request names.
pub const DROP_CACHES_HELPER: &str = "/usr/local/libexec/sparknest/drop-page-cache";

/// Sync and drop this host's clean page cache via the helper.
pub async fn drop_caches_here() -> Result<String, String> {
    let out = tokio::time::timeout(
        Duration::from_secs(120),
        tokio::process::Command::new(DROP_CACHES_HELPER)
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| "timed out after 120 s".to_string())?
    .map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => {
            "helper not installed here: run `sparknest-drop-page-cache --install` on this host"
                .to_string()
        }
        _ => format!("{DROP_CACHES_HELPER}: {e}"),
    })?;
    let text = |b: &[u8]| String::from_utf8_lossy(b).trim().to_string();
    if out.status.success() {
        Ok(text(&out.stdout))
    } else {
        Err(match out.status.code() {
            Some(77) => "helper not installed setuid here: run `sparknest-drop-page-cache --install` on this host".into(),
            _ => text(&out.stderr),
        })
    }
}

pub struct Admin {
    pub(crate) vfs: Arc<Vfs>,
    pub(crate) name: String,
    pub(crate) mountpoint: Option<String>,
    jobs: Mutex<HashMap<u64, Arc<Mutex<JobProgress>>>>,
    /// The node's placer, for the requests about its own jobs.
    pub(crate) placer: std::sync::OnceLock<std::sync::Weak<crate::placer::Placer>>,
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
            placer: std::sync::OnceLock::new(),
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
        // From the metadata, not a directory scan: the UI polls this.
        let (objects, object_bytes) = d
            .meta()
            .open_reader()
            .ok()
            .and_then(|c| nest_meta::query::store_usage(&c, d.id().live_store()).ok())
            .unwrap_or((0, 0));
        let fab = self.vfs.fabric();
        let (fabric_read_bytes, fabric_served_bytes) = fab
            .as_ref()
            .map(|f| {
                (
                    f.stats
                        .read_bytes
                        .load(std::sync::atomic::Ordering::Relaxed),
                    f.stats
                        .served_bytes
                        .load(std::sync::atomic::Ordering::Relaxed),
                )
            })
            .unwrap_or((0, 0));
        NodeInfo {
            node: d.id(),
            name: self.name.clone(),
            mountpoint: self.mountpoint.clone(),
            total_bytes: cap.map(|c| c.total).unwrap_or(0),
            free_bytes: cap.map(|c| c.free).unwrap_or(0),
            objects,
            object_bytes,
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
            fabric_read_bytes,
            fabric_served_bytes,
            fabric_served_fast: fab.as_ref().map_or(0, |f| {
                f.stats
                    .served_fast
                    .load(std::sync::atomic::Ordering::Relaxed)
            }),
            fabric_read_now: fab.as_ref().map_or(0, |f| {
                f.stats.read_now.load(std::sync::atomic::Ordering::Relaxed)
            }),
            local_read_bytes: self.vfs.local_read_bytes(),
            scattered_files: self.vfs.scattered_files() as u64,
            io_windows: self.vfs.io_windows(),
            readahead_dropped_bytes: self.vfs.readahead_totals().0,
            readahead_used_bytes: self.vfs.readahead_totals().1,
            io: self.vfs.io_report(),
            disk_read_bps: self.vfs.disk_read_bps(),
            fabric_timing: fab
                .as_ref()
                .map(|f| {
                    let s = &f.stats;
                    let g = |a: &std::sync::atomic::AtomicU64| {
                        a.load(std::sync::atomic::Ordering::Relaxed)
                    };
                    [
                        g(&s.reads),
                        g(&s.read_slot_wait_ns),
                        g(&s.read_window_wait_ns),
                        g(&s.read_rtt_ns),
                        g(&s.served),
                        g(&s.serve_slot_wait_ns),
                        g(&s.serve_read_ns),
                    ]
                })
                .unwrap_or_default(),
            link_bps: self.vfs.link_bps(),
        }
    }

    fn start_replicate(
        self: &Arc<Self>,
        job: u64,
        files: Vec<(FileId, u64)>,
        parallel: usize,
        target: StoreId,
    ) {
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
                    let progress = progress.clone();
                    async move {
                        if progress.lock().cancelled {
                            return (f, size, None);
                        }
                        (f, size, Some(vfs.replicate_into(f, target).await))
                    }
                })
                .buffer_unordered(parallel.max(1))
                .for_each(|(f, size, r)| {
                    let Some(r) = r else {
                        return futures::future::ready(());
                    };
                    let mut p = progress.lock();
                    p.done_files += 1;
                    p.done_bytes += size;
                    if let Err(e) = r {
                        let path = vfs
                            .data()
                            .meta()
                            .open_reader()
                            .ok()
                            .and_then(|c| nest_meta::query::path_of(&c, f).ok().flatten())
                            .map(|p| String::from_utf8_lossy(&p).into_owned())
                            .unwrap_or_default();
                        tracing::warn!(job, file = f.0, %path, error = %e, "replicate: file not copied");
                        p.failed.push((f, format!("{path}: {e}")));
                    }
                    futures::future::ready(())
                })
                .await;
            progress.lock().finished = true;
        });
    }

    fn area(
        &self,
        store: StoreId,
    ) -> Result<(Arc<nest_data::Archive>, Arc<crate::backup::BackupArea>), String> {
        let d = self.vfs.data();
        let arch = d
            .archive(store)
            .ok_or("this node cannot reach that store (not a gateway, or unmounted)")?;
        let area = crate::backup::BackupArea::open(std::path::Path::new(&arch.config.path), d.id())
            .map_err(|e| e.to_string())?;
        Ok((arch, Arc::new(area)))
    }

    fn track_job(&self, job: u64) -> Arc<Mutex<JobProgress>> {
        let p = Arc::new(Mutex::new(JobProgress::default()));
        self.jobs.lock().insert(job, p.clone());
        p
    }

    async fn evict(&self, files: Vec<FileId>, store: StoreId) -> AdminResp {
        let mut removed = 0;
        let mut refused = Vec::new();
        for f in files {
            match self.vfs.evict_from(f, store).await {
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
                AdminReq::Info => AdminResp::Info(Box::new(a.info())),
                AdminReq::StartReplicate {
                    job,
                    files,
                    parallel,
                    target,
                } => {
                    a.start_replicate(job, files, parallel, target);
                    AdminResp::Started
                }
                AdminReq::InitStore { store, name, path } => {
                    let d = a.vfs.data();
                    let identity = d.store_identity(store, &name);
                    match nest_store::init_marker(std::path::Path::new(&path), &identity) {
                        Ok(()) => match d.archive(store) {
                            Some(_) => AdminResp::Started,
                            None => AdminResp::Err("store is not usable from this gateway".into()),
                        },
                        Err(e) => AdminResp::Err(e.to_string()),
                    }
                }
                AdminReq::StoreHealth { store } => {
                    let d = a.vfs.data().clone();
                    // Marker and free space only: counting objects comes from
                    // the metadata (listing an archive over SMB takes seconds).
                    let usage = d
                        .meta()
                        .open_reader()
                        .ok()
                        .and_then(|c| nest_meta::query::store_usage(&c, store).ok())
                        .unwrap_or((0, 0));
                    let health = tokio::task::spawn_blocking(move || match d.archive(store) {
                        Some(arch) => {
                            let cap = arch.store.capacity().ok();
                            StoreHealth {
                                healthy: arch.healthy_now(),
                                total_bytes: cap.map(|c| c.total).unwrap_or(0),
                                free_bytes: cap.map(|c| c.free).unwrap_or(0),
                                objects: usage.0,
                                object_bytes: usage.1,
                                error: None,
                            }
                        }
                        None => StoreHealth {
                            error: Some("marker missing or path unreachable (unmounted?)".into()),
                            ..Default::default()
                        },
                    })
                    .await
                    .unwrap_or_default();
                    AdminResp::Health(health)
                }
                AdminReq::JobStatus { job } => match a.jobs.lock().get(&job) {
                    Some(p) => AdminResp::Job(p.lock().clone()),
                    None => AdminResp::Err(format!("no job {job}")),
                },
                AdminReq::Evict { files, store } => a.evict(files, store).await,
                AdminReq::Logs(q) => AdminResp::Logs(crate::logs::recent(&q)),
                AdminReq::DropCaches => AdminResp::Dropped(drop_caches_here().await),
                AdminReq::Jobs => match a.placer.get().and_then(|p| p.upgrade()) {
                    Some(p) => AdminResp::Jobs(p.jobs()),
                    None => AdminResp::Jobs(Vec::new()),
                },
                AdminReq::HfDownload {
                    repo,
                    kind,
                    revision,
                    hub,
                    hf,
                } => {
                    let placer = a.placer.get().and_then(|p| p.upgrade());
                    let mount_hub = a
                        .mountpoint
                        .as_ref()
                        .map(|m| std::path::Path::new(m).join(hub.trim_start_matches('/')));
                    let hf = hf
                        .map(std::path::PathBuf::from)
                        .filter(|p| p.is_file())
                        .or_else(crate::hfimport::find_hf);
                    match (placer, mount_hub, hf) {
                        (Some(p), Some(m), Some(hf)) => {
                            AdminResp::JobStarted(p.hf_download(hf, hub, m, repo, kind, revision))
                        }
                        (_, None, _) => AdminResp::Err("this host has no mount".into()),
                        (_, _, None) => AdminResp::Err("hf is not installed on this host".into()),
                        _ => AdminResp::Err("no placer on this node".into()),
                    }
                }
                AdminReq::HfFetch {
                    repo,
                    kind,
                    sha,
                    files,
                    hub,
                } => match a.placer.get().and_then(|p| p.upgrade()) {
                    Some(p) => match p.hf_fetch_here(&hub, repo, kind, sha, files) {
                        Ok(id) => AdminResp::JobStarted(id),
                        Err(e) => AdminResp::Err(e.to_string()),
                    },
                    None => AdminResp::Err("no placer on this node".into()),
                },
                AdminReq::HfFetchStatus { id, cancel } => {
                    match a.placer.get().and_then(|p| p.upgrade()) {
                        Some(p) => match p.hf_fetch_status_here(id, cancel) {
                            Ok(f) => AdminResp::HfFetch(f),
                            Err(e) => AdminResp::Err(e.to_string()),
                        },
                        None => AdminResp::Err("no placer on this node".into()),
                    }
                }
                AdminReq::CancelOwnJob { job } => match a.placer.get().and_then(|p| p.upgrade()) {
                    Some(p) => match p.cancel_own_job(job).await {
                        Ok(()) => AdminResp::Started,
                        Err(e) => AdminResp::Err(e.to_string()),
                    },
                    None => AdminResp::Err("no placer on this node".into()),
                },
                AdminReq::Usage { since_ms } => {
                    let u = a.vfs.usage().clone();
                    match tokio::task::spawn_blocking(move || u.summary(since_ms)).await {
                        Ok(Ok(v)) => AdminResp::Usage(v),
                        Ok(Err(e)) => AdminResp::Err(e.to_string()),
                        Err(e) => AdminResp::Err(e.to_string()),
                    }
                }
                AdminReq::CancelJob { job } => match a.jobs.lock().get(&job) {
                    Some(p) => {
                        p.lock().cancelled = true;
                        AdminResp::Started
                    }
                    None => AdminResp::Err(format!("no job {job}")),
                },
                AdminReq::EvictExact { files, store } => {
                    let mut removed = 0;
                    let mut refused = Vec::new();
                    for (f, g) in files {
                        match a.vfs.evict_generation(f, g, store).await {
                            Ok(()) => removed += 1,
                            Err(e) => refused.push((f, e.to_string())),
                        }
                    }
                    AdminResp::Evicted { removed, refused }
                }
                AdminReq::BackupCreate {
                    job,
                    name,
                    root,
                    store,
                } => match a.area(store) {
                    Ok((_, area)) => {
                        let p = a.track_job(job);
                        let vfs = a.vfs.clone();
                        tokio::spawn(async move {
                            if let Err(e) =
                                crate::backup::create(vfs, area, store, name, root, p.clone()).await
                            {
                                let mut g = p.lock();
                                g.failed.push((FileId(0), e.to_string()));
                                g.finished = true;
                            }
                        });
                        AdminResp::Started
                    }
                    Err(e) => AdminResp::Err(e),
                },
                AdminReq::BackupRestore {
                    job,
                    id,
                    store,
                    dst,
                } => match a.area(store) {
                    Ok((_, area)) => {
                        let p = a.track_job(job);
                        let vfs = a.vfs.clone();
                        tokio::spawn(async move {
                            if let Err(e) =
                                crate::backup::restore(vfs, area, id, dst, p.clone()).await
                            {
                                let mut g = p.lock();
                                g.failed.push((FileId(0), e.to_string()));
                                g.finished = true;
                            }
                        });
                        AdminResp::Started
                    }
                    Err(e) => AdminResp::Err(e),
                },
                AdminReq::BackupDelete { id, store } => match a.area(store) {
                    Ok((_, area)) => match crate::backup::delete(a.vfs.clone(), area, id).await {
                        Ok(n) => AdminResp::Removed(n),
                        Err(e) => AdminResp::Err(e.to_string()),
                    },
                    Err(e) => AdminResp::Err(e),
                },
                AdminReq::MetaSnapshot { store } => match a.area(store) {
                    Ok((arch, _)) => match crate::backup::meta_snapshot(
                        &a.vfs,
                        std::path::Path::new(&arch.config.path),
                        14,
                    ) {
                        Ok(p) => AdminResp::Path(p.to_string_lossy().into_owned()),
                        Err(e) => AdminResp::Err(e.to_string()),
                    },
                    Err(e) => AdminResp::Err(e),
                },
                AdminReq::Membership(change) => {
                    let meta = a.vfs.data().meta().clone();
                    let r = match change {
                        MembershipChange::Add { node, addr, voter } => {
                            meta.add_node(node, addr, voter).await
                        }
                        MembershipChange::Remove { node } => meta.remove_node(node).await,
                    };
                    match r {
                        Ok(()) => AdminResp::Started,
                        Err(e) => AdminResp::Err(e.to_string()),
                    }
                }
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
