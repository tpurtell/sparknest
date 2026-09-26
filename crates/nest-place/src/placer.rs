//! The placement coordinator behind the management API.

use crate::admin::{self, AdminReq, AdminResp, JobProgress, NodeInfo};
use crate::selector::{self, Manifest};
use crate::spec::{RuleSpec, Selector};
use nest_data::Vfs;
use nest_meta::{Command, StoreClass, query};
use nest_types::{FileId, NestError, NestResult, NodeId};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HostRef {
    pub node: NodeId,
    pub name: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ClusterJob {
    pub id: u64,
    pub what: String,
    pub hosts: BTreeMap<String, JobProgress>,
    /// Files skipped because they are being written.
    pub pending: u64,
    pub finished: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeStatus {
    pub node: NodeId,
    pub name: String,
    pub info: Option<NodeInfo>,
    pub error: Option<String>,
}

/// Per-host readiness of a selection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Readiness {
    pub host: String,
    pub files: u64,
    pub bytes: u64,
    pub missing_files: u64,
    pub missing_bytes: u64,
    pub ready: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvictReport {
    pub host: String,
    pub removed: u64,
    pub refused: Vec<(FileId, String)>,
}

pub struct Placer {
    vfs: Arc<Vfs>,
    admin: Arc<admin::Admin>,
    jobs: Mutex<HashMap<u64, Arc<Mutex<ClusterJob>>>>,
    imports: Mutex<HashMap<u64, Arc<Mutex<crate::import::ImportProgress>>>>,
    next_job: AtomicU64,
}

fn sql(e: rusqlite::Error) -> NestError {
    NestError::Io(format!("metadata: {e}"))
}

impl Placer {
    /// Register this node's ADMIN service and its name, and return the
    /// coordinator.
    pub fn start(vfs: Arc<Vfs>, name: String, mountpoint: Option<String>) -> Arc<Placer> {
        let rpc = vfs.data().meta().rpc().clone();
        let admin = admin::Admin::register(&rpc, vfs.clone(), name.clone(), mountpoint);
        let p = Arc::new(Placer {
            vfs: vfs.clone(),
            admin,
            jobs: Mutex::new(HashMap::new()),
            imports: Mutex::new(HashMap::new()),
            next_job: AtomicU64::new(rand::random::<u32>() as u64),
        });
        // Record our name so rules and tools can say "raptor" (needs quorum;
        // retried until it lands).
        let me = vfs.data().id();
        tokio::spawn(async move {
            loop {
                let r = vfs
                    .data()
                    .meta()
                    .propose(Command::RegisterStore {
                        name: name.clone(),
                        class: StoreClass::Live,
                        node: Some(me),
                        config: "{}".into(),
                    })
                    .await;
                match r {
                    Ok(_) | Err(NestError::Exists) => return,
                    Err(_) => tokio::time::sleep(Duration::from_secs(2)).await,
                }
            }
        });
        p
    }

    fn rpc(&self) -> &nest_rpc::Rpc {
        self.vfs.data().meta().rpc()
    }

    fn conn(&self) -> NestResult<rusqlite::Connection> {
        self.vfs.data().meta().open_reader().map_err(sql)
    }

    /// Every member node with its registered name (id as fallback).
    pub fn nodes(&self) -> NestResult<Vec<HostRef>> {
        let names: HashMap<NodeId, String> = query::stores(&self.conn()?)
            .map_err(sql)?
            .into_iter()
            .filter_map(|s| s.node.map(|n| (n, s.name)))
            .collect();
        let m = self
            .vfs
            .data()
            .meta()
            .raft()
            .metrics()
            .borrow()
            .membership_config
            .clone();
        let mut out: Vec<HostRef> = m
            .nodes()
            .map(|(id, _)| {
                let n = NodeId(*id);
                HostRef {
                    node: n,
                    name: names
                        .get(&n)
                        .cloned()
                        .unwrap_or_else(|| format!("node{id}")),
                }
            })
            .collect();
        out.sort_by_key(|h| h.node);
        Ok(out)
    }

    pub fn resolve_hosts(&self, hosts: &[String]) -> NestResult<Vec<HostRef>> {
        let all = self.nodes()?;
        let mut out: Vec<HostRef> = Vec::new();
        for h in hosts {
            if h == "@all" {
                out.extend(all.iter().cloned());
                continue;
            }
            match all
                .iter()
                .find(|x| x.name == *h || format!("node{}", x.node) == *h)
            {
                Some(x) => out.push(x.clone()),
                None => return Err(NestError::Invalid(format!("unknown host {h:?}"))),
            }
        }
        out.sort_by_key(|h| h.node);
        out.dedup_by_key(|h| h.node);
        Ok(out)
    }

    pub async fn manifest(&self, sel: &Selector) -> NestResult<Manifest> {
        let vfs = self.vfs.clone();
        selector::resolve(
            || self.conn(),
            sel,
            |f| {
                let vfs = vfs.clone();
                async move {
                    let (fh, _) = vfs.open(f, 0).await?;
                    let r = vfs.read(fh, 0, 4096).await;
                    vfs.release(fh, None).await;
                    r
                }
            },
        )
        .await
    }

    pub async fn status(&self) -> NestResult<Vec<NodeStatus>> {
        let nodes = self.nodes()?;
        let infos = futures::future::join_all(nodes.iter().map(|h| async move {
            if h.node == self.vfs.data().id() {
                return (h.clone(), Ok(self.admin.info()));
            }
            let r = admin::call(self.rpc(), h.node, &AdminReq::Info, Duration::from_secs(3)).await;
            (
                h.clone(),
                r.and_then(|r| match r {
                    AdminResp::Info(i) => Ok(i),
                    _ => Err(NestError::Io("unexpected".into())),
                }),
            )
        }))
        .await;
        Ok(infos
            .into_iter()
            .map(|(h, r)| NodeStatus {
                node: h.node,
                name: h.name,
                error: r.as_ref().err().map(|e| e.to_string()),
                info: r.ok(),
            })
            .collect())
    }

    /// Which hosts hold a complete current copy of everything selected.
    pub async fn readiness(
        &self,
        sel: &Selector,
        hosts: &[String],
    ) -> NestResult<(Manifest, Vec<Readiness>)> {
        let m = self.manifest(sel).await?;
        let all = ["@all".to_string()];
        let hosts = self.resolve_hosts(if hosts.is_empty() { &all[..] } else { hosts })?;
        let c = self.conn()?;
        let mut out = Vec::new();
        for h in hosts {
            let mut r = Readiness {
                host: h.name.clone(),
                files: m.entries.len() as u64,
                bytes: m.bytes(),
                missing_files: 0,
                missing_bytes: 0,
                ready: false,
            };
            for e in &m.entries {
                if !(e.stable
                    && query::has_live_replica(&c, e.file, e.generation, h.node.live_store())
                        .map_err(sql)?)
                {
                    r.missing_files += 1;
                    r.missing_bytes += e.size;
                }
            }
            r.ready = r.missing_files == 0;
            out.push(r);
        }
        Ok((m, out))
    }

    /// Make every host hold a complete copy of the selection. Returns a job
    /// id; progress via [`Placer::job`].
    pub async fn replicate(
        self: &Arc<Self>,
        sel: Selector,
        hosts: Vec<String>,
        parallel: usize,
    ) -> NestResult<u64> {
        let m = self.manifest(&sel).await?;
        let hosts = self.resolve_hosts(&hosts)?;
        let id = self.next_job.fetch_add(1, Ordering::Relaxed);
        let job = Arc::new(Mutex::new(ClusterJob {
            id,
            what: format!("replicate {}", sel.describe()),
            ..Default::default()
        }));
        self.jobs.lock().insert(id, job.clone());
        let me = self.clone();
        tokio::spawn(async move {
            let r = me.run_replicate(&job, m, hosts, parallel).await;
            let mut j = job.lock();
            j.finished = true;
            j.error = r.err().map(|e| e.to_string());
        });
        Ok(id)
    }

    async fn run_replicate(
        &self,
        job: &Mutex<ClusterJob>,
        m: Manifest,
        hosts: Vec<HostRef>,
        parallel: usize,
    ) -> NestResult<()> {
        let c = self.conn()?;
        job.lock().pending = m.entries.iter().filter(|e| !e.stable).count() as u64;
        let mut started = Vec::new();
        for h in &hosts {
            let files: Vec<(FileId, u64)> = m
                .entries
                .iter()
                .filter(|e| e.stable)
                .filter(|e| {
                    !query::has_live_replica(&c, e.file, e.generation, h.node.live_store())
                        .unwrap_or(false)
                })
                .map(|e| (e.file, e.size))
                .collect();
            let hid = rand::random::<u64>();
            job.lock().hosts.insert(
                h.name.clone(),
                JobProgress {
                    total_files: files.len() as u64,
                    total_bytes: files.iter().map(|f| f.1).sum(),
                    finished: files.is_empty(),
                    ..Default::default()
                },
            );
            if files.is_empty() {
                continue;
            }
            admin::call(
                self.rpc(),
                h.node,
                &AdminReq::StartReplicate {
                    job: hid,
                    files,
                    parallel,
                },
                Duration::from_secs(10),
            )
            .await?;
            started.push((h.clone(), hid));
        }
        loop {
            let mut all_done = true;
            for (h, hid) in &started {
                match admin::call(
                    self.rpc(),
                    h.node,
                    &AdminReq::JobStatus { job: *hid },
                    Duration::from_secs(10),
                )
                .await
                {
                    Ok(AdminResp::Job(p)) => {
                        all_done &= p.finished;
                        job.lock().hosts.insert(h.name.clone(), p);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        all_done = false;
                        tracing::warn!(host = %h.name, error = %e, "polling replication job failed");
                    }
                }
            }
            if all_done {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Import a local directory into the namespace (this node's store).
    pub fn import(self: &Arc<Self>, opts: crate::import::ImportOptions) -> u64 {
        let id = self.next_job.fetch_add(1, Ordering::Relaxed);
        let job = Arc::new(Mutex::new(ClusterJob {
            id,
            what: format!("import {} -> {}", opts.src.display(), opts.dst),
            ..Default::default()
        }));
        self.jobs.lock().insert(id, job.clone());
        let progress = Arc::new(Mutex::new(crate::import::ImportProgress::default()));
        self.imports.lock().insert(id, progress.clone());
        let vfs = self.vfs.clone();
        tokio::spawn(async move {
            let r = crate::import::run(vfs, opts, progress.clone()).await;
            progress.lock().finished = true;
            let mut j = job.lock();
            j.finished = true;
            j.error = r.err().map(|e| e.to_string());
        });
        id
    }

    pub fn import_progress(&self, id: u64) -> Option<crate::import::ImportProgress> {
        self.imports.lock().get(&id).map(|p| p.lock().clone())
    }

    pub fn job(&self, id: u64) -> Option<ClusterJob> {
        self.jobs.lock().get(&id).map(|j| j.lock().clone())
    }

    pub fn jobs(&self) -> Vec<ClusterJob> {
        let mut v: Vec<ClusterJob> = self
            .jobs
            .lock()
            .values()
            .map(|j| j.lock().clone())
            .collect();
        v.sort_by_key(|j| j.id);
        v
    }

    /// Remove copies of the selection from `hosts` (never the last live copy).
    pub async fn evict(&self, sel: &Selector, hosts: &[String]) -> NestResult<Vec<EvictReport>> {
        let m = self.manifest(sel).await?;
        let files: Vec<FileId> = m.entries.iter().map(|e| e.file).collect();
        let mut out = Vec::new();
        for h in self.resolve_hosts(hosts)? {
            match admin::call(
                self.rpc(),
                h.node,
                &AdminReq::Evict {
                    files: files.clone(),
                },
                Duration::from_secs(600),
            )
            .await?
            {
                AdminResp::Evicted { removed, refused } => out.push(EvictReport {
                    host: h.name,
                    removed,
                    refused,
                }),
                other => return Err(NestError::Io(format!("unexpected {other:?}"))),
            }
        }
        Ok(out)
    }

    // ------------------------------------------------------------ rules

    pub fn rules(&self) -> NestResult<Vec<(String, RuleSpec, u64)>> {
        query::rules(&self.conn()?)
            .map_err(sql)?
            .into_iter()
            .map(|r| {
                let spec: RuleSpec = serde_json::from_str(&r.spec)
                    .map_err(|e| NestError::Io(format!("rule {}: {e}", r.name)))?;
                Ok((r.name, spec, r.revision))
            })
            .collect()
    }

    pub async fn set_rule(&self, name: &str, spec: &RuleSpec) -> NestResult<u64> {
        self.resolve_hosts(&spec.hosts)?;
        let json = serde_json::to_string(spec).map_err(|e| NestError::Io(e.to_string()))?;
        match self
            .vfs
            .data()
            .meta()
            .propose(Command::SetRule {
                name: name.into(),
                spec: json,
                expect_revision: None,
            })
            .await?
        {
            nest_meta::Reply::Revision(r) => Ok(r),
            other => Err(NestError::Io(format!("unexpected {other:?}"))),
        }
    }

    pub async fn delete_rule(&self, name: &str) -> NestResult<()> {
        self.vfs
            .data()
            .meta()
            .propose(Command::DeleteRule { name: name.into() })
            .await
            .map(|_| ())
    }

    /// Converge one rule (or all when `name` is None). Returns job ids.
    pub async fn reconcile(
        self: &Arc<Self>,
        name: Option<&str>,
        parallel: usize,
    ) -> NestResult<Vec<u64>> {
        let mut ids = Vec::new();
        for (n, spec, _) in self.rules()? {
            if name.is_some_and(|x| x != n) {
                continue;
            }
            ids.push(
                self.replicate(spec.selector.clone(), spec.hosts.clone(), parallel)
                    .await?,
            );
        }
        if ids.is_empty() && name.is_some() {
            return Err(NestError::NotFound);
        }
        Ok(ids)
    }
}
