//! The placement coordinator behind the management API.

use crate::admin::{self, AdminReq, AdminResp, JobProgress, NodeInfo};
use crate::selector::{self, Manifest};
use crate::spec::{RuleSpec, Selector};
use nest_data::Vfs;
use nest_meta::{Command, StoreClass, query};
use nest_types::{FileId, NestError, NestResult, NodeId, StoreId};
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

/// Where copies go: a node's live store or an archive store, and the node
/// that does the work (the node itself, or a gateway of the archive).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Target {
    pub name: String,
    pub store: StoreId,
    pub node: NodeId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoreStatus {
    pub id: StoreId,
    pub name: String,
    pub path: String,
    pub gateways: Vec<(String, admin::StoreHealth)>,
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
    /// Unix milliseconds.
    #[serde(default)]
    pub started_ms: u64,
    #[serde(default)]
    pub finished_ms: Option<u64>,
    #[serde(default)]
    pub cancelled: bool,
    /// What happened, for people (e.g. per-repo outcomes of an hf import).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
    /// Node-side jobs to tell when cancelled.
    #[serde(skip)]
    host_jobs: Vec<(NodeId, u64)>,
}

fn cancelled(job: &Mutex<ClusterJob>) -> NestResult<()> {
    if job.lock().cancelled {
        return Err(NestError::Io(
            "cancelled; files already copied stay, nothing was removed".into(),
        ));
    }
    Ok(())
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
    /// Recent answers the web UI asks for every few seconds.
    usage_cache: Mutex<Option<(u64, std::time::Instant, UsageMap)>>,
    stores_cache: Mutex<Option<(std::time::Instant, Vec<StoreStatus>)>>,
    plans: Mutex<HashMap<u64, crate::plan::Plan>>,
    next_job: AtomicU64,
    dirty: Arc<tokio::sync::Notify>,
}

/// The leader snapshots metadata into every healthy archive store this often.
const META_SNAPSHOT_EVERY: Duration = Duration::from_secs(6 * 3600);

async fn auto_meta_snapshots(p: std::sync::Weak<Placer>) {
    loop {
        tokio::time::sleep(META_SNAPSHOT_EVERY).await;
        let Some(p) = p.upgrade() else { return };
        if p.vfs.data().meta().leader() != Some(p.vfs.data().id()) {
            continue;
        }
        for (_, name, _) in p.archive_stores().unwrap_or_default() {
            match p.meta_snapshot(&name).await {
                Ok(path) => tracing::info!(%path, "metadata snapshot written"),
                Err(e) => tracing::warn!(store = %name, error = %e, "metadata snapshot failed"),
            }
        }
    }
}

/// Quiet period after the last change before automatic rules are applied.
const AUTO_DEBOUNCE: Duration = Duration::from_secs(5);

async fn auto_reconcile(p: std::sync::Weak<Placer>) {
    loop {
        let Some(dirty) = p.upgrade().map(|p| p.dirty.clone()) else {
            return;
        };
        dirty.notified().await;
        // Debounce: wait until no change for the quiet period.
        while tokio::time::timeout(AUTO_DEBOUNCE, dirty.notified())
            .await
            .is_ok()
        {}
        let Some(p) = p.upgrade() else { return };
        if p.vfs.data().meta().leader() != Some(p.vfs.data().id()) {
            continue;
        }
        let rules = match p.rules() {
            Ok(r) => r,
            Err(_) => continue,
        };
        for (name, spec, _) in rules.into_iter().filter(|(_, s, _)| s.auto) {
            // Skip if an earlier pass for this rule is still running.
            let what = format!("replicate {}", spec.selector.describe());
            if p.jobs().iter().any(|j| !j.finished && j.what == what) {
                continue;
            }
            match p
                .replicate(spec.selector.clone(), spec.hosts.clone(), 8)
                .await
            {
                Ok(id) => tracing::info!(rule = %name, job = id, "automatic reconcile started"),
                Err(e) => tracing::warn!(rule = %name, error = %e, "automatic reconcile failed"),
            }
        }
    }
}

/// Per-host file usage, by node.
pub type UsageMap = HashMap<NodeId, HashMap<FileId, nest_data::usage::FileUsage>>;

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
            usage_cache: Mutex::new(None),
            stores_cache: Mutex::new(None),
            plans: Mutex::new(HashMap::new()),
            next_job: AtomicU64::new(rand::random::<u32>() as u64),
            dirty: Arc::new(tokio::sync::Notify::new()),
        });
        // Automatic rules: any settled content marks them dirty; the leader
        // reconciles after writes go quiet (debounced per pass, not per file).
        let dirty = p.dirty.clone();
        vfs.data().add_tap(Arc::new(move |_i, e| {
            if matches!(
                e,
                nest_meta::Effect::Finalized { .. } | nest_meta::Effect::EntryChanged { .. }
            ) {
                dirty.notify_one();
            }
        }));
        tokio::spawn(auto_reconcile(Arc::downgrade(&p)));
        tokio::spawn(auto_meta_snapshots(Arc::downgrade(&p)));
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

    pub fn vfs(&self) -> &Arc<Vfs> {
        &self.vfs
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
        let m = self.vfs.data().meta().metrics().membership_config;
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

    /// Expand "@all" and "@group" names (groups may name hosts or stores).
    pub fn expand_names(&self, names: &[String]) -> NestResult<Vec<String>> {
        let groups = query::groups(&self.conn()?).map_err(sql)?;
        let mut out = Vec::new();
        for n in names {
            match n.strip_prefix('@') {
                Some("all") => out.push(n.clone()),
                Some(g) => match groups.iter().find(|(name, _)| name == g) {
                    Some((_, members)) => out.extend(members.iter().cloned()),
                    None => return Err(NestError::Invalid(format!("unknown group {n:?}"))),
                },
                None => out.push(n.clone()),
            }
        }
        Ok(out)
    }

    pub fn resolve_hosts(&self, hosts: &[String]) -> NestResult<Vec<HostRef>> {
        let hosts = self.expand_names(hosts)?;
        let all = self.nodes()?;
        let mut out: Vec<HostRef> = Vec::new();
        for h in &hosts {
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

    pub fn groups(&self) -> NestResult<Vec<(String, Vec<String>)>> {
        query::groups(&self.conn()?).map_err(sql)
    }

    pub async fn set_group(&self, name: &str, members: Vec<String>) -> NestResult<()> {
        self.validate_targets(&members)?;
        self.vfs
            .data()
            .meta()
            .propose(Command::SetGroup {
                name: name.into(),
                members,
            })
            .await
            .map(|_| ())
    }

    pub async fn delete_group(&self, name: &str) -> NestResult<()> {
        let tag = format!("@{name}");
        let users: Vec<String> = self
            .rules()?
            .into_iter()
            .filter(|(_, spec, _)| spec.hosts.contains(&tag))
            .map(|(r, _, _)| r)
            .collect();
        if !users.is_empty() {
            return Err(NestError::Invalid(format!(
                "group {tag} is used by rule(s) {}; change them first",
                users.join(", ")
            )));
        }
        self.vfs
            .data()
            .meta()
            .propose(Command::DeleteGroup { name: name.into() })
            .await
            .map(|_| ())
    }

    /// Archive stores: (id, name, config).
    pub fn archive_stores(&self) -> NestResult<Vec<(StoreId, String, nest_data::ArchiveConfig)>> {
        Ok(query::stores(&self.conn()?)
            .map_err(sql)?
            .into_iter()
            .filter(|r| nest_data::is_archive(r.id))
            .filter_map(|r| {
                serde_json::from_str(&r.config)
                    .ok()
                    .map(|c| (r.id, r.name, c))
            })
            .collect())
    }

    /// Check that every name is a node, "@all", or an archive store.
    pub fn validate_targets(&self, names: &[String]) -> NestResult<()> {
        let names = &self.expand_names(names)?;
        let nodes = self.nodes()?;
        let stores = self.archive_stores()?;
        for n in names {
            let known = n == "@all"
                || nodes
                    .iter()
                    .any(|h| h.name == *n || format!("node{}", h.node) == *n)
                || stores.iter().any(|(_, s, _)| s == n);
            if !known {
                return Err(NestError::Invalid(format!("unknown host or store {n:?}")));
            }
        }
        Ok(())
    }

    /// Resolve names to targets. Archive stores are worked by their first
    /// gateway that reports the store healthy.
    pub async fn resolve_targets(&self, names: &[String]) -> NestResult<Vec<Target>> {
        let names = &self.expand_names(names)?;
        self.validate_targets(names)?;
        let stores = self.archive_stores()?;
        let node_names: Vec<String> = names
            .iter()
            .filter(|n| !stores.iter().any(|(_, s, _)| s == *n))
            .cloned()
            .collect();
        let mut out: Vec<Target> = self
            .resolve_hosts(&node_names)?
            .into_iter()
            .map(|h| Target {
                name: h.name,
                store: h.node.live_store(),
                node: h.node,
            })
            .collect();
        for (id, name, cfg) in stores.into_iter().filter(|(_, s, _)| names.contains(s)) {
            let mut chosen = None;
            for g in &cfg.gateways {
                if matches!(self.store_health(*g, id).await, Ok(h) if h.healthy) {
                    chosen = Some(*g);
                    break;
                }
            }
            let node = chosen.ok_or_else(|| {
                NestError::Unavailable(format!("no gateway of store {name} can reach it"))
            })?;
            out.push(Target {
                name,
                store: id,
                node,
            });
        }
        Ok(out)
    }

    async fn store_health(
        &self,
        gateway: NodeId,
        store: StoreId,
    ) -> NestResult<admin::StoreHealth> {
        self.store_health_within(gateway, store, Duration::from_secs(10))
            .await
    }

    async fn store_health_within(
        &self,
        gateway: NodeId,
        store: StoreId,
        within: Duration,
    ) -> NestResult<admin::StoreHealth> {
        match admin::call(
            self.rpc(),
            gateway,
            &AdminReq::StoreHealth { store },
            within,
        )
        .await?
        {
            AdminResp::Health(h) => Ok(h),
            other => Err(NestError::Io(format!("unexpected {other:?}"))),
        }
    }

    /// Register an archive store rooted at `path` on each gateway (a folder:
    /// an SMB share mounted on every node, or a disk on one node) and write
    /// its marker through every gateway.
    pub async fn add_store(
        &self,
        name: &str,
        path: &str,
        gateways: &[String],
    ) -> NestResult<Vec<(String, Result<(), String>)>> {
        let gws = self.resolve_hosts(gateways)?;
        if gws.is_empty() {
            return Err(NestError::Invalid(
                "an archive store needs at least one gateway".into(),
            ));
        }
        let cfg = nest_data::ArchiveConfig {
            path: path.to_string(),
            gateways: gws.iter().map(|g| g.node).collect(),
        };
        let json = serde_json::to_string(&cfg).map_err(|e| NestError::Io(e.to_string()))?;
        let id = match self
            .vfs
            .data()
            .meta()
            .propose(Command::RegisterStore {
                name: name.into(),
                class: StoreClass::Archive,
                node: None,
                config: json,
            })
            .await?
        {
            nest_meta::Reply::Store(id) => id,
            other => return Err(NestError::Io(format!("unexpected {other:?}"))),
        };
        let mut out = Vec::new();
        for g in gws {
            let r = admin::call(
                self.rpc(),
                g.node,
                &AdminReq::InitStore {
                    store: id,
                    name: name.into(),
                    path: path.into(),
                },
                Duration::from_secs(30),
            )
            .await;
            out.push((g.name, r.map(|_| ()).map_err(|e| e.to_string())));
        }
        Ok(out)
    }

    /// Archive stores with the health each gateway reports now.
    pub async fn stores(&self) -> NestResult<Vec<StoreStatus>> {
        let v = self.stores_uncached().await?;
        *self.stores_cache.lock() = Some((std::time::Instant::now(), v.clone()));
        Ok(v)
    }

    /// `stores()`, reusing an answer up to 5 s old: for listings the UI
    /// polls (a NAS share can be slow to report under load). Placement
    /// decisions use `stores()`.
    pub async fn stores_listing(&self) -> NestResult<Vec<StoreStatus>> {
        if let Some((at, v)) = &*self.stores_cache.lock()
            && at.elapsed() < Duration::from_secs(5)
        {
            return Ok(v.clone());
        }
        self.stores().await
    }

    async fn stores_uncached(&self) -> NestResult<Vec<StoreStatus>> {
        let names: HashMap<NodeId, String> = self
            .nodes()?
            .into_iter()
            .map(|h| (h.node, h.name))
            .collect();
        let mut out = Vec::new();
        for (id, name, cfg) in self.archive_stores()? {
            // All gateways at once.
            let names = &names;
            let checks = cfg.gateways.iter().map(|g| async move {
                let h = self
                    .store_health_within(*g, id, Duration::from_secs(4))
                    .await
                    .unwrap_or_else(|e| admin::StoreHealth {
                        error: Some(e.to_string()),
                        ..Default::default()
                    });
                (
                    names.get(g).cloned().unwrap_or_else(|| format!("node{g}")),
                    h,
                )
            });
            let gateways = futures::future::join_all(checks).await;
            out.push(StoreStatus {
                id,
                name,
                path: cfg.path,
                gateways,
            });
        }
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
        let hosts: Vec<Target> = if hosts.is_empty() {
            // Every node, plus every archive store (without health checks).
            let mut t: Vec<Target> = self
                .nodes()?
                .into_iter()
                .map(|h| Target {
                    name: h.name,
                    store: h.node.live_store(),
                    node: h.node,
                })
                .collect();
            t.extend(
                self.archive_stores()?
                    .into_iter()
                    .map(|(id, name, cfg)| Target {
                        name,
                        store: id,
                        node: cfg.gateways.first().copied().unwrap_or_default(),
                    }),
            );
            t
        } else {
            let hosts = &self.expand_names(hosts)?;
            self.validate_targets(hosts)?;
            let stores = self.archive_stores()?;
            let mut t: Vec<Target> = self
                .resolve_hosts(
                    &hosts
                        .iter()
                        .filter(|n| !stores.iter().any(|(_, s, _)| s == *n))
                        .cloned()
                        .collect::<Vec<_>>(),
                )?
                .into_iter()
                .map(|h| Target {
                    name: h.name,
                    store: h.node.live_store(),
                    node: h.node,
                })
                .collect();
            t.extend(
                stores
                    .into_iter()
                    .filter(|(_, s, _)| hosts.contains(s))
                    .map(|(id, name, cfg)| Target {
                        name,
                        store: id,
                        node: cfg.gateways.first().copied().unwrap_or_default(),
                    }),
            );
            t
        };
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
                    && query::has_live_replica(&c, e.file, e.generation, h.store).map_err(sql)?)
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
        let hosts = self.resolve_targets(&hosts).await?;
        let id = self.next_job.fetch_add(1, Ordering::Relaxed);
        let job = Arc::new(Mutex::new(ClusterJob {
            id,
            what: format!("replicate {}", sel.describe()),
            started_ms: now_ms(),
            ..Default::default()
        }));
        self.jobs.lock().insert(id, job.clone());
        // Jobs live in memory only; the log is what outlives a restart.
        tracing::info!(
            job = id,
            selector = %sel.describe(),
            to = ?hosts.iter().map(|h| &h.name).collect::<Vec<_>>(),
            files = m.entries.len(),
            bytes = m.bytes(),
            "replicate started"
        );
        let me = self.clone();
        tokio::spawn(async move {
            let r = me.run_replicate(&job, m, hosts, parallel).await;
            let mut j = job.lock();
            j.finished = true;
            j.finished_ms = Some(now_ms());
            j.error = r.err().map(|e| e.to_string());
        });
        Ok(id)
    }

    async fn run_replicate(
        &self,
        job: &Mutex<ClusterJob>,
        m: Manifest,
        hosts: Vec<Target>,
        parallel: usize,
    ) -> NestResult<()> {
        let c = self.conn()?;
        job.lock().pending = m.entries.iter().filter(|e| !e.stable).count() as u64;
        let mut started = Vec::new();
        for h in &hosts {
            if job.lock().cancelled {
                break;
            }
            let files: Vec<(FileId, u64)> = m
                .entries
                .iter()
                .filter(|e| e.stable)
                .filter(|e| {
                    !query::has_live_replica(&c, e.file, e.generation, h.store).unwrap_or(false)
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
                    target: h.store,
                },
                Duration::from_secs(10),
            )
            .await?;
            let late = {
                let mut j = job.lock();
                j.host_jobs.push((h.node, hid));
                j.cancelled
            };
            if late {
                // Cancelled while this host was starting: tell it too.
                let _ = admin::call(
                    self.rpc(),
                    h.node,
                    &AdminReq::CancelJob { job: hid },
                    Duration::from_secs(5),
                )
                .await;
            }
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
        let what = format!("import {} -> {}", opts.src.display(), opts.dst);
        tracing::info!(src = %opts.src.display(), dst = %opts.dst, r#move = opts.r#move, copy = opts.copy, "import started");
        self.local_job(what, move |vfs, progress, _| {
            crate::import::run(vfs, opts, progress)
        })
    }

    /// Bring a Hugging Face cache into the hub (see `hfimport`).
    pub fn hf_import(self: &Arc<Self>, opts: crate::hfimport::HfImportOptions) -> u64 {
        let what = format!("hf import {} -> {}", opts.src.display(), opts.hub);
        tracing::info!(src = %opts.src.display(), hub = %opts.hub, r#move = opts.r#move, hf = ?opts.hf, "hf import started");
        self.local_job(what, move |vfs, progress, notes| async move {
            let outcomes = Arc::new(Mutex::new(Vec::new()));
            let r = crate::hfimport::run(vfs, opts, progress, outcomes.clone()).await;
            let mut n = notes.lock();
            for o in outcomes.lock().iter() {
                n.push(format!(
                    "{}: {}{}{}",
                    o.repo,
                    o.status,
                    if o.moved { ", moved" } else { "" },
                    if o.note.is_empty() {
                        String::new()
                    } else {
                        format!(" ({})", o.note)
                    }
                ));
            }
            r
        })
    }

    /// A job that runs on this node over an `ImportProgress`: its progress
    /// is mirrored into the job under this host's name (files, bytes and
    /// rate like any other job), it can be cancelled, and `notes` lines end
    /// up on the job.
    fn local_job<F, Fut>(self: &Arc<Self>, what: String, work: F) -> u64
    where
        F: FnOnce(
                Arc<Vfs>,
                Arc<Mutex<crate::import::ImportProgress>>,
                Arc<Mutex<Vec<String>>>,
            ) -> Fut
            + Send
            + 'static,
        Fut: std::future::Future<Output = NestResult<()>> + Send + 'static,
    {
        let id = self.next_job.fetch_add(1, Ordering::Relaxed);
        let job = Arc::new(Mutex::new(ClusterJob {
            id,
            what,
            started_ms: now_ms(),
            ..Default::default()
        }));
        self.jobs.lock().insert(id, job.clone());
        let progress = Arc::new(Mutex::new(crate::import::ImportProgress::default()));
        self.imports.lock().insert(id, progress.clone());
        let vfs = self.vfs.clone();
        let host = self
            .nodes()
            .ok()
            .and_then(|n| n.into_iter().find(|h| h.node == vfs.data().id()))
            .map(|h| h.name)
            .unwrap_or_else(|| "here".into());
        let notes = Arc::new(Mutex::new(Vec::new()));
        let mirror = {
            let (job, progress, notes) = (job.clone(), progress.clone(), notes.clone());
            move || {
                let p = progress.lock().clone();
                let (df, db) = p.done();
                let mut j = job.lock();
                j.notes = notes.lock().clone();
                j.hosts.insert(
                    host.clone(),
                    admin::JobProgress {
                        total_files: p.total_files,
                        done_files: df,
                        total_bytes: p.total_bytes,
                        done_bytes: db,
                        failed: p.errors.iter().map(|e| (FileId(0), e.clone())).collect(),
                        finished: p.finished,
                        cancelled: p.cancelled,
                    },
                );
            }
        };
        let ticker = {
            let (mirror, progress) = (mirror.clone(), progress.clone());
            tokio::spawn(async move {
                while !progress.lock().finished {
                    mirror();
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            })
        };
        let fut = work(vfs, progress.clone(), notes);
        tokio::spawn(async move {
            let r = fut.await;
            progress.lock().finished = true;
            ticker.abort();
            mirror();
            let mut j = job.lock();
            j.finished = true;
            j.finished_ms = Some(now_ms());
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

    /// Cancel a running replicate, offload or plan job: no new files start
    /// (those copying finish), and nothing is evicted afterwards.
    pub async fn cancel_job(&self, id: u64) -> NestResult<()> {
        let job = self
            .jobs
            .lock()
            .get(&id)
            .cloned()
            .ok_or(NestError::NotFound)?;
        let hosts = {
            let mut j = job.lock();
            if j.finished {
                return Err(NestError::Invalid("job already finished".into()));
            }
            if ![
                "replicate ",
                "offload ",
                "apply plan ",
                "import ",
                "hf import ",
            ]
            .iter()
            .any(|k| j.what.starts_with(k))
            {
                return Err(NestError::Invalid(format!(
                    "{} cannot be cancelled",
                    j.what
                )));
            }
            j.cancelled = true;
            j.host_jobs.clone()
        };
        if let Some(p) = self.imports.lock().get(&id) {
            p.lock().cancelled = true;
        }
        tracing::info!(job = id, "job cancelled");
        for (node, hid) in hosts {
            if let Err(e) = admin::call(
                self.rpc(),
                node,
                &AdminReq::CancelJob { job: hid },
                Duration::from_secs(5),
            )
            .await
            {
                tracing::warn!(job = id, node = node.0, error = %e, "could not cancel on a host");
            }
        }
        Ok(())
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
        tracing::info!(selector = %sel.describe(), from = ?hosts, files = m.entries.len(), "evict");
        let files: Vec<FileId> = m.entries.iter().map(|e| e.file).collect();
        let mut out = Vec::new();
        for h in self.resolve_targets(hosts).await? {
            match admin::call(
                self.rpc(),
                h.node,
                &AdminReq::Evict {
                    files: files.clone(),
                    store: h.store,
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

    /// Copy the selection into archive store `store`, then remove every live
    /// copy (the archive copy keeps it available; reads stream through a
    /// gateway, `replicate` recalls it).
    pub async fn offload(
        self: &Arc<Self>,
        sel: Selector,
        store: String,
        parallel: usize,
    ) -> NestResult<u64> {
        let m = self.manifest(&sel).await?;
        let targets = self.resolve_targets(std::slice::from_ref(&store)).await?;
        if targets.len() != 1 || !nest_data::is_archive(targets[0].store) {
            return Err(NestError::Invalid(format!(
                "{store:?} is not an archive store"
            )));
        }
        let id = self.next_job.fetch_add(1, Ordering::Relaxed);
        let job = Arc::new(Mutex::new(ClusterJob {
            id,
            what: format!("offload {} -> {store}", sel.describe()),
            started_ms: now_ms(),
            ..Default::default()
        }));
        self.jobs.lock().insert(id, job.clone());
        tracing::info!(job = id, selector = %sel.describe(), %store, files = m.entries.len(), bytes = m.bytes(), "offload started");
        let me = self.clone();
        tokio::spawn(async move {
            let r = async {
                me.run_replicate(&job, m.clone(), targets, parallel).await?;
                // Never drop live copies for an offload that did not finish.
                cancelled(&job)?;
                let failed: usize = job.lock().hosts.values().map(|p| p.failed.len()).sum();
                if failed > 0 {
                    return Err(NestError::Io(format!(
                        "{failed} files failed to reach the archive; live copies kept"
                    )));
                }
                let files: Vec<FileId> = m
                    .entries
                    .iter()
                    .filter(|e| e.stable)
                    .map(|e| e.file)
                    .collect();
                let nodes: Vec<String> = me.nodes()?.into_iter().map(|h| h.name).collect();
                me.evict_files(&files, &nodes).await?;
                Ok(())
            }
            .await;
            let mut j = job.lock();
            j.finished = true;
            j.finished_ms = Some(now_ms());
            j.error = r.err().map(|e: NestError| e.to_string());
        });
        Ok(id)
    }

    async fn evict_files(&self, files: &[FileId], hosts: &[String]) -> NestResult<()> {
        for h in self.resolve_targets(hosts).await? {
            admin::call(
                self.rpc(),
                h.node,
                &AdminReq::Evict {
                    files: files.to_vec(),
                    store: h.store,
                },
                Duration::from_secs(600),
            )
            .await?;
        }
        Ok(())
    }

    // ------------------------------------------------------------ usage

    /// Every host's per-file usage since `since_ms`, by node. Hosts that do
    /// not answer are left out (their files look unused: callers that
    /// remove copies must treat a missing host as unknown, not idle).
    pub async fn usage(&self, since_ms: u64) -> NestResult<UsageMap> {
        // Usage moves slowly: the same window within a few seconds is
        // answered from memory (the Models page asks on every refresh).
        let key = since_ms / 60_000;
        if let Some((k, at, v)) = &*self.usage_cache.lock()
            && *k == key
            && at.elapsed() < Duration::from_secs(10)
        {
            return Ok(v.clone());
        }
        let v = self.usage_uncached(since_ms).await?;
        *self.usage_cache.lock() = Some((key, std::time::Instant::now(), v.clone()));
        Ok(v)
    }

    async fn usage_uncached(&self, since_ms: u64) -> NestResult<UsageMap> {
        let nodes = self.nodes()?;
        let me = self.vfs.data().id();
        let got = futures::future::join_all(nodes.iter().map(|h| async move {
            if h.node == me {
                let u = self.vfs.usage().clone();
                let r = tokio::task::spawn_blocking(move || u.summary(since_ms))
                    .await
                    .map_err(|e| NestError::Io(e.to_string()))
                    .and_then(|r| r.map_err(sql))
                    .map(AdminResp::Usage);
                return (h.node, r);
            }
            let r = admin::call(
                self.rpc(),
                h.node,
                &AdminReq::Usage { since_ms },
                Duration::from_secs(3),
            )
            .await;
            (h.node, r)
        }))
        .await;
        let mut out = HashMap::new();
        for (node, r) in got {
            match r {
                Ok(AdminResp::Usage(v)) => {
                    out.insert(node, v.into_iter().map(|u| (u.file, u)).collect());
                }
                Ok(other) => tracing::warn!(node = node.0, "unexpected usage reply {other:?}"),
                Err(e) => tracing::warn!(node = node.0, error = %e, "usage unavailable"),
            }
        }
        Ok(out)
    }

    // ------------------------------------------------------------ caches

    /// Drop the clean page cache on `hosts` (every node when empty), at
    /// once, for benchmarks: (host, outcome).
    pub async fn drop_caches(
        &self,
        hosts: &[String],
    ) -> NestResult<Vec<(String, Result<String, String>)>> {
        let nodes: Vec<_> = if hosts.is_empty() {
            self.nodes()?
        } else {
            self.resolve_hosts(hosts)?
        };
        let me = self.vfs.data().id();
        Ok(
            futures::future::join_all(nodes.into_iter().map(|h| async move {
                let r = if h.node == me {
                    admin::drop_caches_here().await
                } else {
                    match admin::call(
                        self.rpc(),
                        h.node,
                        &AdminReq::DropCaches,
                        Duration::from_secs(130),
                    )
                    .await
                    {
                        Ok(AdminResp::Dropped(r)) => r,
                        Ok(other) => Err(format!("unexpected {other:?}")),
                        Err(e) => Err(e.to_string()),
                    }
                };
                (h.name, r)
            }))
            .await,
        )
    }

    // ------------------------------------------------------------ logs

    /// Recent log lines from `host` (or every node), merged oldest first,
    /// the newest `q.limit` overall. Hosts that do not answer are listed.
    pub async fn logs(
        &self,
        host: Option<&str>,
        q: crate::logs::LogQuery,
    ) -> NestResult<(Vec<(String, crate::logs::LogLine)>, Vec<String>)> {
        self.logs_since(host, q, &HashMap::new()).await
    }

    /// `logs`, with a starting time per host (for following).
    pub async fn logs_since(
        &self,
        host: Option<&str>,
        q: crate::logs::LogQuery,
        since: &HashMap<String, u64>,
    ) -> NestResult<(Vec<(String, crate::logs::LogLine)>, Vec<String>)> {
        let nodes: Vec<_> = self
            .nodes()?
            .into_iter()
            .filter(|h| host.is_none_or(|x| x == h.name))
            .collect();
        if nodes.is_empty() {
            return Err(NestError::NotFound);
        }
        let me = self.vfs.data().id();
        let q = &q;
        let got = futures::future::join_all(nodes.iter().map(|h| async move {
            let mut q = q.clone();
            if let Some(t) = since.get(&h.name) {
                q.since_ms = Some(*t);
            }
            if h.node == me {
                return (h.name.clone(), Ok(crate::logs::recent(&q)));
            }
            let r = admin::call(
                self.rpc(),
                h.node,
                &AdminReq::Logs(q),
                Duration::from_secs(3),
            )
            .await;
            (
                h.name.clone(),
                match r {
                    Ok(AdminResp::Logs(l)) => Ok(l),
                    Ok(other) => Err(format!("unexpected {other:?}")),
                    Err(e) => Err(e.to_string()),
                },
            )
        }))
        .await;
        let mut lines = Vec::new();
        let mut missing = Vec::new();
        for (name, r) in got {
            match r {
                Ok(l) => lines.extend(l.into_iter().map(|l| (name.clone(), l))),
                Err(e) => missing.push(format!("{name}: {e}")),
            }
        }
        lines.sort_by_key(|(_, l)| l.ts_ms);
        let limit = q.limit.unwrap_or(500);
        if lines.len() > limit {
            lines.drain(..lines.len() - limit);
        }
        Ok((lines, missing))
    }

    // ------------------------------------------------------------ plans

    /// Propose a plan to reach `free` bytes free on each named host/@group,
    /// offloading sole copies only into the `archives` named (none: only
    /// redundant copies are removed).
    pub async fn plan(&self, goal: crate::plan::Goal) -> NestResult<crate::plan::Plan> {
        let p = crate::plan::make(self, goal).await?;
        self.plans.lock().insert(p.id, p.clone());
        Ok(p)
    }

    /// Execute a proposed plan: offloads (copy into the archive, then evict
    /// exactly the planned generations) and evictions.
    pub async fn apply_plan(self: &Arc<Self>, id: u64) -> NestResult<u64> {
        let plan = self
            .plans
            .lock()
            .get(&id)
            .cloned()
            .ok_or(NestError::NotFound)?;
        let jid = self.next_job.fetch_add(1, Ordering::Relaxed);
        let job = Arc::new(Mutex::new(ClusterJob {
            id: jid,
            what: format!("apply plan {id}"),
            started_ms: now_ms(),
            ..Default::default()
        }));
        self.jobs.lock().insert(jid, job.clone());
        tracing::info!(
            job = jid,
            plan = id,
            steps = plan.steps.len(),
            "plan apply started"
        );
        let me = self.clone();
        tokio::spawn(async move {
            let r: NestResult<()> = async {
                for step in &plan.steps {
                    cancelled(&job)?;
                    if let crate::plan::Step::Replicate {
                        host, node, copies, ..
                    } = step
                    {
                        // Speedup: whole files onto the host; nothing removed.
                        let target = Target {
                            name: host.clone(),
                            store: node.live_store(),
                            node: *node,
                        };
                        let m = Manifest {
                            entries: copies
                                .iter()
                                .map(|c| crate::selector::Entry {
                                    file: c.file,
                                    generation: c.generation,
                                    size: c.size,
                                    stable: true,
                                    path: c.path.clone(),
                                })
                                .collect(),
                            dangling: vec![],
                        };
                        me.run_replicate(&job, m, vec![target], 8).await?;
                        continue;
                    }
                    let requires = match step {
                        crate::plan::Step::Evict { requires, .. } => *requires,
                        _ => None,
                    };
                    let (host, node, copies, store) = match step {
                        crate::plan::Step::Evict {
                            host, node, copies, ..
                        } => (host, *node, copies, None),
                        crate::plan::Step::Offload {
                            host,
                            node,
                            copies,
                            store,
                            ..
                        } => (host, *node, copies, Some(store)),
                        crate::plan::Step::Replicate { .. } => continue, // above
                    };
                    // Revalidate: a rule made since planning may now need
                    // some of these copies here.
                    let required = crate::plan::requirements(&me).await?;
                    let (copies, now_required): (Vec<_>, Vec<_>) = copies
                        .iter()
                        .cloned()
                        .partition(|c| !required.contains_key(&(c.file, node)));
                    if !now_required.is_empty() {
                        let mut j = job.lock();
                        let p = j.hosts.entry(host.clone()).or_default();
                        p.total_files += now_required.len() as u64;
                        for c in now_required {
                            let why = format!(
                                "{}: kept, now required by rule {:?}",
                                c.path,
                                required[&(c.file, node)]
                            );
                            p.failed.push((c.file, why));
                        }
                    }
                    if copies.is_empty() {
                        continue;
                    }
                    if let Some(store) = store {
                        let targets = me.resolve_targets(std::slice::from_ref(store)).await?;
                        let m = Manifest {
                            entries: copies
                                .iter()
                                .map(|c| crate::selector::Entry {
                                    file: c.file,
                                    generation: c.generation,
                                    size: c.size,
                                    stable: true,
                                    path: c.path.clone(),
                                })
                                .collect(),
                            dangling: vec![],
                        };
                        me.run_replicate(&job, m, targets, 8).await?;
                        cancelled(&job)?;
                    }
                    // Consolidating: only what the chosen host now holds.
                    let copies: Vec<_> = match requires {
                        Some(keeper) => {
                            let c = me.conn()?;
                            let (keep, skip): (Vec<_>, Vec<_>) =
                                copies.into_iter().partition(|x| {
                                    query::has_live_replica(
                                        &c,
                                        x.file,
                                        x.generation,
                                        keeper.live_store(),
                                    )
                                    .unwrap_or(false)
                                });
                            if !skip.is_empty() {
                                let mut j = job.lock();
                                let p = j.hosts.entry(host.clone()).or_default();
                                p.total_files += skip.len() as u64;
                                for x in skip {
                                    p.failed.push((
                                        x.file,
                                        format!(
                                            "{}: kept, the target host has no copy yet",
                                            x.path
                                        ),
                                    ));
                                }
                            }
                            keep
                        }
                        None => copies,
                    };
                    if copies.is_empty() {
                        continue;
                    }
                    let files = copies.iter().map(|c| (c.file, c.generation)).collect();
                    match admin::call(
                        me.rpc(),
                        node,
                        &AdminReq::EvictExact {
                            files,
                            store: node.live_store(),
                        },
                        Duration::from_secs(600),
                    )
                    .await?
                    {
                        AdminResp::Evicted { removed, refused } => {
                            let mut j = job.lock();
                            let p = j.hosts.entry(host.clone()).or_default();
                            p.done_files += removed;
                            p.total_files += copies.len() as u64;
                            p.failed.extend(refused);
                        }
                        other => return Err(NestError::Io(format!("unexpected {other:?}"))),
                    }
                }
                Ok(())
            }
            .await;
            let mut j = job.lock();
            j.finished = true;
            j.finished_ms = Some(now_ms());
            j.error = r.err().map(|e| e.to_string());
        });
        Ok(jid)
    }

    // ------------------------------------------------------------ backups

    /// Start a job on one node and mirror its progress under `label`.
    async fn remote_job(
        self: &Arc<Self>,
        what: String,
        host: String,
        node: NodeId,
        req: impl FnOnce(u64) -> AdminReq,
    ) -> NestResult<u64> {
        let hid = rand::random::<u64>();
        admin::call(self.rpc(), node, &req(hid), Duration::from_secs(30)).await?;
        let id = self.next_job.fetch_add(1, Ordering::Relaxed);
        let job = Arc::new(Mutex::new(ClusterJob {
            id,
            what,
            started_ms: now_ms(),
            ..Default::default()
        }));
        self.jobs.lock().insert(id, job.clone());
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                match admin::call(
                    me.rpc(),
                    node,
                    &AdminReq::JobStatus { job: hid },
                    Duration::from_secs(10),
                )
                .await
                {
                    Ok(AdminResp::Job(p)) => {
                        let done = p.finished;
                        let failed = p.failed.len();
                        let mut j = job.lock();
                        j.hosts.insert(host.clone(), p);
                        if done {
                            j.finished = true;
                            j.finished_ms = Some(now_ms());
                            if failed > 0 {
                                j.error = Some(format!("{failed} entries failed"));
                            }
                            return;
                        }
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "polling job failed"),
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
        Ok(id)
    }

    fn selector_root(&self, sel: &Selector) -> String {
        match sel {
            Selector::Path { path } => path.clone(),
            Selector::Hf {
                hub,
                repo,
                repo_type,
                ..
            } => {
                let prefix = if repo_type == "dataset" {
                    "datasets"
                } else {
                    "models"
                };
                format!(
                    "{}/{prefix}--{}",
                    hub.trim_end_matches('/'),
                    repo.replace('/', "--")
                )
            }
        }
    }

    /// Back up a selection into archive store `store` (retained, versioned).
    pub async fn backup_create(
        self: &Arc<Self>,
        sel: Selector,
        name: String,
        store: String,
    ) -> NestResult<u64> {
        let t = self
            .resolve_targets(std::slice::from_ref(&store))
            .await?
            .pop()
            .ok_or(NestError::NotFound)?;
        if !nest_data::is_archive(t.store) {
            return Err(NestError::Invalid(format!(
                "{store:?} is not an archive store"
            )));
        }
        let root = self.selector_root(&sel);
        let st = t.store;
        self.remote_job(
            format!("backup {} -> {store} as {name}", sel.describe()),
            t.name,
            t.node,
            move |job| AdminReq::BackupCreate {
                job,
                name,
                root,
                store: st,
            },
        )
        .await
    }

    fn backup_store(&self, id: u64) -> NestResult<(StoreId, String)> {
        let rows = query::backups(&self.conn()?).map_err(sql)?;
        let b = rows
            .into_iter()
            .find(|b| b.id == id)
            .ok_or(NestError::NotFound)?;
        let name = self
            .archive_stores()?
            .into_iter()
            .find(|(s, _, _)| *s == b.store)
            .map(|(_, n, _)| n)
            .ok_or(NestError::NotFound)?;
        Ok((b.store, name))
    }

    pub async fn backup_restore(self: &Arc<Self>, id: u64, dst: String) -> NestResult<u64> {
        let (store, name) = self.backup_store(id)?;
        let t = self
            .resolve_targets(std::slice::from_ref(&name))
            .await?
            .pop()
            .ok_or(NestError::NotFound)?;
        self.remote_job(
            format!("restore backup {id} -> {dst}"),
            t.name,
            t.node,
            move |job| AdminReq::BackupRestore {
                job,
                id,
                store,
                dst,
            },
        )
        .await
    }

    pub async fn backup_delete(&self, id: u64) -> NestResult<u64> {
        let (store, name) = self.backup_store(id)?;
        let t = self
            .resolve_targets(std::slice::from_ref(&name))
            .await?
            .pop()
            .ok_or(NestError::NotFound)?;
        match admin::call(
            self.rpc(),
            t.node,
            &AdminReq::BackupDelete { id, store },
            Duration::from_secs(600),
        )
        .await?
        {
            AdminResp::Removed(n) => Ok(n),
            other => Err(NestError::Io(format!("unexpected {other:?}"))),
        }
    }

    pub fn backups(&self) -> NestResult<Vec<query::BackupRow>> {
        query::backups(&self.conn()?).map_err(sql)
    }

    /// Snapshot metadata into an archive store (through a healthy gateway).
    pub async fn meta_snapshot(&self, store: &str) -> NestResult<String> {
        let t = self
            .resolve_targets(&[store.to_string()])
            .await?
            .pop()
            .ok_or(NestError::NotFound)?;
        match admin::call(
            self.rpc(),
            t.node,
            &AdminReq::MetaSnapshot { store: t.store },
            Duration::from_secs(600),
        )
        .await?
        {
            AdminResp::Path(p) => Ok(format!("{}:{p}", t.name)),
            other => Err(NestError::Io(format!("unexpected {other:?}"))),
        }
    }

    // ------------------------------------------------------------ membership

    /// Voters, learners and addresses as this node sees them.
    pub fn membership(&self) -> NestResult<serde_json::Value> {
        let m = self.vfs.data().meta().metrics();
        let names: HashMap<NodeId, String> = self
            .nodes()?
            .into_iter()
            .map(|h| (h.node, h.name))
            .collect();
        let mc = m.membership_config.membership();
        let voters: std::collections::BTreeSet<u64> = mc.voter_ids().collect();
        let nodes: Vec<serde_json::Value> = mc
            .nodes()
            .map(|(id, n)| {
                serde_json::json!({
                    "id": id,
                    "name": names.get(&NodeId(*id)).cloned().unwrap_or_default(),
                    "addr": n.addr,
                    "voter": voters.contains(id),
                })
            })
            .collect();
        Ok(
            serde_json::json!({ "leader": m.current_leader, "term": m.current_term, "nodes": nodes }),
        )
    }

    /// Apply a membership change on the leader.
    pub async fn change_membership(&self, change: admin::MembershipChange) -> NestResult<()> {
        let leader = self.vfs.data().meta().leader().ok_or(NestError::NoQuorum)?;
        match admin::call(
            self.rpc(),
            leader,
            &AdminReq::Membership(change),
            Duration::from_secs(60),
        )
        .await?
        {
            AdminResp::Started => Ok(()),
            other => Err(NestError::Io(format!("unexpected {other:?}"))),
        }
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
        self.validate_targets(&spec.hosts)?;
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
