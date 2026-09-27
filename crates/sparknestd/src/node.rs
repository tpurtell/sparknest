use crate::config::Config;
use anyhow::Context;
use nest_data::{DataNode, Vfs, VfsConfig};
use nest_raft::{MetaNode, MetaNodeConfig};
use nest_rpc::{Rpc, RpcConfig};
use nest_store::ObjectStore;
use openraft::BasicNode;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// Timing knobs. Production defaults; tests shrink them.
#[derive(Clone, Debug)]
pub struct Tuning {
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_max_ms: u64,
    pub snapshot_every: u64,
    pub propose_deadline: Duration,
    pub connect_timeout: Duration,
    pub vfs: VfsConfig,
    /// Read lease period (see `DataNode`).
    pub lease: Duration,
    /// Mount the filesystem if the config names a mountpoint.
    pub mount: bool,
    /// RDMA fabric settings; `None` disables it (TCP data path only).
    pub fabric: Option<nest_fabric::FabricConfig>,
    /// Kernel boot id override (tests simulate a host crash with it).
    pub boot_id: Option<String>,
}

impl Default for Tuning {
    fn default() -> Self {
        Tuning {
            heartbeat_ms: 100,
            election_min_ms: 500,
            election_max_ms: 1000,
            snapshot_every: 50_000,
            propose_deadline: Duration::from_secs(15),
            connect_timeout: Duration::from_secs(2),
            vfs: VfsConfig::default(),
            lease: Duration::from_secs(2),
            mount: true,
            fabric: Some(nest_fabric::FabricConfig::default()),
            boot_id: None,
        }
    }
}

/// A running node: RPC endpoint, replicated metadata, local data service.
pub struct Node {
    pub cfg: Config,
    pub rpc: Rpc,
    pub meta: Arc<MetaNode>,
    pub data: Arc<DataNode>,
    pub vfs: Arc<Vfs>,
    pub fabric: Option<Arc<nest_fabric::Fabric>>,
    pub placer: Arc<nest_place::Placer>,
    api_task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    mounted: parking_lot::Mutex<Option<nest_fuse::Mounted>>,
    runstate: parking_lot::Mutex<crate::runstate::RunState>,
    /// How the previous run ended.
    pub prior: crate::runstate::Prior,
    /// What the recovery fsck of a crashed host found, if one ran.
    pub recovery: Option<nest_data::fsck::FsckReport>,
}

impl Node {
    /// Start every service in dependency order. With `bootstrap`, a node
    /// with no Raft state initializes a new cluster from `cluster.members`.
    pub async fn start(
        cfg: Config,
        secret: Vec<u8>,
        tuning: Tuning,
        bootstrap: bool,
    ) -> anyhow::Result<Node> {
        let id = cfg.node.id;
        let secret_for_web = secret.clone();
        std::fs::create_dir_all(&cfg.node.state_dir)
            .with_context(|| format!("creating {}", cfg.node.state_dir.display()))?;
        let rpc = Rpc::bind(RpcConfig {
            node: id,
            cluster: cfg.cluster.name.clone(),
            secret,
            listen: cfg.node.listen,
            connect_timeout: tuning.connect_timeout,
        })
        .await
        .with_context(|| format!("binding {}", cfg.node.listen))?;
        for m in &cfg.cluster.members {
            if m.id != id {
                rpc.set_peer(m.id, m.addr);
            }
        }
        let boot = tuning
            .boot_id
            .clone()
            .unwrap_or_else(crate::runstate::boot_id);
        let (runstate, prior) = crate::runstate::RunState::begin(&cfg.node.state_dir, &boot)
            .context("recording the run state")?;
        if prior.dirty() {
            tracing::warn!(
                "this host went down while sparknestd was running: unsynced writes may be lost; \
                 it will catch up with the cluster and check its objects before serving"
            );
        }
        let store =
            Arc::new(ObjectStore::open(&cfg.node.state_dir).context("opening object store")?);
        store.set_reserve(cfg.node.data_reserve_gib << 30);
        let (data, handler) = DataNode::new(id, store, tuning.lease);
        let mut mc = MetaNodeConfig::new(id, cfg.node.state_dir.clone(), cfg.cluster.name.clone());
        mc.heartbeat_ms = tuning.heartbeat_ms;
        mc.election_min_ms = tuning.election_min_ms;
        mc.election_max_ms = tuning.election_max_ms;
        mc.snapshot_every = tuning.snapshot_every;
        mc.propose_deadline = tuning.propose_deadline;
        let meta = MetaNode::start(mc, rpc.clone(), handler).await?;
        if bootstrap {
            let members: BTreeMap<u64, BasicNode> = cfg
                .cluster
                .members
                .iter()
                .filter(|m| m.voter)
                .map(|m| {
                    (
                        m.id.0,
                        BasicNode {
                            addr: m.addr.to_string(),
                        },
                    )
                })
                .collect();
            if meta.bootstrap(members).await? {
                tracing::info!("initialized new cluster");
            }
        }
        let report = data.attach(meta.clone(), !prior.dirty()).await?;
        tracing::info!(?report, "local store reconciled");
        let vfs = Vfs::new(data.clone(), tuning.vfs.clone());
        let recovery = if prior.dirty() {
            Some(Self::recover(&cfg, &data, &vfs).await?)
        } else {
            None
        };
        let fabric = match (cfg.fabric.mode, &tuning.fabric) {
            (crate::config::FabricMode::Tcp, _) | (_, None) => None,
            (mode, Some(fc)) => {
                let mut fc = fc.clone();
                fc.devices = cfg.fabric.devices.clone();
                match nest_fabric::Fabric::start(id, fc, rpc.clone()) {
                    Ok(Some(f)) => {
                        vfs.attach_fabric(f.clone());
                        Some(f)
                    }
                    Ok(None) if mode == crate::config::FabricMode::Rdma => {
                        anyhow::bail!("fabric.mode = rdma but no RoCE rail was found")
                    }
                    Ok(None) => {
                        tracing::info!("no RoCE rail found; data path uses TCP");
                        None
                    }
                    Err(e) if mode == crate::config::FabricMode::Rdma => return Err(e.into()),
                    Err(e) => {
                        tracing::warn!(error = %e, "RDMA fabric unavailable; data path uses TCP");
                        None
                    }
                }
            }
        };
        let mountpoint = cfg
            .node
            .mountpoint
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned());
        let placer =
            nest_place::Placer::start(vfs.clone(), cfg.node.name.clone(), mountpoint.clone());
        let api = nest_api::Api {
            vfs: vfs.clone(),
            placer: placer.clone(),
            mountpoint,
            hub: "/hub".into(),
            web_token: nest_api::web_token(&secret_for_web),
            web_addr: cfg.node.api_listen,
            host: cfg.node.name.clone(),
        };
        let sock = cfg.api_socket();
        let web_addr = cfg.node.api_listen;
        let api_task = tokio::spawn(async move {
            let api_unix = api.clone();
            let unix = async move {
                if let Err(e) = nest_api::serve_unix(api_unix, sock.clone()).await {
                    tracing::error!(error = %e, socket = %sock.display(), "management socket stopped");
                }
            };
            let tcp = async move {
                if let Some(addr) = web_addr
                    && let Err(e) = nest_api::serve_tcp(api, addr).await
                {
                    tracing::error!(error = %e, %addr, "web UI listener stopped");
                }
            };
            tokio::join!(unix, tcp);
        });
        let node = Node {
            cfg,
            rpc,
            meta,
            data,
            vfs,
            fabric,
            placer,
            api_task: parking_lot::Mutex::new(Some(api_task)),
            mounted: parking_lot::Mutex::new(None),
            runstate: parking_lot::Mutex::new(runstate),
            prior,
            recovery,
        };
        if tuning.mount
            && let Some(mp) = node.cfg.node.mountpoint.clone()
        {
            node.mount_at(&mp)?;
        }
        Ok(node)
    }

    /// Mount the filesystem at `mountpoint` (replacing any current mount).
    /// FUSE over io_uring on this node's mount: (queues, requests served).
    pub fn io_uring(&self) -> Option<(usize, usize)> {
        self.mounted.lock().as_ref().map(|m| m.io_uring())
    }

    pub fn mount_at(&self, mountpoint: &std::path::Path) -> anyhow::Result<()> {
        let m = nest_fuse::mount(
            self.vfs.clone(),
            &nest_fuse::MountConfig {
                mountpoint: mountpoint.to_path_buf(),
                allow_other: self.cfg.fuse.allow_other,
                ttl: Duration::from_millis(self.cfg.fuse.ttl_ms),
                threads: 4,
                io_uring: self.cfg.fuse.io_uring,
            },
        )
        .with_context(|| format!("mounting at {}", mountpoint.display()))?;
        tracing::info!(mountpoint = %mountpoint.display(), "mounted");
        *self.mounted.lock() = Some(m);
        Ok(())
    }

    pub fn unmount(&self) {
        if let Some(m) = self.mounted.lock().take() {
            m.unmount();
        }
    }

    /// A host that crashed: wait until caught up with the cluster, so the
    /// metadata is complete, then settle the object store against it and
    /// start serving (ADR-026).
    async fn recover(
        cfg: &Config,
        data: &Arc<DataNode>,
        vfs: &Arc<Vfs>,
    ) -> anyhow::Result<nest_data::fsck::FsckReport> {
        let mut waited = 0u64;
        while !data.wait_caught_up(Duration::from_secs(10)).await {
            waited += 10;
            tracing::warn!(
                seconds = waited,
                "recovery: still waiting to reach the cluster"
            );
        }
        let report = vfs
            .fsck(&nest_data::fsck::FsckOptions {
                repair: true,
                orphans: nest_data::fsck::Orphans::Delete,
                deep: false,
                settle_owned: true,
                min_age: Duration::ZERO,
                host: cfg.node.name.clone(),
            })
            .await?;
        tracing::info!(
            objects = report.objects,
            findings = report.findings.len(),
            lost = report.lost(),
            "recovery fsck done; serving"
        );
        data.admit();
        Ok(report)
    }

    pub async fn shutdown(&self) {
        if let Some(t) = self.api_task.lock().take() {
            t.abort();
        }
        self.unmount();
        if let Some(f) = &self.fabric {
            f.shutdown();
        }
        self.data.shutdown();
        self.meta.shutdown().await;
        self.rpc.shutdown();
        if let Err(e) = self.runstate.lock().end_clean() {
            tracing::warn!(error = %e, "could not record a clean shutdown");
        }
    }
}
