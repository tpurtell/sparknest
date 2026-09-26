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
    mounted: parking_lot::Mutex<Option<nest_fuse::Mounted>>,
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
        let store =
            Arc::new(ObjectStore::open(&cfg.node.state_dir).context("opening object store")?);
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
        let report = data.attach(meta.clone()).await?;
        tracing::info!(?report, "local store reconciled");
        let vfs = Vfs::new(data.clone(), tuning.vfs.clone());
        let node = Node {
            cfg,
            rpc,
            meta,
            data,
            vfs,
            mounted: parking_lot::Mutex::new(None),
        };
        if tuning.mount
            && let Some(mp) = node.cfg.node.mountpoint.clone()
        {
            node.mount_at(&mp)?;
        }
        Ok(node)
    }

    /// Mount the filesystem at `mountpoint` (replacing any current mount).
    pub fn mount_at(&self, mountpoint: &std::path::Path) -> anyhow::Result<()> {
        let m = nest_fuse::mount(
            self.vfs.clone(),
            &nest_fuse::MountConfig {
                mountpoint: mountpoint.to_path_buf(),
                allow_other: self.cfg.fuse.allow_other,
                ttl: Duration::from_millis(self.cfg.fuse.ttl_ms),
                threads: 4,
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

    pub async fn shutdown(&self) {
        self.unmount();
        self.data.shutdown();
        self.meta.shutdown().await;
        self.rpc.shutdown();
    }
}
