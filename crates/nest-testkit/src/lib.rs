//! In-process multi-node clusters for tests: real nodes (RPC over loopback
//! TCP, Raft, SQLite, object stores in temp directories) with helpers to
//! stop, restart, wipe and partition them.

use nest_types::{FileId, NodeId};
use openraft::BasicNode;
use sparknestd::config::{ClusterSection, Config, FabricSection, FuseSection, Member, NodeSection};
use sparknestd::{Node, Tuning};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

pub use sparknestd;

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();
}

/// Timing suitable for tests: fast elections, short deadlines.
pub fn fast_tuning() -> Tuning {
    Tuning {
        heartbeat_ms: 50,
        election_min_ms: 200,
        election_max_ms: 400,
        snapshot_every: 100_000,
        propose_deadline: Duration::from_secs(5),
        connect_timeout: Duration::from_millis(300),
        vfs: nest_data::VfsConfig {
            finalize_linger: Duration::from_millis(50),
            catch_up_wait: Duration::from_secs(5),
            rpc_timeout: Duration::from_secs(3),
            readahead_chunks: 8,
        },
        lease: Duration::from_millis(800),
        mount: false,
        fabric: None,
        boot_id: None,
        recovery_grace: Duration::from_millis(500),
    }
}

pub struct TestCluster {
    dir: tempfile::TempDir,
    pub tuning: Tuning,
    addrs: BTreeMap<u64, SocketAddr>,
    nodes: BTreeMap<u64, Node>,
    /// Simulated kernel boot id per host (see [`TestCluster::power_loss`]).
    boots: BTreeMap<u64, u64>,
}

impl TestCluster {
    /// Start `n` voting nodes (ids 1..=n) and bootstrap the cluster.
    pub async fn start(n: u64) -> TestCluster {
        Self::start_with(n, fast_tuning()).await
    }

    pub async fn start_with(n: u64, tuning: Tuning) -> TestCluster {
        init_tracing();
        let mut c = TestCluster {
            dir: tempfile::tempdir().unwrap(),
            tuning,
            addrs: BTreeMap::new(),
            nodes: BTreeMap::new(),
            boots: BTreeMap::new(),
        };
        for id in 1..=n {
            c.boot(id, "127.0.0.1:0".parse().unwrap()).await;
        }
        let members: BTreeMap<u64, BasicNode> = c
            .addrs
            .iter()
            .map(|(id, a)| {
                (
                    *id,
                    BasicNode {
                        addr: a.to_string(),
                    },
                )
            })
            .collect();
        for (id, a) in &c.addrs {
            for n in c.nodes.values() {
                n.rpc.set_peer(NodeId(*id), *a);
            }
        }
        c.nodes[&1].meta.bootstrap(members).await.unwrap();
        c.wait_leader().await;
        c
    }

    pub fn state_dir(&self, id: u64) -> PathBuf {
        self.dir.path().join(format!("node{id}"))
    }

    fn config(&self, id: u64, listen: SocketAddr) -> Config {
        Config {
            node: NodeSection {
                data_reserve_gib: 0,
                id: NodeId(id),
                name: format!("n{id}"),
                state_dir: self.state_dir(id),
                mountpoint: None,
                listen,
                api_listen: None,
            },
            cluster: ClusterSection {
                name: "testkit".into(),
                secret_file: self.dir.path().join("secret"),
                members: self
                    .addrs
                    .iter()
                    .map(|(i, a)| Member {
                        id: NodeId(*i),
                        name: format!("n{i}"),
                        addr: *a,
                        voter: true,
                    })
                    .collect(),
            },
            fabric: FabricSection::default(),
            fuse: FuseSection {
                allow_other: false,
                ttl_ms: 1000,
                io_uring: true,
                allow_nonempty: false,
                rescue_unmounted: true,
            },
            hf: Default::default(),
        }
    }

    async fn boot(&mut self, id: u64, listen: SocketAddr) {
        let cfg = self.config(id, listen);
        let mut tuning = self.tuning.clone();
        tuning.boot_id = Some(format!("test-boot-{}", self.boots.entry(id).or_insert(0)));
        let node = start_node(cfg, tuning).await;
        self.addrs.insert(id, node.rpc.local_addr());
        self.nodes.insert(id, node);
    }

    /// Mount node `id` at a fresh directory and return its path. Returns
    /// `None` when FUSE is unavailable here (no /dev/fuse).
    pub fn mount(&self, id: u64) -> Option<PathBuf> {
        if !std::path::Path::new("/dev/fuse").exists() {
            return None;
        }
        let mp = self.dir.path().join(format!("mnt{id}"));
        std::fs::create_dir_all(&mp).unwrap();
        self.nodes[&id].mount_at(&mp).unwrap();
        Some(mp)
    }

    pub fn node(&self, id: u64) -> &Node {
        &self.nodes[&id]
    }

    pub fn running(&self) -> Vec<u64> {
        self.nodes.keys().copied().collect()
    }

    pub async fn stop(&mut self, id: u64) {
        let n = self.nodes.remove(&id).expect("node running");
        n.shutdown().await;
    }

    /// Start several stopped nodes at once (after a full outage each one's
    /// start waits for the others).
    pub async fn restart_many(&mut self, ids: &[u64]) {
        let starts: Vec<_> = ids
            .iter()
            .map(|&id| {
                let cfg = self.config(id, self.addrs[&id]);
                let mut tuning = self.tuning.clone();
                tuning.boot_id = Some(format!("test-boot-{}", self.boots.entry(id).or_insert(0)));
                async move { (id, start_node(cfg, tuning).await) }
            })
            .collect();
        for (id, n) in futures::future::join_all(starts).await {
            self.addrs.insert(id, n.rpc.local_addr());
            self.nodes.insert(id, n);
        }
    }

    /// Node `id`'s configuration (as its daemon would load it).
    pub fn node_config(&self, id: u64) -> Config {
        self.config(id, self.addrs[&id])
    }

    /// Start node `id` on a throwaway address without registering it, to
    /// watch whether it would come up (e.g. under a timeout). Abandoning the
    /// future leaves nothing on the node's real address.
    pub fn start_detached(&mut self, id: u64) -> impl std::future::Future<Output = Node> + use<> {
        let cfg = self.config(id, "127.0.0.1:0".parse().unwrap());
        let mut tuning = self.tuning.clone();
        tuning.boot_id = Some(format!("test-boot-{}", self.boots.entry(id).or_insert(0)));
        async move {
            Node::start(cfg, b"testkit-secret".to_vec(), tuning, false)
                .await
                .unwrap()
        }
    }

    /// Restart a stopped node on its original address.
    pub async fn restart(&mut self, id: u64) {
        let addr = self.addrs[&id];
        self.boot(id, addr).await;
    }

    /// Copy a stopped node's state directory: what its disk holds at this
    /// point (see [`TestCluster::power_loss`]).
    pub fn save_disk(&self, id: u64) -> PathBuf {
        assert!(!self.nodes.contains_key(&id), "stop the node first");
        let to = self
            .dir
            .path()
            .join(format!("saved-node{id}-{}", rand_suffix()));
        copy_dir(&self.state_dir(id), &to);
        to
    }

    /// Simulate the host losing power: the node stops without an orderly
    /// shutdown, and with `durable` its disk goes back to that saved copy
    /// (everything written since was only in memory). The next start sees a
    /// new boot id, i.e. a host that crashed.
    pub async fn power_loss(&mut self, id: u64, durable: Option<&std::path::Path>) {
        if self.nodes.contains_key(&id) {
            self.stop(id).await;
        }
        let dir = self.state_dir(id);
        if let Some(saved) = durable {
            let _ = std::fs::remove_dir_all(&dir);
            copy_dir(saved, &dir);
        }
        let boot = self.boots.entry(id).or_insert(0);
        let marker = serde_json::json!({ "boot_id": format!("test-boot-{boot}"), "clean": false });
        std::fs::write(dir.join("run.json"), marker.to_string()).unwrap();
        *boot += 1;
    }

    /// Delete a stopped node's entire state directory (disk loss).
    pub fn wipe(&self, id: u64) {
        assert!(!self.nodes.contains_key(&id), "stop the node first");
        let _ = std::fs::remove_dir_all(self.state_dir(id));
    }

    /// Cut the links between two groups of running nodes in both directions.
    pub fn partition(&self, a: &[u64], b: &[u64]) {
        for x in a {
            for y in b {
                if let Some(n) = self.nodes.get(x) {
                    n.rpc.filter().block(NodeId(*y));
                }
                if let Some(n) = self.nodes.get(y) {
                    n.rpc.filter().block(NodeId(*x));
                }
            }
        }
    }

    pub fn heal(&self) {
        for n in self.nodes.values() {
            n.rpc.filter().unblock_all();
        }
    }

    pub async fn wait_leader(&self) -> u64 {
        for _ in 0..400 {
            for n in self.nodes.values() {
                if let Some(l) = n.meta.leader()
                    && self.nodes.contains_key(&l.0)
                {
                    return l.0;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("no leader among running nodes");
    }

    /// Wait until every running node has applied everything any of them has.
    pub async fn converge(&self) {
        let target = self
            .nodes
            .values()
            .map(|n| n.meta.applied_index())
            .max()
            .unwrap_or(0);
        for _ in 0..400 {
            if self
                .nodes
                .values()
                .all(|n| n.meta.applied_index() >= target)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("nodes did not converge on index {target}");
    }

    /// Poll `f` until it returns true or the timeout passes.
    pub async fn eventually(
        &self,
        what: &str,
        timeout: Duration,
        mut f: impl FnMut(&Self) -> bool,
    ) {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            if f(self) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for: {what}");
    }

    pub fn lookup(&self, id: u64, parent: FileId, name: &str) -> Option<FileId> {
        let c = self.nodes[&id].meta.open_reader().unwrap();
        nest_meta::query::lookup(&c, parent, name.as_bytes()).unwrap()
    }

    pub fn attr(&self, id: u64, file: FileId) -> Option<nest_types::FileAttr> {
        let c = self.nodes[&id].meta.open_reader().unwrap();
        nest_meta::query::getattr(&c, file).unwrap()
    }
}

fn rand_suffix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap().flatten() {
        let p = e.path();
        let dest = to.join(e.file_name());
        if p.is_dir() {
            copy_dir(&p, &dest);
        } else if p.is_file() {
            std::fs::copy(&p, &dest).unwrap();
        }
    }
}

/// Start a node. A restarted node binds its old port (peers know it), and
/// in a busy test run another test's outgoing connection may hold that
/// ephemeral port for a moment: retry the bind rather than fail the test.
async fn start_node(cfg: Config, tuning: Tuning) -> Node {
    let mut tries = 0;
    loop {
        match Node::start(
            cfg.clone(),
            b"testkit-secret".to_vec(),
            tuning.clone(),
            false,
        )
        .await
        {
            Ok(n) => return n,
            Err(e) if tries < 100 && format!("{e:#}").contains("Address already in use") => {
                tries += 1;
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(e) => panic!("starting node: {e:#}"),
        }
    }
}
