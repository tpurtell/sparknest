use nest_meta::{Command, Effect, Reply, query};
use nest_raft::{MetaNode, MetaNodeConfig, SmEvent};
use nest_rpc::{Rpc, RpcConfig};
use nest_types::*;
use openraft::BasicNode;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
struct Events(Mutex<Vec<SmEvent>>);

impl Events {
    fn effects(&self) -> Vec<Effect> {
        self.0
            .lock()
            .iter()
            .flat_map(|e| match e {
                SmEvent::Applied { effects, .. } => effects.clone(),
                SmEvent::Resync { .. } => vec![],
            })
            .collect()
    }
    fn resyncs(&self) -> usize {
        self.0
            .lock()
            .iter()
            .filter(|e| matches!(e, SmEvent::Resync { .. }))
            .count()
    }
}

struct TestNode {
    meta: Arc<MetaNode>,
    rpc: Rpc,
    events: Arc<Events>,
}

struct Cluster {
    dir: tempfile::TempDir,
    addrs: BTreeMap<u64, SocketAddr>,
    nodes: BTreeMap<u64, TestNode>,
    snapshot_every: u64,
}

impl Cluster {
    async fn new(n: u64, snapshot_every: u64) -> Cluster {
        let mut c = Cluster {
            dir: tempfile::tempdir().unwrap(),
            addrs: BTreeMap::new(),
            nodes: BTreeMap::new(),
            snapshot_every,
        };
        for id in 1..=n {
            c.start(id, "127.0.0.1:0".parse().unwrap()).await;
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
        assert!(c.nodes[&1].meta.bootstrap(members).await.unwrap());
        c.wait_leader().await;
        c
    }

    fn path(&self, id: u64) -> PathBuf {
        self.dir.path().join(format!("n{id}"))
    }

    async fn start(&mut self, id: u64, listen: SocketAddr) {
        let rpc = Rpc::bind(RpcConfig {
            node: NodeId(id),
            cluster: "test".into(),
            secret: b"secret".to_vec(),
            listen,
            connect_timeout: Duration::from_millis(500),
        })
        .await
        .unwrap();
        self.addrs.insert(id, rpc.local_addr());
        let events = Arc::new(Events::default());
        let ev = events.clone();
        let mut cfg = MetaNodeConfig::new(NodeId(id), self.path(id), "test");
        cfg.heartbeat_ms = 50;
        cfg.election_min_ms = 200;
        cfg.election_max_ms = 400;
        cfg.snapshot_every = self.snapshot_every;
        cfg.propose_deadline = Duration::from_secs(4);
        let meta = MetaNode::start(
            cfg,
            rpc.clone(),
            Arc::new(move |e: &SmEvent| ev.0.lock().push(e.clone())),
        )
        .await
        .unwrap();
        self.nodes.insert(id, TestNode { meta, rpc, events });
    }

    async fn stop(&mut self, id: u64) {
        let n = self.nodes.remove(&id).unwrap();
        n.meta.shutdown().await;
        n.rpc.shutdown();
    }

    async fn restart(&mut self, id: u64) {
        let addr = self.addrs[&id];
        self.start(id, addr).await;
    }

    async fn wait_leader(&self) -> u64 {
        for _ in 0..200 {
            for n in self.nodes.values() {
                if let Some(l) = n.meta.leader()
                    && self.nodes.contains_key(&l.0)
                {
                    return l.0;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("no leader elected");
    }

    fn lookup(&self, id: u64, parent: FileId, name: &str) -> Option<FileId> {
        let c = self.nodes[&id].meta.open_reader().unwrap();
        query::lookup(&c, parent, name.as_bytes()).unwrap()
    }

    async fn converge(&self) {
        let target = self
            .nodes
            .values()
            .map(|n| n.meta.applied_index())
            .max()
            .unwrap();
        for _ in 0..200 {
            if self
                .nodes
                .values()
                .all(|n| n.meta.applied_index() >= target)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("nodes did not converge to index {target}");
    }
}

fn mkdir(name: &str) -> Command {
    Command::Mkdir {
        parent: FileId::ROOT,
        name: name.into(),
        perm: 0o755,
        now: Timestamp::now(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarding_read_your_writes_failover() {
    let mut c = Cluster::new(3, 100_000).await;
    let leader = c.wait_leader().await;
    let follower = (1..=3).find(|i| *i != leader).unwrap();

    // A follower proposes: forwarded to the leader, visible locally on return.
    let r = c.nodes[&follower].meta.propose(mkdir("hub")).await.unwrap();
    let Reply::Attr(a) = r else { panic!("{r:?}") };
    assert_eq!(c.lookup(follower, FileId::ROOT, "hub"), Some(a.id));
    // Failures come back as outcomes, not transport errors.
    assert_eq!(
        c.nodes[&follower].meta.propose(mkdir("hub")).await,
        Err(NestError::Exists)
    );

    // The barrier makes another node's view current.
    let other = (1..=3).find(|i| *i != follower && *i != leader).unwrap();
    c.nodes[&other].meta.barrier().await.unwrap();
    assert_eq!(c.lookup(other, FileId::ROOT, "hub"), Some(a.id));

    // Kill the leader: the survivors elect a new one and keep going.
    c.stop(leader).await;
    let new_leader = c.wait_leader().await;
    assert_ne!(new_leader, leader);
    c.nodes[&follower]
        .meta
        .propose(mkdir("after-failover"))
        .await
        .unwrap();

    // The old leader comes back and catches up from the log.
    c.restart(leader).await;
    c.converge().await;
    assert!(c.lookup(leader, FileId::ROOT, "after-failover").is_some());
    let dumps: Vec<_> = c
        .nodes
        .values()
        .map(|n| {
            let r = n.meta.open_reader().unwrap();
            query::readdir(&r, FileId::ROOT, 0, 100).unwrap()
        })
        .collect();
    assert!(dumps.windows(2).all(|w| w[0] == w[1]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minority_cannot_write() {
    let mut c = Cluster::new(3, 100_000).await;
    let leader = c.wait_leader().await;
    let others: Vec<u64> = (1..=3).filter(|i| *i != leader).collect();
    c.stop(others[0]).await;
    c.stop(others[1]).await;
    let meta = c.nodes[&leader].meta.clone();
    let mut cfg_deadline = tokio::time::Instant::now();
    cfg_deadline += Duration::from_secs(20);
    let r = tokio::time::timeout_at(cfg_deadline, meta.propose(mkdir("lonely")))
        .await
        .unwrap();
    assert_eq!(r, Err(NestError::NoQuorum));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_catch_up_emits_resync() {
    let mut c = Cluster::new(3, 40).await;
    // Node 3 goes away and loses its disk.
    c.stop(3).await;
    std::fs::remove_dir_all(c.path(3)).unwrap();
    let leader = c.wait_leader().await;
    for i in 0..150 {
        c.nodes[&leader]
            .meta
            .propose(mkdir(&format!("d{i}")))
            .await
            .unwrap();
    }
    // Make sure the log has been compacted past what node 3 would need.
    c.nodes[&leader]
        .meta
        .raft()
        .trigger()
        .snapshot()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    c.nodes[&leader]
        .meta
        .raft()
        .trigger()
        .purge_log(140)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    c.restart(3).await;
    c.converge().await;
    assert!(
        c.nodes[&3].events.resyncs() >= 1,
        "wiped node should install a snapshot"
    );
    assert!(c.lookup(3, FileId::ROOT, "d149").is_some());
    assert!(c.lookup(3, FileId::ROOT, "d0").is_some());
    // And it keeps applying normally afterwards.
    c.nodes[&3]
        .meta
        .propose(mkdir("post-snapshot"))
        .await
        .unwrap();
    assert!(c.lookup(3, FileId::ROOT, "post-snapshot").is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_requests_apply_once() {
    let c = Cluster::new(3, 100_000).await;
    let leader = c.wait_leader().await;
    let raft = c.nodes[&leader].meta.raft().clone();
    let req = nest_raft::Request {
        client: 42,
        seq: 7,
        cmd: mkdir("once"),
    };
    let a = raft.client_write(req.clone()).await.unwrap().data;
    let b = raft.client_write(req).await.unwrap().data;
    assert_eq!(a, b);
    assert!(a.0.is_ok());
    c.converge().await;
    // Exactly one EntryChanged for "once" on every node.
    for n in c.nodes.values() {
        let count = n
            .events
            .effects()
            .iter()
            .filter(|e| matches!(e, Effect::EntryChanged { name, .. } if name == b"once"))
            .count();
        assert_eq!(count, 1);
    }
}
