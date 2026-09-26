//! Milestone 1 acceptance: replicated metadata and local replica lifecycle
//! on a seven-node in-process cluster.

use nest_meta::{Command, Reply, query};
use nest_store::ObjectKey;
use nest_testkit::TestCluster;
use nest_types::*;
use std::time::Duration;

fn now() -> Timestamp {
    Timestamp::now()
}

fn mkdir(name: &str) -> Command {
    Command::Mkdir {
        parent: FileId::ROOT,
        name: name.into(),
        perm: 0o755,
        now: now(),
    }
}

async fn create_file(c: &TestCluster, on: u64, name: &str, content: &[u8]) -> FileAttr {
    let n = c.node(on);
    let r = n
        .meta
        .propose(Command::Create {
            parent: FileId::ROOT,
            name: name.into(),
            perm: 0o644,
            node: NodeId(on),
            exclusive: true,
            now: now(),
        })
        .await
        .unwrap();
    let Reply::Created(cr) = r else {
        panic!("{r:?}")
    };
    let key = ObjectKey::new(cr.attr.id, cr.attr.generation);
    n.data.wait_ready(key).await;
    std::fs::write(n.data.store().path(key), content).unwrap();
    let r = n
        .meta
        .propose(Command::Finalize {
            file: cr.attr.id,
            epoch: cr.attr.epoch,
            size: content.len() as u64,
            mtime: now(),
            now: now(),
        })
        .await
        .unwrap();
    let Reply::Attr(a) = r else { panic!("{r:?}") };
    a
}

/// Simulate a completed replication to `to`: place the bytes and publish.
async fn replicate(c: &TestCluster, a: &FileAttr, from: u64, to: u64) {
    let key = ObjectKey::new(a.id, a.generation);
    let bytes = std::fs::read(c.node(from).data.store().path(key)).unwrap();
    let st = c.node(to).data.store().begin_staging(key).unwrap();
    std::os::unix::fs::FileExt::write_all_at(st.file(), &bytes, 0).unwrap();
    c.node(to).data.store().commit_staging(st).unwrap();
    c.node(to)
        .meta
        .propose(Command::PublishReplica {
            file: a.id,
            generation: a.generation,
            store: NodeId(to).live_store(),
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn seven_nodes_tolerate_any_three_down() {
    let mut c = TestCluster::start(7).await;
    let leader = c.wait_leader().await;
    // Take down the leader and two others, then three different ones.
    let rounds: Vec<Vec<u64>> = vec![vec![leader, leader % 7 + 1, (leader + 1) % 7 + 1], vec![]];
    let mut down = rounds[0].clone();
    down.sort();
    down.dedup();
    for id in &down {
        c.stop(*id).await;
    }
    let survivor = c.running()[0];
    c.node(survivor)
        .meta
        .propose(mkdir("round1"))
        .await
        .unwrap();
    for id in &down {
        c.restart(*id).await;
    }
    c.converge().await;
    for id in 1..=7 {
        assert!(c.lookup(id, FileId::ROOT, "round1").is_some(), "node {id}");
    }
    let others: Vec<u64> = (1..=7).filter(|i| !down.contains(i)).take(3).collect();
    for id in &others {
        c.stop(*id).await;
    }
    let survivor = c.running()[0];
    c.node(survivor)
        .meta
        .propose(mkdir("round2"))
        .await
        .unwrap();
    // A fourth failure loses quorum: writes must be refused, not faked.
    let fourth = c.running()[1];
    c.stop(fourth).await;
    let survivor = c.running()[0];
    // Let the leader notice it lost its quorum (lease > election timeout).
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        c.node(survivor).meta.propose(mkdir("no-quorum")).await,
        Err(NestError::NoQuorum)
    );
    // Reads of already-applied state keep working locally.
    assert!(c.lookup(survivor, FileId::ROOT, "round2").is_some());
    for id in others.iter().chain([fourth].iter()) {
        c.restart(*id).await;
    }
    c.converge().await;
    for id in 1..=7 {
        assert!(c.lookup(id, FileId::ROOT, "round2").is_some(), "node {id}");
        assert!(
            c.lookup(id, FileId::ROOT, "no-quorum").is_none(),
            "node {id}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn invalidation_deletes_stale_replicas_immediately() {
    let c = TestCluster::start(3).await;
    let a = create_file(&c, 2, "model.safetensors", b"weights-v1").await;
    replicate(&c, &a, 2, 3).await;
    let old = ObjectKey::new(a.id, a.generation);
    assert!(c.node(3).data.store().exists(old));

    // Node 2 (holding a copy) takes ownership for an in-place edit.
    let r = c
        .node(2)
        .meta
        .propose(Command::AcquireOwner {
            file: a.id,
            node: NodeId(2),
            expect_gen: a.generation,
            truncate: false,
            now: now(),
        })
        .await
        .unwrap();
    let Reply::Acquired { attr, from_gen } = r else {
        panic!("{r:?}")
    };
    assert_eq!(from_gen, Some(a.generation));
    let new = ObjectKey::new(a.id, attr.generation);

    // Node 3's copy is fenced and deleted without any job or reconcile pass.
    c.eventually(
        "stale replica deleted on node 3",
        Duration::from_secs(2),
        |c| !c.node(3).data.store().exists(old),
    )
    .await;
    // Node 2 converted its copy in place: same bytes, new generation.
    c.node(2).data.wait_ready(new).await;
    assert_eq!(
        std::fs::read(c.node(2).data.store().path(new)).unwrap(),
        b"weights-v1"
    );
    assert!(!c.node(2).data.store().exists(old));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn unlink_while_open_elsewhere_keeps_data_until_close() {
    let c = TestCluster::start(3).await;
    // Wait for every node to hold a session.
    c.eventually("sessions open", Duration::from_secs(5), |c| {
        (1..=3).all(|i| c.node(i).data.session().is_some())
    })
    .await;
    let a = create_file(&c, 1, "shard-00001", b"tensor bytes").await;
    let key = ObjectKey::new(a.id, a.generation);
    c.converge().await;

    // Node 3 has the file open; node 2 removes the name.
    c.node(3).data.handle_opened(a.id);
    c.node(2)
        .meta
        .propose(Command::Unlink {
            parent: FileId::ROOT,
            name: "shard-00001".into(),
            now: now(),
        })
        .await
        .unwrap();
    c.converge().await;
    assert!(c.lookup(1, FileId::ROOT, "shard-00001").is_none());
    // Nodes without handles release promptly; node 3 still holds it.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        c.attr(1, a.id).is_some(),
        "file object must survive while open on node 3"
    );
    assert!(c.node(1).data.store().exists(key));

    c.node(3).data.handle_closed(a.id);
    c.eventually(
        "orphan deleted after last close",
        Duration::from_secs(3),
        |c| c.attr(1, a.id).is_none() && !c.node(1).data.store().exists(key),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn restarted_node_reconciles_before_serving() {
    let mut c = TestCluster::start(3).await;
    let a = create_file(&c, 1, "cfg.json", b"{}").await;
    replicate(&c, &a, 1, 3).await;
    let old = ObjectKey::new(a.id, a.generation);
    c.converge().await;

    // Node 3 is down while the file is rewritten elsewhere.
    c.stop(3).await;
    c.node(1)
        .meta
        .propose(Command::AcquireOwner {
            file: a.id,
            node: NodeId(1),
            expect_gen: a.generation,
            truncate: true,
            now: now(),
        })
        .await
        .unwrap();
    // Junk left in node 3's store from some crashed operation.
    let junk = ObjectKey::new(FileId(999_999), Generation(1));
    let store3 = nest_store::ObjectStore::open(&c.state_dir(3)).unwrap();
    store3.create(junk).unwrap();
    assert!(store3.exists(old));
    drop(store3);

    c.restart(3).await;
    // Reconciliation removed junk before start() returned; the stale copy is
    // never servable, and goes as soon as the invalidation is caught up.
    assert!(!c.node(3).data.store().exists(junk));
    assert!(!c.node(3).data.servable(old));
    c.eventually("caught up", Duration::from_secs(3), |c| {
        c.node(3).data.caught_up()
    })
    .await;
    assert!(!c.node(3).data.servable(old));
    c.eventually("stale replica deleted", Duration::from_secs(3), |c| {
        !c.node(3).data.store().exists(old)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn wiped_node_rejoins_from_snapshot_and_reports_lost_replicas() {
    let mut tuning = nest_testkit::fast_tuning();
    tuning.snapshot_every = 50;
    let mut c = TestCluster::start_with(3, tuning).await;
    let a = create_file(&c, 1, "big.bin", b"0123456789").await;
    replicate(&c, &a, 1, 3).await;
    c.converge().await;
    c.stop(3).await;
    c.wipe(3);
    let leader = c.wait_leader().await;
    for i in 0..120 {
        c.node(leader)
            .meta
            .propose(mkdir(&format!("d{i}")))
            .await
            .unwrap();
    }
    c.node(leader)
        .meta
        .raft()
        .trigger()
        .snapshot()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let last = c.node(leader).meta.applied_index();
    c.node(leader)
        .meta
        .raft()
        .trigger()
        .purge_log(last)
        .await
        .unwrap();

    c.restart(3).await;
    c.converge().await;
    assert!(c.lookup(3, FileId::ROOT, "d119").is_some());
    // Metadata said node 3 held a copy; its disk is empty, so that copy is
    // retired rather than advertised.
    c.eventually("lost replica retired", Duration::from_secs(5), |c| {
        let r = c.node(1).meta.open_reader().unwrap();
        let stores: Vec<u64> = query::replicas(&r, a.id)
            .unwrap()
            .iter()
            .map(|r| r.store.0)
            .collect();
        stores == vec![1]
    })
    .await;
}
