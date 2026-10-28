//! Off-grid hosts (ADR-047): learners on the LAN only, which the fabric
//! hosts have no address for. Everything they exchange travels over the
//! connections the off-grid host opens.

use nest_data::vfs::oflags;
use nest_meta::Command;
use nest_store::ObjectKey;
use nest_testkit::TestCluster;
use nest_types::*;
use std::time::Duration;

const RO: i32 = 0;

async fn ready(n: u64) -> TestCluster {
    let c = TestCluster::start(n).await;
    c.eventually("all nodes serving", Duration::from_secs(8), |c| {
        (1..=n).all(|i| c.node(i).data.caught_up() && c.node(i).data.session().is_some())
    })
    .await;
    c
}

async fn with_offgrid(n: u64, id: u64) -> TestCluster {
    let mut c = ready(n).await;
    c.add_offgrid(id).await;
    c.eventually("off-grid host serving", Duration::from_secs(8), |c| {
        c.node(id).data.caught_up() && c.node(id).data.session().is_some()
    })
    .await;
    // Its store (and so its name) registered, as node 1 sees it.
    let name = format!("n{id}");
    c.eventually("off-grid host named", Duration::from_secs(8), |c| {
        c.node(1)
            .placer
            .nodes()
            .is_ok_and(|v| v.iter().any(|h| h.name == name))
    })
    .await;
    c
}

async fn read_all(c: &TestCluster, node: u64, file: FileId) -> Vec<u8> {
    let v = &c.node(node).vfs;
    let (fh, _) = v.open(file, RO).await.unwrap();
    let mut out = Vec::new();
    loop {
        let b = v.read(fh, out.len() as u64, 1 << 20).await.unwrap();
        if b.is_empty() {
            break;
        }
        out.extend_from_slice(&b);
    }
    v.release(fh, None).await;
    out
}

async fn file(c: &TestCluster, node: u64, name: &str, content: &[u8]) -> FileId {
    let v = &c.node(node).vfs;
    let (a, fh, _) = v
        .create(
            FileId::ROOT,
            name.as_bytes(),
            0o644,
            oflags::WRONLY | oflags::EXCL,
        )
        .await
        .unwrap();
    v.write(fh, 0, content.to_vec()).await.unwrap();
    v.release(fh, None).await;
    c.eventually("file stable", Duration::from_secs(5), |c| {
        c.attr(node, a.id)
            .is_some_and(|a| a.gen_state == GenState::Stable)
    })
    .await;
    c.converge().await;
    a.id
}

/// Copy the current generation from `from` to `to` and publish it.
async fn replicate(c: &TestCluster, f: FileId, from: u64, to: u64) {
    let a = c.attr(from, f).unwrap();
    let key = ObjectKey::new(f, a.generation);
    let bytes = std::fs::read(c.node(from).data.store().path(key)).unwrap();
    let st = c.node(to).data.store().begin_staging(key).unwrap();
    std::os::unix::fs::FileExt::write_all_at(st.file(), &bytes, 0).unwrap();
    c.node(to).data.store().commit_staging(st).unwrap();
    c.node(to)
        .meta
        .propose(Command::PublishReplica {
            file: f,
            generation: a.generation,
            store: NodeId(to).live_store(),
        })
        .await
        .unwrap();
    c.converge().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_offgrid_host_joins_as_a_learner_reads_writes_and_owns() {
    let c = with_offgrid(3, 4).await;
    // A learner, and the fabric hosts cannot dial it.
    let m = c.node(1).meta.metrics().membership_config;
    assert!(m.nodes().any(|(id, _)| *id == 4));
    assert!(!m.membership().voter_ids().any(|v| v == 4));
    for i in 1..=3 {
        assert_eq!(c.node(i).rpc.peer_addr(NodeId(4)), None);
        assert_eq!(c.node(i).vfs.offgrid_hosts(), [NodeId(4)].into());
    }

    // It reads what a fabric host wrote...
    let f = file(&c, 1, "from-fabric", b"weights from raptor").await;
    assert_eq!(read_all(&c, 4, f).await, b"weights from raptor");
    // ...writes a file it owns, which a fabric host reads from it...
    let g = file(&c, 4, "from-offgrid", b"written on kakapo").await;
    let a = c.attr(1, g).unwrap();
    assert!(
        c.node(1)
            .meta
            .open_reader()
            .map(|r| nest_meta::query::replicas(&r, g).unwrap())
            .unwrap()
            .iter()
            .any(|r| r.store == NodeId(4).live_store() && r.generation == a.generation)
    );
    assert_eq!(read_all(&c, 2, g).await, b"written on kakapo");
    // ...and rewrites a fabric host's file (fenced across the cluster).
    let v4 = &c.node(4).vfs;
    let (fh, _) = v4.open(f, oflags::RDWR).await.unwrap();
    v4.write(fh, 0, b"WEIGHTS".to_vec()).await.unwrap();
    v4.release(fh, None).await;
    c.converge().await;
    assert_eq!(read_all(&c, 3, f).await, b"WEIGHTS from raptor");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_offgrid_host_does_not_count_toward_quorum() {
    let mut c = with_offgrid(3, 4).await;
    c.stop(2).await;
    c.stop(3).await;
    // One voter of three is left, plus the learner: nothing commits.
    let v4 = &c.node(4).vfs;
    let r = tokio::time::timeout(
        Duration::from_secs(10),
        v4.create(FileId::ROOT, b"no-quorum", 0o644, oflags::WRONLY),
    )
    .await;
    assert!(
        !matches!(r, Ok(Ok(_))),
        "a write committed with one voter of three and a learner"
    );
    c.restart_many(&[2, 3]).await;
    c.eventually("quorum back", Duration::from_secs(15), |c| {
        c.node(4).data.caught_up() && c.node(1).data.caught_up()
    })
    .await;
    file(&c, 4, "with-quorum", b"ok").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn reads_prefer_the_offgrid_hosts_own_copy_and_fabric_holders_elsewhere() {
    let c = with_offgrid(3, 4).await;
    let f = file(&c, 1, "model", &vec![7u8; 256 << 10]).await;
    replicate(&c, f, 1, 2).await;
    replicate(&c, f, 1, 4).await;

    // The off-grid host reads its own copy although two others exist.
    let before = c.node(4).vfs.local_read_bytes();
    assert_eq!(read_all(&c, 4, f).await.len(), 256 << 10);
    assert_eq!(c.node(4).vfs.local_read_bytes() - before, 256 << 10);

    // A fabric host without a copy reads from fabric holders first.
    let order = c.node(3).vfs.read_order(f).unwrap();
    assert_eq!(order.len(), 3, "{order:?}");
    assert_eq!(order.last(), Some(&NodeId(4)), "{order:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_offgrid_host_rejoins_after_a_fabric_restart_and_its_own() {
    let mut c = with_offgrid(3, 4).await;
    let f = file(&c, 1, "a", b"one").await;
    assert_eq!(read_all(&c, 4, f).await, b"one");
    // The leader's host restarts: the off-grid host dials in again.
    let leader = c.wait_leader().await;
    c.stop(leader).await;
    c.restart(leader).await;
    let g = file(&c, leader, "b", b"two").await;
    c.eventually("off-grid host caught up", Duration::from_secs(10), |c| {
        c.lookup(4, FileId::ROOT, "b").is_some()
    })
    .await;
    assert_eq!(read_all(&c, 4, g).await, b"two");
    // The off-grid host restarts: it waits for the cluster and catches up.
    c.stop(4).await;
    c.restart(4).await;
    c.eventually(
        "off-grid host serving again",
        Duration::from_secs(10),
        |c| c.node(4).data.caught_up() && c.node(4).data.session().is_some(),
    )
    .await;
    let h = file(&c, 2, "c", b"three").await;
    c.eventually("sees new files", Duration::from_secs(10), |c| {
        c.lookup(4, FileId::ROOT, "c").is_some()
    })
    .await;
    assert_eq!(read_all(&c, 4, h).await, b"three");
}

fn lane_read(c: &TestCluster, id: u64) -> u64 {
    c.node(id).vfs.lanes().map_or(0, |l| {
        l.stats
            .read_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    })
}

/// Bulk data to and from an off-grid host travels over the TCP lanes it
/// dialled (ADR-048): reads both ways and a whole-file copy onto it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn offgrid_data_goes_over_tcp_lanes_both_ways() {
    let c = with_offgrid(3, 4).await;
    c.eventually("lanes up", Duration::from_secs(5), |c| {
        let l = c.node(4).vfs.lanes().unwrap();
        (1..=3).all(|i| l.connected(NodeId(i)) >= 2)
    })
    .await;
    let big: Vec<u8> = (0..(9 << 20) + 777).map(|i| (i % 251) as u8).collect();
    let f = file(&c, 1, "big", &big).await;

    // Read from a fabric host: over lanes, in whole chunks.
    let before = lane_read(&c, 4);
    assert_eq!(read_all(&c, 4, f).await, big);
    assert!(
        lane_read(&c, 4) - before >= big.len() as u64,
        "not read over lanes"
    );

    // Copied onto the off-grid host (onboarding): over lanes too.
    let before = lane_read(&c, 4);
    c.node(4).vfs.replicate_here(f).await.unwrap();
    c.converge().await;
    assert!(
        lane_read(&c, 4) - before >= big.len() as u64,
        "copy not over lanes"
    );
    let local = c.node(4).vfs.local_read_bytes();
    assert_eq!(read_all(&c, 4, f).await, big);
    assert_eq!(c.node(4).vfs.local_read_bytes() - local, big.len() as u64);

    // A fabric host reads what only the off-grid host holds, over the
    // lanes the off-grid host opened.
    let g = file(&c, 4, "offgrid-only", &big[..3 << 20]).await;
    let before = lane_read(&c, 2);
    assert_eq!(read_all(&c, 2, g).await, &big[..3 << 20]);
    assert!(
        lane_read(&c, 2) - before >= 3 << 20,
        "fabric host did not use lanes"
    );
}

async fn wait_job(c: &TestCluster, id: u64) -> nest_place::placer::ClusterJob {
    c.eventually("job finished", Duration::from_secs(30), |c| {
        c.node(1).placer.job(id).is_some_and(|j| j.finished)
    })
    .await;
    c.node(1).placer.job(id).unwrap()
}

fn holders(c: &TestCluster, f: FileId) -> Vec<u64> {
    let r = c.node(1).meta.open_reader().unwrap();
    let mut v: Vec<u64> = nest_meta::query::replicas(&r, f)
        .unwrap()
        .iter()
        .map(|r| r.store.0)
        .collect();
    v.sort();
    v
}

/// The Off-grid mode puts full copies on the off-grid hosts picked and
/// removes them from the others; Place never counts, uses or removes them;
/// offloading to an off-grid host drops the fabric hosts' copies, which is
/// not a deletion.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn offgrid_placement_place_and_offload() {
    use nest_place::plan::{Goal, Step};
    let c = with_offgrid(3, 4).await;
    let p = c.node(1).placer.clone();
    let v = c.node(1).vfs.clone();
    let hosts = p.nodes().unwrap();
    assert_eq!(
        hosts
            .iter()
            .filter(|h| h.offgrid)
            .map(|h| h.name.as_str())
            .collect::<Vec<_>>(),
        ["n4"]
    );
    let dir = v.mkdir(FileId::ROOT, b"m", 0o755).await.unwrap().id;
    let mut files = Vec::new();
    for i in 0..3u8 {
        let (a, fh, _) = v
            .create(dir, format!("part{i}").as_bytes(), 0o644, oflags::WRONLY)
            .await
            .unwrap();
        v.write(fh, 0, vec![i; 1 << 20]).await.unwrap();
        v.release(fh, None).await;
        files.push(a.id);
    }
    c.eventually("stable", Duration::from_secs(5), |c| {
        files.iter().all(|f| {
            c.attr(1, *f)
                .is_some_and(|x| x.gen_state == GenState::Stable)
        })
    })
    .await;
    c.converge().await;

    // Off-grid: a full copy on n4, nothing removed from fabric hosts.
    let offgrid = |hosts: &[&str]| Goal::Offgrid {
        selector: "/m".into(),
        hosts: hosts.iter().map(|h| h.to_string()).collect(),
        hub: "/hub".into(),
        spread: false,
    };
    let plan = p.plan(offgrid(&["n4"])).await.unwrap();
    assert!(plan.feasible, "{plan:?}");
    assert!(
        matches!(&plan.steps[..], [Step::Replicate { host, copies, .. }] if host == "n4" && copies.len() == 3)
    );
    let job = wait_job(&c, p.apply_plan(plan.id).await.unwrap()).await;
    assert!(job.error.is_none(), "{job:?}");
    c.converge().await;
    for f in &files {
        assert_eq!(holders(&c, *f), [1, 4]);
    }
    // Fabric hosts are refused as off-grid targets, and off-grid as Place targets.
    assert!(p.plan(offgrid(&["n2"])).await.is_err());
    let place = |hosts: &[&str], factor: f64| Goal::Place {
        selector: "/m".into(),
        hosts: hosts.iter().map(|h| h.to_string()).collect(),
        factor,
        by_speed: false,
        hub: "/hub".into(),
    };
    assert!(p.plan(place(&["n4"], 1.0)).await.is_err());

    // Place over every (fabric) host, one copy each: n4 is neither a
    // target nor a source of removals, and keeps its copies.
    let plan = p.plan(place(&[], 1.0 / 3.0)).await.unwrap();
    assert!(
        plan.steps.iter().all(|s| !matches!(s, Step::Replicate { host, .. } | Step::Evict { host, .. } if host == "n4")),
        "{plan:?}"
    );
    let job = wait_job(&c, p.apply_plan(plan.id).await.unwrap()).await;
    assert!(job.error.is_none(), "{job:?}");
    c.converge().await;
    for f in &files {
        let h = holders(&c, *f);
        assert!(h.contains(&4) && h.len() == 2, "{h:?}");
    }

    // Offload to n4: the fabric hosts' copies go, n4's stays, files read on.
    let sel = nest_place::Selector::parse("/m", "/hub").unwrap();
    let job = wait_job(&c, p.offload(sel, "n4".into(), 2).await.unwrap()).await;
    assert!(job.error.is_none(), "{job:?}");
    c.converge().await;
    for (i, f) in files.iter().enumerate() {
        assert_eq!(holders(&c, *f), [4]);
        assert_eq!(read_all(&c, 2, *f).await, vec![i as u8; 1 << 20]);
    }

    // Off-grid copies removed everywhere: never the only copy.
    let plan = p.plan(offgrid(&[])).await.unwrap();
    assert!(plan.steps.is_empty(), "{plan:?}");
    assert!(
        plan.notes.iter().any(|n| n.contains("only copy")),
        "{plan:?}"
    );
}

/// A fabric host moves off-grid and back (ADR-047): stopped, its role
/// changed in membership, started with the matching configuration.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_host_moves_off_the_fabric_and_back() {
    let mut c = ready(4).await;
    let voters = |c: &TestCluster| -> Vec<u64> {
        let mut v: Vec<u64> = c
            .node(1)
            .meta
            .metrics()
            .membership_config
            .membership()
            .voter_ids()
            .collect();
        v.sort();
        v
    };
    assert_eq!(voters(&c), [1, 2, 3, 4]);
    let f = file(&c, 1, "before", b"written on the fabric").await;

    // Off the fabric.
    c.stop(4).await;
    c.node(1)
        .meta
        .set_role(NodeId(4), String::new())
        .await
        .unwrap();
    c.set_offgrid(4, true);
    c.restart(4).await;
    c.eventually("4 serving off-grid", Duration::from_secs(10), |c| {
        c.node(4).data.caught_up() && c.node(4).data.session().is_some()
    })
    .await;
    assert_eq!(voters(&c), [1, 2, 3]);
    c.eventually(
        "fabric hosts forgot its address",
        Duration::from_secs(5),
        |c| (1..=3).all(|i| c.node(i).rpc.peer_addr(NodeId(4)).is_none()),
    )
    .await;
    assert_eq!(read_all(&c, 4, f).await, b"written on the fabric");
    let g = file(&c, 4, "off-grid", b"written off the fabric").await;
    assert_eq!(read_all(&c, 2, g).await, b"written off the fabric");
    // Three voters must remain.
    assert!(
        c.node(1)
            .meta
            .set_role(NodeId(3), String::new())
            .await
            .is_err()
    );

    // Back onto the fabric.
    c.stop(4).await;
    let addr = c.node_config(4).node.listen.to_string();
    c.node(1).meta.set_role(NodeId(4), addr).await.unwrap();
    c.set_offgrid(4, false);
    c.restart(4).await;
    c.eventually("4 a voter again", Duration::from_secs(10), |c| {
        c.node(4).data.caught_up() && voters(c) == [1, 2, 3, 4]
    })
    .await;
    assert!(c.node(1).vfs.offgrid_hosts().is_empty());
    let h = file(&c, 4, "back", b"on the fabric again").await;
    assert_eq!(read_all(&c, 3, h).await, b"on the fabric again");
}

/// Whether `node` opens `f` for passthrough.
async fn passthrough(c: &TestCluster, node: u64, f: FileId) -> bool {
    let v = &c.node(node).vfs;
    let (fh, mode) = v.open(f, RO).await.unwrap();
    v.release(fh, None).await;
    matches!(mode, nest_data::OpenMode::Passthrough(_))
}

async fn mark(c: &TestCluster, f: FileId, always: bool) {
    c.node(1)
        .meta
        .propose(Command::SetPassthrough { file: f, always })
        .await
        .unwrap();
    c.converge().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_local_copy_goes_to_the_kernel() {
    // `sole` keeps no file from the kernel any more (ADR-065): the daemon
    // reads a local copy from local disk either way.
    let mut t = nest_testkit::fast_tuning();
    t.vfs.passthrough = nest_data::Passthrough::Sole;
    let mut c = TestCluster::start_with(3, t).await;
    c.eventually("all nodes serving", Duration::from_secs(8), |c| {
        (1..=3).all(|i| c.node(i).data.caught_up() && c.node(i).data.session().is_some())
    })
    .await;
    c.add_offgrid(4).await;
    c.eventually("off-grid host serving", Duration::from_secs(8), |c| {
        c.node(4).data.caught_up() && c.node(4).data.session().is_some()
    })
    .await;
    let f = file(&c, 1, "engram", &vec![3u8; 64 << 10]).await;
    c.node(1)
        .meta
        .propose(Command::Seal {
            file: f,
            sealed: true,
            now: Timestamp(1),
        })
        .await
        .unwrap();
    c.converge().await;

    assert!(passthrough(&c, 1, f).await, "the only copy");
    replicate(&c, f, 1, 4).await;
    replicate(&c, f, 1, 2).await;
    assert!(passthrough(&c, 1, f).await, "beside other copies");
    assert!(passthrough(&c, 2, f).await, "a second fabric copy");
    assert!(
        !passthrough(&c, 3, f).await,
        "passthrough without a local copy"
    );
    // Marked always passthrough (ADR-053): still every host with a copy.
    mark(&c, f, true).await;
    assert!(passthrough(&c, 1, f).await);
    assert!(!passthrough(&c, 3, f).await);
    mark(&c, f, false).await;
    assert!(passthrough(&c, 1, f).await);
}

fn lane_writes(c: &TestCluster, id: u64) -> (u64, u64) {
    c.node(id).vfs.lanes().map_or((0, 0), |l| {
        (
            l.stats
                .write_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            l.stats
                .written_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    })
}

/// Writes between an off-grid host and a fabric owner travel over the TCP
/// lanes (ADR-057), not the control connection, and land intact both ways.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn offgrid_writes_go_over_tcp_lanes_both_ways() {
    let c = with_offgrid(3, 4).await;
    c.eventually("lanes take writes", Duration::from_secs(5), |c| {
        let l4 = c.node(4).vfs.lanes().unwrap();
        let l1 = c.node(1).vfs.lanes().unwrap();
        l4.writable(NodeId(1)) && l1.writable(NodeId(4))
    })
    .await;
    // The off-grid host rewrites a file a fabric host owns.
    let base: Vec<u8> = (0..(3 << 20) + 5).map(|i| (i % 241) as u8).collect();
    let f = file(&c, 1, "fabric-owned", &base).await;
    let v4 = &c.node(4).vfs;
    let (fh, _) = v4.open(f, oflags::RDWR).await.unwrap();
    let new: Vec<u8> = (0..2 << 20).map(|i| (i % 239) as u8).collect();
    let (sent, applied) = (lane_writes(&c, 4).0, lane_writes(&c, 1).1);
    v4.write(fh, 100, new[..1 << 20].to_vec()).await.unwrap();
    v4.write(fh, 100 + (1 << 20), new[1 << 20..].to_vec())
        .await
        .unwrap();
    assert_eq!(c.attr(4, f).unwrap().owner, Some(NodeId(1)));
    assert_eq!(lane_writes(&c, 4).0 - sent, 2 << 20, "not sent over lanes");
    assert_eq!(lane_writes(&c, 1).1 - applied, 2 << 20);
    v4.release(fh, None).await;
    c.converge().await;
    let mut want = base.clone();
    want[100..100 + (2 << 20)].copy_from_slice(&new);
    assert_eq!(read_all(&c, 2, f).await, want);

    // A fabric host appends to a file the off-grid host owns.
    let g = file(&c, 4, "offgrid-owned", b"log:").await;
    let v2 = &c.node(2).vfs;
    let (fh, _) = v2.open(g, oflags::WRONLY | oflags::APPEND).await.unwrap();
    let sent = lane_writes(&c, 2).0;
    for _ in 0..3 {
        v2.write(fh, 0, b"abc".to_vec()).await.unwrap();
    }
    assert_eq!(c.attr(2, g).unwrap().owner, Some(NodeId(4)));
    assert_eq!(lane_writes(&c, 2).0 - sent, 9, "not sent over lanes");
    v2.release(fh, None).await;
    c.converge().await;
    assert_eq!(read_all(&c, 3, g).await, b"log:abcabcabc");
}

/// Off-grid spread: with two off-grid hosts picked, each file goes to one of
/// them (bytes kept even) instead of both; picking spread over hosts that
/// hold full copies moves each file's second copy away, never the last.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn offgrid_spread_puts_one_copy_over_the_hosts_picked() {
    use nest_place::plan::{Goal, Step};
    let mut c = with_offgrid(3, 4).await;
    c.add_offgrid(5).await;
    c.eventually("n5 serving and named", Duration::from_secs(8), |c| {
        c.node(5).data.caught_up()
            && c.node(1)
                .placer
                .nodes()
                .is_ok_and(|v| v.iter().any(|h| h.name == "n5"))
    })
    .await;
    let p = c.node(1).placer.clone();
    let v = c.node(1).vfs.clone();
    let dir = v.mkdir(FileId::ROOT, b"m", 0o755).await.unwrap().id;
    let mut files = Vec::new();
    for i in 0..6u8 {
        let (a, fh, _) = v
            .create(dir, format!("part{i}").as_bytes(), 0o644, oflags::WRONLY)
            .await
            .unwrap();
        v.write(fh, 0, vec![i; 1 << 20]).await.unwrap();
        v.release(fh, None).await;
        files.push(a.id);
    }
    c.eventually("stable", Duration::from_secs(5), |c| {
        files.iter().all(|f| {
            c.attr(1, *f)
                .is_some_and(|x| x.gen_state == GenState::Stable)
        })
    })
    .await;
    c.converge().await;
    let goal = |spread| Goal::Offgrid {
        selector: "/m".into(),
        hosts: vec!["n4".into(), "n5".into()],
        hub: "/hub".into(),
        spread,
    };
    let adds = |plan: &nest_place::plan::Plan| -> Vec<(String, usize)> {
        let mut v: Vec<_> = plan
            .steps
            .iter()
            .filter_map(|s| match s {
                Step::Replicate { host, copies, .. } => Some((host.clone(), copies.len())),
                _ => None,
            })
            .collect();
        v.sort();
        v
    };
    // Spread: three files each.
    let plan = p.plan(goal(true)).await.unwrap();
    assert!(plan.feasible, "{plan:?}");
    assert_eq!(adds(&plan), [("n4".to_string(), 3), ("n5".to_string(), 3)]);
    assert!(
        plan.notes.iter().any(|n| n.contains("spread")),
        "{:?}",
        plan.notes
    );
    // Unspread: a full copy on each.
    let full = p.plan(goal(false)).await.unwrap();
    assert_eq!(adds(&full), [("n4".to_string(), 6), ("n5".to_string(), 6)]);
    let job = wait_job(&c, p.apply_plan(full.id).await.unwrap()).await;
    assert!(job.error.is_none(), "{job:?}");
    c.converge().await;
    for f in &files {
        assert_eq!(holders(&c, *f), [1, 4, 5]);
    }
    // Spread over hosts holding full copies: each drops the other's half,
    // nothing is copied, the fabric copy stays.
    let plan = p.plan(goal(true)).await.unwrap();
    assert!(adds(&plan).is_empty(), "{plan:?}");
    let drops: Vec<(String, usize)> = plan
        .steps
        .iter()
        .filter_map(|s| match s {
            Step::Evict { host, copies, .. } => Some((host.clone(), copies.len())),
            _ => None,
        })
        .collect();
    assert_eq!(drops.iter().map(|d| d.1).sum::<usize>(), 6, "{drops:?}");
    let job = wait_job(&c, p.apply_plan(plan.id).await.unwrap()).await;
    assert!(job.error.is_none(), "{job:?}");
    c.converge().await;
    let mut per = [0; 2];
    for f in &files {
        let h = holders(&c, *f);
        assert_eq!(h.len(), 2, "{h:?}");
        assert!(h.contains(&1));
        per[if h.contains(&4) { 0 } else { 1 }] += 1;
    }
    assert_eq!(per, [3, 3]);
}

/// Off-grid hosts on the LAN (ADR-071): each has its address in
/// membership, so every host dials it, and two off-grid hosts copy and
/// read between them directly over lanes, with no fabric host holding
/// a copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn offgrid_hosts_on_the_lan_reach_each_other() {
    let mut c = ready(3).await;
    for id in [4, 5] {
        c.add_offgrid_at(id).await;
    }
    c.eventually("off-grid hosts serving", Duration::from_secs(10), |c| {
        [4, 5]
            .iter()
            .all(|&i| c.node(i).data.caught_up() && c.node(i).data.session().is_some())
    })
    .await;
    for i in 1..=5 {
        assert_eq!(c.node(i).vfs.offgrid_hosts(), [NodeId(4), NodeId(5)].into());
    }
    c.eventually(
        "every host has both addresses",
        Duration::from_secs(5),
        |c| {
            (1..=5).all(|i| {
                [4, 5]
                    .iter()
                    .all(|&o| o == i || c.node(i).rpc.peer_addr(NodeId(o)).is_some())
            })
        },
    )
    .await;
    c.eventually(
        "lanes between the off-grid hosts",
        Duration::from_secs(10),
        |c| {
            c.node(4).vfs.lanes().unwrap().connected(NodeId(5)) >= 2
                && c.node(5).vfs.lanes().unwrap().connected(NodeId(4)) >= 2
        },
    )
    .await;

    // Written on 4, held only there; 5 copies it from 4.
    let big: Vec<u8> = (0..(5 << 20) + 13).map(|i| (i % 241) as u8).collect();
    let f = file(&c, 4, "on-4", &big).await;
    assert_eq!(holders(&c, f), [4]);
    let before = lane_read(&c, 5);
    c.node(5).vfs.replicate_here(f).await.unwrap();
    c.converge().await;
    assert_eq!(holders(&c, f), [4, 5]);
    assert!(
        lane_read(&c, 5) - before >= big.len() as u64,
        "copy not over lanes"
    );
    let local = c.node(5).vfs.local_read_bytes();
    assert_eq!(read_all(&c, 5, f).await, big);
    assert_eq!(c.node(5).vfs.local_read_bytes() - local, big.len() as u64);

    // Written on 5, read from 4 without a copy.
    let g = file(&c, 5, "on-5", &big[..2 << 20]).await;
    assert_eq!(read_all(&c, 4, g).await, &big[..2 << 20]);
}

/// A dial-in off-grid host (behind NAT) is given its LAN address while it
/// runs: the others start dialling it there, and an off-grid peer copies
/// from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_dial_in_offgrid_host_is_given_its_lan_address() {
    let mut c = with_offgrid(3, 4).await;
    c.add_offgrid_at(5).await;
    c.eventually("5 serving", Duration::from_secs(10), |c| {
        c.node(5).data.caught_up() && c.node(5).data.session().is_some()
    })
    .await;
    let f = file(&c, 4, "on-4", b"held only by the dial-in host").await;
    assert_eq!(holders(&c, f), [4]);
    assert_eq!(c.node(1).rpc.peer_addr(NodeId(4)), None);

    let addr = c.node_config(4).node.listen.to_string();
    c.node(1)
        .meta
        .set_offgrid_addr(NodeId(4), addr)
        .await
        .unwrap();
    c.eventually("every host dials 4", Duration::from_secs(5), |c| {
        (1..=5).all(|i| i == 4 || c.node(i).rpc.peer_addr(NodeId(4)).is_some())
    })
    .await;
    assert_eq!(c.node(1).vfs.offgrid_hosts(), [NodeId(4), NodeId(5)].into());
    c.node(5).vfs.replicate_here(f).await.unwrap();
    c.converge().await;
    assert_eq!(holders(&c, f), [4, 5]);
    assert_eq!(read_all(&c, 5, f).await, b"held only by the dial-in host");
}

/// A file that cannot be copied is listed as failed and not counted as
/// done: a job's progress shows only bytes that landed.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_copy_that_fails_is_not_counted_as_done() {
    let c = ready(3).await;
    let f = file(&c, 1, "m", &[7u8; 1 << 20]).await;
    // n2 has no room for it.
    c.node(2).data.store().set_reserve(u64::MAX / 4);
    let sel = nest_place::Selector::parse("/m", "/hub").unwrap();
    let id = c
        .node(1)
        .placer
        .replicate(sel, vec!["n2".into()], 2)
        .await
        .unwrap();
    let job = wait_job(&c, id).await;
    let h = &job.hosts["n2"];
    assert_eq!(h.failed.len(), 1, "{job:?}");
    assert_eq!((h.done_files, h.done_bytes), (0, 0), "{job:?}");
    assert_eq!((h.total_files, h.total_bytes), (1, 1 << 20), "{job:?}");
    assert_eq!(holders(&c, f), [1]);
}
