//! Full outages and upgrades: the cluster re-founds itself (ADR-026).

use nest_data::vfs::oflags;
use nest_testkit::TestCluster;
use nest_types::*;
use std::time::Duration;

async fn ready(n: u64) -> TestCluster {
    let c = TestCluster::start(n).await;
    c.eventually("serving", Duration::from_secs(8), |c| {
        (1..=n).all(|i| c.node(i).data.caught_up() && c.node(i).data.session().is_some())
    })
    .await;
    c
}

async fn write_file(c: &TestCluster, node: u64, name: &str) -> FileId {
    let v = c.node(node).vfs.clone();
    let (a, fh, _) = v
        .create(FileId::ROOT, name.as_bytes(), 0o644, oflags::WRONLY)
        .await
        .unwrap();
    v.write(fh, 0, name.as_bytes().to_vec()).await.unwrap();
    v.release(fh, None).await;
    c.eventually("stable", Duration::from_secs(5), |c| {
        c.attr(node, a.id)
            .is_some_and(|x| x.gen_state == GenState::Stable)
    })
    .await;
    a.id
}

async fn read_all(c: &TestCluster, node: u64, f: FileId) -> Vec<u8> {
    let v = c.node(node).vfs.clone();
    let (fh, _) = v.open(f, 0).await.unwrap();
    let b = v.read(fh, 0, 1 << 16).await.unwrap();
    v.release(fh, None).await;
    b
}

async fn stop_all(c: &mut TestCluster, ids: &[u64]) {
    for &i in ids {
        c.stop(i).await;
    }
}

/// Every host loses power; their disks rolled back to different points.
/// They find no running cluster, agree on the most advanced metadata as the
/// seed, re-found, and quarantine objects the seed does not know.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn full_outage_refounds_from_the_most_advanced_host() {
    let mut c = ready(3).await;
    let early = write_file(&c, 1, "early").await;
    stop_all(&mut c, &[1, 2, 3]).await;
    let d0: Vec<_> = (1..=3).map(|i| c.save_disk(i)).collect();
    c.restart_many(&[1, 2, 3]).await;
    c.eventually("serving", Duration::from_secs(8), |c| {
        (1..=3).all(|i| c.node(i).data.caught_up())
    })
    .await;
    let middle = write_file(&c, 1, "middle").await;
    // Host 1's disk durably holds everything up to here.
    c.stop(1).await;
    let d1 = c.save_disk(1);
    c.restart(1).await;
    c.eventually("n1 serving", Duration::from_secs(8), |c| {
        c.node(1).data.caught_up()
    })
    .await;
    let late = write_file(&c, 2, "late").await;

    c.power_loss(1, Some(&d1)).await;
    c.power_loss(2, Some(&d0[1])).await;
    c.power_loss(3, Some(&d0[2])).await;
    // An object on host 2 whose metadata did not survive anywhere.
    let stray = nest_store::ObjectKey::new(FileId(7_000_001), Generation(1));
    let obj = c
        .state_dir(2)
        .join("objects")
        .join(format!("{:02x}", (stray.file.0 & 0xff) as u8));
    std::fs::create_dir_all(&obj).ok();
    let path = nest_store::ObjectStore::open(&c.state_dir(2))
        .unwrap()
        .path(stray);
    std::fs::write(&path, b"only copy").unwrap();

    c.restart_many(&[1, 2, 3]).await;
    let inc = c.node(1).meta.incarnation();
    assert!(inc.is_some(), "a new incarnation was founded");
    for i in 1..=3 {
        assert!(c.node(i).prior.dirty());
        assert_eq!(c.node(i).meta.incarnation(), inc, "n{i}");
    }
    c.wait_leader().await;
    c.converge().await;
    for i in 1..=3 {
        assert!(c.lookup(i, FileId::ROOT, "early").is_some(), "n{i}");
        assert!(c.lookup(i, FileId::ROOT, "middle").is_some(), "n{i}");
        assert!(
            c.lookup(i, FileId::ROOT, "late").is_none(),
            "n{i}: written after every durable point"
        );
    }
    assert_eq!(read_all(&c, 3, middle).await, b"middle");
    assert_eq!(read_all(&c, 2, early).await, b"early");
    let _ = late;
    // The stray object is kept, not deleted: /.lost+found/<run>/n2/unknown/.
    let lf = c.lookup(1, FileId::ROOT, ".lost+found").unwrap();
    let runs: Vec<_> = c
        .node(1)
        .vfs
        .readdir(lf, 0, 16)
        .unwrap()
        .into_iter()
        .filter(|(_, e)| e.name != b"." && e.name != b"..")
        .collect();
    assert_eq!(runs.len(), 1, "one fsck run collected something");
    let d = c.lookup(1, runs[0].1.id, "n2").unwrap();
    let d = c.lookup(1, d, "unknown").unwrap();
    let f = c
        .lookup(1, d, &format!("{:016x}.1", stray.file.0))
        .expect("stray quarantined");
    assert_eq!(read_all(&c, 1, f).await, b"only copy");
    // And the new cluster takes writes.
    write_file(&c, 3, "after").await;
}

/// Rewrite a stopped host's Raft state as an older build would have left it
/// (Raft format 1), as if this host now ran an upgraded binary.
fn make_format_1(dir: &std::path::Path) {
    let r = rusqlite::Connection::open(dir.join("raft.sqlite")).unwrap();
    r.execute("DELETE FROM state WHERE k = 'raft_format'", [])
        .unwrap();
    r.execute(
        "INSERT OR REPLACE INTO state (k, v) VALUES ('format', ?1)",
        [postcard::to_stdvec(&1u32).unwrap()],
    )
    .unwrap();
    let m = rusqlite::Connection::open(dir.join("meta.sqlite")).unwrap();
    m.execute("DELETE FROM sm_state WHERE k = 'raft_format'", [])
        .unwrap();
}

/// Everyone stops cleanly and comes back on a build with a new Raft
/// format: the cluster re-founds itself with nothing lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn upgrade_of_the_whole_cluster_is_lossless() {
    let mut c = ready(3).await;
    let files: Vec<FileId> = futures_join(&c, &["a", "b", "c", "d"]).await;
    stop_all(&mut c, &[1, 2, 3]).await;
    for i in 1..=3 {
        make_format_1(&c.state_dir(i));
    }
    c.restart_many(&[1, 2, 3]).await;
    assert!(c.node(1).meta.incarnation().is_some());
    c.wait_leader().await;
    c.converge().await;
    for (f, name) in files.iter().zip(["a", "b", "c", "d"]) {
        for i in 1..=3 {
            assert!(c.lookup(i, FileId::ROOT, name).is_some(), "n{i} {name}");
        }
        assert_eq!(read_all(&c, 2, *f).await, name.as_bytes());
    }
    write_file(&c, 2, "after").await;
}

async fn futures_join(c: &TestCluster, names: &[&str]) -> Vec<FileId> {
    let mut out = Vec::new();
    for (i, n) in names.iter().enumerate() {
        out.push(write_file(c, 1 + (i as u64 % 3), n).await);
    }
    out
}

/// One host comes back on the new build while the others are still down:
/// it does not re-found alone. When the others return (already current),
/// it sets its old state aside and joins them.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_lone_upgraded_host_waits_then_joins() {
    let mut c = ready(3).await;
    let f = write_file(&c, 1, "kept").await;
    stop_all(&mut c, &[1, 2, 3]).await;
    make_format_1(&c.state_dir(3));

    // Alone it cannot reach a majority: it keeps waiting.
    let pending = tokio::time::timeout(Duration::from_secs(3), c.start_detached(3)).await;
    assert!(pending.is_err(), "a minority must not re-found");

    c.restart_many(&[1, 2, 3]).await;
    assert_eq!(c.node(3).meta.incarnation(), c.node(1).meta.incarnation());
    c.converge().await;
    assert!(c.lookup(3, FileId::ROOT, "kept").is_some());
    assert_eq!(read_all(&c, 3, f).await, b"kept");
}

/// A host that was down during a re-found comes back (cleanly) on the old
/// incarnation: it must not mix Raft state with the new group. It sets its
/// state aside, joins afresh and receives the seeded state as a snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn host_absent_during_refound_joins_the_new_incarnation() {
    let mut c = ready(3).await;
    let f = write_file(&c, 1, "before").await;
    c.stop(3).await; // cleanly, and stays away
    c.power_loss(1, None).await;
    c.power_loss(2, None).await;
    c.restart_many(&[1, 2]).await;
    let inc = c.node(1).meta.incarnation();
    assert!(inc.is_some(), "two of three re-founded");
    write_file(&c, 2, "during").await;

    c.restart(3).await;
    assert_eq!(c.node(3).meta.incarnation(), inc);
    c.converge().await;
    assert!(c.lookup(3, FileId::ROOT, "before").is_some());
    assert!(c.lookup(3, FileId::ROOT, "during").is_some());
    assert_eq!(read_all(&c, 3, f).await, b"before");
}
