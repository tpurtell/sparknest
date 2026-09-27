//! Milestone 8: hardening. Its own test binary, so no parallel test moves
//! the filesystem's free space underneath the reserve checks.

use nest_data::vfs::oflags;
use nest_place::Selector;
use nest_testkit::TestCluster;
use nest_types::*;
use std::sync::Arc;
use std::time::Duration;

async fn ready(n: u64) -> TestCluster {
    let c = TestCluster::start(n).await;
    c.eventually("serving", Duration::from_secs(8), |c| {
        (1..=n).all(|i| c.node(i).data.caught_up() && c.node(i).data.session().is_some())
    })
    .await;
    c.eventually("names", Duration::from_secs(8), |c| {
        c.node(1)
            .placer
            .nodes()
            .map(|n| n.iter().all(|h| !h.name.starts_with("node")))
            .unwrap_or(false)
    })
    .await;
    c
}

/// A node whose disk is down to its reserve refuses object data (writes,
/// the FUSE fast path, incoming replicas) with ENOSPC, while metadata keeps
/// working; once space returns, so do writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn data_reserve_refuses_data_but_not_metadata() {
    let c = ready(2).await;
    let v1 = &c.node(1).vfs;
    let store = c.node(1).vfs.data().store().clone();
    let free = store.capacity().unwrap().free;
    // Leave 256 MiB of room above the reserve.
    store.set_reserve(free - (256 << 20));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let (a, fh, _) = v1
        .create(FileId::ROOT, b"f", 0o644, oflags::WRONLY)
        .await
        .unwrap();
    assert_eq!(v1.write(fh, 0, vec![1u8; 1 << 20]).await.unwrap(), 1 << 20);
    let e = v1
        .write(fh, 1 << 20, vec![1u8; 512 << 20])
        .await
        .unwrap_err();
    assert_eq!(e, NestError::NoSpace);
    if let Some(r) = v1.try_write_now(fh, 1 << 20, &vec![1u8; 512 << 20]) {
        assert_eq!(r.unwrap_err(), NestError::NoSpace);
    }
    v1.release(fh, None).await;

    // Metadata still works on the full node.
    v1.mkdir(FileId::ROOT, b"d", 0o755).await.unwrap();
    let d = c.lookup(1, FileId::ROOT, "d").unwrap();
    v1.rename(FileId::ROOT, b"f", d, b"g", Default::default())
        .await
        .unwrap();

    // A replica too large for the room left is refused; the file stays whole
    // on its writer.
    let v2 = &c.node(2).vfs;
    let (b, fh, _) = v2
        .create(FileId::ROOT, b"big", 0o644, oflags::WRONLY)
        .await
        .unwrap();
    v2.write(fh, 0, vec![2u8; 300 << 20]).await.unwrap();
    v2.release(fh, None).await;
    c.eventually("stable", Duration::from_secs(10), |c| {
        c.attr(2, b.id)
            .is_some_and(|x| x.gen_state == GenState::Stable)
    })
    .await;
    let p = &c.node(1).placer;
    let j = p
        .replicate(
            Selector::parse("/big", "/hub").unwrap(),
            vec!["n1".into()],
            2,
        )
        .await
        .unwrap();
    c.eventually("job", Duration::from_secs(20), |c| {
        c.node(1).placer.job(j).is_some_and(|x| x.finished)
    })
    .await;
    let job = p.job(j).unwrap();
    let failed = &job.hosts["n1"].failed;
    assert!(
        failed.len() == 1 && failed[0].1.contains("space"),
        "{job:?}"
    );

    // Space comes back: writes and replication work again.
    store.set_reserve(0);
    let (fh, _) = v1.open(a.id, oflags::WRONLY).await.unwrap();
    assert_eq!(
        v1.write(fh, 1 << 20, vec![1u8; 4 << 20]).await.unwrap(),
        4 << 20
    );
    v1.release(fh, None).await;
    assert!(c.node(1).vfs.replicate_here(b.id).await.unwrap() > 0);
}

/// Where a metadata commit's time goes. A measurement, not a check:
///   TMPDIR=<disk dir> cargo test -p nest-testkit --test m8 commit_latency -- --ignored --nocapture
///   TMPDIR=/dev/shm   ...   (fsync is free on tmpfs)
/// In-process nodes talk over loopback TCP, so network hops cost only the
/// software path; compare with the same numbers on the real cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement"]
async fn commit_latency_breakdown() {
    fn line(what: &str, mut ts: Vec<Duration>) {
        ts.sort();
        let n = ts.len();
        println!(
            "  {what:<40} median {:>6.2} ms  p90 {:>6.2} ms",
            ts[n / 2].as_secs_f64() * 1e3,
            ts[n * 9 / 10].as_secs_f64() * 1e3
        );
    }
    let dir = std::env::temp_dir();
    let probe = dir.join(format!("fsync-probe-{}", std::process::id()));
    let f = std::fs::File::create(&probe).unwrap();
    let mut ts = Vec::new();
    for i in 0..100u64 {
        use std::os::unix::fs::FileExt;
        f.write_all_at(&[1u8; 4096], i * 4096).unwrap();
        let t = std::time::Instant::now();
        f.sync_data().unwrap();
        ts.push(t.elapsed());
    }
    drop(f);
    let _ = std::fs::remove_file(&probe);
    println!("state in {}", dir.display());
    line("fsync of 4 KiB", ts);

    let sizes: Vec<u64> = std::env::var("LAT_NODES")
        .map(|v| v.split(',').filter_map(|x| x.parse().ok()).collect())
        .unwrap_or_else(|_| vec![3, 7]);
    let only_seq = std::env::var("LAT_SEQ_ONLY").is_ok();
    for nodes in sizes {
        let c = ready(nodes).await;
        let leader = c.node(1).meta.leader().unwrap().0;
        let follower = (1..=nodes).find(|i| *i != leader).unwrap();
        println!("{nodes} nodes (leader n{leader}):");
        for (role, id) in [("leader", leader), ("follower", follower)] {
            let meta = &c.node(id).meta;
            let mut ts = Vec::new();
            for _ in 0..300 {
                let t = std::time::Instant::now();
                meta.propose(nest_meta::Command::Batch(vec![]))
                    .await
                    .unwrap();
                ts.push(t.elapsed());
            }
            line(&format!("empty commit on the {role}"), ts);
            let v = &c.node(id).vfs;
            let mut ts = Vec::new();
            for i in 0..300 {
                let name = format!("{role}{nodes}-{i}");
                let t = std::time::Instant::now();
                v.mkdir(FileId::ROOT, name.as_bytes(), 0o755).await.unwrap();
                ts.push(t.elapsed());
            }
            line(&format!("mkdir on the {role}"), ts);
            if only_seq {
                continue;
            }
            // Throughput with many operations in flight (e.g. parallel rm).
            let t = std::time::Instant::now();
            let n = 2000;
            let mut set = tokio::task::JoinSet::new();
            let sem = Arc::new(tokio::sync::Semaphore::new(32));
            for _ in 0..n {
                let meta = c.node(id).meta.clone();
                let permit = sem.clone().acquire_owned().await.unwrap();
                set.spawn(async move {
                    meta.propose(nest_meta::Command::Batch(vec![]))
                        .await
                        .unwrap();
                    drop(permit);
                });
            }
            while set.join_next().await.is_some() {}
            println!(
                "  {:<40} {:>6.0} commits/s",
                format!("32 in flight on the {role}"),
                n as f64 / t.elapsed().as_secs_f64()
            );
        }
    }
}

/// An application's fsync makes the file's metadata durable on a majority:
/// it succeeds with one of three hosts down and fails, rather than pretend,
/// when no majority can confirm.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn fsync_waits_for_a_durable_majority() {
    let mut c = ready(3).await;
    let v1 = c.node(1).vfs.clone();
    let (_, fh, _) = v1
        .create(FileId::ROOT, b"f", 0o644, oflags::WRONLY)
        .await
        .unwrap();
    v1.write(fh, 0, vec![1u8; 4096]).await.unwrap();
    v1.fsync(fh, false).await.unwrap();
    v1.fsync(fh, true).await.unwrap();
    v1.sync_metadata().await.unwrap();

    c.stop(3).await;
    v1.write(fh, 4096, vec![2u8; 4096]).await.unwrap();
    v1.fsync(fh, false).await.unwrap();
    v1.release(fh, None).await;

    c.stop(2).await;
    let t = std::time::Instant::now();
    assert!(
        v1.sync_metadata().await.is_err(),
        "no majority left to confirm"
    );
    assert!(t.elapsed() < Duration::from_secs(30));
}

/// `remove_tree` removes a whole tree, children before directories, many
/// entries per commit; dry run counts without changing anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn remove_tree_in_batches() {
    let c = ready(3).await;
    let v = c.node(1).vfs.clone();
    let top = v.mkdir(FileId::ROOT, b"tree", 0o755).await.unwrap().id;
    let mut want_files = 0u64;
    for d in 0..50 {
        let dir = v
            .mkdir(top, format!("d{d}").as_bytes(), 0o755)
            .await
            .unwrap()
            .id;
        let sub = v.mkdir(dir, b"sub", 0o755).await.unwrap().id;
        for f in 0..30 {
            let parent = if f % 2 == 0 { dir } else { sub };
            let (_, fh, _) = v
                .create(parent, format!("f{f}").as_bytes(), 0o644, oflags::WRONLY)
                .await
                .unwrap();
            v.release(fh, None).await;
            want_files += 1;
        }
        v.symlink(dir, b"link", b"sub/f1").await.unwrap();
        want_files += 1;
    }
    let want_dirs = 1 + 50 * 2;

    let e = v
        .remove_tree(FileId::ROOT, b"tree", false, false)
        .await
        .unwrap_err();
    assert!(matches!(e, NestError::Invalid(_)), "{e:?}");
    let dry = v
        .remove_tree(FileId::ROOT, b"tree", true, true)
        .await
        .unwrap();
    assert_eq!((dry.files, dry.dirs), (want_files, want_dirs));
    assert!(
        c.lookup(1, FileId::ROOT, "tree").is_some(),
        "dry run changes nothing"
    );

    let t = std::time::Instant::now();
    let r = v
        .remove_tree(FileId::ROOT, b"tree", true, false)
        .await
        .unwrap();
    let batched = t.elapsed();
    assert!(r.errors.is_empty(), "{:?}", r.errors);
    assert_eq!((r.files, r.dirs), (want_files, want_dirs));
    c.converge().await;
    for n in 1..=3 {
        assert!(c.lookup(n, FileId::ROOT, "tree").is_none());
    }

    // For scale: the same number of single unlinks.
    let d = v.mkdir(FileId::ROOT, b"flat", 0o755).await.unwrap().id;
    for f in 0..200 {
        let (_, fh, _) = v
            .create(d, format!("f{f}").as_bytes(), 0o644, oflags::WRONLY)
            .await
            .unwrap();
        v.release(fh, None).await;
    }
    let t = std::time::Instant::now();
    for f in 0..200 {
        v.unlink(d, format!("f{f}").as_bytes()).await.unwrap();
    }
    let single = t.elapsed() / 200;
    eprintln!(
        "remove_tree: {} entries in {:?} ({:?} each); single unlink {:?} each",
        want_files + want_dirs,
        batched,
        batched / (want_files + want_dirs) as u32,
        single
    );
}

async fn write_file(v: &nest_data::Vfs, name: &[u8], len: usize) -> FileId {
    let (a, fh, _) = v
        .create(FileId::ROOT, name, 0o644, oflags::WRONLY)
        .await
        .unwrap();
    v.write(fh, 0, vec![7u8; len]).await.unwrap();
    v.release(fh, None).await;
    a.id
}

/// A minority host loses power (its disk rolls back to an earlier durable
/// point): it notices, catches up with the healthy quorum, settles its
/// objects against the now-authoritative metadata, and serves again.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn crashed_minority_host_recovers_against_the_quorum() {
    use nest_data::fsck::{Action, Issue};
    let mut c = ready(3).await;
    let v1 = c.node(1).vfs.clone();
    let doomed = write_file(&v1, b"doomed", 1000).await;
    c.eventually("stable", Duration::from_secs(5), |c| {
        c.attr(1, doomed)
            .is_some_and(|a| a.gen_state == GenState::Stable)
    })
    .await;
    let p = c.node(1).placer.clone();
    let j = p
        .replicate(
            Selector::parse("/doomed", "/hub").unwrap(),
            vec!["n3".into()],
            2,
        )
        .await
        .unwrap();
    c.eventually("job", Duration::from_secs(10), |c| {
        c.node(1).placer.job(j).is_some_and(|x| x.finished)
    })
    .await;

    // What host 3's disk durably holds.
    c.stop(3).await;
    let durable = c.save_disk(3);
    c.restart(3).await;
    c.eventually("n3 serving", Duration::from_secs(8), |c| {
        c.node(3).data.caught_up()
    })
    .await;

    // After that point: a file only on host 3, a copy onto host 3, and a
    // delete of a file host 3 held.
    let v3 = c.node(3).vfs.clone();
    let only3 = write_file(&v3, b"only-on-3", 2000).await;
    let copied = write_file(&v1, b"copied", 3000).await;
    c.eventually("stable", Duration::from_secs(5), |c| {
        [only3, copied].iter().all(|f| {
            c.attr(1, *f)
                .is_some_and(|a| a.gen_state == GenState::Stable)
        })
    })
    .await;
    let j = p
        .replicate(
            Selector::parse("/copied", "/hub").unwrap(),
            vec!["n3".into()],
            2,
        )
        .await
        .unwrap();
    c.eventually("job", Duration::from_secs(10), |c| {
        c.node(1).placer.job(j).is_some_and(|x| x.finished)
    })
    .await;
    v1.unlink(FileId::ROOT, b"doomed").await.unwrap();
    c.converge().await;

    c.power_loss(3, Some(&durable)).await;
    c.restart(3).await;
    let n3 = c.node(3);
    assert!(n3.prior.dirty());
    let rep = n3
        .recovery
        .clone()
        .expect("a crashed host runs the recovery fsck");
    let find =
        |pred: &dyn Fn(&Issue) -> bool| rep.findings.iter().find(|f| pred(&f.issue)).cloned();
    // Written only on host 3 after its durable point: gone (catching up
    // recreates an empty working object, which fsck reports as damaged).
    let lost = find(&|i| {
        matches!(i, Issue::Missing { file, .. } | Issue::Damaged { file, .. } if *file == only3.0)
    })
    .expect("only-on-3");
    assert!(matches!(lost.action, Action::Lost { .. }), "{lost:?}");
    let retired =
        find(&|i| matches!(i, Issue::Missing { file, .. } if *file == copied.0)).expect("copied");
    assert!(matches!(retired.action, Action::Retired), "{retired:?}");
    // The deleted file's object went with the replayed delete.
    let k = nest_store::ObjectKey::new(doomed, Generation(1));
    assert!(!n3.data.store().exists(k));

    // Host 3 agrees with the others and serves again.
    c.converge().await;
    assert!(c.lookup(3, FileId::ROOT, "doomed").is_none());
    assert!(c.lookup(3, FileId::ROOT, "copied").is_some());
    assert!(c.node(3).data.lease_valid());
    let (fh, _) = v1.open(copied, 0).await.unwrap();
    assert_eq!(v1.read(fh, 0, 10).await.unwrap(), vec![7u8; 10]);
    v1.release(fh, None).await;

    // An object nothing refers to: an online fsck quarantines it.
    let stray = nest_store::ObjectKey::new(FileId(9_999_999), Generation(3));
    std::fs::write(c.node(3).data.store().path(stray), b"stray bytes").unwrap();
    let rep = c
        .node(3)
        .vfs
        .fsck(&nest_data::fsck::FsckOptions {
            repair: true,
            orphans: nest_data::fsck::Orphans::Quarantine,
            deep: false,
            settle_owned: false,
            min_age: Duration::ZERO,
            host: "n3".into(),
            stamp: "run1".into(),
        })
        .await
        .unwrap();
    let q = rep
        .findings
        .iter()
        .find(|f| {
            matches!(
                f.issue,
                Issue::Orphan {
                    file: 9_999_999,
                    ..
                }
            )
        })
        .expect("stray");
    let Action::Quarantined { to } = &q.action else {
        panic!("{q:?}")
    };
    assert_eq!(to, "/.lost+found/run1/n3/unknown/000000000098967f.3");
    c.converge().await;
    let lf = c.lookup(1, FileId::ROOT, ".lost+found").unwrap();
    let run = c.lookup(1, lf, "run1").unwrap();
    let host = c.lookup(1, run, "n3").unwrap();
    let dir = c.lookup(1, host, "unknown").unwrap();
    let f = c.lookup(1, dir, "000000000098967f.3").unwrap();
    let (fh, _) = v1.open(f, 0).await.unwrap();
    assert_eq!(v1.read(fh, 0, 64).await.unwrap(), b"stray bytes");
    v1.release(fh, None).await;
}
