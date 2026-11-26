//! A host's copies in a folder of their own (`node.data_dir`, ADR-094),
//! and the per-host limit on copies written at once (`disk_transfers`).

use nest_data::vfs::oflags;
use nest_place::Selector;
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

async fn write(c: &TestCluster, name: &str, byte: u8, len: usize) -> FileId {
    write_in(c, FileId::ROOT, name, byte, len).await
}

async fn write_in(c: &TestCluster, dir: FileId, name: &str, byte: u8, len: usize) -> FileId {
    let v = c.node(1).vfs.clone();
    let (a, fh, _) = v
        .create(dir, name.as_bytes(), 0o644, oflags::WRONLY)
        .await
        .unwrap();
    v.write(fh, 0, vec![byte; len]).await.unwrap();
    v.release(fh, None).await;
    c.eventually("stable", Duration::from_secs(5), |c| {
        c.attr(1, a.id)
            .is_some_and(|x| x.gen_state == GenState::Stable)
    })
    .await;
    a.id
}

async fn read_all(v: &nest_data::Vfs, f: FileId) -> Vec<u8> {
    let (fh, _) = v.open(f, 0).await.unwrap();
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

async fn copy_to(c: &TestCluster, path: &str, host: &str, parallel: usize) {
    let p = c.node(1).placer.clone();
    let job = p
        .replicate(
            Selector::parse(path, "/hub").unwrap(),
            vec![host.into()],
            parallel,
        )
        .await
        .unwrap();
    c.eventually("job finished", Duration::from_secs(30), |c| {
        c.node(1).placer.job(job).is_some_and(|j| j.finished)
    })
    .await;
    let j = c.node(1).placer.job(job).unwrap();
    assert!(
        j.error.is_none() && j.hosts.values().all(|h| h.failed.is_empty()),
        "{j:?}"
    );
}

fn has_copy(c: &TestCluster, f: FileId, host: u64) -> bool {
    let r = c.node(1).meta.open_reader().unwrap();
    nest_meta::query::replicas(&r, f)
        .unwrap()
        .iter()
        .any(|r| r.store == NodeId(host).live_store())
}

fn objects_in(dir: &std::path::Path) -> usize {
    (0..=255u8)
        .map(|i| {
            std::fs::read_dir(dir.join("objects").join(format!("{i:02x}")))
                .map(|d| d.count())
                .unwrap_or(0)
        })
        .sum()
}

fn mv(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to.parent().unwrap()).unwrap();
    std::fs::rename(from, to).unwrap();
}

/// What aviary will do: a host that ran with everything in its state dir
/// is stopped, its `objects/`, `staging/` and marker move to a folder of
/// their own, and it starts with `data_dir` there. It serves its copies,
/// takes new ones into the new folder, and keeps its own state (HOLD-free,
/// usage, backups' scratch) in the state dir.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_host_moves_its_copies_to_a_folder_of_their_own() {
    let mut c = ready(3).await;
    let f1 = write(&c, "before", 1, 3 << 20).await;
    copy_to(&c, "/before", "n3", 4).await;
    assert!(has_copy(&c, f1, 3));
    let state = c.state_dir(3);
    // A 0.9.4 start marks the folder holding its objects.
    let marker = std::fs::read_to_string(state.join(nest_data::HOST_MARKER)).unwrap();
    assert_eq!(marker.trim(), "cluster=testkit host=3");

    c.stop(3).await;
    c.split_data(3);
    let data = c.data_dir(3);
    assert_ne!(data, state);
    std::fs::create_dir_all(&data).unwrap();
    for name in ["objects", "staging", nest_data::HOST_MARKER] {
        mv(&state.join(name), &data.join(name));
    }
    c.restart(3).await;
    c.eventually("n3 serving", Duration::from_secs(8), |c| {
        c.node(3).data.caught_up() && c.node(3).data.session().is_some()
    })
    .await;
    // Nothing was lost on the way: the copy is still listed, and read from
    // the new folder.
    assert!(has_copy(&c, f1, 3));
    assert!(!c.node(3).data.held(), "a moved folder is the host's own");
    assert_eq!(c.node(3).data.store().root(), data.as_path());
    assert_eq!(c.node(3).data.state_dir(), state.as_path());
    assert_eq!(read_all(&c.node(3).vfs, f1).await, vec![1u8; 3 << 20]);
    // New copies land in the new folder only.
    let f2 = write(&c, "after", 2, 2 << 20).await;
    copy_to(&c, "/after", "n3", 4).await;
    assert!(has_copy(&c, f2, 3));
    assert_eq!(objects_in(&data), 2);
    assert!(
        !state.join("objects").exists(),
        "no objects dir left in the state dir"
    );
    // State other than copies stays in the state dir.
    assert!(state.join("meta.sqlite").exists() && state.join("raft.sqlite").exists());
    assert!(!data.join("meta.sqlite").exists());
    c.eventually("usage in the state dir", Duration::from_secs(10), |_| {
        state.join("usage.sqlite").exists()
    })
    .await;
    assert!(!data.join("usage.sqlite").exists());
}

/// A host with metadata never starts on a data dir without its marker (an
/// unmounted disk's empty mountpoint, or the wrong folder): every copy it
/// lists would look lost and be retired.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_data_dir_without_the_hosts_marker_is_refused() {
    let mut c = ready(3).await;
    let f = write(&c, "kept", 3, 2 << 20).await;
    copy_to(&c, "/kept", "n3", 4).await;
    c.stop(3).await;
    let state = c.state_dir(3);
    c.split_data(3);
    let data = c.data_dir(3);
    // "Unmounted": the folder is not there at all.
    let e = c.try_restart(3).await.unwrap_err();
    assert!(format!("{e:#}").contains("is its disk mounted"), "{e:#}");
    assert!(!data.exists(), "nothing is created on a refused start");
    // The objects moved, but the marker was left behind: refused too.
    std::fs::create_dir_all(&data).unwrap();
    mv(&state.join("objects"), &data.join("objects"));
    assert!(c.try_restart(3).await.is_err());
    // Another host's marker: refused.
    std::fs::write(
        data.join(nest_data::HOST_MARKER),
        "cluster=testkit host=2\n",
    )
    .unwrap();
    let e = c.try_restart(3).await.unwrap_err();
    assert!(format!("{e:#}").contains("another host's"), "{e:#}");
    // Its own: it starts, and the copy was never retired.
    mv(
        &state.join(nest_data::HOST_MARKER),
        &data.join(nest_data::HOST_MARKER),
    );
    c.restart(3).await;
    c.eventually("n3 serving", Duration::from_secs(8), |c| {
        c.node(3).data.caught_up() && c.node(3).data.session().is_some()
    })
    .await;
    assert!(has_copy(&c, f, 3));
    assert_eq!(read_all(&c.node(3).vfs, f).await, vec![3u8; 2 << 20]);
}

/// A new host can start split from the first: its empty data dir is marked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_host_marks_its_data_dir() {
    let mut c = ready(3).await;
    c.stop(3).await;
    c.wipe(3);
    c.split_data(3);
    c.restart(3).await;
    c.eventually("n3 serving", Duration::from_secs(10), |c| {
        c.node(3).data.caught_up() && c.node(3).data.session().is_some()
    })
    .await;
    let data = c.data_dir(3);
    let m = std::fs::read_to_string(data.join(nest_data::HOST_MARKER)).unwrap();
    assert_eq!(m.trim(), "cluster=testkit host=3");
    assert!(!c.state_dir(3).join(nest_data::HOST_MARKER).exists());
    let f = write(&c, "fresh", 4, 1 << 20).await;
    copy_to(&c, "/fresh", "n3", 4).await;
    assert!(has_copy(&c, f, 3));
    assert_eq!(objects_in(&data), 1);
}

/// `disk_transfers` caps the copies written into a host's disk at once,
/// whatever each job's `parallel` asks for; the host's other jobs wait
/// their turn and every copy still lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disk_transfers_caps_copies_written_at_once() {
    let mut c = ready(3).await;
    c.stop(3).await;
    c.set_disk_transfers(3, 1);
    c.restart(3).await;
    c.eventually("n3 serving", Duration::from_secs(8), |c| {
        c.node(3).data.caught_up() && c.node(3).data.session().is_some()
    })
    .await;
    assert_eq!(c.node(3).data.disk_transfers(), 1);
    assert_eq!(c.node(2).data.disk_transfers(), nest_data::DISK_TRANSFERS);
    let v = c.node(1).vfs.clone();
    let (da, db) = (
        v.mkdir(FileId::ROOT, b"a", 0o755).await.unwrap().id,
        v.mkdir(FileId::ROOT, b"b", 0o755).await.unwrap().id,
    );
    let mut files = Vec::new();
    for i in 0..6u8 {
        let dir = if i < 3 { da } else { db };
        files.push(write_in(&c, dir, &format!("f{i}"), i + 1, 4 << 20).await);
    }
    // Watch n3's staging dir while two jobs, 8 files wide each, copy there.
    let staging = c.data_dir(3).join("staging");
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let most = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let watcher = {
        let (stop, most, staging) = (stop.clone(), most.clone(), staging.clone());
        std::thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let n = std::fs::read_dir(&staging).map(|d| d.count()).unwrap_or(0);
                most.fetch_max(n, std::sync::atomic::Ordering::Relaxed);
                std::thread::sleep(Duration::from_micros(200));
            }
        })
    };
    let p = c.node(1).placer.clone();
    let a = p
        .replicate(Selector::parse("/a", "/hub").unwrap(), vec!["n3".into()], 8)
        .await
        .unwrap();
    let b = p
        .replicate(Selector::parse("/b", "/hub").unwrap(), vec!["n3".into()], 8)
        .await
        .unwrap();
    c.eventually("both jobs finished", Duration::from_secs(30), |c| {
        [a, b]
            .iter()
            .all(|j| c.node(1).placer.job(*j).is_some_and(|j| j.finished))
    })
    .await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    watcher.join().unwrap();
    for j in [a, b] {
        let j = c.node(1).placer.job(j).unwrap();
        assert!(j.error.is_none(), "{j:?}");
    }
    for f in &files {
        assert!(has_copy(&c, *f, 3));
    }
    let most = most.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        most <= 1,
        "{most} copies were being written into n3 at once"
    );
}
