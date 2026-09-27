//! `nest hf import`: Hugging Face caches in either layout land in the hub
//! as huggingface_hub would write them, verified, sources kept unless moved.

use nest_place::Selector;
use nest_place::hfimport::HfImportOptions;
use nest_testkit::TestCluster;
use nest_types::*;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
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

async fn wait_job(c: &TestCluster, id: u64) -> nest_place::placer::ClusterJob {
    c.eventually("job finished", Duration::from_secs(30), |c| {
        c.node(1).placer.job(id).is_some_and(|j| j.finished)
    })
    .await;
    c.node(1).placer.job(id).unwrap()
}

fn write(p: &Path, body: &[u8]) {
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, body).unwrap();
}

/// A classic-layout model and an hf 2.0 (shared store) dataset.
fn fake_cache(root: &Path) -> PathBuf {
    let hub = root.join("hub");
    let m = hub.join("models--org--old");
    write(&m.join("blobs/etag-weights"), &vec![3u8; 3 << 20]);
    write(&m.join("blobs/etag-config"), b"{\"a\":1}");
    write(&m.join("blobs/etag-unused"), b"old revision leftovers");
    std::fs::create_dir_all(m.join("snapshots/c1/sub")).unwrap();
    symlink("../../blobs/etag-weights", m.join("snapshots/c1/model.bin")).unwrap();
    symlink(
        "../../../blobs/etag-config",
        m.join("snapshots/c1/sub/config.json"),
    )
    .unwrap();
    write(&m.join("refs/main"), b"c1");
    // hf 2.0: repo blob entries link into the shared store.
    let d = hub.join("datasets--org--new");
    write(&hub.join("blobs/ab/abcdef"), &vec![5u8; 1 << 20]);
    std::fs::create_dir_all(d.join("blobs")).unwrap();
    symlink("../../blobs/ab/abcdef", d.join("blobs/etag-data")).unwrap();
    std::fs::create_dir_all(d.join("snapshots/c2")).unwrap();
    symlink(
        "../../blobs/etag-data",
        d.join("snapshots/c2/train.parquet"),
    )
    .unwrap();
    write(&d.join("refs/main"), b"c2");
    root.to_path_buf()
}

async fn read_path(c: &TestCluster, path: &str) -> Vec<u8> {
    let conn = c.node(1).meta.open_reader().unwrap();
    // Follow links the way a reader through the mount would.
    let mut p = path.to_string();
    let id = loop {
        let (id, dir) = nest_place::selector::resolve_path(&conn, &p).unwrap();
        match nest_meta::query::readlink(&conn, id).unwrap() {
            Some(t) if !t.is_empty() => {
                let base = nest_meta::query::path_of(&conn, dir)
                    .unwrap()
                    .unwrap_or_default();
                p = format!(
                    "{}/{}",
                    String::from_utf8_lossy(&base).trim_end_matches('/'),
                    String::from_utf8_lossy(&t)
                );
            }
            _ => break id,
        }
    };
    let v = c.node(1).vfs.clone();
    let (fh, _) = v.open(id, 0).await.unwrap();
    let mut out = Vec::new();
    loop {
        let b = v.read(fh, out.len() as u64, 1 << 20).await.unwrap();
        if b.is_empty() {
            break;
        }
        out.extend(b);
    }
    v.release(fh, None).await;
    out
}

fn opts(src: PathBuf, mv: bool, mount_hub: Option<PathBuf>) -> HfImportOptions {
    HfImportOptions {
        src,
        hub: "/hub".into(),
        mount_hub,
        hf: None,
        r#move: mv,
        copy: true,
        spread: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn both_layouts_import_verified_and_move_leaves_a_link() {
    let c = ready(2).await;
    let p = c.node(1).placer.clone();
    // Beside the state directory: the same filesystem, so blobs hard-link.
    let root = c.state_dir(1).parent().unwrap().join("hf-home");
    let src = fake_cache(&root);

    let job = wait_job(&c, p.hf_import(opts(src.clone(), false, None))).await;
    assert!(job.error.is_none(), "{job:?}");
    assert_eq!(job.notes.len(), 2, "{:?}", job.notes);
    assert!(
        job.notes.iter().all(|n| n.contains("mirrored")),
        "{:?}",
        job.notes
    );

    // Blobs are regular files where hf looks; snapshots resolve; refs kept.
    assert_eq!(
        read_path(&c, "/hub/models--org--old/snapshots/c1/model.bin").await,
        vec![3u8; 3 << 20]
    );
    assert_eq!(
        read_path(&c, "/hub/models--org--old/snapshots/c1/sub/config.json").await,
        b"{\"a\":1}"
    );
    assert_eq!(
        read_path(&c, "/hub/models--org--old/refs/main").await,
        b"c1"
    );
    assert_eq!(
        read_path(&c, "/hub/datasets--org--new/snapshots/c2/train.parquet").await,
        vec![5u8; 1 << 20]
    );
    let conn = c.node(1).meta.open_reader().unwrap();
    let (blob, _) =
        nest_place::selector::resolve_path(&conn, "/hub/datasets--org--new/blobs/etag-data")
            .unwrap();
    let a = nest_meta::query::getattr(&conn, blob).unwrap().unwrap();
    assert_eq!(
        a.kind,
        FileKind::Regular,
        "the shared-store link was followed"
    );
    assert!(a.sealed);
    assert!(
        nest_place::selector::resolve_path(&conn, "/hub/models--org--old/blobs/etag-unused")
            .is_err(),
        "blobs no snapshot uses are left behind"
    );
    // Complete once the refs just written settle (a short linger).
    for sel in ["hf:org/old", "hf-dataset:org/new"] {
        let sel = Selector::parse(sel, "/hub").unwrap();
        let mut r = Vec::new();
        for _ in 0..50 {
            r = p.readiness(&sel, &["n1".into()]).await.unwrap().1;
            if r[0].ready {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(r[0].ready, "{sel:?}: {r:?}");
    }
    assert!(
        src.join("hub/models--org--old/blobs/etag-weights")
            .is_file(),
        "source untouched"
    );

    // Again with --move: nothing new to place, and the cache becomes a link.
    let mount_hub = PathBuf::from("/mnt/sparknest/hub");
    let job = wait_job(
        &c,
        p.hf_import(opts(src.clone(), true, Some(mount_hub.clone()))),
    )
    .await;
    assert!(job.error.is_none(), "{job:?}");
    assert!(
        job.notes.iter().all(|n| n.contains("moved")),
        "{:?}",
        job.notes
    );
    let hub = src.join("hub");
    assert_eq!(std::fs::read_link(&hub).unwrap(), mount_hub);
    let leftovers: Vec<_> = std::fs::read_dir(&src)
        .unwrap()
        .flatten()
        .map(|e| e.file_name())
        .collect();
    assert_eq!(
        leftovers,
        [std::ffi::OsString::from("hub")],
        "old tree removed"
    );
    // The data lives on in the store (the import held its own link).
    assert_eq!(
        read_path(&c, "/hub/models--org--old/snapshots/c1/model.bin").await,
        vec![3u8; 3 << 20]
    );
}

/// From another filesystem the blobs are copied into the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn another_filesystem_is_copied() {
    let shm = Path::new("/dev/shm");
    if !shm.is_dir() {
        return;
    }
    let c = ready(1).await;
    use std::os::unix::fs::MetadataExt;
    if std::fs::metadata(shm).unwrap().dev() == std::fs::metadata(c.state_dir(1)).unwrap().dev() {
        return; // same filesystem here: nothing to prove
    }
    let root = shm.join(format!("nest-hf-{}", std::process::id()));
    let src = fake_cache(&root);
    let p = c.node(1).placer.clone();
    let mut o = opts(src.clone(), false, None);
    o.copy = false;
    let job = wait_job(&c, p.hf_import(o)).await;
    assert!(
        job.notes.iter().any(|n| n.contains("failed")),
        "without copy, another filesystem fails: {:?}",
        job.notes
    );
    let job = wait_job(&c, p.hf_import(opts(src.clone(), false, None))).await;
    assert!(job.error.is_none(), "{job:?}");
    assert!(
        job.notes.iter().all(|n| n.contains("mirrored")),
        "{:?}",
        job.notes
    );
    assert_eq!(
        read_path(&c, "/hub/models--org--old/snapshots/c1/model.bin").await,
        vec![3u8; 3 << 20]
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A spread import leaves exactly one copy of each blob, handed out over
/// the hosts (the importing node keeps only what it won), all readable.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn spread_import_leaves_one_copy_spread_over_the_hosts() {
    let c = ready(3).await;
    let p = c.node(1).placer.clone();
    let root = c.state_dir(1).parent().unwrap().join("hf-spread");
    let m = root.join("hub/models--org--big");
    std::fs::create_dir_all(m.join("snapshots/c1")).unwrap();
    for i in 0..12u8 {
        write(
            &m.join(format!("blobs/etag-{i}")),
            &vec![i; (1 << 20) + i as usize],
        );
        symlink(
            format!("../../blobs/etag-{i}"),
            m.join(format!("snapshots/c1/shard-{i}.bin")),
        )
        .unwrap();
    }
    write(&m.join("refs/main"), b"c1");
    let mut o = opts(root.clone(), false, None);
    o.spread = true;
    let job = wait_job(&c, p.hf_import(o)).await;
    assert!(job.error.is_none(), "{job:?}");
    let conn = c.node(1).meta.open_reader().unwrap();
    let mut hosts = std::collections::HashSet::new();
    for i in 0..12u8 {
        let path = format!("/hub/models--org--big/blobs/etag-{i}");
        let (id, _) = nest_place::selector::resolve_path(&conn, &path).unwrap();
        let a = nest_meta::query::getattr(&conn, id).unwrap().unwrap();
        let live: Vec<u64> = nest_meta::query::replicas(&conn, id)
            .unwrap()
            .into_iter()
            .filter(|r| r.generation == a.generation && r.state == ReplicaState::Live)
            .map(|r| r.store.0)
            .collect();
        assert_eq!(live.len(), 1, "{path}: {live:?}");
        hosts.insert(live[0]);
        assert_eq!(
            read_path(
                &c,
                &format!("/hub/models--org--big/snapshots/c1/shard-{i}.bin")
            )
            .await,
            vec![i; (1 << 20) + i as usize]
        );
    }
    assert!(hosts.len() >= 2, "all blobs on {hosts:?}");
    let ip = p.import_progress(job.id).unwrap();
    assert!(ip.spread_files > 0, "{ip:?}");
    assert_eq!(ip.spread_files + ip.kept_files, 12, "{ip:?}");
}
