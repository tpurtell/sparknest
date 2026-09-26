//! Milestone 6: archive stores, offload, reads through gateways, recall.

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

fn stores_of(c: &TestCluster, f: FileId) -> Vec<u64> {
    let r = c.node(1).meta.open_reader().unwrap();
    nest_meta::query::replicas(&r, f)
        .unwrap()
        .iter()
        .map(|r| r.store.0)
        .collect()
}

async fn wait_job(c: &TestCluster, id: u64) -> nest_place::placer::ClusterJob {
    c.eventually("job finished", Duration::from_secs(20), |c| {
        c.node(1).placer.job(id).is_some_and(|j| j.finished)
    })
    .await;
    c.node(1).placer.job(id).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn offload_read_through_gateway_recall_and_invalidate() {
    let c = ready(3).await;
    let p = &c.node(1).placer;
    // A disk on n1 only (like raptor's /mnt/scratch).
    let scratch = c.state_dir(1).join("scratch-disk");
    std::fs::create_dir_all(&scratch).unwrap();
    let r = p
        .add_store("scratch", scratch.to_str().unwrap(), &["n1".into()])
        .await
        .unwrap();
    assert!(r.iter().all(|(_, r)| r.is_ok()), "{r:?}");
    let st = p.stores().await.unwrap();
    assert!(st[0].gateways[0].1.healthy);

    // A file living on n2 is offloaded to the scratch disk.
    let v2 = &c.node(2).vfs;
    let (a, fh, _) = v2
        .create(FileId::ROOT, b"old-model.bin", 0o644, oflags::WRONLY)
        .await
        .unwrap();
    v2.write(fh, 0, vec![9u8; 5 << 20]).await.unwrap();
    v2.release(fh, None).await;
    c.eventually("stable", Duration::from_secs(5), |c| {
        c.attr(1, a.id)
            .is_some_and(|x| x.gen_state == GenState::Stable)
    })
    .await;
    let sel = Selector::parse("/old-model.bin", "/hub").unwrap();
    let job = p.offload(sel.clone(), "scratch".into(), 4).await.unwrap();
    let j = wait_job(&c, job).await;
    assert!(j.error.is_none(), "{j:?}");
    c.converge().await;
    let scratch_id = st[0].id.0;
    assert_eq!(
        stores_of(&c, a.id),
        vec![scratch_id],
        "only the archive copy remains"
    );
    c.eventually("live copy deleted on n2", Duration::from_secs(3), |c| {
        !c.node(2)
            .data
            .store()
            .exists(nest_store::ObjectKey::new(a.id, a.generation))
    })
    .await;

    // n3 has no copy: it reads through the gateway (n1).
    let v3 = &c.node(3).vfs;
    let (rfh, _) = v3.open(a.id, 0).await.unwrap();
    let b = v3.read(rfh, (5 << 20) - 4, 16).await.unwrap();
    assert_eq!(b, vec![9u8; 4]);
    v3.release(rfh, None).await;

    // Recall onto n3.
    let job = p
        .replicate(sel.clone(), vec!["n3".into()], 4)
        .await
        .unwrap();
    assert!(wait_job(&c, job).await.error.is_none());
    c.converge().await;
    let mut s = stores_of(&c, a.id);
    s.sort();
    assert_eq!(s, vec![3, scratch_id]);

    // Writing it invalidates the archive copy; the gateway deletes it.
    let (wfh, _) = v3.open(a.id, oflags::RDWR).await.unwrap();
    v3.write(wfh, 0, b"new".to_vec()).await.unwrap();
    v3.release(wfh, None).await;
    let archived = scratch
        .join("objects")
        .join(format!("{:02x}", a.id.0 & 0xff))
        .join(format!("{:016x}.{:x}", a.id.0, a.generation.0));
    c.eventually("stale archive copy removed", Duration::from_secs(3), |_| {
        !archived.exists()
    })
    .await;

    // An unmounted store (marker gone) is unhealthy and never used.
    std::fs::remove_file(scratch.join(nest_store::MARKER)).unwrap();
    let st = p.stores().await.unwrap();
    assert!(!st[0].gateways[0].1.healthy);
    assert!(
        p.add_store("nowhere", "/definitely/not/mounted", &["n1".into()])
            .await
            .unwrap()[0]
            .1
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shared_store_with_several_gateways() {
    let c = ready(3).await;
    let p = &c.node(1).placer;
    // Like the NAS share mounted on every node.
    let nas = c.state_dir(1).join("nas-share");
    std::fs::create_dir_all(&nas).unwrap();
    let r = p
        .add_store("nas", nas.to_str().unwrap(), &["@all".into()])
        .await
        .unwrap();
    assert_eq!(r.len(), 3);
    assert!(r.iter().all(|(_, r)| r.is_ok()), "{r:?}");
    let v1 = &c.node(1).vfs;
    let (a, fh, _) = v1
        .create(FileId::ROOT, b"f", 0o644, oflags::WRONLY)
        .await
        .unwrap();
    v1.write(fh, 0, vec![1u8; 1 << 20]).await.unwrap();
    v1.release(fh, None).await;
    c.eventually("stable", Duration::from_secs(5), |c| {
        c.attr(1, a.id)
            .is_some_and(|x| x.gen_state == GenState::Stable)
    })
    .await;
    let job = p
        .offload(Selector::parse("/f", "/hub").unwrap(), "nas".into(), 2)
        .await
        .unwrap();
    assert!(wait_job(&c, job).await.error.is_none());
    c.converge().await;
    // Any node reads it: each is itself a gateway of the share.
    for n in 1..=3 {
        let v = &c.node(n).vfs;
        let (fh, _) = v.open(a.id, 0).await.unwrap();
        assert_eq!(v.read(fh, 0, 4).await.unwrap(), vec![1u8; 4], "node {n}");
        v.release(fh, None).await;
    }
    let (_, ready) = p
        .readiness(&Selector::parse("/f", "/hub").unwrap(), &["nas".into()])
        .await
        .unwrap();
    assert!(ready[0].ready);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn backups_survive_edits_restore_and_export_offline() {
    let c = ready(3).await;
    let p = &c.node(1).placer;
    let nas = c.state_dir(1).join("nas-backups");
    std::fs::create_dir_all(&nas).unwrap();
    p.add_store("nas", nas.to_str().unwrap(), &["@all".into()])
        .await
        .unwrap();

    // A small HF-shaped tree on n2: a blob, a snapshot symlink, a ref.
    let v2 = &c.node(2).vfs;
    let repo = v2.mkdir(FileId::ROOT, b"repo", 0o755).await.unwrap().id;
    let blobs = v2.mkdir(repo, b"blobs", 0o755).await.unwrap().id;
    let snap = v2.mkdir(repo, b"snap", 0o755).await.unwrap().id;
    let write = |parent: FileId, name: &'static [u8], data: Vec<u8>| {
        let v2 = v2.clone();
        async move {
            let (a, fh, _) = v2
                .create(parent, name, 0o644, oflags::WRONLY | oflags::EXCL)
                .await
                .unwrap();
            v2.write(fh, 0, data).await.unwrap();
            v2.release(fh, None).await;
            a.id
        }
    };
    let blob = write(blobs, b"abc123", vec![5u8; 3 << 20]).await;
    let refm = write(repo, b"main", b"v1".to_vec()).await;
    v2.symlink(snap, b"model.bin", b"../blobs/abc123")
        .await
        .unwrap();
    c.eventually("stable", Duration::from_secs(5), |c| {
        [blob, refm].iter().all(|f| {
            c.attr(1, *f)
                .is_some_and(|x| x.gen_state == GenState::Stable)
        })
    })
    .await;

    // Back up, then change the live tree.
    let job = p
        .backup_create(
            Selector::parse("/repo", "/hub").unwrap(),
            "before-edit".into(),
            "nas".into(),
        )
        .await
        .unwrap();
    let j = wait_job(&c, job).await;
    assert!(j.error.is_none(), "{j:?}");
    let backups = p.backups().unwrap();
    assert_eq!((backups.len(), backups[0].files), (1, 2));
    let (fh, _) = v2.open(refm, oflags::WRONLY | oflags::TRUNC).await.unwrap();
    v2.write(fh, 0, b"v2".to_vec()).await.unwrap();
    v2.release(fh, None).await;

    // Restore elsewhere: the old bytes and the symlink come back.
    let job = p
        .backup_restore(backups[0].id, "/restored".into())
        .await
        .unwrap();
    let j = wait_job(&c, job).await;
    assert!(j.error.is_none(), "{j:?}");
    c.converge().await;
    let v3 = &c.node(3).vfs;
    let read = |path: &'static str| {
        let c = &c;
        async move {
            let r = c.node(3).meta.open_reader().unwrap();
            let (id, _) = nest_place::selector::resolve_path(&r, path).unwrap();
            drop(r);
            let (fh, _) = v3.open(id, 0).await.unwrap();
            let b = v3.read(fh, 0, 4 << 20).await.unwrap();
            v3.release(fh, None).await;
            b
        }
    };
    assert_eq!(read("/restored/repo/main").await, b"v1");
    assert_eq!(
        read("/restored/repo/blobs/abc123").await,
        vec![5u8; 3 << 20]
    );
    {
        let r = c.node(3).meta.open_reader().unwrap();
        let (l, _) =
            nest_place::selector::resolve_path(&r, "/restored/repo/snap/model.bin").unwrap();
        assert_eq!(
            nest_meta::query::readlink(&r, l).unwrap().unwrap(),
            b"../blobs/abc123"
        );
    }
    assert_eq!(
        read("/repo/main").await,
        b"v2",
        "live tree untouched by restore"
    );

    // A second backup reuses the unchanged blob; deleting the first keeps it.
    let job = p
        .backup_create(
            Selector::parse("/repo", "/hub").unwrap(),
            "after-edit".into(),
            "nas".into(),
        )
        .await
        .unwrap();
    assert!(wait_job(&c, job).await.error.is_none());
    let removed = p.backup_delete(backups[0].id).await.unwrap();
    assert_eq!(removed, 1, "only the old ref generation is unreferenced");

    // Offline export from a metadata snapshot and raw object directories.
    let snap_path = p.meta_snapshot("nas").await.unwrap();
    let snap_file = snap_path.split_once(':').unwrap().1.to_string();
    let out = c.state_dir(1).join("exported");
    let r = nest_testkit::sparknestd::export::run(&nest_testkit::sparknestd::export::ExportArgs {
        meta: snap_file.into(),
        objects: vec![c.state_dir(1), c.state_dir(2), c.state_dir(3), nas.clone()],
        out: out.clone(),
        path: "/repo".into(),
        link: true,
    })
    .unwrap();
    assert!(r.missing.is_empty(), "{:?}", r.missing);
    assert_eq!(std::fs::read(out.join("main")).unwrap(), b"v2");
    assert_eq!(
        std::fs::read(out.join("snap/model.bin")).unwrap(),
        vec![5u8; 3 << 20]
    );
    assert_eq!(
        std::fs::read_link(out.join("snap/model.bin")).unwrap(),
        std::path::PathBuf::from("../blobs/abc123")
    );
}
