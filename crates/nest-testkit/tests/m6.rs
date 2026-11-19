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

fn free_goal(free: &[(String, u64)], archives: &[&str]) -> nest_place::plan::Goal {
    nest_place::plan::Goal::Free {
        free: free.to_vec(),
        archives: archives.iter().map(|s| s.to_string()).collect(),
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn free_space_plan_evicts_redundant_then_offloads_and_respects_rules() {
    let c = ready(3).await;
    let p = &c.node(1).placer;
    let nas = c.state_dir(2).join("nas-plan");
    std::fs::create_dir_all(&nas).unwrap();
    p.add_store("nas", nas.to_str().unwrap(), &["n2".into()])
        .await
        .unwrap();
    p.set_group("both", vec!["n1".into(), "n2".into()])
        .await
        .unwrap();

    let v1 = &c.node(1).vfs;
    let size = 32u64 << 20;
    let mk = |name: &'static [u8]| {
        let v1 = v1.clone();
        async move {
            let (a, fh, _) = v1
                .create(FileId::ROOT, name, 0o644, oflags::WRONLY)
                .await
                .unwrap();
            v1.write(fh, 0, vec![1u8; size as usize]).await.unwrap();
            v1.release(fh, None).await;
            a.id
        }
    };
    let redundant = mk(b"redundant").await;
    let only = mk(b"only").await;
    let pinned = mk(b"pinned").await;
    c.eventually("stable", Duration::from_secs(5), |c| {
        [redundant, only, pinned].iter().all(|f| {
            c.attr(1, *f)
                .is_some_and(|x| x.gen_state == GenState::Stable)
        })
    })
    .await;
    // `redundant` also lives on n2; `pinned` is required on n1 by a rule.
    let j = p
        .replicate(
            Selector::parse("/redundant", "/hub").unwrap(),
            vec!["n2".into()],
            2,
        )
        .await
        .unwrap();
    assert!(wait_job(&c, j).await.error.is_none());
    p.set_rule(
        "pin",
        &nest_place::RuleSpec {
            selector: Selector::parse("/pinned", "/hub").unwrap(),
            hosts: vec!["@both".into()],
            auto: false,
        },
    )
    .await
    .unwrap();
    c.converge().await;

    let free_now = p
        .status()
        .await
        .unwrap()
        .iter()
        .find(|s| s.name == "n1")
        .unwrap()
        .info
        .as_ref()
        .unwrap()
        .free_bytes;
    let target = [("n1".into(), free_now + 2 * size - (1 << 20))];
    // With no archive chosen, only the redundant copy goes; the sole copy
    // is reported, not offloaded.
    let plan = p.plan(free_goal(&target, &[])).await.unwrap();
    assert!(!plan.feasible, "{plan:?}");
    assert!(plan.archives.is_empty());
    assert_eq!(plan.steps.len(), 1, "{plan:?}");
    assert!(matches!(
        plan.steps[0],
        nest_place::plan::Step::Evict { .. }
    ));
    assert!(
        plan.blocked[0].contains("no archive store was chosen"),
        "{:?}",
        plan.blocked
    );
    assert!(p.plan(free_goal(&target, &["nope"])).await.is_err());

    let plan = p.plan(free_goal(&target, &["nas"])).await.unwrap();
    assert!(plan.feasible, "{plan:?}");
    let a = &plan.archives[0];
    assert!(a.store == "nas" && a.reachable, "{a:?}");
    assert_eq!(a.adds, size, "the sole copy lands in the archive");
    assert_eq!(a.projected_free, a.free_now - size);
    let kinds: Vec<(&str, Vec<FileId>)> = plan
        .steps
        .iter()
        .map(|s| match s {
            nest_place::plan::Step::Evict { copies, .. } => {
                ("evict", copies.iter().map(|c| c.file).collect())
            }
            nest_place::plan::Step::Offload { copies, .. } => {
                ("offload", copies.iter().map(|c| c.file).collect())
            }
            nest_place::plan::Step::Replicate { copies, .. } => {
                ("replicate", copies.iter().map(|c| c.file).collect())
            }
        })
        .collect();
    assert_eq!(
        kinds,
        vec![("evict", vec![redundant]), ("offload", vec![only])],
        "{plan:?}"
    );

    let j = p.apply_plan(plan.id).await.unwrap();
    let job = wait_job(&c, j).await;
    assert!(job.error.is_none(), "{job:?}");
    c.converge().await;
    let nas_id = p.archive_stores().unwrap()[0].0.0;
    assert_eq!(stores_of(&c, redundant), vec![2]);
    assert_eq!(stores_of(&c, only), vec![nas_id]);
    assert!(
        stores_of(&c, pinned).contains(&1),
        "rule-required copy kept"
    );

    // A target that only removing the pinned file could meet is reported.
    let plan = p
        .plan(free_goal(&[("n1".into(), free_now + 100 * size)], &["nas"]))
        .await
        .unwrap();
    assert!(!plan.feasible);
    assert!(
        plan.blocked[0].contains("rule \"pin\""),
        "{:?}",
        plan.blocked
    );

    // A group a rule names cannot be deleted.
    let e = p.delete_group("both").await.unwrap_err();
    assert!(e.to_string().contains("pin"), "{e}");

    // A host named directly beats its group, whichever comes first.
    let plan = p
        .plan(free_goal(&[("n1".into(), 7), ("@both".into(), 9)], &[]))
        .await
        .unwrap();
    let t = |h: &str| plan.hosts.iter().find(|x| x.host == h).unwrap().target;
    assert_eq!((t("n1"), t("n2")), (7, 9));

    // A rule made between planning and applying is honoured: the copy stays.
    let late = mk(b"late").await;
    c.eventually("stable", Duration::from_secs(5), |c| {
        c.attr(1, late)
            .is_some_and(|x| x.gen_state == GenState::Stable)
    })
    .await;
    let j = p
        .replicate(
            Selector::parse("/late", "/hub").unwrap(),
            vec!["n2".into()],
            2,
        )
        .await
        .unwrap();
    assert!(wait_job(&c, j).await.error.is_none());
    c.converge().await;
    let free_now = p
        .status()
        .await
        .unwrap()
        .iter()
        .find(|s| s.name == "n1")
        .unwrap()
        .info
        .as_ref()
        .unwrap()
        .free_bytes;
    let plan = p
        .plan(free_goal(&[("n1".into(), free_now + size / 2)], &[]))
        .await
        .unwrap();
    assert_eq!(plan.steps.len(), 1, "{plan:?}");
    p.set_rule(
        "late",
        &nest_place::RuleSpec {
            selector: Selector::parse("/late", "/hub").unwrap(),
            hosts: vec!["n1".into()],
            auto: false,
        },
    )
    .await
    .unwrap();
    let job = wait_job(&c, p.apply_plan(plan.id).await.unwrap()).await;
    assert!(job.error.is_none(), "{job:?}");
    let failed = &job.hosts["n1"].failed;
    assert!(
        failed.len() == 1 && failed[0].1.contains("rule \"late\""),
        "{failed:?}"
    );
    c.converge().await;
    let mut st = stores_of(&c, late);
    st.sort();
    assert_eq!(st, vec![1, 2]);
}

/// A gateway reading for itself prefers a live copy on another node over
/// its local archive copy (fabric beats an archive disk), and falls back to
/// the archive when no live holder answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn gateway_prefers_live_copies_over_its_archive() {
    let mut c = ready(3).await;
    let p = c.node(1).placer.clone();
    let scratch = c.state_dir(1).join("scratch-disk");
    std::fs::create_dir_all(&scratch).unwrap();
    p.add_store("scratch", scratch.to_str().unwrap(), &["n1".into()])
        .await
        .unwrap();
    let v2 = c.node(2).vfs.clone();
    let (a, fh, _) = v2
        .create(FileId::ROOT, b"m.bin", 0o644, oflags::WRONLY)
        .await
        .unwrap();
    v2.write(fh, 0, vec![9u8; 1 << 20]).await.unwrap();
    v2.release(fh, None).await;
    c.eventually("stable", Duration::from_secs(5), |c| {
        c.attr(1, a.id)
            .is_some_and(|x| x.gen_state == GenState::Stable)
    })
    .await;
    let sel = Selector::parse("/m.bin", "/hub").unwrap();
    let j = p.replicate(sel, vec!["scratch".into()], 2).await.unwrap();
    assert!(wait_job(&c, j).await.error.is_none());
    c.converge().await;

    // Make the archive copy distinguishable: same size, different bytes.
    let name = format!("{:016x}.{:x}", a.id.0, a.generation.0);
    let obj = walk(&scratch)
        .into_iter()
        .find(|p| p.file_name().unwrap() == name.as_str())
        .unwrap();
    std::fs::write(&obj, vec![7u8; 1 << 20]).unwrap();

    let read_on_n1 = |c: &TestCluster| {
        let v1 = c.node(1).vfs.clone();
        async move {
            let (h, _) = v1.open(a.id, 0).await.unwrap();
            let b = v1.read(h, 0, 4096).await.unwrap();
            v1.release(h, None).await;
            b[0]
        }
    };
    assert_eq!(read_on_n1(&c).await, 9, "the live copy on n2 is preferred");
    c.stop(2).await;
    assert_eq!(
        read_on_n1(&c).await,
        7,
        "the archive serves when no live holder answers"
    );
}

fn walk(d: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(d).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else {
            out.push(p);
        }
    }
    out
}

/// Cancelling an offload stops new copies and never drops live copies.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cancelled_offload_keeps_every_live_copy() {
    let c = ready(2).await;
    let p = &c.node(1).placer;
    let nas = c.state_dir(2).join("nas-cancel");
    std::fs::create_dir_all(&nas).unwrap();
    p.add_store("nas", nas.to_str().unwrap(), &["n2".into()])
        .await
        .unwrap();
    let v = c.node(1).vfs.clone();
    let dir = v.mkdir(FileId::ROOT, b"many", 0o755).await.unwrap().id;
    let mut files = Vec::new();
    for i in 0..40 {
        let (a, fh, _) = v
            .create(dir, format!("f{i}").as_bytes(), 0o644, oflags::WRONLY)
            .await
            .unwrap();
        v.write(fh, 0, vec![i as u8; 4 << 20]).await.unwrap();
        v.release(fh, None).await;
        files.push(a.id);
    }
    c.eventually("stable", Duration::from_secs(10), |c| {
        files.iter().all(|f| {
            c.attr(1, *f)
                .is_some_and(|x| x.gen_state == GenState::Stable)
        })
    })
    .await;

    let id = p
        .offload(Selector::parse("/many", "/hub").unwrap(), "nas".into(), 1)
        .await
        .unwrap();
    p.cancel_job(id).await.unwrap();
    let job = wait_job(&c, id).await;
    assert!(job.cancelled, "{job:?}");
    assert!(
        job.error
            .as_deref()
            .is_some_and(|e| e.contains("cancelled")),
        "{job:?}"
    );
    let nas_id = p.archive_stores().unwrap()[0].0.0;
    let archived = files
        .iter()
        .filter(|f| stores_of(&c, **f).contains(&nas_id))
        .count();
    assert!(archived < files.len(), "stopped before copying everything");
    for f in &files {
        assert!(stores_of(&c, *f).contains(&1), "live copy of {f:?} kept");
    }
    // A finished job cannot be cancelled.
    assert!(p.cancel_job(id).await.is_err());
}

/// A store added before its folder existed (the gateway could not write the
/// marker) is added again once it does: the same folder initializes; a
/// different folder under the same name is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn adding_a_store_again_initializes_its_folder() {
    let c = ready(2).await;
    let p = &c.node(1).placer;
    let dir = c.state_dir(1).join("late-nas");
    let r = p
        .add_store("nas", dir.to_str().unwrap(), &["n1".into()])
        .await
        .unwrap();
    assert!(r[0].1.is_err(), "{r:?}");
    assert!(!p.stores().await.unwrap()[0].gateways[0].1.healthy);
    std::fs::create_dir_all(&dir).unwrap();
    let r = p
        .add_store("nas", dir.to_str().unwrap(), &["n1".into()])
        .await
        .unwrap();
    assert!(r.iter().all(|(_, r)| r.is_ok()), "{r:?}");
    let st = p.stores().await.unwrap();
    assert_eq!(st.len(), 1);
    assert!(st[0].gateways[0].1.healthy);
    assert!(
        p.add_store("nas", "/somewhere/else", &["n1".into()])
            .await
            .is_err()
    );
}

/// Cancelling stops a file mid-copy: nothing is published and its partial
/// staging file is removed, rather than the copy running to its end.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cancel_stops_a_file_mid_copy() {
    let c = ready(2).await;
    let p = &c.node(1).placer;
    let nas = c.state_dir(1).join("nas-midcopy");
    std::fs::create_dir_all(&nas).unwrap();
    p.add_store("nas", nas.to_str().unwrap(), &["n1".into()])
        .await
        .unwrap();
    // Big enough that copying it takes far longer than noticing a cancel.
    let v = c.node(2).vfs.clone();
    let (a, fh, _) = v
        .create(FileId::ROOT, b"big", 0o644, oflags::WRONLY)
        .await
        .unwrap();
    for i in 0..64u64 {
        v.write(fh, i << 22, vec![i as u8; 4 << 20]).await.unwrap();
    }
    v.release(fh, None).await;
    c.eventually("stable", Duration::from_secs(10), |c| {
        c.attr(1, a.id)
            .is_some_and(|x| x.gen_state == GenState::Stable)
    })
    .await;
    // Only n2 has it: n1, the archive's gateway, pulls it over the
    // in-process fabric.
    let id = p
        .replicate(
            Selector::parse("/big", "/hub").unwrap(),
            vec!["nas".into()],
            1,
        )
        .await
        .unwrap();
    let staged = || -> usize {
        std::fs::read_dir(&nas)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with("staging"))
            .flat_map(|d| std::fs::read_dir(d.path()).into_iter().flatten().flatten())
            .count()
    };
    c.eventually("copy under way", Duration::from_secs(10), |_| staged() > 0)
        .await;
    // Hold the copy where it is: n2's answers stop arriving (until the
    // LAN standby takes over, ADR-057, and the copy could carry on).
    c.node(1).rpc.filter().mute(NodeId(2));
    p.cancel_job(id).await.unwrap();
    let t = std::time::Instant::now();
    let job = wait_job(&c, id).await;
    assert!(job.cancelled, "{job:?}");
    // Stopped, not run to an end (copied, or failed on the muted link).
    let h = &job.hosts["nas"];
    assert_eq!((h.done_files, h.failed.len()), (0, 0), "{job:?}");
    assert!(
        t.elapsed() < Duration::from_secs(3),
        "took {:?}",
        t.elapsed()
    );
    c.heal();
    // Long enough for a copy that kept going to have finished.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let nas_id = p.archive_stores().unwrap()[0].0.0;
    assert!(
        !stores_of(&c, a.id).contains(&nas_id),
        "published after cancel"
    );
    c.eventually("partial copy removed", Duration::from_secs(5), |_| {
        staged() == 0
    })
    .await;
}

/// ADR-090: an archive whose folder becomes a host's state dir (a NAS that
/// runs sparknest itself) is adopted by that host. Only the metadata
/// changes; the host keeps every object, before and after the adoption,
/// and serves them.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_host_adopts_the_archive_in_its_own_folder() {
    let mut c = ready(3).await;
    c.add_offgrid(4).await;
    c.eventually("n4 serving", Duration::from_secs(8), |c| {
        c.node(4).data.caught_up() && c.node(4).data.session().is_some()
    })
    .await;
    c.eventually("n4 named", Duration::from_secs(8), |c| {
        c.node(1)
            .placer
            .nodes()
            .is_ok_and(|v| v.iter().any(|h| h.name == "n4"))
    })
    .await;
    // The archive lives in n4's state dir and n1 reaches it (raptor over
    // SMB, with the NAS's folder).
    let folder = c.state_dir(4);
    let p = c.node(1).placer.clone();
    let r = p
        .add_store("nas", folder.to_str().unwrap(), &["n1".into()])
        .await
        .unwrap();
    assert!(r.iter().all(|(_, r)| r.is_ok()), "{r:?}");
    let nas = p.stores().await.unwrap()[0].id;

    // Two files live only in the archive.
    let v2 = &c.node(2).vfs;
    let mut files = Vec::new();
    for (name, byte) in [("a.bin", 7u8), ("b.bin", 9u8)] {
        let (a, fh, _) = v2
            .create(FileId::ROOT, name.as_bytes(), 0o644, oflags::WRONLY)
            .await
            .unwrap();
        v2.write(fh, 0, vec![byte; 3 << 20]).await.unwrap();
        v2.release(fh, None).await;
        files.push((a.id, byte));
    }
    c.eventually("stable", Duration::from_secs(5), |c| {
        files.iter().all(|(f, _)| {
            c.attr(1, *f)
                .is_some_and(|x| x.gen_state == GenState::Stable)
        })
    })
    .await;
    for name in ["/a.bin", "/b.bin"] {
        let sel = Selector::parse(name, "/hub").unwrap();
        let j = wait_job(&c, p.offload(sel, "nas".into(), 4).await.unwrap()).await;
        assert!(j.error.is_none(), "{j:?}");
    }
    c.converge().await;
    for (f, _) in &files {
        assert_eq!(stores_of(&c, *f), vec![nas.0]);
    }

    // n4 restarts before the adoption: the archive's objects sit in its
    // objects/, unlisted as its own, and must survive.
    c.stop(4).await;
    c.restart(4).await;
    c.eventually("n4 back", Duration::from_secs(8), |c| {
        c.node(4).data.caught_up() && c.node(4).data.session().is_some()
    })
    .await;
    for (f, _) in &files {
        let a = c.attr(1, *f).unwrap();
        assert!(
            c.node(4)
                .data
                .store()
                .exists(nest_store::ObjectKey::new(*f, a.generation)),
            "n4's start removed an archive object"
        );
    }

    // A host that does not see the archive's folder (n3) cannot adopt it.
    let e = p.adopt_store("nas", "n3", true).await.unwrap_err();
    assert!(e.to_string().contains("no marker"), "{e}");
    // A dry run only checks; a missing object stops it, changing nothing.
    assert_eq!(p.adopt_store("nas", "n4", true).await.unwrap(), 2);
    let (f0, _) = files[0];
    let a0 = c.attr(1, f0).unwrap();
    let obj = c
        .node(4)
        .data
        .store()
        .path(nest_store::ObjectKey::new(f0, a0.generation));
    let aside = obj.with_extension("aside");
    std::fs::rename(&obj, &aside).unwrap();
    let e = p.adopt_store("nas", "n4", false).await.unwrap_err();
    assert!(e.to_string().contains("not in its folder"), "{e}");
    assert_eq!(stores_of(&c, f0), vec![nas.0]);
    std::fs::rename(&aside, &obj).unwrap();

    // Adopted: n4 holds them, the archive is gone.
    assert_eq!(p.adopt_store("nas", "n4", false).await.unwrap(), 2);
    c.converge().await;
    for (f, _) in &files {
        assert_eq!(stores_of(&c, *f), vec![4]);
    }
    assert!(p.stores().await.unwrap().is_empty());
    // The folder is n4's own now, without the archive's marker.
    assert!(!folder.join(nest_store::MARKER).exists());

    // Read from a fabric host (n3 has no copy: n4 serves it), and n4 keeps
    // them across another restart.
    for (f, byte) in &files {
        let v3 = &c.node(3).vfs;
        let (fh, _) = v3.open(*f, 0).await.unwrap();
        let b = v3.read(fh, (3 << 20) - 4, 16).await.unwrap();
        assert_eq!(b, vec![*byte; 4]);
        v3.release(fh, None).await;
    }
    c.stop(4).await;
    c.restart(4).await;
    c.eventually("n4 back again", Duration::from_secs(8), |c| {
        c.node(4).data.caught_up() && c.node(4).data.session().is_some()
    })
    .await;
    for (f, _) in &files {
        let a = c.attr(1, *f).unwrap();
        assert!(
            c.node(4)
                .data
                .store()
                .exists(nest_store::ObjectKey::new(*f, a.generation))
        );
    }
}

/// ADR-090: metadata backups go to hosts, into each one's own state dir:
/// by default the hosts with the most room, else the `meta-backups`
/// group. Each is a complete, readable copy of the metadata.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn metadata_backups_land_on_the_chosen_hosts() {
    let c = ready(4).await;
    let p = c.node(1).placer.clone();
    let v = &c.node(2).vfs;
    let (a, fh, _) = v
        .create(FileId::ROOT, b"kept.bin", 0o644, oflags::WRONLY)
        .await
        .unwrap();
    v.write(fh, 0, b"in the backup".to_vec()).await.unwrap();
    v.release(fh, None).await;
    c.converge().await;

    // No group: three hosts, each writing from its own replica.
    let r = p.meta_backup().await.unwrap();
    assert_eq!(r.len(), 3, "{r:?}");
    for (host, out) in &r {
        let path = out.as_ref().unwrap_or_else(|e| panic!("{host}: {e}"));
        let id: u64 = host.trim_start_matches('n').parse().unwrap();
        assert!(
            path.starts_with(c.state_dir(id).join("backups/meta").to_str().unwrap()),
            "{host} wrote {path}"
        );
        let db = rusqlite::Connection::open(path).unwrap();
        let n: i64 = db
            .query_row(
                "SELECT count(*) FROM files WHERE id = ?1",
                [a.id.0 as i64],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1, "{host}'s backup lacks the file");
    }

    // A group names them.
    p.set_group(
        nest_place::placer::META_BACKUP_GROUP,
        vec!["n2".into(), "n4".into()],
    )
    .await
    .unwrap();
    let r = p.meta_backup().await.unwrap();
    let hosts: Vec<&str> = r.iter().map(|(h, _)| h.as_str()).collect();
    assert_eq!(hosts, ["n2", "n4"]);
    assert!(r.iter().all(|(_, o)| o.is_ok()), "{r:?}");
    // Each host keeps at most 14.
    for _ in 0..15 {
        p.meta_backup().await.unwrap();
    }
    let kept = std::fs::read_dir(c.state_dir(4).join("backups/meta"))
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("meta-"))
        .count();
    assert!(kept <= 14, "{kept} kept");
}
