//! Several disks per host (ADR-073): each extra disk is a place of its own,
//! named like a host; copies go there only when it is named.

use nest_data::vfs::oflags;
use nest_place::Selector;
use nest_testkit::TestCluster;
use nest_types::NewFiles;
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

fn stores_of(c: &TestCluster, f: FileId) -> Vec<StoreId> {
    let r = c.node(1).meta.open_reader().unwrap();
    nest_meta::query::replicas(&r, f)
        .unwrap()
        .iter()
        .filter(|r| r.state == ReplicaState::Live)
        .map(|r| r.store)
        .collect()
}

async fn write_file(c: &TestCluster, on: u64, name: &str, byte: u8, len: usize) -> FileId {
    let v = &c.node(on).vfs;
    let (a, fh, _) = v
        .create(FileId::ROOT, name.as_bytes(), 0o644, oflags::WRONLY)
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
        out.extend(b);
    }
    v.release(fh, None).await;
    out
}

async fn wait_job(c: &TestCluster, id: u64) -> nest_place::placer::ClusterJob {
    c.eventually("job finished", Duration::from_secs(30), |c| {
        c.node(1).placer.job(id).is_some_and(|j| j.finished)
    })
    .await;
    c.node(1).placer.job(id).unwrap()
}

/// A directory standing in for a mounted disk on host `id`.
fn disk_dir(c: &TestCluster, id: u64, name: &str) -> String {
    let d = c
        .state_dir(id)
        .parent()
        .unwrap()
        .join(format!("n{id}-{name}"));
    std::fs::create_dir_all(&d).unwrap();
    d.to_str().unwrap().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_disk_is_a_place_of_its_own() {
    let mut c = ready(3).await;
    let p = c.node(1).placer.clone();
    let d2 = disk_dir(&c, 2, "nvme1");
    let d3 = disk_dir(&c, 2, "nvme2");
    let s2 = p.add_disk("n2", &d2, None).await.unwrap();
    let s3 = p.add_disk("n2", &d3, None).await.unwrap();
    assert_eq!((s2, s3), (NodeId(2).disk_store(1), NodeId(2).disk_store(2)));
    assert_eq!(p.find_disk("n2 (D2)").unwrap().0, s2);
    assert_eq!(p.find_disk("n2:d3").unwrap().0, s3);
    // The same path again is the same disk.
    assert_eq!(p.add_disk("n2", &d2, None).await.unwrap(), s2);
    let listed = p.disks().await.unwrap();
    let n2 = listed
        .iter()
        .find(|(h, _)| h.name == "n2")
        .unwrap()
        .1
        .as_ref()
        .unwrap();
    assert_eq!(n2.len(), 3, "{n2:?}");
    assert!(n2[0].primary && n2.iter().all(|d| d.healthy), "{n2:?}");
    let names: Vec<String> = p.places().unwrap().into_iter().map(|h| h.name).collect();
    assert_eq!(names, ["n1", "n2", "n3", "n2 (D2)", "n2 (D3)"]);

    let mut files = Vec::new();
    for i in 0..6u8 {
        files.push(write_file(&c, 1, &format!("w{i}"), i + 1, 3 << 20).await);
    }
    // A host's name is its primary disk: nothing goes to D2 or D3 unasked.
    let job = p
        .replicate(
            Selector::parse("/w0", "/hub").unwrap(),
            vec!["n2".into()],
            4,
        )
        .await
        .unwrap();
    assert!(wait_job(&c, job).await.error.is_none());
    assert!(stores_of(&c, files[0]).contains(&StoreId(2)));
    // Named, a disk takes a copy of its own, beside the primary's.
    let job = p
        .replicate(
            Selector::parse("/w0", "/hub").unwrap(),
            vec!["n2 (D2)".into()],
            4,
        )
        .await
        .unwrap();
    let j = wait_job(&c, job).await;
    assert!(
        j.error.is_none() && j.hosts["n2 (D2)"].failed.is_empty(),
        "{j:?}"
    );
    let st = stores_of(&c, files[0]);
    assert!(st.contains(&StoreId(2)) && st.contains(&s2), "{st:?}");

    // Room as on real disks (the test dirs share one, nearly full).
    for n in 1..=3 {
        c.node(n).data.store().set_free(Some(1 << 40));
    }
    for d in [s2, s3] {
        c.node(2)
            .data
            .disk(d)
            .unwrap()
            .store
            .set_free(Some(1 << 40));
    }
    // Place: one copy of each file over n2 (D2) and n2 (D3), even.
    let plan = p
        .plan(nest_place::plan::Goal::Place {
            selector: "/".into(),
            hosts: vec!["n2 (D2)".into(), "n2:d3".into()],
            factor: 0.5,
            by_speed: false,
            hub: "/hub".into(),
        })
        .await
        .unwrap();
    assert!(plan.feasible, "{plan:?}");
    let job = p.apply_plan(plan.id).await.unwrap();
    let j = wait_job(&c, job).await;
    assert!(j.error.is_none(), "{j:?}");
    c.converge().await;
    let (mut on2, mut on3) = (0, 0);
    for f in &files {
        let st = stores_of(&c, *f);
        let picked: Vec<_> = st.iter().filter(|s| **s == s2 || **s == s3).collect();
        assert_eq!(picked.len(), 1, "one of the two disks: {st:?}");
        if st.contains(&s2) {
            on2 += 1;
        } else {
            on3 += 1;
        }
    }
    assert_eq!((on2, on3), (3, 3), "even");

    // A disk's copy is read on its host, and from it by others.
    let on_d = *files
        .iter()
        .find(|f| stores_of(&c, **f).contains(&s3))
        .unwrap();
    let want = read_all(&c.node(1).vfs, on_d).await;
    assert_eq!(read_all(&c.node(2).vfs, on_d).await, want);
    for h in ["n1", "n2"] {
        let r = p
            .evict(&Selector::parse("/", "/hub").unwrap(), &[h.into()])
            .await
            .unwrap();
        assert!(!r.is_empty());
    }
    c.converge().await;
    assert_eq!(stores_of(&c, on_d), vec![s3]);
    assert_eq!(read_all(&c.node(3).vfs, on_d).await, want);
    assert_eq!(c.node(3).vfs.read_order(on_d).unwrap(), vec![NodeId(2)]);

    // A restart keeps the disk copies (reconciled, not discarded).
    c.stop(2).await;
    c.restart(2).await;
    c.eventually("n2 serving", Duration::from_secs(10), |c| {
        c.node(2).data.caught_up()
    })
    .await;
    assert_eq!(read_all(&c.node(2).vfs, on_d).await, want);
    assert_eq!(read_all(&c.node(3).vfs, on_d).await, want);
    let on_n2_disk = on_d;

    // Writing a file held only on a disk: n2 owns it, the old copy goes.
    let v2 = &c.node(2).vfs;
    let (fh, _) = v2.open(on_n2_disk, oflags::WRONLY).await.unwrap();
    v2.write(fh, 0, vec![0xee; 4096]).await.unwrap();
    v2.release(fh, None).await;
    c.eventually("stable again", Duration::from_secs(10), |c| {
        c.attr(1, on_n2_disk)
            .is_some_and(|x| x.gen_state == GenState::Stable && x.generation.0 > 1)
    })
    .await;
    assert_eq!(stores_of(&c, on_n2_disk), vec![StoreId(2)]);
    let got = read_all(&c.node(3).vfs, on_n2_disk).await;
    assert_eq!(&got[..4096], &[0xee; 4096][..]);
    assert_eq!(&got[4096..], &want[4096..]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_disk_is_drained_and_removed_and_an_unmounted_one_is_never_used() {
    let c = ready(3).await;
    let p = c.node(1).placer.clone();
    let d1 = disk_dir(&c, 2, "nvme1");
    let s1 = p.add_disk("n2", &d1, None).await.unwrap();
    let f = write_file(&c, 1, "big", 7, 2 << 20).await;
    // Fill n2's disk directly: copy into it.
    c.node(2).vfs.replicate_into(f, s1).await.unwrap();
    assert!(stores_of(&c, f).contains(&s1));
    // Not removable while it holds a copy.
    assert!(p.remove_disk("n2 (D2)", false).await.is_err());

    // The disk goes away (unmounted): its marker is gone, so it is never
    // read; n3 reads from n1 instead.
    let marker = std::path::Path::new(&d1).join(nest_store::MARKER);
    let saved = std::fs::read(&marker).unwrap();
    std::fs::remove_file(&marker).unwrap();
    c.eventually("unusable", Duration::from_secs(10), |c| {
        c.node(2).data.disk(s1).is_none()
    })
    .await;
    let listed = p.disks().await.unwrap();
    let n2 = listed
        .iter()
        .find(|(h, _)| h.name == "n2")
        .unwrap()
        .1
        .as_ref()
        .unwrap()
        .clone();
    assert!(!n2[1].healthy, "{n2:?}");
    assert!(!c.node(2).vfs.holds_live(&c.attr(2, f).unwrap()));
    // Back: served again.
    std::fs::write(&marker, saved).unwrap();
    c.eventually("usable", Duration::from_secs(10), |c| {
        c.node(2).data.disk(s1).is_some()
    })
    .await;
    assert!(c.node(2).vfs.holds_live(&c.attr(2, f).unwrap()));

    // Drain: the copy moves to n2's live store, on n2.
    let job = p.drain_disk("n2 (D2)", None).await.unwrap();
    let j = wait_job(&c, job).await;
    assert!(j.error.is_none(), "{j:?}");
    c.converge().await;
    let st = stores_of(&c, f);
    assert!(st.contains(&StoreId(2)) && !st.contains(&s1), "{st:?}");
    c.eventually("disk emptied", Duration::from_secs(5), |_| {
        std::fs::read_dir(std::path::Path::new(&d1).join("objects"))
            .unwrap()
            .all(|d| {
                std::fs::read_dir(d.unwrap().path())
                    .unwrap()
                    .next()
                    .is_none()
            })
    })
    .await;
    p.remove_disk("n2 (D2)", false).await.unwrap();
    assert!(p.find_disk("n2 (D2)").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_download_for_a_disk_moves_onto_it() {
    let c = ready(3).await;
    let p = c.node(1).placer.clone();
    let d = disk_dir(&c, 2, "nvme1");
    let s = p.add_disk("n2", &d, None).await.unwrap();
    // As a Hub fetch on n2 leaves them: blobs written through n2's mount,
    // here on its primary disk (its policy says so), and snapshot entries
    // linking to them.
    set_new_files(&c, "n2").await;
    let v2 = &c.node(2).vfs;
    let hub = v2.mkdir(FileId::ROOT, b"hub", 0o755).await.unwrap().id;
    let repo = v2.mkdir(hub, b"models--o--m", 0o755).await.unwrap().id;
    let blobs = v2.mkdir(repo, b"blobs", 0o755).await.unwrap().id;
    let snaps = v2.mkdir(repo, b"snapshots", 0o755).await.unwrap().id;
    let snap = v2.mkdir(snaps, b"abc", 0o755).await.unwrap().id;
    let mut files = Vec::new();
    for (i, n) in ["a.safetensors", "b.safetensors"].iter().enumerate() {
        let blob = format!("e{i}");
        let (a, fh, _) = v2
            .create(blobs, blob.as_bytes(), 0o644, oflags::WRONLY)
            .await
            .unwrap();
        v2.write(fh, 0, vec![i as u8 + 1; 1 << 20]).await.unwrap();
        v2.release(fh, None).await;
        v2.symlink(snap, n.as_bytes(), format!("../../blobs/{blob}").as_bytes())
            .await
            .unwrap();
        files.push(a.id);
    }
    for f in &files {
        c.eventually("stable", Duration::from_secs(5), |c| {
            c.attr(1, *f)
                .is_some_and(|x| x.gen_state == GenState::Stable)
        })
        .await;
        assert_eq!(stores_of(&c, *f), vec![StoreId(2)]);
    }
    let unmoved = p
        .settle_onto(
            NodeId(2),
            "/hub/models--o--m/snapshots/abc",
            vec![
                "a.safetensors".into(),
                "b.safetensors".into(),
                "gone".into(),
            ],
            s,
        )
        .await;
    // The one that is not there is reported; the others are on the disk only.
    assert_eq!(unmoved, vec!["gone".to_string()]);
    c.converge().await;
    for (i, f) in files.iter().enumerate() {
        assert_eq!(stores_of(&c, *f), vec![s]);
        assert_eq!(
            read_all(&c.node(3).vfs, *f).await,
            vec![i as u8 + 1; 1 << 20]
        );
    }
    // A download for the primary whose files the mount put on the disk
    // (ADR-077) ends on the primary; one already there is left alone.
    let unmoved = p
        .settle_onto(
            NodeId(2),
            "/hub/models--o--m/snapshots/abc",
            vec!["a.safetensors".into(), "b.safetensors".into()],
            StoreId(2),
        )
        .await;
    assert!(unmoved.is_empty(), "{unmoved:?}");
    let again = p
        .settle_onto(
            NodeId(2),
            "/hub/models--o--m/snapshots/abc",
            vec!["a.safetensors".into()],
            StoreId(2),
        )
        .await;
    assert!(again.is_empty(), "{again:?}");
    c.converge().await;
    for (i, f) in files.iter().enumerate() {
        assert_eq!(stores_of(&c, *f), vec![StoreId(2)]);
        assert_eq!(
            read_all(&c.node(3).vfs, *f).await,
            vec![i as u8 + 1; 1 << 20]
        );
    }
}

/// Where n2 puts new files (ADR-077), as the API stores it.
async fn set_new_files(c: &TestCluster, mode: &str) {
    c.node(1).placer.set_new_files("n2", mode).await.unwrap();
    c.eventually("policy applied on n2", Duration::from_secs(5), |c| {
        c.node(2).placer.new_files(NodeId(2)).ok() == c.node(1).placer.new_files(NodeId(2)).ok()
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn new_files_go_where_their_host_says_and_survive_a_crash_mid_write() {
    let mut c = ready(3).await;
    let p = c.node(1).placer.clone();
    let s2 = p
        .add_disk("n2", &disk_dir(&c, 2, "nvme1"), None)
        .await
        .unwrap();
    let s3 = p
        .add_disk("n2", &disk_dir(&c, 2, "nvme2"), None)
        .await
        .unwrap();

    // Fixed on D2: every new file is written there and its copy is D2's.
    set_new_files(&c, "n2 (D2)").await;
    assert_eq!(p.new_files(NodeId(2)).unwrap(), NewFiles::Disk(1));
    let mut on_d2 = Vec::new();
    for i in 0..4u8 {
        on_d2.push(write_file(&c, 2, &format!("d2-{i}"), i + 1, 1 << 20).await);
    }
    for f in &on_d2 {
        assert_eq!(stores_of(&c, *f), vec![s2], "{f}");
        assert_eq!(
            read_all(&c.node(3).vfs, *f).await,
            vec![1 + (f.0 - on_d2[0].0) as u8; 1 << 20]
        );
    }
    let d2 = c.node(2).data.disk(s2).unwrap();
    assert!(on_d2.iter().all(|f| {
        d2.store
            .exists(nest_store::ObjectKey::new(*f, Generation(1)))
    }));
    assert!(on_d2.iter().all(|f| {
        !c.node(2)
            .data
            .store()
            .exists(nest_store::ObjectKey::new(*f, Generation(1)))
    }));
    // Hosts with one disk, and other hosts' writes, are unaffected.
    // Reading them on n2 is a local read of D2, counted for D2 (not the
    // primary, not an archive).
    let before = c.node(2).vfs.store_reads();
    let local0 = c.node(2).vfs.local_read_bytes();
    assert_eq!(read_all(&c.node(2).vfs, on_d2[0]).await.len(), 1 << 20);
    let after = c.node(2).vfs.store_reads();
    let got = |v: &[(StoreId, u64)], s: StoreId| v.iter().find(|x| x.0 == s).unwrap().1;
    assert_eq!(got(&after, s2) - got(&before, s2), 1 << 20);
    assert_eq!(got(&after, StoreId(2)), got(&before, StoreId(2)));
    assert_eq!(c.node(2).vfs.local_read_bytes() - local0, 1 << 20);
    let n1 = write_file(&c, 1, "n1-file", 9, 4096).await;
    assert_eq!(stores_of(&c, n1), vec![StoreId(1)]);

    // The primary, named by the host's own name.
    set_new_files(&c, "n2").await;
    let f = write_file(&c, 2, "primary", 7, 4096).await;
    assert_eq!(stores_of(&c, f), vec![StoreId(2)]);

    // Most free: the emptiest of the three, each time.
    for n in 1..=3 {
        c.node(n).data.store().set_free(Some(1 << 30));
    }
    c.node(2)
        .data
        .disk(s2)
        .unwrap()
        .store
        .set_free(Some(2 << 30));
    c.node(2)
        .data
        .disk(s3)
        .unwrap()
        .store
        .set_free(Some(5 << 30));
    set_new_files(&c, "most-free").await;
    let f = write_file(&c, 2, "most-free", 3, 4096).await;
    assert_eq!(stores_of(&c, f), vec![s3]);

    // Weighted: every store with room gets some.
    set_new_files(&c, "weighted").await;
    c.node(2)
        .data
        .disk(s3)
        .unwrap()
        .store
        .set_free(Some(1 << 30));
    let mut seen = std::collections::HashSet::new();
    for i in 0..24u8 {
        let f = write_file(&c, 2, &format!("w-{i}"), i, 4096).await;
        seen.extend(stores_of(&c, f));
    }
    assert_eq!(seen.len(), 3, "{seen:?}");

    // A file half written on D2 when its host loses power keeps what it
    // had: found on D2 and finalized there, not recreated empty.
    set_new_files(&c, "n2 (D2)").await;
    let v2 = &c.node(2).vfs;
    let (a, fh, _) = v2
        .create(FileId::ROOT, b"interrupted", 0o644, oflags::WRONLY)
        .await
        .unwrap();
    v2.write(fh, 0, vec![0x5a; 3 << 20]).await.unwrap();
    v2.fsync(fh, false).await.unwrap();
    assert!(
        d2.store
            .exists(nest_store::ObjectKey::new(a.id, a.generation))
    );
    drop(d2);
    c.power_loss(2, None).await;
    c.restart(2).await;
    c.eventually("n2 serving", Duration::from_secs(15), |c| {
        c.node(2).data.caught_up() && c.node(2).data.session().is_some()
    })
    .await;
    c.eventually("finalized on D2", Duration::from_secs(15), |c| {
        c.attr(1, a.id)
            .is_some_and(|x| x.gen_state == GenState::Stable)
    })
    .await;
    assert_eq!(stores_of(&c, a.id), vec![s2]);
    assert_eq!(read_all(&c.node(3).vfs, a.id).await, vec![0x5a; 3 << 20]);
    // A clean restart reconciles the disk and keeps every copy on it.
    c.stop(2).await;
    c.restart(2).await;
    c.eventually("n2 back", Duration::from_secs(10), |c| {
        c.node(2).data.caught_up()
    })
    .await;
    for f in on_d2.iter().chain([&a.id]) {
        assert_eq!(stores_of(&c, *f), vec![s2]);
        assert!(!read_all(&c.node(2).vfs, *f).await.is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_import_links_into_the_disk_its_files_are_on() {
    let c = ready(3).await;
    let p = c.node(1).placer.clone();
    // A disk and a quant beside it on a filesystem other than the hosts'
    // primaries (the crate's directory, against the system temp dir the
    // cluster runs in), where it differs; else every store shares one and
    // any of them could link it.
    let base = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).unwrap();
    let dir = base.path().join("disk");
    std::fs::create_dir_all(&dir).unwrap();
    let s2 = p.add_disk("n2", dir.to_str().unwrap(), None).await.unwrap();
    let src = base.path().join("n2-quant");
    std::fs::create_dir_all(&src).unwrap();
    for i in 0..3u8 {
        std::fs::write(src.join(format!("shard-{i}.bin")), vec![i + 1; 1 << 20]).unwrap();
    }
    let apart = std::fs::hard_link(src.join("shard-0.bin"), c.state_dir(2).join("probe")).is_err();
    let job = c.node(2).placer.import(nest_place::import::ImportOptions {
        src: src.clone(),
        dst: "/quant".into(),
        r#move: false,
        seal: nest_place::import::SealMode::All,
        copy: false,
    });
    c.eventually("import done", Duration::from_secs(20), |c| {
        c.node(2).placer.job(job).is_some_and(|j| j.finished)
    })
    .await;
    let j = c.node(2).placer.job(job).unwrap();
    assert!(j.error.is_none(), "{j:?}");
    let r = c.node(1).meta.open_reader().unwrap();
    for i in 0..3u8 {
        let f = nest_meta::query::resolve_follow(&r, format!("/quant/shard-{i}.bin").as_bytes())
            .unwrap()
            .unwrap();
        let st = stores_of(&c, f);
        if apart {
            // Linked into the disk it sits on, the only store that can.
            assert_eq!(st, vec![s2], "shard {i}");
        } else {
            assert!(st == vec![StoreId(2)] || st == vec![s2], "{st:?}");
        }
        assert_eq!(read_all(&c.node(3).vfs, f).await, vec![i + 1; 1 << 20]);
    }
    if !apart {
        eprintln!("one filesystem here: linking into the matching disk not exercised");
    }
}
