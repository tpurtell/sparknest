//! Several disks per host (ADR-073): each extra disk is a store of its own,
//! copies spread over a host's disks, a host still holds a file once.

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
async fn copies_spread_over_a_hosts_disks_and_are_read_from_them() {
    let mut c = ready(3).await;
    let p = c.node(1).placer.clone();
    let d1 = disk_dir(&c, 2, "nvme1");
    let d2 = disk_dir(&c, 2, "nvme2");
    let s1 = p.add_disk("n2", &d1, None).await.unwrap();
    let s2 = p.add_disk("n2", &d2, Some("n2:fast")).await.unwrap();
    assert_eq!((s1, s2), (NodeId(2).disk_store(1), NodeId(2).disk_store(2)));
    // The same path again is the same disk.
    assert_eq!(p.add_disk("n2", &d1, None).await.unwrap(), s1);
    let listed = p.disks().await.unwrap();
    let n2 = &listed.iter().find(|(h, _)| h.name == "n2").unwrap().1;
    let n2 = n2.as_ref().unwrap();
    assert_eq!(n2.len(), 3, "{n2:?}");
    assert!(n2[0].primary && n2.iter().all(|d| d.healthy), "{n2:?}");

    // Six files written on n1, copied to n2: they land over n2's stores.
    let mut files = Vec::new();
    for i in 0..6u8 {
        files.push(write_file(&c, 1, &format!("w{i}"), i + 1, 3 << 20).await);
    }
    let sel = Selector::parse("/", "/hub").unwrap();
    let job = p.replicate(sel, vec!["n2".into()], 4).await.unwrap();
    let j = wait_job(&c, job).await;
    assert!(j.error.is_none(), "{j:?}");
    assert!(j.hosts["n2"].failed.is_empty(), "{j:?}");
    let mut on_disk = 0;
    for f in &files {
        let st = stores_of(&c, *f);
        let n2_copies: Vec<_> = st.iter().filter(|s| s.host() == Some(NodeId(2))).collect();
        assert_eq!(n2_copies.len(), 1, "one copy per host: {st:?}");
        on_disk += n2_copies.iter().filter(|s| s.is_disk()).count();
    }
    assert!(
        on_disk >= 2,
        "copies went to the extra disks too ({on_disk} of 6)"
    );
    // Already held (on a disk): a second copy to the host does nothing.
    let job = p
        .replicate(
            Selector::parse("/w0", "/hub").unwrap(),
            vec!["n2".into()],
            4,
        )
        .await
        .unwrap();
    let j = wait_job(&c, job).await;
    assert_eq!(j.hosts["n2"].total_files, 0, "{j:?}");

    // n2 reads its disk copies locally; n3 reads them from n2.
    let on_n2_disk = *files
        .iter()
        .find(|f| stores_of(&c, **f).iter().any(|s| s.is_disk()))
        .unwrap();
    let want = read_all(&c.node(1).vfs, on_n2_disk).await;
    assert_eq!(read_all(&c.node(2).vfs, on_n2_disk).await, want);
    p.evict(&Selector::parse("/", "/hub").unwrap(), &["n1".into()])
        .await
        .unwrap();
    c.converge().await;
    assert!(
        stores_of(&c, on_n2_disk)
            .iter()
            .all(|s| s.host() == Some(NodeId(2)))
    );
    assert_eq!(read_all(&c.node(3).vfs, on_n2_disk).await, want);
    assert_eq!(
        c.node(3).vfs.read_order(on_n2_disk).unwrap(),
        vec![NodeId(2)]
    );

    // A restart keeps the disk copies (reconciled, not discarded).
    c.stop(2).await;
    c.restart(2).await;
    c.eventually("n2 serving", Duration::from_secs(10), |c| {
        c.node(2).data.caught_up()
    })
    .await;
    assert_eq!(read_all(&c.node(2).vfs, on_n2_disk).await, want);
    assert_eq!(read_all(&c.node(3).vfs, on_n2_disk).await, want);

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
    assert!(p.remove_disk("n2:disk1", false).await.is_err());

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
    let job = p.drain_disk("n2:disk1").await.unwrap();
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
    p.remove_disk("n2:disk1", false).await.unwrap();
    assert!(p.find_disk("n2:disk1").is_err());
}
