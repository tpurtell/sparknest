//! Milestone 8: hardening. Its own test binary, so no parallel test moves
//! the filesystem's free space underneath the reserve checks.

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
