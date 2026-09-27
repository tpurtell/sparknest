//! Milestone 3: the multi-node data plane (over TCP), exercised through each
//! node's Vfs exactly as the FUSE frontend drives it.

use nest_data::vfs::oflags;
use nest_meta::{Command, query};
use nest_store::ObjectKey;
use nest_testkit::TestCluster;
use nest_types::*;
use std::sync::Arc;
use std::time::Duration;

const RW: i32 = oflags::RDWR;
const RO: i32 = 0;

async fn ready(n: u64) -> TestCluster {
    let c = TestCluster::start(n).await;
    c.eventually(
        "all nodes serving with sessions",
        Duration::from_secs(8),
        |c| (1..=n).all(|i| c.node(i).data.caught_up() && c.node(i).data.session().is_some()),
    )
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

async fn wait_stable(c: &TestCluster, file: FileId) {
    c.eventually("file stable", Duration::from_secs(5), |c| {
        c.attr(1, file)
            .is_some_and(|a| a.gen_state == GenState::Stable)
    })
    .await;
    c.converge().await;
}

/// Create `name` on `node` with `content` and let it settle.
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
    wait_stable(c, a.id).await;
    a.id
}

/// Copy the current generation to `to` and publish it (stand-in for M5's
/// replication engine).
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

fn stores(c: &TestCluster, node: u64, f: FileId) -> Vec<u64> {
    let r = c.node(node).meta.open_reader().unwrap();
    query::replicas(&r, f)
        .unwrap()
        .iter()
        .map(|r| r.store.0)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn remote_reads_see_writes_before_close() {
    let c = ready(3).await;
    let v1 = &c.node(1).vfs;
    let (a, fh, _) = v1
        .create(FileId::ROOT, b"log", 0o644, oflags::WRONLY)
        .await
        .unwrap();
    v1.write(fh, 0, b"hello".to_vec()).await.unwrap();
    c.converge().await;
    // Node 2 has no copy: it reads through the owner, and sees the live size.
    assert_eq!(read_all(&c, 2, a.id).await, b"hello");
    assert_eq!(c.node(2).vfs.getattr(a.id).await.unwrap().size, 5);
    v1.write(fh, 5, b" world".to_vec()).await.unwrap();
    assert_eq!(read_all(&c, 2, a.id).await, b"hello world");
    v1.release(fh, None).await;
    wait_stable(&c, a.id).await;
    assert_eq!(read_all(&c, 3, a.id).await, b"hello world");
    assert_eq!(c.node(3).vfs.getattr(a.id).await.unwrap().size, 11);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn writers_without_a_copy_write_through_the_holder() {
    let c = ready(3).await;
    let f = file(&c, 1, "model.bin", b"0123456789").await;
    let v2 = &c.node(2).vfs;
    let (fh, _) = v2.open(f, RW).await.unwrap();
    v2.write(fh, 3, b"abc".to_vec()).await.unwrap();
    // Node 1 held the content, so node 1 owns the new generation.
    assert_eq!(c.attr(2, f).unwrap().owner, Some(NodeId(1)));
    assert_eq!(read_all(&c, 3, f).await, b"012abc6789");
    v2.release(fh, None).await;
    wait_stable(&c, f).await;
    assert_eq!(stores(&c, 3, f), vec![1]);
    assert_eq!(read_all(&c, 2, f).await, b"012abc6789");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn truncating_open_takes_ownership_locally() {
    let c = ready(3).await;
    let f = file(&c, 1, "cfg", b"old contents").await;
    let old = ObjectKey::new(f, c.attr(1, f).unwrap().generation);
    let v2 = &c.node(2).vfs;
    let (fh, _) = v2.open(f, oflags::WRONLY | oflags::TRUNC).await.unwrap();
    assert_eq!(c.attr(2, f).unwrap().owner, Some(NodeId(2)));
    v2.write(fh, 0, b"new".to_vec()).await.unwrap();
    v2.release(fh, None).await;
    wait_stable(&c, f).await;
    assert_eq!(stores(&c, 1, f), vec![2]);
    assert_eq!(read_all(&c, 1, f).await, b"new");
    c.eventually("old copy deleted on node 1", Duration::from_secs(2), |c| {
        !c.node(1).data.store().exists(old)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn revocation_never_serves_stale_bytes() {
    let c = Arc::new(ready(3).await);
    let f = file(&c, 1, "weights", &[b'1'; 4096]).await;
    replicate(&c, f, 1, 2).await;
    assert_eq!(stores(&c, 3, f), vec![1, 2]);

    // Readers on every node hammer the file while node 2 rewrites it.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut readers = Vec::new();
    for n in 1..=3u64 {
        let (c, stop) = (c.clone(), stop.clone());
        readers.push(tokio::spawn(async move {
            let mut seen = Vec::new();
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let b = read_all(&c, n, f).await;
                seen.push(b[0]);
            }
            seen
        }));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let v2 = &c.node(2).vfs;
    let (fh, _) = v2.open(f, RW).await.unwrap();
    v2.write(fh, 0, vec![b'2'; 4096]).await.unwrap();
    // Once the write has returned, no node may read the old bytes.
    for n in 1..=3 {
        assert_eq!(read_all(&c, n, f).await, vec![b'2'; 4096], "node {n}");
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for r in readers {
        let seen = r.await.unwrap();
        // Each reader moves from old to new exactly once, never back.
        let first_new = seen.iter().position(|b| *b == b'2').unwrap_or(seen.len());
        assert!(
            seen[first_new..].iter().all(|b| *b == b'2'),
            "a reader went back to stale bytes"
        );
    }
    v2.release(fh, None).await;
    wait_stable(&c, f).await;
    assert_eq!(stores(&c, 1, f), vec![2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn unlink_while_a_remote_reader_holds_it_open() {
    let c = ready(3).await;
    let f = file(&c, 1, "shard", b"tensor data").await;
    let v2 = &c.node(2).vfs;
    let (fh, _) = v2.open(f, RO).await.unwrap();
    assert_eq!(v2.read(fh, 0, 6).await.unwrap(), b"tensor");
    c.node(3).vfs.unlink(FileId::ROOT, b"shard").await.unwrap();
    c.converge().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    // The name is gone everywhere, but node 2 keeps reading from node 1.
    assert!(c.lookup(1, FileId::ROOT, "shard").is_none());
    assert_eq!(v2.read(fh, 7, 4).await.unwrap(), b"data");
    v2.release(fh, None).await;
    c.eventually("deleted after last close", Duration::from_secs(3), |c| {
        c.attr(1, f).is_none()
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn partitioned_owner_fails_closed() {
    let c = ready(3).await;
    let v1 = &c.node(1).vfs;
    let (a, fh, _) = v1
        .create(FileId::ROOT, b"live", 0o644, oflags::WRONLY)
        .await
        .unwrap();
    v1.write(fh, 0, b"in progress".to_vec()).await.unwrap();
    c.converge().await;
    c.partition(&[1], &[2, 3]);
    let v2 = &c.node(2).vfs;
    let (rfh, _) = v2.open(a.id, RO).await.unwrap();
    let r = v2.read(rfh, 0, 64).await;
    assert!(matches!(r, Err(NestError::Unavailable(_))), "{r:?}");
    c.heal();
    assert_eq!(v2.read(rfh, 0, 64).await.unwrap(), b"in progress");
    v2.release(rfh, None).await;
    v1.release(fh, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn fence_waits_out_an_unreachable_holders_lease() {
    let c = ready(3).await;
    let f = file(&c, 1, "weights", &[b'1'; 1024]).await;
    replicate(&c, f, 1, 2).await;
    // Node 1 holds a copy and is cut off: it cannot acknowledge the fence.
    c.partition(&[1], &[2, 3]);
    let v2 = &c.node(2).vfs;
    let (fh, _) = v2.open(f, RW).await.unwrap();
    let t = std::time::Instant::now();
    v2.write(fh, 0, vec![b'2'; 1024]).await.unwrap();
    let waited = t.elapsed();
    let lease = c.tuning.lease;
    assert!(
        waited >= lease,
        "write returned after {waited:?}, before node 1's lease ({lease:?}) could lapse"
    );
    // Node 1's lease has lapsed: it refuses to serve its stale copy.
    let v1 = &c.node(1).vfs;
    let (rfh, _) = v1.open(f, RO).await.unwrap();
    let r = v1.read(rfh, 0, 16).await;
    assert!(
        r.is_err(),
        "partitioned node served {:?}",
        r.map(|b| String::from_utf8_lossy(&b).into_owned())
    );
    c.heal();
    c.eventually("node 1 serving again", Duration::from_secs(5), |c| {
        c.node(1).data.lease_valid()
    })
    .await;
    assert_eq!(v1.read(rfh, 0, 4).await.unwrap(), b"2222");
    v1.release(rfh, None).await;
    v2.release(fh, None).await;
}

/// Objects kept open for serving are closed when evicted here and swept
/// when idle: an open file keeps its disk space after deletion, which once
/// filled raptor's disk during a spread import.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn served_objects_are_not_held_open() {
    use nest_fabric::ReadSource;
    let c = ready(2).await;
    let f = file(&c, 1, "blob", &vec![7u8; 1 << 20]).await;
    let a = c.attr(1, f).unwrap();
    replicate(&c, f, 1, 2).await;
    let v1 = &c.node(1).vfs;
    // Served to another host: kept open for the next request.
    let sf = ReadSource::open(&**v1, f, a.generation).await.unwrap();
    drop(sf);
    assert_eq!(v1.serving_open(), 1);
    // Evicted here: closed at once.
    assert!(v1.evict_from(f, NodeId(1).live_store()).await.unwrap());
    assert_eq!(v1.serving_open(), 0);
    // Anything else idle is closed within a few seconds.
    let g = file(&c, 1, "other", b"x").await;
    let b = c.attr(1, g).unwrap();
    drop(ReadSource::open(&**v1, g, b.generation).await.unwrap());
    assert_eq!(v1.serving_open(), 1);
    c.eventually("idle served objects closed", Duration::from_secs(6), |c| {
        c.node(1).vfs.serving_open() == 0
    })
    .await;
}
