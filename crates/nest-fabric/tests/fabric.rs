//! The read protocol between two fabric instances in one process (two
//! "nodes" over RDMA loopback on this machine's NIC). Skips without RoCE.

use futures::FutureExt;
use futures::future::BoxFuture;
use nest_fabric::{Fabric, FabricConfig, ReadSource, Slot};
use nest_rpc::{Rpc, RpcConfig};
use nest_types::{FileId, Generation, NestError, NodeId};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Serves a synthetic 256 MiB "file" 7, generation 3: byte i = f(i).
struct Synthetic;

fn byte(i: u64) -> u8 {
    (i.wrapping_mul(2654435761) >> 13) as u8
}

const SIZE: u64 = 256 << 20;

impl ReadSource for Synthetic {
    fn read_into(
        &self,
        file: FileId,
        generation: Generation,
        offset: u64,
        len: usize,
        mut buf: Slot,
    ) -> BoxFuture<'static, (Slot, Result<usize, NestError>)> {
        async move {
            if file != FileId(7) {
                return (buf, Err(NestError::NotFound));
            }
            if generation != Generation(3) {
                return (buf, Err(NestError::Stale));
            }
            let n = (SIZE.saturating_sub(offset) as usize).min(len);
            let dst = buf.as_mut_slice(n);
            for (k, b) in dst.iter_mut().enumerate() {
                *b = byte(offset + k as u64);
            }
            (buf, Ok(n))
        }
        .boxed()
    }
}

async fn node(id: u64, cfg: FabricConfig) -> Option<(Rpc, Arc<Fabric>)> {
    let rpc = Rpc::bind(RpcConfig {
        node: NodeId(id),
        cluster: "fabric-test".into(),
        secret: b"s".to_vec(),
        listen: "127.0.0.1:0".parse().unwrap(),
        connect_timeout: Duration::from_secs(2),
    })
    .await
    .unwrap();
    let f = Fabric::start(NodeId(id), cfg, rpc.clone()).unwrap()?;
    Some((rpc, f))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn read_protocol_over_loopback() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_env_filter("warn")
        .try_init();
    let cfg = FabricConfig {
        chunk: 4 << 20,
        client_slots: 16,
        server_slots: 8,
        window: 8,
        devices: vec![],
        ..FabricConfig::default()
    };
    let Some((rpc1, f1)) = node(1, cfg.clone()).await else {
        return;
    };
    let (rpc2, f2) = node(2, cfg).await.unwrap();
    rpc2.set_peer(NodeId(1), rpc1.local_addr());
    rpc1.set_peer(NodeId(2), rpc2.local_addr());
    let src: Arc<dyn ReadSource> = Arc::new(Synthetic);
    f1.set_source(Arc::downgrade(&src));

    // Exact bytes at assorted offsets and lengths, including the tail.
    for (off, len) in [
        (0u64, 4096usize),
        (12345, 1 << 20),
        (SIZE - 1000, 4 << 20),
        (SIZE, 4096),
        (3 << 20, 4 << 20),
    ] {
        let b = f2
            .read(NodeId(1), FileId(7), Generation(3), off, len)
            .await
            .unwrap();
        let want = (SIZE.saturating_sub(off) as usize).min(len);
        assert_eq!(b.len(), want);
        assert!(
            b.as_slice()
                .iter()
                .enumerate()
                .all(|(k, v)| *v == byte(off + k as u64)),
            "mismatch at {off}"
        );
    }
    // Errors travel back as the right kind.
    assert_eq!(
        f2.read(NodeId(1), FileId(7), Generation(2), 0, 16)
            .await
            .err(),
        Some(NestError::Stale)
    );
    assert_eq!(
        f2.read(NodeId(1), FileId(8), Generation(3), 0, 16)
            .await
            .err(),
        Some(NestError::NotFound)
    );

    // Many concurrent reads (more than slots and window): bounded, correct.
    let t = Instant::now();
    let reads = 256u64;
    let mut tasks = Vec::new();
    for i in 0..reads {
        let f2 = f2.clone();
        tasks.push(tokio::spawn(async move {
            let off = (i * (4 << 20)) % SIZE;
            let b = f2
                .read(NodeId(1), FileId(7), Generation(3), off, 4 << 20)
                .await
                .unwrap();
            assert_eq!(b.as_slice()[4095], byte(off + 4095));
            b.len()
        }));
    }
    let mut total = 0usize;
    for t in tasks {
        total += t.await.unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    eprintln!(
        "loopback: {} MiB in {:.3}s = {:.2} GB/s",
        total >> 20,
        secs,
        total as f64 / secs / 1e9
    );
    assert_eq!(
        f2.stats.reads.load(std::sync::atomic::Ordering::Relaxed),
        5 + reads
    );
    f1.shutdown();
    f2.shutdown();
}

/// Like `Synthetic`, but every answer takes a while (a busy disk).
struct Slow;

impl ReadSource for Slow {
    fn read_into(
        &self,
        file: FileId,
        generation: Generation,
        offset: u64,
        len: usize,
        buf: Slot,
    ) -> BoxFuture<'static, (Slot, Result<usize, NestError>)> {
        async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            Synthetic
                .read_into(file, generation, offset, len, buf)
                .await
        }
        .boxed()
    }
}

/// A reader that gives up on a read (readahead cancelling a chunk) must
/// not let the next read of that landing slot take the late answer as its
/// own: that surfaced as short reads (a tail chunk's length) and wrong bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cancelled_reads_never_answer_later_ones() {
    let cfg = FabricConfig {
        chunk: 4 << 20,
        client_slots: 8,
        server_slots: 8,
        window: 8,
        devices: vec![],
        ..FabricConfig::default()
    };
    let Some((rpc1, f1)) = node(1, cfg.clone()).await else {
        return;
    };
    let (rpc2, f2) = node(2, cfg).await.unwrap();
    rpc2.set_peer(NodeId(1), rpc1.local_addr());
    rpc1.set_peer(NodeId(2), rpc2.local_addr());
    let src: Arc<dyn ReadSource> = Arc::new(Slow);
    f1.set_source(Arc::downgrade(&src));
    // Warm the link up.
    f2.read(NodeId(1), FileId(7), Generation(3), 0, 4096)
        .await
        .unwrap();
    for round in 0..4 {
        // Tail reads (1000 bytes each) abandoned while the peer works on them.
        for _ in 0..8 {
            let _ = tokio::time::timeout(
                Duration::from_millis(2),
                f2.read(NodeId(1), FileId(7), Generation(3), SIZE - 1000, 4 << 20),
            )
            .await;
        }
        // Full chunks right after, reusing the same slots.
        let mut tasks = Vec::new();
        for i in 0..16u64 {
            let f2 = f2.clone();
            tasks.push(tokio::spawn(async move {
                let off = i * (4 << 20);
                let b = f2
                    .read(NodeId(1), FileId(7), Generation(3), off, 4 << 20)
                    .await
                    .unwrap();
                assert_eq!(b.len(), 4 << 20, "round {round} read {i}: short");
                assert!(
                    b.as_slice()
                        .iter()
                        .enumerate()
                        .step_by(4099)
                        .all(|(k, v)| *v == byte(off + k as u64)),
                    "round {round} read {i}: wrong bytes"
                );
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
    }
}

/// Answers small reads at once (as a host does for objects it already has
/// open) and anything else on the async path.
struct Quick;

impl ReadSource for Quick {
    fn try_read_now(
        &self,
        file: FileId,
        generation: Generation,
        offset: u64,
        dst: &mut [u8],
    ) -> Option<Result<usize, NestError>> {
        if file != FileId(7) || generation != Generation(3) {
            return None; // errors come from the async path
        }
        let n = (SIZE.saturating_sub(offset) as usize).min(dst.len());
        for (k, b) in dst[..n].iter_mut().enumerate() {
            *b = byte(offset + k as u64);
        }
        Some(Ok(n))
    }

    fn read_into(
        &self,
        file: FileId,
        generation: Generation,
        offset: u64,
        len: usize,
        buf: Slot,
    ) -> BoxFuture<'static, (Slot, Result<usize, NestError>)> {
        Synthetic.read_into(file, generation, offset, len, buf)
    }
}

/// Small reads are served on the completion thread with the same bytes,
/// lengths and errors as the async path; large ones still take it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn small_reads_take_the_fast_path() {
    let cfg = FabricConfig {
        chunk: 4 << 20,
        client_slots: 16,
        server_slots: 8,
        window: 8,
        devices: vec![],
        ..FabricConfig::default()
    };
    let Some((rpc1, f1)) = node(1, cfg.clone()).await else {
        return;
    };
    let (rpc2, f2) = node(2, cfg).await.unwrap();
    rpc2.set_peer(NodeId(1), rpc1.local_addr());
    rpc1.set_peer(NodeId(2), rpc2.local_addr());
    let src: Arc<dyn ReadSource> = Arc::new(Quick);
    f1.set_source(Arc::downgrade(&src));
    let fast = || {
        f1.stats
            .served_fast
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    let before = fast();
    let mut tasks = Vec::new();
    for i in 0..512u64 {
        let f2 = f2.clone();
        tasks.push(tokio::spawn(async move {
            let off = (i * 7_777_777) % (SIZE - 4096);
            let b = f2
                .read(NodeId(1), FileId(7), Generation(3), off, 4096)
                .await
                .unwrap();
            assert_eq!(b.len(), 4096);
            assert!(
                b.as_slice()
                    .iter()
                    .enumerate()
                    .all(|(k, v)| *v == byte(off + k as u64))
            );
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    // Page-sized reads have their own tier (1024 staging slots): even a
    // burst is served fast.
    let concurrent = fast() - before;
    assert_eq!(concurrent, 512, "only {concurrent} of 512 served fast");
    // One at a time, every small read is.
    let n = fast();
    for i in 0..64u64 {
        let off = i * 1_000_003;
        let b = f2
            .read(NodeId(1), FileId(7), Generation(3), off, 4096)
            .await
            .unwrap();
        assert_eq!(b.as_slice()[4095], byte(off + 4095));
    }
    assert_eq!(fast() - n, 64);
    // The tail, errors and a large read behave as before.
    let b = f2
        .read(NodeId(1), FileId(7), Generation(3), SIZE - 100, 4096)
        .await
        .unwrap();
    assert_eq!(b.len(), 100);
    assert_eq!(
        f2.read(NodeId(1), FileId(7), Generation(2), 0, 16)
            .await
            .err(),
        Some(NestError::Stale)
    );
    let n = fast();
    let b = f2
        .read(NodeId(1), FileId(7), Generation(3), 0, 4 << 20)
        .await
        .unwrap();
    assert_eq!(b.len(), 4 << 20);
    assert_eq!(fast(), n, "a large read took the fast path");
}

/// Page reads, kernel-sized reads and chunks at once: each takes its tier,
/// lengths come back right (the length now travels as the completion's
/// byte count), and bytes land in the right slots.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn mixed_sizes_share_the_tiers() {
    let cfg = FabricConfig {
        chunk: 4 << 20,
        client_slots: 8,
        server_slots: 4,
        small_tiers: vec![
            nest_fabric::Tier {
                size: 4 << 10,
                landing: 16,
                staging: 8,
            },
            nest_fabric::Tier {
                size: 128 << 10,
                landing: 8,
                staging: 4,
            },
        ],
        window: 16,
        devices: vec![],
        ..FabricConfig::default()
    };
    let Some((rpc1, f1)) = node(1, cfg.clone()).await else {
        return;
    };
    let (rpc2, f2) = node(2, cfg).await.unwrap();
    rpc2.set_peer(NodeId(1), rpc1.local_addr());
    rpc1.set_peer(NodeId(2), rpc2.local_addr());
    let src: Arc<dyn ReadSource> = Arc::new(Quick);
    f1.set_source(Arc::downgrade(&src));
    let mut tasks = Vec::new();
    for i in 0..600u64 {
        let f2 = f2.clone();
        tasks.push(tokio::spawn(async move {
            let len = match i % 3 {
                0 => 4096,
                1 => 100 << 10,
                _ => 4 << 20,
            };
            let off = (i * 3_333_331) % (SIZE - len as u64);
            let b = f2
                .read(NodeId(1), FileId(7), Generation(3), off, len)
                .await
                .unwrap();
            assert_eq!(b.len(), len, "read {i}");
            assert!(
                b.as_slice()
                    .iter()
                    .enumerate()
                    .step_by(997)
                    .all(|(k, v)| *v == byte(off + k as u64)),
                "read {i}: wrong bytes"
            );
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    // A tail shorter than the request, and an empty read at the end.
    let b = f2
        .read(NodeId(1), FileId(7), Generation(3), SIZE - 10, 4096)
        .await
        .unwrap();
    assert_eq!(b.len(), 10);
    let b = f2
        .read(NodeId(1), FileId(7), Generation(3), SIZE, 4096)
        .await
        .unwrap();
    assert_eq!(b.len(), 0);
}
