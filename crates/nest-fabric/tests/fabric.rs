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
        max_inflight: 16,
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
