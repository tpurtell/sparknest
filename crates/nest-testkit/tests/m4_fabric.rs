//! The data plane over the RDMA fabric (in-process nodes talking through
//! this machine's NIC). Skips when no RoCE rail is present.

use nest_data::vfs::oflags;
use nest_testkit::TestCluster;
use nest_types::*;
use std::sync::atomic::Ordering;
use std::time::Duration;

async fn fabric_cluster(n: u64) -> Option<TestCluster> {
    if nest_fabric::discover(&[]).is_empty() {
        return None;
    }
    let mut t = nest_testkit::fast_tuning();
    t.fabric = Some(nest_fabric::FabricConfig {
        chunk: 4 << 20,
        client_slots: 32,
        server_slots: 16,
        window: 16,
        devices: vec![],
    });
    let c = TestCluster::start_with(n, t).await;
    c.eventually("serving", Duration::from_secs(8), |c| {
        (1..=n).all(|i| c.node(i).data.caught_up() && c.node(i).data.session().is_some())
    })
    .await;
    Some(c)
}

fn pattern(i: u64) -> u8 {
    (i.wrapping_mul(0x9E3779B97F4A7C15) >> 29) as u8
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sequential_remote_read_uses_rdma_with_readahead() {
    let Some(c) = fabric_cluster(3).await else {
        return;
    };
    let v1 = &c.node(1).vfs;
    let size: u64 = 64 << 20;
    let (a, fh, _) = v1
        .create(FileId::ROOT, b"shard", 0o644, oflags::WRONLY)
        .await
        .unwrap();
    let data: Vec<u8> = (0..size).map(pattern).collect();
    for off in (0..size).step_by(1 << 20) {
        v1.write(
            fh,
            off,
            data[off as usize..(off + (1 << 20)) as usize].to_vec(),
        )
        .await
        .unwrap();
    }
    v1.release(fh, None).await;
    c.eventually("stable", Duration::from_secs(5), |c| {
        c.attr(2, a.id)
            .is_some_and(|x| x.gen_state == GenState::Stable)
    })
    .await;

    // Node 2 reads it 1 MiB at a time like FUSE does.
    let v2 = &c.node(2).vfs;
    let (rfh, _) = v2.open(a.id, 0).await.unwrap();
    let t = std::time::Instant::now();
    let mut got = Vec::with_capacity(size as usize);
    loop {
        let b = v2.read(rfh, got.len() as u64, 1 << 20).await.unwrap();
        if b.is_empty() {
            break;
        }
        got.extend_from_slice(&b);
    }
    let secs = t.elapsed().as_secs_f64();
    assert!(got == data, "content mismatch");
    let fab = c.node(2).fabric.as_ref().unwrap();
    let reads = fab.stats.reads.load(Ordering::Relaxed);
    eprintln!(
        "64 MiB over RDMA in {secs:.3}s ({:.2} GB/s), {reads} fabric reads",
        size as f64 / secs / 1e9
    );
    assert_eq!(
        fab.stats.read_bytes.load(Ordering::Relaxed),
        size,
        "every byte should arrive over RDMA once"
    );
    assert_eq!(reads, size / (4 << 20));

    // Random reads still correct (window collapses, no over-fetch beyond need).
    for off in [(37u64 << 20) + 11, 5 << 20, (63 << 20) + 1000] {
        let b = v2.read(rfh, off, 4096).await.unwrap();
        let end = (off + 4096).min(size);
        assert_eq!(b, &data[off as usize..end as usize]);
    }
    v2.release(rfh, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rdma_reads_respect_generation_fencing() {
    let Some(c) = fabric_cluster(3).await else {
        return;
    };
    let v1 = &c.node(1).vfs;
    let (a, fh, _) = v1
        .create(FileId::ROOT, b"w", 0o644, oflags::RDWR)
        .await
        .unwrap();
    v1.write(fh, 0, vec![b'1'; 8192]).await.unwrap();
    v1.release(fh, None).await;
    c.eventually("stable", Duration::from_secs(5), |c| {
        c.attr(3, a.id)
            .is_some_and(|x| x.gen_state == GenState::Stable)
    })
    .await;
    let v3 = &c.node(3).vfs;
    let (rfh, _) = v3.open(a.id, 0).await.unwrap();
    assert_eq!(v3.read(rfh, 0, 4).await.unwrap(), b"1111");
    // Rewrite on node 1; node 3's handle (with readahead state for the old
    // generation) must see the new bytes, never the old ones.
    let (wfh, _) = v1.open(a.id, oflags::RDWR).await.unwrap();
    v1.write(wfh, 0, vec![b'2'; 8192]).await.unwrap();
    assert_eq!(v3.read(rfh, 0, 4).await.unwrap(), b"2222");
    assert_eq!(v3.read(rfh, 4096, 4).await.unwrap(), b"2222");
    v1.release(wfh, None).await;
    v3.release(rfh, None).await;
}
