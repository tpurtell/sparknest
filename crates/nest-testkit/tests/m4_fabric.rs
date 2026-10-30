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
        ..nest_fabric::FabricConfig::default()
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
async fn sequential_remote_read_uses_rdma() {
    let Some(c) = fabric_cluster(3).await else {
        return;
    };
    let v1 = &c.node(1).vfs;
    let size: u64 = std::env::var("NEST_BENCH_MIB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(64)
        << 20;
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
    // Timed pass: discard the bytes (a benchmark must not measure its own
    // page faults), then a second pass checks every byte.
    let t = std::time::Instant::now();
    let mut pos = 0u64;
    let mut sink = 0u64;
    loop {
        let b = v2.read(rfh, pos, 1 << 20).await.unwrap();
        if b.is_empty() {
            break;
        }
        sink = sink.wrapping_add(b[0] as u64);
        pos += b.len() as u64;
    }
    let secs = t.elapsed().as_secs_f64();
    assert_eq!(pos, size);
    let mut got = Vec::with_capacity(size as usize);
    loop {
        let b = v2.read(rfh, got.len() as u64, 1 << 20).await.unwrap();
        if b.is_empty() {
            break;
        }
        got.extend_from_slice(&b);
    }
    assert!(got == data, "content mismatch ({sink})");
    let fab = &c.node(2).fabric().unwrap();
    let reads = fab.stats.reads.load(Ordering::Relaxed);
    eprintln!(
        "{} MiB over the Vfs read path: {:.2} GB/s ({reads} fabric reads over both passes)",
        size >> 20,
        size as f64 / secs / 1e9
    );
    assert_eq!(
        fab.stats.read_bytes.load(Ordering::Relaxed),
        2 * size,
        "every byte should arrive over RDMA once per pass"
    );
    // Read exactly as asked (ADR-065): one fabric read per 1 MiB request,
    // plus each pass's read at the end of the file.
    assert_eq!(reads, 2 * (size.div_ceil(1 << 20) + 1));

    // Same bytes straight from the fabric with 8 chunks in flight, to
    // separate transport + server cost from the Vfs read path.
    let generation = c.attr(2, a.id).unwrap().generation;
    let t = std::time::Instant::now();
    let chunk = 4u64 << 20;
    let mut inflight = std::collections::VecDeque::new();
    let mut off = 0u64;
    let mut total = 0usize;
    while off < size || !inflight.is_empty() {
        while inflight.len() < 8 && off < size {
            let f = fab.clone();
            let o = off;
            inflight.push_back(tokio::spawn(async move {
                f.read(NodeId(1), a.id, generation, o, chunk as usize)
                    .await
                    .unwrap()
                    .len()
            }));
            off += chunk;
        }
        total += inflight.pop_front().unwrap().await.unwrap();
    }
    let secs = t.elapsed().as_secs_f64();
    eprintln!(
        "raw fabric, 8 in flight: {:.2} GB/s",
        total as f64 / secs / 1e9
    );

    // Random reads are correct too.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn owned_file_reads_see_latest_writes_within_a_generation() {
    let Some(c) = fabric_cluster(3).await else {
        return;
    };
    let v1 = &c.node(1).vfs;
    let (a, fh, _) = v1
        .create(FileId::ROOT, b"growing", 0o644, oflags::RDWR)
        .await
        .unwrap();
    v1.write(fh, 0, vec![b'a'; 8 << 20]).await.unwrap();
    c.converge().await;
    let v2 = &c.node(2).vfs;
    let (rfh, _) = v2.open(a.id, 0).await.unwrap();
    assert_eq!(&v2.read(rfh, 0, 4).await.unwrap()[..], b"aaaa");
    assert_eq!(&v2.read(rfh, 5 << 20, 4).await.unwrap()[..], b"aaaa");
    // Same generation (still owned by node 1), new bytes: never cached.
    v1.write(fh, 0, vec![b'b'; 8 << 20]).await.unwrap();
    assert_eq!(c.attr(2, a.id).unwrap().generation, a.generation);
    assert_eq!(&v2.read(rfh, 0, 4).await.unwrap()[..], b"bbbb");
    assert_eq!(&v2.read(rfh, 5 << 20, 4).await.unwrap()[..], b"bbbb");
    assert!(
        v2.try_read_now(rfh, 0, 4).is_none(),
        "owned remote files must not be served from readahead"
    );
    v2.release(rfh, None).await;
    v1.release(fh, None).await;
}

/// The FUSE fast path runs on threads outside the tokio runtime: a
/// readahead hit there must be able to keep the pipeline going.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn fast_path_readahead_from_a_non_runtime_thread() {
    let Some(c) = fabric_cluster(2).await else {
        return;
    };
    let v1 = &c.node(1).vfs;
    let size = 48u64 << 20;
    let (a, fh, _) = v1
        .create(FileId::ROOT, b"f", 0o644, oflags::WRONLY)
        .await
        .unwrap();
    for off in (0..size).step_by(1 << 20) {
        v1.write(fh, off, vec![(off >> 20) as u8; 1 << 20])
            .await
            .unwrap();
    }
    v1.release(fh, None).await;
    c.eventually("stable", Duration::from_secs(5), |c| {
        c.attr(2, a.id)
            .is_some_and(|x| x.gen_state == GenState::Stable)
    })
    .await;
    let v2 = c.node(2).vfs.clone();
    let (rfh, _) = v2.open(a.id, 0).await.unwrap();
    // Prime readahead from the runtime, as the async path does.
    assert_eq!(v2.read(rfh, 0, 1 << 20).await.unwrap()[0], 0);
    let rt = tokio::runtime::Handle::current();
    let total = std::thread::spawn(move || {
        let mut pos = 1u64 << 20;
        while pos < size {
            let b = match v2.try_read_now(rfh, pos, 1 << 20) {
                Some(r) => r.unwrap(),
                None => rt.block_on(v2.read(rfh, pos, 1 << 20)).unwrap(),
            };
            assert_eq!(b[0], (pos >> 20) as u8, "at {pos}");
            pos += b.len() as u64;
        }
        pos
    })
    .join()
    .unwrap();
    assert_eq!(total, size);
    c.node(2).vfs.release(rfh, None).await;
}

/// ADR-057: writes to a file another host owns travel over RDMA (the
/// owner pulls them), land exactly once with the right bytes, and appends
/// from two hosts never overlap. Also measures 1 MiB remote writes against
/// the same writes over the control connection (`NEST_BENCH_MIB` sizes it).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn remote_writes_go_over_rdma() {
    let Some(c) = fabric_cluster(3).await else {
        return;
    };
    let v1 = &c.node(1).vfs;
    let (a, fh1, _) = v1
        .create(FileId::ROOT, b"shared", 0o644, oflags::RDWR)
        .await
        .unwrap();
    v1.write(fh1, 0, b"head".to_vec()).await.unwrap();
    c.converge().await;
    let v2 = &c.node(2).vfs;
    let (fh2, _) = v2.open(a.id, oflags::RDWR).await.unwrap();
    let f1 = c.node(1).fabric().unwrap();
    let f2 = c.node(2).fabric().unwrap();
    let pulled = || f1.stats.pulled.load(Ordering::Relaxed);
    let before = pulled();
    let data: Vec<u8> = (0..(3u64 << 20) + 777).map(pattern).collect();
    for (i, chunk) in data.chunks(1 << 20).enumerate() {
        let n = v2
            .write(fh2, 4 + ((i as u64) << 20), chunk.to_vec())
            .await
            .unwrap();
        assert_eq!(n as usize, chunk.len());
    }
    assert_eq!(c.attr(2, a.id).unwrap().owner, Some(NodeId(1)));
    assert_eq!(pulled() - before, 4, "writes did not go over RDMA");
    assert!(f2.stats.writes.load(Ordering::Relaxed) >= 4);
    // Node 3 reads through the owner: the bytes as written.
    let v3 = &c.node(3).vfs;
    let (rfh, _) = v3.open(a.id, 0).await.unwrap();
    let mut got = Vec::new();
    loop {
        let b = v3.read(rfh, got.len() as u64, 1 << 20).await.unwrap();
        if b.is_empty() {
            break;
        }
        got.extend_from_slice(&b);
    }
    v3.release(rfh, None).await;
    assert_eq!(&got[..4], b"head");
    assert!(got[4..] == data[..], "remote write bytes differ");
    v2.release(fh2, None).await;
    v1.release(fh1, None).await;

    // Appends from two hosts at once, one over RDMA and one local.
    let (b, fh, _) = v1
        .create(FileId::ROOT, b"log", 0o644, oflags::WRONLY | oflags::APPEND)
        .await
        .unwrap();
    v1.write(fh, 0, vec![0u8; 0]).await.unwrap();
    c.converge().await;
    let (fh2, _) = v2
        .open(b.id, oflags::WRONLY | oflags::APPEND)
        .await
        .unwrap();
    let rec = 4096usize;
    let mut tasks = Vec::new();
    for (node, h) in [(1u64, fh), (2, fh2)] {
        for t in 0..4u8 {
            let v = c.node(node).vfs.clone();
            tasks.push(tokio::spawn(async move {
                for i in 0..16u8 {
                    let tag = [node as u8, t * 16 + i];
                    let rec: Vec<u8> = tag.iter().copied().cycle().take(rec).collect();
                    v.write(h, 0, rec).await.unwrap();
                }
            }));
        }
    }
    for t in tasks {
        t.await.unwrap();
    }
    v2.release(fh2, None).await;
    v1.release(fh, None).await;
    c.eventually("log stable", Duration::from_secs(5), |c| {
        c.attr(3, b.id)
            .is_some_and(|x| x.gen_state == GenState::Stable)
    })
    .await;
    let bytes = std::fs::read(c.node(1).data.store().path(nest_store::ObjectKey::new(
        b.id,
        c.attr(1, b.id).unwrap().generation,
    )))
    .unwrap();
    assert_eq!(bytes.len(), 2 * 64 * rec, "lost or duplicated appends");
    let mut seen: Vec<(u8, u8)> = bytes
        .chunks(rec)
        .map(|r| {
            assert!(r.chunks(2).all(|p| p == &r[..2]), "appends overlapped");
            (r[0], r[1])
        })
        .collect();
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 128);

    // 1 MiB remote writes: RDMA, then the same over the control connection
    // (a fabric limited to protocol 2 cannot write).
    let mib: u64 = std::env::var("NEST_BENCH_MIB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    let (d, fh1, _) = v1
        .create(FileId::ROOT, b"bench", 0o644, oflags::RDWR)
        .await
        .unwrap();
    v1.write(fh1, 0, vec![1]).await.unwrap();
    c.converge().await;
    let (fh2, _) = v2.open(d.id, oflags::RDWR).await.unwrap();
    let buf = vec![7u8; 1 << 20];
    let run = |inflight: usize| {
        let v2 = v2.clone();
        let buf = buf.clone();
        async move {
            let t = std::time::Instant::now();
            let mut done = 0u64;
            while done < mib {
                let batch: Vec<_> = (0..inflight as u64)
                    .take_while(|k| done + k < mib)
                    .map(|k| v2.write(fh2, (done + k) << 20, buf.clone()))
                    .collect();
                let n = batch.len() as u64;
                for r in futures::future::join_all(batch).await {
                    r.unwrap();
                }
                done += n;
            }
            (mib << 20) as f64 / t.elapsed().as_secs_f64() / 1e9
        }
    };
    let before = pulled();
    let rdma1 = run(1).await;
    let rdma8 = run(8).await;
    assert_eq!(pulled() - before, 2 * mib, "bench writes not over RDMA");
    // Force the control connection: no fabric, no lanes for this node.
    let saved = c.node(2).vfs.swap_transports(None, None);
    let tcp1 = run(1).await;
    let tcp8 = run(8).await;
    assert_eq!(pulled() - before, 2 * mib, "control writes went over RDMA");
    c.node(2).vfs.swap_transports(saved.0, saved.1);
    eprintln!(
        "remote 1 MiB writes, {mib} MiB: RDMA {rdma1:.2} GB/s (1 in flight), {rdma8:.2} GB/s (8); \
         control TCP {tcp1:.2} GB/s (1), {tcp8:.2} GB/s (8)"
    );
    v2.release(fh2, None).await;
    v1.release(fh1, None).await;
}

/// Remote reads through a real mount over RDMA, read as asked with the
/// kernel reading ahead (ADR-065): a remote sealed file read sequentially,
/// cold, then at random 4 KiB offsets. A measurement; it checks the bytes.
///   NEST_BENCH_MIB=1024 NEST_RA_KB_WAIT=16384 cargo test --release -p nest-testkit --test m4_fabric remote_reads_through -- --ignored --nocapture
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn remote_reads_through_the_mount() {
    use std::os::unix::fs::FileExt;
    let Some(c) = fabric_cluster(2).await else {
        return;
    };
    let size: u64 = std::env::var("NEST_BENCH_MIB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(256)
        << 20;
    let v1 = &c.node(1).vfs;
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
    c.eventually("stable", Duration::from_secs(10), |c| {
        c.attr(2, a.id)
            .is_some_and(|x| x.gen_state == GenState::Stable)
    })
    .await;
    v1.seal(a.id, true).await.unwrap();
    let Some(mp) = c.mount(2) else {
        return;
    };
    // Production mounts read ahead 16 MiB (the unit raises it as root);
    // a test mount starts at the kernel's 128 KiB. NEST_RA_KB_WAIT makes
    // the test wait until something (sudo, by hand) has raised it.
    let bdi = {
        use std::os::unix::fs::MetadataExt;
        let d = std::fs::metadata(&mp).unwrap().dev();
        format!(
            "/sys/class/bdi/{}:{}/read_ahead_kb",
            libc::major(d),
            libc::minor(d)
        )
    };
    if let Ok(want) = std::env::var("NEST_RA_KB_WAIT") {
        eprintln!("waiting for {bdi} = {want}: echo {want} | sudo tee {bdi}");
        c.eventually("read_ahead_kb", Duration::from_secs(600), |_| {
            std::fs::read_to_string(&bdi).is_ok_and(|v| v.trim() == want)
        })
        .await;
    }
    eprintln!(
        "mount read_ahead_kb = {}",
        std::fs::read_to_string(&bdi).unwrap_or_default().trim()
    );
    let path = mp.join("shard");
    {
        let p = path.clone();
        let want = data.clone();
        let (seq, rnd) = tokio::task::spawn_blocking(move || {
            let f = std::fs::File::open(&p).unwrap();
            // Cold: drop this file's pages from the kernel's cache.
            // SAFETY: plain syscall on an open fd.
            unsafe {
                libc::posix_fadvise(
                    std::os::fd::AsRawFd::as_raw_fd(&f),
                    0,
                    0,
                    libc::POSIX_FADV_DONTNEED,
                )
            };
            let mut buf = vec![0u8; 1 << 20];
            let t = std::time::Instant::now();
            let mut pos = 0u64;
            loop {
                let n = f.read_at(&mut buf, pos).unwrap();
                if n == 0 {
                    break;
                }
                assert_eq!(&buf[..n], &want[pos as usize..pos as usize + n], "at {pos}");
                pos += n as u64;
            }
            assert_eq!(pos, want.len() as u64);
            let seq = t.elapsed();
            unsafe {
                libc::posix_fadvise(
                    std::os::fd::AsRawFd::as_raw_fd(&f),
                    0,
                    0,
                    libc::POSIX_FADV_DONTNEED,
                )
            };
            let mut b4 = [0u8; 4096];
            let t = std::time::Instant::now();
            let mut x = 0x9e3779b97f4a7c15u64;
            for _ in 0..2000 {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let off = (x % (want.len() as u64 - 4096)) & !4095;
                f.read_exact_at(&mut b4, off).unwrap();
                assert_eq!(&b4[..], &want[off as usize..off as usize + 4096]);
            }
            (seq, t.elapsed())
        })
        .await
        .unwrap();
        eprintln!(
            "sequential {} MiB {:.2} GB/s; 2000 random 4 KiB reads {:.0} µs each",
            size >> 20,
            size as f64 / seq.as_secs_f64() / 1e9,
            rnd.as_micros() as f64 / 2000.0
        );
    }
    c.node(2).unmount();
}

/// Remote reads on the FUSE fast path run no metadata queries: the handle
/// remembers that there is no copy here (each read once queried SQLite
/// twice, and 64 FUSE queues serialised on its page-cache mutex). A copy
/// arriving here, or leaving, is noticed at the next read.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn remote_reads_run_no_metadata_queries() {
    let Some(c) = fabric_cluster(2).await else {
        return;
    };
    let v1 = &c.node(1).vfs;
    let size = 8u64 << 20;
    let (a, fh, _) = v1
        .create(FileId::ROOT, b"table", 0o644, oflags::WRONLY)
        .await
        .unwrap();
    let data: Vec<u8> = (0..size).map(pattern).collect();
    v1.write(fh, 0, data.clone()).await.unwrap();
    v1.release(fh, None).await;
    c.eventually("stable", Duration::from_secs(5), |c| {
        c.attr(2, a.id)
            .is_some_and(|x| x.gen_state == GenState::Stable)
    })
    .await;
    let v2 = c.node(2).vfs.clone();
    let (rfh, _) = v2.open(a.id, 0).await.unwrap();
    // The first read makes the RDMA link (the fast path never waits).
    assert_eq!(v2.read(rfh, 0, 4096).await.unwrap(), &data[..4096]);
    // As the FUSE frontend reads a page: here if it can, else over the
    // fabric from the completion thread. Returns where it came from.
    let page = |off: u64| -> (bool, Vec<u8>) {
        if let Some(r) = v2.try_read_now(rfh, off, 4096) {
            return (true, r.unwrap());
        }
        let r = v2.prepare_remote(rfh, off, 4096).expect("fabric read");
        let (tx, rx) = std::sync::mpsc::channel();
        r.start(Box::new(move |b| {
            let _ = tx.send(b.map(|b| b.to_vec()));
        }));
        (false, rx.recv().unwrap().unwrap())
    };
    let offsets: Vec<u64> = (0..2000u64)
        .map(|i| (i.wrapping_mul(0x9E3779B97F4A7C15) % (size / 4096)) * 4096)
        .collect();
    let check = |off: u64, b: &[u8]| {
        assert_eq!(b, &data[off as usize..off as usize + 4096], "at {off}");
    };
    // The first read decides; the rest ask nothing of SQLite.
    let (local, b) = page(offsets[0]);
    assert!(!local);
    check(offsets[0], &b);
    let before = v2.metadata_queries();
    let t = std::time::Instant::now();
    for &off in &offsets[1..] {
        let (local, b) = page(off);
        assert!(!local);
        check(off, &b);
    }
    // Within a second (the remembered answer's lifetime).
    if t.elapsed() < Duration::from_millis(900) {
        assert_eq!(v2.metadata_queries() - before, 0, "queries per remote read");
    }

    // A copy here: the next reads are local.
    v2.replicate_into(a.id, StoreId(2)).await.unwrap();
    let (local, b) = page(offsets[1]);
    assert!(local, "a new local copy is read at once");
    check(offsets[1], &b);
    let before = v2.metadata_queries();
    for &off in &offsets[2..200] {
        let (local, b) = page(off);
        assert!(local);
        check(off, &b);
    }
    assert!(
        v2.metadata_queries() - before <= 2,
        "local reads query at most once per second"
    );
    // Gone again: never read from the evicted copy.
    assert!(v2.evict_from(a.id, StoreId(2)).await.unwrap());
    let (local, b) = page(offsets[3]);
    assert!(!local, "an evicted copy is not read");
    check(offsets[3], &b);
    v2.release(rfh, None).await;
}
