//! Without io_uring (QNAP's kernel has none), a transfer's writes must not
//! block the async runtime: a disk that stalls for seconds held every
//! worker of the NAS host's daemon, and its lease renewal with them, so it
//! stopped serving (ADR-092). Its own test binary: the ring is chosen once
//! per process.

use nest_store::{ObjectKey, ObjectStore, uring};
use nest_types::{FileId, Generation};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[test]
fn a_stalled_write_leaves_the_runtime_running() {
    // SAFETY (env): set before any thread of this process reads it.
    unsafe { std::env::set_var("SPARKNEST_NO_IO_URING", "1") };
    assert!(uring::ring().is_none());
    let dir = tempfile::tempdir().unwrap();
    let store = ObjectStore::open(dir.path()).unwrap();
    let st = Arc::new(
        store
            .begin_staging(ObjectKey::new(FileId(1), Generation(1)))
            .unwrap(),
    );
    // One worker: a write blocking it would stop the ticker below.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let ticks = Arc::new(AtomicU64::new(0));
    rt.block_on(async {
        let t = ticks.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                t.fetch_add(1, Ordering::Relaxed);
            }
        });
        // A stalling disk: every write holds its thread for 300 ms. Twelve
        // of them, in the transfer's window, are 3.6 s of blocked disk.
        uring::set_test_write_delay(Duration::from_millis(300));
        let before = ticks.load(Ordering::Relaxed);
        let started = std::time::Instant::now();
        // On a worker, as the daemon's copies run (block_on's own future
        // runs on this thread, not on the runtime's worker).
        let w = st.clone();
        tokio::spawn(async move {
            for i in 0..12u64 {
                let buf = uring::AlignedBuf::copy_of(&vec![i as u8; 1 << 20]);
                w.write(buf, i << 20).await.unwrap();
            }
            w.flushed().await.unwrap();
        })
        .await
        .unwrap();
        let took = started.elapsed();
        uring::set_test_write_delay(Duration::ZERO);
        let ticked = ticks.load(Ordering::Relaxed) - before;
        // The ticker ran throughout (about took/10ms ticks); a blocked
        // worker would have stopped it for most of the run.
        let expected = took.as_millis() as u64 / 10;
        assert!(
            ticked * 2 >= expected,
            "the runtime stalled with the disk: {ticked} ticks in {took:?} (expected ~{expected})"
        );
    });
    let st = Arc::try_unwrap(st).ok().unwrap();
    st.file().set_len(12 << 20).unwrap();
    let k = store.commit_staging(st).unwrap();
    let mut got = Vec::new();
    use std::io::Read;
    store.open_read(k).unwrap().read_to_end(&mut got).unwrap();
    assert_eq!(got.len(), 12 << 20);
    for i in 0..12usize {
        assert!(got[i << 20..(i + 1) << 20].iter().all(|&b| b == i as u8));
    }
}
