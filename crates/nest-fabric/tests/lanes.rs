//! TCP lanes (ADR-048) over loopback: a dialling host and an accepting one
//! read from each other over the lanes the dialler opened.

use futures::FutureExt;
use futures::future::BoxFuture;
use nest_fabric::tcp::{Lanes, LanesConfig};
use nest_fabric::{ReadSource, ServeFile};
use nest_types::{FileId, Generation, NestError, NodeId};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

/// Serves files from a map; any other generation is stale.
struct Files(HashMap<FileId, (Generation, Arc<std::fs::File>)>);

impl ReadSource for Files {
    fn open_now(
        &self,
        file: FileId,
        generation: Generation,
    ) -> Option<Result<ServeFile, NestError>> {
        Some(match self.0.get(&file) {
            Some((g, f)) if *g == generation => Ok(ServeFile {
                file: f.clone(),
                guard: Box::new(()),
            }),
            Some(_) => Err(NestError::Stale),
            None => Err(NestError::NotFound),
        })
    }

    fn open(
        &self,
        file: FileId,
        generation: Generation,
    ) -> BoxFuture<'static, Result<ServeFile, NestError>> {
        let r = self.open_now(file, generation).unwrap();
        async move { r }.boxed()
    }
}

fn pattern(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

fn files(dir: &std::path::Path, list: &[(u64, u64, Vec<u8>)]) -> Arc<dyn ReadSource> {
    std::fs::create_dir_all(dir).unwrap();
    let mut m = HashMap::new();
    for (id, g, data) in list {
        let p = dir.join(format!("{id}"));
        std::fs::File::create(&p).unwrap().write_all(data).unwrap();
        m.insert(
            FileId(*id),
            (Generation(*g), Arc::new(std::fs::File::open(&p).unwrap())),
        );
    }
    Arc::new(Files(m))
}

async fn pair(cfg: LanesConfig) -> (Arc<Lanes>, Arc<Lanes>) {
    let core = Lanes::start(
        NodeId(1),
        "lanes-test".into(),
        b"s".to_vec(),
        cfg.clone(),
        Some("127.0.0.1:0".parse().unwrap()),
    )
    .await
    .unwrap();
    let offgrid = Lanes::start(
        NodeId(8),
        "lanes-test".into(),
        b"s".to_vec(),
        cfg.clone(),
        None,
    )
    .await
    .unwrap();
    offgrid.dial(NodeId(1), core.local_addr().unwrap());
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while offgrid.connected(NodeId(1)) < cfg.per_peer || core.connected(NodeId(8)) < cfg.per_peer {
        assert!(
            std::time::Instant::now() < deadline,
            "lanes did not come up"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (core, offgrid)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lanes_carry_reads_both_ways() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LanesConfig {
        per_peer: 4,
        chunk: 1 << 20,
        ..LanesConfig::default()
    };
    let (core, offgrid) = pair(cfg).await;
    let big = pattern(1, (5 << 20) + 12345);
    let core_src = files(&dir.path().join("c"), &[(7, 3, big.clone())]);
    let small = pattern(2, 4000);
    let off_src = files(&dir.path().join("o"), &[(9, 1, small.clone())]);
    core.set_source(Arc::downgrade(&core_src));
    offgrid.set_source(Arc::downgrade(&off_src));

    // The off-grid host reads the whole file, many chunks in flight.
    let reads: Vec<_> = (0..big.len().div_ceil(1 << 20))
        .map(|i| {
            offgrid.read(
                NodeId(1),
                FileId(7),
                Generation(3),
                (i << 20) as u64,
                1 << 20,
            )
        })
        .collect();
    let mut got = Vec::new();
    for r in futures::future::join_all(reads).await {
        got.extend_from_slice(&r.unwrap());
    }
    assert_eq!(got, big, "chunks differ");
    // The core host reads from the off-grid host over the lanes it dialled.
    let r = core
        .read(NodeId(8), FileId(9), Generation(1), 100, 1 << 20)
        .await
        .unwrap();
    assert_eq!(r, small[100..]);
    // Failures come back as such.
    assert_eq!(
        offgrid
            .read(NodeId(1), FileId(7), Generation(2), 0, 10)
            .await,
        Err(NestError::Stale)
    );
    assert_eq!(
        offgrid
            .read(NodeId(1), FileId(70), Generation(1), 0, 10)
            .await,
        Err(NestError::NotFound)
    );
    // Past the end: nothing.
    let r = offgrid
        .read(NodeId(1), FileId(7), Generation(3), big.len() as u64, 10)
        .await;
    assert_eq!(r.unwrap(), Vec::<u8>::new());
    assert!(
        core.stats
            .served_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
            >= big.len() as u64
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lanes_come_back_after_the_accepting_host_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LanesConfig {
        per_peer: 2,
        chunk: 1 << 20,
        ..LanesConfig::default()
    };
    let (core, offgrid) = pair(cfg.clone()).await;
    let data = pattern(3, 3000);
    let src = files(dir.path(), &[(5, 1, data.clone())]);
    core.set_source(Arc::downgrade(&src));
    assert_eq!(
        offgrid
            .read(NodeId(1), FileId(5), Generation(1), 0, 3000)
            .await
            .unwrap(),
        data
    );

    let addr = core.local_addr().unwrap();
    core.shutdown();
    drop(core);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        offgrid
            .read(NodeId(1), FileId(5), Generation(1), 0, 10)
            .await
            .is_err()
    );
    let core = Lanes::start(
        NodeId(1),
        "lanes-test".into(),
        b"s".to_vec(),
        cfg,
        Some(addr),
    )
    .await
    .unwrap();
    core.set_source(Arc::downgrade(&src));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while offgrid.connected(NodeId(1)) < 2 {
        assert!(std::time::Instant::now() < deadline, "lanes not redialled");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        offgrid
            .read(NodeId(1), FileId(5), Generation(1), 0, 3000)
            .await
            .unwrap(),
        data
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wrong_secret_gets_no_lane() {
    let core = Lanes::start(
        NodeId(1),
        "lanes-test".into(),
        b"right".to_vec(),
        LanesConfig::default(),
        Some("127.0.0.1:0".parse().unwrap()),
    )
    .await
    .unwrap();
    let other = Lanes::start(
        NodeId(8),
        "lanes-test".into(),
        b"wrong".to_vec(),
        LanesConfig::default(),
        None,
    )
    .await
    .unwrap();
    other.dial(NodeId(1), core.local_addr().unwrap());
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(other.connected(NodeId(1)), 0);
    assert_eq!(core.connected(NodeId(8)), 0);
}

/// Applies writes to one file, checking the epoch as an owner does, and
/// choosing append offsets itself.
struct Sink {
    file: std::fs::File,
    epoch: nest_types::Epoch,
    end: std::sync::Mutex<()>,
}

impl nest_fabric::WriteSink for Sink {
    fn write(
        &self,
        op: nest_fabric::WriteOp,
        data: bytes::Bytes,
    ) -> BoxFuture<'static, Result<u32, NestError>> {
        use std::os::unix::fs::FileExt;
        let r = if op.file != FileId(5) {
            Err(NestError::NotFound)
        } else if op.epoch != self.epoch {
            Err(NestError::Stale)
        } else {
            let _end = self.end.lock().unwrap();
            let off = if op.append {
                self.file.metadata().unwrap().len()
            } else {
                op.offset
            };
            self.file.write_all_at(&data, off).unwrap();
            Ok(data.len() as u32)
        };
        async move { r }.boxed()
    }
}

/// ADR-057: writes over lanes land with the right bytes, appends at the
/// end the owner chooses, and the owner's errors come back as such.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lanes_carry_writes_to_the_owner() {
    use nest_fabric::{WriteError, WriteOp, WriteSink};
    let dir = tempfile::tempdir().unwrap();
    let cfg = LanesConfig {
        per_peer: 3,
        chunk: 1 << 20,
        ..LanesConfig::default()
    };
    let (core, offgrid) = pair(cfg).await;
    let path = dir.path().join("owned");
    let sink: Arc<dyn WriteSink> = Arc::new(Sink {
        file: std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap(),
        epoch: nest_types::Epoch(4),
        end: Default::default(),
    });
    core.set_sink(Arc::downgrade(&sink));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !offgrid.writable(NodeId(1)) || !core.writable(NodeId(8)) {
        assert!(std::time::Instant::now() < deadline, "no lane takes writes");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let op = |offset, append| WriteOp {
        file: FileId(5),
        epoch: nest_types::Epoch(4),
        writer: (NodeId(8), 1),
        offset,
        append,
    };
    // Positioned writes, one a full chunk.
    let a = pattern(4, 1 << 20);
    assert_eq!(
        offgrid
            .write(NodeId(1), op(0, false), a.clone().into())
            .await,
        Ok(1 << 20)
    );
    assert_eq!(
        offgrid
            .write(NodeId(1), op(10, false), b"0123".to_vec().into())
            .await,
        Ok(4)
    );
    // Concurrent appends from many tasks: none overlap.
    let rec = 4096usize;
    let appends: Vec<_> = (0..48u8)
        .map(|i| {
            let o = offgrid.clone();
            async move {
                o.write(NodeId(1), op(0, true), vec![i; rec].into())
                    .await
                    .unwrap()
            }
        })
        .collect();
    for n in futures::future::join_all(appends).await {
        assert_eq!(n as usize, rec);
    }
    let got = std::fs::read(&path).unwrap();
    assert_eq!(got.len(), (1 << 20) + 48 * rec);
    let mut want = a.clone();
    want[10..14].copy_from_slice(b"0123");
    assert_eq!(&got[..1 << 20], &want[..]);
    let mut seen: Vec<u8> = got[1 << 20..]
        .chunks(rec)
        .map(|c| {
            assert!(c.iter().all(|b| *b == c[0]), "appends interleaved");
            c[0]
        })
        .collect();
    seen.sort();
    assert_eq!(seen, (0..48).collect::<Vec<u8>>());
    // The owner's refusals are final, not "unsent".
    let stale = WriteOp {
        epoch: nest_types::Epoch(3),
        ..op(0, false)
    };
    assert_eq!(
        offgrid.write(NodeId(1), stale, b"x".to_vec().into()).await,
        Err(WriteError::Failed(NestError::Stale))
    );
    // Reads still work on the same lanes.
    let src = files(&dir.path().join("r"), &[(9, 1, pattern(9, 100))]);
    core.set_source(Arc::downgrade(&src));
    assert_eq!(
        offgrid
            .read(NodeId(1), FileId(9), Generation(1), 0, 100)
            .await
            .unwrap(),
        pattern(9, 100)
    );
    // No lane to a peer: unsent, safe to send another way.
    assert!(matches!(
        offgrid
            .write(NodeId(2), op(0, false), b"x".to_vec().into())
            .await,
        Err(WriteError::Unsent(_))
    ));
}

/// A relay between the dialling host and the accepting one that can
/// silently drop what the accepting host sends on the lanes open now,
/// while still carrying the other direction (a NAT that lost one side of
/// its mapping): lanes opened afterwards are relayed whole.
struct OneWayCut {
    addr: std::net::SocketAddr,
    /// Lanes numbered below this lose the accepting host's bytes.
    cut_below: Arc<std::sync::atomic::AtomicU64>,
    opened: Arc<std::sync::atomic::AtomicU64>,
}

impl OneWayCut {
    async fn start(to: std::net::SocketAddr) -> OneWayCut {
        use std::sync::atomic::{AtomicU64, Ordering};
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let cut_below = Arc::new(AtomicU64::new(0));
        let opened = Arc::new(AtomicU64::new(0));
        let (cut, n) = (cut_below.clone(), opened.clone());
        tokio::spawn(async move {
            while let Ok((a, _)) = l.accept().await {
                let me = n.fetch_add(1, Ordering::SeqCst);
                let Ok(b) = tokio::net::TcpStream::connect(to).await else {
                    continue;
                };
                let (mut ar, mut aw) = a.into_split();
                let (mut br, mut bw) = b.into_split();
                tokio::spawn(async move {
                    let _ = tokio::io::copy(&mut ar, &mut bw).await;
                });
                let cut = cut.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = vec![0u8; 64 << 10];
                    loop {
                        let k = match br.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(k) => k,
                        };
                        if me < cut.load(Ordering::SeqCst) {
                            continue;
                        }
                        if aw.write_all(&buf[..k]).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        OneWayCut {
            addr,
            cut_below,
            opened,
        }
    }

    /// Drop the accepting host's bytes on every lane open now.
    fn cut(&self) {
        use std::sync::atomic::Ordering;
        self.cut_below
            .store(self.opened.load(Ordering::SeqCst), Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_lane_that_stops_answering_is_replaced() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LanesConfig {
        per_peer: 3,
        chunk: 1 << 20,
        read_timeout: Duration::from_millis(300),
        ..LanesConfig::default()
    };
    let core = Lanes::start(
        NodeId(1),
        "lanes-test".into(),
        b"s".to_vec(),
        cfg.clone(),
        Some("127.0.0.1:0".parse().unwrap()),
    )
    .await
    .unwrap();
    let offgrid = Lanes::start(
        NodeId(8),
        "lanes-test".into(),
        b"s".to_vec(),
        cfg.clone(),
        None,
    )
    .await
    .unwrap();
    let relay = OneWayCut::start(core.local_addr().unwrap()).await;
    offgrid.dial(NodeId(1), relay.addr);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while core.connected(NodeId(8)) < 3 {
        assert!(
            std::time::Instant::now() < deadline,
            "lanes did not come up"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let data = pattern(4, 5000);
    let src = files(dir.path(), &[(11, 1, data.clone())]);
    offgrid.set_source(Arc::downgrade(&src));
    assert_eq!(
        core.read(NodeId(8), FileId(11), Generation(1), 0, 5000)
            .await
            .unwrap(),
        data
    );

    // The accepting host's requests now vanish on every open lane; the
    // dialling host's side still looks healthy and never redials by
    // itself. Each unanswered read closes its lane, so within a few reads
    // the dialler has opened fresh lanes that carry them again.
    relay.cut();
    let start = std::time::Instant::now();
    let mut failed = 0;
    loop {
        match core
            .read(NodeId(8), FileId(11), Generation(1), 0, 5000)
            .await
        {
            Ok(r) => {
                assert_eq!(r, data);
                break;
            }
            Err(_) => failed += 1,
        }
        assert!(
            failed <= 3,
            "reads still going down lanes that stopped answering"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(failed >= 1, "the cut did not take");
    assert!(start.elapsed() < Duration::from_secs(5));
    // And they keep working.
    for _ in 0..20 {
        assert_eq!(
            core.read(NodeId(8), FileId(11), Generation(1), 0, 5000)
                .await
                .unwrap(),
            data
        );
    }
}

/// Messages (lane protocol 3) reach the sink registered for their service,
/// both ways, carrying a push slot's size; the sink's error comes back, and
/// a service nobody registered is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lanes_carry_messages_to_their_service() {
    use nest_fabric::WriteError;
    use nest_fabric::tcp::MessageSink;
    struct Inbox(parking_lot::Mutex<Vec<(NodeId, Vec<u8>)>>);
    impl MessageSink for Inbox {
        fn message(
            &self,
            peer: NodeId,
            data: Vec<u8>,
        ) -> BoxFuture<'static, Result<(), NestError>> {
            let r = if data == b"refuse" {
                Err(NestError::Invalid("refused".into()))
            } else {
                self.0.lock().push((peer, data));
                Ok(())
            };
            async move { r }.boxed()
        }
    }
    let (core, offgrid) = pair(LanesConfig {
        per_peer: 2,
        ..LanesConfig::default()
    })
    .await;
    let (a, b) = (
        Arc::new(Inbox(Default::default())),
        Arc::new(Inbox(Default::default())),
    );
    core.set_message_sink(7, Arc::downgrade(&(a.clone() as Arc<dyn MessageSink>)));
    offgrid.set_message_sink(7, Arc::downgrade(&(b.clone() as Arc<dyn MessageSink>)));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !offgrid.takes_messages(NodeId(1)) || !core.takes_messages(NodeId(8)) {
        assert!(
            std::time::Instant::now() < deadline,
            "no lane takes messages"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let slot = pattern(9, 16 << 20);
    offgrid
        .message(NodeId(1), 7, slot.clone().into())
        .await
        .unwrap();
    core.message(NodeId(8), 7, b"back".to_vec().into())
        .await
        .unwrap();
    assert_eq!(*a.0.lock(), vec![(NodeId(8), slot)]);
    assert_eq!(*b.0.lock(), vec![(NodeId(1), b"back".to_vec())]);
    assert!(matches!(
        offgrid
            .message(NodeId(1), 7, b"refuse".to_vec().into())
            .await,
        Err(WriteError::Failed(NestError::Invalid(_)))
    ));
    assert!(matches!(
        offgrid.message(NodeId(1), 9, b"x".to_vec().into()).await,
        Err(WriteError::Failed(NestError::Unavailable(_)))
    ));
    assert_eq!(
        offgrid
            .stats
            .messages
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

/// Threads of this process named `name` (Linux: /proc/self/task/*/comm).
fn threads_named(name: &str) -> usize {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|t| std::fs::read_to_string(t.ok()?.path().join("comm")).ok())
        .filter(|c| c.trim_end() == name)
        .count()
}

/// A dialler expecting another node at this address (a stale entry, as
/// kakapo had for the id aviary first got) drops each lane after the
/// handshake and dials again. The accepting side's lane threads must end
/// with each lane: in 0.9.0 its writer waited forever on a channel the
/// lane itself kept open, one leaked thread per attempt, until the NAS
/// ran out of thread stacks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lanes_dropped_after_the_handshake_leave_no_threads() {
    let core = Lanes::start(
        NodeId(12),
        "lanes-test".into(),
        b"s".to_vec(),
        LanesConfig::default(),
        Some("127.0.0.1:0".parse().unwrap()),
    )
    .await
    .unwrap();
    // An id no other test here uses: the threads are counted by name, and
    // the tests share this process.
    let dialler = Lanes::start(
        NodeId(77),
        "lanes-test".into(),
        b"s".to_vec(),
        LanesConfig::default(),
        None,
    )
    .await
    .unwrap();
    // Expects node 9 there: every handshake answers 12, so every lane is
    // dropped and dialled again, every half second.
    dialler.dial(NodeId(9), core.local_addr().unwrap());
    tokio::time::sleep(Duration::from_millis(3200)).await;
    assert_eq!(dialler.connected(NodeId(9)), 0);
    dialler.dial_n(NodeId(9), "0.0.0.0:0".parse().unwrap(), 0);
    // Let the last attempts finish and their threads see the close.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let (tx, rx) = (threads_named("lane-tx-77"), threads_named("lane-rx-77"));
    assert!(
        tx <= 1 && rx <= 1,
        "lane threads left behind on the accepting side: {tx} writers, {rx} readers"
    );
}
