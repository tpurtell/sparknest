//! Push loads (ADR-063) end to end: a coordinator opens a group on its
//! host, ranks on other hosts join with memfd rings, and every rank's
//! arena ends up holding exactly the bytes its plan names.

use nest_data::vfs::oflags;
use nest_push::client::{Coordinator, JoinArgs, Rank};
use nest_push::plan::{Dim, Entry, Pattern, Steps};
use nest_push::proto::{Event, RankSpec};
use nest_push::slot;
use nest_testkit::TestCluster;
use nest_types::{FileId, GenState, NodeId};
use std::os::fd::{AsRawFd, OwnedFd};
use std::time::Duration;

async fn ready(n: u64) -> TestCluster {
    let c = TestCluster::start(n).await;
    c.eventually("serving", Duration::from_secs(8), |c| {
        (1..=n).all(|i| c.node(i).data.caught_up() && c.node(i).data.session().is_some())
    })
    .await;
    c.eventually("names registered", Duration::from_secs(8), |c| {
        c.node(1)
            .placer
            .nodes()
            .map(|n| n.iter().all(|h| !h.name.starts_with("node")))
            .unwrap_or(false)
    })
    .await;
    c
}

/// A sealed file of `data` written on node `on`.
async fn sealed(c: &TestCluster, on: u64, name: &str, data: Vec<u8>) -> FileId {
    let v = &c.node(on).vfs;
    let (a, fh, _) = v
        .create(FileId::ROOT, name.as_bytes(), 0o644, oflags::WRONLY)
        .await
        .unwrap();
    let mut off = 0;
    for chunk in data.chunks(1 << 20) {
        v.write(fh, off, chunk.to_vec()).await.unwrap();
        off += chunk.len() as u64;
    }
    v.release(fh, None).await;
    c.eventually("stable", Duration::from_secs(5), |c| {
        c.attr(on, a.id).unwrap().gen_state == GenState::Stable
    })
    .await;
    v.seal(a.id, true).await.unwrap();
    a.id
}

fn memfd(len: usize) -> OwnedFd {
    let fd = nix::sys::memfd::memfd_create(c"push-ring", nix::sys::memfd::MFdFlags::MFD_CLOEXEC)
        .unwrap();
    nix::unistd::ftruncate(&fd, len as i64).unwrap();
    fd
}

/// What an engine rank does: copy each filled slot's fragments into its
/// arena, release the slot, until Done. Returns the arena.
fn run_rank(
    r: Rank,
    ring: OwnedFd,
    slots: u32,
    slot_bytes: u32,
    arena: usize,
) -> Result<Vec<u8>, String> {
    let file = std::fs::File::from(ring);
    let mut out = vec![0u8; arena];
    loop {
        match r.next().map_err(|e| e.to_string())? {
            Some(Event::Filled { slot: i, len }) => {
                assert!(i < slots && len == slot_bytes);
                let mut buf = vec![0u8; slot_bytes as usize];
                use std::os::unix::fs::FileExt;
                file.read_exact_at(&mut buf, i as u64 * slot_bytes as u64)
                    .unwrap();
                let p = slot::parse(&buf, arena as u64).map_err(|e| e.to_string())?;
                let mut at = 0usize;
                for (dst, n) in slot::Fragments::new(&buf, p) {
                    out[dst as usize..dst as usize + n as usize]
                        .copy_from_slice(&buf[at..at + n as usize]);
                    at += n as usize;
                }
                r.release(i).map_err(|e| e.to_string())?;
            }
            Some(Event::Done) => return Ok(out),
            Some(Event::Failed { reason, .. }) => return Err(reason),
            Some(other) => return Err(format!("unexpected {other:?}")),
            None => return Err("socket closed".into()),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn ranks_receive_exactly_their_strided_shares() {
    strided_shares(false).await;
}

/// With TCP lanes between the hosts (the standby lanes fabric pairs keep),
/// slots that cannot go by RDMA travel on them, off the control connection
/// (ADR-067).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn slots_without_rdma_travel_on_tcp_lanes() {
    strided_shares(true).await;
}

async fn strided_shares(lanes: bool) {
    let c = if lanes {
        let c = TestCluster::start_with_standby(3, nest_testkit::fast_tuning()).await;
        c.eventually("serving", Duration::from_secs(8), |c| {
            (1..=3).all(|i| c.node(i).data.caught_up() && c.node(i).data.session().is_some())
        })
        .await;
        c.eventually("lanes take messages", Duration::from_secs(10), |c| {
            [(2, 1), (2, 3), (3, 1)].iter().all(|(a, b)| {
                c.node(*a)
                    .vfs
                    .lanes()
                    .is_some_and(|l| l.takes_messages(NodeId(*b)))
            })
        })
        .await;
        c.eventually("names registered", Duration::from_secs(8), |c| {
            c.node(1)
                .placer
                .nodes()
                .map(|n| n.iter().all(|h| !h.name.starts_with("node")))
                .unwrap_or(false)
        })
        .await;
        c
    } else {
        ready(3).await
    };
    let messages = |c: &TestCluster| -> u64 {
        (1..=3)
            .filter_map(|i| c.node(i).vfs.lanes())
            .map(|l| l.stats.messages.load(std::sync::atomic::Ordering::Relaxed))
            .sum()
    };
    let before = messages(&c);
    // Two files: one on node 2, one on node 3 (each written there).
    let (rows, units, unit) = (64u32, 6u32, 1024u32);
    let row = (units * unit) as usize;
    let len = rows as usize * row;
    let a: Vec<u8> = (0..len).map(|i| (i * 7 % 251) as u8).collect();
    let b: Vec<u8> = (0..len).map(|i| (i * 13 % 241) as u8).collect();
    sealed(&c, 2, "a.bin", a.clone()).await;
    sealed(&c, 3, "b.bin", b.clone()).await;

    // Rank 0 (on node 1) takes units {0,2,4} of every row, rank 1 (on
    // node 3) units {1,3,5}, packed; of both files, one after the other
    // in the arena.
    let owned: [Vec<u32>; 2] = [vec![0, 2, 4], vec![1, 3, 5]];
    let share = rows as u64 * 3 * unit as u64;
    let patterns: Vec<Pattern> = owned
        .iter()
        .map(|sel| Pattern {
            run: unit,
            dims: vec![
                Dim {
                    steps: Steps::Count(rows),
                    src_stride: row as u64,
                    dst_stride: sel.len() as u64 * unit as u64,
                },
                Dim {
                    steps: Steps::Indices(sel.clone()),
                    src_stride: unit as u64,
                    dst_stride: unit as u64,
                },
            ],
        })
        .collect();
    let mut entries = Vec::new();
    for f in 0..2u32 {
        for r in 0..2u32 {
            entries.push(Entry {
                file: f,
                pattern: r,
                src_base: 0,
                dst_base: f as u64 * share,
                rank: r,
            });
        }
    }
    let arena = 2 * share;
    let (slots, slot_bytes) = (4u32, 64u32 << 10);
    let hub = c.push_socket(1);
    let rank_sockets = [c.push_socket(1), c.push_socket(3)];

    let res = tokio::task::spawn_blocking(move || {
        let t = Some(Duration::from_secs(20));
        let g = Coordinator::create(
            &hub,
            vec![
                RankSpec {
                    rank: 0,
                    host: "n1".into(),
                    arena_bytes: arena,
                },
                RankSpec {
                    rank: 1,
                    host: "n3".into(),
                    arena_bytes: arena,
                },
            ],
            vec!["/a.bin".into(), "/b.bin".into()],
            t,
        )
        .map_err(|e| format!("create: {e}"))?;
        g.patterns(patterns).unwrap();
        g.entries(entries).unwrap();
        let (group, token) = (g.group, g.token);
        // Ranks join from their own hosts while Start waits for them.
        let ranks: Vec<_> = (0..2u32)
            .map(|r| {
                let sock = rank_sockets[r as usize].clone();
                std::thread::spawn(move || {
                    // Join only once the hub has told this host to expect it.
                    let ring = memfd(slots as usize * slot_bytes as usize);
                    let mut tries = 0;
                    let rank = loop {
                        match Rank::join(
                            &sock,
                            JoinArgs {
                                group,
                                token,
                                rank: r,
                                ring_fd: ring.as_raw_fd(),
                                slots,
                                slot_bytes,
                            },
                            Some(Duration::from_secs(20)),
                        ) {
                            Ok(k) => break k,
                            Err(e) if tries < 100 => {
                                let _ = e;
                                tries += 1;
                                std::thread::sleep(Duration::from_millis(50));
                            }
                            Err(e) => return Err(format!("join: {e}")),
                        }
                    };
                    run_rank(rank, ring, slots, slot_bytes, arena as usize)
                })
            })
            .collect();
        let forecast = g.start();
        let results: Vec<Result<Vec<u8>, String>> =
            ranks.into_iter().map(|h| h.join().unwrap()).collect();
        let forecast = forecast.map_err(|e| {
            format!(
                "start: {e}; ranks: {:?}",
                results.iter().map(|r| r.as_ref().err()).collect::<Vec<_>>()
            )
        })?;
        let arenas: Vec<Vec<u8>> = results
            .into_iter()
            .enumerate()
            .map(|(i, r)| r.map_err(|e| format!("rank {i}: {e}")))
            .collect::<Result<_, _>>()?;
        // The coordinator hears Done after every rank has everything.
        let mut done = false;
        while let Some(e) = g.next().map_err(|e| e.to_string())? {
            match e {
                Event::Done => {
                    done = true;
                    break;
                }
                Event::Progress { .. } => {}
                other => return Err(format!("coordinator got {other:?}")),
            }
        }
        assert!(done);
        Ok::<_, String>((forecast, arenas))
    })
    .await
    .unwrap();
    let (forecast, arenas) = res.unwrap();
    // The hub kept a record of the load: who sent whom how much, over time.
    let recs = c.node(1).push.history(None);
    let rec = recs.last().expect("a record");
    assert_eq!(rec.outcome, nest_data::push_record::Outcome::Done);
    let full = c.node(1).push.history(Some(rec.group));
    let full = full.iter().find(|r| r.group == rec.group).unwrap();
    let totals = full.totals();
    let mut sources = full.sources.clone();
    sources.sort();
    assert_eq!(sources, vec!["n2", "n3"]);
    assert_eq!(full.ranks, vec!["n1", "n3"]);
    let sum: u64 = totals.iter().flatten().sum();
    assert_eq!(sum, 2 * arena, "{totals:?}");
    for r in 0..2 {
        assert_eq!(totals.iter().map(|row| row[r]).sum::<u64>(), arena);
    }
    assert!(full.source_done_ms.iter().all(|d| d.is_some()));
    assert!(full.rank_done_ms.iter().all(|d| d.is_some()));
    assert_eq!(forecast.rank_bytes, vec![arena, arena]);
    // Each file was streamed by the host holding it.
    let mut stores: Vec<_> = forecast.streams.iter().map(|s| s.store.clone()).collect();
    stores.sort();
    assert_eq!(stores, vec!["n2", "n3"]);
    for (r, got) in arenas.iter().enumerate() {
        let mut want = Vec::new();
        for f in [&a, &b] {
            for rr in 0..rows as usize {
                for u in &owned[r] {
                    let at = rr * row + *u as usize * unit as usize;
                    want.extend_from_slice(&f[at..at + unit as usize]);
                }
            }
        }
        assert_eq!(got.len(), want.len());
        assert!(got == &want, "rank {r}'s arena differs from its share");
    }
    // Every slot to a rank on another host went on a lane (node 3's own
    // slots to its own rank go straight in).
    let sent = messages(&c) - before;
    if lanes {
        assert!(sent > 0, "no slot travelled on a lane");
    } else {
        assert_eq!(sent, 0);
    }
    // Node 3 holds b.bin and rank 1: those slots count as local, not as a
    // fallback.
    let i3 = full.sources.iter().position(|s| s == "n3").unwrap();
    assert!(full.local_slots[i3] > 0, "{:?}", full.local_slots);
    let i2 = full.sources.iter().position(|s| s == "n2").unwrap();
    assert_eq!(full.local_slots[i2], 0, "n2 holds no rank");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_bad_plan_is_refused_before_anything_moves() {
    let c = ready(2).await;
    sealed(&c, 2, "w.bin", vec![1u8; 4096]).await;
    let hub = c.push_socket(1);
    let r = tokio::task::spawn_blocking(move || {
        let g = Coordinator::create(
            &hub,
            vec![RankSpec {
                rank: 0,
                host: "n1".into(),
                arena_bytes: 1024,
            }],
            vec!["/w.bin".into()],
            Some(Duration::from_secs(10)),
        )
        .unwrap();
        g.patterns(vec![Pattern {
            run: 2048,
            dims: vec![],
        }])
        .unwrap();
        // 2 KiB into a 1 KiB arena.
        g.entries(vec![Entry {
            file: 0,
            pattern: 0,
            src_base: 0,
            dst_base: 0,
            rank: 0,
        }])
        .unwrap();
        g.start().map(|_| ()).map_err(|e| e.to_string())
    })
    .await
    .unwrap();
    let e = r.unwrap_err();
    assert!(e.contains("arena"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_unsealed_file_is_refused() {
    let c = ready(1).await;
    let v = &c.node(1).vfs;
    let (_, fh, _) = v
        .create(FileId::ROOT, b"open.bin", 0o644, oflags::WRONLY)
        .await
        .unwrap();
    v.write(fh, 0, vec![1u8; 4096]).await.unwrap();
    v.release(fh, None).await;
    let hub = c.push_socket(1);
    let r = tokio::task::spawn_blocking(move || {
        let g = Coordinator::create(
            &hub,
            vec![RankSpec {
                rank: 0,
                host: "n1".into(),
                arena_bytes: 4096,
            }],
            vec!["/open.bin".into()],
            Some(Duration::from_secs(10)),
        )
        .unwrap();
        g.patterns(vec![Pattern {
            run: 4096,
            dims: vec![],
        }])
        .unwrap();
        g.entries(vec![Entry {
            file: 0,
            pattern: 0,
            src_base: 0,
            dst_base: 0,
            rank: 0,
        }])
        .unwrap();
        g.start().map(|_| ()).map_err(|e| e.to_string())
    })
    .await
    .unwrap();
    assert!(r.unwrap_err().contains("not sealed"));
}

/// One rank, one file; returns (coordinator, group, token).
fn one_rank_group(hub: &str, arena: u64, path: &str) -> (Coordinator, u64, u64) {
    let g = Coordinator::create(
        hub,
        vec![RankSpec {
            rank: 0,
            host: "n1".into(),
            arena_bytes: arena,
        }],
        vec![path.into()],
        Some(Duration::from_secs(20)),
    )
    .unwrap();
    g.patterns(vec![Pattern {
        run: arena as u32,
        dims: vec![],
    }])
    .unwrap();
    g.entries(vec![Entry {
        file: 0,
        pattern: 0,
        src_base: 0,
        dst_base: 0,
        rank: 0,
    }])
    .unwrap();
    let (group, token) = (g.group, g.token);
    (g, group, token)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_rank_that_leaves_fails_the_group() {
    let c = ready(2).await;
    let len = 1usize << 20;
    sealed(&c, 2, "w.bin", vec![5u8; len]).await;
    let (hub, sock) = (c.push_socket(1), c.push_socket(1));
    let r = tokio::task::spawn_blocking(move || {
        let (g, group, token) = one_rank_group(&hub, len as u64, "/w.bin");
        let (slots, slot_bytes) = (2u32, 64u32 << 10);
        let joiner = std::thread::spawn(move || {
            let ring = memfd(slots as usize * slot_bytes as usize);
            let rank = loop {
                if let Ok(k) = Rank::join(
                    &sock,
                    JoinArgs {
                        group,
                        token,
                        rank: 0,
                        ring_fd: ring.as_raw_fd(),
                        slots,
                        slot_bytes,
                    },
                    Some(Duration::from_secs(10)),
                ) {
                    break k;
                }
                std::thread::sleep(Duration::from_millis(20));
            };
            // Take one slot and leave without releasing it.
            assert!(matches!(rank.next().unwrap(), Some(Event::Filled { .. })));
            drop(rank);
        });
        g.start().unwrap();
        joiner.join().unwrap();
        loop {
            match g.next().unwrap() {
                Some(Event::Progress { .. }) => continue,
                other => return other,
            }
        }
    })
    .await
    .unwrap();
    assert!(
        matches!(&r, Some(Event::Failed { reason, .. }) if reason.contains("closed")),
        "{r:?}"
    );
}

/// A rank leaving fails the group on every host: a rank on another host
/// hears at once, not after its own timeout (group 4132356212719644: the
/// hub's own rank left, the hub forgot the group without telling the
/// other hosts, and their ranks and sources ran on for minutes).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_rank_that_leaves_ends_the_group_on_every_host() {
    let c = ready(3).await;
    let len = 1usize << 20;
    sealed(&c, 3, "w.bin", vec![5u8; 2 * len]).await;
    let (hub, s1, s2) = (c.push_socket(1), c.push_socket(1), c.push_socket(2));
    let r = tokio::task::spawn_blocking(move || {
        let g = Coordinator::create(
            &hub,
            (0..2)
                .map(|r| RankSpec {
                    rank: r,
                    host: format!("n{}", r + 1),
                    arena_bytes: len as u64,
                })
                .collect(),
            vec!["/w.bin".into()],
            Some(Duration::from_secs(20)),
        )
        .unwrap();
        g.patterns(vec![Pattern {
            run: len as u32,
            dims: vec![],
        }])
        .unwrap();
        g.entries(
            (0..2)
                .map(|r| Entry {
                    file: 0,
                    pattern: 0,
                    // Rank 0's bytes first: the source streams in file
                    // order, so rank 0 gets a slot before rank 1's ring
                    // (never released) blocks it.
                    src_base: r as u64 * len as u64,
                    dst_base: 0,
                    rank: r,
                })
                .collect(),
        )
        .unwrap();
        let (group, token) = (g.group, g.token);
        let (slots, slot_bytes) = (2u32, 64u32 << 10);
        let join = move |sock: String, rank: u32| {
            std::thread::spawn(move || {
                let ring = memfd(slots as usize * slot_bytes as usize);
                let k = loop {
                    if let Ok(k) = Rank::join(
                        &sock,
                        JoinArgs {
                            group,
                            token,
                            rank,
                            ring_fd: ring.as_raw_fd(),
                            slots,
                            slot_bytes,
                        },
                        Some(Duration::from_secs(10)),
                    ) {
                        break k;
                    }
                    std::thread::sleep(Duration::from_millis(20));
                };
                (k, ring)
            })
        };
        let (j0, j1) = (join(s1, 0), join(s2, 1));
        g.start().unwrap();
        let (leave, _ring0) = j0.join().unwrap();
        let (stay, _ring1) = j1.join().unwrap();
        // Rank 0 (on the hub's host) takes one slot and leaves; rank 1 (on
        // n2) never releases, and the coordinator stays.
        assert!(matches!(leave.next().unwrap(), Some(Event::Filled { .. })));
        drop(leave);
        let t = std::time::Instant::now();
        let ev = loop {
            match stay.next() {
                Ok(Some(Event::Filled { .. })) => continue,
                Ok(other) => break Ok(other),
                Err(e) => break Err(e.to_string()),
            }
        };
        drop(g);
        (ev, t.elapsed())
    })
    .await
    .unwrap();
    assert!(
        matches!(&r.0, Ok(Some(Event::Failed { reason, .. })) if reason.contains("rank 0 closed")),
        "{r:?}"
    );
    assert!(r.1 < Duration::from_secs(5), "{r:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_coordinator_that_leaves_cancels_the_ranks() {
    let c = ready(2).await;
    let len = 1usize << 20;
    sealed(&c, 2, "w.bin", vec![5u8; len]).await;
    let (hub, sock) = (c.push_socket(1), c.push_socket(1));
    let r = tokio::task::spawn_blocking(move || {
        let (g, group, token) = one_rank_group(&hub, len as u64, "/w.bin");
        let (slots, slot_bytes) = (2u32, 64u32 << 10);
        let ring = memfd(slots as usize * slot_bytes as usize);
        let (fd, s2) = (ring.as_raw_fd(), sock.clone());
        let starter = std::thread::spawn(move || {
            g.start().unwrap();
            // The coordinator goes away once loading has begun.
            std::thread::sleep(Duration::from_millis(100));
            drop(g);
        });
        let rank = loop {
            if let Ok(k) = Rank::join(
                &s2,
                JoinArgs {
                    group,
                    token,
                    rank: 0,
                    ring_fd: fd,
                    slots,
                    slot_bytes,
                },
                Some(Duration::from_secs(10)),
            ) {
                break k;
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        starter.join().unwrap();
        // Never release: the load cannot finish; the rank hears it failed.
        loop {
            match rank.next().unwrap() {
                Some(Event::Filled { .. }) => continue,
                other => return other,
            }
        }
    })
    .await
    .unwrap();
    assert!(
        matches!(&r, Some(Event::Failed { reason, .. }) if reason.contains("coordinator")),
        "{r:?}"
    );
}

/// Over the real RDMA fabric (skips without RoCE): every slot travels as a
/// WRITE_WITH_IMM into the rank's registered ring, none by RPC.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn over_rdma_slots_land_by_nic_writes() {
    over_rdma(0).await;
}

/// A write that landed but whose completion was lost (the lane failed
/// first) is sent again by RPC: the duplicate is dropped, the load
/// completes with every byte right.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_landed_slot_sent_again_by_rpc_is_dropped() {
    over_rdma(5).await;
}

async fn over_rdma(lost_acks: u64) {
    if nest_fabric::discover(&[]).is_empty() {
        return;
    }
    let mut t = nest_testkit::fast_tuning();
    t.fabric = Some(nest_fabric::FabricConfig {
        devices: vec![],
        ..nest_fabric::FabricConfig::default()
    });
    let c = TestCluster::start_with(3, t).await;
    c.eventually("serving", Duration::from_secs(8), |c| {
        (1..=3).all(|i| c.node(i).data.caught_up() && c.node(i).data.session().is_some())
    })
    .await;
    c.eventually("names registered", Duration::from_secs(8), |c| {
        c.node(1)
            .placer
            .nodes()
            .map(|n| n.iter().all(|h| !h.name.starts_with("node")))
            .unwrap_or(false)
    })
    .await;
    let mib: usize = std::env::var("NEST_PUSH_MIB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    let len = mib << 20;
    let data: Vec<u8> = (0..len)
        .map(|i| (i.wrapping_mul(2654435761) >> 11) as u8)
        .collect();
    sealed(&c, 2, "big.bin", data.clone()).await;
    c.node(2).push.lose_rdma_acks(lost_acks);
    // Rank 0 on node 3 takes even 4 KiB runs, rank 1 on node 1 odd ones.
    let run = 4096u32;
    let rows = (len / (2 * run as usize)) as u32;
    let pat = Pattern {
        run,
        dims: vec![Dim {
            steps: Steps::Count(rows),
            src_stride: 2 * run as u64,
            dst_stride: run as u64,
        }],
    };
    let share = rows as u64 * run as u64;
    let (slots, slot_bytes) = (32u32, 1u32 << 20);
    let (hub, socks) = (c.push_socket(1), [c.push_socket(3), c.push_socket(1)]);
    let before = c
        .node(2)
        .vfs
        .fabric()
        .unwrap()
        .stats
        .push_writes
        .load(std::sync::atomic::Ordering::Relaxed);
    let t0 = std::time::Instant::now();
    let arenas = tokio::task::spawn_blocking(move || {
        let g = Coordinator::create(
            &hub,
            vec![
                RankSpec {
                    rank: 0,
                    host: "n3".into(),
                    arena_bytes: share,
                },
                RankSpec {
                    rank: 1,
                    host: "n1".into(),
                    arena_bytes: share,
                },
            ],
            vec!["/big.bin".into()],
            Some(Duration::from_secs(30)),
        )
        .unwrap();
        g.patterns(vec![pat]).unwrap();
        g.entries(vec![
            Entry {
                file: 0,
                pattern: 0,
                src_base: 0,
                dst_base: 0,
                rank: 0,
            },
            Entry {
                file: 0,
                pattern: 0,
                src_base: run as u64,
                dst_base: 0,
                rank: 1,
            },
        ])
        .unwrap();
        let (group, token) = (g.group, g.token);
        let ranks: Vec<_> = (0..2u32)
            .map(|r| {
                let sock = socks[r as usize].clone();
                std::thread::spawn(move || {
                    let ring = memfd(slots as usize * slot_bytes as usize);
                    let rank = loop {
                        if let Ok(k) = Rank::join(
                            &sock,
                            JoinArgs {
                                group,
                                token,
                                rank: r,
                                ring_fd: ring.as_raw_fd(),
                                slots,
                                slot_bytes,
                            },
                            Some(Duration::from_secs(30)),
                        ) {
                            break k;
                        }
                        std::thread::sleep(Duration::from_millis(20));
                    };
                    run_rank(rank, ring, slots, slot_bytes, share as usize)
                })
            })
            .collect();
        g.start().unwrap();
        let out: Vec<Vec<u8>> = ranks
            .into_iter()
            .map(|h| h.join().unwrap().expect("rank"))
            .collect();
        loop {
            match g.next().unwrap() {
                Some(Event::Done) => break,
                Some(Event::Progress { .. }) => {}
                other => panic!("coordinator got {other:?}"),
            }
        }
        out
    })
    .await
    .unwrap();
    let dt = t0.elapsed();
    let f2 = c.node(2).vfs.fabric().unwrap();
    let writes = f2
        .stats
        .push_writes
        .load(std::sync::atomic::Ordering::Relaxed)
        - before;
    for (r, got) in arenas.iter().enumerate() {
        let want: Vec<u8> = (0..rows as usize)
            .flat_map(|k| {
                let at = (2 * k + r) * run as usize;
                data[at..at + run as usize].iter().copied()
            })
            .collect();
        assert!(got == &want, "rank {r}");
    }
    // Both ranks are remote from the source (node 2): every slot went by
    // RDMA, none fell back to RPC but those whose ack was lost.
    let rpc = c.node(2).push.stats().rpc_slots;
    assert!(
        writes > 0 && rpc == lost_acks,
        "{writes} RDMA writes, {rpc} by RPC"
    );
    eprintln!(
        "push over RDMA: {} MiB to 2 ranks in {dt:?} ({:.2} GB/s), {writes} NIC writes",
        (2 * share) >> 20,
        (2 * share) as f64 / dt.as_secs_f64() / 1e9
    );
}

/// The engine's order (ADR-063): Create, the plan, every rank's Join
/// returning, and only then Start. Rank hosts expect their ranks from
/// Create on, so a Join before Start is taken; the load completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn ranks_may_join_before_start() {
    let c = ready(3).await;
    let len = 4 << 20;
    let data: Vec<u8> = (0..len).map(|i| (i * 11 % 239) as u8).collect();
    sealed(&c, 2, "early.bin", data.clone()).await;
    let half = (len / 2) as u64;
    let (hub, socks) = (c.push_socket(1), [c.push_socket(3), c.push_socket(1)]);
    let (slots, slot_bytes) = (8u32, 256u32 << 10);
    let arenas = tokio::task::spawn_blocking(move || {
        let t = Some(Duration::from_secs(20));
        let g = Coordinator::create(
            &hub,
            vec![
                RankSpec {
                    rank: 0,
                    host: "n3".into(),
                    arena_bytes: half,
                },
                RankSpec {
                    rank: 1,
                    host: "n1".into(),
                    arena_bytes: half,
                },
            ],
            vec!["/early.bin".into()],
            t,
        )
        .unwrap();
        g.patterns(vec![Pattern {
            run: half as u32,
            dims: vec![],
        }])
        .unwrap();
        g.entries(
            (0..2u32)
                .map(|r| Entry {
                    file: 0,
                    pattern: 0,
                    src_base: r as u64 * half,
                    dst_base: 0,
                    rank: r,
                })
                .collect(),
        )
        .unwrap();
        // Every Join returns before Start is sent: no retrying.
        let joined: Vec<_> = (0..2u32)
            .map(|r| {
                let ring = memfd(slots as usize * slot_bytes as usize);
                let rank = Rank::join(
                    &socks[r as usize],
                    JoinArgs {
                        group: g.group,
                        token: g.token,
                        rank: r,
                        ring_fd: ring.as_raw_fd(),
                        slots,
                        slot_bytes,
                    },
                    t,
                )
                .unwrap_or_else(|e| panic!("rank {r} joining before Start: {e}"));
                (rank, ring)
            })
            .collect();
        let ranks: Vec<_> = joined
            .into_iter()
            .map(|(rank, ring)| {
                std::thread::spawn(move || run_rank(rank, ring, slots, slot_bytes, half as usize))
            })
            .collect();
        g.start().unwrap();
        let out: Vec<Vec<u8>> = ranks
            .into_iter()
            .map(|h| h.join().unwrap().expect("rank"))
            .collect();
        loop {
            match g.next().unwrap() {
                Some(Event::Done) => break,
                Some(Event::Progress { .. }) => {}
                other => panic!("coordinator got {other:?}"),
            }
        }
        out
    })
    .await
    .unwrap();
    assert!(arenas[0] == data[..len / 2]);
    assert!(arenas[1] == data[len / 2..]);
}

/// CuteAFD's first production load: a rank that stops releasing ring
/// slots stalls every source; when the coordinator gives up, the load's
/// reason names that rank, its full ring and when it last released.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_rank_that_stops_releasing_is_named() {
    let c = ready(3).await;
    let len = 8 << 20;
    let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
    sealed(&c, 2, "stall.bin", data).await;
    let (hub, sock) = (c.push_socket(1), c.push_socket(3));
    let (slots, slot_bytes) = (4u32, 256u32 << 10);
    let reason = tokio::task::spawn_blocking(move || {
        let t = Some(Duration::from_secs(20));
        let g = Coordinator::create(
            &hub,
            vec![RankSpec {
                rank: 0,
                host: "n3".into(),
                arena_bytes: len as u64,
            }],
            vec!["/stall.bin".into()],
            t,
        )
        .unwrap();
        g.patterns(vec![Pattern {
            run: len as u32,
            dims: vec![],
        }])
        .unwrap();
        g.entries(vec![Entry {
            file: 0,
            pattern: 0,
            src_base: 0,
            dst_base: 0,
            rank: 0,
        }])
        .unwrap();
        let ring = memfd(slots as usize * slot_bytes as usize);
        let rank = Rank::join(
            &sock,
            JoinArgs {
                group: g.group,
                token: g.token,
                rank: 0,
                ring_fd: ring.as_raw_fd(),
                slots,
                slot_bytes,
            },
            t,
        )
        .unwrap();
        // Releases the first two slots, then stops releasing.
        let stuck = std::thread::spawn(move || {
            let mut released = 0;
            while let Ok(Some(e)) = rank.next() {
                if let Event::Filled { slot, .. } = e
                    && released < 2
                {
                    rank.release(slot).unwrap();
                    released += 1;
                }
            }
        });
        g.start().unwrap();
        // The rank host reports its ring within its sweep (5 s); then the
        // coordinator gives up, as the engine's timeout did.
        std::thread::sleep(Duration::from_secs(12));
        let group = g.group;
        drop(g);
        let _ = stuck.join();
        group
    })
    .await
    .unwrap();
    c.eventually("the load ended", Duration::from_secs(5), |c| {
        c.node(1)
            .push
            .history(None)
            .iter()
            .any(|r| r.group == reason && r.outcome != nest_data::push_record::Outcome::Running)
    })
    .await;
    let rec = c
        .node(1)
        .push
        .history(None)
        .into_iter()
        .find(|r| r.group == reason)
        .unwrap();
    let why = rec.reason.clone().unwrap_or_default();
    assert!(
        why.contains("rank 0 (n3) stopped releasing")
            && why.contains("of 4 slots filled and held")
            && why.contains("last release"),
        "{why}"
    );
    let ring = rec.rings[0].clone().expect("the rank's ring in the record");
    assert_eq!(
        (ring.slots, ring.free, ring.released),
        (4, 0, 2),
        "{ring:?}"
    );
}

/// The production stall (group 4092384301547520): a source with a small
/// share finishes first, yet rank hosts kept granting it ring slots it
/// would never fill, until every slot was held by it and the others
/// starved. A finished source must give its grants back.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_finished_source_gives_back_its_ring_slots() {
    let c = ready(3).await;
    // n2 holds a small file, n3 a large one; one rank on n1 reads both.
    let small: Vec<u8> = (0..(1usize << 20)).map(|i| (i % 241) as u8).collect();
    let large: Vec<u8> = (0..(24usize << 20)).map(|i| (i % 239) as u8).collect();
    sealed(&c, 2, "small.bin", small.clone()).await;
    sealed(&c, 3, "large.bin", large.clone()).await;
    let total = (small.len() + large.len()) as u64;
    let (hub, sock) = (c.push_socket(1), c.push_socket(1));
    // A ring far smaller than the load, as in production (128 of 1 MiB).
    let (slots, slot_bytes) = (8u32, 128u32 << 10);
    let (sl, ll) = (small.len() as u64, large.len() as u64);
    let out = tokio::task::spawn_blocking(move || {
        let t = Some(Duration::from_secs(30));
        let g = Coordinator::create(
            &hub,
            vec![RankSpec {
                rank: 0,
                host: "n1".into(),
                arena_bytes: total,
            }],
            vec!["/small.bin".into(), "/large.bin".into()],
            t,
        )
        .unwrap();
        g.patterns(vec![
            Pattern {
                run: sl as u32,
                dims: vec![],
            },
            Pattern {
                run: ll as u32,
                dims: vec![],
            },
        ])
        .unwrap();
        g.entries(vec![
            Entry {
                file: 0,
                pattern: 0,
                src_base: 0,
                dst_base: 0,
                rank: 0,
            },
            Entry {
                file: 1,
                pattern: 1,
                src_base: 0,
                dst_base: sl,
                rank: 0,
            },
        ])
        .unwrap();
        let ring = memfd(slots as usize * slot_bytes as usize);
        let rank = Rank::join(
            &sock,
            JoinArgs {
                group: g.group,
                token: g.token,
                rank: 0,
                ring_fd: ring.as_raw_fd(),
                slots,
                slot_bytes,
            },
            t,
        )
        .unwrap();
        let h = std::thread::spawn(move || run_rank(rank, ring, slots, slot_bytes, total as usize));
        g.start().unwrap();
        let arena = h.join().unwrap().expect("the load completes");
        loop {
            match g.next().unwrap() {
                Some(Event::Done) => break,
                Some(Event::Progress { .. }) => {}
                other => panic!("coordinator got {other:?}"),
            }
        }
        arena
    })
    .await
    .unwrap();
    assert!(out[..small.len()] == small[..]);
    assert!(out[small.len()..] == large[..]);
}

/// One group for the whole load (ADR-063 amendment): two ranks on the
/// hub's host, which is also a source, beside ranks on other hosts. Each
/// rank gets exactly its share.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_ranks_on_the_hub_which_is_also_a_source() {
    let c = ready(3).await;
    let (rows, units, unit) = (64u32, 4u32, 1024u32);
    let row = (units * unit) as usize;
    let len = rows as usize * row;
    let a: Vec<u8> = (0..len).map(|i| (i * 7 % 251) as u8).collect();
    let b: Vec<u8> = (0..len).map(|i| (i * 13 % 241) as u8).collect();
    sealed(&c, 1, "a.bin", a.clone()).await;
    sealed(&c, 2, "b.bin", b.clone()).await;

    // Rank r takes unit r of every row of both files; ranks 0 and 1 are on
    // node 1 (the hub, holding a.bin), rank 2 on node 2, rank 3 on node 3.
    let hosts = [1u64, 1, 2, 3];
    let share = rows as u64 * unit as u64;
    let patterns: Vec<Pattern> = (0..units)
        .map(|_| Pattern {
            run: unit,
            dims: vec![Dim {
                steps: Steps::Count(rows),
                src_stride: row as u64,
                dst_stride: unit as u64,
            }],
        })
        .collect();
    let mut entries = Vec::new();
    for f in 0..2u32 {
        for r in 0..units {
            entries.push(Entry {
                file: f,
                pattern: r,
                src_base: (r * unit) as u64,
                dst_base: f as u64 * share,
                rank: r,
            });
        }
    }
    let arena = 2 * share;
    let (slots, slot_bytes) = (4u32, 16u32 << 10);
    let hub = c.push_socket(1);
    let sockets: Vec<_> = hosts.iter().map(|h| c.push_socket(*h)).collect();

    let arenas = tokio::task::spawn_blocking(move || {
        let t = Some(Duration::from_secs(20));
        let specs = hosts
            .iter()
            .enumerate()
            .map(|(r, h)| RankSpec {
                rank: r as u32,
                host: format!("n{h}"),
                arena_bytes: arena,
            })
            .collect();
        let g = Coordinator::create(&hub, specs, vec!["/a.bin".into(), "/b.bin".into()], t)
            .map_err(|e| format!("create: {e}"))?;
        g.patterns(patterns).unwrap();
        g.entries(entries).unwrap();
        let (group, token) = (g.group, g.token);
        // Ranks join before Start, as the engine does.
        let joined: Vec<_> = (0..units)
            .map(|r| {
                let ring = memfd(slots as usize * slot_bytes as usize);
                let k = Rank::join(
                    &sockets[r as usize],
                    JoinArgs {
                        group,
                        token,
                        rank: r,
                        ring_fd: ring.as_raw_fd(),
                        slots,
                        slot_bytes,
                    },
                    t,
                )
                .map_err(|e| format!("join {r}: {e}"))?;
                Ok((k, ring))
            })
            .collect::<Result<_, String>>()?;
        let ranks: Vec<_> = joined
            .into_iter()
            .map(|(k, ring)| {
                std::thread::spawn(move || run_rank(k, ring, slots, slot_bytes, arena as usize))
            })
            .collect();
        g.start().map_err(|e| format!("start: {e}"))?;
        let arenas = ranks
            .into_iter()
            .enumerate()
            .map(|(i, h)| h.join().unwrap().map_err(|e| format!("rank {i}: {e}")))
            .collect::<Result<Vec<_>, _>>()?;
        loop {
            match g.next().map_err(|e| e.to_string())? {
                Some(Event::Done) => break,
                Some(Event::Progress { .. }) => {}
                other => return Err(format!("coordinator got {other:?}")),
            }
        }
        Ok::<_, String>(arenas)
    })
    .await
    .unwrap()
    .unwrap();
    for (r, got) in arenas.iter().enumerate() {
        let mut want = Vec::new();
        for f in [&a, &b] {
            for rr in 0..rows as usize {
                let at = rr * row + r * unit as usize;
                want.extend_from_slice(&f[at..at + unit as usize]);
            }
        }
        assert!(got == &want, "rank {r}'s arena differs from its share");
    }
    let rec = c.node(1).push.history(None);
    let rec = rec.last().unwrap();
    let full = c.node(1).push.history(Some(rec.group));
    let full = full.iter().find(|x| x.group == rec.group).unwrap();
    assert_eq!(full.ranks, vec!["n1", "n1", "n2", "n3"]);
    let i1 = full.sources.iter().position(|s| s == "n1").unwrap();
    assert!(full.local_slots[i1] > 0, "{:?}", full.local_slots);
}

/// Two ranks on one host must not deadlock on that host's incast budget
/// (group 120235912134667). Each file's first half is rank 0's and its
/// second half rank 1's, both on node 1; sources stream in file order, so
/// every source fills rank 0's grants first and then blocks on rank 1.
/// With a budget of a few slots, rank 0's grants held at sources blocked
/// on rank 1 used it all, and rank 1 never got a grant. Each owing source's
/// floor grant, outside the budget, keeps the load moving.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_ranks_on_one_host_share_its_budget_without_deadlock() {
    let c = ready(3).await;
    let half = 8usize << 20;
    let files: Vec<Vec<u8>> = (0..4)
        .map(|f| {
            (0..2 * half)
                .map(|i| ((i * 7 + f * 31) % 251) as u8)
                .collect()
        })
        .collect();
    for (f, data) in files.iter().enumerate() {
        sealed(&c, 2 + (f as u64 % 2), &format!("f{f}.bin"), data.clone()).await;
    }
    // Three slots of budget on node 1, for two ranks fed by two sources,
    // through the cluster setting (read at Join).
    let slot_bytes = 1u32 << 20;
    c.node(1)
        .meta
        .propose(nest_meta::Command::SetSetting {
            name: nest_types::Setting::PushIncastMib.name().into(),
            value: Some(3),
        })
        .await
        .unwrap();
    c.converge().await;
    c.eventually("the budget applies", Duration::from_secs(5), |c| {
        c.node(1).vfs.setting(nest_types::Setting::PushIncastMib) == 3
    })
    .await;
    let patterns = vec![Pattern {
        run: half as u32,
        dims: vec![],
    }];
    let mut entries = Vec::new();
    for f in 0..4u32 {
        for r in 0..2u32 {
            entries.push(Entry {
                file: f,
                pattern: 0,
                src_base: r as u64 * half as u64,
                dst_base: f as u64 * half as u64,
                rank: r,
            });
        }
    }
    let arena = 4 * half as u64;
    let slots = 32u32;
    let hub = c.push_socket(1);
    let sock = c.push_socket(1);
    let arenas = tokio::task::spawn_blocking(move || {
        let t = Some(Duration::from_secs(20));
        let specs = (0..2u32)
            .map(|r| RankSpec {
                rank: r,
                host: "n1".into(),
                arena_bytes: arena,
            })
            .collect();
        let names = (0..4).map(|f| format!("/f{f}.bin")).collect();
        let g = Coordinator::create(&hub, specs, names, t).map_err(|e| format!("create: {e}"))?;
        g.patterns(patterns).unwrap();
        g.entries(entries).unwrap();
        let (group, token) = (g.group, g.token);
        let ranks: Vec<_> = (0..2u32)
            .map(|r| {
                let ring = memfd(slots as usize * slot_bytes as usize);
                let k = Rank::join(
                    &sock,
                    JoinArgs {
                        group,
                        token,
                        rank: r,
                        ring_fd: ring.as_raw_fd(),
                        slots,
                        slot_bytes,
                    },
                    t,
                )
                .map_err(|e| format!("join {r}: {e}"))?;
                Ok(std::thread::spawn(move || {
                    run_rank(k, ring, slots, slot_bytes, arena as usize)
                }))
            })
            .collect::<Result<_, String>>()?;
        g.start().map_err(|e| format!("start: {e}"))?;
        let arenas = ranks
            .into_iter()
            .enumerate()
            .map(|(i, h): (usize, std::thread::JoinHandle<_>)| {
                h.join().unwrap().map_err(|e| format!("rank {i}: {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok::<_, String>(arenas)
    })
    .await
    .unwrap()
    .unwrap();
    for (r, got) in arenas.iter().enumerate() {
        for (f, data) in files.iter().enumerate() {
            let want = &data[r * half..(r + 1) * half];
            assert!(&got[f * half..(f + 1) * half] == want, "rank {r}, file {f}");
        }
    }
}

/// One rank per host can deadlock too (group 120235912134669). Grants of a
/// rank go round robin to every source owing it, needed yet or not. Node
/// 1's file opens with a long stretch for rank 1 (the coordinator's dense
/// tensors) and nodes 2 and 3 open with stretches for rank 0 (experts):
/// each grant of rank 0 handed to node 1 stays there unfilled, so within a
/// few cycles all of rank 0's budget is parked at node 1, and all of rank
/// 1's at nodes 2 and 3. Each owing source's floor grant, outside the
/// budget, keeps every source moving.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn grants_parked_at_busy_sources_do_not_deadlock_one_rank_per_host() {
    let c = ready(3).await;
    let stretch = 16usize << 20;
    let files: Vec<Vec<u8>> = (0..3)
        .map(|f| {
            (0..2 * stretch)
                .map(|i| ((i * 13 + f * 17) % 251) as u8)
                .collect()
        })
        .collect();
    for (f, data) in files.iter().enumerate() {
        sealed(&c, 1 + f as u64, &format!("p{f}.bin"), data.clone()).await;
    }
    c.node(1)
        .meta
        .propose(nest_meta::Command::SetSetting {
            name: nest_types::Setting::PushIncastMib.name().into(),
            value: Some(3),
        })
        .await
        .unwrap();
    c.converge().await;
    c.eventually("the budget applies", Duration::from_secs(5), |c| {
        (1..=3).all(|i| c.node(i).vfs.setting(nest_types::Setting::PushIncastMib) == 3)
    })
    .await;
    let patterns = vec![Pattern {
        run: stretch as u32,
        dims: vec![],
    }];
    // File 0 (node 1): rank 1 first, then rank 0. Files 1 and 2: rank 0
    // first, then rank 1. Arena: file f's stretch for a rank at f·stretch.
    let mut entries = Vec::new();
    for f in 0..3u32 {
        for r in 0..2u32 {
            let first = if f == 0 { 1 } else { 0 };
            let half = if r == first { 0 } else { 1 };
            entries.push(Entry {
                file: f,
                pattern: 0,
                src_base: half * stretch as u64,
                dst_base: f as u64 * stretch as u64,
                rank: r,
            });
        }
    }
    let expect = entries.clone();
    let arena = 3 * stretch as u64;
    let (slots, slot_bytes) = (32u32, 1u32 << 20);
    let hub = c.push_socket(1);
    let socks = [c.push_socket(2), c.push_socket(3)];
    let got = tokio::task::spawn_blocking(move || {
        let t = Some(Duration::from_secs(20));
        let specs = (0..2u32)
            .map(|r| RankSpec {
                rank: r,
                host: format!("n{}", r + 2),
                arena_bytes: arena,
            })
            .collect();
        let names = (0..3).map(|f| format!("/p{f}.bin")).collect();
        let g = Coordinator::create(&hub, specs, names, t).map_err(|e| format!("create: {e}"))?;
        g.patterns(patterns).unwrap();
        g.entries(entries).unwrap();
        let (group, token) = (g.group, g.token);
        let ranks: Vec<_> = (0..2u32)
            .map(|r| {
                let ring = memfd(slots as usize * slot_bytes as usize);
                let k = Rank::join(
                    &socks[r as usize],
                    JoinArgs {
                        group,
                        token,
                        rank: r,
                        ring_fd: ring.as_raw_fd(),
                        slots,
                        slot_bytes,
                    },
                    t,
                )
                .map_err(|e| format!("join {r}: {e}"))?;
                Ok(std::thread::spawn(move || {
                    run_rank(k, ring, slots, slot_bytes, arena as usize)
                }))
            })
            .collect::<Result<_, String>>()?;
        g.start().map_err(|e| format!("start: {e}"))?;
        ranks
            .into_iter()
            .enumerate()
            .map(|(i, h): (usize, std::thread::JoinHandle<_>)| {
                h.join().unwrap().map_err(|e| format!("rank {i}: {e}"))
            })
            .collect::<Result<Vec<_>, _>>()
    })
    .await
    .unwrap()
    .unwrap();
    for e in &expect {
        let (s, d) = (e.src_base as usize, e.dst_base as usize);
        assert!(
            got[e.rank as usize][d..d + stretch] == files[e.file as usize][s..s + stretch],
            "rank {}, file {}",
            e.rank,
            e.file
        );
    }
}

/// An archive is a source of its own (ADR-063): a file only in an archive
/// streams from its gateway beside that host's own store, as another
/// disk of the same host, and the record names the archive.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_archive_only_file_streams_from_its_gateway() {
    let c = ready(2).await;
    let p = c.node(1).placer.clone();
    let scratch = c.state_dir(1).join("scratch-disk");
    std::fs::create_dir_all(&scratch).unwrap();
    let r = p
        .add_store("scratch", scratch.to_str().unwrap(), &["n1".into()])
        .await
        .unwrap();
    assert!(r.iter().all(|(_, r)| r.is_ok()), "{r:?}");
    let len = 3usize << 20;
    let a: Vec<u8> = (0..len).map(|i| (i * 7 % 251) as u8).collect();
    let b: Vec<u8> = (0..len).map(|i| (i * 13 % 241) as u8).collect();
    sealed(&c, 1, "live.bin", a.clone()).await;
    let fb = sealed(&c, 2, "archived.bin", b.clone()).await;
    let sel = nest_place::Selector::parse("/archived.bin", "/hub").unwrap();
    let job = p.offload(sel, "scratch".into(), 2).await.unwrap();
    c.eventually("offloaded", Duration::from_secs(20), |c| {
        c.node(1).placer.job(job).is_some_and(|j| j.finished)
    })
    .await;
    assert!(p.job(job).unwrap().error.is_none());
    c.converge().await;
    let only_archive = |c: &TestCluster| {
        let r = c.node(1).meta.open_reader().unwrap();
        nest_meta::query::replicas(&r, fb)
            .unwrap()
            .iter()
            .all(|r| r.store.0 >= 1 << 20)
    };
    c.eventually(
        "only the archive holds it",
        Duration::from_secs(5),
        only_archive,
    )
    .await;

    let arena = 2 * len as u64;
    let (hub, sock) = (c.push_socket(1), c.push_socket(2));
    let got = tokio::task::spawn_blocking(move || {
        let g = Coordinator::create(
            &hub,
            vec![RankSpec {
                rank: 0,
                host: "n2".into(),
                arena_bytes: arena,
            }],
            vec!["/live.bin".into(), "/archived.bin".into()],
            Some(Duration::from_secs(20)),
        )
        .map_err(|e| format!("create: {e}"))?;
        g.patterns(vec![Pattern {
            run: len as u32,
            dims: vec![],
        }])
        .unwrap();
        g.entries(
            (0..2u32)
                .map(|f| Entry {
                    file: f,
                    pattern: 0,
                    src_base: 0,
                    dst_base: f as u64 * len as u64,
                    rank: 0,
                })
                .collect(),
        )
        .unwrap();
        let (group, token) = (g.group, g.token);
        let (slots, slot_bytes) = (4u32, 64u32 << 10);
        let joiner = std::thread::spawn(move || {
            let ring = memfd(slots as usize * slot_bytes as usize);
            let rank = loop {
                if let Ok(k) = Rank::join(
                    &sock,
                    JoinArgs {
                        group,
                        token,
                        rank: 0,
                        ring_fd: ring.as_raw_fd(),
                        slots,
                        slot_bytes,
                    },
                    Some(Duration::from_secs(20)),
                ) {
                    break k;
                }
                std::thread::sleep(Duration::from_millis(20));
            };
            run_rank(rank, ring, slots, slot_bytes, arena as usize)
        });
        let forecast = g.start().map_err(|e| format!("start: {e}"))?;
        let out = joiner.join().unwrap()?;
        Ok::<_, String>((forecast, out))
    })
    .await
    .unwrap();
    let (forecast, out) = got.unwrap();
    let mut stores: Vec<_> = forecast.streams.iter().map(|s| s.store.clone()).collect();
    stores.sort();
    assert_eq!(stores, vec!["n1", "scratch"]);
    assert!(out[..len] == a[..], "the live file's bytes");
    assert!(out[len..] == b[..], "the archived file's bytes");
    let rec = c.node(1).push.history(None);
    let rec = rec.last().unwrap();
    let mut sources = rec.sources.clone();
    sources.sort();
    assert_eq!(sources, vec!["n1", "scratch"]);
}

/// An off-grid host (ADR-047) is a source when nothing nearer holds a
/// file: its slots travel over the connections it opened, and a fabric
/// host's copy of another file is still read from the fabric host.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn an_offgrid_host_sources_what_only_it_holds() {
    let mut c = ready(2).await;
    c.add_offgrid(3).await;
    c.eventually("off-grid host serving", Duration::from_secs(8), |c| {
        c.node(3).data.caught_up() && c.node(3).data.session().is_some()
    })
    .await;
    c.eventually("off-grid host named", Duration::from_secs(8), |c| {
        c.node(1)
            .placer
            .nodes()
            .is_ok_and(|v| v.iter().any(|h| h.name == "n3"))
    })
    .await;
    let len = 2usize << 20;
    let a: Vec<u8> = (0..len).map(|i| (i * 7 % 251) as u8).collect();
    let b: Vec<u8> = (0..len).map(|i| (i * 13 % 241) as u8).collect();
    sealed(&c, 1, "here.bin", a.clone()).await;
    sealed(&c, 3, "away.bin", b.clone()).await;
    c.converge().await;

    let arena = 2 * len as u64;
    let (hub, sock) = (c.push_socket(1), c.push_socket(2));
    let got = tokio::task::spawn_blocking(move || {
        let g = Coordinator::create(
            &hub,
            vec![RankSpec {
                rank: 0,
                host: "n2".into(),
                arena_bytes: arena,
            }],
            vec!["/here.bin".into(), "/away.bin".into()],
            Some(Duration::from_secs(20)),
        )
        .map_err(|e| format!("create: {e}"))?;
        g.patterns(vec![Pattern {
            run: len as u32,
            dims: vec![],
        }])
        .unwrap();
        g.entries(
            (0..2u32)
                .map(|f| Entry {
                    file: f,
                    pattern: 0,
                    src_base: 0,
                    dst_base: f as u64 * len as u64,
                    rank: 0,
                })
                .collect(),
        )
        .unwrap();
        let (group, token) = (g.group, g.token);
        let (slots, slot_bytes) = (4u32, 64u32 << 10);
        let joiner = std::thread::spawn(move || {
            let ring = memfd(slots as usize * slot_bytes as usize);
            let rank = loop {
                if let Ok(k) = Rank::join(
                    &sock,
                    JoinArgs {
                        group,
                        token,
                        rank: 0,
                        ring_fd: ring.as_raw_fd(),
                        slots,
                        slot_bytes,
                    },
                    Some(Duration::from_secs(20)),
                ) {
                    break k;
                }
                std::thread::sleep(Duration::from_millis(20));
            };
            run_rank(rank, ring, slots, slot_bytes, arena as usize)
        });
        let forecast = g.start().map_err(|e| format!("start: {e}"))?;
        let out = joiner.join().unwrap()?;
        Ok::<_, String>((forecast, out))
    })
    .await
    .unwrap();
    let (forecast, out) = got.unwrap();
    let mut stores: Vec<_> = forecast.streams.iter().map(|s| s.store.clone()).collect();
    stores.sort();
    assert_eq!(stores, vec!["n1", "n3"]);
    assert!(out[..len] == a[..], "the fabric host's file");
    assert!(out[len..] == b[..], "the off-grid host's file");
}

/// A source's last RDMA writes may land after the hub has told the rank
/// host that source finished (its send completions come back before the
/// rank host's completion thread has handed the landings on). Those grants
/// are still owed: they are filled, not taken back (GLM Flash on
/// 2026-11-03: "a slot without a credit", and the load stalled).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn landings_after_their_source_finished_still_count() {
    if nest_fabric::discover(&[]).is_empty() {
        return;
    }
    let mut t = nest_testkit::fast_tuning();
    t.fabric = Some(nest_fabric::FabricConfig {
        devices: vec![],
        ..nest_fabric::FabricConfig::default()
    });
    let c = TestCluster::start_with(3, t).await;
    c.eventually("serving", Duration::from_secs(8), |c| {
        (1..=3).all(|i| c.node(i).data.caught_up() && c.node(i).data.session().is_some())
    })
    .await;
    c.eventually("names registered", Duration::from_secs(8), |c| {
        c.node(1)
            .placer
            .nodes()
            .map(|n| n.iter().all(|h| !h.name.starts_with("node")))
            .unwrap_or(false)
    })
    .await;
    // n2 holds a small file and finishes first; n3 a large one, still
    // streaming when n2's last landings are handed on.
    let small: Vec<u8> = (0..(4usize << 20)).map(|i| (i % 241) as u8).collect();
    let large: Vec<u8> = (0..(48usize << 20)).map(|i| (i % 239) as u8).collect();
    sealed(&c, 2, "small.bin", small.clone()).await;
    sealed(&c, 3, "large.bin", large.clone()).await;
    // The rank on n1, the hub: its landings wait, so the hub's word that
    // n2 finished always comes first.
    c.node(1).push.delay_landings(Duration::from_millis(50));
    let total = (small.len() + large.len()) as u64;
    let (hub, sock) = (c.push_socket(1), c.push_socket(1));
    let (slots, slot_bytes) = (32u32, 128u32 << 10);
    let (sl, ll) = (small.len() as u64, large.len() as u64);
    let out = tokio::task::spawn_blocking(move || {
        let t = Some(Duration::from_secs(30));
        let g = Coordinator::create(
            &hub,
            vec![RankSpec {
                rank: 0,
                host: "n1".into(),
                arena_bytes: total,
            }],
            vec!["/small.bin".into(), "/large.bin".into()],
            t,
        )
        .unwrap();
        g.patterns(vec![
            Pattern {
                run: sl as u32,
                dims: vec![],
            },
            Pattern {
                run: ll as u32,
                dims: vec![],
            },
        ])
        .unwrap();
        g.entries(vec![
            Entry {
                file: 0,
                pattern: 0,
                src_base: 0,
                dst_base: 0,
                rank: 0,
            },
            Entry {
                file: 1,
                pattern: 1,
                src_base: 0,
                dst_base: sl,
                rank: 0,
            },
        ])
        .unwrap();
        let ring = memfd(slots as usize * slot_bytes as usize);
        let rank = Rank::join(
            &sock,
            JoinArgs {
                group: g.group,
                token: g.token,
                rank: 0,
                ring_fd: ring.as_raw_fd(),
                slots,
                slot_bytes,
            },
            t,
        )
        .unwrap();
        let h = std::thread::spawn(move || run_rank(rank, ring, slots, slot_bytes, total as usize));
        g.start().unwrap();
        let arena = h.join().unwrap().expect("the load completes");
        loop {
            match g.next().unwrap() {
                Some(Event::Done) => break,
                Some(Event::Progress { .. }) => {}
                other => panic!("coordinator got {other:?}"),
            }
        }
        arena
    })
    .await
    .unwrap();
    assert!(out[..small.len()] == small[..]);
    assert!(out[small.len()..] == large[..]);
    // Every slot went by RDMA: none fell back to RPC.
    assert_eq!(
        c.node(2).push.stats().rpc_slots + c.node(3).push.stats().rpc_slots,
        0
    );
}
