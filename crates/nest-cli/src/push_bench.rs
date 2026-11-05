//! `nest push bench` and `nest push rank`: play the engine's part of a push
//! load (ADR-063), to measure it without the engine.

use anyhow::{Context, Result, bail};
use nest_push::client::{Coordinator, JoinArgs, Poll, Rank};
use nest_push::plan::{Dim, Entry, Pattern, Steps};
use nest_push::proto::{Event, RankSpec};
use nest_push::slot;
use serde_json::{Value, json};
use std::os::fd::{AsRawFd, OwnedFd};
use std::time::{Duration, Instant};

pub struct Bench {
    pub socket: String,
    pub files: Vec<String>,
    pub sizes: Vec<u64>,
    pub hosts: Vec<String>,
    pub run: u32,
    pub local_ranks: bool,
    pub slots: u32,
    pub slot_bytes: u32,
}

/// Rank r's share of a file of `size`: run `r` of every N, packed. Bytes
/// past the last whole N·run are left out.
fn share(size: u64, n: u64, run: u64) -> u64 {
    size / (n * run) * run
}

impl Bench {
    pub fn run(self) -> Result<Value> {
        let n = self.hosts.len() as u64;
        let run = self.run as u64;
        // Per rank: every file's share, one after another.
        let mut arena = 0u64;
        let mut entries = Vec::new();
        let mut patterns = Vec::new();
        for (i, &size) in self.sizes.iter().enumerate() {
            let rows = size / (n * run);
            if rows == 0 {
                continue;
            }
            patterns.push(Pattern {
                run: self.run,
                dims: vec![Dim {
                    steps: Steps::Count(rows as u32),
                    src_stride: n * run,
                    dst_stride: run,
                }],
            });
            let p = patterns.len() as u32 - 1;
            for r in 0..n {
                entries.push(Entry {
                    file: i as u32,
                    pattern: p,
                    src_base: r * run,
                    dst_base: arena,
                    rank: r as u32,
                });
            }
            arena += share(size, n, run);
        }
        if entries.is_empty() {
            bail!("the files are smaller than one stride");
        }
        let ranks: Vec<RankSpec> = self
            .hosts
            .iter()
            .enumerate()
            .map(|(r, h)| RankSpec {
                rank: r as u32,
                host: h.clone(),
                arena_bytes: arena,
            })
            .collect();
        let t0 = Instant::now();
        let g = Coordinator::create(
            &self.socket,
            ranks,
            self.files.clone(),
            Some(Duration::from_secs(60)),
        )
        .with_context(|| format!("creating a push group on {}", self.socket))?;
        g.patterns(patterns)?;
        g.entries(entries)?;
        let (group, token) = (g.group, g.token);
        let mut local = Vec::new();
        for r in 0..n as u32 {
            if self.local_ranks {
                let (sock, slots, sb) = (self.socket.clone(), self.slots, self.slot_bytes);
                local.push(std::thread::spawn(move || {
                    consume(&sock, group, token, r, arena, slots, sb)
                }));
            } else {
                eprintln!(
                    "on {}: nest push rank --group {group} --token {token} --rank {r} --arena {arena} --slots {} --slot-bytes {}",
                    self.hosts[r as usize], self.slots, self.slot_bytes
                );
            }
        }
        let forecast = g.start().context("starting the load")?;
        let t_start = t0.elapsed();
        let mut done = false;
        while let Some(e) = g.next()? {
            match e {
                Event::Progress { .. } => {}
                Event::Done => {
                    done = true;
                    break;
                }
                Event::Failed { reason, .. } => bail!("the load failed: {reason}"),
                other => bail!("unexpected {other:?}"),
            }
        }
        if !done {
            bail!("the hub closed the socket");
        }
        let dt = t0.elapsed();
        for h in local {
            h.join().map_err(|_| anyhow::anyhow!("a rank panicked"))??;
        }
        let total = arena * n;
        Ok(json!({
            "group": group,
            "ranks": n,
            "bytes_per_rank": arena,
            "bytes": total,
            "seconds": dt.as_secs_f64(),
            "until_started_s": t_start.as_secs_f64(),
            "gb_per_s": total as f64 / dt.as_secs_f64() / 1e9,
            "per_rank_gb_per_s": arena as f64 / dt.as_secs_f64() / 1e9,
            "streams": forecast.streams.iter().map(|s| json!({
                "store": s.store, "bytes": s.bytes, "files": s.files, "transport": s.transport,
            })).collect::<Vec<_>>(),
        }))
    }
}

fn memfd(len: usize) -> Result<OwnedFd> {
    let fd =
        nix::sys::memfd::memfd_create(c"nest-push-bench", nix::sys::memfd::MFdFlags::MFD_CLOEXEC)?;
    nix::unistd::ftruncate(&fd, len as i64)?;
    Ok(fd)
}

/// One rank: join (retrying until the hub has told this host to expect it),
/// copy every filled slot's fragments into a heap arena, release it, until
/// Done. Returns what it received.
pub fn consume(
    socket: &str,
    group: u64,
    token: u64,
    rank: u32,
    arena: u64,
    slots: u32,
    slot_bytes: u32,
) -> Result<Value> {
    use std::os::unix::fs::FileExt;
    let ring_fd = memfd(slots as usize * slot_bytes as usize)?;
    let ring = std::fs::File::from(ring_fd.try_clone()?);
    let deadline = Instant::now() + Duration::from_secs(60);
    let r = loop {
        match Rank::join(
            socket,
            JoinArgs {
                group,
                token,
                rank,
                ring_fd: ring_fd.as_raw_fd(),
                slots,
                slot_bytes,
            },
            Some(Duration::from_secs(60)),
        ) {
            Ok(r) => break r,
            Err(e) if Instant::now() < deadline => {
                let _ = e;
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => return Err(e).context("joining the group"),
        }
    };
    let mut out = vec![0u8; arena as usize];
    let mut buf = vec![0u8; slot_bytes as usize];
    let (mut got, mut filled) = (0u64, 0u64);
    let t0 = Instant::now();
    loop {
        let e = match r.try_next()? {
            Poll::Ready(e) => e,
            Poll::Empty => match r.next()? {
                Some(e) => e,
                None => bail!("sparknestd closed the socket"),
            },
            Poll::Closed => bail!("sparknestd closed the socket"),
        };
        match e {
            Event::Filled { slot: i, len } => {
                ring.read_exact_at(&mut buf[..len as usize], i as u64 * slot_bytes as u64)?;
                let p = slot::parse(&buf[..len as usize], arena)?;
                let mut at = 0usize;
                for (dst, n) in slot::Fragments::new(&buf[..len as usize], p) {
                    out[dst as usize..dst as usize + n as usize]
                        .copy_from_slice(&buf[at..at + n as usize]);
                    at += n as usize;
                }
                got += p.payload as u64;
                filled += 1;
                r.release(i)?;
            }
            Event::Done => break,
            Event::Failed { reason, .. } => bail!("rank {rank}: {reason}"),
            // The group's progress (ADR-087): it keeps the idle deadline off.
            Event::Progress { .. } => {}
            other => bail!("rank {rank}: unexpected {other:?}"),
        }
    }
    let dt = t0.elapsed();
    let v = json!({
        "rank": rank,
        "bytes": got,
        "slots": filled,
        "seconds": dt.as_secs_f64(),
        "gb_per_s": got as f64 / dt.as_secs_f64() / 1e9,
    });
    eprintln!("{v}");
    Ok(v)
}
