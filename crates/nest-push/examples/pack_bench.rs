//! How fast one push source packs a plan, CPU only: the source's loop
//! (sparknestd `push::Stream::file`) replayed against synthetic bytes.
//!
//! `cargo run --release -p nest-push --example pack_bench -- PLAN.json [GB] [SOURCES] [MODE]`
//!
//! PLAN.json is `{plan, file_sizes, arenas}` (an engine's checked plan).
//! Files go to SOURCES sources by size, as the hub does without disk rates;
//! source 0's files are packed until GB gigabytes have been read. No disk,
//! no network: what is left is walking runs, copying into slots, and the
//! copy into RDMA staging.
//!
//! MODE `0.6.2` replays the loop as 0.6.2 shipped it, for comparison: every
//! read walks all of the file's entries, and the read buffer is copied out
//! of the direct read's aligned buffer first. MODE `0.6.3` packs each slot
//! into a fresh zeroed buffer and copies it into RDMA staging, as 0.6.3
//! does. The default is the loop as it is now ([`FileEntries`], bytes
//! packed where the read left them, straight into reused staging slots).

use nest_push::plan::{Entry, Extent, FileEntries, Pattern, Plan, RunWalk};
use nest_push::slot::Packer;
use serde::Deserialize;
use std::time::{Duration, Instant};

const READ_CHUNK: u64 = 16 << 20;
const GAP: u64 = 1 << 20;
const SLOT: usize = 1 << 20;

#[derive(Deserialize)]
struct Checked {
    plan: Plan,
    file_sizes: Vec<u64>,
    arenas: Vec<u64>,
}

#[derive(Default)]
struct Times {
    pack: Duration,
    read_copy: Duration,
    staging: Duration,
    runs: u64,
    walks: u64,
    slots: u64,
    bytes: u64,
    /// Bytes copied into slots: what ranks get of `bytes` read.
    payload: u64,
}

struct Packing {
    open: Vec<Option<Packer>>,
    staging: Vec<u8>,
    /// Packing in place: staging slots, reused (they hold old bytes).
    in_place: bool,
    free: Vec<Vec<u8>>,
}

impl Packing {
    fn put(&mut self, t: &mut Times, r: usize, mut dst: u64, mut data: &[u8]) {
        while !data.is_empty() {
            if self.open[r].is_none() {
                let buf = if self.in_place {
                    self.free.pop().unwrap_or_else(|| vec![0xAAu8; SLOT])
                } else {
                    vec![0u8; SLOT]
                };
                self.open[r] = Some(Packer::new(buf));
            }
            let took = self.open[r].as_mut().unwrap().push(dst, data);
            if took == 0 {
                let (bytes, _, _) = self.open[r].take().unwrap().finish();
                if self.in_place {
                    // The NIC sends it from here; back to the pool after.
                    std::hint::black_box(&bytes);
                    self.free.push(bytes);
                } else {
                    // The copy into registered RDMA staging.
                    let t1 = Instant::now();
                    self.staging.copy_from_slice(&bytes);
                    std::hint::black_box(&self.staging);
                    t.staging += t1.elapsed();
                }
                t.slots += 1;
                continue;
            }
            data = &data[took..];
            dst += took as u64;
        }
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let path = a
        .get(1)
        .expect("usage: pack_bench PLAN.json [GB] [SOURCES] [0.6.2|0.6.3]");
    let limit = a.get(2).map_or(20.0, |s| s.parse::<f64>().unwrap()) * 1e9;
    let sources: usize = a.get(3).map_or(6, |s| s.parse().unwrap());
    let mode = a.get(4).map_or("current", |s| s.as_str());
    let old = mode == "0.6.2";
    let c: Checked = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let Plan {
        files,
        patterns,
        entries,
    } = c.plan;
    println!(
        "{} files, {} patterns, {} entries, {} ranks; loop: {}",
        files.len(),
        patterns.len(),
        entries.len(),
        c.arenas.len(),
        mode
    );
    let mut per_file: Vec<Vec<Entry>> = vec![Vec::new(); files.len()];
    for e in entries {
        per_file[e.file as usize].push(e);
    }
    // Biggest file first to the least loaded source.
    let mut order: Vec<usize> = (0..files.len())
        .filter(|i| !per_file[*i].is_empty())
        .collect();
    order.sort_by_key(|i| std::cmp::Reverse(c.file_sizes[*i]));
    let mut load = vec![0u64; sources];
    let mut mine = Vec::new();
    for i in order {
        let s = (0..sources).min_by_key(|s| load[*s]).unwrap();
        load[s] += c.file_sizes[i];
        if s == 0 {
            mine.push(i);
        }
    }
    mine.sort();
    let extents: Vec<Option<Extent>> = patterns.iter().map(|p| p.check().ok()).collect();
    // A direct read's aligned buffer, holding one read's bytes.
    let src: Vec<u8> = (0..READ_CHUNK as usize + 4096)
        .map(|i| (i * 31 % 251) as u8)
        .collect();
    let mut k = Packing {
        open: (0..c.arenas.len()).map(|_| None).collect(),
        staging: vec![0u8; SLOT],
        in_place: mode == "current",
        free: Vec::new(),
    };
    let mut t = Times::default();
    let start = Instant::now();
    'files: for &fi in &mine {
        let index = FileEntries::new(&per_file[fi], &extents);
        let mut merged: Vec<(u64, u64)> = Vec::new();
        for (a, b) in index.ranges() {
            match merged.last_mut() {
                Some(l) if a <= l.1.saturating_add(GAP) => l.1 = l.1.max(b),
                _ => merged.push((a, b)),
            }
        }
        for &(a, b) in &merged {
            for at in (a..b).step_by(READ_CHUNK as usize) {
                let end = (at + READ_CHUNK).min(b);
                let n = (end - at) as usize;
                let copied;
                let buf: &[u8] = if old {
                    // 0.6.2: the trim copy out of the aligned buffer.
                    let t0 = Instant::now();
                    copied = src[..n].to_vec();
                    t.read_copy += t0.elapsed();
                    &copied
                } else {
                    &src[..n]
                };
                t.bytes += end - at;
                let t0 = Instant::now();
                let visit: Box<dyn Iterator<Item = &Entry>> = if old {
                    Box::new(per_file[fi].iter())
                } else {
                    Box::new(index.overlapping(at, end))
                };
                for e in visit {
                    let p: &Pattern = &patterns[e.pattern as usize];
                    t.walks += 1;
                    for (s, d, len) in RunWalk::new(p, e, at, end) {
                        t.runs += 1;
                        t.payload += len as u64;
                        let o = (s - at) as usize;
                        k.put(&mut t, e.rank as usize, d, &buf[o..o + len as usize]);
                    }
                }
                t.pack += t0.elapsed();
                if t.bytes as f64 >= limit {
                    break 'files;
                }
            }
        }
    }
    let all = start.elapsed();
    let pack = t.pack - t.staging;
    let gbs = |d: Duration| t.bytes as f64 / d.as_secs_f64() / 1e9;
    println!(
        "source 0 of {sources}: {} files, {:.1} GB in {:.2} s: {:.2} GB/s",
        mine.len(),
        t.bytes as f64 / 1e9,
        all.as_secs_f64(),
        gbs(all)
    );
    println!(
        "  runs {} (avg {:.0} B), entries visited {} ({:.0} per 16 MiB read), slots {}",
        t.runs,
        t.bytes as f64 / t.runs.max(1) as f64,
        t.walks,
        t.walks as f64 / (t.bytes as f64 / READ_CHUNK as f64),
        t.slots
    );
    println!(
        "  payload {:.1} GB of {:.1} GB read ({:.0}%: the rest is gaps read through)",
        t.payload as f64 / 1e9,
        t.bytes as f64 / 1e9,
        100.0 * t.payload as f64 / t.bytes.max(1) as f64
    );
    println!(
        "  walk+pack {:.2} s, read copy {:.2} s, staging copy {:.2} s",
        pack.as_secs_f64(),
        t.read_copy.as_secs_f64(),
        t.staging.as_secs_f64()
    );
}
