//! How long this host's reads take, by kind and size, over recent windows.
//!
//! Every read records its latency into a log2 histogram (microseconds) for
//! its kind (this host's disk, a fabric read made, a fabric read served)
//! and size class (the slot tiers: ≤4 KiB, ≤128 KiB, larger). Recording is
//! a few relaxed atomic adds. Once a second `roll` snapshots the running
//! totals, keeping ten minutes of them, so a window is the difference of
//! two snapshots: rates and percentiles for the last 10 s, 1 min, 10 min.
//! Readahead's dropped and used bytes ride along for windowed waste.

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Kind {
    /// This host's own disk (reads for its readers).
    Disk,
    /// Fabric reads this host made, from request to bytes in hand.
    Fabric,
    /// Fabric reads this host served: reading the bytes.
    Served,
}

const KINDS: [Kind; 3] = [Kind::Disk, Kind::Fabric, Kind::Served];
/// Size classes, by upper bound in bytes (the last takes the rest).
const CLASSES: [u64; 3] = [4 << 10, 128 << 10, u64::MAX];
const BUCKETS: usize = 32;
const KEEP: usize = 601;
/// Windows reported, seconds.
pub const WINDOWS: [u64; 3] = [10, 60, 600];

fn class_of(bytes: u64) -> usize {
    CLASSES
        .iter()
        .position(|c| bytes <= *c)
        .unwrap_or(CLASSES.len() - 1)
}

fn bucket_of(us: u64) -> usize {
    (64 - us.max(1).leading_zeros() as usize).min(BUCKETS - 1)
}

#[derive(Default)]
struct Cell {
    count: AtomicU64,
    bytes: AtomicU64,
    us: AtomicU64,
    buckets: [AtomicU64; BUCKETS],
}

/// Totals at one moment.
#[derive(Clone)]
struct Snap {
    at: Instant,
    cells: Vec<[u64; BUCKETS + 3]>,
    dropped: u64,
    used: u64,
}

#[derive(Default)]
pub struct IoStats {
    cells: [[Cell; 3]; 3],
    ra_dropped: AtomicU64,
    ra_used: AtomicU64,
    snaps: Mutex<VecDeque<Snap>>,
}

impl std::fmt::Debug for IoStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("IoStats")
    }
}

/// One kind and size class over a window.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassStat {
    pub kind: Option<Kind>,
    /// Upper bound of the size class in bytes (0: larger than the others).
    pub max_bytes: u64,
    pub count: u64,
    pub bytes: u64,
    pub mean_us: u64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
}

/// Everything over one window.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Window {
    /// Milliseconds actually covered (shorter just after start).
    pub ms: u64,
    pub classes: Vec<ClassStat>,
    /// Readahead chunks dropped, and bytes of them readers used.
    pub readahead_dropped: u64,
    pub readahead_used: u64,
}

impl IoStats {
    pub fn record(&self, kind: Kind, bytes: u64, took: Duration) {
        let us = took.as_micros() as u64;
        let c = &self.cells[kind as usize][class_of(bytes)];
        c.count.fetch_add(1, Relaxed);
        c.bytes.fetch_add(bytes, Relaxed);
        c.us.fetch_add(us, Relaxed);
        c.buckets[bucket_of(us)].fetch_add(1, Relaxed);
    }

    /// Readahead dropped `bytes` of chunks, `used` of them consumed.
    pub fn readahead(&self, bytes: u64, used: u64) {
        self.ra_dropped.fetch_add(bytes, Relaxed);
        self.ra_used.fetch_add(used.min(bytes), Relaxed);
    }

    fn snap(&self) -> Snap {
        let mut cells = Vec::with_capacity(9);
        for k in 0..3 {
            for c in 0..3 {
                let x = &self.cells[k][c];
                let mut v = [0u64; BUCKETS + 3];
                v[0] = x.count.load(Relaxed);
                v[1] = x.bytes.load(Relaxed);
                v[2] = x.us.load(Relaxed);
                for (i, b) in x.buckets.iter().enumerate() {
                    v[3 + i] = b.load(Relaxed);
                }
                cells.push(v);
            }
        }
        Snap {
            at: Instant::now(),
            cells,
            dropped: self.ra_dropped.load(Relaxed),
            used: self.ra_used.load(Relaxed),
        }
    }

    /// Take this second's snapshot (call about once a second).
    pub fn roll(&self) {
        let s = self.snap();
        let mut q = self.snaps.lock();
        q.push_back(s);
        while q.len() > KEEP {
            q.pop_front();
        }
    }

    /// The last 10 s, 1 min and 10 min (as far back as snapshots go).
    pub fn windows(&self) -> Vec<Window> {
        let now = self.snap();
        let q = self.snaps.lock();
        WINDOWS
            .iter()
            .map(|w| {
                let want = now.at.checked_sub(Duration::from_secs(*w));
                let base = q
                    .iter()
                    .find(|s| want.is_none_or(|t| s.at >= t))
                    .or(q.front());
                match base {
                    Some(b) => diff(b, &now),
                    None => Window::default(),
                }
            })
            .collect()
    }
}

fn diff(a: &Snap, b: &Snap) -> Window {
    let mut classes = Vec::new();
    for (i, (x, y)) in a.cells.iter().zip(&b.cells).enumerate() {
        let d: Vec<u64> = x.iter().zip(y).map(|(p, q)| q.saturating_sub(*p)).collect();
        if d[0] == 0 {
            continue;
        }
        let pct = |p: f64| -> u64 {
            let want = (d[0] as f64 * p).ceil() as u64;
            let mut seen = 0;
            for (j, n) in d[3..].iter().enumerate() {
                seen += n;
                if seen >= want {
                    // The bucket's upper bound: j bits of microseconds.
                    return 1u64 << j;
                }
            }
            1u64 << (BUCKETS - 1)
        };
        let class = CLASSES[i % 3];
        classes.push(ClassStat {
            kind: Some(KINDS[i / 3]),
            max_bytes: if class == u64::MAX { 0 } else { class },
            count: d[0],
            bytes: d[1],
            mean_us: d[2] / d[0],
            p50_us: pct(0.5),
            p90_us: pct(0.9),
            p99_us: pct(0.99),
        });
    }
    Window {
        ms: b.at.duration_since(a.at).as_millis() as u64,
        classes,
        readahead_dropped: b.dropped - a.dropped,
        readahead_used: b.used - a.used,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_split_by_kind_and_size() {
        let s = IoStats::default();
        s.roll();
        for _ in 0..90 {
            s.record(Kind::Fabric, 4096, Duration::from_micros(300));
        }
        for _ in 0..10 {
            s.record(Kind::Fabric, 4096, Duration::from_micros(5000));
        }
        s.record(Kind::Disk, 4 << 20, Duration::from_millis(2));
        s.readahead(8 << 20, 1 << 20);
        let w = &s.windows()[0];
        let f = w
            .classes
            .iter()
            .find(|c| c.kind == Some(Kind::Fabric))
            .unwrap();
        assert_eq!((f.count, f.max_bytes, f.bytes), (100, 4096, 409600));
        // 300 us falls in the 512 us bucket; 5 ms in 8192.
        assert_eq!((f.p50_us, f.p90_us, f.p99_us), (512, 512, 8192));
        assert_eq!(f.mean_us, (90 * 300 + 10 * 5000) / 100);
        let d = w
            .classes
            .iter()
            .find(|c| c.kind == Some(Kind::Disk))
            .unwrap();
        assert_eq!((d.count, d.max_bytes), (1, 0));
        assert_eq!((w.readahead_dropped, w.readahead_used), (8 << 20, 1 << 20));
    }
}
