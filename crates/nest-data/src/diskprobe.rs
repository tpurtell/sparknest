//! How fast this host's disk reads (ADR-030): measured on the objects the
//! store already holds, with direct I/O (the page cache must not flatter
//! it), in the chunk size reads use, several at once. Run after boot and
//! then daily, only while the host is idle; the result is kept in
//! `disk-bandwidth.json` in the state directory so a restart knows it at
//! once. Readers elsewhere use it to decide how many copies are worth
//! reading from at once.

use serde::{Deserialize, Serialize};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const CHUNK: usize = 4 << 20;
const THREADS: usize = 8;
/// Stop after this much, or this long, whichever comes first.
const MAX_BYTES: u64 = 4 << 30;
const MAX_TIME: Duration = Duration::from_secs(3);

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DiskBandwidth {
    pub bytes_per_s: u64,
    /// Unix seconds.
    pub measured_at: u64,
}

fn file(dir: &Path) -> PathBuf {
    dir.join("disk-bandwidth.json")
}

pub fn load(dir: &Path) -> Option<DiskBandwidth> {
    serde_json::from_slice(&std::fs::read(file(dir)).ok()?).ok()
}

pub fn save(dir: &Path, b: &DiskBandwidth) {
    let _ = std::fs::write(file(dir), serde_json::to_vec(b).unwrap_or_default());
}

/// A 4096-aligned slice of `buf` of `len` bytes (O_DIRECT needs aligned
/// memory); `buf` must have `len + 4096` bytes.
fn aligned(buf: &mut [u8], len: usize) -> &mut [u8] {
    let off = buf.as_ptr().align_offset(4096);
    &mut buf[off..off + len]
}

/// Measure. `None` if the store holds too little to measure (under 256 MiB
/// in objects of at least one chunk). Blocking: run on a blocking thread.
pub fn measure(store: &nest_store::ObjectStore) -> Option<u64> {
    let mut objects: Vec<_> = store
        .scan()
        .ok()?
        .into_iter()
        .filter(|o| o.size >= CHUNK as u64)
        .collect();
    objects.sort_by_key(|o| std::cmp::Reverse(o.size));
    let total: u64 = objects.iter().map(|o| o.size).sum();
    if total < 256 << 20 {
        return None;
    }
    // Work: (path, offset) chunks across the largest objects.
    let mut work = Vec::new();
    'outer: for o in &objects {
        let path = store.path(o.key);
        let mut off = 0;
        while off + CHUNK as u64 <= o.size {
            work.push((path.clone(), off));
            off += CHUNK as u64;
            if work.len() as u64 * CHUNK as u64 >= MAX_BYTES {
                break 'outer;
            }
        }
    }
    let work = std::sync::Mutex::new(work.into_iter());
    let start = Instant::now();
    let read = std::sync::atomic::AtomicU64::new(0);
    std::thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                let mut raw = vec![0u8; CHUNK + 4096];
                loop {
                    if start.elapsed() > MAX_TIME {
                        break;
                    }
                    let Some((path, off)) = work.lock().ok().and_then(|mut w| w.next()) else {
                        break;
                    };
                    let Ok(f) = std::fs::OpenOptions::new()
                        .read(true)
                        .custom_flags(libc::O_DIRECT)
                        .open(&path)
                    else {
                        continue;
                    };
                    if let Ok(n) = f.read_at(aligned(&mut raw, CHUNK), off) {
                        read.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            });
        }
    });
    let secs = start.elapsed().as_secs_f64();
    let bytes = read.into_inner();
    (bytes >= 64 << 20 && secs > 0.0).then(|| (bytes as f64 / secs) as u64)
}

/// This host's RDMA links, in bytes per second (sum of the rails in use),
/// from sysfs (`/sys/class/infiniband/<dev>/ports/1/rate`, "200 Gb/sec").
pub fn link_bytes_per_s(devices: &[String]) -> u64 {
    devices
        .iter()
        .filter_map(|d| {
            std::fs::read_to_string(format!("/sys/class/infiniband/{d}/ports/1/rate")).ok()
        })
        .filter_map(|r| {
            r.split_whitespace()
                .next()
                .and_then(|g| g.parse::<f64>().ok())
        })
        .map(|gbit| (gbit * 1e9 / 8.0) as u64)
        .sum()
}
