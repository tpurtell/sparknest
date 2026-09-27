//! Which files this host reads at random, learned from how readers use
//! readahead (ADR-030).
//!
//! Readahead pays off for streams: a loader reading shards front to back.
//! Some files are read in scattered tiny pieces instead (lookup tables such
//! as engram tables, a mapped file faulted at random): each 4 MiB chunk the
//! readahead fetches serves a few KiB, the rest is discarded, and the disks
//! and fabric drown in reads nobody asked for. So every chunk readahead
//! discards reports how much of it readers consumed; once a file's chunks
//! are mostly unused it is read directly: exactly the requested range, from
//! this host's copy when it has one (passthrough for new opens, when
//! allowed) and otherwise from whichever copy is least busy, over every
//! copy. Direct reads that turn out to continue each other switch it back.
//!
//! Judged per file, for every handle on this host, so a reader that opens a
//! file for each lookup still teaches the next open. A verdict is recorded
//! in the file's metadata (`SetReadPattern`, ADR-031) so every host and
//! every restart starts from it; opens seed this table from there. A host
//! serving the file by passthrough no longer sees its reads, so only hosts
//! reading through the daemon revise it.

use nest_types::FileId;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Judge a file after readahead discarded this many bytes of chunks.
const JUDGE_BYTES: u64 = 64 << 20;
/// Random: readers consumed less than this share of the discarded chunks.
const USED_SHARE: f64 = 0.25;
/// Back to readahead after this many direct bytes...
const SEQ_JUDGE_BYTES: u64 = 256 << 20;
/// ...if at least this share of them continued the previous read.
const SEQ_SHARE: f64 = 0.9;
/// Forget files not read for this long (and keep the table bounded).
const IDLE: Duration = Duration::from_secs(30 * 60);
const MAX_FILES: usize = 1 << 16;
/// A verdict this host just changed is not overridden by what the metadata
/// said before its record landed.
const SETTLE: Duration = Duration::from_secs(10);

#[derive(Debug)]
struct Profile {
    random: bool,
    /// Readahead chunks discarded, and how much of them readers consumed.
    discarded: u64,
    consumed: u64,
    /// Direct reads since switching, and those continuing the one before.
    direct: u64,
    sequential: u64,
    last: Instant,
    /// When this host last changed its verdict (its record may be in flight).
    changed: Option<Instant>,
}

impl Profile {
    fn new() -> Profile {
        Profile {
            random: false,
            discarded: 0,
            consumed: 0,
            direct: 0,
            sequential: 0,
            last: Instant::now(),
            changed: None,
        }
    }
}

/// Told when this host changes its verdict on a file (to record it).
pub type OnChange = Box<dyn Fn(FileId, bool) + Send + Sync>;

#[derive(Default)]
pub struct Patterns {
    files: Mutex<HashMap<FileId, Profile>>,
    on_change: std::sync::OnceLock<OnChange>,
    /// Host-wide: bytes of readahead chunks dropped, and of them consumed
    /// (their difference is readahead's waste).
    dropped: std::sync::atomic::AtomicU64,
    used: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for Patterns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Patterns({} files)", self.files.lock().len())
    }
}

impl Patterns {
    pub fn set_on_change(&self, f: OnChange) {
        let _ = self.on_change.set(f);
    }

    fn changed(&self, file: FileId, random: bool) {
        if let Some(f) = self.on_change.get() {
            f(file, random);
        }
    }

    /// Whether reads of this file go direct instead of through readahead.
    pub fn is_random(&self, file: FileId) -> bool {
        self.files.lock().get(&file).is_some_and(|p| p.random)
    }

    /// What the file's metadata records, seen at open. The record wins
    /// (another host's verdict, or `nest read-pattern`), except just after
    /// this host changed its own verdict, whose record may still be in
    /// flight.
    pub fn seed(&self, file: FileId, scattered: bool) {
        let mut m = self.files.lock();
        match m.get_mut(&file) {
            Some(p) => {
                let settling = p.changed.is_some_and(|t| t.elapsed() < SETTLE);
                if p.random != scattered && !settling {
                    p.random = scattered;
                    p.direct = 0;
                    p.sequential = 0;
                    p.discarded = 0;
                    p.consumed = 0;
                }
            }
            None if scattered => {
                prune(&mut m);
                let mut p = Profile::new();
                p.random = true;
                m.insert(file, p);
            }
            None => {}
        }
    }

    /// Readahead of `file` discarded chunks of `bytes` of which readers
    /// consumed `consumed`. Returns whether the file just became random.
    pub fn discarded(&self, file: FileId, bytes: u64, consumed: u64) -> bool {
        if bytes == 0 {
            return false;
        }
        use std::sync::atomic::Ordering::Relaxed;
        self.dropped.fetch_add(bytes, Relaxed);
        self.used.fetch_add(consumed.min(bytes), Relaxed);
        let mut m = self.files.lock();
        prune(&mut m);
        let p = m.entry(file).or_insert_with(Profile::new);
        p.last = Instant::now();
        p.discarded += bytes;
        p.consumed += consumed.min(bytes);
        if p.discarded < JUDGE_BYTES {
            return false;
        }
        let random = !p.random && (p.consumed as f64) < p.discarded as f64 * USED_SHARE;
        // A decaying tally, not fresh windows: a stream that ends discards
        // its unread tail at once, which alone would look scattered.
        if p.discarded >= 4 * JUDGE_BYTES {
            p.discarded /= 2;
            p.consumed /= 2;
        }
        if random {
            p.random = true;
            p.changed = Some(Instant::now());
            p.direct = 0;
            p.sequential = 0;
            tracing::info!(
                file = file.0,
                "reads of this file are scattered: reading it directly"
            );
        }
        drop(m);
        if random {
            self.changed(file, true);
        }
        random
    }

    /// A direct read of a random file; `sequential` when it started where
    /// the handle's previous read ended.
    pub fn direct(&self, file: FileId, bytes: u64, sequential: bool) {
        let mut m = self.files.lock();
        let Some(p) = m.get_mut(&file) else {
            return;
        };
        let mut streams = false;
        p.last = Instant::now();
        p.direct += bytes;
        if sequential {
            p.sequential += bytes;
        }
        if p.direct >= SEQ_JUDGE_BYTES {
            if p.sequential as f64 >= p.direct as f64 * SEQ_SHARE {
                p.random = false;
                p.changed = Some(Instant::now());
                p.discarded = 0;
                p.consumed = 0;
                streams = true;
                tracing::info!(
                    file = file.0,
                    "reads of this file stream again: reading ahead"
                );
            }
            p.direct = 0;
            p.sequential = 0;
        }
        drop(m);
        if streams {
            self.changed(file, false);
        }
    }

    /// Readahead chunks dropped so far on this host, and bytes of them
    /// readers consumed.
    pub fn readahead_totals(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (self.dropped.load(Relaxed), self.used.load(Relaxed))
    }

    /// Files currently read directly.
    pub fn random_files(&self) -> usize {
        self.files.lock().values().filter(|p| p.random).count()
    }
}

fn prune(m: &mut HashMap<FileId, Profile>) {
    if m.len() < MAX_FILES {
        return;
    }
    let now = Instant::now();
    m.retain(|_, p| now.duration_since(p.last) < IDLE);
    if m.len() >= MAX_FILES {
        // Still full of recent files: forget the ones not judged random.
        m.retain(|_, p| p.random);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const F: FileId = FileId(9);
    const CHUNK: u64 = 4 << 20;

    #[test]
    fn a_stream_stays_on_readahead() {
        let p = Patterns::default();
        // Every discarded chunk was read whole (its growing window's
        // unread tail is discarded only at the end).
        for _ in 0..64 {
            p.discarded(F, CHUNK, CHUNK);
        }
        p.discarded(F, 16 * CHUNK, 0);
        assert!(!p.is_random(F));
    }

    #[test]
    fn scattered_lookups_go_direct() {
        let p = Patterns::default();
        let mut became = false;
        for _ in 0..16 {
            became |= p.discarded(F, CHUNK, 4096);
        }
        assert!(became && p.is_random(F));
        assert!(!p.is_random(FileId(10)));
        assert_eq!(p.random_files(), 1);
    }

    #[test]
    fn small_reads_per_open_go_direct() {
        // Open, read one 1 MiB piece, close: the window's chunks go unread.
        let p = Patterns::default();
        for _ in 0..8 {
            p.discarded(F, 2 * CHUNK, 1 << 20);
        }
        assert!(p.is_random(F));
    }

    #[test]
    fn direct_reads_that_stream_switch_back() {
        let p = Patterns::default();
        for _ in 0..16 {
            p.discarded(F, CHUNK, 0);
        }
        assert!(p.is_random(F));
        // Mostly scattered: stays direct.
        for i in 0..512u64 {
            p.direct(F, 1 << 20, i % 2 == 0);
        }
        assert!(p.is_random(F));
        // Streaming: back to readahead.
        for _ in 0..256 {
            p.direct(F, 1 << 20, true);
        }
        assert!(!p.is_random(F));
    }

    #[test]
    fn recorded_verdicts_seed_and_changes_are_reported() {
        let p = Patterns::default();
        let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
        let s2 = seen.clone();
        p.set_on_change(Box::new(move |f, r| s2.lock().push((f, r))));
        p.seed(F, true);
        assert!(p.is_random(F));
        assert!(seen.lock().is_empty(), "a seed is not a change");
        p.seed(FileId(10), false);
        assert!(!p.is_random(FileId(10)));
        // An override reaches a host that formed no verdict of its own.
        p.seed(F, false);
        assert!(!p.is_random(F));
        p.seed(F, true);
        for _ in 0..16 {
            p.discarded(FileId(10), CHUNK, 0);
        }
        // Its own fresh verdict is not undone by the old record.
        p.seed(FileId(10), false);
        assert!(p.is_random(FileId(10)));
        for _ in 0..256 {
            p.direct(F, 1 << 20, true);
        }
        assert_eq!(*seen.lock(), vec![(FileId(10), true), (F, false)]);
    }
}
