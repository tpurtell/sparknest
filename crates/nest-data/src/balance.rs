//! Which copy to read a chunk from (ADR-030).
//!
//! A STABLE file with copies on several hosts can be read from any of
//! them: the local disk, or any holder over the fabric. Each source's
//! expected wait is `latency × (in flight + 1)`, latency being a
//! peak-sensitive moving average of its recent chunk reads (it jumps up at
//! once when a source slows and decays over a couple of seconds).
//!
//! - The local disk goes first while it keeps up: it takes a chunk unless
//!   its expected wait is more than `SLACK` times the best available.
//! - What the local disk cannot take goes to the other holders by
//!   rendezvous hashing: the file is cut into stripes, each stripe ranks
//!   the holders by a hash of (file, stripe, holder), and the first-ranked
//!   holder not clearly busier than the best takes it. Hosts that load the
//!   same file at once (a model loaded on every Spark at boot) send the same
//!   stripe to the same holder, which reads it from disk once and serves
//!   the rest from its page cache; a busy holder sheds stripes to the next.
//! - Faster sources (raptor's disk, a page-cache hit) keep more work
//!   without being told.
//! - A stripe only ever goes to as many of its ranked holders as can
//!   together fill this host's links: the first holders whose measured disk
//!   bandwidth sums to `HEADROOM` × the link rate. Asking more disks gains
//!   nothing the links could carry, and a small set per stripe keeps the
//!   page caches of concurrent readers useful.
//!
//! The same table records bytes per source over a sliding window for
//! `nest io`, which prints what each host has learned.

use nest_types::NodeId;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Source {
    Local,
    Peer(NodeId),
}

/// How much busier than the best choice a preferred source may be.
const SLACK: f64 = 2.0;
/// The holders a stripe may use supply this much of the links' rate.
const HEADROOM: f64 = 1.25;

/// Rendezvous weight of holder `node` for `key` (a stripe of a file): the
/// same on every host.
fn weight(key: u64, node: NodeId) -> u64 {
    // SplitMix64 of the pair.
    let mut z = key ^ node.0.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Latency assumed before a source has been measured, per chunk.
const PRIOR_LOCAL: Duration = Duration::from_micros(1500);
const PRIOR_PEER: Duration = Duration::from_micros(2500);
/// How fast a high latency sample decays back.
const DECAY: Duration = Duration::from_secs(2);
/// Window for the byte rates `nest io` shows.
const WINDOW: Duration = Duration::from_secs(10);

#[derive(Debug)]
struct Stat {
    /// Seconds per chunk (peak EWMA).
    latency: f64,
    measured: bool,
    last: Instant,
    in_flight: u32,
    bytes_total: u64,
    reads_total: u64,
    errors: u64,
    /// (when, bytes) of recent completions, for the windowed rate.
    recent: std::collections::VecDeque<(Instant, u64)>,
}

impl Stat {
    fn new(src: Source) -> Stat {
        let prior = match src {
            Source::Local => PRIOR_LOCAL,
            Source::Peer(_) => PRIOR_PEER,
        };
        Stat {
            latency: prior.as_secs_f64(),
            measured: false,
            last: Instant::now(),
            in_flight: 0,
            bytes_total: 0,
            reads_total: 0,
            errors: 0,
            recent: Default::default(),
        }
    }

    fn sample(&mut self, secs: f64, bytes: u64, now: Instant) {
        if !self.measured || secs >= self.latency {
            self.latency = secs;
        } else {
            let dt = now.duration_since(self.last).as_secs_f64();
            let w = (-dt / DECAY.as_secs_f64()).exp();
            self.latency = self.latency * w + secs * (1.0 - w);
        }
        self.measured = true;
        self.last = now;
        self.bytes_total += bytes;
        self.reads_total += 1;
        self.recent.push_back((now, bytes));
        while self
            .recent
            .front()
            .is_some_and(|(t, _)| now.duration_since(*t) > WINDOW)
        {
            self.recent.pop_front();
        }
    }

    fn score(&self) -> f64 {
        self.latency * (self.in_flight as f64 + 1.0)
    }
}

/// One host's view of its read sources.
#[derive(Default)]
pub struct Balancer {
    stats: Mutex<HashMap<Source, Stat>>,
    /// Holders' measured disk read rate (bytes/s) and this host's link
    /// rate, for capping how many holders a stripe uses.
    capacity: Mutex<(HashMap<NodeId, u64>, u64)>,
}

/// A read in progress from one source; recorded when finished.
pub struct Ticket {
    b: Arc<Balancer>,
    src: Source,
    start: Instant,
    done: bool,
}

impl Ticket {
    pub fn source(&self) -> Source {
        self.src
    }

    pub fn finish(mut self, bytes: u64) {
        let now = Instant::now();
        let mut st = self.b.stats.lock();
        let s = st.entry(self.src).or_insert_with(|| Stat::new(self.src));
        s.in_flight = s.in_flight.saturating_sub(1);
        s.sample(now.duration_since(self.start).as_secs_f64(), bytes, now);
        self.done = true;
    }
}

impl Ticket {
    /// The read failed: count it, and let a slow failure raise the latency.
    pub fn fail(mut self) {
        let now = Instant::now();
        let mut st = self.b.stats.lock();
        let s = st.entry(self.src).or_insert_with(|| Stat::new(self.src));
        s.in_flight = s.in_flight.saturating_sub(1);
        s.errors += 1;
        let secs = now.duration_since(self.start).as_secs_f64();
        if secs > s.latency {
            s.latency = secs;
        }
        self.done = true;
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        // Abandoned (a speculative chunk the reader skipped): free the slot.
        if !self.done {
            let mut st = self.b.stats.lock();
            if let Some(s) = st.get_mut(&self.src) {
                s.in_flight = s.in_flight.saturating_sub(1);
            }
        }
    }
}

impl Balancer {
    /// Nothing is being read through the balancer right now.
    pub fn idle(&self) -> bool {
        self.stats.lock().values().all(|s| s.in_flight == 0)
    }

    /// Holders' disk read rates and this host's link rate (bytes/s; 0 for
    /// unknown).
    pub fn set_capacity(&self, disks: HashMap<NodeId, u64>, link: u64) {
        *self.capacity.lock() = (disks, link);
    }

    /// The prefix of `ranked` whose disks can fill the links with headroom
    /// (all of them when rates are unknown).
    fn cap(&self, ranked: &mut Vec<NodeId>) {
        let (disks, link) = &*self.capacity.lock();
        if *link == 0 || disks.is_empty() {
            return;
        }
        let mut known: Vec<u64> = disks.values().copied().filter(|b| *b > 0).collect();
        if known.is_empty() {
            return;
        }
        known.sort_unstable();
        let median = known[known.len() / 2];
        let want = (*link as f64 * HEADROOM) as u64;
        let mut sum = 0u64;
        let mut keep = 0;
        for n in ranked.iter() {
            keep += 1;
            sum += disks.get(n).copied().filter(|b| *b > 0).unwrap_or(median);
            if sum >= want {
                break;
            }
        }
        ranked.truncate(keep.max(1));
    }

    /// Pick whichever of `candidates` (non-empty) answers soonest by its
    /// load, for a small scattered read: no stripes, no cap (ADR-031).
    pub fn pick_least_busy(self: &Arc<Self>, candidates: &[Source]) -> Ticket {
        let mut st = self.stats.lock();
        let src = *candidates
            .iter()
            .min_by(|a, b| {
                let sa = st
                    .get(a)
                    .map_or_else(|| Stat::new(**a).score(), |x| x.score());
                let sb = st
                    .get(b)
                    .map_or_else(|| Stat::new(**b).score(), |x| x.score());
                sa.total_cmp(&sb)
            })
            .expect("non-empty");
        st.entry(src).or_insert_with(|| Stat::new(src)).in_flight += 1;
        drop(st);
        Ticket {
            b: self.clone(),
            src,
            start: Instant::now(),
            done: false,
        }
    }

    /// Pick the source for the next chunk of stripe `key` among
    /// `candidates` (non-empty) and count it in flight.
    pub fn pick(self: &Arc<Self>, candidates: &[Source], key: u64) -> Ticket {
        let mut st = self.stats.lock();
        let score = |s: &Source| {
            st.get(s)
                .map_or_else(|| Stat::new(*s).score(), |x| x.score())
        };
        let best = candidates.iter().map(&score).fold(f64::INFINITY, f64::min);
        let src = if candidates.contains(&Source::Local) && score(&Source::Local) <= best * SLACK {
            Source::Local
        } else {
            let mut peers: Vec<NodeId> = candidates
                .iter()
                .filter_map(|s| match s {
                    Source::Peer(n) => Some(*n),
                    Source::Local => None,
                })
                .collect();
            peers.sort_by_key(|n| std::cmp::Reverse(weight(key, *n)));
            self.cap(&mut peers);
            let best = peers
                .iter()
                .map(|n| score(&Source::Peer(*n)))
                .fold(f64::INFINITY, f64::min)
                .min(best);
            let capped: Vec<Source> = peers.into_iter().map(Source::Peer).collect();
            capped
                .iter()
                .copied()
                .find(|s| score(s) <= best * SLACK)
                .unwrap_or_else(|| {
                    // Every allowed holder is busy: the least busy of them
                    // (or the local disk).
                    capped
                        .iter()
                        .chain(candidates.iter().filter(|c| **c == Source::Local))
                        .copied()
                        .min_by(|a, b| score(a).total_cmp(&score(b)))
                        .expect("candidates")
                })
        };
        st.entry(src).or_insert_with(|| Stat::new(src)).in_flight += 1;
        Ticket {
            b: self.clone(),
            src,
            start: Instant::now(),
            done: false,
        }
    }

    /// What this host has learned, for `nest io`.
    pub fn report(&self) -> Vec<SourceReport> {
        let now = Instant::now();
        let st = self.stats.lock();
        let mut v: Vec<SourceReport> = st
            .iter()
            .map(|(src, s)| {
                let recent: u64 = s
                    .recent
                    .iter()
                    .filter(|(t, _)| now.duration_since(*t) <= WINDOW)
                    .map(|(_, b)| *b)
                    .sum();
                SourceReport {
                    source: *src,
                    latency_us: (s.latency * 1e6) as u64,
                    measured: s.measured,
                    in_flight: s.in_flight,
                    bytes_per_s: recent / WINDOW.as_secs(),
                    bytes_total: s.bytes_total,
                    reads_total: s.reads_total,
                    errors: s.errors,
                }
            })
            .collect();
        v.sort_by_key(|r| match r.source {
            Source::Local => 0,
            Source::Peer(n) => n.0 + 1,
        });
        v
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceReport {
    pub source: Source,
    /// Per chunk, recent (peak EWMA).
    pub latency_us: u64,
    /// False while still the prior guess.
    pub measured: bool,
    pub in_flight: u32,
    /// Over the last 10 s.
    pub bytes_per_s: u64,
    pub bytes_total: u64,
    pub reads_total: u64,
    pub errors: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: Source = Source::Peer(NodeId(2));
    const B: Source = Source::Peer(NodeId(3));

    fn teach(b: &Balancer, src: Source, secs: f64) {
        let mut st = b.stats.lock();
        let s = st.entry(src).or_insert_with(|| Stat::new(src));
        s.latency = secs;
        s.measured = true;
    }

    #[test]
    fn local_first_then_the_stripe_ranked_peer() {
        let b = Arc::new(Balancer::default());
        let all = [Source::Local, A, B];
        for s in all {
            teach(&b, s, 0.002);
        }
        // Local takes chunks while within SLACK of the best (idle peers:
        // 2 ms); at 2 in flight it is at 6 ms > 2 × 2 ms, so the next spills.
        let l1 = b.pick(&all, 7);
        let l2 = b.pick(&all, 7);
        assert_eq!((l1.source(), l2.source()), (Source::Local, Source::Local));
        let spill = b.pick(&all, 7);
        assert_ne!(spill.source(), Source::Local);
        // The spill is the stripe's first-ranked peer, the same on any host.
        let first = if weight(7, NodeId(2)) > weight(7, NodeId(3)) {
            A
        } else {
            B
        };
        assert_eq!(spill.source(), first);
        drop((l1, l2, spill));
    }

    #[test]
    fn hosts_spilling_the_same_stripe_ask_the_same_holder() {
        // Hosts 1 and 2 both busy locally; holders 3, 4, 5 shared.
        let peers = [NodeId(3), NodeId(4), NodeId(5)];
        // Each host has its own balancer and a saturated local disk.
        let pick_on = |key: u64| {
            let b = Arc::new(Balancer::default());
            teach(&b, Source::Local, 1.0);
            let mut c: Vec<Source> = peers.iter().map(|n| Source::Peer(*n)).collect();
            c.push(Source::Local);
            b.pick(&c, key).source()
        };
        let mut spread = std::collections::HashMap::new();
        for key in 0..300u64 {
            let (x, y) = (pick_on(key), pick_on(key));
            assert_eq!(x, y, "stripe {key}");
            *spread.entry(format!("{x:?}")).or_insert(0) += 1;
        }
        assert_eq!(spread.len(), 3, "{spread:?}");
        assert!(spread.values().all(|n| *n > 60), "{spread:?}");
    }

    #[test]
    fn a_busy_peer_sheds_and_failures_count() {
        let b = Arc::new(Balancer::default());
        let peers = [A, B];
        teach(&b, A, 0.002);
        teach(&b, B, 0.002);
        let key = (0..1000u64)
            .find(|k| weight(*k, NodeId(2)) > weight(*k, NodeId(3)))
            .unwrap();
        let held: Vec<Ticket> = (0..3).map(|_| b.pick(&[A], key)).collect();
        // A: 2 ms × 4 = 8 ms > 2 × B's 2 ms: the stripe goes to B.
        let t = b.pick(&peers, key);
        assert_eq!(t.source(), B);
        drop(held);
        let t2 = b.pick(&peers, key);
        assert_eq!(t2.source(), A, "idle again: back to its first choice");
        t2.fail(); // a failure counts; an abandoned read does not
        drop(t);
        let r = b.report();
        assert!(r.iter().any(|x| x.source == A && x.errors == 1), "{r:?}");
        assert!(
            r.iter()
                .all(|x| x.in_flight == 0 && (x.source == A || x.errors == 0)),
            "{r:?}"
        );
    }

    #[test]
    fn a_stripe_uses_only_the_holders_the_links_can_use() {
        let b = Arc::new(Balancer::default());
        // Links: 10 GB/s; holders' disks 5 GB/s each: 10 × 1.25 needs 3.
        let peers: Vec<NodeId> = (2..=8).map(NodeId).collect();
        b.set_capacity(
            peers.iter().map(|n| (*n, 5_000_000_000)).collect(),
            10_000_000_000,
        );
        let c: Vec<Source> = peers.iter().map(|n| Source::Peer(*n)).collect();
        for n in &peers {
            teach(&b, Source::Peer(*n), 0.002);
        }
        let key = 42;
        let mut ranked = peers.clone();
        ranked.sort_by_key(|n| std::cmp::Reverse(weight(key, *n)));
        // Load the stripe heavily: it may spread, but never past the top 3.
        let held: Vec<Ticket> = (0..40).map(|_| b.pick(&c, key)).collect();
        let used: std::collections::HashSet<Source> = held.iter().map(|t| t.source()).collect();
        assert!(used.len() > 1, "a loaded stripe spreads: {used:?}");
        for s in &used {
            let Source::Peer(n) = s else { panic!() };
            assert!(
                ranked[..3].contains(n),
                "{n:?} is outside the top 3 {:?}",
                &ranked[..3]
            );
        }
    }

    #[test]
    fn peak_ewma_jumps_up_and_decays() {
        let mut s = Stat::new(A);
        let t0 = Instant::now();
        s.sample(0.002, 1, t0);
        s.sample(0.020, 1, t0 + Duration::from_millis(10));
        assert!((s.latency - 0.020).abs() < 1e-9, "jumps to a slow sample");
        s.sample(0.002, 1, t0 + Duration::from_secs(6));
        assert!(
            s.latency < 0.004,
            "decays after a few seconds: {}",
            s.latency
        );
    }
}
