//! Which copy to read a chunk from (ADR-030).
//!
//! A STABLE file with copies on several hosts can be read from any of
//! them: the local disk, or any holder over the fabric. Each chunk goes to
//! the source with the lowest expected wait, `latency × (in flight + 1)`,
//! where latency is a peak-sensitive moving average of recent chunk reads
//! from that source (it jumps up at once when a source slows and decays
//! over a couple of seconds). An idle local disk always wins: the network
//! is used to add bandwidth once the local disk is busy, not instead of it.
//! Sources that are faster (raptor's disk, a page-cache hit) earn more
//! chunks without being told.
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
    /// Pick the source for the next chunk among `candidates` (non-empty)
    /// and count it in flight.
    pub fn pick(self: &Arc<Self>, candidates: &[Source]) -> Ticket {
        let mut st = self.stats.lock();
        let local_idle = candidates.contains(&Source::Local)
            && st.get(&Source::Local).is_none_or(|s| s.in_flight == 0);
        let src = if local_idle {
            Source::Local
        } else {
            *candidates
                .iter()
                .min_by(|a, b| {
                    let sa = st
                        .get(a)
                        .map_or_else(|| Stat::new(**a).score(), |s| s.score());
                    let sb = st
                        .get(b)
                        .map_or_else(|| Stat::new(**b).score(), |s| s.score());
                    // Ties go to the local disk.
                    sa.total_cmp(&sb)
                        .then_with(|| (**b == Source::Local).cmp(&(**a == Source::Local)))
                })
                .expect("candidates")
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
    fn idle_local_first_then_spill_to_the_fastest_peer() {
        let b = Arc::new(Balancer::default());
        let all = [Source::Local, A, B];
        // Idle local disk wins even with peers available.
        let t1 = b.pick(&all);
        assert_eq!(t1.source(), Source::Local);
        // Local busy: a peer takes the next chunk.
        let t2 = b.pick(&all);
        assert_ne!(t2.source(), Source::Local);
        t2.finish(4 << 20);
        t1.finish(4 << 20);
        teach(&b, Source::Local, 0.003);
        teach(&b, A, 0.001);
        teach(&b, B, 0.010);
        // Local busy again: the faster peer is chosen over the slower one,
        // and with A busy too, local (3 ms × 2) still beats B (10 ms).
        let l = b.pick(&all);
        assert_eq!(l.source(), Source::Local);
        let a = b.pick(&all);
        assert_eq!(a.source(), A);
        let third = b.pick(&all);
        assert_eq!(
            third.source(),
            A,
            "A: 1 ms × 2 beats local 3 ms × 2 and B 10 ms"
        );
        drop(third); // abandoned: not an error
        drop(a);
        l.fail();
        let r = b.report();
        assert!(
            r.iter().any(|x| x.source == Source::Local && x.errors == 1),
            "{r:?}"
        );
        assert!(
            r.iter().all(|x| x.source == Source::Local || x.errors == 0),
            "{r:?}"
        );
        assert!(r.iter().all(|x| x.in_flight == 0), "{r:?}");
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
