//! What a push load did, for status and its visualization: bytes from each
//! source to each rank over time, in fixed bins, kept on the hub for the
//! last few loads (ADR-063).
//!
//! Sources report cumulative bytes per rank every `TICK`; the hub turns the
//! differences into bins. Bytes count when a slot is handed to its send, so
//! a bin is what left the source in that interval, to within a slot.

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::Duration;

/// How often sources report, and the width of a bin.
pub const TICK: Duration = Duration::from_millis(250);
/// Loads kept on a hub.
pub const KEEP: usize = 16;
/// Bins kept per load (an hour at `TICK`); later bins fold into the last.
const MAX_BINS: usize = 14_400;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    #[default]
    Running,
    Done,
    Failed,
}

/// One load, as the hub recorded it.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Record {
    pub group: u64,
    /// The hub's host.
    pub hub: String,
    /// Wall clock at start, Unix milliseconds.
    pub started_ms: u64,
    /// Milliseconds from start to the end (or to now, while running).
    pub elapsed_ms: u64,
    pub outcome: Outcome,
    pub reason: Option<String>,
    /// Source hosts (stores), in their order in `bins`.
    pub sources: Vec<String>,
    /// Each rank's host, by rank.
    pub ranks: Vec<String>,
    pub total_bytes: u64,
    pub bin_ms: u64,
    /// `bins[t][s][r]`: bytes from source s to rank r in bin t.
    pub bins: Vec<Vec<Vec<u64>>>,
    /// Per source: slots sent by RDMA, and by a fallback (a TCP lane or
    /// the control connection), as the source reported.
    pub rdma_slots: Vec<u64>,
    pub rpc_slots: Vec<u64>,
    /// Per source: slots to a rank on its own host (no network).
    #[serde(default)]
    pub local_slots: Vec<u64>,
    /// Milliseconds from start when each source finished, and each rank.
    pub source_done_ms: Vec<Option<u64>>,
    pub rank_done_ms: Vec<Option<u64>>,
    /// Each rank's ring as its host last reported it (free slots, filled,
    /// released, when the engine last released one).
    #[serde(default)]
    pub rings: Vec<Option<crate::push::RingState>>,
    /// The push-reorder-lookahead the load ran with (0: plan order), and
    /// how long the hub took to reorder (ADR-083).
    #[serde(default)]
    pub reorder_lookahead: u64,
    #[serde(default)]
    pub reorder_ms: Option<u64>,
}

impl Record {
    pub fn new(
        group: u64,
        hub: String,
        started_ms: u64,
        sources: Vec<String>,
        ranks: Vec<String>,
        total: u64,
    ) -> Record {
        let (s, r) = (sources.len(), ranks.len());
        Record {
            group,
            hub,
            started_ms,
            total_bytes: total,
            bin_ms: TICK.as_millis() as u64,
            rdma_slots: vec![0; s],
            rpc_slots: vec![0; s],
            local_slots: vec![0; s],
            source_done_ms: vec![None; s],
            rank_done_ms: vec![None; r],
            sources,
            ranks,
            ..Default::default()
        }
    }

    /// Bytes `source` has sent each rank so far (cumulative), reported at
    /// `at_ms` after start: the growth since its last report goes into the
    /// bin of `at_ms`.
    pub fn report(&mut self, source: usize, at_ms: u64, sent: &[u64], last: &mut Vec<u64>) {
        if source >= self.sources.len() {
            return;
        }
        let bin = ((at_ms / self.bin_ms.max(1)) as usize).min(MAX_BINS - 1);
        let (s, r) = (self.sources.len(), self.ranks.len());
        while self.bins.len() <= bin {
            self.bins.push(vec![vec![0; r]; s]);
        }
        last.resize(r, 0);
        for (rank, &now) in sent.iter().enumerate().take(r) {
            let d = now.saturating_sub(last[rank]);
            self.bins[bin][source][rank] += d;
            last[rank] = last[rank].max(now);
        }
        self.elapsed_ms = self.elapsed_ms.max(at_ms);
    }

    /// Bytes from each source to each rank over the whole load.
    pub fn totals(&self) -> Vec<Vec<u64>> {
        let mut t = vec![vec![0u64; self.ranks.len()]; self.sources.len()];
        for b in &self.bins {
            for (s, row) in b.iter().enumerate() {
                for (r, v) in row.iter().enumerate() {
                    t[s][r] += v;
                }
            }
        }
        t
    }

    /// The record without its bins (for listing).
    pub fn summary(&self) -> Record {
        Record {
            bins: Vec::new(),
            ..self.clone()
        }
    }
}

/// A hub's recent loads, newest last.
#[derive(Default)]
pub struct History(VecDeque<Record>);

impl History {
    pub fn push(&mut self, r: Record) {
        if self.0.len() >= KEEP {
            self.0.pop_front();
        }
        self.0.push_back(r);
    }

    pub fn get_mut(&mut self, group: u64) -> Option<&mut Record> {
        self.0.iter_mut().find(|r| r.group == group)
    }

    pub fn all(&self) -> impl Iterator<Item = &Record> {
        self.0.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_bin_their_growth() {
        let mut r = Record::new(
            1,
            "h".into(),
            0,
            vec!["a".into(), "b".into()],
            vec!["x".into(), "y".into()],
            100,
        );
        let (mut la, mut lb) = (Vec::new(), Vec::new());
        r.report(0, 100, &[10, 0], &mut la);
        r.report(0, 300, &[30, 5], &mut la);
        r.report(1, 310, &[0, 40], &mut lb);
        // Out of order or repeated reports never go backwards.
        r.report(0, 320, &[25, 5], &mut la);
        assert_eq!(r.bins.len(), 2);
        assert_eq!(r.bins[0], vec![vec![10, 0], vec![0, 0]]);
        assert_eq!(r.bins[1], vec![vec![20, 5], vec![0, 40]]);
        assert_eq!(r.totals(), vec![vec![30, 5], vec![0, 40]]);
        assert_eq!(r.elapsed_ms, 320);
        assert!(r.summary().bins.is_empty());
    }

    #[test]
    fn history_keeps_the_last_few() {
        let mut h = History::default();
        for g in 0..(KEEP as u64 + 3) {
            h.push(Record {
                group: g,
                ..Default::default()
            });
        }
        assert_eq!(h.all().count(), KEEP);
        assert_eq!(h.all().next().unwrap().group, 3);
    }
}
