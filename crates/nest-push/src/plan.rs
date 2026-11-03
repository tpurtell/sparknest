//! A push plan (ADR-063 §2–3): which bytes of which files go where in each
//! rank's arena, as strided patterns, and the checks made before any byte
//! moves.
//!
//! A pattern copies `run` bytes at each point of an N-d index space:
//!
//! ```text
//! src = src_base + Σ iₖ · src_strideₖ      dst = dst_base + Σ jₖ · dst_strideₖ
//! ```
//!
//! where, along each dimension, `jₖ` counts steps `0, 1, 2, …` and `iₖ` is
//! the same (a `Count` dimension) or the step's value from a sorted index
//! list: an index list selects from the source and packs into the
//! destination. Dims are outermost first. Every stride must step past everything inside it, on
//! both sides, so a pattern's runs are disjoint, ascending in both the file
//! and the arena, and each entry's footprint has closed-form bounds.

use serde::{Deserialize, Serialize};

/// Most dimensions a pattern may have.
pub const MAX_DIMS: usize = 4;
/// Smallest run. Runs of a few bytes are allowed (a checkpoint's scalars
/// and scale slices), only costly in bulk.
pub const MIN_RUN: u32 = 1;

/// One dimension of a pattern.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dim {
    pub steps: Steps,
    pub src_stride: u64,
    pub dst_stride: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Steps {
    /// `0..n`.
    Count(u32),
    /// These indices, sorted and unique (mixed-K selections).
    Indices(Vec<u32>),
}

impl Steps {
    fn len(&self) -> u64 {
        match self {
            Steps::Count(n) => *n as u64,
            Steps::Indices(v) => v.len() as u64,
        }
    }
    /// The highest index used (None when empty).
    fn last(&self) -> Option<u64> {
        match self {
            Steps::Count(0) => None,
            Steps::Count(n) => Some(*n as u64 - 1),
            Steps::Indices(v) => v.last().map(|x| *x as u64),
        }
    }
    fn get(&self, i: u64) -> u64 {
        match self {
            Steps::Count(_) => i,
            Steps::Indices(v) => v[i as usize] as u64,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pattern {
    pub run: u32,
    pub dims: Vec<Dim>,
}

/// A pattern applied to one file for one rank.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// Index into the plan's files.
    pub file: u32,
    /// Index into the plan's patterns.
    pub pattern: u32,
    pub src_base: u64,
    pub dst_base: u64,
    pub rank: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Plan {
    /// Namespace paths (as under the mount), resolved by the hub at start.
    pub files: Vec<String>,
    pub patterns: Vec<Pattern>,
    pub entries: Vec<Entry>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    #[error("pattern {0}: {1}")]
    Pattern(u32, String),
    #[error("entry {0}: {1}")]
    Entry(usize, String),
    #[error("entries {0} and {1} write overlapping bytes of rank {2}")]
    Overlap(usize, usize, u32),
}

/// What a pattern covers: bytes copied, and its extent from the base on
/// each side (the last run's end).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Extent {
    pub bytes: u64,
    pub src_len: u64,
    pub dst_len: u64,
}

impl Pattern {
    /// Check the pattern's shape; its extent if valid.
    pub fn check(&self) -> Result<Extent, String> {
        if self.run < MIN_RUN {
            return Err(format!("run {} below {MIN_RUN} bytes", self.run));
        }
        if self.dims.len() > MAX_DIMS {
            return Err(format!("{} dims, at most {MAX_DIMS}", self.dims.len()));
        }
        if self
            .dims
            .iter()
            .filter(|d| matches!(d.steps, Steps::Indices(_)))
            .count()
            > 1
        {
            return Err("more than one index-list dimension".into());
        }
        // Innermost first: each stride must step past everything inside.
        let (mut src_inner, mut dst_inner) = (self.run as u64, self.run as u64);
        let mut bytes = self.run as u64;
        for (k, d) in self.dims.iter().enumerate().rev() {
            if let Steps::Indices(v) = &d.steps
                && v.windows(2).any(|w| w[0] >= w[1])
            {
                return Err(format!("dim {k}: indices not sorted and unique"));
            }
            let Some(last) = d.steps.last() else {
                return Ok(Extent {
                    bytes: 0,
                    src_len: 0,
                    dst_len: 0,
                });
            };
            if (last > 0 && d.src_stride < src_inner)
                || (d.steps.len() > 1 && d.dst_stride < dst_inner)
            {
                return Err(format!(
                    "dim {k}: strides {}/{} overlap the {src_inner}/{dst_inner} bytes inside",
                    d.src_stride, d.dst_stride
                ));
            }
            // The message is built only on failure: plans run to millions
            // of entries.
            let overflow = || format!("dim {k}: overflow");
            let mul = |a: u64, b: u64| a.checked_mul(b).ok_or_else(overflow);
            src_inner = mul(last, d.src_stride)?
                .checked_add(src_inner)
                .ok_or_else(overflow)?;
            // The destination steps by position, not by index value.
            dst_inner = mul(d.steps.len() - 1, d.dst_stride)?
                .checked_add(dst_inner)
                .ok_or_else(overflow)?;
            bytes = mul(bytes, d.steps.len())?;
        }
        Ok(Extent {
            bytes,
            src_len: src_inner,
            dst_len: dst_inner,
        })
    }

    /// Every run as (src offset, dst offset) from the bases, ascending in
    /// both. For tests and small plans; sources use [`RunWalk`].
    pub fn runs(&self) -> Vec<(u64, u64)> {
        let mut out = Vec::new();
        let mut idx = vec![0u64; self.dims.len()];
        if self.dims.iter().any(|d| d.steps.len() == 0) {
            return out;
        }
        loop {
            let (mut s, mut d) = (0u64, 0u64);
            for (k, dim) in self.dims.iter().enumerate() {
                s += dim.steps.get(idx[k]) * dim.src_stride;
                d += idx[k] * dim.dst_stride;
            }
            out.push((s, d));
            // Increment the innermost index, carrying outward.
            let mut k = self.dims.len();
            loop {
                if k == 0 {
                    return out;
                }
                k -= 1;
                idx[k] += 1;
                if idx[k] < self.dims[k].steps.len() {
                    break;
                }
                idx[k] = 0;
            }
            if self.dims.is_empty() {
                return out;
            }
        }
    }
}

/// What the hub checked, per rank and per file.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Totals {
    /// Bytes each rank receives.
    pub rank_bytes: Vec<u64>,
    /// Bytes read out of each file (sum over its entries).
    pub file_bytes: Vec<u64>,
    /// Bytes each file sends each rank: `file_rank_bytes[file * ranks + rank]`.
    pub file_rank_bytes: Vec<u64>,
}

impl Plan {
    /// Check the plan against file sizes and arena sizes (ADR-063 §3):
    /// every entry inside its file and its rank's arena, and no two entries
    /// of a rank writing the same bytes.
    pub fn check(&self, file_sizes: &[u64], arenas: &[u64]) -> Result<Totals, PlanError> {
        if file_sizes.len() != self.files.len() {
            return Err(PlanError::Entry(
                0,
                "file sizes do not match the plan".into(),
            ));
        }
        let extents: Vec<Extent> = self
            .patterns
            .iter()
            .enumerate()
            .map(|(i, p)| p.check().map_err(|e| PlanError::Pattern(i as u32, e)))
            .collect::<Result<_, _>>()?;
        let mut t = Totals {
            rank_bytes: vec![0; arenas.len()],
            file_bytes: vec![0; self.files.len()],
            file_rank_bytes: vec![0; self.files.len() * arenas.len()],
        };
        // (rank, dst start, dst end, entry) for the overlap check.
        let mut spans: Vec<(u32, u64, u64, usize)> = Vec::with_capacity(self.entries.len());
        for (i, e) in self.entries.iter().enumerate() {
            let bad = |m: String| PlanError::Entry(i, m);
            let ext = *extents
                .get(e.pattern as usize)
                .ok_or_else(|| bad(format!("no pattern {}", e.pattern)))?;
            let size = *file_sizes
                .get(e.file as usize)
                .ok_or_else(|| bad(format!("no file {}", e.file)))?;
            let arena = *arenas
                .get(e.rank as usize)
                .ok_or_else(|| bad(format!("no rank {}", e.rank)))?;
            if ext.bytes == 0 {
                continue;
            }
            let src_end = e
                .src_base
                .checked_add(ext.src_len)
                .ok_or_else(|| bad("overflow".into()))?;
            if src_end > size {
                return Err(bad(format!(
                    "reads to {src_end}, past the file's {size} bytes"
                )));
            }
            let dst_end = e
                .dst_base
                .checked_add(ext.dst_len)
                .ok_or_else(|| bad("overflow".into()))?;
            if dst_end > arena {
                return Err(bad(format!(
                    "writes to {dst_end}, past rank {}'s {arena}-byte arena",
                    e.rank
                )));
            }
            t.rank_bytes[e.rank as usize] += ext.bytes;
            t.file_bytes[e.file as usize] += ext.bytes;
            t.file_rank_bytes[e.file as usize * arenas.len() + e.rank as usize] += ext.bytes;
            spans.push((e.rank, e.dst_base, dst_end, i));
        }
        spans.sort_unstable();
        for w in spans.windows(2) {
            let (a, b) = (w[0], w[1]);
            if a.0 == b.0 && b.1 < a.2 {
                return Err(PlanError::Overlap(a.3, b.3, a.0));
            }
        }
        Ok(t)
    }
}

/// One file's entries, ordered so a source visits only those that read
/// each window: per window, the entries whose extent overlaps it, without
/// scanning the rest (a file of a TP plan holds tens of thousands of
/// entries; a 16 MiB window touches about a hundred).
pub struct FileEntries {
    /// (src start, src end, entry), by start.
    spans: Vec<(u64, u64, Entry)>,
    /// `max_end[i]`: the largest end among `spans[..=i]`.
    max_end: Vec<u64>,
}

impl FileEntries {
    /// Index `entries` (all of one file) by their extents; entries that
    /// read nothing or name a bad pattern are left out.
    pub fn new(entries: &[Entry], extents: &[Option<Extent>]) -> FileEntries {
        let mut spans: Vec<(u64, u64, Entry)> = entries
            .iter()
            .filter_map(|e| {
                let x = (*extents.get(e.pattern as usize)?)?;
                (x.bytes > 0).then(|| (e.src_base, e.src_base + x.src_len, e.clone()))
            })
            .collect();
        spans.sort_by_key(|s| s.0);
        let mut m = 0;
        let max_end = spans
            .iter()
            .map(|s| {
                m = m.max(s.1);
                m
            })
            .collect();
        FileEntries { spans, max_end }
    }

    /// Every entry's source range, by start.
    pub fn ranges(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.spans.iter().map(|s| (s.0, s.1))
    }

    /// The entries overlapping `[from, to)`, in index order. Windows asked
    /// in rising order cost O(log n + entries overlapping).
    pub fn overlapping(&self, from: u64, to: u64) -> impl Iterator<Item = &Entry> + '_ {
        // Skip the prefix that ends at or before `from` (max_end rises).
        let lo = self.max_end.partition_point(|&m| m <= from);
        self.spans[lo..]
            .iter()
            .take_while(move |s| s.0 < to)
            .filter(move |s| s.1 > from)
            .map(|s| &s.2)
    }
}

/// The runs of one entry that fall in `[from, to)` of its file, in file
/// order: (file offset, arena offset, length), each run clipped to the
/// window on both ends. Jumps to the first run at or after `from` without walking the
/// ones before (O(dims) per jump), so a source scanning its read buffers
/// pays only for runs it copies.
pub struct RunWalk<'a> {
    p: &'a Pattern,
    src_base: u64,
    dst_base: u64,
    idx: Vec<u64>,
    from: u64,
    to: u64,
    done: bool,
}

impl<'a> RunWalk<'a> {
    pub fn new(p: &'a Pattern, e: &Entry, from: u64, to: u64) -> RunWalk<'a> {
        let mut w = RunWalk {
            p,
            src_base: e.src_base,
            dst_base: e.dst_base,
            idx: vec![0; p.dims.len()],
            from,
            to,
            done: p.dims.iter().any(|d| d.steps.len() == 0),
        };
        if !w.done {
            w.seek(from);
        }
        w
    }

    /// Position on the first run ending after `at` (absolute file offset).
    fn seek(&mut self, at: u64) {
        let mut rel = at.saturating_sub(self.src_base);
        // Outermost first: pick the last step whose start is ≤ rel; the
        // remainder is relative to it.
        for k in 0..self.p.dims.len() {
            let d = &self.p.dims[k];
            let n = d.steps.len();
            // A zero stride (only possible with one step) stays at 0.
            let pos = match (rel.checked_div(d.src_stride), &d.steps) {
                (None, _) => 0,
                (Some(want), Steps::Count(_)) => want.min(n - 1),
                // Last index ≤ want, else the first.
                (Some(want), Steps::Indices(v)) => {
                    v.partition_point(|x| (*x as u64) <= want).saturating_sub(1) as u64
                }
            };
            self.idx[k] = pos;
            rel = rel.saturating_sub(d.steps.get(pos) * d.src_stride);
            // A step past the inner extent means rel overshoots: the next
            // step's first run is the answer; `next` skips runs that end
            // before `at` anyway.
        }
        self.skip_before(at);
    }

    fn offsets(&self) -> (u64, u64) {
        let (mut s, mut d) = (self.src_base, self.dst_base);
        for (k, dim) in self.p.dims.iter().enumerate() {
            s += dim.steps.get(self.idx[k]) * dim.src_stride;
            d += self.idx[k] * dim.dst_stride;
        }
        (s, d)
    }

    fn advance(&mut self) {
        let mut k = self.p.dims.len();
        loop {
            if k == 0 {
                self.done = true;
                return;
            }
            k -= 1;
            self.idx[k] += 1;
            if self.idx[k] < self.p.dims[k].steps.len() {
                return;
            }
            self.idx[k] = 0;
        }
    }

    fn skip_before(&mut self, at: u64) {
        while !self.done {
            let (s, _) = self.offsets();
            if s + self.p.run as u64 > at {
                return;
            }
            if self.p.dims.is_empty() {
                self.done = true;
                return;
            }
            self.advance();
        }
    }
}

impl Iterator for RunWalk<'_> {
    /// (file offset, arena offset, length).
    type Item = (u64, u64, u32);

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let (s, d) = self.offsets();
        if s >= self.to {
            self.done = true;
            return None;
        }
        let start = s.max(self.from);
        let end = (s + self.p.run as u64).min(self.to);
        if self.p.dims.is_empty() {
            self.done = true;
        } else {
            self.advance();
        }
        Some((start, d + (start - s), (end - start) as u32))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exl3_tp6() -> Pattern {
        // GLM K4 TP6 gate/up: per H128 unit, 1 KiB runs every 16 KiB over
        // 384 rows; units recur every 6 KiB within a row (2 per rank shown).
        Pattern {
            run: 1024,
            dims: vec![
                Dim {
                    steps: Steps::Count(384),
                    src_stride: 16 << 10,
                    dst_stride: 2048,
                },
                Dim {
                    steps: Steps::Count(2),
                    src_stride: 6 << 10,
                    dst_stride: 1024,
                },
            ],
        }
    }

    #[test]
    fn extent_matches_the_runs() {
        let p = exl3_tp6();
        let ext = p.check().unwrap();
        let runs = p.runs();
        assert_eq!(ext.bytes, runs.len() as u64 * 1024);
        let (s, d) = runs.last().unwrap();
        assert_eq!(ext.src_len, s + 1024);
        assert_eq!(ext.dst_len, d + 1024);
        // Packed on the destination side: contiguous.
        assert_eq!(ext.dst_len, ext.bytes);
    }

    #[test]
    fn overlapping_strides_are_refused() {
        let mut p = exl3_tp6();
        p.dims[1].src_stride = 512;
        assert!(p.check().unwrap_err().contains("overlap"));
        let mut p = exl3_tp6();
        p.dims[0].dst_stride = 1024;
        assert!(p.check().unwrap_err().contains("overlap"));
    }

    #[test]
    fn plan_checks_bounds_and_overlap() {
        let p = exl3_tp6();
        let ext = p.check().unwrap();
        let plan = Plan {
            files: vec!["/a".into()],
            patterns: vec![p],
            entries: vec![
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
                    src_base: 2048,
                    dst_base: 0,
                    rank: 1,
                },
            ],
        };
        let t = plan
            .check(&[ext.src_len + 2048], &[ext.dst_len, ext.dst_len])
            .unwrap();
        assert_eq!(t.rank_bytes, vec![ext.bytes, ext.bytes]);
        assert!(matches!(
            plan.check(&[ext.src_len], &[ext.dst_len; 2]),
            Err(PlanError::Entry(1, _))
        ));
        assert!(matches!(
            plan.check(&[ext.src_len + 2048], &[ext.dst_len - 1, ext.dst_len]),
            Err(PlanError::Entry(0, _))
        ));
        let mut both = plan.clone();
        both.entries[1].rank = 0;
        both.entries[1].dst_base = ext.dst_len - 1;
        assert!(matches!(
            both.check(&[ext.src_len + 2048], &[2 * ext.dst_len, 0]),
            Err(PlanError::Overlap(0, 1, 0))
        ));
    }

    #[test]
    fn walk_finds_the_runs_in_a_window() {
        let p = exl3_tp6();
        let e = Entry {
            file: 0,
            pattern: 0,
            src_base: 4096,
            dst_base: 100,
            rank: 0,
        };
        let all: Vec<(u64, u64)> = p
            .runs()
            .into_iter()
            .map(|(s, d)| (s + 4096, d + 100))
            .collect();
        for (from, to) in [
            (0, u64::MAX),
            (4096 + 7000, 4096 + 300_000),
            (5000, 5100),
            (4096 + 6144 + 500, 4096 + 6144 + 600),
        ] {
            let want: Vec<(u64, u64, u32)> = all
                .iter()
                .filter_map(|&(s, d)| {
                    let (a, b) = (s.max(from), (s + 1024).min(to));
                    (a < b).then(|| (a, d + (a - s), (b - a) as u32))
                })
                .collect();
            let got: Vec<(u64, u64, u32)> = RunWalk::new(&p, &e, from, to).collect();
            assert_eq!(got, want, "window {from}..{to}");
        }
    }

    /// Balanced EXL3 gate/up at TP6: each rank owns an irregular set of
    /// H128 units per row; one entry per rank lands them packed.
    #[test]
    fn index_lists_pack_into_the_destination() {
        let (rows, units, unit) = (8u32, 24u32, 1024u32);
        let owned: Vec<Vec<u32>> = (0..6u32)
            .map(|r| (0..units).filter(|u| (u * 7 + r) % 6 == 0).collect())
            .collect();
        let patterns: Vec<Pattern> = owned
            .iter()
            .map(|sel| Pattern {
                run: unit,
                dims: vec![
                    Dim {
                        steps: Steps::Count(rows),
                        src_stride: units as u64 * unit as u64,
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
        let row = units as u64 * unit as u64;
        for (r, p) in patterns.iter().enumerate() {
            let x = p.check().unwrap();
            // Packed: the extent is exactly the bytes.
            assert_eq!(x.dst_len, x.bytes, "rank {r}");
            assert_eq!(x.bytes, rows as u64 * owned[r].len() as u64 * unit as u64);
            // Each run comes from the right unit and lands at its position.
            for (k, (s, d)) in p.runs().into_iter().enumerate() {
                let (row_i, pos) = (k / owned[r].len(), k % owned[r].len());
                assert_eq!(s, row_i as u64 * row + owned[r][pos] as u64 * unit as u64);
                assert_eq!(d, k as u64 * unit as u64);
            }
        }
        // All six ranks' entries on one file: checked, no overlap per rank.
        let plan = Plan {
            files: vec!["/w".into()],
            entries: (0..6)
                .map(|r| Entry {
                    file: 0,
                    pattern: r,
                    src_base: 0,
                    dst_base: 0,
                    rank: r,
                })
                .collect(),
            patterns,
        };
        let arenas: Vec<u64> = owned
            .iter()
            .map(|o| rows as u64 * o.len() as u64 * unit as u64)
            .collect();
        let t = plan.check(&[rows as u64 * row], &arenas).unwrap();
        assert_eq!(t.rank_bytes, arenas);
        assert_eq!(t.file_bytes[0], rows as u64 * row);
    }

    #[test]
    fn tiny_runs_are_allowed() {
        let p = Pattern {
            run: 4,
            dims: vec![],
        };
        assert_eq!(p.check().unwrap().bytes, 4);
    }

    proptest::proptest! {
        /// For random valid patterns and windows, the walk yields exactly
        /// the runs overlapping the window.
        #[test]
        fn walk_agrees_with_the_full_list(
            run in 64u32..300,
            n0 in 1u32..6, g0 in 0u64..500,
            n1 in 1u32..6, g1 in 0u64..200,
            pick in proptest::collection::btree_set(0u32..8, 1..5),
            from in 0u64..20_000, len in 1u64..20_000,
        ) {
            let s1 = run as u64 + g1;
            let s0 = (n1 as u64 - 1) * s1 + run as u64 + g0;
            let idx: Vec<u32> = pick.into_iter().collect();
            let s2 = (*idx.last().unwrap() as u64 + 1) * s0 + 7;
            let p = Pattern {
                run,
                dims: vec![
                    Dim { steps: Steps::Count(n0), src_stride: s2, dst_stride: 1 << 30 },
                    Dim { steps: Steps::Indices(idx), src_stride: s0, dst_stride: 1 << 24 },
                    Dim { steps: Steps::Count(n1), src_stride: s1, dst_stride: run as u64 },
                ],
            };
            proptest::prop_assume!(p.check().is_ok());
            let e = Entry { file: 0, pattern: 0, src_base: 33, dst_base: 0, rank: 0 };
            let to = from + len;
            let want: Vec<(u64, u32)> = p
                .runs()
                .into_iter()
                .map(|(s, _)| s + 33)
                .filter(|s| s + run as u64 > from && *s < to)
                .map(|s| (s.max(from), ((s + run as u64).min(to) - s.max(from)) as u32))
                .collect();
            let got: Vec<(u64, u32)> = RunWalk::new(&p, &e, from, to).map(|(s, _, n)| (s, n)).collect();
            proptest::prop_assert_eq!(got, want);
        }

        /// The index yields exactly the entries a full scan finds
        /// overlapping each window, in any order of windows.
        #[test]
        fn file_entries_match_a_scan(
            ents in proptest::collection::vec((0u64..50_000, 0u32..3), 0..60),
            windows in proptest::collection::vec((0u64..60_000, 1u64..20_000), 1..10),
        ) {
            // Patterns of different extents: 1 KiB, 10 KiB, and empty.
            let ps = [
                Pattern { run: 1024, dims: vec![] },
                Pattern { run: 1024, dims: vec![Dim { steps: Steps::Count(10), src_stride: 1024, dst_stride: 1024 }] },
                Pattern { run: 1024, dims: vec![Dim { steps: Steps::Count(0), src_stride: 1024, dst_stride: 1024 }] },
            ];
            let extents: Vec<Option<Extent>> = ps.iter().map(|p| p.check().ok()).collect();
            let entries: Vec<Entry> = ents
                .iter()
                .enumerate()
                .map(|(i, &(src_base, pattern))| Entry {
                    file: 0,
                    pattern,
                    src_base,
                    dst_base: i as u64 * 20_000,
                    rank: 0,
                })
                .collect();
            let idx = FileEntries::new(&entries, &extents);
            for (from, len) in windows {
                let to = from + len;
                let mut want: Vec<u64> = entries
                    .iter()
                    .filter(|e| {
                        let x = extents[e.pattern as usize].unwrap();
                        x.bytes > 0 && e.src_base < to && e.src_base + x.src_len > from
                    })
                    .map(|e| e.dst_base)
                    .collect();
                let mut got: Vec<u64> = idx.overlapping(from, to).map(|e| e.dst_base).collect();
                want.sort();
                got.sort();
                proptest::prop_assert_eq!(got, want);
            }
        }
    }
}
