//! Per-handle sequential readahead over the RDMA fabric.
//!
//! Remote reads arrive from FUSE as ≤1 MiB requests, one at a time for a
//! sequential reader. The readahead turns them into whole-chunk fabric
//! reads kept in flight ahead of the reader: the window doubles while
//! access stays sequential (up to `max_window` chunks) and collapses to the
//! demanded chunk on random access. Fetched chunks stay in their registered
//! landing slots (no extra copy) until the reader moves past them.
//! Speculative chunks are only requested while the node has spare landing
//! slots, so a demanded read can always get one (PROPOSAL invariant 9).
//! Everything held here is transient: reading never creates a replica.

use nest_fabric::{Fabric, ReadBuf};
use nest_types::{FileId, Generation, NestError, NestResult, NodeId};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::task::JoinHandle;

enum Chunk {
    Pending(JoinHandle<NestResult<ReadBuf>>),
    Ready(ReadBuf),
}

pub(crate) struct Readahead {
    file: FileId,
    generation: Generation,
    source: NodeId,
    chunk: u64,
    window: usize,
    max_window: usize,
    /// End of the previous read.
    next: u64,
    eof: Option<u64>,
    chunks: BTreeMap<u64, Chunk>,
}

impl Readahead {
    /// `size` is the exact length when known (a STABLE generation never
    /// changes), so nothing is fetched past it.
    pub(crate) fn new(
        fabric: &Fabric,
        file: FileId,
        generation: Generation,
        source: NodeId,
        max_window: usize,
        size: Option<u64>,
    ) -> Readahead {
        Readahead {
            file,
            generation,
            source,
            chunk: fabric.chunk() as u64,
            window: 1,
            max_window: max_window.max(1),
            next: 0,
            eof: size,
            chunks: BTreeMap::new(),
        }
    }

    pub(crate) fn matches(&self, file: FileId, generation: Generation, source: NodeId) -> bool {
        self.file == file && self.generation == generation && self.source == source
    }

    fn fetch(&mut self, fabric: &Arc<Fabric>, start: u64) {
        if self.chunks.contains_key(&start) || self.eof.is_some_and(|e| start >= e) {
            return;
        }
        let (f, file, generation, source, len) = (
            fabric.clone(),
            self.file,
            self.generation,
            self.source,
            self.chunk as usize,
        );
        self.chunks.insert(
            start,
            Chunk::Pending(tokio::spawn(async move {
                f.read(source, file, generation, start, len).await
            })),
        );
    }

    async fn ready(&mut self, start: u64) -> NestResult<&ReadBuf> {
        if let Some(Chunk::Pending(_)) = self.chunks.get(&start) {
            let Some(Chunk::Pending(h)) = self.chunks.remove(&start) else {
                unreachable!()
            };
            let buf = h
                .await
                .map_err(|e| NestError::Io(format!("readahead task: {e}")))??;
            if (buf.len() as u64) < self.chunk {
                self.eof = Some(start + buf.len() as u64);
            }
            self.chunks.insert(start, Chunk::Ready(buf));
        }
        match self.chunks.get(&start) {
            Some(Chunk::Ready(b)) => Ok(b),
            _ => Err(NestError::Io("readahead chunk missing".into())),
        }
    }

    /// Read `[offset, offset+size)`, returning fewer bytes only at EOF.
    pub(crate) async fn read(
        &mut self,
        fabric: &Arc<Fabric>,
        offset: u64,
        size: u32,
    ) -> NestResult<Vec<u8>> {
        let c = self.chunk;
        let first = offset / c * c;
        let sequential = offset == self.next || self.chunks.contains_key(&first);
        self.window = if sequential {
            (self.window * 2).min(self.max_window)
        } else {
            1
        };
        self.next = offset + size as u64;
        // Drop chunks the reader has moved past.
        let stale: Vec<u64> = self.chunks.range(..first).map(|(k, _)| *k).collect();
        for k in stale {
            if let Some(Chunk::Pending(h)) = self.chunks.remove(&k) {
                h.abort();
            }
        }
        // Demanded chunks, then speculative ones while slots are spare.
        let last = (offset + size as u64).saturating_sub(1) / c * c;
        let mut s = first;
        while s <= last {
            self.fetch(fabric, s);
            s += c;
        }
        let reserve = fabric.landing_slots() / 4;
        for i in 1..self.window as u64 {
            if fabric.spare_landing() <= reserve {
                break;
            }
            self.fetch(fabric, last + i * c);
        }
        let mut out = Vec::with_capacity(size as usize);
        let mut pos = offset;
        let end = offset + size as u64;
        while pos < end {
            let start = pos / c * c;
            let buf = match self.ready(start).await {
                Ok(b) => b,
                Err(e) => {
                    // A failed chunk (e.g. Stale) poisons this state.
                    self.chunks.clear();
                    return Err(e);
                }
            };
            let from = (pos - start) as usize;
            if from >= buf.len() {
                break; // EOF
            }
            let n = (buf.len() - from).min((end - pos) as usize);
            out.extend_from_slice(&buf.as_slice()[from..from + n]);
            pos += n as u64;
            if buf.len() < c as usize {
                break;
            }
        }
        Ok(out)
    }
}

impl Drop for Readahead {
    fn drop(&mut self) {
        for (_, c) in std::mem::take(&mut self.chunks) {
            if let Chunk::Pending(h) = c {
                h.abort();
            }
        }
    }
}
