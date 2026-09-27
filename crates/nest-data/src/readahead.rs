//! Per-handle sequential readahead, from one copy or several (ADR-030).
//!
//! Reads arrive from FUSE as ≤1 MiB requests, one at a time for a
//! sequential reader. The readahead turns them into whole-chunk reads kept
//! in flight ahead of the reader: the window doubles while access stays
//! sequential (up to `max_window` chunks) and collapses to the demanded
//! chunk on random access.
//!
//! Each chunk comes from the source the node's `Balancer` picks among the
//! copies given: this host's disk (a pread on the blocking pool) or a holder
//! over the fabric. A source that fails a chunk (its copy gone, a peer
//! down) is dropped for this stream and the chunk is fetched elsewhere.
//! Fabric chunks are copied out of their registered landing slot on arrival
//! (holding slots until the reader got there starved new requests);
//! speculative fabric chunks are still only requested while the node has
//! spare slots, so a demanded read can always get one (PROPOSAL
//! invariant 9). Everything held here is transient:
//! reading never creates a replica.

use crate::balance::{Balancer, Source};
use nest_fabric::Fabric;
use nest_store::{ObjectKey, ObjectStore};
use nest_types::{FileId, Generation, NestError, NestResult};
use std::collections::BTreeMap;
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use tokio::task::JoinHandle;

/// A fetched chunk. Fabric chunks are copied out of their landing slot as
/// soon as they arrive: a slot held until the reader got there (chunks from
/// other hosts arrive ahead of the local ones) starved new requests of
/// slots, and waiting for one dominated the latency of remote chunks.
struct Buf(Vec<u8>);

impl Buf {
    fn as_slice(&self) -> &[u8] {
        &self.0
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}

enum Chunk {
    Pending(Source, JoinHandle<NestResult<Buf>>),
    Ready(Source, Buf),
}

pub(crate) struct Readahead {
    file: FileId,
    generation: Generation,
    /// Where copies are; `Local` reads from `local`.
    sources: Vec<Source>,
    local: Option<Arc<ObjectStore>>,
    balancer: Arc<Balancer>,
    chunk: u64,
    window: usize,
    max_window: usize,
    /// End of the previous read.
    next: u64,
    eof: Option<u64>,
    chunks: BTreeMap<u64, Chunk>,
    /// Bytes handed out, by origin (local, remote), for usage accounting.
    served: [u64; 2],
    /// Captured at creation: fetches may be started from a FUSE thread that
    /// is not inside the runtime (fast path).
    rt: tokio::runtime::Handle,
}

impl Readahead {
    /// `size` is the exact length when known (a STABLE generation never
    /// changes), so nothing is fetched past it. `sources` is non-empty;
    /// `local` must be given when it includes `Source::Local`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        fabric: &Fabric,
        balancer: Arc<Balancer>,
        file: FileId,
        generation: Generation,
        sources: Vec<Source>,
        local: Option<Arc<ObjectStore>>,
        max_window: usize,
        size: Option<u64>,
    ) -> Readahead {
        Readahead {
            file,
            generation,
            sources,
            local,
            balancer,
            chunk: fabric.chunk() as u64,
            window: 1,
            max_window: max_window.max(1),
            next: 0,
            eof: size,
            chunks: BTreeMap::new(),
            served: [0, 0],
            rt: tokio::runtime::Handle::current(),
        }
    }

    pub(crate) fn matches(&self, file: FileId, generation: Generation) -> bool {
        self.file == file && self.generation == generation
    }

    /// Bytes served since the last call, (local, remote).
    pub(crate) fn take_served(&mut self) -> (u64, u64) {
        let s = self.served;
        self.served = [0, 0];
        (s[0], s[1])
    }

    /// Start fetching the chunk at `start`. A speculative chunk may use a
    /// fabric landing slot only while slots are spare: fetched chunks hold
    /// their slot until the reader consumes them, so speculation must never
    /// take the slots a demanded read needs (PROPOSAL invariant 9; without
    /// this, many readers reading ahead deadlock). With slots short it goes
    /// to the local disk if there is a copy, else waits.
    fn fetch(&mut self, fabric: &Arc<Fabric>, start: u64, speculative: bool) {
        if self.chunks.contains_key(&start) || self.eof.is_some_and(|e| start >= e) {
            return;
        }
        let slots_short = fabric.spare_landing() <= fabric.landing_slots() / 4;
        let local_only = [Source::Local];
        let candidates: &[Source] = if speculative && slots_short {
            if !self.sources.contains(&Source::Local) {
                return;
            }
            &local_only
        } else {
            &self.sources
        };
        // Stripes of 32 MiB: long sequential reads for the serving disk, yet
        // enough of them to spread a model over every copy.
        let stripe = start / (8 * self.chunk);
        let key = self.file.0.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ stripe;
        let ticket = self.balancer.pick(candidates, key);
        let src = ticket.source();
        let (file, generation, len) = (self.file, self.generation, self.chunk as usize);
        let task = match src {
            Source::Local => {
                let store = self.local.clone();
                self.rt.spawn(async move {
                    let r = tokio::task::spawn_blocking(move || -> NestResult<Vec<u8>> {
                        let store =
                            store.ok_or_else(|| NestError::Unavailable("no local copy".into()))?;
                        let f = store
                            .open_read(ObjectKey::new(file, generation))
                            .map_err(|e| match e.kind() {
                                std::io::ErrorKind::NotFound => NestError::Stale,
                                _ => NestError::Io(e.to_string()),
                            })?;
                        let mut buf = vec![0u8; len];
                        let mut got = 0;
                        while got < len {
                            let n = f
                                .read_at(&mut buf[got..], start + got as u64)
                                .map_err(|e| NestError::Io(e.to_string()))?;
                            if n == 0 {
                                break;
                            }
                            got += n;
                        }
                        buf.truncate(got);
                        Ok(buf)
                    })
                    .await
                    .map_err(|e| NestError::Io(e.to_string()))
                    .and_then(|r| r);
                    match r {
                        Ok(buf) => {
                            ticket.finish(buf.len() as u64);
                            Ok(Buf(buf))
                        }
                        Err(e) => {
                            ticket.fail();
                            Err(e)
                        }
                    }
                })
            }
            Source::Peer(node) => {
                let f = fabric.clone();
                self.rt.spawn(async move {
                    match f.read(node, file, generation, start, len).await {
                        Ok(b) => {
                            let v = b.as_slice().to_vec();
                            drop(b); // the landing slot is free again
                            ticket.finish(v.len() as u64);
                            Ok(Buf(v))
                        }
                        Err(e) => {
                            ticket.fail();
                            Err(e)
                        }
                    }
                })
            }
        };
        self.chunks.insert(start, Chunk::Pending(src, task));
    }

    async fn ready(&mut self, fabric: &Arc<Fabric>, start: u64) -> NestResult<()> {
        while let Some(Chunk::Pending(..)) = self.chunks.get(&start) {
            let Some(Chunk::Pending(src, h)) = self.chunks.remove(&start) else {
                unreachable!()
            };
            match h
                .await
                .map_err(|e| NestError::Io(format!("readahead task: {e}")))?
            {
                Ok(buf) => {
                    if (buf.len() as u64) < self.chunk {
                        self.eof = Some(start + buf.len() as u64);
                    }
                    self.chunks.insert(start, Chunk::Ready(src, buf));
                }
                // That copy failed; others may still serve.
                Err(e) if self.sources.len() > 1 => {
                    tracing::debug!(file = self.file.0, ?src, error = %e, "chunk source failed; trying another copy");
                    self.sources.retain(|s| *s != src);
                    self.fetch(fabric, start, false);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn account(&mut self, src: Source, n: usize) {
        self.served[matches!(src, Source::Peer(_)) as usize] += n as u64;
    }

    /// Serve `[offset, offset+size)` only if every byte is already in ready
    /// chunks (no waiting). Used on the FUSE thread to avoid thread hops.
    pub(crate) fn try_ready(&mut self, offset: u64, size: u32) -> Option<Vec<u8>> {
        let c = self.chunk;
        let end = self
            .eof
            .map_or(offset + size as u64, |e| e.min(offset + size as u64));
        if end <= offset {
            return (self.eof.is_some_and(|e| offset >= e)).then(Vec::new);
        }
        let mut out = Vec::with_capacity((end - offset) as usize);
        let mut pos = offset;
        let mut taken = Vec::new();
        while pos < end {
            let start = pos / c * c;
            let Some(Chunk::Ready(src, buf)) = self.chunks.get(&start) else {
                return None;
            };
            let from = (pos - start) as usize;
            if from >= buf.len() {
                break;
            }
            let n = (buf.len() - from).min((end - pos) as usize);
            out.extend_from_slice(&buf.as_slice()[from..from + n]);
            taken.push((*src, n));
            pos += n as u64;
            if buf.len() < c as usize {
                break;
            }
        }
        for (src, n) in taken {
            self.account(src, n);
        }
        // Keep the pipeline moving exactly as a full read would.
        let sequential = offset == self.next;
        self.window = if sequential {
            (self.window * 2).min(self.max_window)
        } else {
            self.window
        };
        self.next = offset + out.len() as u64;
        Some(out)
    }

    /// Top up the window after `[offset, offset+size)` with speculative
    /// chunks (fabric ones only while landing slots are spare).
    fn top_up(&mut self, fabric: &Arc<Fabric>, last: u64) {
        let c = self.chunk;
        for i in 1..self.window as u64 {
            self.fetch(fabric, last + i * c, true);
        }
    }

    /// After a fast-path hit: drop consumed chunks and top up the window.
    pub(crate) fn advance(&mut self, fabric: &Arc<Fabric>, offset: u64, size: u32) {
        let c = self.chunk;
        let first = offset / c * c;
        let stale: Vec<u64> = self.chunks.range(..first).map(|(k, _)| *k).collect();
        for k in stale {
            self.chunks.remove(&k);
        }
        let last = (offset + size as u64).saturating_sub(1) / c * c;
        self.top_up(fabric, last);
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
            if let Some(Chunk::Pending(_, h)) = self.chunks.remove(&k) {
                h.abort();
            }
        }
        // Demanded chunks, then speculative ones.
        let last = (offset + size as u64).saturating_sub(1) / c * c;
        let mut s = first;
        while s <= last {
            self.fetch(fabric, s, false);
            s += c;
        }
        self.top_up(fabric, last);
        let mut out = Vec::with_capacity(size as usize);
        let mut pos = offset;
        let end = offset + size as u64;
        while pos < end {
            let start = pos / c * c;
            if let Err(e) = self.ready(fabric, start).await {
                // A chunk no copy could serve (e.g. Stale) poisons this state.
                self.chunks.clear();
                return Err(e);
            }
            let Some(Chunk::Ready(src, buf)) = self.chunks.get(&start) else {
                return Err(NestError::Io("readahead chunk missing".into()));
            };
            let from = (pos - start) as usize;
            if from >= buf.len() {
                break; // EOF
            }
            let n = (buf.len() - from).min((end - pos) as usize);
            let full = buf.len() == c as usize;
            out.extend_from_slice(&buf.as_slice()[from..from + n]);
            let src = *src;
            self.account(src, n);
            pos += n as u64;
            if !full {
                break;
            }
        }
        Ok(out)
    }
}

impl Drop for Readahead {
    fn drop(&mut self) {
        for (_, c) in std::mem::take(&mut self.chunks) {
            if let Chunk::Pending(_, h) = c {
                h.abort();
            }
        }
    }
}
