//! Registered slot pools: one region split into fixed-size slots, handed out
//! as exclusive `Slot`s and returned on drop. Bounded by construction:
//! acquiring waits when every slot is in use (PROPOSAL invariant 9).

use crate::verbs::{Context, Region};
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::Notify;

pub struct Pool {
    region: Region,
    slot_size: usize,
    /// Slot ids of this pool start here, so ids are unique across the
    /// tiers of a device (they key pending reads and name slots on the wire).
    base: u32,
    free: Mutex<Vec<u32>>,
    returned: Notify,
}

impl Pool {
    pub fn new(
        ctx: &Arc<Context>,
        slots: u32,
        slot_size: usize,
        remote_write: bool,
        base: u32,
    ) -> std::io::Result<Arc<Pool>> {
        let region = Region::new(ctx, slots as usize * slot_size, remote_write)?;
        Ok(Arc::new(Pool {
            region,
            slot_size,
            base,
            free: Mutex::new((0..slots).rev().collect()),
            returned: Notify::new(),
        }))
    }

    pub fn slot_size(&self) -> usize {
        self.slot_size
    }

    pub fn region(&self) -> &Region {
        &self.region
    }

    pub fn try_acquire(self: &Arc<Self>) -> Option<Slot> {
        self.free.lock().pop().map(|i| Slot {
            pool: self.clone(),
            index: i,
        })
    }

    pub async fn acquire(self: &Arc<Self>) -> Slot {
        loop {
            let notified = self.returned.notified();
            if let Some(s) = self.try_acquire() {
                return s;
            }
            notified.await;
        }
    }

    pub fn available(&self) -> usize {
        self.free.lock().len()
    }
}

/// Exclusive use of one pool slot until dropped.
pub struct Slot {
    pool: Arc<Pool>,
    index: u32,
}

impl Slot {
    /// Unique among the slots of every tier of its device.
    pub fn id(&self) -> u32 {
        self.pool.base + self.index
    }

    pub fn offset(&self) -> usize {
        self.index as usize * self.pool.slot_size
    }

    pub fn addr(&self) -> u64 {
        self.pool.region.addr() + self.offset() as u64
    }

    pub fn ptr(&self) -> *mut u8 {
        self.pool.region.ptr_at(self.offset())
    }

    pub fn lkey(&self) -> u32 {
        self.pool.region.lkey()
    }

    pub fn rkey(&self) -> u32 {
        self.pool.region.rkey()
    }

    pub fn capacity(&self) -> usize {
        self.pool.slot_size
    }

    /// The slot's bytes. Owning the `Slot` means no one else (including
    /// the NIC, once the transfer that filled it completed) touches them.
    pub fn as_slice(&self, len: usize) -> &[u8] {
        // SAFETY: exclusive ownership of the slot; len bounded by capacity.
        unsafe {
            self.pool
                .region
                .slice(self.offset(), len.min(self.pool.slot_size))
        }
    }

    pub fn as_mut_slice(&mut self, len: usize) -> &mut [u8] {
        // SAFETY: exclusive ownership of the slot; len bounded by capacity.
        unsafe {
            self.pool
                .region
                .slice_mut(self.offset(), len.min(self.pool.slot_size))
        }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.pool.free.lock().push(self.index);
        self.pool.returned.notify_one();
    }
}

/// A device's pools of one kind (landing or staging) in size tiers, smallest
/// first: a read takes a slot of the smallest tier it fits, so thousands of
/// page-sized reads can be in flight without pinning the 4 MiB slots bulk
/// reads need, and without registering gigabytes.
pub struct Tiers {
    pools: Vec<Arc<Pool>>,
}

impl Tiers {
    /// `tiers`: (slot size, slots), any order; ids are assigned from 0.
    pub fn new(
        ctx: &Arc<Context>,
        tiers: &[(usize, u32)],
        remote_write: bool,
    ) -> std::io::Result<Tiers> {
        let mut t: Vec<(usize, u32)> = tiers.iter().copied().filter(|(_, n)| *n > 0).collect();
        t.sort_by_key(|(size, _)| *size);
        let mut pools = Vec::new();
        let mut base = 0u32;
        for (size, n) in t {
            pools.push(Pool::new(ctx, n, size, remote_write, base)?);
            base += n;
        }
        Ok(Tiers { pools })
    }

    /// A free slot for `len` bytes: the smallest fitting tier with one free,
    /// else a larger one.
    pub fn try_acquire(&self, len: usize) -> Option<Slot> {
        self.pools
            .iter()
            .filter(|p| p.slot_size >= len)
            .find_map(|p| p.try_acquire())
    }

    /// A slot for `len` bytes, waiting for the smallest fitting tier when
    /// every fitting tier is full.
    pub async fn acquire(&self, len: usize) -> Slot {
        if let Some(s) = self.try_acquire(len) {
            return s;
        }
        self.pools
            .iter()
            .find(|p| p.slot_size >= len)
            .expect("the largest tier fits every read")
            .acquire()
            .await
    }

    /// The largest tier (bulk reads).
    pub fn big(&self) -> &Arc<Pool> {
        self.pools.last().expect("at least one tier")
    }
}
