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
    free: Mutex<Vec<u32>>,
    returned: Notify,
}

impl Pool {
    pub fn new(
        ctx: &Arc<Context>,
        slots: u32,
        slot_size: usize,
        remote_write: bool,
    ) -> std::io::Result<Arc<Pool>> {
        let region = Region::new(ctx, slots as usize * slot_size, remote_write)?;
        Ok(Arc::new(Pool {
            region,
            slot_size,
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
    pub fn index(&self) -> u32 {
        self.index
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
