use nest_meta::{Effect, query};
use nest_raft::{EffectHandler, MetaNode, SmEvent};
use nest_store::{ObjectKey, ObjectStore};
use nest_types::{FileId, Generation, NodeId, SessionId};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use tokio::sync::{Notify, mpsc, watch};

/// Object operations are sharded by file id so that operations on one file
/// run in apply order while different files proceed in parallel.
const SHARDS: usize = 8;

#[derive(Debug)]
enum ObjOp {
    /// Remove a fenced object (invalidated replica or dead working object).
    Delete(ObjectKey),
    /// Make the owner's working object exist: convert `from` in place or
    /// start empty.
    Prepare {
        key: ObjectKey,
        from: Option<ObjectKey>,
    },
}

#[derive(Default)]
pub(crate) struct State {
    pub(crate) session: Option<SessionId>,
    /// Local open handles per file (maintained by the FUSE frontend).
    pub(crate) open: HashMap<FileId, u32>,
    /// Orphans this node must release once its handles close.
    pub(crate) orphans_held: HashSet<FileId>,
    /// Orphans ready to be released in the next batch.
    pub(crate) release: Vec<FileId>,
    /// Objects that must not be served: deletion is in flight.
    pub(crate) fenced: HashSet<ObjectKey>,
    /// Working objects ready for writes.
    pub(crate) ready: HashSet<ObjectKey>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    pub kept: usize,
    pub deleted: Vec<ObjectKey>,
    /// Working objects recovered from the previous generation.
    pub rekeyed: Vec<ObjectKey>,
    /// Working objects that had to be recreated (content lost).
    pub recreated: Vec<ObjectKey>,
    /// Replicas metadata says we hold but the disk does not.
    pub missing: Vec<ObjectKey>,
}

pub struct DataNode {
    pub(crate) id: NodeId,
    pub(crate) store: Arc<ObjectStore>,
    pub(crate) meta: OnceLock<Arc<MetaNode>>,
    shards: Vec<mpsc::UnboundedSender<ObjOp>>,
    /// Workers hold operations while reconciliation runs.
    gate: watch::Sender<bool>,
    pub(crate) st: Mutex<State>,
    ready_notify: Notify,
    pub(crate) release_notify: Notify,
    pub(crate) stopping: watch::Sender<bool>,
    deletions: AtomicU64,
    /// Set once this node has passed a leader read barrier after start:
    /// before that its view may predate invalidations and it serves nothing.
    caught_up: std::sync::atomic::AtomicBool,
    weak: Weak<DataNode>,
}

struct Handler(Weak<DataNode>);

impl EffectHandler for Handler {
    fn on_event(&self, ev: &SmEvent) {
        if let Some(d) = self.0.upgrade() {
            d.on_event(ev);
        }
    }
}

impl DataNode {
    /// Create the data service and the effect handler to pass to
    /// [`MetaNode::start`]. Call [`DataNode::attach`] once the meta node
    /// is running.
    pub fn new(id: NodeId, store: Arc<ObjectStore>) -> (Arc<DataNode>, Arc<dyn EffectHandler>) {
        let (gate, _) = watch::channel(false);
        let (stopping, _) = watch::channel(false);
        let mut senders = Vec::with_capacity(SHARDS);
        let mut receivers = Vec::with_capacity(SHARDS);
        for _ in 0..SHARDS {
            let (tx, rx) = mpsc::unbounded_channel();
            senders.push(tx);
            receivers.push(rx);
        }
        let d = Arc::new_cyclic(|weak| DataNode {
            weak: weak.clone(),
            id,
            store,
            meta: OnceLock::new(),
            shards: senders,
            gate,
            st: Mutex::new(State::default()),
            ready_notify: Notify::new(),
            release_notify: Notify::new(),
            stopping,
            deletions: AtomicU64::new(0),
            caught_up: std::sync::atomic::AtomicBool::new(false),
        });
        for rx in receivers {
            tokio::spawn(worker(Arc::downgrade(&d), rx, d.gate.subscribe()));
        }
        let h: Arc<dyn EffectHandler> = Arc::new(Handler(Arc::downgrade(&d)));
        (d, h)
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn store(&self) -> &Arc<ObjectStore> {
        &self.store
    }

    pub fn meta(&self) -> &Arc<MetaNode> {
        self.meta.get().expect("DataNode::attach not called")
    }

    /// Number of object deletions completed (for tests and metrics).
    pub fn deletions(&self) -> u64 {
        self.deletions.load(Ordering::Relaxed)
    }

    pub fn session(&self) -> Option<SessionId> {
        self.st.lock().session
    }

    /// True if `key` may be served from this node's store right now: the
    /// node is caught up, committed metadata lists it as a live replica
    /// here, it is not fenced for deletion, and it exists.
    pub fn servable(&self, key: ObjectKey) -> bool {
        if !self.caught_up() || self.st.lock().fenced.contains(&key) {
            return false;
        }
        let Some(meta) = self.meta.get() else {
            return false;
        };
        let live = meta
            .open_reader()
            .and_then(|c| {
                query::has_live_replica(&c, key.file, key.generation, self.id.live_store())
            })
            .unwrap_or(false);
        live && self.store.exists(key)
    }

    /// Whether this node has confirmed its metadata is current since start.
    pub fn caught_up(&self) -> bool {
        self.caught_up.load(Ordering::SeqCst)
    }

    /// Wire up the meta node, reconcile local objects with committed
    /// metadata, open the workers, and start background duties.
    pub async fn attach(self: &Arc<Self>, meta: Arc<MetaNode>) -> anyhow::Result<ReconcileReport> {
        meta.wait_startup_replay().await?;
        let _ = self.meta.set(meta);
        let report = self.reconcile().await?;
        let _ = self.gate.send(true);
        self.clone().spawn_catch_up();
        self.clone().spawn_startup_duties(report.clone());
        crate::session::spawn(self.clone());
        Ok(report)
    }

    pub fn shutdown(&self) {
        let _ = self.stopping.send(true);
    }

    fn shard(&self, file: FileId) -> &mpsc::UnboundedSender<ObjOp> {
        &self.shards[(file.0 as usize) % SHARDS]
    }

    fn submit(&self, op: ObjOp) {
        let file = match &op {
            ObjOp::Delete(k) | ObjOp::Prepare { key: k, .. } => k.file,
        };
        let _ = self.shard(file).send(op);
    }

    fn on_event(&self, ev: &SmEvent) {
        match ev {
            SmEvent::Applied { effects, .. } => {
                for e in effects {
                    self.on_effect(e);
                }
            }
            SmEvent::Resync { .. } => {
                // State was replaced wholesale; reconcile off the apply path
                // with the workers held.
                if self.meta.get().is_none() {
                    return; // still starting: attach() reconciles anyway
                }
                let weak = self.weak.clone();
                let _ = self.gate.send(false);
                tokio::spawn(async move {
                    let Some(d) = weak.upgrade() else { return };
                    match d.reconcile().await {
                        Ok(r) => {
                            tracing::info!(?r, "reconciled after snapshot install");
                            let _ = d.gate.send(true);
                            d.retire_missing(r.missing).await;
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "reconcile after snapshot failed");
                            let _ = d.gate.send(true);
                        }
                    }
                });
            }
        }
    }

    fn on_effect(&self, e: &Effect) {
        let mine = self.id.live_store();
        match e {
            Effect::ReplicaInvalidated {
                file,
                generation,
                store,
            } if *store == mine => {
                let key = ObjectKey::new(*file, *generation);
                self.st.lock().fenced.insert(key);
                self.submit(ObjOp::Delete(key));
            }
            Effect::WorkingObjectDeleted {
                file,
                generation,
                owner,
            } if *owner == self.id => {
                let key = ObjectKey::new(*file, *generation);
                let mut st = self.st.lock();
                st.fenced.insert(key);
                st.ready.remove(&key);
                drop(st);
                self.submit(ObjOp::Delete(key));
            }
            Effect::OwnershipGranted {
                file,
                owner,
                generation,
                from_gen,
                ..
            } if *owner == self.id => {
                let key = ObjectKey::new(*file, *generation);
                let from = from_gen.map(|g| ObjectKey::new(*file, g));
                self.submit(ObjOp::Prepare { key, from });
            }
            Effect::Orphaned { file, sessions } => {
                let mut st = self.st.lock();
                if st.session.is_some_and(|s| sessions.contains(&s)) {
                    if st.open.get(file).copied().unwrap_or(0) == 0 {
                        st.release.push(*file);
                        drop(st);
                        self.release_notify.notify_one();
                    } else {
                        st.orphans_held.insert(*file);
                    }
                }
            }
            Effect::FileDeleted { file } => {
                let mut st = self.st.lock();
                st.orphans_held.remove(file);
            }
            Effect::SessionExpired { session, .. } => {
                let mut st = self.st.lock();
                if st.session == Some(*session) {
                    // The cluster gave up on us (partition or restart race).
                    st.session = None;
                    drop(st);
                    self.release_notify.notify_one();
                }
            }
            _ => {}
        }
    }

    /// Run one object operation. Idempotent: reconciliation and replayed
    /// effects may repeat work.
    fn run_op(&self, op: ObjOp) {
        match op {
            ObjOp::Delete(key) => {
                match self.store.delete(key) {
                    Ok(_) => {
                        self.deletions.fetch_add(1, Ordering::Relaxed);
                        self.st.lock().fenced.remove(&key);
                    }
                    // Stays fenced; startup reconciliation retries.
                    Err(e) => {
                        tracing::error!(?key, error = %e, "deleting invalidated object failed")
                    }
                }
            }
            ObjOp::Prepare { key, from } => {
                let r = if self.store.exists(key) {
                    Ok(())
                } else if let Some(from) = from.filter(|f| self.store.exists(*f)) {
                    self.store.rekey(from, key)
                } else {
                    self.store.create(key).map(|_| ())
                };
                match r {
                    Ok(()) => {
                        self.st.lock().ready.insert(key);
                        self.ready_notify.notify_waiters();
                    }
                    Err(e) => tracing::error!(?key, error = %e, "preparing working object failed"),
                }
            }
        }
    }

    /// A local handle to `file` was opened (FUSE open/create).
    pub fn handle_opened(&self, file: FileId) {
        *self.st.lock().open.entry(file).or_default() += 1;
    }

    /// A local handle to `file` was released. The last close of an
    /// orphaned file releases it cluster-wide.
    pub fn handle_closed(&self, file: FileId) {
        let mut st = self.st.lock();
        let n = st.open.entry(file).or_default();
        *n = n.saturating_sub(1);
        if *n == 0 {
            st.open.remove(&file);
            if st.orphans_held.remove(&file) {
                st.release.push(file);
                drop(st);
                self.release_notify.notify_one();
            }
        }
    }

    /// Wait until the working object for `key` is ready for writes.
    pub async fn wait_ready(&self, key: ObjectKey) {
        loop {
            let notified = self.ready_notify.notified();
            if self.st.lock().ready.contains(&key) {
                return;
            }
            notified.await;
        }
    }

    /// Make the object directory agree with committed metadata. Runs with
    /// workers held so no queued operation interleaves with the scan.
    pub async fn reconcile(&self) -> anyhow::Result<ReconcileReport> {
        let meta = self.meta().clone();
        let store = self.store.clone();
        let me = self.id;
        let report =
            tokio::task::spawn_blocking(move || reconcile_blocking(&meta, &store, me)).await??;
        let mut st = self.st.lock();
        for k in &report.deleted {
            st.fenced.remove(k);
        }
        let owned = {
            let c = self.meta().open_reader()?;
            query::owned_by(&c, me)?
        };
        for a in owned {
            st.ready.insert(ObjectKey::new(a.id, a.generation));
        }
        drop(st);
        self.ready_notify.notify_waiters();
        Ok(report)
    }

    /// Pass a leader read barrier: afterwards every invalidation committed
    /// before we started has been applied here (and fenced).
    fn spawn_catch_up(self: Arc<Self>) {
        let weak = self.weak.clone();
        drop(self);
        tokio::spawn(async move {
            loop {
                let Some(d) = weak.upgrade() else { return };
                if *d.stopping.borrow() {
                    return;
                }
                if d.meta().barrier().await.is_ok() {
                    d.caught_up.store(true, Ordering::SeqCst);
                    tracing::info!("caught up with the cluster; serving");
                    return;
                }
                drop(d);
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        });
    }

    /// Metadata says we hold these copies but the disk does not: stop
    /// advertising them.
    async fn retire_missing(&self, missing: Vec<ObjectKey>) {
        for k in missing {
            tracing::error!(?k, "replica missing from local store; retiring it");
            let r = self
                .meta()
                .propose(nest_meta::Command::RetireReplica {
                    file: k.file,
                    generation: k.generation,
                    store: self.id.live_store(),
                    allow_last: true,
                })
                .await;
            if let Err(e) = r {
                tracing::warn!(?k, error = %e, "retiring missing replica failed");
            }
        }
    }

    /// After startup: report lost replicas and settle ownerships whose
    /// writers died with the previous process.
    fn spawn_startup_duties(self: Arc<Self>, report: ReconcileReport) {
        tokio::spawn(async move {
            let meta = self.meta().clone();
            self.retire_missing(report.missing).await;
            let owned = match meta
                .open_reader()
                .and_then(|c| query::owned_by(&c, self.id))
            {
                Ok(o) => o,
                Err(e) => {
                    tracing::error!(error = %e, "reading owned files failed");
                    return;
                }
            };
            for a in owned {
                let key = ObjectKey::new(a.id, a.generation);
                let (size, mtime) = match self.store.stat(key) {
                    Ok(s) => s,
                    Err(_) => (a.size, a.mtime),
                };
                let now = nest_types::Timestamp::now();
                let r = meta
                    .propose(nest_meta::Command::Finalize {
                        file: a.id,
                        epoch: a.epoch,
                        size,
                        mtime,
                        now,
                    })
                    .await;
                tracing::info!(file = %a.id, ?r, "finalized ownership left by previous run");
            }
        });
    }
}

fn reconcile_blocking(
    meta: &MetaNode,
    store: &ObjectStore,
    me: NodeId,
) -> anyhow::Result<ReconcileReport> {
    let c = meta.open_reader()?;
    let expected: HashSet<ObjectKey> = query::store_inventory(&c, me.live_store())?
        .into_iter()
        .map(|(f, g)| ObjectKey::new(f, g))
        .collect();
    let owned = query::owned_by(&c, me)?;
    drop(c);
    let disk: HashSet<ObjectKey> = store.scan()?.into_iter().map(|o| o.key).collect();
    let mut report = ReconcileReport::default();
    let mut keep: HashSet<ObjectKey> = HashSet::new();
    for a in &owned {
        let key = ObjectKey::new(a.id, a.generation);
        keep.insert(key);
        if disk.contains(&key) {
            continue;
        }
        // Crash between committing ownership and converting the object:
        // the previous generation's copy is the content (truncated to the
        // recorded size, which is 0 after a truncating acquire).
        let prev = ObjectKey::new(a.id, Generation(a.generation.0.saturating_sub(1)));
        if a.generation.0 > 1 && disk.contains(&prev) && !expected.contains(&prev) {
            store.rekey(prev, key)?;
            store.open_write(key)?.set_len(a.size)?;
            report.rekeyed.push(key);
        } else {
            let f = store.create(key)?;
            f.set_len(a.size)?;
            if a.size > 0 {
                tracing::error!(file = %a.id, generation = %a.generation, "working object lost; recreated sparse");
            }
            report.recreated.push(key);
        }
    }
    for k in &disk {
        if expected.contains(k) || keep.contains(k) {
            report.kept += 1;
        } else if !report
            .rekeyed
            .iter()
            .any(|r| r.file == k.file && r.generation.0 == k.generation.0 + 1)
        {
            store.delete(*k)?;
            report.deleted.push(*k);
        }
    }
    for k in &expected {
        if !disk.contains(k) {
            report.missing.push(*k);
        }
    }
    report.deleted.sort();
    report.missing.sort();
    Ok(report)
}

async fn worker(
    d: Weak<DataNode>,
    mut rx: mpsc::UnboundedReceiver<ObjOp>,
    mut gate: watch::Receiver<bool>,
) {
    while let Some(op) = rx.recv().await {
        if gate.wait_for(|open| *open).await.is_err() {
            return;
        }
        let Some(node) = d.upgrade() else { return };
        // Unlinking a large file can take a while; keep it off the runtime.
        if tokio::task::spawn_blocking(move || node.run_op(op))
            .await
            .is_err()
        {
            return;
        }
    }
}
