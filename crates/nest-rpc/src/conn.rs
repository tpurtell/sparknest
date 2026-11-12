use crate::path::{PathState, Paths, StandbyConfig};
use crate::{Filter, RpcError, auth};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::future::BoxFuture;
use nest_types::NodeId;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};

const MAX_FRAME: usize = 256 << 20;
const KIND_REQ: u8 = 0;
const KIND_OK: u8 = 1;
const KIND_ERR: u8 = 2;

/// Marks a connection as the dialler's standby (ADR-057): the first
/// request on it, sent right after authentication. A peer that predates
/// standbys answers `NoService` and the dialler drops the connection, so
/// nothing changes for it; the primary connection never carries one.
pub(crate) const SVC_STANDBY: u8 = 250;
/// A liveness probe, answered by the connection's reader itself (never a
/// handler), so a busy host answers at once. Peers that predate it answer
/// `NoService` from their reader just as fast, which serves equally well.
pub(crate) const SVC_PING: u8 = 251;

#[derive(Clone, Debug)]
pub struct RpcConfig {
    pub node: NodeId,
    pub cluster: String,
    pub secret: Vec<u8>,
    pub listen: SocketAddr,
    pub connect_timeout: Duration,
}

/// A service implementation. Errors are returned to the caller as
/// [`RpcError::Remote`].
pub trait Handler: Send + Sync + 'static {
    fn call(&self, peer: NodeId, body: Bytes) -> BoxFuture<'static, Result<Bytes, String>>;
}

impl<F> Handler for F
where
    F: Fn(NodeId, Bytes) -> BoxFuture<'static, Result<Bytes, String>> + Send + Sync + 'static,
{
    fn call(&self, peer: NodeId, body: Bytes) -> BoxFuture<'static, Result<Bytes, String>> {
        self(peer, body)
    }
}

/// Handle to this node's RPC endpoint: server for incoming requests and
/// client pool for outgoing ones. Cheap to clone.
#[derive(Clone)]
pub struct Rpc {
    pub(crate) inner: Arc<Inner>,
}

pub(crate) struct Inner {
    pub(crate) cfg: RpcConfig,
    pub(crate) standby: StandbyConfig,
    local_addr: SocketAddr,
    handlers: parking_lot::RwLock<HashMap<u8, Arc<dyn Handler>>>,
    addrs: parking_lot::RwLock<HashMap<NodeId, SocketAddr>>,
    pinned: parking_lot::RwLock<HashMap<NodeId, SocketAddr>>,
    conns: Mutex<HashMap<NodeId, ConnSlot>>,
    /// Standby addresses and each such peer's path state (ADR-057).
    pub(crate) paths: Paths,
    /// Primary dials in progress to peers with a standby, shared by every
    /// request waiting for one.
    dials: Mutex<HashMap<NodeId, SharedDial>>,
    pub(crate) filter: Filter,
    pub(crate) shutdown: watch::Sender<bool>,
}

struct Frame {
    id: u64,
    kind: u8,
    service: u8,
    body: Bytes,
}

async fn write_frame(w: &mut BufWriter<OwnedWriteHalf>, f: &Frame) -> std::io::Result<()> {
    let mut hdr = BytesMut::with_capacity(14);
    hdr.put_u32_le((10 + f.body.len()) as u32);
    hdr.put_u64_le(f.id);
    hdr.put_u8(f.kind);
    hdr.put_u8(f.service);
    w.write_all(&hdr).await?;
    w.write_all(&f.body).await
}

async fn read_frame(r: &mut OwnedReadHalf) -> std::io::Result<Frame> {
    let len = r.read_u32_le().await? as usize;
    if !(10..=MAX_FRAME).contains(&len) {
        return Err(std::io::Error::other(format!("bad frame length {len}")));
    }
    let mut buf = BytesMut::zeroed(len);
    r.read_exact(&mut buf).await?;
    let mut hdr = &buf[..10];
    let id = hdr.get_u64_le();
    let kind = hdr.get_u8();
    let service = hdr.get_u8();
    let body = buf.freeze().slice(10..);
    Ok(Frame {
        id,
        kind,
        service,
        body,
    })
}

/// Writer task: drains frames and flushes when the queue is momentarily
/// empty, so bursts coalesce into few syscalls. Stops when the reader does
/// (`closed`), which closes the socket, so the peer sees the end at once.
async fn writer(
    mut w: BufWriter<OwnedWriteHalf>,
    mut rx: mpsc::Receiver<Frame>,
    closed: Arc<tokio::sync::Notify>,
) {
    loop {
        let f = tokio::select! {
            f = rx.recv() => f,
            _ = closed.notified() => None,
        };
        let Some(f) = f else { return };
        if write_frame(&mut w, &f).await.is_err() {
            return;
        }
        while let Ok(f) = rx.try_recv() {
            if write_frame(&mut w, &f).await.is_err() {
                return;
            }
        }
        if w.flush().await.is_err() {
            return;
        }
    }
}

/// Per-peer slot; the async mutex serializes (re)connection attempts.
type ConnSlot = Arc<tokio::sync::Mutex<Option<Arc<Conn>>>>;

type Pending = Mutex<HashMap<u64, oneshot::Sender<Result<Bytes, RpcError>>>>;

type SharedDial = futures::future::Shared<BoxFuture<'static, Result<Arc<Conn>, RpcError>>>;

/// How long a connection has gone without a word from the peer while we
/// wait on it (ADR-057). `since` is when the first request was sent after
/// anything last arrived; it clears whenever any frame arrives.
#[derive(Default)]
struct Quiet {
    since: Option<Instant>,
    /// A probe was sent during this silence.
    probed: bool,
}

/// An authenticated connection to one peer. Either end may send requests
/// on it (ADR-047): each numbers its own, and the frame kind tells a
/// request from the other end apart from an answer to one of ours. A node
/// dials the peers it has an address for; a peer it cannot reach (behind
/// NAT) dials in, and its connection is used for both directions.
pub(crate) struct Conn {
    peer: NodeId,
    tx: mpsc::Sender<Frame>,
    pending: Arc<Pending>,
    next_id: AtomicU64,
    dead: Arc<AtomicBool>,
    /// Wakes the reader to close the connection.
    kill: Arc<tokio::sync::Notify>,
    /// A standby connection (ADR-057): dialled as one, or announced as one
    /// by the peer that dialled it. The test filter's cut and mute spare it.
    standby: Arc<AtomicBool>,
    quiet: Arc<Mutex<Quiet>>,
}

impl Conn {
    async fn open(
        inner: &Arc<Inner>,
        peer: NodeId,
        addr: SocketAddr,
        standby: bool,
    ) -> Result<Arc<Conn>, RpcError> {
        let unreachable = |e: String| RpcError::Unreachable(peer, e);
        if !standby && inner.filter.is_cut(peer) {
            return Err(unreachable("primary path cut by filter".into()));
        }
        let mut s = tokio::time::timeout(inner.cfg.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| unreachable("connect timeout".into()))?
            .map_err(|e| unreachable(e.to_string()))?;
        s.set_nodelay(true).ok();
        crate::sockopt::set_rto_min(&s, crate::sockopt::RTO_MIN);
        crate::sockopt::tune(&s, crate::sockopt::Traffic::Control);
        let who = tokio::time::timeout(
            inner.cfg.connect_timeout,
            auth::client(
                &mut s,
                &inner.cfg.secret,
                &inner.cfg.cluster,
                inner.cfg.node,
            ),
        )
        .await
        .map_err(|_| unreachable("handshake timeout".into()))?
        .map_err(|e| unreachable(e.to_string()))?;
        if who != peer {
            return Err(unreachable(format!(
                "{addr} is node {who}, expected {peer}"
            )));
        }
        let conn = Conn::start(inner, peer, s, standby);
        if standby {
            let hello = tokio::time::timeout(
                inner.cfg.connect_timeout,
                conn.call(SVC_STANDBY, Bytes::new()),
            )
            .await
            .unwrap_or_else(|_| Err(unreachable("standby hello timed out".into())));
            if let Err(e) = hello {
                conn.close("standby refused");
                return Err(match e {
                    RpcError::NoService(_) => unreachable("peer has no standby support".into()),
                    e => e,
                });
            }
        }
        Ok(conn)
    }

    /// Run an authenticated stream: answer the peer's requests and route
    /// answers to ours.
    fn start(inner: &Arc<Inner>, peer: NodeId, s: TcpStream, standby: bool) -> Arc<Conn> {
        let (mut r, w) = s.into_split();
        let (tx, rx) = mpsc::channel(1024);
        let closed = Arc::new(tokio::sync::Notify::new());
        tokio::spawn(writer(
            BufWriter::with_capacity(256 << 10, w),
            rx,
            closed.clone(),
        ));
        let conn = Arc::new(Conn {
            peer,
            tx: tx.clone(),
            pending: Arc::default(),
            next_id: AtomicU64::new(1),
            dead: Arc::new(AtomicBool::new(false)),
            kill: Arc::default(),
            standby: Arc::new(AtomicBool::new(standby)),
            quiet: Arc::default(),
        });
        let (pending, dead, kill, standby, quiet) = (
            conn.pending.clone(),
            conn.dead.clone(),
            conn.kill.clone(),
            conn.standby.clone(),
            conn.quiet.clone(),
        );
        // The reader owns the connection for as long as it is open: an
        // accepted one nobody else holds may still become a standby.
        let this = conn.clone();
        let inner = inner.clone();
        let mut stop = inner.shutdown.subscribe();
        tokio::spawn(async move {
            let reply = |id: u64| Frame {
                id,
                kind: KIND_OK,
                service: 0,
                body: Bytes::new(),
            };
            loop {
                let f = tokio::select! {
                    f = read_frame(&mut r) => f,
                    _ = stop.changed() => break,
                    _ = kill.notified() => break,
                };
                let Ok(f) = f else { break };
                if f.kind == KIND_REQ && inner.filter.is_blocked(peer) {
                    continue; // partitioned: the request is lost
                }
                if f.kind == KIND_REQ && f.service == SVC_STANDBY {
                    standby.store(true, Ordering::Relaxed);
                }
                if !standby.load(Ordering::Relaxed) {
                    if inner.filter.is_cut(peer) {
                        break;
                    }
                    if inner.filter.is_muted(peer) {
                        continue; // a switch eating everything
                    }
                } else if inner.filter.is_lan_muted(peer) {
                    continue;
                }
                // Anything at all from the peer restarts the silence clock:
                // a slow handler on a live connection is never silence.
                let waiting = !pending.lock().is_empty();
                *quiet.lock() = Quiet {
                    since: waiting.then(Instant::now),
                    probed: false,
                };
                match f.kind {
                    KIND_REQ if f.service == SVC_PING => {
                        let _ = tx.try_send(reply(f.id));
                    }
                    KIND_REQ if f.service == SVC_STANDBY => {
                        // The peer dialled this as its standby to us: it is
                        // ours to the peer too.
                        inner.paths.accepted_standby(peer, this.clone());
                        let _ = tx.try_send(reply(f.id));
                    }
                    KIND_REQ => answer(&inner, peer, &tx, f),
                    KIND_OK | KIND_ERR => {
                        let res = if f.kind == KIND_OK {
                            Ok(f.body)
                        } else {
                            Err(match f.service {
                                0 => {
                                    RpcError::Remote(String::from_utf8_lossy(&f.body).into_owned())
                                }
                                s => RpcError::NoService(s),
                            })
                        };
                        if let Some(tx) = pending.lock().remove(&f.id) {
                            let _ = tx.send(res);
                        }
                    }
                    _ => break,
                }
            }
            dead.store(true, Ordering::SeqCst);
            closed.notify_one();
            drop(this);
            for (_, tx) in pending.lock().drain() {
                let _ = tx.send(Err(RpcError::Lost(peer, "connection closed".into())));
            }
        });
        conn
    }

    pub(crate) fn alive(&self) -> bool {
        !self.dead.load(Ordering::SeqCst)
    }

    pub(crate) fn is_standby(&self) -> bool {
        self.standby.load(Ordering::Relaxed)
    }

    /// Close the connection: the reader stops and every request waiting on
    /// it fails as `Lost` (none is resent here).
    pub(crate) fn close(&self, why: &str) {
        self.dead.store(true, Ordering::SeqCst);
        self.kill.notify_one();
        for (_, tx) in self.pending.lock().drain() {
            let _ = tx.send(Err(RpcError::Lost(self.peer, why.into())));
        }
    }

    /// How long the peer has said nothing here while a request of ours was
    /// waiting, and whether a probe went out meanwhile.
    pub(crate) fn silence(&self) -> Option<(Duration, bool)> {
        if self.pending.lock().is_empty() {
            return None;
        }
        let q = self.quiet.lock();
        q.since.map(|t| (t.elapsed(), q.probed))
    }

    /// Ask the peer for any frame at all (its reader answers): proof that
    /// the connection still carries bytes, however busy its handlers are.
    pub(crate) fn probe(&self) {
        self.quiet.lock().probed = true;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let _ = self.tx.try_send(Frame {
            id,
            kind: KIND_REQ,
            service: SVC_PING,
            body: Bytes::new(),
        });
    }

    pub(crate) async fn call(&self, service: u8, body: Bytes) -> Result<Bytes, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        {
            let mut p = self.pending.lock();
            let first = p.is_empty();
            p.insert(id, tx);
            let mut q = self.quiet.lock();
            if first || q.since.is_none() {
                // Silence counts from the first request nobody answered yet.
                *q = Quiet {
                    since: Some(Instant::now()),
                    probed: false,
                };
            }
        }
        if !self.alive()
            || self
                .tx
                .send(Frame {
                    id,
                    kind: KIND_REQ,
                    service,
                    body,
                })
                .await
                .is_err()
        {
            self.pending.lock().remove(&id);
            return Err(RpcError::Unreachable(self.peer, "connection closed".into()));
        }
        rx.await
            .unwrap_or_else(|_| Err(RpcError::Lost(self.peer, "connection dropped".into())))
    }
}

/// Run a request from `peer` and send the answer back on its connection.
fn answer(inner: &Arc<Inner>, peer: NodeId, tx: &mpsc::Sender<Frame>, f: Frame) {
    if inner.filter.is_blocked(peer) {
        return; // partitioned: the request is lost
    }
    let handler = inner.handlers.read().get(&f.service).cloned();
    let (tx, inner) = (tx.clone(), inner.clone());
    tokio::spawn(async move {
        let (kind, service, body) = match handler {
            None => (KIND_ERR, f.service, Bytes::new()),
            Some(h) => match h.call(peer, f.body).await {
                Ok(b) => (KIND_OK, 0, b),
                Err(e) => (KIND_ERR, 0, Bytes::from(e.into_bytes())),
            },
        };
        if inner.filter.is_blocked(peer) {
            return;
        }
        let _ = tx
            .send(Frame {
                id: f.id,
                kind,
                service,
                body,
            })
            .await;
    });
}

impl Rpc {
    /// Bind the listener and start serving. Handlers may be registered
    /// afterwards; requests for unregistered services get `NoService`.
    pub async fn bind(cfg: RpcConfig) -> std::io::Result<Rpc> {
        Self::bind_with(cfg, StandbyConfig::default()).await
    }

    /// `bind` with the standby path's timing (tests shorten it).
    pub async fn bind_with(cfg: RpcConfig, standby: StandbyConfig) -> std::io::Result<Rpc> {
        let listener = TcpListener::bind(cfg.listen).await?;
        let local_addr = listener.local_addr()?;
        let (shutdown, _) = watch::channel(false);
        let inner = Arc::new(Inner {
            cfg,
            standby,
            local_addr,
            handlers: Default::default(),
            addrs: Default::default(),
            pinned: Default::default(),
            conns: Default::default(),
            paths: Default::default(),
            dials: Default::default(),
            filter: Filter::default(),
            shutdown,
        });
        let weak = Arc::downgrade(&inner);
        let mut stop = inner.shutdown.subscribe();
        tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    a = listener.accept() => a,
                    _ = stop.changed() => return,
                };
                let Ok((s, _)) = accepted else { continue };
                let Some(inner) = weak.upgrade() else { return };
                tokio::spawn(serve(inner, s));
            }
        });
        let rpc = Rpc { inner };
        rpc.keep_pinned_connected();
        crate::path::watch(&rpc);
        Ok(rpc)
    }

    pub fn node(&self) -> NodeId {
        self.inner.cfg.node
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    pub fn filter(&self) -> &Filter {
        &self.inner.filter
    }

    /// Install a service handler, returning the previous one if replaced.
    pub fn register(&self, service: u8, handler: Arc<dyn Handler>) -> Option<Arc<dyn Handler>> {
        self.inner.handlers.write().insert(service, handler)
    }

    /// Learn a peer's address (from membership). Ignored for a peer whose
    /// address was pinned. Never touches a standby address.
    pub fn set_peer(&self, node: NodeId, addr: SocketAddr) {
        if self.inner.pinned.read().contains_key(&node) {
            return;
        }
        let old = self.inner.addrs.write().insert(node, addr);
        if old.is_some_and(|o| o != addr) {
            self.inner.conns.lock().remove(&node);
        }
    }

    /// Fix a peer's address (from this host's own configuration): nothing
    /// learned later replaces it, and the connection to it is kept open so
    /// the peer can send its requests on it (an off-grid host pins the
    /// fabric hosts' LAN addresses, ADR-047).
    pub fn pin_peer(&self, node: NodeId, addr: SocketAddr) {
        self.inner.pinned.write().insert(node, addr);
        let old = self.inner.addrs.write().insert(node, addr);
        if old.is_some_and(|o| o != addr) {
            self.inner.conns.lock().remove(&node);
        }
    }

    /// Send control requests on the standby (LAN) while it answers, the
    /// fabric the fallback (ADR-068); off, the standby is only the fallback.
    pub fn prefer_standby(&self, on: bool) {
        let was = self
            .inner
            .paths
            .prefer_standby
            .swap(on, std::sync::atomic::Ordering::Relaxed);
        if was != on {
            tracing::info!(on, "control traffic prefers the LAN");
        }
    }

    /// Give `node` a standby address, its 10G LAN address (ADR-057). Of
    /// each pair the lower node id dials and keeps a second, idle
    /// connection there; the higher one uses the connection it accepts.
    /// `node`'s requests move to the standby when its primary connection
    /// fails and come back once the primary has been healthy for a while.
    pub fn set_standby(&self, node: NodeId, addr: SocketAddr) {
        if node != self.inner.cfg.node {
            self.inner.paths.set_standby(node, addr);
        }
    }

    /// Stop dialling `node` (it went off-grid and dials in, ADR-047). A
    /// pinned address stays; an open connection stays until it drops.
    pub fn forget_peer(&self, node: NodeId) {
        if self.inner.pinned.read().contains_key(&node) {
            return;
        }
        self.inner.addrs.write().remove(&node);
    }

    pub fn peer_addr(&self, node: NodeId) -> Option<SocketAddr> {
        self.inner.addrs.read().get(&node).copied()
    }

    /// Whether a live connection to `peer` is open (either direction,
    /// either path).
    pub fn connected(&self, peer: NodeId) -> bool {
        self.primary_now(peer).is_some() || self.inner.paths.standby_conn(peer).is_some()
    }

    /// Which path `peer`'s requests take, and how often they moved
    /// (ADR-057). `None` for a peer without a standby.
    pub fn path(&self, peer: NodeId) -> Option<PathState> {
        self.inner.paths.state(peer, self.inner.standby.ping_fresh)
    }

    /// Every peer with a standby, and its path state.
    pub fn paths(&self) -> Vec<(NodeId, PathState)> {
        self.inner.paths.all(self.inner.standby.ping_fresh)
    }

    /// Send one request and wait for its response.
    pub async fn call(
        &self,
        peer: NodeId,
        service: u8,
        body: Bytes,
        timeout: Duration,
    ) -> Result<Bytes, RpcError> {
        if self.inner.filter.is_blocked(peer) {
            return Err(RpcError::Unreachable(peer, "blocked by filter".into()));
        }
        if peer == self.inner.cfg.node {
            // Local calls skip the network but keep the same semantics.
            let h = self.inner.handlers.read().get(&service).cloned();
            let h = h.ok_or(RpcError::NoService(service))?;
            return tokio::time::timeout(timeout, h.call(peer, body))
                .await
                .map_err(|_| RpcError::Timeout(peer))?
                .map_err(RpcError::Remote);
        }
        tokio::time::timeout(timeout, async {
            let conn = self.route(peer).await?;
            conn.call(service, body).await
        })
        .await
        .map_err(|_| RpcError::Timeout(peer))?
    }

    /// The connection `peer`'s next request goes on: its standby while
    /// failed over, else the primary (dialled if needed). A primary that
    /// cannot be dialled fails the peer over at once when its standby is
    /// up, and this request (never sent) goes there instead.
    async fn route(&self, peer: NodeId) -> Result<Arc<Conn>, RpcError> {
        if let Some(c) = self.inner.paths.active_standby(peer) {
            return Ok(c);
        }
        if let Some(c) = self.primary_now(peer) {
            return Ok(c);
        }
        if !self.inner.paths.has(peer) {
            return self.conn(peer).await;
        }
        // Dial in a task of its own, shared by every caller meanwhile: a
        // caller with a short budget (a heartbeat) gives up long before a
        // dead fabric's connect timeout, and the failover must still happen
        // when the dial fails.
        let dial = {
            let mut dials = self.inner.dials.lock();
            match dials.get(&peer) {
                Some(d) => d.clone(),
                None => {
                    let rpc = self.clone();
                    let task = tokio::spawn(async move {
                        let r = match rpc.conn(peer).await {
                            Err(e @ RpcError::Unreachable(..)) => {
                                let why = format!("fabric connect failed: {e}");
                                rpc.inner
                                    .paths
                                    .fail_over(&rpc, peer, &why, None)
                                    .await
                                    .ok_or(e)
                            }
                            r => r,
                        };
                        rpc.inner.dials.lock().remove(&peer);
                        r
                    });
                    let d = futures::FutureExt::shared(Box::pin(async move {
                        task.await
                            .unwrap_or_else(|e| Err(RpcError::Protocol(format!("dial task: {e}"))))
                    })
                        as BoxFuture<'static, Result<Arc<Conn>, RpcError>>);
                    dials.insert(peer, d.clone());
                    d
                }
            }
        };
        dial.await
    }

    /// The live primary connection to `peer`, without dialling.
    pub(crate) fn primary_now(&self, peer: NodeId) -> Option<Arc<Conn>> {
        let slot = self.inner.conns.lock().get(&peer).cloned()?;
        let c = slot.try_lock().ok()?.clone()?;
        (c.alive() && !c.is_standby()).then_some(c)
    }

    /// The primary connection to `peer`, dialled if there is none.
    pub(crate) async fn conn(&self, peer: NodeId) -> Result<Arc<Conn>, RpcError> {
        let slot = self.inner.conns.lock().entry(peer).or_default().clone();
        let mut slot = slot.lock().await;
        if let Some(c) = slot.as_ref()
            && c.alive()
            && !c.is_standby()
        {
            return Ok(c.clone());
        }
        let addr = self.peer_addr(peer).ok_or_else(|| {
            RpcError::Unreachable(peer, "no address, and it has not connected here".into())
        })?;
        let c = Conn::open(&self.inner, peer, addr, false).await?;
        *slot = Some(c.clone());
        Ok(c)
    }

    /// Dial `peer`'s standby address.
    pub(crate) async fn open_standby(
        &self,
        peer: NodeId,
        addr: SocketAddr,
    ) -> Result<Arc<Conn>, RpcError> {
        Conn::open(&self.inner, peer, addr, true).await
    }

    /// Reconnect to pinned peers as soon as their connection drops, so
    /// they can always reach this host.
    fn keep_pinned_connected(&self) {
        let weak = Arc::downgrade(&self.inner);
        let mut stop = self.inner.shutdown.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                    _ = stop.changed() => return,
                }
                let Some(inner) = weak.upgrade() else { return };
                let rpc = Rpc { inner };
                let peers: Vec<NodeId> = rpc.inner.pinned.read().keys().copied().collect();
                for p in peers {
                    if !rpc.inner.filter.is_blocked(p) && rpc.primary_now(p).is_none() {
                        let r = rpc.clone();
                        tokio::spawn(async move {
                            let _ = r.conn(p).await;
                        });
                    }
                }
            }
        });
    }

    /// Stop serving and close connections. Outstanding calls fail.
    pub fn shutdown(&self) {
        let _ = self.inner.shutdown.send(true);
        self.inner.conns.lock().clear();
        self.inner.paths.clear();
    }
}

async fn serve(inner: Arc<Inner>, mut s: TcpStream) {
    s.set_nodelay(true).ok();
    crate::sockopt::set_rto_min(&s, crate::sockopt::RTO_MIN);
    crate::sockopt::tune(&s, crate::sockopt::Traffic::Control);
    let hs = tokio::time::timeout(
        inner.cfg.connect_timeout,
        auth::server(
            &mut s,
            &inner.cfg.secret,
            &inner.cfg.cluster,
            inner.cfg.node,
        ),
    )
    .await;
    let peer = match hs {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "rejected rpc connection");
            return;
        }
        Err(_) => return,
    };
    // Which network it arrived on is not known here (a fabric host listens
    // on every address): a standby announces itself with its first request
    // and the reader then registers it as the peer's standby. A peer that
    // predates standbys never does, so its connections stay primary.
    let conn = Conn::start(&inner, peer, s, false);
    // A peer we cannot dial (no address: it is behind NAT) is reached over
    // the connection it opened; the newest one wins. Peers we can dial keep
    // their own outgoing connection for our requests.
    if !inner.addrs.read().contains_key(&peer) {
        let slot = inner.conns.lock().entry(peer).or_default().clone();
        let mut slot = slot.lock().await;
        // The standby hello may already have arrived: never make the
        // standby the primary.
        if !conn.is_standby() {
            *slot = Some(conn);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    async fn node(id: u64, secret: &[u8]) -> Rpc {
        let rpc = Rpc::bind(RpcConfig {
            node: NodeId(id),
            cluster: "test".into(),
            secret: secret.to_vec(),
            listen: "127.0.0.1:0".parse().unwrap(),
            connect_timeout: Duration::from_secs(2),
        })
        .await
        .unwrap();
        let me = id;
        rpc.register(
            7,
            Arc::new(move |peer: NodeId, body: Bytes| {
                async move {
                    if body.as_ref() == b"fail" {
                        return Err("nope".to_string());
                    }
                    let mut v = format!("{me}<-{peer}:").into_bytes();
                    v.extend_from_slice(&body);
                    Ok(Bytes::from(v))
                }
                .boxed()
            }),
        );
        rpc
    }

    #[tokio::test]
    async fn roundtrip_errors_and_filter() {
        let a = node(1, b"s").await;
        let b = node(2, b"s").await;
        a.set_peer(NodeId(2), b.local_addr());
        let t = Duration::from_secs(2);
        let r = a
            .call(NodeId(2), 7, Bytes::from_static(b"hi"), t)
            .await
            .unwrap();
        assert_eq!(&r[..], b"2<-1:hi");
        // Many concurrent calls over one connection.
        let futs: Vec<_> = (0..200)
            .map(|i| a.call(NodeId(2), 7, Bytes::from(format!("{i}")), t))
            .collect();
        for (i, r) in futures::future::join_all(futs)
            .await
            .into_iter()
            .enumerate()
        {
            assert_eq!(r.unwrap(), Bytes::from(format!("2<-1:{i}")));
        }
        let connection = a.primary_now(NodeId(2)).unwrap();
        assert_eq!(
            a.call(NodeId(2), 7, Bytes::from_static(b"fail"), t).await,
            Err(RpcError::Remote("nope".into()))
        );
        assert!(Arc::ptr_eq(&connection, &a.primary_now(NodeId(2)).unwrap()));
        assert_eq!(
            a.call(NodeId(2), 7, Bytes::from_static(b"still here"), t)
                .await
                .unwrap(),
            Bytes::from_static(b"2<-1:still here")
        );
        assert!(Arc::ptr_eq(&connection, &a.primary_now(NodeId(2)).unwrap()));
        assert_eq!(
            a.call(NodeId(2), 9, Bytes::new(), t).await,
            Err(RpcError::NoService(9))
        );
        // Local call path.
        assert_eq!(
            &a.call(NodeId(1), 7, Bytes::from_static(b"x"), t)
                .await
                .unwrap()[..],
            b"1<-1:x"
        );
        // Partition.
        a.filter().block(NodeId(2));
        assert!(matches!(
            a.call(NodeId(2), 7, Bytes::new(), t).await,
            Err(RpcError::Unreachable(..))
        ));
        a.filter().unblock(NodeId(2));
        b.filter().block(NodeId(1));
        assert_eq!(
            a.call(NodeId(2), 7, Bytes::new(), Duration::from_millis(200))
                .await,
            Err(RpcError::Timeout(NodeId(2)))
        );
    }

    #[tokio::test]
    async fn wrong_secret_is_rejected() {
        let a = node(1, b"secret-a").await;
        let b = node(2, b"secret-b").await;
        a.set_peer(NodeId(2), b.local_addr());
        let r = a
            .call(NodeId(2), 7, Bytes::new(), Duration::from_secs(2))
            .await;
        assert!(matches!(r, Err(RpcError::Unreachable(..))), "{r:?}");
    }

    #[tokio::test]
    async fn reconnects_after_peer_restart() {
        let a = node(1, b"s").await;
        let b = node(2, b"s").await;
        a.set_peer(NodeId(2), b.local_addr());
        let t = Duration::from_secs(2);
        a.call(NodeId(2), 7, Bytes::new(), t).await.unwrap();
        let addr = b.local_addr();
        b.shutdown();
        drop(b);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(a.call(NodeId(2), 7, Bytes::new(), t).await.is_err());
        let b2 = Rpc::bind(RpcConfig {
            node: NodeId(2),
            cluster: "test".into(),
            secret: b"s".to_vec(),
            listen: addr,
            connect_timeout: t,
        })
        .await
        .unwrap();
        b2.register(
            7,
            Arc::new(|_p: NodeId, _b: Bytes| async { Ok(Bytes::from_static(b"back")) }.boxed()),
        );
        assert_eq!(
            &a.call(NodeId(2), 7, Bytes::new(), t).await.unwrap()[..],
            b"back"
        );
    }

    /// ADR-047: a host behind NAT dials out; the host it dialled has no
    /// address for it and sends its own requests back over that connection.
    #[tokio::test]
    async fn a_peer_behind_nat_is_reached_over_the_connection_it_opened() {
        let core = node(1, b"s").await;
        let offgrid = node(2, b"s").await;
        let t = Duration::from_secs(2);
        // No address for node 2 on the core side, and none learned later.
        assert!(matches!(
            core.call(NodeId(2), 7, Bytes::new(), t).await,
            Err(RpcError::Unreachable(..))
        ));
        offgrid.pin_peer(NodeId(1), core.local_addr());
        // Membership later names node 1 by a fabric address: ignored.
        offgrid.set_peer(NodeId(1), "10.255.255.1:9".parse().unwrap());
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while !core.connected(NodeId(2)) {
            assert!(
                std::time::Instant::now() < deadline,
                "node 2 never dialled in"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let r = core.call(NodeId(2), 7, Bytes::from_static(b"hi"), t).await;
        assert_eq!(&r.unwrap()[..], b"2<-1:hi");
        let r = offgrid
            .call(NodeId(1), 7, Bytes::from_static(b"yo"), t)
            .await;
        assert_eq!(&r.unwrap()[..], b"1<-2:yo");
        // Both directions at once, many in flight.
        let ups: Vec<_> = (0..100)
            .map(|i| core.call(NodeId(2), 7, Bytes::from(format!("{i}")), t))
            .collect();
        let downs: Vec<_> = (0..100)
            .map(|i| offgrid.call(NodeId(1), 7, Bytes::from(format!("{i}")), t))
            .collect();
        let (ups, downs) = futures::join!(
            futures::future::join_all(ups),
            futures::future::join_all(downs)
        );
        for (i, (u, d)) in ups.into_iter().zip(downs).enumerate() {
            assert_eq!(u.unwrap(), Bytes::from(format!("2<-1:{i}")));
            assert_eq!(d.unwrap(), Bytes::from(format!("1<-2:{i}")));
        }

        // The core host restarts: the off-grid host dials in again and the
        // core reaches it as before.
        let addr = core.local_addr();
        core.shutdown();
        drop(core);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let core = Rpc::bind(RpcConfig {
            node: NodeId(1),
            cluster: "test".into(),
            secret: b"s".to_vec(),
            listen: addr,
            connect_timeout: t,
        })
        .await
        .unwrap();
        core.register(
            7,
            Arc::new(|p: NodeId, b: Bytes| {
                async move {
                    let mut v = format!("1'<-{p}:").into_bytes();
                    v.extend_from_slice(&b);
                    Ok(Bytes::from(v))
                }
                .boxed()
            }),
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while !core.connected(NodeId(2)) {
            assert!(
                std::time::Instant::now() < deadline,
                "node 2 did not reconnect"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let r = core
            .call(NodeId(2), 7, Bytes::from_static(b"again"), t)
            .await;
        assert_eq!(&r.unwrap()[..], b"2<-1:again");
        let r = offgrid
            .call(NodeId(1), 7, Bytes::from_static(b"x"), t)
            .await;
        assert_eq!(&r.unwrap()[..], b"1'<-2:x");
    }

    // ------------------------------------------------------------ standby

    fn fast() -> StandbyConfig {
        StandbyConfig {
            silence: Duration::from_millis(300),
            ping_every: Duration::from_millis(100),
            ping_fresh: Duration::from_millis(400),
            redial_every: Duration::from_millis(200),
            return_after: Duration::from_secs(1),
        }
    }

    /// A node on every local address: 127.0.0.1 plays the fabric and
    /// 127.0.0.2 the LAN, one listener for both as on the real hosts.
    async fn lan_node(id: u64) -> Rpc {
        let rpc = Rpc::bind_with(
            RpcConfig {
                node: NodeId(id),
                cluster: "test".into(),
                secret: b"s".to_vec(),
                listen: "0.0.0.0:0".parse().unwrap(),
                connect_timeout: Duration::from_secs(2),
            },
            fast(),
        )
        .await
        .unwrap();
        let me = id;
        rpc.register(
            7,
            Arc::new(move |peer: NodeId, body: Bytes| {
                async move {
                    let mut v = format!("{me}<-{peer}:").into_bytes();
                    v.extend_from_slice(&body);
                    Ok(Bytes::from(v))
                }
                .boxed()
            }),
        );
        // Slow but alive: answers after 1 s.
        rpc.register(
            8,
            Arc::new(|_p: NodeId, _b: Bytes| {
                async {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    Ok(Bytes::from_static(b"late"))
                }
                .boxed()
            }),
        );
        rpc
    }

    fn fabric(r: &Rpc) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], r.local_addr().port()))
    }

    fn lan(r: &Rpc) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 2], r.local_addr().port()))
    }

    /// Two nodes that know each other on the fabric, standbys up. Node 1
    /// (the lower id) dials the standby; node 2 is given node 1's standby
    /// address only with `both`, and accepts the connection either way.
    async fn pair(both: bool) -> (Rpc, Rpc) {
        let (a, b) = (lan_node(1).await, lan_node(2).await);
        a.set_peer(NodeId(2), fabric(&b));
        b.set_peer(NodeId(1), fabric(&a));
        a.set_standby(NodeId(2), lan(&b));
        if both {
            b.set_standby(NodeId(1), lan(&a));
        }
        until("standbys up", Duration::from_secs(3), || {
            a.path(NodeId(2)).is_some_and(|p| p.standby_up)
                && b.path(NodeId(1)).is_some_and(|p| p.standby_up)
        })
        .await;
        (a, b)
    }

    fn lost_or_unreachable(e: &RpcError) -> bool {
        matches!(e, RpcError::Lost(..) | RpcError::Unreachable(..))
    }

    async fn until(what: &str, within: Duration, mut f: impl FnMut() -> bool) {
        let deadline = Instant::now() + within;
        while !f() {
            assert!(Instant::now() < deadline, "timed out waiting for: {what}");

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn echo(from: &Rpc, to: u64, body: &'static [u8]) -> Result<Bytes, RpcError> {
        from.call(
            NodeId(to),
            7,
            Bytes::from_static(body),
            Duration::from_secs(2),
        )
        .await
    }

    /// ADR-057: the fabric connection breaks (closed, and dialling it is
    /// refused): requests move to the standby, both ways; once the fabric
    /// is back and healthy for `return_after` they move back.
    #[tokio::test]
    async fn fails_over_when_the_primary_breaks_and_returns() {
        let (a, b) = pair(true).await;
        assert_eq!(&echo(&a, 2, b"x").await.unwrap()[..], b"2<-1:x");
        assert_eq!(&echo(&b, 1, b"y").await.unwrap()[..], b"1<-2:y");
        let p = a.path(NodeId(2)).unwrap();
        assert!(!p.on_standby && p.failovers == 0, "{p:?}");

        a.filter().cut(NodeId(2));
        b.filter().cut(NodeId(1));
        let t = Instant::now();
        let mut ok = 0;
        while ok < 20 {
            // In flight at the switch: may be lost, never sent twice.
            match echo(&a, 2, b"x").await {
                Ok(r) => {
                    assert_eq!(&r[..], b"2<-1:x");
                    ok += 1;
                }
                Err(e) => assert!(lost_or_unreachable(&e), "{e:?}"),
            }
            assert!(t.elapsed() < Duration::from_secs(3), "no failover");
        }
        // Node 2's first request may be the one in flight at its switch.
        let r = match echo(&b, 1, b"y").await {
            Err(e) if lost_or_unreachable(&e) => echo(&b, 1, b"y").await,
            r => r,
        };
        assert_eq!(&r.unwrap()[..], b"1<-2:y");
        for (rpc, peer) in [(&a, 2), (&b, 1)] {
            let p = rpc.path(NodeId(peer)).unwrap();
            assert!(p.on_standby && p.failovers == 1 && p.standby_up, "{p:?}");
        }
        // Still cut: redials fail, requests stay on the standby.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(a.path(NodeId(2)).unwrap().on_standby);
        assert!(echo(&a, 2, b"x").await.is_ok());

        a.filter().uncut(NodeId(2));
        b.filter().uncut(NodeId(1));
        let healed = Instant::now();
        until("back on the fabric", Duration::from_secs(5), || {
            !a.path(NodeId(2)).unwrap().on_standby && !b.path(NodeId(1)).unwrap().on_standby
        })
        .await;
        assert!(
            healed.elapsed() >= Duration::from_millis(900),
            "returned before the fabric was healthy long enough"
        );
        let p = a.path(NodeId(2)).unwrap();
        assert_eq!((p.failovers, p.returns), (1, 1), "{p:?}");
        assert!(a.primary_now(NodeId(2)).is_some());
        assert_eq!(&echo(&a, 2, b"z").await.unwrap()[..], b"2<-1:z");
    }

    /// The fabric connection stays open but nothing arrives on it (a switch
    /// dropping everything): a request unanswered for `silence`, with not
    /// even a probe answered meanwhile, moves the peer to the standby. The
    /// request in flight fails `Unreachable`; the next ones succeed. The
    /// accepting side (no standby address of its own) fails over too.
    #[tokio::test]
    async fn fails_over_when_the_primary_goes_silent() {
        let (a, b) = pair(false).await;
        assert!(echo(&a, 2, b"x").await.is_ok());
        assert!(echo(&b, 1, b"y").await.is_ok());
        a.filter().mute(NodeId(2));
        b.filter().mute(NodeId(1));
        let t = Instant::now();
        let r = echo(&a, 2, b"x").await;
        assert!(
            matches!(r, Err(RpcError::Lost(..))),
            "in flight at the switch: {r:?}"
        );
        let took = t.elapsed();
        assert!(
            took >= Duration::from_millis(300) && took < Duration::from_millis(1200),
            "switched after {took:?}"
        );
        assert_eq!(&echo(&a, 2, b"x").await.unwrap()[..], b"2<-1:x");
        let p = a.path(NodeId(2)).unwrap();
        assert!(p.on_standby && p.failovers == 1, "{p:?}");
        assert!(p.reason.unwrap().contains("no answer"));
        // Node 2, over the standby it accepted.
        let mut answered = false;
        for _ in 0..5 {
            if echo(&b, 1, b"y").await.is_ok() {
                answered = true;
                break;
            }
        }
        assert!(answered);
        assert!(b.path(NodeId(1)).unwrap().on_standby);
        a.filter().unmute(NodeId(2));
        b.filter().unmute(NodeId(1));
        until("back on the fabric", Duration::from_secs(5), || {
            !a.path(NodeId(2)).unwrap().on_standby && !b.path(NodeId(1)).unwrap().on_standby
        })
        .await;
        assert!(echo(&a, 2, b"x").await.is_ok());
        assert!(echo(&b, 1, b"y").await.is_ok());
    }

    /// Slowness alone never switches: a handler taking three times the
    /// silence budget answers on the primary, and nothing moves.
    #[tokio::test]
    async fn a_slow_answer_does_not_fail_over() {
        let (a, _b) = pair(true).await;
        let t = Instant::now();
        let r = a
            .call(NodeId(2), 8, Bytes::new(), Duration::from_secs(5))
            .await;
        assert_eq!(&r.unwrap()[..], b"late");
        assert!(t.elapsed() >= Duration::from_secs(1));
        let p = a.path(NodeId(2)).unwrap();
        assert!(!p.on_standby && p.failovers == 0, "{p:?}");
        // Many slow calls at once, the peer otherwise idle: still none.
        let calls: Vec<_> = (0..20)
            .map(|_| a.call(NodeId(2), 8, Bytes::new(), Duration::from_secs(5)))
            .collect();
        for r in futures::future::join_all(calls).await {
            assert!(r.is_ok(), "{r:?}");
        }
        assert_eq!(a.path(NodeId(2)).unwrap().failovers, 0);
    }

    /// Without a working standby nothing fails over: requests to a peer
    /// whose fabric is cut fail as before.
    #[tokio::test]
    async fn no_failover_without_a_standby_answering() {
        let (a, b) = (lan_node(1).await, lan_node(2).await);
        a.set_peer(NodeId(2), fabric(&b));
        // Nobody listens there.
        let dead = std::net::TcpListener::bind("127.0.0.2:0").unwrap();
        let nowhere = dead.local_addr().unwrap();
        drop(dead);
        a.set_standby(NodeId(2), nowhere);
        assert!(echo(&a, 2, b"x").await.is_ok());
        a.filter().cut(NodeId(2));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(matches!(
            echo(&a, 2, b"x").await,
            Err(RpcError::Unreachable(..))
        ));
        let p = a.path(NodeId(2)).unwrap();
        assert!(!p.on_standby && !p.standby_up && p.failovers == 0, "{p:?}");
    }

    /// A peer that predates standbys (0.4.4): it answers the standby hello
    /// `NoService`; the primary works as before, the standby is dropped and
    /// not dialled again for a minute.
    #[tokio::test]
    async fn interoperates_with_a_peer_without_standby_support() {
        let l = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicU64::new(0));
        let n = accepted.clone();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = l.accept().await.unwrap();
                n.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    // The 0.4.4 serving loop: echo service 7, else NoService.
                    auth::server(&mut s, b"s", "test", NodeId(2)).await.unwrap();
                    let (mut r, w) = s.into_split();
                    let mut w = BufWriter::new(w);
                    while let Ok(f) = read_frame(&mut r).await {
                        let reply = match f.service {
                            7 => Frame {
                                id: f.id,
                                kind: KIND_OK,
                                service: 0,
                                body: f.body,
                            },
                            s => Frame {
                                id: f.id,
                                kind: KIND_ERR,
                                service: s,
                                body: Bytes::new(),
                            },
                        };
                        write_frame(&mut w, &reply).await.unwrap();
                        w.flush().await.unwrap();
                    }
                });
            }
        });
        let a = lan_node(1).await;
        a.set_peer(NodeId(2), SocketAddr::from(([127, 0, 0, 1], port)));
        a.set_standby(NodeId(2), SocketAddr::from(([127, 0, 0, 2], port)));
        for _ in 0..10 {
            assert_eq!(&echo(&a, 2, b"old").await.unwrap()[..], b"old");
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(&echo(&a, 2, b"old").await.unwrap()[..], b"old");
        let p = a.path(NodeId(2)).unwrap();
        assert!(!p.standby_up && !p.on_standby, "{p:?}");
        // The primary, and one standby attempt.
        assert_eq!(accepted.load(Ordering::SeqCst), 2);
    }
}
