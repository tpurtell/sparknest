//! A blocking client for the push socket, for engines (ADR-063 §8).
//!
//! ```no_run
//! use nest_push::{client::*, plan::*, proto::*};
//! # let (ranks, files, patterns, entries): (Vec<RankSpec>, Vec<String>, Vec<Pattern>, Vec<Entry>) = Default::default();
//! // Coordinator, on the hub:
//! let g = Coordinator::create(SOCKET, ranks, files, Some(std::time::Duration::from_secs(10)))?;
//! g.patterns(patterns)?;
//! for chunk in entries.chunks(4096) { g.entries(chunk.to_vec())?; }
//! let forecast = g.start()?;
//! while let Some(ev) = g.next()? { /* Progress, then Done or Failed */ }
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! Each rank opens its own connection to its host's sparknestd with
//! [`Rank::join`], passing the ring's fd, then loops on [`Rank::next`]:
//! copy each `Filled` slot out, then [`Rank::release`] it; skip
//! `Progress` (the group's, sent while other ranks are still being fed,
//! ADR-087). Any error, or the socket being absent, means: load through
//! the mount instead.
//!
//! Timeouts: the constructors take one and set it before their first send,
//! so a hung sparknestd fails `create`/`join` with `ErrorKind::TimedOut`.
//! The same timeout is then an idle deadline on every `next()`: no event
//! for that long is `TimedOut` (sparknestd sends `Progress` at least every
//! few seconds while a group runs).

use crate::plan::{Entry, Pattern};
use crate::proto::{self, Event, Forecast, RankSpec, Request};
use nix::sys::socket::{
    self, AddressFamily, ControlMessage, ControlMessageOwned, MsgFlags, SockFlag, SockType,
    UnixAddr, sockopt,
};
use nix::sys::time::TimeVal;
use std::io::{self, IoSlice, IoSliceMut};
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};

/// A connected SEQPACKET socket.
pub struct Conn {
    fd: OwnedFd,
}

fn nix_io(e: nix::Error) -> io::Error {
    io::Error::from_raw_os_error(e as i32)
}

/// `@name` is an abstract address; anything else a filesystem path.
fn address(path: &str) -> io::Result<UnixAddr> {
    match path.strip_prefix('@') {
        Some(name) => UnixAddr::new_abstract(name.as_bytes()),
        None => UnixAddr::new(path),
    }
    .map_err(nix_io)
}

impl Conn {
    /// Connect to `path` (`@name` for an abstract socket). A Unix
    /// socket's connect does not wait: it connects or fails at once
    /// (ECONNREFUSED when nothing listens, ENOENT for a missing path).
    pub fn connect(path: &str) -> io::Result<Conn> {
        Self::connect_timeout(path, None)
    }

    /// Connect, waiting at most `timeout` (a listener whose backlog is full
    /// makes a Unix connect wait), and keep `timeout` for every later send
    /// and receive (see [`Conn::set_timeout`]).
    pub fn connect_timeout(path: &str, timeout: Option<std::time::Duration>) -> io::Result<Conn> {
        let addr = address(path)?;
        let fd = socket::socket(
            AddressFamily::Unix,
            SockType::SeqPacket,
            SockFlag::SOCK_CLOEXEC,
            None,
        )
        .map_err(nix_io)?;
        let conn = Conn { fd };
        // Linux bounds a Unix connect by SO_SNDTIMEO.
        conn.set_timeout(timeout)?;
        loop {
            match socket::connect(conn.fd.as_raw_fd(), &addr) {
                Ok(()) => return Ok(conn),
                Err(nix::Error::EINTR) => continue,
                Err(e) => return Err(timed_out(nix_io(e))),
            }
        }
    }

    pub fn from_fd(fd: OwnedFd) -> Conn {
        Conn { fd }
    }

    /// A connected pair, for tests and in-process use.
    pub fn pair() -> io::Result<(Conn, Conn)> {
        let (a, b) = socket::socketpair(
            AddressFamily::Unix,
            SockType::SeqPacket,
            None,
            SockFlag::SOCK_CLOEXEC,
        )
        .map_err(nix_io)?;
        Ok((Conn { fd: a }, Conn { fd: b }))
    }

    pub fn fd(&self) -> BorrowedFd<'_> {
        use std::os::fd::AsFd;
        self.fd.as_fd()
    }

    /// Bound every later send and receive: one that waits longer fails
    /// with `ErrorKind::TimedOut`. `None` waits forever (the default).
    pub fn set_timeout(&self, t: Option<std::time::Duration>) -> io::Result<()> {
        // A zero TimeVal means forever; round a zero duration up to 1 µs.
        let tv = match t {
            Some(d) => TimeVal::from(libc::timeval {
                tv_sec: d.as_secs() as libc::time_t,
                tv_usec: d.subsec_micros().max(u32::from(d.is_zero())) as libc::suseconds_t,
            }),
            None => TimeVal::from(libc::timeval {
                tv_sec: 0,
                tv_usec: 0,
            }),
        };
        socket::setsockopt(&self.fd, sockopt::ReceiveTimeout, &tv).map_err(nix_io)?;
        socket::setsockopt(&self.fd, sockopt::SendTimeout, &tv).map_err(nix_io)?;
        Ok(())
    }

    /// Send one message, with `pass` as SCM_RIGHTS if given.
    pub fn send(&self, msg: &[u8], pass: Option<RawFd>) -> io::Result<()> {
        let iov = [IoSlice::new(msg)];
        let fds = pass.map(|f| [f]);
        let cmsg: Vec<ControlMessage> = fds
            .as_ref()
            .map(|f| ControlMessage::ScmRights(f))
            .into_iter()
            .collect();
        loop {
            match socket::sendmsg::<()>(
                self.fd.as_raw_fd(),
                &iov,
                &cmsg,
                MsgFlags::MSG_NOSIGNAL,
                None,
            ) {
                Ok(_) => return Ok(()),
                Err(nix::Error::EINTR) => continue,
                Err(e) => return Err(timed_out(nix_io(e))),
            }
        }
    }

    /// Receive one message (and an fd passed with it). `None` when the peer
    /// closed.
    pub fn recv(&self, buf: &mut Vec<u8>) -> io::Result<Option<Option<OwnedFd>>> {
        self.recv_with(buf, MsgFlags::empty())
    }

    fn recv_with(&self, buf: &mut Vec<u8>, flags: MsgFlags) -> io::Result<Option<Option<OwnedFd>>> {
        buf.resize(proto::MAX_MSG, 0);
        let mut cbuf = nix::cmsg_space!([RawFd; 1]);
        let (n, fd, truncated) = loop {
            let mut iov = [IoSliceMut::new(buf)];
            match socket::recvmsg::<()>(
                self.fd.as_raw_fd(),
                &mut iov,
                Some(&mut cbuf),
                MsgFlags::MSG_CMSG_CLOEXEC | flags,
            ) {
                Ok(m) => {
                    let mut fd = None;
                    for c in m.cmsgs().map_err(nix_io)? {
                        if let ControlMessageOwned::ScmRights(fds) = c {
                            for f in fds {
                                // SAFETY: the kernel just installed this fd for us;
                                // nothing else owns it.
                                let owned = unsafe { OwnedFd::from_raw_fd(f) };
                                fd.get_or_insert(owned);
                            }
                        }
                    }
                    break (m.bytes, fd, m.flags.contains(MsgFlags::MSG_TRUNC));
                }
                Err(nix::Error::EINTR) => continue,
                Err(e) => return Err(timed_out(nix_io(e))),
            }
        };
        if truncated {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "message too large",
            ));
        }
        if n == 0 && fd.is_none() {
            buf.clear();
            return Ok(None);
        }
        buf.truncate(n);
        Ok(Some(fd))
    }

    /// The peer's (pid, uid, gid), from SO_PEERCRED.
    pub fn peer(&self) -> io::Result<(i32, u32, u32)> {
        let c = socket::getsockopt(&self.fd, sockopt::PeerCredentials).map_err(nix_io)?;
        Ok((c.pid(), c.uid(), c.gid()))
    }

    pub fn request(&self, r: &Request, pass: Option<RawFd>) -> io::Result<()> {
        self.send(&proto::encode(r), pass)
    }

    /// The next event; `None` when sparknestd closed the socket.
    /// The next event if one is waiting, without blocking: `Ok(Ready(e))`,
    /// `Ok(Empty)` when none is, `Ok(Closed)` when sparknestd closed.
    pub fn try_event(&self) -> io::Result<Poll> {
        let mut buf = Vec::new();
        match self.recv_with(&mut buf, MsgFlags::MSG_DONTWAIT) {
            Ok(None) => Ok(Poll::Closed),
            Ok(Some(_)) => proto::decode(&buf)
                .map(Poll::Ready)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
            // MSG_DONTWAIT with nothing queued (timed_out maps it).
            Err(e)
                if e.kind() == io::ErrorKind::TimedOut || e.kind() == io::ErrorKind::WouldBlock =>
            {
                Ok(Poll::Empty)
            }
            Err(e) => Err(e),
        }
    }

    pub fn event(&self) -> io::Result<Option<Event>> {
        let mut buf = Vec::new();
        match self.recv(&mut buf)? {
            None => Ok(None),
            Some(_) => proto::decode(&buf)
                .map(Some)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
        }
    }

    /// The next request, with an fd if one came; `None` when the engine
    /// closed (the server side).
    pub fn next_request(&self) -> io::Result<Option<(Request, Option<OwnedFd>)>> {
        let mut buf = Vec::new();
        match self.recv(&mut buf)? {
            None => Ok(None),
            Some(fd) => proto::decode(&buf)
                .map(|r| Some((r, fd)))
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
        }
    }

    pub fn send_event(&self, e: &Event) -> io::Result<()> {
        self.send(&proto::encode(e), None)
    }
}

/// What a nonblocking receive found.
#[derive(Debug)]
pub enum Poll {
    Ready(Event),
    /// Nothing waiting yet.
    Empty,
    /// sparknestd closed the socket.
    Closed,
}

/// The server side: a listening SEQPACKET socket (sparknestd).
pub struct Listener {
    fd: OwnedFd,
}

impl Listener {
    /// Bind `path` (`@name`: abstract; a path is replaced if present).
    pub fn bind(path: &str) -> io::Result<Listener> {
        if !path.starts_with('@') {
            let _ = std::fs::remove_file(path);
        }
        let addr = address(path)?;
        let fd = socket::socket(
            AddressFamily::Unix,
            SockType::SeqPacket,
            SockFlag::SOCK_CLOEXEC,
            None,
        )
        .map_err(nix_io)?;
        socket::bind(fd.as_raw_fd(), &addr).map_err(nix_io)?;
        socket::listen(&fd, socket::Backlog::new(64).map_err(nix_io)?).map_err(nix_io)?;
        Ok(Listener { fd })
    }

    /// The next connection.
    pub fn accept(&self) -> io::Result<Conn> {
        loop {
            match socket::accept4(self.fd.as_raw_fd(), SockFlag::SOCK_CLOEXEC) {
                // SAFETY: accept4 returned a fresh fd that nothing else owns.
                Ok(fd) => return Ok(Conn::from_fd(unsafe { OwnedFd::from_raw_fd(fd) })),
                Err(nix::Error::EINTR) => continue,
                Err(e) => return Err(nix_io(e)),
            }
        }
    }
}

/// Send `items` as `wrap(batch)` messages, each under `proto::MAX_MSG`,
/// keeping their order (halving a batch until it fits).
fn send_batched<T: Clone + serde::Serialize>(
    conn: &Conn,
    items: Vec<T>,
    wrap: fn(Vec<T>) -> Request,
) -> io::Result<()> {
    let msg = proto::encode(&wrap(items.clone()));
    if msg.len() <= proto::MAX_MSG {
        return conn.send(&msg, None);
    }
    if items.len() == 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "one plan item encodes to {} bytes, over {}",
                msg.len(),
                proto::MAX_MSG
            ),
        ));
    }
    let mut items = items;
    let tail = items.split_off(items.len() / 2);
    send_batched(conn, items, wrap)?;
    send_batched(conn, tail, wrap)
}

/// A socket timeout (EAGAIN under SO_RCVTIMEO/SO_SNDTIMEO) as `TimedOut`.
fn timed_out(e: io::Error) -> io::Error {
    if e.kind() == io::ErrorKind::WouldBlock {
        io::Error::new(io::ErrorKind::TimedOut, "push socket timed out")
    } else {
        e
    }
}

fn refused(e: Event) -> io::Error {
    io::Error::other(match e {
        Event::Refused(m) => m,
        Event::Failed { reason, .. } => reason,
        other => format!("unexpected {other:?}"),
    })
}

/// The coordinator's side of a group.
pub struct Coordinator {
    conn: Conn,
    pub group: u64,
    pub token: u64,
}

impl Coordinator {
    /// `timeout` bounds every call on the group's socket from the first
    /// send on (see [`Conn::set_timeout`]); [`Coordinator::set_timeout`]
    /// changes it later.
    pub fn create(
        socket: &str,
        ranks: Vec<RankSpec>,
        files: Vec<String>,
        timeout: Option<std::time::Duration>,
    ) -> io::Result<Coordinator> {
        let conn = Conn::connect_timeout(socket, timeout)?;
        conn.request(
            &Request::Create {
                version: proto::VERSION,
                ranks,
                files,
            },
            None,
        )?;
        match conn.event()? {
            Some(Event::Created { group, token }) => Ok(Coordinator { conn, group, token }),
            Some(e) => Err(refused(e)),
            None => Err(io::Error::other("sparknestd closed the push socket")),
        }
    }

    pub fn set_timeout(&self, t: Option<std::time::Duration>) -> io::Result<()> {
        self.conn.set_timeout(t)
    }

    /// Send patterns (appended in order), in as many messages as needed.
    pub fn patterns(&self, p: Vec<Pattern>) -> io::Result<()> {
        send_batched(&self.conn, p, Request::Patterns)
    }

    /// Send entries (appended), in as many messages as needed.
    pub fn entries(&self, e: Vec<Entry>) -> io::Result<()> {
        send_batched(&self.conn, e, Request::Entries)
    }

    /// Check the plan; streaming starts once every rank has joined.
    /// Progress before Planned is skipped: sparknestd 0.6.3 and earlier
    /// could send some once sources were streaming.
    pub fn start(&self) -> io::Result<Forecast> {
        self.conn.request(&Request::Start, None)?;
        loop {
            match self.conn.event()? {
                Some(Event::Planned(f)) => return Ok(f),
                Some(Event::Progress { .. }) => {}
                Some(e) => return Err(refused(e)),
                None => return Err(io::Error::other("sparknestd closed the push socket")),
            }
        }
    }

    /// Progress, then Done or Failed; `None` if the socket closed.
    pub fn next(&self) -> io::Result<Option<Event>> {
        self.conn.event()
    }
}

/// What a rank joins with.
#[derive(Clone, Copy, Debug)]
pub struct JoinArgs {
    pub group: u64,
    pub token: u64,
    pub rank: u32,
    pub ring_fd: RawFd,
    pub slots: u32,
    pub slot_bytes: u32,
}

/// A rank's side of a group.
pub struct Rank {
    conn: Conn,
}

impl Rank {
    /// The socket, to poll for readability alongside other work (the
    /// engine's CUDA events): readable means `try_next` has an event.
    pub fn fd(&self) -> BorrowedFd<'_> {
        self.conn.fd()
    }

    /// The next event if one is waiting, without blocking.
    pub fn try_next(&self) -> io::Result<Poll> {
        self.conn.try_event()
    }

    /// Join with the ring at `ring_fd` (`slots × slot_bytes`, mappable
    /// MAP_SHARED).
    /// Join `j.group` as `j.rank` with the ring at `j.ring_fd`
    /// (`slots × slot_bytes`, mappable MAP_SHARED). `timeout` bounds every
    /// call from the first send on (see [`Conn::set_timeout`]).
    pub fn join(
        socket: &str,
        j: JoinArgs,
        timeout: Option<std::time::Duration>,
    ) -> io::Result<Rank> {
        let conn = Conn::connect_timeout(socket, timeout)?;
        conn.request(
            &Request::Join {
                version: proto::VERSION,
                group: j.group,
                token: j.token,
                rank: j.rank,
                slots: j.slots,
                slot_bytes: j.slot_bytes,
            },
            Some(j.ring_fd),
        )?;
        match conn.event()? {
            Some(Event::Joined) => Ok(Rank { conn }),
            Some(e) => Err(refused(e)),
            None => Err(io::Error::other("sparknestd closed the push socket")),
        }
    }

    /// Filled slots, then Done or Failed; `None` if the socket closed.
    pub fn next(&self) -> io::Result<Option<Event>> {
        self.conn.event()
    }

    pub fn set_timeout(&self, t: Option<std::time::Duration>) -> io::Result<()> {
        self.conn.set_timeout(t)
    }

    pub fn release(&self, slot: u32) -> io::Result<()> {
        self.conn.request(&Request::Release { slot }, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn memfd() -> OwnedFd {
        nix::sys::memfd::memfd_create(c"nest-push-test", nix::sys::memfd::MFdFlags::MFD_CLOEXEC)
            .unwrap()
    }

    #[test]
    fn messages_and_an_fd_cross_a_socketpair() {
        let (a, b) = Conn::pair().unwrap();
        let f = memfd();
        a.request(
            &Request::Join {
                version: 1,
                group: 7,
                token: 9,
                rank: 3,
                slots: 2,
                slot_bytes: 4096,
            },
            Some(f.as_raw_fd()),
        )
        .unwrap();
        let (r, fd) = b.next_request().unwrap().unwrap();
        assert!(fd.is_some(), "fd passed");
        assert!(matches!(r, Request::Join { rank: 3, .. }));
        assert_eq!(b.peer().unwrap().1, nix::unistd::getuid().as_raw());
        drop(a);
        assert!(b.next_request().unwrap().is_none(), "peer closed");
    }

    /// A message of MAX_MSG crosses with default socket buffers, and large
    /// plans go in batches under it, in order.
    #[test]
    fn big_plans_are_batched_under_the_socket_limit() {
        let (a, b) = Conn::pair().unwrap();
        a.send(&vec![7u8; proto::MAX_MSG], None).unwrap();
        let mut buf = Vec::new();
        b.recv(&mut buf).unwrap().unwrap();
        assert_eq!(buf.len(), proto::MAX_MSG);
        let entries: Vec<Entry> = (0..50_000)
            .map(|i| Entry {
                file: i,
                pattern: 1,
                src_base: u64::MAX - i as u64,
                dst_base: (i as u64) << 20,
                rank: 3,
            })
            .collect();
        let want = entries.clone();
        let t = std::thread::spawn(move || send_batched(&a, entries, Request::Entries));
        let mut got = Vec::new();
        let mut msgs = 0;
        while got.len() < want.len() {
            match b.next_request().unwrap().unwrap().0 {
                Request::Entries(e) => got.extend(e),
                other => panic!("{other:?}"),
            }
            msgs += 1;
        }
        t.join().unwrap().unwrap();
        assert!(msgs > 1);
        assert_eq!(got, want);
    }

    /// Group 1535845564284940: a source's first slots landed before
    /// Planned reached the coordinator, and start() took Progress as a
    /// refusal.
    #[test]
    fn start_skips_progress_before_planned() {
        let (a, b) = Conn::pair().unwrap();
        let g = Coordinator {
            conn: b,
            group: 1,
            token: 2,
        };
        a.send_event(&Event::Progress { done: 1, total: 9 })
            .unwrap();
        a.send_event(&Event::Planned(Forecast::default())).unwrap();
        assert_eq!(g.start().unwrap(), Forecast::default());
        assert!(matches!(
            a.next_request().unwrap(),
            Some((Request::Start, None))
        ));
        a.send_event(&Event::Refused("plan: no".into())).unwrap();
        assert_eq!(g.start().unwrap_err().to_string(), "plan: no");
    }

    #[test]
    fn try_event_does_not_block() {
        let (a, b) = Conn::pair().unwrap();
        assert!(matches!(b.try_event().unwrap(), Poll::Empty));
        a.send_event(&Event::Joined).unwrap();
        assert!(matches!(b.try_event().unwrap(), Poll::Ready(Event::Joined)));
        assert!(matches!(b.try_event().unwrap(), Poll::Empty));
        drop(a);
        assert!(matches!(b.try_event().unwrap(), Poll::Closed));
    }

    #[test]
    fn a_silent_peer_times_out() {
        let (a, _b) = Conn::pair().unwrap();
        a.set_timeout(Some(std::time::Duration::from_millis(50)))
            .unwrap();
        let t = std::time::Instant::now();
        let e = a.event().unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::TimedOut);
        assert!(t.elapsed() < std::time::Duration::from_secs(2));
    }

    #[test]
    fn connect_to_a_listener_that_never_accepts_times_out() {
        let name = format!("@nest-push-test-{}", std::process::id());
        let l = socket::socket(
            AddressFamily::Unix,
            SockType::SeqPacket,
            SockFlag::SOCK_CLOEXEC,
            None,
        )
        .unwrap();
        socket::bind(l.as_raw_fd(), &address(&name).unwrap()).unwrap();
        socket::listen(&l, socket::Backlog::new(0).unwrap()).unwrap();
        // Fill the backlog; then a connect would wait for an accept.
        let mut held = Vec::new();
        let t = std::time::Instant::now();
        let err = loop {
            match Conn::connect_timeout(&name, Some(std::time::Duration::from_millis(100))) {
                Ok(c) => held.push(c),
                Err(e) => break e,
            }
            assert!(held.len() < 64, "backlog never filled");
        };
        assert_eq!(err.kind(), io::ErrorKind::TimedOut, "{err}");
        assert!(t.elapsed() < std::time::Duration::from_secs(5));
    }

    #[test]
    fn listener_accepts_and_sees_the_peer() {
        let name = format!("@nest-push-listen-{}", std::process::id());
        let l = Listener::bind(&name).unwrap();
        let c = Conn::connect_timeout(&name, Some(std::time::Duration::from_secs(1))).unwrap();
        let s = l.accept().unwrap();
        assert_eq!(s.peer().unwrap().0, std::process::id() as i32);
        c.send(b"x", None).unwrap();
        let mut b = Vec::new();
        assert!(s.recv(&mut b).unwrap().is_some());
        assert_eq!(b, b"x");
    }

    #[test]
    fn abstract_and_path_addresses() {
        assert!(address("@sparknest-push").unwrap().as_abstract().is_some());
        assert!(address("/run/x.sock").unwrap().path().is_some());
    }
}
