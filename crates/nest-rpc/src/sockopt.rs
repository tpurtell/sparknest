//! Socket options for sparknest's TCP connections.
//!
//! Retransmit floor (ADR-056): Linux waits at least 200 ms before
//! retransmitting a lost segment. A Raft heartbeat is one small segment with
//! a budget shorter than that, so on a fabric that drops packets under
//! incast every lost one became a timeout. `TCP_RTO_MIN_US` (Linux 6.x)
//! lowers the floor for one socket; older kernels (WSL, off-grid hosts)
//! refuse it and keep the default.
//!
//! Congestion control and DSCP (ADR-088): every connection runs cubic,
//! whatever the host default, and is marked by what it carries, so a switch
//! can queue control ahead of bulk. BBR (v1) as a host default ignored loss
//! and ECN: 28 bulk lanes from one source kept a shared 10G link full and
//! dropping, and off-grid hosts lost their leases.

use nix::sys::socket::{setsockopt, sockopt};
use nix::{setsockopt_impl, sockopt_impl};
use std::ffi::OsString;
use std::os::fd::AsFd;
use std::time::Duration;

/// Not yet in the libc crate: include/uapi/linux/tcp.h.
const TCP_RTO_MIN_US: libc::c_int = 45;

sockopt_impl!(
    RtoMinUs,
    SetOnly,
    libc::IPPROTO_TCP,
    TCP_RTO_MIN_US,
    libc::c_int,
    nix::sys::socket::sockopt::SetStruct<libc::c_int>
);

/// The retransmit floor on sparknest's peer connections.
pub const RTO_MIN: Duration = Duration::from_millis(5);

/// Lower a connection's retransmit floor; false where the kernel refuses.
pub fn set_rto_min(fd: &impl AsFd, rto: Duration) -> bool {
    let us = rto.as_micros().min(libc::c_int::MAX as u128) as libc::c_int;
    setsockopt(fd, RtoMinUs, &us).is_ok()
}

/// What a connection carries, which sets its DSCP.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Traffic {
    /// Raft, leases, metadata, admin: small and latency-bound. AF41 (34).
    Control,
    /// TCP data lanes: reads, copies, push slots. CS1 (8), "lower effort".
    Bulk,
}

impl Traffic {
    /// The DSCP value.
    pub fn dscp(self) -> u8 {
        match self {
            Traffic::Control => 34,
            Traffic::Bulk => 8,
        }
    }
}

/// The congestion control every sparknest connection asks for. Built into
/// every kernel it runs on, backs off on loss and on ECN marks.
pub const CONGESTION: &str = "cubic";

/// Cubic and `traffic`'s DSCP on a connection, keeping the ECN bits the
/// kernel sets. Each is best effort: a kernel that refuses one keeps its
/// default, and the connection still works. Returns what took.
pub fn tune(fd: &impl AsFd, traffic: Traffic) -> (bool, bool) {
    let cc = setsockopt(fd, sockopt::TcpCongestion, &OsString::from(CONGESTION)).is_ok();
    let tos = (traffic.dscp() as libc::c_int) << 2;
    // An IPv4 socket takes IP_TOS, an IPv6 one IPV6_TCLASS (which also
    // marks IPv4-mapped peers); one of the two applies.
    let v4 = setsockopt(fd, sockopt::Ipv4Tos, &tos).is_ok();
    let v6 = setsockopt(fd, sockopt::Ipv6TClass, &tos).is_ok();
    (cc, v4 || v6)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix::sys::socket::getsockopt;

    #[test]
    fn the_floor_is_set_where_the_kernel_supports_it() {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let s = std::net::TcpStream::connect(l.local_addr().unwrap()).unwrap();
        let set = set_rto_min(&s, RTO_MIN);
        // Linux 6.x and later honour it; the build hosts all do.
        let (major, minor) = kernel();
        if (major, minor) >= (6, 11) {
            assert!(set, "kernel {major}.{minor} refused TCP_RTO_MIN_US");
        }
    }

    #[test]
    fn connections_run_cubic_and_carry_their_class() {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        for t in [Traffic::Control, Traffic::Bulk] {
            let s = std::net::TcpStream::connect(l.local_addr().unwrap()).unwrap();
            assert_eq!(tune(&s, t), (true, true));
            let cc = getsockopt(&s, sockopt::TcpCongestion).unwrap();
            assert_eq!(cc.to_string_lossy().trim_end_matches('\0'), CONGESTION);
            let tos = getsockopt(&s, sockopt::Ipv4Tos).unwrap();
            assert_eq!(tos >> 2, t.dscp() as libc::c_int, "{t:?}");
        }
    }

    fn kernel() -> (u32, u32) {
        let r = nix::sys::utsname::uname().unwrap();
        let rel = r.release().to_string_lossy().into_owned();
        let mut p = rel
            .split(|c: char| !c.is_ascii_digit())
            .filter_map(|x| x.parse().ok());
        (p.next().unwrap_or(0), p.next().unwrap_or(0))
    }
}
