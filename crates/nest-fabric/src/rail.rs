//! Rail discovery: every (verbs device, RoCE v2 GID) whose IPv4 address is
//! configured on an UP netdevice of that device, on an ACTIVE Ethernet port.
//!
//! Enumerating GIDs rather than netdevices matters: raptor's single 400G
//! port carries both 10.55.0.22 and 10.55.1.22, which become two rails that
//! pair with a Spark's two functions by subnet (ported from rdmapipe's
//! discovery, generalized to several addresses per netdevice).

use serde::{Deserialize, Serialize};
use std::fs;
use std::net::Ipv4Addr;
use std::path::Path;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rail {
    pub ibdev: String,
    pub netdev: String,
    pub port: u8,
    pub gid_index: u32,
    pub gid: [u8; 16],
    pub addr: Ipv4Addr,
    pub prefix: u8,
    /// Advertised port rate in Gb/s.
    pub rate_gbps: u32,
}

impl Rail {
    /// Rails pair when their addresses share a subnet.
    pub fn same_subnet(&self, other: &Rail) -> bool {
        let p = self.prefix.min(other.prefix);
        let mask = if p == 0 {
            0
        } else {
            u32::MAX << (32 - p as u32)
        };
        (u32::from(self.addr) & mask) == (u32::from(other.addr) & mask)
    }
}

fn read(p: impl AsRef<Path>) -> Option<String> {
    fs::read_to_string(p).ok().map(|s| s.trim().to_string())
}

/// IPv4 addresses (with prefix length) configured on each UP netdevice.
fn ipv4_by_netdev() -> Vec<(String, Ipv4Addr, u8)> {
    let mut out = Vec::new();
    let Ok(addrs) = nix::ifaddrs::getifaddrs() else {
        return out;
    };
    for a in addrs {
        if !a.flags.contains(nix::net::if_::InterfaceFlags::IFF_UP) {
            continue;
        }
        let (Some(addr), Some(mask)) = (a.address, a.netmask) else {
            continue;
        };
        let (Some(ip), Some(m)) = (addr.as_sockaddr_in(), mask.as_sockaddr_in()) else {
            continue;
        };
        out.push((
            a.interface_name.clone(),
            ip.ip(),
            u32::from(m.ip()).count_ones() as u8,
        ));
    }
    out
}

/// Discover rails. `filter` (verbs device, netdev or address names) narrows
/// the result when non-empty.
pub fn discover(filter: &[String]) -> Vec<Rail> {
    let mut rails = Vec::new();
    let v4 = ipv4_by_netdev();
    let Ok(devs) = fs::read_dir("/sys/class/infiniband") else {
        return rails;
    };
    for d in devs.flatten() {
        let ibdev = d.file_name().to_string_lossy().into_owned();
        let base = d.path().join("ports/1");
        if !read(base.join("state")).is_some_and(|s| s.starts_with("4:")) {
            continue;
        }
        if read(base.join("link_layer")).as_deref() != Some("Ethernet") {
            continue;
        }
        let rate = read(base.join("rate"))
            .and_then(|r| r.split_whitespace().next().and_then(|n| n.parse().ok()))
            .unwrap_or(0);
        let Ok(ndevs) = fs::read_dir(base.join("gid_attrs/ndevs")) else {
            continue;
        };
        for g in ndevs.flatten() {
            let Ok(idx) = g.file_name().to_string_lossy().parse::<u32>() else {
                continue;
            };
            let Some(netdev) = read(g.path()) else {
                continue;
            };
            if read(base.join(format!("gid_attrs/types/{idx}"))).as_deref() != Some("RoCE v2") {
                continue;
            }
            let Some(gid_text) = read(base.join(format!("gids/{idx}"))) else {
                continue;
            };
            let Ok(gid6) = gid_text.parse::<std::net::Ipv6Addr>() else {
                continue;
            };
            let Some(ip) = gid6.to_ipv4_mapped() else {
                continue;
            };
            let Some((_, _, prefix)) = v4.iter().find(|(n, a, _)| *n == netdev && *a == ip) else {
                continue;
            };
            let rail = Rail {
                ibdev: ibdev.clone(),
                netdev: netdev.clone(),
                port: 1,
                gid_index: idx,
                gid: gid6.octets(),
                addr: ip,
                prefix: *prefix,
                rate_gbps: rate,
            };
            let wanted = filter.is_empty()
                || filter
                    .iter()
                    .any(|f| *f == rail.ibdev || *f == rail.netdev || *f == rail.addr.to_string());
            if wanted {
                rails.push(rail);
            }
        }
    }
    rails.sort_by_key(|r| r.addr);
    rails
}
