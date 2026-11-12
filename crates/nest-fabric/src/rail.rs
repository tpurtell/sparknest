//! Rail discovery: every (verbs device, RoCE v2 GID) whose IPv4 address is
//! configured on an UP netdevice of that device, on an ACTIVE Ethernet port.
//!
//! Enumerating GIDs rather than netdevices matters: raptor's single 400G
//! port carries both 10.55.0.22 and 10.55.1.22, which become two rails that
//! pair with a Spark's two functions by subnet (ported from rdmapipe's
//! discovery, generalized to several addresses per netdevice).
//!
//! Functions sharing one physical port are used only as far as the port
//! needs (`trim`): a Spark's single cable reaches two PCIe functions, each
//! capped by its PCIe x4 link (~115 Gb/s). At 100 Gb one function carries
//! the whole port; at 200 Gb both are needed. Each port's rate counts once
//! toward the host's link rate (`link_gbps`).

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

impl Rail {
    /// The GID table index this rail's GID has now. The kernel numbers a
    /// port's IP-derived GIDs as it adds them, so a link flap that drops and
    /// regains the address can move it (a Spark's IPv4 RoCE v2 GID sits at 3
    /// on some hosts, 4 on others). Connecting with the index found at start
    /// then names an empty entry, and every QP fails RTR with ENODATA:
    /// rhea, after the switch lost power on 2026-11-11, until a restart.
    /// Falls back to the index found at start where it cannot be read.
    pub fn current_gid_index(&self) -> u32 {
        let base = Path::new("/sys/class/infiniband")
            .join(&self.ibdev)
            .join(format!("ports/{}", self.port));
        gid_index_in(&base, &self.gid).unwrap_or(self.gid_index)
    }
}

/// The RoCE v2 entry of the port at `base` (`.../ports/N`) holding `gid`.
fn gid_index_in(base: &Path, gid: &[u8; 16]) -> Option<u32> {
    let want = std::net::Ipv6Addr::from(*gid);
    let mut found: Vec<u32> = fs::read_dir(base.join("gids"))
        .ok()?
        .flatten()
        .filter_map(|e| e.file_name().to_string_lossy().parse::<u32>().ok())
        .filter(|i| {
            read(base.join(format!("gid_attrs/types/{i}"))).as_deref() == Some("RoCE v2")
                && read(base.join(format!("gids/{i}")))
                    .and_then(|t| t.parse::<std::net::Ipv6Addr>().ok())
                    == Some(want)
        })
        .collect();
    found.sort_unstable();
    found.first().copied()
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
    if filter.is_empty() {
        trim(rails)
    } else {
        rails
    }
}

/// The physical port a device's port 1 is on: functions of one NIC (same
/// switch id) with the same PCI function number share it.
fn port_key(ibdev: &str) -> String {
    let dev = Path::new("/sys/class/infiniband")
        .join(ibdev)
        .join("device");
    let func = fs::canonicalize(&dev)
        .ok()
        .and_then(|p| p.file_name().map(|f| f.to_string_lossy().into_owned()))
        .and_then(|pci| pci.rsplit('.').next().map(str::to_string));
    let switch = fs::read_dir(dev.join("net"))
        .ok()
        .and_then(|mut d| d.next())
        .and_then(|n| n.ok())
        .and_then(|n| read(n.path().join("phys_switch_id")))
        .filter(|s| !s.is_empty());
    match (switch, func) {
        (Some(s), Some(f)) => format!("{s}/{f}"),
        _ => ibdev.to_string(),
    }
}

/// A device's PCIe bandwidth in Gb/s: lanes × transfer rate, 90% of it
/// usable (0: unknown).
fn pcie_gbps(ibdev: &str) -> u32 {
    let dev = Path::new("/sys/class/infiniband")
        .join(ibdev)
        .join("device");
    let gts: f64 = read(dev.join("current_link_speed"))
        .and_then(|s| s.split_whitespace().next().and_then(|n| n.parse().ok()))
        .unwrap_or(0.0);
    let width: f64 = read(dev.join("current_link_width"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    (gts * width * 0.9) as u32
}

/// One device's part in choosing: its port, the port's rate, its PCIe
/// capacity (Gb/s), and its lowest rail address (the tie-break every host
/// shares, so hosts pick matching subnets).
#[derive(Clone, Debug)]
struct DevInfo {
    ibdev: String,
    port: String,
    rate: u32,
    pcie: u32,
    first: Ipv4Addr,
}

/// Per port, the fewest devices (lowest addresses first) whose PCIe
/// capacity covers the port's rate; unknown capacity keeps them all.
fn choose(devs: &[DevInfo]) -> std::collections::HashSet<String> {
    let mut by_port: std::collections::BTreeMap<&str, Vec<&DevInfo>> = Default::default();
    for d in devs {
        by_port.entry(d.port.as_str()).or_default().push(d);
    }
    let mut keep = std::collections::HashSet::new();
    for (_, mut ds) in by_port {
        ds.sort_by_key(|d| d.first);
        let rate = ds.iter().map(|d| d.rate).max().unwrap_or(0);
        let mut cap = 0u32;
        for d in &ds {
            keep.insert(d.ibdev.clone());
            if d.pcie == 0 {
                continue;
            }
            cap += d.pcie;
            if cap >= rate {
                break;
            }
        }
    }
    keep
}

fn infos(rails: &[Rail]) -> Vec<DevInfo> {
    let mut out: Vec<DevInfo> = Vec::new();
    for r in rails {
        match out.iter_mut().find(|d| d.ibdev == r.ibdev) {
            Some(d) => d.first = d.first.min(r.addr),
            None => out.push(DevInfo {
                ibdev: r.ibdev.clone(),
                port: port_key(&r.ibdev),
                rate: r.rate_gbps,
                pcie: pcie_gbps(&r.ibdev),
                first: r.addr,
            }),
        }
    }
    out
}

/// Drop rails of functions the port does not need (see the module doc).
pub fn trim(rails: Vec<Rail>) -> Vec<Rail> {
    let devs = infos(&rails);
    let keep = choose(&devs);
    for d in devs.iter().filter(|d| !keep.contains(&d.ibdev)) {
        tracing::info!(
            device = %d.ibdev,
            port_gbit = d.rate,
            "not using this RDMA function: another on the same port carries its rate"
        );
    }
    rails
        .into_iter()
        .filter(|r| keep.contains(&r.ibdev))
        .collect()
}

/// This host's link rate in Gb/s: each physical port once.
pub fn link_gbps(rails: &[Rail]) -> u64 {
    let mut ports = std::collections::HashMap::new();
    for d in infos(rails) {
        let e = ports.entry(d.port).or_insert(0u32);
        *e = (*e).max(d.rate);
    }
    ports.values().map(|r| *r as u64).sum()
}

#[cfg(test)]
mod choose_tests {
    use super::*;

    fn dev(ibdev: &str, port: &str, rate: u32, pcie: u32, first: [u8; 4]) -> DevInfo {
        DevInfo {
            ibdev: ibdev.into(),
            port: port.into(),
            rate,
            pcie,
            first: Ipv4Addr::from(first),
        }
    }

    #[test]
    fn a_spark_uses_one_function_at_100g_and_both_at_200g() {
        let at = |rate| {
            let mut k: Vec<String> = choose(&[
                dev("roceP2p1s0f0", "sw/0", rate, 115, [10, 55, 1, 1]),
                dev("rocep1s0f0", "sw/0", rate, 115, [10, 55, 0, 1]),
            ])
            .into_iter()
            .collect();
            k.sort();
            k
        };
        assert_eq!(
            at(100),
            vec!["rocep1s0f0"],
            "the lower subnet, on every host"
        );
        assert_eq!(at(200), vec!["roceP2p1s0f0", "rocep1s0f0"]);
    }

    /// A fake `ports/1`: (index, gid text, type) per entry.
    fn port(entries: &[(u32, &str, &str)]) -> tempfile::TempDir {
        let d = tempfile::tempdir().unwrap();
        fs::create_dir_all(d.path().join("gids")).unwrap();
        fs::create_dir_all(d.path().join("gid_attrs/types")).unwrap();
        for i in 0..8 {
            let (g, t) = entries
                .iter()
                .find(|e| e.0 == i)
                .map_or(("0000:0000:0000:0000:0000:0000:0000:0000", ""), |e| {
                    (e.1, e.2)
                });
            fs::write(d.path().join(format!("gids/{i}")), format!("{g}\n")).unwrap();
            fs::write(
                d.path().join(format!("gid_attrs/types/{i}")),
                format!("{t}\n"),
            )
            .unwrap();
        }
        d
    }

    #[test]
    fn a_gid_moved_by_a_link_flap_is_found_at_its_new_index() {
        let v4 = "0000:0000:0000:0000:0000:ffff:0a37:0005";
        let gid = "::ffff:10.55.0.5"
            .parse::<std::net::Ipv6Addr>()
            .unwrap()
            .octets();
        let ll = "fe80:0000:0000:0000:4a21:0bff:fe9d:77bf";
        // At start: v1 at 2, v2 at 3.
        let before = port(&[
            (0, ll, "IB/RoCE v1"),
            (1, ll, "RoCE v2"),
            (2, v4, "IB/RoCE v1"),
            (3, v4, "RoCE v2"),
        ]);
        assert_eq!(gid_index_in(before.path(), &gid), Some(3));
        // After the flap: re-added with v2 at 4, 3 left empty (kiwi, ostrich).
        let after = port(&[
            (0, ll, "IB/RoCE v1"),
            (1, ll, "RoCE v2"),
            (2, v4, "IB/RoCE v1"),
            (4, v4, "RoCE v2"),
        ]);
        assert_eq!(gid_index_in(after.path(), &gid), Some(4));
        // Gone altogether (link down): none, and the caller keeps its own.
        let gone = port(&[(0, ll, "IB/RoCE v1"), (1, ll, "RoCE v2")]);
        assert_eq!(gid_index_in(gone.path(), &gid), None);
    }

    #[test]
    fn separate_ports_and_unknown_capacity_are_kept() {
        let k = choose(&[
            dev("mlx5_0", "a/0", 400, 460, [10, 55, 0, 22]),
            dev("mlx5_1", "b/0", 100, 0, [10, 56, 0, 22]),
        ]);
        assert_eq!(k.len(), 2);
    }
}
