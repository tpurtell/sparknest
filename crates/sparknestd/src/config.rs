//! `node.toml`: identity, cluster membership, listeners, paths.

use anyhow::{Context, Result, bail};
use nest_types::NodeId;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub node: NodeSection,
    pub cluster: ClusterSection,
    #[serde(default)]
    pub fabric: FabricSection,
    #[serde(default)]
    pub fuse: FuseSection,
    #[serde(default)]
    pub hf: HfSection,
    #[serde(default)]
    pub push: PushSection,
}

/// Push loads (ADR-063): the socket engines connect to.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushSection {
    /// `@name` for an abstract socket (reachable from containers sharing
    /// the host's network namespace), else a path. Empty: no push.
    #[serde(default = "default_push_socket")]
    pub socket: String,
}

fn default_push_socket() -> String {
    nest_push::proto::SOCKET.to_string()
}

impl Default for PushSection {
    fn default() -> Self {
        PushSection {
            socket: default_push_socket(),
        }
    }
}

/// Hugging Face: where HF_HOME lives in the namespace. Point HF_HOME at the
/// same path under the mount (e.g. /mnt/sparknest/hf-home); huggingface_hub
/// keeps its cache in `<home>/hub`, and also writes tokens and the Xet
/// cache under HF_HOME, which is why it is a directory of its own and not
/// the namespace root.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HfSection {
    #[serde(default = "default_hf_home")]
    pub home: String,
}

impl Default for HfSection {
    fn default() -> Self {
        HfSection {
            home: default_hf_home(),
        }
    }
}

fn default_hf_home() -> String {
    "/hf-home".into()
}

impl HfSection {
    /// The hub cache inside HF_HOME, as a namespace path.
    pub fn hub(&self) -> String {
        format!("{}/hub", self.home.trim_end_matches('/'))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSection {
    pub id: NodeId,
    pub name: String,
    /// Root of all node-local state (databases, objects, staging).
    pub state_dir: PathBuf,
    /// Where the filesystem is mounted. Omit to run without a mount
    /// (metadata/data service only).
    pub mountpoint: Option<PathBuf>,
    /// Control-plane listener: Raft RPC, data-service control, fabric bootstrap.
    pub listen: SocketAddr,
    /// Management API listener on the network (bearer auth). The Unix
    /// socket `<state_dir>/api.sock` is always served.
    pub api_listen: Option<SocketAddr>,
    /// Space object data must leave free on the state directory's
    /// filesystem for metadata and the Raft log (writes past it get ENOSPC).
    #[serde(default = "default_reserve_gib")]
    pub data_reserve_gib: u64,
    /// `fabric` (default) or `offgrid`: a host reaching the cluster over
    /// the LAN only (ADR-047). It joins as a learner, dials the members
    /// listed here at their LAN addresses and keeps those connections
    /// open, and reads its own copies first.
    #[serde(default)]
    pub role: Role,
    /// Where TCP lanes to off-grid hosts are accepted (ADR-048); defaults
    /// to `listen` with the port plus 2.
    #[serde(default)]
    pub lanes_listen: Option<SocketAddr>,
    /// TCP lanes this host keeps to each host it dials (off-grid hosts).
    #[serde(default = "default_lanes")]
    pub lanes: usize,
    /// Standby TCP lanes between fabric hosts on the LAN (ADR-057), used
    /// when the RDMA link to the peer is down; dialled by the lower id of
    /// each pair, to members with a `lan` address. 0: none.
    #[serde(default = "default_lanes_fabric")]
    pub lanes_fabric: usize,
    /// RDMA counters as JSON over HTTP (ADR-051); loopback only. The
    /// binary stream is always at `<state_dir>/run/rdma-counters.sock` (ADR-052).
    #[serde(default = "default_counters_listen")]
    pub counters_listen: Option<SocketAddr>,
}

fn default_counters_listen() -> Option<SocketAddr> {
    Some(SocketAddr::from(([127, 0, 0, 1], 7403)))
}

fn default_lanes() -> usize {
    4
}

fn default_lanes_fabric() -> usize {
    1
}

impl NodeSection {
    pub fn lanes_listen(&self) -> SocketAddr {
        self.lanes_listen
            .unwrap_or_else(|| port_plus_2(self.listen))
    }
}

pub(crate) fn port_plus_2(a: SocketAddr) -> SocketAddr {
    SocketAddr::new(a.ip(), a.port().wrapping_add(2))
}

/// Where a host sits (ADR-047).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// On the RDMA fabric; votes.
    #[default]
    Fabric,
    /// LAN only; a Raft learner that dials out.
    Offgrid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterSection {
    /// Distinguishes clusters sharing hosts (e.g. `trial` vs `prod`).
    pub name: String,
    /// File holding the shared secret for control-plane authentication.
    pub secret_file: PathBuf,
    /// Initial membership used only to bootstrap a brand-new cluster.
    pub members: Vec<Member>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Member {
    pub id: NodeId,
    pub name: String,
    pub addr: SocketAddr,
    #[serde(default = "default_true")]
    pub voter: bool,
    /// Its TCP lanes address, when it is not `addr` with the port plus 2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lanes: Option<SocketAddr>,
    /// Its 10G LAN address (port as `addr`): the standby for the control
    /// connection and data lanes when the fabric fails (ADR-057). Fabric
    /// hosts only; an off-grid host's `addr` already is a LAN address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lan: Option<SocketAddr>,
}

impl Member {
    pub fn lanes(&self) -> SocketAddr {
        self.lanes.unwrap_or_else(|| port_plus_2(self.addr))
    }

    /// Its standby lanes address on the LAN: `lan` with the port plus 2.
    pub fn lan_lanes(&self) -> Option<SocketAddr> {
        self.lan.map(port_plus_2)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FabricSection {
    #[serde(default)]
    pub mode: FabricMode,
    /// Optional verbs device / netdev / address filter (e.g. `["mlx5_0"]`).
    #[serde(default)]
    pub devices: Vec<String>,
    /// Deprecated: the incast cap as a count of 4 MiB chunks (read as
    /// `inflight_mib = 4 × max_inflight`). 0: unset.
    #[serde(default)]
    pub max_inflight: u32,
    /// Remote reads outstanding at once from this host, MiB (the incast
    /// cap). 0: 2 ms of this host's links, at least 64 MiB.
    #[serde(default)]
    pub inflight_mib: u64,
}

impl FabricSection {
    /// The incast cap in bytes (0: the fabric's default).
    pub fn inflight_bytes(&self) -> u64 {
        match (self.inflight_mib, self.max_inflight) {
            (0, 0) => 0,
            (0, n) => n as u64 * (4 << 20),
            (m, _) => m << 20,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FabricMode {
    /// RDMA when both ends can, TCP otherwise.
    #[default]
    Auto,
    Rdma,
    Tcp,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FuseSection {
    #[serde(default = "default_true")]
    pub allow_other: bool,
    /// Kernel entry/attr cache TTL in milliseconds.
    #[serde(default = "default_ttl_ms")]
    pub ttl_ms: u64,
    /// FUSE over io_uring when the kernel offers it (Linux 6.14+ with
    /// `fuse.enable_uring=1`); otherwise /dev/fuse, with a warning.
    #[serde(default = "default_true")]
    pub io_uring: bool,
    /// Mount even if the mountpoint directory has files in it (they would be
    /// hidden under the mount). Off: such a mount is refused and reported.
    #[serde(default)]
    pub allow_nonempty: bool,
    /// Move files written into the bare mountpoint aside and import them into
    /// /.lost+found/<run>/<host>/unmounted/ instead of refusing to mount
    /// (up to 16 GiB / 200k entries).
    #[serde(default = "default_true")]
    pub rescue_unmounted: bool,
    /// Whether a sealed file with a local copy is handed to the kernel:
    /// "always" or "never" ("sole" is read as "always" since ADR-065: the
    /// daemon no longer spreads a local file's reads over other copies).
    #[serde(default)]
    pub passthrough: nest_data::Passthrough,
    /// Retired (ADR-065): reads are no longer spread by the daemon's
    /// readahead. Accepted so existing configs load.
    #[serde(default = "default_true")]
    pub balance_reads: bool,
}

impl Default for FuseSection {
    fn default() -> Self {
        Self {
            allow_other: true,
            ttl_ms: default_ttl_ms(),
            io_uring: true,
            allow_nonempty: false,
            rescue_unmounted: true,
            passthrough: Default::default(),
            balance_reads: true,
        }
    }
}

fn default_reserve_gib() -> u64 {
    4
}

fn default_true() -> bool {
    true
}
fn default_ttl_ms() -> u64 {
    1000
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let cfg: Config =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        let mut ids: Vec<_> = self.cluster.members.iter().map(|m| m.id).collect();
        ids.sort();
        ids.dedup();
        if ids.len() != self.cluster.members.len() {
            bail!("duplicate node ids in cluster.members");
        }
        if self.node.id.0 == 0 {
            bail!("node.id must be non-zero");
        }
        if let Some(a) = self.node.counters_listen
            && !a.ip().is_loopback()
        {
            bail!(
                "node.counters_listen must be a loopback address (got {a}): the counters are unauthenticated"
            );
        }
        if self.node.role == Role::Offgrid {
            if self
                .cluster
                .members
                .iter()
                .any(|m| m.id == self.node.id && m.voter)
            {
                bail!(
                    "an off-grid host (node.role = \"offgrid\") cannot be a voter: set voter = false"
                );
            }
            if !self.cluster.members.iter().any(|m| m.id != self.node.id) {
                bail!(
                    "an off-grid host needs the fabric hosts in cluster.members, at their LAN addresses"
                );
            }
        }
        // sockaddr_un holds 108 bytes including the terminating NUL.
        let sock = self.api_socket();
        if sock.as_os_str().len() >= 108 {
            bail!(
                "node.state_dir is too long for the management socket {} (Unix socket paths are limited to 107 bytes)",
                sock.display()
            );
        }
        Ok(())
    }

    pub fn api_socket(&self) -> PathBuf {
        self.node.state_dir.join("api.sock")
    }

    /// The RDMA counters stream (ADR-051).
    pub fn counters_socket(&self) -> PathBuf {
        self.node.state_dir.join("run").join("rdma-counters.sock")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offgrid_sample_parses_and_offgrid_hosts_cannot_vote() {
        let text = include_str!("../../../packaging/config/node-offgrid.toml.sample");
        let cfg: Config = toml::from_str(text).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.node.role, Role::Offgrid);
        let voting = text.replace(
            r#"addr = "172.22.2.7:7400", voter = false"#,
            r#"addr = "172.22.2.7:7400""#,
        );
        let cfg: Config = toml::from_str(&voting).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn members_may_name_a_lan_standby() {
        let text = include_str!("../../../packaging/config/node.toml.sample");
        let cfg: Config = toml::from_str(text).unwrap();
        let ostrich = &cfg.cluster.members[1];
        assert_eq!(ostrich.lan, Some("172.22.2.1:7410".parse().unwrap()));
        assert_eq!(
            ostrich.lan_lanes(),
            Some("172.22.2.1:7412".parse().unwrap())
        );
        assert_eq!(cfg.node.lanes_fabric, 1);
        // A 0.4 config (no `lan`) still parses, and writes none back.
        let old = r#"
            [node]
            id = 1
            name = "a"
            state_dir = "/tmp/x"
            listen = "10.55.0.22:7400"
            [cluster]
            name = "c"
            secret_file = "/tmp/s"
            members = [{ id = 1, name = "a", addr = "10.55.0.22:7400" }]
        "#;
        let cfg: Config = toml::from_str(old).unwrap();
        assert_eq!(cfg.cluster.members[0].lan, None);
        assert!(!toml::to_string(&cfg).unwrap().contains("lan ="));
    }

    #[test]
    fn sample_config_parses() {
        let text = include_str!("../../../packaging/config/node.toml.sample");
        let cfg: Config = toml::from_str(text).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.cluster.members.len(), 7);
    }
}
