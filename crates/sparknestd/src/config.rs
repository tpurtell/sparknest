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
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FabricSection {
    #[serde(default)]
    pub mode: FabricMode,
    /// Optional verbs device / netdev / address filter (e.g. `["mlx5_0"]`).
    #[serde(default)]
    pub devices: Vec<String>,
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
    /// When a sealed file with a local copy is handed to the kernel:
    /// "always", "sole" (only when no other host holds a copy, so reads of
    /// replicated files can be spread over every copy) or "never".
    #[serde(default)]
    pub passthrough: nest_data::Passthrough,
    /// Spread reads of files with several copies over them (ADR-030).
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_config_parses() {
        let text = include_str!("../../../packaging/config/node.toml.sample");
        let cfg: Config = toml::from_str(text).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.cluster.members.len(), 7);
    }
}
