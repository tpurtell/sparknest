//! `nest`: the sparknest command line. Talks only to the local daemon's
//! management API (Unix socket).

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use std::path::PathBuf;
use std::time::Duration;

mod push_bench;

#[derive(Parser, Debug)]
#[command(name = "nest", version, about = "sparknest management CLI")]
struct Cli {
    /// Management socket (default: <state_dir>/api.sock from node.toml).
    #[arg(long, env = "SPARKNEST_SOCKET", global = true)]
    socket: Option<PathBuf>,
    /// Node config used to find the socket.
    #[arg(long, env = "SPARKNEST_CONFIG", global = true)]
    config: Option<PathBuf>,
    /// Print raw JSON.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    cmd: Cmd,
}

// Parsed once per run: the size of the largest variant does not matter.
#[allow(clippy::large_enum_variant)]
#[derive(Subcommand, Debug)]
enum Cmd {
    /// Cluster, node and store overview.
    Status,
    /// Print a link to this node's web UI (with its access token).
    Ui,
    /// List a directory (or one file) with where each file's copies live.
    Ls { path: String },
    /// Seal (or unseal) files: enforced immutability; enables passthrough.
    Seal {
        path: String,
        #[arg(short, long)]
        recursive: bool,
        #[arg(long)]
        unseal: bool,
    },
    /// Always hand a file, tree or model to the kernel where a host has it all
    ///
    /// Hosts with a complete local copy of a marked file give it to the
    /// kernel (passthrough) whatever their `passthrough` policy: reads skip
    /// the daemon, for the lowest latency, but are never spread over other
    /// copies. `default` returns files to each host's policy; with neither,
    /// prints how many of the selection's files are marked. A selection is
    /// a path or hf:org/name[@revision] (hf-dataset: for datasets).
    Passthrough {
        selector: String,
        #[arg(value_parser = ["always", "default"])]
        mode: Option<String>,
    },
    /// Remove files or trees in bulk (far faster than `rm -r` through the mount)
    ///
    /// Many entries go into each metadata commit: tens of thousands of
    /// entries a second, against a few hundred through the mount.
    Rm {
        #[arg(required = true)]
        paths: Vec<String>,
        #[arg(short, long)]
        recursive: bool,
        /// Count what would be removed; change nothing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Wait until this host serves and its mount is up
    ///
    /// For services that depend on sparknest, e.g. in a systemd unit:
    /// `ExecStartPre=nest wait-ready`.
    WaitReady {
        /// Give up after this many seconds (0: wait forever).
        #[arg(long, default_value_t = 0)]
        timeout: u64,
    },
    /// Check this host's objects against the metadata; report by default.
    Fsck {
        /// Apply the standard resolutions (never deletes data that might be
        /// the only copy: such objects go to /.lost+found/<host>/).
        #[arg(short = 'y', long = "yes")]
        yes: bool,
        /// Also verify Hugging Face blobs against their SHA-256; copies
        /// verified by an earlier run are skipped.
        #[arg(long)]
        deep: bool,
        /// With --deep: hash copies already verified again.
        #[arg(long, requires = "deep")]
        recheck: bool,
    },
    /// Set a directory's automatic sealing policy.
    Policy {
        path: String,
        /// off | incomplete (seal on rename from *.incomplete, for HF) | finalize | inherit
        policy: String,
    },
    /// Which hosts hold a complete copy of a selection
    ///
    /// A selection is a path, or hf:org/name[@revision] (hf-dataset: for
    /// datasets).
    Where {
        selector: String,
        #[arg(long, value_delimiter = ',')]
        hosts: Vec<String>,
    },
    /// Copy a selection to hosts (whole files, over RDMA).
    Replicate {
        selector: String,
        #[arg(long, value_delimiter = ',', required = true)]
        hosts: Vec<String>,
        #[arg(long, default_value_t = 8)]
        parallel: usize,
        /// Follow progress until the job finishes.
        #[arg(long)]
        wait: bool,
    },
    /// Remove copies of a selection from hosts (never the last copy).
    Evict {
        selector: String,
        #[arg(long, value_delimiter = ',', required = true)]
        hosts: Vec<String>,
    },
    /// Placement rules.
    Rule {
        #[command(subcommand)]
        cmd: RuleCmd,
    },
    /// Plan changes toward a goal: make room, tidy up, or speed up
    ///
    /// A plan only proposes and says why for every file; `nest plan apply
    /// ID` carries it out. Rule-required copies and the last copy of a file
    /// are never removed.
    #[command(after_long_help = PLAN_EXAMPLES)]
    Plan {
        #[command(subcommand)]
        cmd: Option<PlanCmd>,
        /// HOST_OR_@GROUP=SIZE: target free space (repeatable; a bare number is
        /// GiB). Above a host's free space it sheds; below, the host may take
        /// sole copies other hosts shed (room granted), before any archive.
        #[arg(long)]
        free: Vec<String>,
        /// Remove copies not opened on their host within this window (1d, 7d, 30d, 1w, 1m).
        #[arg(long, value_name = "WINDOW", conflicts_with_all = ["free", "speedup"])]
        tidy: Option<String>,
        /// Add copies where hosts read files over the network within this window.
        #[arg(long, value_name = "WINDOW", conflicts_with = "free")]
        speedup: Option<String>,
        /// Move a selection to one host (--host, or the one that used it most)
        /// and remove the other hosts' copies.
        #[arg(long, value_name = "SELECTOR", conflicts_with_all = ["free", "tidy", "speedup"])]
        consolidate: Option<String>,
        /// Place a selection on hosts (--host, default all) with --factor
        /// copies: each file on factor x hosts of them, the split kept even,
        /// hosts that read a file most chosen first; copies elsewhere go.
        #[arg(long, value_name = "SELECTOR", conflicts_with_all = ["free", "tidy", "speedup", "consolidate"])]
        place: Option<String>,
        /// --place: share of the hosts each file is on, 1/hosts (spread) to
        /// 1 (everywhere); a fraction like 2/7 or a number (default 1).
        #[arg(long, requires = "place")]
        factor: Option<String>,
        /// --place: size each host's share by its measured disk read rate
        /// instead of evenly (push loads then finish on every host together).
        #[arg(long, requires = "place")]
        by_speed: bool,
        /// Keep a full copy of a selection on exactly the off-grid hosts
        /// given with --host (none: no off-grid copies); copies on other
        /// off-grid hosts go, never the last one. Fabric hosts are untouched.
        #[arg(long, value_name = "SELECTOR", conflicts_with_all = ["free", "tidy", "speedup", "consolidate", "place"])]
        offgrid: Option<String>,
        /// --offgrid: one copy of each file over the hosts given (their bytes
        /// kept even) instead of a full copy on each.
        #[arg(long, requires = "offgrid")]
        spread: bool,
        /// Hosts or @groups for --tidy / --speedup (default: all).
        #[arg(long = "host")]
        hosts: Vec<String>,
        /// --speedup: least bytes read over the network that earns a copy (default 1 GiB).
        #[arg(long)]
        min: Option<String>,
        /// --speedup: free space each host keeps (default a tenth of its disk).
        #[arg(long)]
        keep_free: Option<String>,
        /// Archive store sole copies may be offloaded into (repeatable, filled
        /// in order). Without it the plan only removes redundant copies.
        #[arg(long = "to", value_name = "STORE")]
        to: Vec<String>,
    },
    /// Host groups (@name).
    Group {
        #[command(subcommand)]
        cmd: GroupCmd,
    },
    /// Cluster-wide settings, read live by every host.
    Config {
        #[command(subcommand)]
        cmd: ConfigCmd,
    },
    /// Retained backups and metadata snapshots.
    Backup {
        #[command(subcommand)]
        cmd: BackupCmd,
    },
    /// Archive stores.
    Store {
        #[command(subcommand)]
        cmd: StoreCmd,
    },
    /// Hosts' disks: each extra disk is a store of its own (ADR-073).
    Disk {
        #[command(subcommand)]
        cmd: DiskCmd,
    },
    /// Copy a selection into an archive store, then drop its live copies.
    Offload {
        selector: String,
        #[arg(long)]
        store: String,
        #[arg(long, default_value_t = 8)]
        parallel: usize,
        /// Follow progress until the job finishes.
        #[arg(long)]
        wait: bool,
    },
    /// Cluster membership.
    Cluster {
        #[command(subcommand)]
        cmd: ClusterCmd,
    },
    /// Converge rules (all, or one by name).
    Reconcile {
        name: Option<String>,
        /// Follow progress until the job finishes.
        #[arg(long)]
        wait: bool,
    },
    /// How each host reads: per source (its disk, each other host), the
    /// recent latency per chunk, reads in flight, and bytes per second
    ///
    /// Reads of files with several copies are spread over them by these
    /// numbers (ADR-030): an idle local disk first, then whichever source
    /// has the lowest latency × (in flight + 1).
    Io,
    /// How each host reaches the others: which network control requests
    /// use, the RDMA link, TCP lanes, Raft round trips, NIC loss counters
    ///
    /// Control requests use the fabric and move to the 10G LAN standby only
    /// when the fabric connection breaks or goes silent; data reads use the
    /// LAN lane while an RDMA link is down (ADR-057). `ctl lan` marks a peer
    /// failed over; `rdma off` one whose link is backing off.
    Net,
    /// Drop the clean page cache on every host (or --host), for cold-cache
    /// benchmarks
    ///
    /// Each host syncs and writes 1 to /proc/sys/vm/drop_caches through its
    /// setuid helper, installed once per host with `sudo` by
    /// `sparknest-drop-page-cache --install`. Other workloads on those hosts
    /// lose their cached pages too.
    DropCaches {
        /// Only these hosts or @groups (repeatable).
        #[arg(long = "host")]
        hosts: Vec<String>,
    },
    /// Recent log lines from every node (or one), merged by time.
    Logs {
        /// Only this host.
        #[arg(long)]
        host: Option<String>,
        /// Least severe level: error, warn, info, debug.
        #[arg(long, default_value = "info")]
        level: String,
        /// Only lines containing this text.
        #[arg(long)]
        grep: Option<String>,
        /// How many lines.
        #[arg(short = 'n', long, default_value_t = 200)]
        lines: usize,
    },
    /// List jobs, or show one.
    Jobs {
        id: Option<u64>,
        /// Follow progress until the job finishes.
        #[arg(long)]
        wait: bool,
        /// Cancel job ID: files not yet started are skipped, nothing is
        /// removed afterwards.
        #[arg(long, requires = "id")]
        cancel: bool,
    },
    /// Hugging Face caches (import)
    Hf {
        #[command(subcommand)]
        cmd: HfCmd,
    },
    /// Push loads (ADR-063): a benchmark that plays the engine's part
    Push {
        #[command(subcommand)]
        cmd: PushCmd,
    },
    /// Import a local directory without copying (hard links into the store)
    ///
    /// The source must be on the same filesystem as this node's store, or
    /// pass --copy. A relative DST is inside sparknest. For Hugging Face
    /// caches use `nest hf import`, which also finalizes each model.
    #[command(after_long_help = IMPORT_EXAMPLES)]
    Import {
        src: PathBuf,
        dst: String,
        /// Remove the sources once imported.
        #[arg(long = "move")]
        mv: bool,
        /// none | all | blobs | auto (follow the destination's policy)
        #[arg(long, default_value = "auto")]
        seal: String,
        /// Copy files on another filesystem than the store (default: only
        /// hard-link, which moves no data).
        #[arg(long)]
        copy: bool,
        /// Follow progress until the job finishes.
        #[arg(long)]
        wait: bool,
    },
}

const PLAN_EXAMPLES: &str = "\
Examples:
  Make room on raptor; its only copies move to ostrich (may fill to 200 GiB
  free), then to the nas archive:
    nest plan --free raptor=800GiB --free ostrich=200GiB --to nas
  Tidy copies nobody opened on their host for a week:
    nest plan --tidy 7d
    nest plan --tidy 1m --host dodo --to nas
  Keep a model only on the host that uses it most (or a chosen one):
    nest plan --consolidate hf:Qwen/Qwen3-8B
    nest plan --consolidate hf:Qwen/Qwen3-8B --host dodo
  Spread a model over three hosts, one copy of each file; or everywhere:
    nest plan --place hf:Qwen/Qwen3-8B --host dodo --host kiwi --host emu --factor 1/3
    nest plan --place hf:Qwen/Qwen3-8B
  One copy spread over the Sparks, each holding a share sized by its disk
  read rate (push loads then finish on every host together):
    nest plan --place hf:moonshotai/Kimi-K2 --host ostrich --host dodo --host emu \
      --host kiwi --host rhea --host moa --factor 1/6 --by-speed
  A full copy on off-grid hosts, one copy spread over them, or none:
    nest plan --offgrid hf:Qwen/Qwen3-8B --host kakapo --host oliver
    nest plan --offgrid hf:Qwen/Qwen3-8B --host kakapo --host cassowary --spread
    nest plan --offgrid hf:Qwen/Qwen3-8B
  Edit a plan before applying it: leave a model or one host out:
    nest plan show 1234567 --files
    nest plan skip 1234567 --host dodo
    nest plan skip 1234567 Qwen/Qwen3-8B
    nest plan unskip 1234567 Qwen/Qwen3-8B/config.json
  Copy what hosts keep pulling over the network:
    nest plan --speedup 7d
    nest plan --speedup 7d --min 4GiB --keep-free 200GiB
  Carry out a plan:
    nest plan apply 1234567 --wait";

const IMPORT_EXAMPLES: &str = "\
Examples:
  A directory on the store's filesystem, into /datasets/imagenet:
    nest import ~/data/imagenet datasets/imagenet
  From a NAS mount, copying, and deleting the source afterwards:
    nest import /mnt/nas/corpus corpora --copy --move --wait";

const HF_IMPORT_EXAMPLES: &str = "\
Examples:
  The default cache, leaving it in place:
    nest hf import ~/.cache/huggingface --wait
  Another disk's cache, moved (the old directory becomes a link):
    nest hf import /mnt/scratch/hf_cache --move --wait
  One model, without asking the Hub:
    nest hf import ~/hf/hub/models--Qwen--Qwen3-8B --offline
  Into the archive store `scratch` (on one of its gateways; hard links when
  the cache is on the archive's disk):
    nest hf import /mnt/scratch/hf --archive scratch --wait";

#[derive(Subcommand, Debug)]
enum PushCmd {
    /// Load files into ranks by push, as an engine would, and time it
    ///
    /// Run on the coordinator's host. Every rank takes a strided 1/N share
    /// of every file (`--run` bytes of every N·run), packed into its arena,
    /// like a TP split of expert weights. Prints a `nest push rank` command
    /// line per rank, waits for each rank to join (run those on the rank
    /// hosts, or pass --local-ranks to run them all here), then reports
    /// the time and the rate.
    Bench {
        /// Files (paths under the mount or in the namespace).
        #[arg(required = true)]
        files: Vec<String>,
        /// The rank hosts, in rank order.
        #[arg(long, value_delimiter = ',', required = true)]
        hosts: Vec<String>,
        /// Bytes per run of a rank's share.
        #[arg(long, default_value_t = 4096)]
        run: u32,
        /// Run every rank's consumer in this process (ranks on this host).
        #[arg(long)]
        local_ranks: bool,
        /// Ring slots per rank and their size.
        #[arg(long, default_value_t = 128)]
        slots: u32,
        #[arg(long, default_value_t = 1 << 20)]
        slot_bytes: u32,
    },
    /// Consume one rank of a push group (copy slots out, release them)
    Rank {
        #[arg(long)]
        group: u64,
        #[arg(long)]
        token: u64,
        #[arg(long)]
        rank: u32,
        /// The rank's arena size in bytes.
        #[arg(long)]
        arena: u64,
        #[arg(long, default_value_t = 128)]
        slots: u32,
        #[arg(long, default_value_t = 1 << 20)]
        slot_bytes: u32,
    },
}

#[derive(Subcommand, Debug)]
enum HfCmd {
    /// Bring an existing Hugging Face cache into sparknest's hub
    ///
    /// Blobs are hard-linked (or copied from another filesystem) to where
    /// huggingface_hub looks, then `hf download` finishes each snapshot,
    /// fetching only what the source never finished; offline (or when the
    /// Hub refuses), the source's snapshot links are mirrored. Refs are
    /// copied and every file is verified. The source is left alone unless
    /// --move.
    ///
    /// SRC is a hub cache (models--* inside), an HF_HOME (hub/ inside), or
    /// one models--org--name directory.
    #[command(after_long_help = HF_IMPORT_EXAMPLES)]
    Import {
        /// A hub cache, an HF_HOME, or one models--org--name directory.
        src: PathBuf,
        /// After verifying, remove the source and leave a symlink to
        /// sparknest's hub in its place (the whole cache when every repo
        /// made it, else each repo that did).
        #[arg(long = "move")]
        mv: bool,
        /// Do not ask the Hub: mirror the source's snapshots exactly.
        #[arg(long)]
        offline: bool,
        /// Only hard-link: fail files on another filesystem than the store.
        #[arg(long)]
        no_copy: bool,
        /// Spread the blobs over the cluster, each to the host with the
        /// most free space (a cache larger than this host's disk)
        ///
        /// Blobs are copied here in batches of ~64 GiB, pulled by their
        /// host over the fabric, and this host's copy removed; the next
        /// batch copies meanwhile. Every host keeps 64 GiB or 5% free.
        /// Blobs the cluster already has are skipped.
        #[arg(long)]
        spread: bool,
        /// Put the blobs in this archive store instead of on hosts
        ///
        /// Run on one of its gateways. Blobs on the archive's filesystem are
        /// hard-linked into it, others copied (paced); snapshots are mirrored
        /// from the source. The models read through the gateway until
        /// copied onto hosts.
        #[arg(long, conflicts_with = "spread")]
        archive: Option<String>,
        /// Follow progress until the job finishes.
        #[arg(long)]
        wait: bool,
    },
    /// Move repos' own blobs into the hub's shared blob store
    ///
    /// Repos written by huggingface_hub before 1.32 keep their blobs in
    /// their own blobs/ folder; hf ≥ 1.32 stores each file once in the hub's
    /// shared store and links to it. This converts them with hf's own code:
    /// renames through the mount, so no data moves and every copy stays
    /// where it is. Without REPO: every repo that has its own blobs.
    Share {
        /// org/name, dataset/org/name, or models--org--name.
        repos: Vec<String>,
        /// Show what would change; change nothing.
        #[arg(long)]
        dry_run: bool,
        /// Check each blob's sha256 before moving it (reads every byte).
        #[arg(long)]
        verify: bool,
        /// Follow progress until the job finishes.
        #[arg(long)]
        wait: bool,
    },
    /// Make the hub's refs true where hf ignores a repo (shows the changes)
    ///
    /// hf skips a repo whose ref names a commit the hub has no snapshot of,
    /// or that has no snapshots, so it can neither list nor delete it. From
    /// what the hub holds (never the Hub): a ref is pointed at the repo's
    /// only snapshot, a stale ref beside a good one is dropped, a repo of
    /// nothing but refs is removed; anything else is reported. Model files
    /// are never touched. Shows the changes; --apply makes them.
    Repair {
        #[arg(long)]
        apply: bool,
    },
    /// Remove revisions no ref points at, and incomplete downloads (shows them)
    ///
    /// `hf cache prune` on the hub: blobs only those revisions used go too;
    /// anything a current snapshot or another repo links stays. Run `nest hf
    /// repair` first: repos hf ignores are not seen. Shows what would go;
    /// --apply deletes it on every host.
    Prune {
        #[arg(long)]
        apply: bool,
    },
    /// Delete Hugging Face repos from sparknest's hub (every copy, every host)
    ///
    /// Runs `hf cache rm` on the hub through this host's mount, so hf decides
    /// which of the hub's shared blobs other repos still use. REPO is
    /// org/name (a model), dataset/org/name, or hf:org/name; append
    /// @branch (or @commit) to remove only that revision.
    Rm {
        #[arg(required = true)]
        repos: Vec<String>,
        /// Only show what would be removed.
        #[arg(long)]
        dry_run: bool,
        /// Follow the job until it finishes.
        #[arg(long)]
        wait: bool,
    },
    /// Download a repo from the Hub, spread over the hosts or onto one
    ///
    /// The files no host has are assigned to hosts (largest first, evenly,
    /// within each host's free space) and each is downloaded through the
    /// mount of its host, so its first copy lands there. Four files download
    /// at once across the cluster by default (--in-flight); a host that
    /// cannot fetch (it fails twice in a row, or stalls) hands its files to
    /// the others. Files the cluster already holds stay where they are.
    /// Without --revision, a repo the hub already has is finished at its
    /// cached revision; a new one is fetched at its main branch. Several
    /// branches of one repo can be downloaded; each is its own model.
    Download {
        /// org/name, org/name@branch, or a huggingface.co URL (a
        /// .../tree/<branch> link names that branch).
        repo: String,
        /// Put every file on this host instead of spreading them.
        #[arg(long, conflicts_with = "hosts")]
        host: Option<String>,
        /// Spread over these hosts or @groups only (comma-separated or
        /// repeated; default: every serving host).
        #[arg(long, value_delimiter = ',')]
        hosts: Vec<String>,
        /// It is a dataset.
        #[arg(long)]
        dataset: bool,
        /// A commit, branch or tag (default: what the hub has, else latest).
        #[arg(long)]
        revision: Option<String>,
        /// Files downloading at once across the cluster.
        #[arg(long, default_value_t = 4)]
        in_flight: usize,
        /// Download into this archive store instead of onto hosts
        ///
        /// Run on one of its gateways. Files are fetched one at a time into
        /// a scratch hub on the archive's own disk, then linked into the
        /// archive; the models read through the gateway until copied onto
        /// hosts.
        #[arg(long, conflicts_with_all = ["host", "hosts"])]
        archive: Option<String>,
        /// Follow the job until it finishes.
        #[arg(long)]
        wait: bool,
    },
    /// List a repo's branches and tags on the Hub, and which are downloaded
    Refs {
        /// org/name or a huggingface.co URL.
        repo: String,
        #[arg(long)]
        dataset: bool,
    },
    /// Search the Hub (most downloaded first)
    Search {
        query: String,
        #[arg(long)]
        dataset: bool,
        #[arg(long, default_value_t = 20)]
        limit: u32,
    },
}

#[derive(Subcommand, Debug)]
enum PlanCmd {
    /// Execute a proposed plan (what was not skipped).
    Apply {
        id: u64,
        /// Follow progress until the job finishes.
        #[arg(long)]
        wait: bool,
    },
    /// Show a proposed plan as edited: each step's models and files.
    Show {
        id: u64,
        /// List every file, not only models and directories.
        #[arg(long)]
        files: bool,
    },
    /// Leave copies out of a plan: models (org/name), directories or files;
    /// every copy (on --host) when none are named.
    Skip {
        id: u64,
        /// Only this host's copies.
        #[arg(long)]
        host: Option<String>,
        matches: Vec<String>,
    },
    /// Take skipped copies back into a plan (same matching as skip).
    Unskip {
        id: u64,
        #[arg(long)]
        host: Option<String>,
        matches: Vec<String>,
    },
}

#[derive(Subcommand, Debug)]
enum GroupCmd {
    /// Define a host group, used as @name wherever hosts are accepted.
    Set {
        name: String,
        #[arg(value_delimiter = ',', required = true)]
        members: Vec<String>,
    },
    Ls,
    Rm {
        name: String,
    },
}

#[derive(Subcommand, Debug)]
enum ConfigCmd {
    /// List every setting with its value and default.
    Ls,
    /// Print one setting's value.
    Get { name: String },
    /// Set a value for the whole cluster ("5s", "500ms", or a number in the
    /// setting's unit; a host name or node id for preferred-leader). Every
    /// host must run this version or later.
    Set { name: String, value: String },
    /// Return a setting to its default.
    Reset { name: String },
}

/// A setting value in milliseconds from "5s", "500ms", "2m" or a bare
/// number (already milliseconds).
fn parse_ms(s: &str) -> Result<u64> {
    let s = s.trim().to_ascii_lowercase();
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: f64 = num.parse().with_context(|| format!("bad duration {s:?}"))?;
    let scale = match unit {
        "" | "ms" => 1.0,
        "s" | "sec" | "secs" => 1_000.0,
        "m" | "min" | "mins" => 60_000.0,
        other => bail!("unknown duration unit {other:?} (use ms, s or m)"),
    };
    Ok((n * scale).round() as u64)
}

fn print_setting(s: &Value) {
    let unit = s["unit"].as_str().unwrap_or("");
    let (value, unit) = match (unit, s["host"].as_str()) {
        ("node", Some(h)) => (format!("{h} ({})", s["value"]), ""),
        ("node", None) if s["value"] == 0 => ("none".to_string(), ""),
        _ => (s["value"].to_string(), unit),
    };
    println!(
        "{:<20} {:>8} {unit:<4} {}  {}",
        s["name"].as_str().unwrap_or(""),
        value,
        if s["set"].as_bool() == Some(true) {
            format!("(default {})", s["default"])
        } else {
            "(default)".into()
        },
        s["about"].as_str().unwrap_or(""),
    );
}

/// Parse "500GiB", "1.5T", "200G", "1048576".
/// A usage window: 1d, 7d, 30d, 2w, 1m (a month is 30 days); bare days.
fn parse_days(s: &str) -> Result<u64> {
    let s = s.trim().to_ascii_lowercase();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: u64 = num.parse().with_context(|| format!("bad window {s:?}"))?;
    Ok(n * match unit {
        "" | "d" | "day" | "days" => 1,
        "w" | "week" | "weeks" => 7,
        "m" | "month" | "months" => 30,
        other => bail!("unknown window unit {other:?} (use d, w or m)"),
    })
}

fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim();
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: f64 = num.parse().with_context(|| format!("bad size {s:?}"))?;
    let mult: f64 = match unit.trim().to_ascii_lowercase().as_str() {
        // A free-space floor in bytes is never meant: "600" is 600 GiB.
        "" | "g" | "gb" | "gib" => 1024.0 * 1024.0 * 1024.0,
        "b" => 1.0,
        "k" | "kb" | "kib" => 1024.0,
        "m" | "mb" | "mib" => 1024.0 * 1024.0,
        "t" | "tb" | "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        other => bail!("unknown size unit {other:?}"),
    };
    Ok((n * mult) as u64)
}

#[derive(Subcommand, Debug)]
enum BackupCmd {
    /// Capture a selection into an archive store (retained; later writes
    /// and deletes never touch it).
    Create {
        selector: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        store: String,
        /// Follow progress until the job finishes.
        #[arg(long)]
        wait: bool,
    },
    Ls,
    /// Recreate a backup under a namespace directory.
    Restore {
        id: u64,
        dst: String,
        /// Follow progress until the job finishes.
        #[arg(long)]
        wait: bool,
    },
    /// Delete a backup and content no other backup needs.
    Rm {
        id: u64,
    },
    /// Snapshot the metadata into an archive store now.
    Meta {
        #[arg(long)]
        store: String,
    },
}

#[derive(Subcommand, Debug)]
enum StoreCmd {
    /// Register an archive store: a folder reachable from each gateway
    /// (e.g. an SMB share mounted on every node, or a disk on one node).
    Add {
        name: String,
        path: String,
        #[arg(long, value_delimiter = ',', required = true)]
        gateways: Vec<String>,
    },
    /// Archive stores with health and capacity per gateway.
    Ls,
}

#[derive(Subcommand, Debug)]
enum DiskCmd {
    /// Every host's stores: its live store, then each extra disk.
    Ls,
    /// Add a disk to a host: PATH is a mounted directory on that host,
    /// empty (or this disk's own). It is then a place of its own, named
    /// like `raptor (D2)` (or `raptor:d2`): copies go there only when a
    /// command or plan names it.
    Add {
        host: String,
        path: String,
        /// Defaults to `<host>:disk<k>`.
        #[arg(long)]
        name: Option<String>,
    },
    /// Move every copy off a disk onto another of its host's disks (on
    /// that host; nothing crosses the network).
    Drain {
        name: String,
        /// Where to: another disk of the same host (default: its primary).
        #[arg(long)]
        to: Option<String>,
        #[arg(long)]
        wait: bool,
    },
    /// Forget a disk with nothing on it.
    Rm {
        name: String,
        /// The disk is lost: retire the copies recorded on it first (never
        /// a file's last copy).
        #[arg(long)]
        forget_copies: bool,
    },
}

#[derive(Subcommand, Debug)]
enum ClusterCmd {
    /// Show members, voters and the leader.
    Ls,
    /// Remove a node from the cluster (e.g. before re-imaging it).
    Remove {
        host: String,
        /// The host is gone for good (its disk lost): drop the copies the
        /// cluster records on it. Refused while it answers, and for any
        /// file it held the last copy of (remove those first).
        #[arg(long)]
        forget_copies: bool,
    },
    /// Move a host off the RDMA fabric or onto it (ADR-047). Stop it, run
    /// this, switch its cable and node.toml (role, listen, members), start it.
    /// `offgrid --addr` gives an off-grid host its LAN address, where every
    /// host then dials it (ADR-071); it may stay running.
    Role {
        host: String,
        #[arg(value_parser = ["offgrid", "fabric"])]
        role: String,
        /// Its control address (ip:port): on the fabric for `fabric`, on the
        /// LAN for `offgrid` (omitted: it dials in, as behind NAT).
        #[arg(long, required_if_eq("role", "fabric"))]
        addr: Option<String>,
    },
    /// Add (or re-add) a node by id and control address (ip:port).
    Add {
        id: u64,
        /// Control address (an off-grid host's LAN address); omitted for an
        /// off-grid host that dials in (behind NAT).
        #[arg(required_unless_present = "offgrid")]
        addr: Option<String>,
        /// Join as a non-voting learner.
        #[arg(long)]
        learner: bool,
        /// An off-grid host (ADR-047): a learner, dialled at ADDR when given
        /// (ADR-071), else only over the connections it opens; start it
        /// after adding it.
        #[arg(long, conflicts_with = "learner")]
        offgrid: bool,
    },
}

#[derive(Subcommand, Debug)]
enum RuleCmd {
    /// Create or replace a rule.
    Set {
        name: String,
        selector: String,
        #[arg(long, value_delimiter = ',', required = true)]
        hosts: Vec<String>,
        /// Re-apply automatically after new content settles.
        #[arg(long)]
        auto: bool,
    },
    Ls,
    Rm {
        name: String,
    },
}

/// This host's push socket, from the same node.toml the API socket comes
/// from (`[push] socket`), else the default.
fn push_socket(cli: &Cli) -> Result<String> {
    let home = std::env::var("HOME").unwrap_or_default();
    let candidates: Vec<PathBuf> = match &cli.config {
        Some(c) => vec![c.clone()],
        None => vec![
            PathBuf::from("/srv/sparknest/node.toml"),
            PathBuf::from(format!("{home}/sparknest-test/state/node.toml")),
            PathBuf::from("/srv/sparknest-test/node.toml"),
        ],
    };
    for c in candidates {
        if let Ok(text) = std::fs::read_to_string(&c) {
            let v: toml::Value =
                toml::from_str(&text).with_context(|| format!("parsing {}", c.display()))?;
            if v.get("node").is_none() {
                continue;
            }
            return Ok(v
                .get("push")
                .and_then(|p| p.get("socket"))
                .and_then(|s| s.as_str())
                .unwrap_or(nest_push::proto::SOCKET)
                .to_string());
        }
    }
    Ok(nest_push::proto::SOCKET.to_string())
}

fn find_socket(cli: &Cli) -> Result<PathBuf> {
    if let Some(s) = &cli.socket {
        return Ok(s.clone());
    }
    let home = std::env::var("HOME").unwrap_or_default();
    let candidates: Vec<PathBuf> = match &cli.config {
        Some(c) => vec![c.clone()],
        // A real deployment wins over the trial layouts.
        None => vec![
            PathBuf::from("/srv/sparknest/node.toml"),
            PathBuf::from(format!("{home}/sparknest-test/state/node.toml")),
            PathBuf::from("/srv/sparknest-test/node.toml"),
        ],
    };
    for c in candidates {
        if let Ok(text) = std::fs::read_to_string(&c) {
            let v: toml::Value =
                toml::from_str(&text).with_context(|| format!("parsing {}", c.display()))?;
            if let Some(dir) = v
                .get("node")
                .and_then(|n| n.get("state_dir"))
                .and_then(|d| d.as_str())
            {
                return Ok(PathBuf::from(dir).join("api.sock"));
            }
        }
    }
    bail!("cannot find the daemon: pass --socket or --config (or set SPARKNEST_SOCKET)")
}

/// `HH:MM:SS.mmm` UTC from Unix milliseconds (no date library needed).
fn chrono_like(ms: u64) -> String {
    let s = ms / 1000;
    format!(
        "{:02}:{:02}:{:02}.{:03}Z",
        (s / 3600) % 24,
        (s / 60) % 60,
        s % 60,
        ms % 1000
    )
}

fn enc(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

struct Client {
    sock: PathBuf,
}

impl Client {
    async fn call(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        let stream = tokio::net::UnixStream::connect(&self.sock)
            .await
            .with_context(|| {
                format!(
                    "connecting to {} (is sparknestd running?)",
                    self.sock.display()
                )
            })?;
        let (mut sender, conn) =
            hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
        tokio::spawn(conn);
        let body = body.map(|b| b.to_string()).unwrap_or_default();
        let req = Request::builder()
            .method(method)
            .uri(path)
            .header("host", "sparknest")
            .header("content-type", "application/json")
            .body(Full::new(bytes::Bytes::from(body)))?;
        let resp = sender.send_request(req).await?;
        let status = resp.status();
        let bytes = resp.into_body().collect().await?.to_bytes();
        let v: Value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| json!({ "error": String::from_utf8_lossy(&bytes) }));
        if !status.is_success() {
            bail!("{}", v["error"].as_str().unwrap_or("request failed"));
        }
        Ok(v)
    }
    async fn get(&self, p: &str) -> Result<Value> {
        self.call(Method::GET, p, None).await
    }
    async fn post(&self, p: &str, b: Value) -> Result<Value> {
        self.call(Method::POST, p, Some(b)).await
    }
}

/// `nest net`: per host, one line per peer.
fn print_net(v: &serde_json::Value) {
    let hosts = v.as_array().cloned().unwrap_or_default();
    let names: std::collections::HashMap<u64, String> = hosts
        .iter()
        .map(|h| {
            (
                h["node"].as_u64().unwrap_or(0),
                h["name"].as_str().unwrap_or("").to_string(),
            )
        })
        .collect();
    let us = |x: &serde_json::Value| match x.as_u64() {
        Some(0) | None => "-".to_string(),
        Some(u) if u >= 10_000 => format!("{:.0}ms", u as f64 / 1000.0),
        Some(u) if u >= 1000 => format!("{:.1}ms", u as f64 / 1000.0),
        Some(u) => format!("{u}us"),
    };
    for h in &hosts {
        let host = h["name"].as_str().unwrap_or("?");
        let Some(r) = h["report"].as_object() else {
            println!(
                "{host}: {}",
                h["error"]
                    .as_str()
                    .unwrap_or("no answer (older than 0.5.0?)")
            );
            continue;
        };
        let leader = r["leader"]
            .as_u64()
            .and_then(|l| names.get(&l).cloned())
            .unwrap_or_else(|| "none".into());
        println!("{host}: leader {leader}, term {}", r["term"]);
        println!(
            "    {:<9} {:<4} {:>6} {:<8} {:>6} {:>8} {:>8} {:>8} {:>5} {:>5}",
            "peer", "ctl", "moves", "rdma", "lanes", "hb", "hb max", "standby", "t/o", "lag"
        );
        for p in r["peers"].as_array().into_iter().flatten() {
            let peer = p["peer"]
                .as_u64()
                .and_then(|id| names.get(&id).cloned())
                .unwrap_or_else(|| p["peer"].to_string());
            let ctl = if p["control_on_standby"].as_bool() == Some(true) {
                "lan"
            } else {
                "fab"
            };
            let moves = format!(
                "{}/{}",
                p["failovers"].as_u64().unwrap_or(0),
                p["returns"].as_u64().unwrap_or(0)
            );
            let rdma = if p["rdma_link"].as_bool() == Some(true) {
                format!("up v{}", p["rdma_protocol"].as_u64().unwrap_or(0))
            } else if p["rdma_backing_off"].as_bool() == Some(true) {
                "off".to_string()
            } else {
                "-".to_string()
            };
            let lanes = match (
                p["lanes"].as_u64().unwrap_or(0),
                p["lanes_writable"].as_bool(),
            ) {
                (0, _) => "-".to_string(),
                (n, Some(true)) => format!("{n} rw"),
                (n, _) => format!("{n} r"),
            };
            let standby = if p["standby_addr"].is_null() {
                "-".to_string()
            } else if p["standby_up"].as_bool() == Some(true) {
                us(&p["standby_rtt_us"])
            } else {
                "down".to_string()
            };
            let hb = &p["raft"]["heartbeat"];
            let lag = p["raft"]["lag"]
                .as_u64()
                .map_or("-".to_string(), |l| l.to_string());
            println!(
                "    {:<9} {:<4} {:>6} {:<8} {:>6} {:>8} {:>8} {:>8} {:>5} {:>5}",
                peer,
                ctl,
                moves,
                rdma,
                lanes,
                us(&hb["ewma_us"]),
                us(&hb["max_10s_us"]),
                standby,
                hb["timeouts"].as_u64().unwrap_or(0),
                lag,
            );
            if let Some(why) = p["reason"].as_str() {
                println!(
                    "              on the {} since {}s: {why}",
                    if p["control_on_standby"].as_bool() == Some(true) {
                        "LAN"
                    } else {
                        "fabric"
                    },
                    p["control_since_s"]
                );
            }
        }
        for port in r["nic"].as_array().into_iter().flatten() {
            let name = port[0].as_str().unwrap_or("?");
            let vals: Vec<String> = port[1]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|kv| kv[1].as_u64().unwrap_or(0) > 0)
                .map(|kv| format!("{}={}", kv[0].as_str().unwrap_or("?"), kv[1]))
                .collect();
            if !vals.is_empty() {
                println!("    nic {name}: {}", vals.join(" "));
            }
        }
    }
}

fn human(b: u64) -> String {
    let units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < units.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{b} B")
    } else {
        format!("{v:.1} {}", units[i])
    }
}

/// A namespace path from what the user typed. Absolute paths are sent as
/// given (the daemon maps paths under its mountpoint into the namespace, so
/// `/mnt/sparknest/hub` and `/hub` both work). A relative path is relative
/// to the working directory when that is inside a sparknest mount, and to
/// the namespace root otherwise: `nest import ~/models models` from a home
/// directory lands in `/models`, not in `/home/you/models`.
fn abspath(p: &str) -> String {
    if p.starts_with('/') || p.starts_with("hf:") || p.starts_with("hf-dataset:") {
        return p.to_string();
    }
    let cwd = std::env::current_dir().unwrap_or_default();
    if sparknest_mounts().iter().any(|m| cwd.starts_with(m)) {
        return cwd.join(p).to_string_lossy().into_owned();
    }
    format!("/{}", p.trim_start_matches("./"))
}

/// A program on PATH, as an absolute path.
fn which(prog: &str) -> Option<String> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|d| d.join(prog))
            .find(|p| p.is_file())
            .map(|p| p.to_string_lossy().into_owned())
    })
}

/// Mountpoints of sparknest filesystems on this machine.
fn sparknest_mounts() -> Vec<PathBuf> {
    std::fs::read_to_string("/proc/self/mountinfo")
        .unwrap_or_default()
        .lines()
        .filter_map(|l| {
            // "... <mount point> <options> ... - <fstype> <source> ..."
            let (pre, post) = l.split_once(" - ")?;
            post.starts_with("fuse.sparknest ")
                .then(|| {
                    pre.split(' ')
                        .nth(4)
                        .map(|m| PathBuf::from(m.replace("\\040", " ")))
                })
                .flatten()
        })
        .collect()
}

async fn wait_job(c: &Client, id: u64) -> Result<Value> {
    loop {
        let v = c.get(&format!("/v1/jobs/{id}")).await?;
        let j = &v["job"];
        let mut line = String::new();
        if let Some(hosts) = j["hosts"].as_object() {
            for (h, p) in hosts {
                line += &format!(
                    "  {h}: {}/{} files, {}/{}",
                    p["done_files"],
                    p["total_files"],
                    human(p["done_bytes"].as_u64().unwrap_or(0)),
                    human(p["total_bytes"].as_u64().unwrap_or(0))
                );
            }
        }
        if let Some(ip) = v["import"].as_object() {
            if ip["spread_files"].as_u64().unwrap_or(0) + ip["kept_files"].as_u64().unwrap_or(0) > 0
            {
                line += &format!(
                    "  {} handed to other hosts ({}), {} kept here,",
                    ip["spread_files"],
                    human(ip["spread_bytes"].as_u64().unwrap_or(0)),
                    ip["kept_files"]
                );
            }
            if ip["adopted"].as_u64().unwrap_or(0) > 0 {
                line += &format!(
                    "  {} existing blobs adopted ({}),",
                    ip["adopted"],
                    human(ip["adopted_bytes"].as_u64().unwrap_or(0))
                );
            }
            line += &format!(
                "  {} files ({}), {} dirs, {} symlinks, {} skipped, {} errors",
                ip["files"],
                human(ip["bytes"].as_u64().unwrap_or(0)),
                ip["dirs"],
                ip["symlinks"],
                ip["skipped"],
                ip["errors"].as_array().map(|a| a.len()).unwrap_or(0)
            );
        }
        eprint!("\r{}{line}\x1b[K", j["what"].as_str().unwrap_or(""));
        if j["finished"].as_bool() == Some(true) {
            eprintln!();
            if let Some(e) = j["error"].as_str() {
                bail!("job failed: {e}");
            }
            if j["cancelled"].as_bool() == Some(true) {
                bail!("job {id} was cancelled");
            }
            return Ok(v);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let c = Client {
        sock: find_socket(&cli)?,
    };
    let out = match &cli.cmd {
        Cmd::Status => {
            let v = c.get("/v1/status").await?;
            if !cli.json {
                println!(
                    "leader: node{}   applied: {}   entries: {}   rules: {}",
                    v["leader"], v["applied"], v["entries"], v["rules"]
                );
                println!(
                    "{:<9} {:<8} {:>10} {:>10} {:>8} {:>10}  rails",
                    "host", "serving", "free", "total", "objects", "stored"
                );
                for n in v["nodes"].as_array().into_iter().flatten() {
                    match n["info"].as_object() {
                        Some(i) => println!(
                            "{:<9} {:<8} {:>10} {:>10} {:>8} {:>10}  {}",
                            n["name"].as_str().unwrap_or("?"),
                            if i["serving"].as_bool() == Some(true) {
                                "yes"
                            } else {
                                "no"
                            },
                            human(i["free_bytes"].as_u64().unwrap_or(0)),
                            human(i["total_bytes"].as_u64().unwrap_or(0)),
                            i["objects"],
                            human(i["object_bytes"].as_u64().unwrap_or(0)),
                            i["rails"]
                                .as_array()
                                .map(|a| a
                                    .iter()
                                    .filter_map(|x| x.as_str())
                                    .collect::<Vec<_>>()
                                    .join(" "))
                                .unwrap_or_default()
                        ),
                        None => println!(
                            "{:<9} unreachable: {}",
                            n["name"].as_str().unwrap_or("?"),
                            n["error"].as_str().unwrap_or("")
                        ),
                    }
                }
                return Ok(());
            }
            v
        }
        Cmd::Ui => {
            let v = c.get("/v1/web").await?;
            let Some(addr) = v["addr"].as_str() else {
                bail!("this node has no api_listen configured")
            };
            let (host, port) = addr.rsplit_once(':').unwrap_or((addr, "7411"));
            let host = if host == "0.0.0.0" || host == "[::]" {
                std::fs::read_to_string("/proc/sys/kernel/hostname")
                    .map(|h| h.trim().to_string())
                    .unwrap_or_else(|_| "localhost".into())
            } else {
                host.to_string()
            };
            println!(
                "http://{host}:{port}/#token={}",
                v["token"].as_str().unwrap_or("")
            );
            return Ok(());
        }
        Cmd::Ls { path } => {
            let v = c
                .get(&format!("/v1/ls?path={}", enc(&abspath(path))))
                .await?;
            if !cli.json {
                for e in v["entries"].as_array().into_iter().flatten() {
                    let kind = e["kind"].as_str().unwrap_or("");
                    let name = e["name"].as_str().unwrap_or("");
                    match kind {
                        "directory" => println!("{:>10}  {:<3} {}/", "-", "", name),
                        "symlink" => println!(
                            "{:>10}  {:<3} {} -> {}",
                            "-",
                            "",
                            name,
                            e["target"].as_str().unwrap_or("")
                        ),
                        _ => {
                            let hosts: Vec<&str> = e["hosts"]
                                .as_array()
                                .into_iter()
                                .flatten()
                                .filter_map(|h| h.as_str())
                                .collect();
                            let state = match e["writing_on"].as_str() {
                                Some(w) => format!("[writing on {w}]"),
                                None => format!("[{}]", hosts.join(",")),
                            };
                            println!(
                                "{:>10}  {:<3} {} {}",
                                human(e["size"].as_u64().unwrap_or(0)),
                                if e["sealed"].as_bool() == Some(true) {
                                    "S"
                                } else {
                                    ""
                                },
                                name,
                                state
                            );
                        }
                    }
                }
                return Ok(());
            }
            v
        }
        Cmd::Seal {
            path,
            recursive,
            unseal,
        } => {
            c.post(
                "/v1/seal",
                json!({ "path": abspath(path), "recursive": recursive, "sealed": !unseal }),
            )
            .await?
        }
        Cmd::Passthrough { selector, mode } => {
            let selector = abspath(selector);
            match mode.as_deref() {
                Some(m) => {
                    c.post(
                        "/v1/passthrough",
                        json!({ "selector": selector, "always": m == "always" }),
                    )
                    .await?
                }
                None => {
                    let v = c
                        .get(&format!("/v1/passthrough?selector={}", enc(&selector)))
                        .await?;
                    if !cli.json {
                        println!(
                            "{selector}: {} of {} files always passthrough",
                            v["always"], v["files"]
                        );
                        return Ok(());
                    }
                    v
                }
            }
        }
        Cmd::Rm {
            paths,
            recursive,
            dry_run,
        } => {
            let mut failed = false;
            let mut all = Vec::new();
            for p in paths {
                let v = c
                    .post(
                        "/v1/rm",
                        json!({ "path": abspath(p), "recursive": recursive, "dry_run": dry_run }),
                    )
                    .await;
                match v {
                    Ok(v) => {
                        let errs = v["errors"].as_array().cloned().unwrap_or_default();
                        if !cli.json {
                            println!(
                                "{p}: {} {} files, {} directories{}",
                                if *dry_run { "would remove" } else { "removed" },
                                v["files"],
                                v["dirs"],
                                if errs.is_empty() {
                                    String::new()
                                } else {
                                    format!(", {} not removed", errs.len())
                                }
                            );
                            for e in errs.iter().take(20) {
                                println!("  {}", e.as_str().unwrap_or(""));
                            }
                        }
                        failed |= !errs.is_empty();
                        all.push(v);
                    }
                    Err(e) => {
                        eprintln!("{p}: {e:#}");
                        failed = true;
                    }
                }
            }
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&all)?);
            }
            if failed {
                std::process::exit(1);
            }
            return Ok(());
        }
        Cmd::WaitReady { timeout } => {
            let start = std::time::Instant::now();
            let mut last = String::new();
            loop {
                let why = match c.get("/v1/status").await {
                    Err(_) => "the daemon is not answering yet".to_string(),
                    Ok(v) if v["serving"] != true => {
                        "not serving yet (recovering or catching up)".into()
                    }
                    Ok(v) => match v["mountpoint"].as_str() {
                        Some(mp) if !is_sparknest_mount(mp) => format!("{mp} is not mounted yet"),
                        _ => break,
                    },
                };
                if why != last {
                    eprintln!("waiting: {why}");
                    last = why;
                }
                if *timeout > 0 && start.elapsed() > Duration::from_secs(*timeout) {
                    bail!("not ready after {timeout}s: {last}");
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            if !cli.json {
                println!("ready");
            }
            return Ok(());
        }
        Cmd::Fsck { yes, deep, recheck } => {
            let v = c
                .post(
                    "/v1/fsck",
                    json!({ "repair": yes, "deep": deep, "recheck": recheck }),
                )
                .await?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&v)?);
            } else {
                let findings = v["findings"].as_array().cloned().unwrap_or_default();
                println!(
                    "{}: {} objects checked, {} findings, {} files lost",
                    v["host"].as_str().unwrap_or("?"),
                    v["objects"],
                    findings.len(),
                    v["lost"]
                );
                if *deep {
                    println!(
                        "  hashed {}, {} verified by an earlier run",
                        v["hashed"], v["already_verified"]
                    );
                }
                for f in &findings {
                    let what = f["issue"].as_str().unwrap_or("?");
                    let path = f["path"]
                        .as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("object {}.{}", f["file"], f["generation"]));
                    let mut line = format!("  {what:<10} {path}");
                    if let Some(n) = f["note"].as_str() {
                        line += &format!(" ({n})");
                    }
                    if what == "damaged" {
                        line += &format!(" [{} bytes, expected {}]", f["found"], f["expected"]);
                    }
                    let act = f["action"].as_str().unwrap_or("?");
                    line += &match act {
                        "quarantined" => format!(" -> {}", f["to"].as_str().unwrap_or("")),
                        "lost" => match f["to"].as_str() {
                            Some(t) => format!(" -> LOST (bytes kept at {t})"),
                            None => " -> LOST".into(),
                        },
                        "failed" => format!(" -> failed: {}", f["error"].as_str().unwrap_or("")),
                        "reported" => String::new(),
                        other => format!(" -> {other}"),
                    };
                    println!("{line}");
                }
                if !yes && !findings.is_empty() {
                    println!("run with -y to apply the standard resolutions");
                }
            }
            let bad = v["lost"].as_u64().unwrap_or(0) > 0
                || v["findings"]
                    .as_array()
                    .is_some_and(|a| a.iter().any(|f| f["action"] == "failed"));
            if bad {
                std::process::exit(1);
            }
            return Ok(());
        }
        Cmd::Policy { path, policy } => {
            let p = match policy.as_str() {
                "off" => "off",
                "incomplete" | "hf" => "rename_from_incomplete",
                "finalize" => "on_finalize",
                "inherit" => "inherit",
                other => bail!("unknown policy {other:?}"),
            };
            c.post(
                "/v1/seal-policy",
                json!({ "path": abspath(path), "policy": p }),
            )
            .await?
        }
        Cmd::Where { selector, hosts } => {
            let mut q = format!("/v1/where?selector={}", enc(&abspath(selector)));
            if !hosts.is_empty() {
                q += &format!("&hosts={}", enc(&hosts.join(",")));
            }
            let v = c.get(&q).await?;
            if !cli.json {
                println!(
                    "{}: {} files, {}{}",
                    v["selector"].as_str().unwrap_or(""),
                    v["files"],
                    human(v["bytes"].as_u64().unwrap_or(0)),
                    if v["writing"].as_u64().unwrap_or(0) > 0 {
                        format!(", {} being written", v["writing"])
                    } else {
                        String::new()
                    }
                );
                for h in v["hosts"].as_array().into_iter().flatten() {
                    if h["ready"].as_bool() == Some(true) {
                        println!("  {:<9} ready", h["host"].as_str().unwrap_or(""));
                    } else {
                        println!(
                            "  {:<9} missing {} files ({})",
                            h["host"].as_str().unwrap_or(""),
                            h["missing_files"],
                            human(h["missing_bytes"].as_u64().unwrap_or(0))
                        );
                    }
                }
                if let Some(d) = v["dangling"].as_array().filter(|d| !d.is_empty()) {
                    println!("  {} dangling symlinks", d.len());
                }
                return Ok(());
            }
            v
        }
        Cmd::Replicate {
            selector,
            hosts,
            parallel,
            wait,
        } => {
            let v = c
                .post(
                    "/v1/replicate",
                    json!({ "selector": abspath(selector), "hosts": hosts, "parallel": parallel }),
                )
                .await?;
            let id = v["job"].as_u64().unwrap_or(0);
            if *wait { wait_job(&c, id).await? } else { v }
        }
        Cmd::Evict { selector, hosts } => {
            c.post(
                "/v1/evict",
                json!({ "selector": abspath(selector), "hosts": hosts, "parallel": 1 }),
            )
            .await?
        }
        Cmd::Rule { cmd } => match cmd {
            RuleCmd::Set {
                name,
                selector,
                hosts,
                auto,
            } => {
                c.call(
                    Method::PUT,
                    &format!("/v1/rules/{}", enc(name)),
                    Some(json!({ "selector": abspath(selector), "hosts": hosts, "auto": auto })),
                )
                .await?
            }
            RuleCmd::Ls => {
                let v = c.get("/v1/rules").await?;
                if !cli.json {
                    for r in v["rules"].as_array().into_iter().flatten() {
                        println!(
                            "{:<20} {:<5} {:<50} -> {}",
                            r["name"].as_str().unwrap_or(""),
                            if r["auto"].as_bool() == Some(true) {
                                "auto"
                            } else {
                                ""
                            },
                            r["selector"].as_str().unwrap_or(""),
                            r["hosts"]
                                .as_array()
                                .map(|a| a
                                    .iter()
                                    .filter_map(|x| x.as_str())
                                    .collect::<Vec<_>>()
                                    .join(","))
                                .unwrap_or_default()
                        );
                    }
                    return Ok(());
                }
                v
            }
            RuleCmd::Rm { name } => {
                c.call(Method::DELETE, &format!("/v1/rules/{}", enc(name)), None)
                    .await?
            }
        },
        Cmd::Plan {
            cmd: Some(PlanCmd::Show { id, files }),
            ..
        } => {
            let v = c.get(&format!("/v1/plans/{id}")).await?;
            if !cli.json {
                print_plan(&v, *files);
                return Ok(());
            }
            v
        }
        Cmd::Plan {
            cmd: Some(PlanCmd::Skip { id, host, matches } | PlanCmd::Unskip { id, host, matches }),
            ..
        } => {
            let on = matches!(
                cli.cmd,
                Cmd::Plan {
                    cmd: Some(PlanCmd::Unskip { .. }),
                    ..
                }
            );
            let v = c
                .post(
                    &format!("/v1/plans/{id}/select"),
                    json!({ "host": host, "matches": matches, "on": on }),
                )
                .await?;
            if !cli.json {
                let n = v["changed"].as_u64().unwrap_or(0);
                println!("{} {n} copies", if on { "took back" } else { "skipped" });
                print_plan(&v["plan"], false);
                return Ok(());
            }
            v
        }
        Cmd::Plan {
            cmd: Some(PlanCmd::Apply { id, wait }),
            ..
        } => {
            let v = c.post(&format!("/v1/plans/{id}/apply"), json!({})).await?;
            let jid = v["job"].as_u64().unwrap_or(0);
            if *wait { wait_job(&c, jid).await? } else { v }
        }
        Cmd::Plan {
            cmd: None,
            free,
            to,
            tidy,
            speedup,
            consolidate,
            place,
            factor,
            by_speed,
            offgrid,
            spread,
            hosts,
            min,
            keep_free,
        } => {
            let body = if let Some(sel) = offgrid {
                json!({ "goal": "offgrid", "selector": abspath(sel), "hosts": hosts, "spread": spread })
            } else if let Some(sel) = place {
                let factor = match factor.as_deref() {
                    None => 1.0,
                    Some(f) => match f.split_once('/') {
                        Some((a, b)) => a.trim().parse::<f64>()? / b.trim().parse::<f64>()?,
                        None => f.trim().parse::<f64>()?,
                    },
                };
                json!({ "goal": "place", "selector": abspath(sel), "hosts": hosts, "factor": factor, "by_speed": by_speed })
            } else if let Some(sel) = consolidate {
                json!({ "goal": "consolidate", "selector": abspath(sel), "host": hosts.first() })
            } else if let Some(w) = tidy {
                json!({ "goal": "tidy", "days": parse_days(w)?, "hosts": hosts, "archives": to })
            } else if let Some(w) = speedup {
                let mut b = json!({ "goal": "speedup", "days": parse_days(w)?, "hosts": hosts });
                if let Some(m) = min {
                    b["min_remote_bytes"] = json!(parse_size(m)?);
                }
                if let Some(k) = keep_free {
                    b["keep_free"] = json!(parse_size(k)?);
                }
                b
            } else {
                if free.is_empty() {
                    bail!("give --free HOST=SIZE, --tidy WINDOW or --speedup WINDOW");
                }
                let mut pairs = Vec::new();
                for f in free {
                    let (h, sz) = f
                        .split_once('=')
                        .with_context(|| format!("expected HOST=SIZE, got {f:?}"))?;
                    pairs.push(json!([h, parse_size(sz)?]));
                }
                json!({ "goal": "free", "free": pairs, "archives": to })
            };
            let v = c.post("/v1/plans", body).await?;
            if !cli.json {
                print_plan(&v, false);
                return Ok(());
            }
            v
        }
        Cmd::Group { cmd } => match cmd {
            GroupCmd::Set { name, members } => {
                let v = c
                    .call(
                        Method::PUT,
                        &format!("/v1/groups/{}", enc(name)),
                        Some(json!({ "members": members })),
                    )
                    .await?;
                if !cli.json {
                    println!("@{name} = {}", members.join(","));
                    return Ok(());
                }
                v
            }
            GroupCmd::Ls => {
                let v = c.get("/v1/groups").await?;
                if !cli.json {
                    for g in v["groups"].as_array().into_iter().flatten() {
                        println!(
                            "@{:<12} {}",
                            g["name"].as_str().unwrap_or(""),
                            g["members"]
                                .as_array()
                                .map(|a| a
                                    .iter()
                                    .filter_map(|x| x.as_str())
                                    .collect::<Vec<_>>()
                                    .join(","))
                                .unwrap_or_default()
                        );
                    }
                    return Ok(());
                }
                v
            }
            GroupCmd::Rm { name } => {
                let v = c
                    .call(Method::DELETE, &format!("/v1/groups/{}", enc(name)), None)
                    .await?;
                if !cli.json {
                    println!("removed @{name}");
                    return Ok(());
                }
                v
            }
        },
        Cmd::Config { cmd } => {
            let v = match cmd {
                ConfigCmd::Ls | ConfigCmd::Get { .. } => c.get("/v1/settings").await?,
                ConfigCmd::Set { name, value } => {
                    let cur = c.get("/v1/settings").await?;
                    let unit = cur["settings"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .find(|s| s["name"] == name.as_str())
                        .and_then(|s| s["unit"].as_str())
                        .unwrap_or("ms")
                        .to_string();
                    // A host setting takes a name or a node id; the
                    // daemon resolves names.
                    let v = match unit.as_str() {
                        "node" => match value.parse::<u64>() {
                            Ok(n) => json!(n),
                            Err(_) => json!(value),
                        },
                        _ => json!(parse_ms(value)?),
                    };
                    c.call(
                        Method::PUT,
                        &format!("/v1/settings/{}", enc(name)),
                        Some(json!({ "value": v })),
                    )
                    .await?;
                    c.get("/v1/settings").await?
                }
                ConfigCmd::Reset { name } => {
                    c.call(Method::DELETE, &format!("/v1/settings/{}", enc(name)), None)
                        .await?;
                    c.get("/v1/settings").await?
                }
            };
            let only = match cmd {
                ConfigCmd::Ls => None,
                ConfigCmd::Get { name }
                | ConfigCmd::Set { name, .. }
                | ConfigCmd::Reset { name } => Some(name.as_str()),
            };
            let all: Vec<&Value> = v["settings"].as_array().into_iter().flatten().collect();
            let shown: Vec<&Value> = all
                .into_iter()
                .filter(|s| only.is_none_or(|n| s["name"] == n))
                .collect();
            if let Some(n) = only
                && shown.is_empty()
            {
                bail!("no setting {n:?}");
            }
            if cli.json {
                json!({ "settings": shown })
            } else {
                shown.iter().for_each(|s| print_setting(s));
                return Ok(());
            }
        }
        Cmd::Backup { cmd } => match cmd {
            BackupCmd::Create {
                selector,
                name,
                store,
                wait,
            } => {
                let v = c
                    .post(
                        "/v1/backups",
                        json!({ "selector": abspath(selector), "name": name, "store": store }),
                    )
                    .await?;
                let id = v["job"].as_u64().unwrap_or(0);
                if *wait { wait_job(&c, id).await? } else { v }
            }
            BackupCmd::Ls => {
                let v = c.get("/v1/backups").await?;
                if !cli.json {
                    for b in v["backups"].as_array().into_iter().flatten() {
                        println!(
                            "{:>4}  {:<20} {:<10} {:>6} files {:>10}  {}",
                            b["id"],
                            b["name"].as_str().unwrap_or(""),
                            b["store"].as_str().unwrap_or("?"),
                            b["files"],
                            human(b["bytes"].as_u64().unwrap_or(0)),
                            b["selector"].as_str().unwrap_or("")
                        );
                    }
                    return Ok(());
                }
                v
            }
            BackupCmd::Restore { id, dst, wait } => {
                let v = c
                    .post(
                        &format!("/v1/backups/{id}/restore"),
                        json!({ "dst": abspath(dst) }),
                    )
                    .await?;
                let jid = v["job"].as_u64().unwrap_or(0);
                if *wait { wait_job(&c, jid).await? } else { v }
            }
            BackupCmd::Rm { id } => {
                c.call(Method::DELETE, &format!("/v1/backups/{id}"), None)
                    .await?
            }
            BackupCmd::Meta { store } => {
                let v = c
                    .post("/v1/backups/meta", json!({ "store": store }))
                    .await?;
                if !cli.json {
                    println!(
                        "metadata snapshot {}",
                        v["snapshot"].as_str().unwrap_or("?")
                    );
                    return Ok(());
                }
                v
            }
        },
        Cmd::Store { cmd } => match cmd {
            StoreCmd::Add {
                name,
                path,
                gateways,
            } => {
                let v = c
                    .post(
                        "/v1/stores",
                        json!({ "name": name, "path": path, "gateways": gateways }),
                    )
                    .await?;
                if !cli.json {
                    for g in v["gateways"].as_array().into_iter().flatten() {
                        match g["error"].as_str() {
                            None => println!("  {:<9} ok", g["gateway"].as_str().unwrap_or("")),
                            Some(e) => println!("  {:<9} {e}", g["gateway"].as_str().unwrap_or("")),
                        }
                    }
                    return Ok(());
                }
                v
            }
            StoreCmd::Ls => {
                let v = c.get("/v1/stores").await?;
                if !cli.json {
                    for st in v["stores"].as_array().into_iter().flatten() {
                        println!(
                            "{} ({})",
                            st["name"].as_str().unwrap_or(""),
                            st["path"].as_str().unwrap_or("")
                        );
                        for g in st["gateways"].as_array().into_iter().flatten() {
                            let (name, h) = (&g[0], &g[1]);
                            match h["error"].as_str() {
                                None => println!(
                                    "  {:<9} {}  free {} of {}, {} objects ({})",
                                    name.as_str().unwrap_or(""),
                                    if h["healthy"].as_bool() == Some(true) {
                                        "healthy"
                                    } else {
                                        "UNHEALTHY"
                                    },
                                    human(h["free_bytes"].as_u64().unwrap_or(0)),
                                    human(h["total_bytes"].as_u64().unwrap_or(0)),
                                    h["objects"],
                                    human(h["object_bytes"].as_u64().unwrap_or(0))
                                ),
                                Some(e) => println!("  {:<9} {e}", name.as_str().unwrap_or("")),
                            }
                        }
                    }
                    return Ok(());
                }
                v
            }
        },
        Cmd::Disk { cmd } => match cmd {
            DiskCmd::Ls => {
                let v = c.get("/v1/disks").await?;
                if !cli.json {
                    for h in v["hosts"].as_array().into_iter().flatten() {
                        let host = h["host"].as_str().unwrap_or("");
                        if let Some(e) = h["error"].as_str() {
                            println!("{host}: {e}");
                            continue;
                        }
                        println!("{host}");
                        for d in h["disks"].as_array().into_iter().flatten() {
                            let rate = d["read_bps"].as_u64().unwrap_or(0);
                            println!(
                                "  {:<16} {:<9} free {} of {}, {} copies ({}), {}  {}",
                                d["name"].as_str().unwrap_or(""),
                                match (d["primary"].as_bool(), d["healthy"].as_bool()) {
                                    (_, Some(false)) => "UNUSABLE",
                                    (Some(true), _) => "primary",
                                    _ => "disk",
                                },
                                human(d["free_bytes"].as_u64().unwrap_or(0)),
                                human(d["total_bytes"].as_u64().unwrap_or(0)),
                                d["objects"],
                                human(d["object_bytes"].as_u64().unwrap_or(0)),
                                if rate > 0 {
                                    format!("{}/s", human(rate))
                                } else {
                                    "rate unmeasured".into()
                                },
                                d["path"].as_str().unwrap_or(""),
                            );
                        }
                    }
                    return Ok(());
                }
                v
            }
            DiskCmd::Add { host, path, name } => {
                c.post(
                    "/v1/disks",
                    json!({ "host": host, "path": path, "name": name }),
                )
                .await?
            }
            DiskCmd::Drain { name, to, wait } => {
                let v = c
                    .post(
                        &format!("/v1/disks/{}/drain", enc(name)),
                        json!({ "to": to }),
                    )
                    .await?;
                let id = v["job"].as_u64().unwrap_or(0);
                if *wait { wait_job(&c, id).await? } else { v }
            }
            DiskCmd::Rm {
                name,
                forget_copies,
            } => {
                c.call(
                    Method::DELETE,
                    &format!("/v1/disks/{}?forget_copies={forget_copies}", enc(name)),
                    None,
                )
                .await?
            }
        },
        Cmd::Offload {
            selector,
            store,
            parallel,
            wait,
        } => {
            let v = c
                .post(
                    "/v1/offload",
                    json!({ "selector": abspath(selector), "store": store, "parallel": parallel }),
                )
                .await?;
            let id = v["job"].as_u64().unwrap_or(0);
            if *wait { wait_job(&c, id).await? } else { v }
        }
        Cmd::Cluster { cmd } => match cmd {
            ClusterCmd::Ls => {
                let v = c.get("/v1/cluster").await?;
                if !cli.json {
                    println!("leader: node{}   term: {}", v["leader"], v["term"]);
                    for n in v["nodes"].as_array().into_iter().flatten() {
                        println!(
                            "  {:>3} {:<9} {:<22} {}",
                            n["id"],
                            n["name"].as_str().unwrap_or(""),
                            n["addr"].as_str().unwrap_or(""),
                            if n["voter"].as_bool() == Some(true) {
                                "voter"
                            } else {
                                "learner"
                            }
                        );
                    }
                    return Ok(());
                }
                v
            }
            ClusterCmd::Remove {
                host,
                forget_copies,
            } => {
                c.post(
                    "/v1/cluster/remove",
                    json!({ "host": host, "forget_copies": forget_copies }),
                )
                .await?
            }
            ClusterCmd::Role { host, role, addr } => {
                c.post(
                    "/v1/cluster/role",
                    json!({ "host": host, "role": role, "addr": addr }),
                )
                .await?
            }
            ClusterCmd::Add {
                id,
                addr,
                learner,
                offgrid,
            } => {
                let addr = addr.clone().unwrap_or_default();
                let voter = !(*learner || *offgrid);
                c.post(
                    "/v1/cluster/add",
                    json!({ "id": id, "addr": addr, "voter": voter }),
                )
                .await?
            }
        },
        Cmd::Reconcile { name, wait } => {
            let v = c.post("/v1/reconcile", json!({ "name": name })).await?;
            if *wait {
                for id in v["jobs"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|j| j.as_u64())
                {
                    wait_job(&c, id).await?;
                }
            }
            v
        }
        Cmd::Net => {
            let v = c.get("/v1/net").await?;
            if !cli.json {
                print_net(&v);
                return Ok(());
            }
            v
        }
        Cmd::Push { cmd } => {
            let sock = push_socket(&cli)?;
            match cmd {
                PushCmd::Bench {
                    files,
                    hosts,
                    run,
                    local_ranks,
                    slots,
                    slot_bytes,
                } => {
                    // Sizes through the mount (symlinks followed, as the
                    // hub resolves them); the namespace path for the plan.
                    let st = c.get("/v1/status").await?;
                    let mnt = st["mountpoint"].as_str().unwrap_or("").to_string();
                    let mut sizes = Vec::new();
                    let mut names = Vec::new();
                    for f in files {
                        let ns = f
                            .strip_prefix(mnt.as_str())
                            .filter(|_| !mnt.is_empty())
                            .unwrap_or(f);
                        let local = format!(
                            "{}/{}",
                            mnt.trim_end_matches('/'),
                            ns.trim_start_matches('/')
                        );
                        let size = std::fs::metadata(&local)
                            .with_context(|| format!("{local}: not readable through the mount"))?
                            .len();
                        sizes.push(size);
                        names.push(ns.to_string());
                    }
                    let b = push_bench::Bench {
                        socket: sock,
                        files: names,
                        sizes,
                        hosts: hosts.clone(),
                        run: *run,
                        local_ranks: *local_ranks,
                        slots: *slots,
                        slot_bytes: *slot_bytes,
                    };
                    tokio::task::spawn_blocking(move || b.run()).await??
                }
                PushCmd::Rank {
                    group,
                    token,
                    rank,
                    arena,
                    slots,
                    slot_bytes,
                } => {
                    let (g, t, r, a, n, b) = (*group, *token, *rank, *arena, *slots, *slot_bytes);
                    tokio::task::spawn_blocking(move || {
                        push_bench::consume(&sock, g, t, r, a, n, b)
                    })
                    .await??
                }
            }
        }
        Cmd::Io => {
            let v = c.get("/v1/status").await?;
            if !cli.json {
                let names: std::collections::HashMap<u64, String> = v["nodes"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|n| {
                        (
                            n["node"].as_u64().unwrap_or(0),
                            n["name"].as_str().unwrap_or("").to_string(),
                        )
                    })
                    .collect();
                for n in v["nodes"].as_array().into_iter().flatten() {
                    let host = n["name"].as_str().unwrap_or("");
                    let io = n["info"]["io"].as_array().cloned().unwrap_or_default();
                    let rate = |k: &str| match n["info"][k].as_u64().unwrap_or(0) {
                        0 => "?".to_string(),
                        b => format!("{}/s", human(b)),
                    };
                    let caps = format!(
                        "disk {} · links {}",
                        rate("disk_read_bps"),
                        rate("link_bps")
                    );
                    if io.is_empty() {
                        println!("{host}: {caps}; no spread reads yet");
                        io_window_table(&n["info"]["io_windows"][1]);
                        continue;
                    }
                    println!("{host}: {caps}");
                    io_window_table(&n["info"]["io_windows"][1]);
                    println!(
                        "    {:<10} {:>10} {:>8} {:>12} {:>12} {:>7}",
                        "source", "latency", "flight", "last 10 s", "total", "errors"
                    );
                    for s in io {
                        let src = match &s["source"] {
                            serde_json::Value::String(l) if l == "Local" => "disk".to_string(),
                            other => other["Peer"]
                                .as_u64()
                                .and_then(|id| names.get(&id).cloned())
                                .unwrap_or_else(|| other.to_string()),
                        };
                        let lat = s["latency_us"].as_u64().unwrap_or(0);
                        println!(
                            "    {:<10} {:>8.2}ms{} {:>8} {:>10}/s {:>12} {:>7}",
                            src,
                            lat as f64 / 1000.0,
                            if s["measured"].as_bool() == Some(true) {
                                " "
                            } else {
                                "?"
                            },
                            s["in_flight"],
                            human(s["bytes_per_s"].as_u64().unwrap_or(0)),
                            human(s["bytes_total"].as_u64().unwrap_or(0)),
                            s["errors"]
                        );
                    }
                }
                return Ok(());
            }
            v
        }
        Cmd::DropCaches { hosts } => {
            let v = c.post("/v1/drop-caches", json!({ "hosts": hosts })).await?;
            if !cli.json {
                let mut failed = 0;
                for h in v["hosts"].as_array().into_iter().flatten() {
                    let ok = h["ok"].as_bool() == Some(true);
                    failed += (!ok) as u32;
                    println!(
                        "  {:<9} {} {}",
                        h["host"].as_str().unwrap_or(""),
                        if ok { "dropped" } else { "FAILED " },
                        if ok {
                            ""
                        } else {
                            h["message"].as_str().unwrap_or("")
                        }
                    );
                }
                if failed > 0 {
                    bail!("{failed} host(s) could not drop their page cache");
                }
                return Ok(());
            }
            v
        }
        Cmd::Logs {
            host,
            level,
            grep,
            lines,
        } => {
            let mut path = format!("/v1/logs?level={}&limit={lines}", enc(level));
            if let Some(h) = host {
                path += &format!("&host={}", enc(h));
            }
            if let Some(g) = grep {
                path += &format!("&q={}", enc(g));
            }
            let v = c.get(&path).await?;
            if !cli.json {
                for l in v["lines"].as_array().into_iter().flatten() {
                    let ms = l["ts_ms"].as_u64().unwrap_or(0);
                    let t = chrono_like(ms);
                    println!(
                        "{t} {:<8} {:<5} {}: {}",
                        l["host"].as_str().unwrap_or(""),
                        l["level"].as_str().unwrap_or(""),
                        l["target"].as_str().unwrap_or(""),
                        l["message"].as_str().unwrap_or("")
                    );
                }
                for u in v["unreachable"].as_array().into_iter().flatten() {
                    eprintln!("unreachable: {}", u.as_str().unwrap_or(""));
                }
                return Ok(());
            }
            v
        }
        Cmd::Jobs { id, wait, cancel } => match id {
            Some(id) if *cancel => c.post(&format!("/v1/jobs/{id}/cancel"), json!({})).await?,
            Some(id) if *wait => wait_job(&c, *id).await?,
            Some(id) => {
                let v = c.get(&format!("/v1/jobs/{id}")).await?;
                if !cli.json {
                    println!("{}", job_line(&v["job"]));
                    return Ok(());
                }
                v
            }
            None => c.get("/v1/jobs").await?,
        },
        Cmd::Hf {
            cmd:
                HfCmd::Download {
                    repo,
                    host,
                    hosts,
                    dataset,
                    revision,
                    in_flight,
                    archive,
                    wait,
                },
        } => {
            let v = c
                .post(
                    "/v1/hf/download",
                    json!({
                        "repo": repo,
                        "host": host.clone().unwrap_or_default(),
                        "hosts": hosts,
                        "kind": if *dataset { "dataset" } else { "model" },
                        "revision": revision,
                        "in_flight": in_flight,
                        "archive": archive,
                    }),
                )
                .await?;
            let jid = v["job"].as_u64().unwrap_or(0);
            if v["attached"].as_bool() == Some(true) {
                eprintln!("already downloading as job {jid}");
            }
            if *wait { wait_job(&c, jid).await? } else { v }
        }
        Cmd::Hf {
            cmd: HfCmd::Refs { repo, dataset },
        } => {
            let v = c
                .get(&format!(
                    "/v1/hf/refs?repo={}&kind={}",
                    urlencode(repo),
                    if *dataset { "dataset" } else { "model" }
                ))
                .await?;
            if !cli.json {
                for (what, k) in [("branch", "branches"), ("tag", "tags")] {
                    for r in v[k].as_array().into_iter().flatten() {
                        let cached = match r["cached"].as_str().unwrap_or("") {
                            "current" => "downloaded",
                            "older" => "older copy here",
                            _ => "",
                        };
                        println!(
                            "{what:<6} {:<32} {:.10}  {cached}",
                            r["name"].as_str().unwrap_or(""),
                            r["sha"].as_str().unwrap_or("")
                        );
                    }
                }
                return Ok(());
            }
            v
        }
        Cmd::Hf {
            cmd:
                HfCmd::Search {
                    query,
                    dataset,
                    limit,
                },
        } => {
            let v = c
                .get(&format!(
                    "/v1/hf/search?q={}&kind={}&limit={limit}",
                    urlencode(query),
                    if *dataset { "dataset" } else { "model" }
                ))
                .await?;
            if !cli.json {
                for r in v["results"].as_array().into_iter().flatten() {
                    println!(
                        "{:<60} {:>12} downloads  {}",
                        r["id"].as_str().unwrap_or(""),
                        r["downloads"].as_u64().unwrap_or(0),
                        r["pipeline_tag"].as_str().unwrap_or("")
                    );
                }
                return Ok(());
            }
            v
        }
        Cmd::Hf {
            cmd:
                HfCmd::Share {
                    repos,
                    dry_run,
                    verify,
                    wait,
                },
        } => {
            let v = c
                .post(
                    "/v1/hf/share",
                    json!({ "repos": repos, "dry_run": dry_run, "verify": verify, "hf": which("hf") }),
                )
                .await?;
            if *dry_run {
                if !cli.json {
                    let (mut n, mut moved, mut linked, mut bytes, mut freed) = (0, 0, 0, 0, 0);
                    for r in v["dry_run"].as_array().into_iter().flatten() {
                        let repo = r["repo"].as_str().unwrap_or("?");
                        if let Some(e) = r["error"].as_str() {
                            println!("  {repo}: {e}");
                            continue;
                        }
                        let (m, l, b) = (
                            r["moved"].as_u64().unwrap_or(0),
                            r["linked"].as_u64().unwrap_or(0),
                            r["bytes"].as_u64().unwrap_or(0),
                        );
                        let skipped = r["skipped"].as_array().map_or(0, Vec::len);
                        println!(
                            "  {repo}: {m} moved, {l} linked ({}){}",
                            human(b),
                            if skipped > 0 {
                                format!(", {skipped} not converted")
                            } else {
                                String::new()
                            }
                        );
                        for s in r["skipped"].as_array().into_iter().flatten().take(3) {
                            println!("      {}", s.as_str().unwrap_or(""));
                        }
                        n += 1;
                        moved += m;
                        linked += l;
                        bytes += b;
                        freed += r["freed"].as_u64().unwrap_or(0);
                    }
                    println!(
                        "would convert {n} repo(s): {moved} blobs moved, {linked} linked ({}); frees {}",
                        human(bytes),
                        human(freed)
                    );
                    return Ok(());
                }
                v
            } else {
                let jid = v["job"].as_u64().unwrap_or(0);
                if *wait { wait_job(&c, jid).await? } else { v }
            }
        }
        Cmd::Hf {
            cmd:
                HfCmd::Rm {
                    repos,
                    dry_run,
                    wait,
                },
        } => {
            let v = c
                .post(
                    "/v1/hf/remove",
                    json!({ "targets": repos, "dry_run": dry_run, "hf": which("hf") }),
                )
                .await?;
            if *dry_run {
                if !cli.json {
                    let d = &v["dry_run"];
                    println!(
                        "would remove {} repo(s), {} revision(s), {}",
                        d["repos"],
                        d["revisions"],
                        d["size"].as_str().unwrap_or("?")
                    );
                    return Ok(());
                }
                v
            } else {
                let jid = v["job"].as_u64().unwrap_or(0);
                if *wait { wait_job(&c, jid).await? } else { v }
            }
        }
        Cmd::Hf {
            cmd: HfCmd::Repair { apply },
        } => {
            let v = c
                .post("/v1/hf/repair", json!({ "dry_run": !apply }))
                .await?;
            if !cli.json {
                let steps = v["steps"].as_array().cloned().unwrap_or_default();
                if steps.is_empty() {
                    println!("every repo's refs are true: nothing to repair");
                    return Ok(());
                }
                for s in &steps {
                    let what = match s["action"].as_str().unwrap_or("") {
                        "point" => {
                            let to = s["to"].as_str().unwrap_or("");
                            format!(
                                "refs/{} → {}",
                                s["ref"].as_str().unwrap_or(""),
                                &to[..to.len().min(12)]
                            )
                        }
                        "drop-ref" => format!("drop refs/{}", s["ref"].as_str().unwrap_or("")),
                        "remove-repo" => "remove the repo (only refs)".to_string(),
                        _ => "left alone".to_string(),
                    };
                    println!(
                        "  {:<60} {:<32} {}",
                        s["repo"].as_str().unwrap_or("?"),
                        what,
                        s["why"].as_str().unwrap_or("")
                    );
                }
                let act = steps.iter().filter(|s| s["action"] != "report").count();
                if *apply {
                    println!("repaired {act} repo change(s)");
                } else {
                    println!("would make {act} change(s); `nest hf repair --apply` makes them");
                }
                return Ok(());
            }
            v
        }
        Cmd::Hf {
            cmd: HfCmd::Prune { apply },
        } => {
            let v = c
                .post(
                    "/v1/hf/prune",
                    json!({ "dry_run": !apply, "hf": which("hf") }),
                )
                .await?;
            if !cli.json {
                let r = &v["report"];
                if let Some(o) = r["output"].as_str() {
                    println!("{o}");
                } else {
                    println!("{}", serde_json::to_string_pretty(r)?);
                }
                if !*apply {
                    println!("`nest hf prune --apply` deletes it");
                }
                return Ok(());
            }
            v
        }
        Cmd::Hf {
            cmd:
                HfCmd::Import {
                    src,
                    mv,
                    offline,
                    no_copy,
                    spread,
                    archive,
                    wait,
                },
        } => {
            let src = std::fs::canonicalize(src).with_context(|| format!("{}", src.display()))?;
            let hf = if *offline {
                None
            } else {
                let found = which("hf");
                if found.is_none() {
                    eprintln!("hf not found on PATH: mirroring snapshots offline");
                }
                found
            };
            let v = c
                .post(
                    "/v1/hf/import",
                    json!({ "src": src, "move": mv, "hf": hf, "copy": !no_copy, "spread": spread, "archive": archive }),
                )
                .await?;
            let id = v["job"].as_u64().unwrap_or(0);
            if *wait {
                let v = wait_job(&c, id).await?;
                for n in v["job"]["notes"].as_array().into_iter().flatten() {
                    println!("  {}", n.as_str().unwrap_or(""));
                }
                v
            } else {
                v
            }
        }
        Cmd::Import {
            src,
            dst,
            mv,
            seal,
            copy,
            wait,
        } => {
            let src = std::fs::canonicalize(src).with_context(|| format!("{}", src.display()))?;
            let v = c
                .post(
                    "/v1/import",
                    json!({ "src": src, "dst": abspath(dst), "move": mv, "seal": seal, "copy": copy }),
                )
                .await?;
            let id = v["job"].as_u64().unwrap_or(0);
            if *wait { wait_job(&c, id).await? } else { v }
        }
    };
    if cli.json {
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        print_human(&out);
    }
    Ok(())
}

fn job_line(j: &Value) -> String {
    let cancelled = j["cancelled"].as_bool() == Some(true);
    let state = if j["finished"].as_bool() != Some(true) {
        if cancelled { "cancelling" } else { "running" }.to_string()
    } else if cancelled {
        "cancelled".to_string()
    } else if let Some(e) = j["error"].as_str() {
        format!("failed: {e}")
    } else {
        "done".to_string()
    };
    let mut line = format!(
        "{:>6}  {:<8} {}",
        j["id"],
        state,
        j["what"].as_str().unwrap_or("")
    );
    if let Some(n) = j["node"].as_str().filter(|n| !n.is_empty()) {
        line += &format!("  (on {n})");
    }
    for (h, p) in j["hosts"].as_object().into_iter().flatten() {
        line += &format!(
            "\n          {h}: {}/{} files, {}/{}",
            p["done_files"],
            p["total_files"],
            human(p["done_bytes"].as_u64().unwrap_or(0)),
            human(p["total_bytes"].as_u64().unwrap_or(0))
        );
        for f in p["failed"].as_array().into_iter().flatten() {
            line += &format!("\n            not done: {}", f[1].as_str().unwrap_or(""));
        }
    }
    line
}

/// Plain-text rendering for responses without a dedicated view.
fn print_human(v: &Value) {
    let obj = v.as_object();
    let only = |k: &str| obj.is_some_and(|o| o.len() == 1 && o.contains_key(k));
    // A job that was waited for: its progress already went to stderr.
    if v["job"]["finished"].as_bool() == Some(true) {
        println!("done");
    } else if v["job"].is_object() {
        println!("{}", job_line(&v["job"]));
    } else if let Some(id) = v["job"].as_u64().filter(|_| only("job")) {
        println!("started job {id}; follow it with: nest jobs {id} --wait");
    } else if let Some(jobs) = v["jobs"].as_array().filter(|_| only("jobs")) {
        if jobs.is_empty() {
            println!("no jobs");
        }
        for j in jobs {
            match j.as_u64() {
                Some(id) => println!("started job {id}; follow it with: nest jobs {id} --wait"),
                None => println!("{}", job_line(j)),
            }
        }
    } else if let Some(hosts) = v["hosts"].as_array().filter(|_| only("hosts")) {
        for h in hosts {
            let refused = h["refused"].as_array().map(|a| a.len()).unwrap_or(0);
            print!(
                "{}: removed {}",
                h["host"].as_str().unwrap_or("?"),
                h["removed"]
            );
            if refused > 0 {
                print!(", kept {refused}");
            }
            println!();
            for r in h["refused"].as_array().into_iter().flatten() {
                println!("  kept: {}", r[1].as_str().unwrap_or(""));
            }
        }
    } else if let Some(o) = obj {
        for (k, x) in o {
            match x {
                Value::String(s) => println!("{k}: {s}"),
                Value::Array(a) if a.iter().all(|e| !e.is_object() && !e.is_array()) => {
                    let items: Vec<String> = a
                        .iter()
                        .map(|e| {
                            e.as_str()
                                .map(str::to_string)
                                .unwrap_or_else(|| e.to_string())
                        })
                        .collect();
                    println!(
                        "{k}: {}",
                        if items.is_empty() {
                            "none".into()
                        } else {
                            items.join(", ")
                        }
                    );
                }
                Value::Array(_) | Value::Object(_) => println!("{k}: {x}"),
                _ => println!("{k}: {x}"),
            }
        }
    } else {
        println!("{v}");
    }
}

/// Whether `mp` is currently a sparknest FUSE mount.
fn is_sparknest_mount(mp: &str) -> bool {
    let mp = mp.trim_end_matches('/');
    std::fs::read_to_string("/proc/self/mountinfo")
        .map(|m| {
            m.lines().any(|l| {
                let f: Vec<&str> = l.split(' ').collect();
                let sep = f.iter().position(|x| *x == "-");
                f.get(4) == Some(&mp)
                    && sep
                        .and_then(|i| f.get(i + 1))
                        .is_some_and(|t| t.starts_with("fuse"))
            })
        })
        .unwrap_or(false)
}

/// Read latencies by kind and size over one window (`nest io`).
fn io_window_table(w: &Value) {
    let classes = w["classes"].as_array().cloned().unwrap_or_default();
    let secs = w["ms"].as_u64().unwrap_or(0) as f64 / 1000.0;
    if classes.is_empty() || secs <= 0.0 {
        return;
    }
    let us = |v: &Value| match v.as_u64().unwrap_or(0) {
        u if u >= 10_000 => format!("{:.0}ms", u as f64 / 1000.0),
        u if u >= 1000 => format!("{:.1}ms", u as f64 / 1000.0),
        u => format!("{u}us"),
    };
    println!(
        "    last {:.0} s: {:<7} {:>6} {:>9} {:>11} {:>7} {:>7} {:>7} {:>7}",
        secs, "reads", "size", "per s", "rate", "mean", "p50", "p90", "p99"
    );
    for c in &classes {
        let size = match c["max_bytes"].as_u64().unwrap_or(0) {
            0 => ">128K".to_string(),
            b => format!("≤{}K", b >> 10),
        };
        let kind = match c["kind"].as_str().unwrap_or("") {
            "Disk" => "disk",
            "Fabric" => "fabric",
            "Served" => "served",
            k => k,
        };
        println!(
            "    {:<14} {:>6} {:>9.0} {:>9}/s {:>7} {:>7} {:>7} {:>7}",
            kind,
            size,
            c["count"].as_u64().unwrap_or(0) as f64 / secs,
            human((c["bytes"].as_u64().unwrap_or(0) as f64 / secs) as u64),
            us(&c["mean_us"]),
            us(&c["p50_us"]),
            us(&c["p90_us"]),
            us(&c["p99_us"])
        );
    }
}

/// Percent-encode a query-string value.
fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// A plan for people: hosts' free space, then each step as a tree of
/// models (or directories) and, with `files`, their files; skipped copies
/// are marked.
fn print_plan(v: &serde_json::Value, files: bool) {
    println!(
        "plan {}{}",
        v["id"],
        if v["feasible"].as_bool() == Some(true) {
            ""
        } else {
            "  (does not fully reach the targets)"
        }
    );
    for h in v["hosts"].as_array().into_iter().flatten() {
        let now = h["free_now"].as_u64().unwrap_or(0);
        let after = h["projected_free"].as_u64().unwrap_or(0);
        let target = h["target"].as_u64().unwrap_or(0);
        let gain = if after > now {
            format!(" (+{})", human(after - now))
        } else {
            String::new()
        };
        if target == 0 {
            // Goals other than free space set no target.
            println!(
                "  {:<9} free {} -> {}{gain}",
                h["host"].as_str().unwrap_or(""),
                human(now),
                human(after)
            );
            continue;
        }
        let mark = if after >= target { "ok" } else { "SHORT" };
        println!(
            "  {:<9} free {} -> {}{gain}, target {}  {mark}",
            h["host"].as_str().unwrap_or(""),
            human(now),
            human(after),
            human(target)
        );
    }
    for a in v["archives"].as_array().into_iter().flatten() {
        let now = a["free_now"].as_u64().unwrap_or(0);
        let after = a["projected_free"].as_u64().unwrap_or(0);
        if a["reachable"].as_bool() == Some(false) {
            println!(
                "  archive {:<9} unreachable",
                a["store"].as_str().unwrap_or("")
            );
            continue;
        }
        println!(
            "  archive {:<9} free {} -> {} (+{} stored) of {}",
            a["store"].as_str().unwrap_or(""),
            human(now),
            human(after),
            human(a["adds"].as_u64().unwrap_or(0)),
            human(a["total"].as_u64().unwrap_or(0))
        );
    }
    print_steps(v, files);
    for b in v["blocked"].as_array().into_iter().flatten() {
        println!("  blocked: {}", b.as_str().unwrap_or(""));
    }
    for b in v["notes"].as_array().into_iter().flatten() {
        println!("  note: {}", b.as_str().unwrap_or(""));
    }
    if v["steps"].as_array().is_some_and(|s| !s.is_empty()) {
        println!(
            "edit with: nest plan skip|unskip {} [--host HOST] [MODEL|PATH...]",
            v["id"]
        );
        println!("apply with: nest plan apply {}", v["id"]);
    }
}

fn print_steps(v: &serde_json::Value, files: bool) {
    fn on(c: &serde_json::Value) -> bool {
        c["skip"].as_bool() != Some(true)
    }
    fn size(c: &serde_json::Value) -> u64 {
        c["size"].as_u64().unwrap_or(0)
    }
    for st in v["steps"].as_array().into_iter().flatten() {
        let copies: Vec<&serde_json::Value> =
            st["copies"].as_array().into_iter().flatten().collect();
        let n = copies.iter().filter(|c| on(c)).count();
        let bytes: u64 = copies.iter().filter(|c| on(c)).map(|c| size(c)).sum();
        let skipped = copies.len() - n;
        let host = st["host"].as_str().unwrap_or("");
        let head = match st["kind"].as_str() {
            Some("evict") => format!("- remove {n} copies ({}) from {host}", human(bytes)),
            Some("replicate") => format!("+ copy {n} files ({}) to {host}", human(bytes)),
            _ => format!(
                "> offload {n} files ({}) from {host} to {}",
                human(bytes),
                st["store"].as_str().unwrap_or("")
            ),
        };
        let skip_note = if skipped > 0 {
            format!(", {skipped} skipped")
        } else {
            String::new()
        };
        println!("  {head}{skip_note}");
        // Models (or directories), largest first.
        let mut groups: Vec<(String, Vec<&serde_json::Value>)> = Vec::new();
        for c in &copies {
            let g = c["group"].as_str().unwrap_or("").to_string();
            match groups.iter_mut().find(|(x, _)| *x == g) {
                Some((_, v)) => v.push(c),
                None => groups.push((g, vec![c])),
            }
        }
        groups.sort_by_key(|(_, v)| std::cmp::Reverse(v.iter().map(|c| size(c)).sum::<u64>()));
        for (g, cs) in groups {
            let n = cs.iter().filter(|c| on(c)).count();
            let b: u64 = cs.iter().filter(|c| on(c)).map(|c| size(c)).sum();
            let mark = if n == 0 {
                "[ ]"
            } else if n < cs.len() {
                "[~]"
            } else {
                "[x]"
            };
            println!(
                "      {mark} {}  {n}/{} files, {}",
                if g.is_empty() { "(other)" } else { &g },
                cs.len(),
                human(b)
            );
            if !files {
                continue;
            }
            for c in cs {
                let name = c["name"]
                    .as_str()
                    .filter(|x| !x.is_empty())
                    .or(c["path"].as_str())
                    .unwrap_or("");
                println!(
                    "          {} {name}  {}  {}",
                    if on(c) { "[x]" } else { "[ ]" },
                    human(size(c)),
                    c["why"].as_str().unwrap_or("")
                );
            }
        }
    }
}
