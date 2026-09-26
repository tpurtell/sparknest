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
    /// Set a directory's automatic sealing policy.
    Policy {
        path: String,
        /// off | incomplete (seal on rename from *.incomplete, for HF) | finalize | inherit
        policy: String,
    },
    /// Which hosts hold a complete copy of a selection
    /// (a path, or hf:org/name[@revision]).
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
    /// Copy a selection into an archive store, then drop its live copies.
    Offload {
        selector: String,
        #[arg(long)]
        store: String,
        #[arg(long, default_value_t = 8)]
        parallel: usize,
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
        #[arg(long)]
        wait: bool,
    },
    /// List jobs, or show one.
    Jobs {
        id: Option<u64>,
        #[arg(long)]
        wait: bool,
    },
    /// Import a local directory into the namespace without copying (hard
    /// links into this node's store; must be on the same filesystem).
    Import {
        src: PathBuf,
        dst: String,
        /// Remove the sources once imported.
        #[arg(long = "move")]
        mv: bool,
        /// none | all | blobs | auto (follow the destination's policy)
        #[arg(long, default_value = "auto")]
        seal: String,
        #[arg(long)]
        wait: bool,
    },
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
        #[arg(long)]
        wait: bool,
    },
    Ls,
    /// Recreate a backup under a namespace directory.
    Restore {
        id: u64,
        dst: String,
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
enum ClusterCmd {
    /// Show members, voters and the leader.
    Ls,
    /// Remove a node from the cluster (e.g. before re-imaging it).
    Remove { host: String },
    /// Add (or re-add) a node by id and control address (ip:port).
    Add {
        id: u64,
        addr: String,
        /// Join as a non-voting learner.
        #[arg(long)]
        learner: bool,
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

fn find_socket(cli: &Cli) -> Result<PathBuf> {
    if let Some(s) = &cli.socket {
        return Ok(s.clone());
    }
    let home = std::env::var("HOME").unwrap_or_default();
    let candidates: Vec<PathBuf> = match &cli.config {
        Some(c) => vec![c.clone()],
        None => vec![
            PathBuf::from(format!("{home}/sparknest-test/state/node.toml")),
            PathBuf::from("/srv/sparknest/node.toml"),
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

/// Absolute paths are sent as given (the daemon translates mountpoint
/// paths); relative paths are made absolute against the cwd.
fn abspath(p: &str) -> String {
    if p.starts_with('/') || p.starts_with("hf:") || p.starts_with("hf-dataset:") {
        return p.to_string();
    }
    let cwd = std::env::current_dir().unwrap_or_default();
    cwd.join(p).to_string_lossy().into_owned()
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
                c.post("/v1/backups/meta", json!({ "store": store }))
                    .await?
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
            ClusterCmd::Remove { host } => {
                c.post("/v1/cluster/remove", json!({ "host": host }))
                    .await?
            }
            ClusterCmd::Add { id, addr, learner } => {
                c.post(
                    "/v1/cluster/add",
                    json!({ "id": id, "addr": addr, "voter": !learner }),
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
        Cmd::Jobs { id, wait } => match id {
            Some(id) if *wait => wait_job(&c, *id).await?,
            Some(id) => c.get(&format!("/v1/jobs/{id}")).await?,
            None => c.get("/v1/jobs").await?,
        },
        Cmd::Import {
            src,
            dst,
            mv,
            seal,
            wait,
        } => {
            let src = std::fs::canonicalize(src).with_context(|| format!("{}", src.display()))?;
            let v = c
                .post(
                    "/v1/import",
                    json!({ "src": src, "dst": abspath(dst), "move": mv, "seal": seal }),
                )
                .await?;
            let id = v["job"].as_u64().unwrap_or(0);
            if *wait { wait_job(&c, id).await? } else { v }
        }
    };
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}
