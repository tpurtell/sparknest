use anyhow::{Context, Result};
use clap::Parser;
use sparknestd::config::Config;
use sparknestd::{Node, Tuning};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "sparknestd", version, about = "sparknest node daemon")]
struct Args {
    /// Path to node.toml
    #[arg(
        short,
        long,
        env = "SPARKNEST_CONFIG",
        default_value = "/srv/sparknest/node.toml"
    )]
    config: PathBuf,
    /// Validate configuration and exit.
    #[arg(long)]
    check: bool,
    /// Initialize a new cluster from `cluster.members` if this node has no
    /// Raft state yet. Use on exactly one node, once.
    #[arg(long)]
    bootstrap: bool,
    #[command(subcommand)]
    cmd: Option<Sub>,
}

#[derive(clap::Subcommand, Debug)]
enum Sub {
    /// Disaster recovery without a cluster: rebuild the namespace (or part
    /// of it) as a plain directory tree from a metadata snapshot and object
    /// directories (live stores, archive stores, backup areas).
    Export {
        /// Metadata snapshot (an archive's meta/meta-*.sqlite, or meta.sqlite).
        #[arg(long)]
        meta: PathBuf,
        /// Directories holding objects/ (repeatable).
        #[arg(long = "objects", required = true)]
        objects: Vec<PathBuf>,
        #[arg(long)]
        out: PathBuf,
        /// Namespace subtree to export.
        #[arg(long, default_value = "/")]
        path: String,
        /// Hard-link instead of copying where possible.
        #[arg(long)]
        link: bool,
    },
    /// Check this host's objects against its local metadata without a
    /// running daemon (report only; repairs need the cluster: use
    /// `nest fsck -y` once it runs). Uses the node config for paths.
    Fsck,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,openraft=warn".into()),
        )
        .init();
    let args = Args::parse();
    if let Some(Sub::Export {
        meta,
        objects,
        out,
        path,
        link,
    }) = &args.cmd
    {
        let r = sparknestd::export::run(&sparknestd::export::ExportArgs {
            meta: meta.clone(),
            objects: objects.clone(),
            out: out.clone(),
            path: path.clone(),
            link: *link,
        })?;
        println!(
            "exported {} dirs, {} files ({} bytes), {} symlinks",
            r.dirs, r.files, r.bytes, r.symlinks
        );
        if !r.missing.is_empty() {
            println!(
                "{} files had no object in the given directories:",
                r.missing.len()
            );
            for m in r.missing.iter().take(50) {
                println!("  {m}");
            }
            std::process::exit(2);
        }
        return Ok(());
    }
    let cfg = Config::load(&args.config)?;
    if let Some(Sub::Fsck) = &args.cmd {
        let r = sparknestd::offline::fsck(&cfg)?;
        println!(
            "{}: {} objects, {} copies listed here; {} missing, {} wrong size, {} unknown objects",
            cfg.node.name,
            r.objects,
            r.listed,
            r.missing.len(),
            r.damaged.len(),
            r.unknown.len()
        );
        for m in r.missing.iter().take(50) {
            println!("  missing  {m}");
        }
        for d in r.damaged.iter().take(50) {
            println!("  damaged  {d}");
        }
        for u in r.unknown.iter().take(50) {
            println!("  unknown  {u}");
        }
        if !(r.missing.is_empty() && r.damaged.is_empty() && r.unknown.is_empty()) {
            std::process::exit(1);
        }
        return Ok(());
    }
    tracing::info!(node = %cfg.node.name, id = %cfg.node.id, cluster = %cfg.cluster.name, "configuration loaded");
    if args.check {
        return Ok(());
    }
    let secret = std::fs::read(&cfg.cluster.secret_file)
        .with_context(|| format!("reading {}", cfg.cluster.secret_file.display()))?;
    // Ready at once: boot never waits on the cluster (a crashed host may
    // wait minutes for a majority). Dependents use `nest wait-ready`.
    sparknestd::sdnotify::notify("READY=1");
    sparknestd::sdnotify::status("starting");
    let node = Node::start(cfg, secret, Tuning::default(), args.bootstrap).await?;
    tracing::info!("sparknestd running");
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
    tracing::info!("shutting down");
    sparknestd::sdnotify::notify("STOPPING=1");
    node.shutdown().await;
    Ok(())
}
