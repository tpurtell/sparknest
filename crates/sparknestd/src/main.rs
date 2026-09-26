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
    let cfg = Config::load(&args.config)?;
    tracing::info!(node = %cfg.node.name, id = %cfg.node.id, cluster = %cfg.cluster.name, "configuration loaded");
    if args.check {
        return Ok(());
    }
    let secret = std::fs::read(&cfg.cluster.secret_file)
        .with_context(|| format!("reading {}", cfg.cluster.secret_file.display()))?;
    let node = Node::start(cfg, secret, Tuning::default(), args.bootstrap).await?;
    tracing::info!("sparknestd running");
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
    tracing::info!("shutting down");
    node.shutdown().await;
    Ok(())
}
