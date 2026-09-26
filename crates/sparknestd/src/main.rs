use anyhow::Result;
use clap::Parser;
use sparknestd::config::Config;
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
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    let cfg = Config::load(&args.config)?;
    tracing::info!(node = %cfg.node.name, id = %cfg.node.id, cluster = %cfg.cluster.name, "configuration loaded");
    if args.check {
        return Ok(());
    }
    anyhow::bail!("daemon runtime not implemented yet (M1)")
}
