use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "nest", version, about = "sparknest management CLI")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Cluster, node and store overview.
    Status,
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Status => anyhow::bail!("management API not implemented yet"),
    }
}
