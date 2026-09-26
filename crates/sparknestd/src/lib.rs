//! Node wiring for the sparknest daemon. `main.rs` is a thin shell over this
//! so that `nest-testkit` can run several nodes in one process.

pub mod config;
pub mod export;
mod node;

pub use node::{Node, Tuning};
