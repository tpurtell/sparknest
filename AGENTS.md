# Working on sparknest (for agents and humans)

Read in this order: `PROPOSAL.md` (architecture and invariants),
`docs/DECISIONS.md` (why), `docs/ENVIRONMENT.md` (what is installed where),
`ROADMAP.md` (what is next), `docs/PROGRESS.md` (where we are).

## Ground rules

- The invariants in `PROPOSAL.md` §11 are not negotiable without a new ADR.
  Do not add chunking, hashing, read-triggered replication, GC delays, or
  stale-replica retention to solve a local problem.
- Get the architecture right, then measure. Tuning constants (slot sizes,
  read-ahead, TTLs) are chosen from benchmarks in `benchmarks/`, not guessed.
- Prefer the simplest thing that keeps the invariants. Seven nodes, one user.
- Tests first for anything touching lifecycle, fencing, or crash recovery;
  `nest-testkit` exists so those run in-process without hardware.
- The user's real data is off limits until the migration milestone. Trial
  clusters use `/srv/sparknest-test` and `/mnt/sparknest-test` with synthetic
  or copied data.
- Root actions need the user (sudo prompts for a password). Batch them into a
  short checklist in `docs/ENVIRONMENT.md` and ask once, rather than blocking
  repeatedly.

## Git workflow

- Work on `main`. Commit small and often; push after every green step and at
  least at every milestone boundary. Never leave the day's work unpushed.
- Commit messages: imperative subject, one blank line, why-not-what body when
  it matters. No attribution trailers.
- Tag milestones `m0`, `m1`, … when their acceptance list is green.
- Every commit builds (`scripts/build.sh`) and passes `cargo test --workspace`
  and `cargo clippy --workspace -- -D warnings`. Format with `cargo fmt`.
- Record decisions in `docs/DECISIONS.md` (new ADR, never edit history) and
  progress/benchmarks in `docs/PROGRESS.md` and `benchmarks/`.

## Build, deploy, run

- `scripts/build.sh` builds daemon + CLI (+ web bundle when node is present).
- `scripts/deploy-cluster.sh` builds natively on raptor (amd64) and ostrich
  (arm64), then ships binaries and sample configs to all nodes over SSH; use
  `rdmasync`/`rdmapipe` when moving large test data between nodes.
- Trial daemons run as a systemd **user** service (`packaging/systemd/`),
  under `/srv/sparknest-test`, mounted at `/mnt/sparknest-test`.
- Tools may be installed with Homebrew; every tool the build or test loop
  needs is logged in `docs/TOOLING.md` with the install command.
- Hosts: raptor (this machine) and Sparks ostrich, dodo, kiwi, emu, rhea, moa
  via passwordless SSH. Fabric IPs 10.55.0.x / 10.55.1.x.

## Code conventions

- Rust 2024 edition, `tokio` runtime, `tracing` for logs, `anyhow` at edges
  and typed errors inside crates, `clap` derive for CLIs, `serde` DTOs in
  `nest-types`.
- `unsafe` only in `nest-fabric`'s verbs layer, each block with a safety
  comment; registered buffers have RAII owners.
- Blocking filesystem/SQLite work goes on dedicated blocking pools; never on
  the FUSE reply path's async executor threads.
- Public API changes update the OpenAPI document in the same commit.

## Definition of done for a milestone

Acceptance list in `ROADMAP.md` green, benchmarks (where applicable) recorded,
`docs/PROGRESS.md` updated, tag pushed.
