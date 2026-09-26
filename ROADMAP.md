# Roadmap

Milestones are sequential deliverables; each ends with the acceptance list
green, a tag `m<N>`, and a short entry in `docs/PROGRESS.md`. Trial deployments
on all seven nodes are the normal test loop from M3 on. Benchmarks live in
`benchmarks/` with the same style as rdmasync (commands, topology, medians,
raw CSV).

## M0 — Skeleton and toolchain (days)

- Cargo workspace with all crates stubbed; `scripts/build.sh`;
  `scripts/deploy-cluster.sh` (build on raptor + ostrich, push to all nodes);
  `packaging/` with systemd user unit, `node.toml` sample, fuse.conf note.
- Rust (Homebrew) on raptor and ostrich. Vendored verbs bindings moved to M4.
- `docs/TOOLING.md` started. CI-free but `cargo test` and `cargo clippy` clean.

## M1 — Metadata core (nest-meta, nest-raft, nest-store)

- Schema, command enum, deterministic apply with property tests.
- openraft spike (0.9 vs 0.10-alpha) → ADR-002a; log store and state machine
  on SQLite; snapshots; membership add/remove/promote.
- Object store with intents journal and startup reconciliation.
- `nest-testkit`: N in-process nodes over TCP; kill/restart/partition helpers.
- Accept: 7-node in-process cluster survives kill of any 3 nodes with
  namespace ops continuing; restart replays intents; snapshot + log compaction
  + rejoin from snapshot works.

## M2 — Single-node FUSE with lifecycle (nest-fuse, nest-data over TCP)

- Full namespace ops, handles, attr/entry caching + invalidation, locks.
- Owner-on-first-mutation, revocation, finalize, seal, immediate deletion.
- Passthrough for sealed local files; kernel-cached reads for remote;
  direct I/O under mutation.
- Accept: pjdfstest-style namespace suite (subset we support) passes;
  concurrent writer test; `git clone` + build of a small repo inside the mount;
  mmap read of a sealed file is passthrough (verified by daemon counters).

## M3 — Multi-node over TCP, trial deployment

- Remote reads/writes routed by ownership; read grants; generation fencing.
- Deploy trial cluster to all seven nodes with synthetic data.
- Accept: the correctness matrix from Proposal A §15 (owner partition during
  mutation, lost append response, rename/unlink with open handles, stale node
  rejoin, generation change during copy, crash between flush and publish) as
  automated tests in testkit, plus manual run on the real cluster.

## M4 — RDMA fabric and prefetcher (nest-fabric RDMA)

- verbs transport; persistent QPs; rails (Spark dual rail always); budgets;
  bounded adaptive prefetcher; source failover.
- Benchmarks at 100G/rail: native local read, local FUSE passthrough, remote
  FUSE stream, whole-file replica creation; sequential and mmap-fault
  patterns; many-shard concurrency; CPU and registered memory.
- Ask the user to raise the switch to 200G and repeat the fabric-bound cases.
- Targets (initial, revise from data): remote sequential read ≥ 80% of
  rdmasync's measured rate on the same pair; passthrough within 5% of native;
  registered memory flat as open shards grow.

## M5 — Placement, HF plugin, CLI

- Rules/manifests/plans/jobs; whole-file transfer engine; free-space planner;
  reconcile and optimize.
- HF resolver: shared-blob store normative, legacy repo-local and no-symlink
  layouts readable (ADR-010); `trees/<commit>.json` for completeness; auto-seal
  at `*.incomplete` rename under `blobs/`; readiness matrix.
- Validation with huggingface_hub 2.0.x (and one legacy 1.2x client) on a
  trial mount: real `hf download` of a small Xet-backed model lands in
  `hub/blobs/<xx>/` with `.refs` appended and no silent fallback; concurrent
  download of the same repo from two nodes with `.locks` working; parallel
  random-offset Xet writes sustain multi-GB/s; `hf cache ls/rm/prune` behave.
- `nest` CLI complete against the OpenAPI contract; `nest import --adopt`.
- Accept: model replicated to a host set by rule; planner frees a requested
  amount without violating exact rules; import of a copied HF cache subtree by
  rename is instant and readable from another node.

## M6 — Stores, backup, archive, recall

- Store registration and health; NAS SMB store via all gateways; raptor scratch
  store via fabric routing; backup versions with manifests; offload and recall;
  metadata snapshot to archive; DR restore test with the cluster down.

## M7 — Web UI

- SPA: cluster/capacity, model readiness matrix, rules, plans with progress,
  stores/backups, events. Embedded bundle option.

## M8 — Hardening, release, migration

- Soak on the trial cluster (reboot Sparks during loads, pull a cable,
  fill a disk). Performance tuning from benchmark data. FUSE-over-io_uring
  evaluation.
- GitHub release with per-arch tarballs; formula in `local-ai-tap`;
  install docs.
- Migration playbook (user-driven): adopt raptor `hub/`, adopt Spark caches,
  symlink `~/.cache/huggingface`, drain `/mnt/scratch`, reformat, register as
  archive.
