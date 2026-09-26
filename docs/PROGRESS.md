# Progress log

Newest first. One entry per meaningful step: what landed, what was measured,
what is next. Keep entries short; link to benchmarks and ADRs.

## 2026-09-26 — M1 complete: replicated metadata and local replica lifecycle

- `nest-meta`: schema, semantic commands, deterministic apply, effects,
  fsck; property test (3000 random sequences) for invariants and replica
  determinism.
- `nest-rpc`: authenticated multiplexed RPC with partition filter (ADR-012).
- `nest-raft`: openraft 0.9.25 over SQLite (ADR-002a): dedup, synchronous
  effect hook, VACUUM INTO snapshots, restore-API installs, forwarding with
  read-your-writes, barriers, leader lease refusal (ADR-013).
- `nest-store`: objects by (file, generation), staging, adoption.
- `nest-data`: effect-driven deletion (immediate, per-file ordered),
  ownership preparation, reconcile-first recovery, caught-up gate, sessions,
  cluster-wide orphans.
- `sparknestd`: node wiring and a daemon that runs; `nest-testkit`: real
  in-process clusters with stop/restart/wipe/partition.
- M1 acceptance (`crates/nest-testkit/tests/m1.rs`), 7 nodes: any three
  down keeps writing; a fourth refuses; restart and snapshot rejoin
  converge; stale replicas are deleted immediately and never served.
- Deferred from M0 to M4: vendored verbs bindings (not needed until the RDMA
  transport). Rust on ostrich via Homebrew hit a ghcr.io download reset;
  retrying.
- Next: M2, the FUSE frontend on one node (namespace ops, handles,
  direct-I/O for unsealed files, passthrough for sealed, locks).

## 2026-09-26 — Design accepted

- Environment surveyed on all seven nodes (`docs/ENVIRONMENT.md`).
- Official proposal written (`PROPOSAL.md`), ADR-001…009 recorded.
- Language: Rust throughout. Consensus: openraft with custom SQLite state
  machine. FUSE: fuser without libfuse. Fabric: verbs FFI ported from
  rdmapipe/rdmasync design.
- Blocking on user: one-time root checklist in `docs/ENVIRONMENT.md`
  (fuse.conf `user_allow_other`, mountpoints, linger).
- Next: M0 skeleton (workspace, build/deploy scripts, packaging samples,
  rustup on raptor + ostrich, vendored verbs bindings).
