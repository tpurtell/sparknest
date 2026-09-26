# Progress log

Newest first. One entry per meaningful step: what landed, what was measured,
what is next. Keep entries short; link to benchmarks and ADRs.

## 2026-09-26 — M2: FUSE frontend on one node

- `nest-data::Vfs`: all filesystem semantics independent of FUSE;
  owner-on-first-mutation for write/truncate/O_TRUNC, finalize linger,
  fsync, local reads, cluster-wide fcntl/flock locks (ADR-014).
- `nest-fuse`: fuser 0.18 adapter; direct I/O for unsealed files,
  passthrough for sealed local files with page-cache fallback, effect-driven
  kernel cache invalidation on its own thread.
- Real-mount tests: POSIX basics, 64 MiB roundtrip, sparse files, flock,
  sealed immutability plus mmap, explicit timestamps across finalize, 8
  parallel random-offset writers (hf_xet pattern).
- Smoke run with the release daemon: git clone/commit/fsck/gc in the mount;
  a crate compiled with its target dir in the mount (13.4 s vs 5.4 s
  native, metadata-bound); restart reconciled 1026 objects cleanly.
- Baseline (raptor, warm, single stream): FUSE write 3.6 GB/s, FUSE direct
  read 3.9 GB/s (1 MiB) to 4.9 GB/s (4 MiB), native 19 GB/s.
- Passthrough negotiates on kernel 7.0 but backing registration needs
  `CAP_SYS_ADMIN`; verified fallback. Verifying passthrough itself waits on
  installing the system unit (root checklist in `docs/ENVIRONMENT.md`).
- Not done from the M2 list: a pjdfstest subset (most of it needs root);
  covered instead by the tests above.
- Next: M3, multi-node data plane over TCP (remote reads and writes routed
  by ownership, revocation acknowledgements, trial deployment on all nodes).

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
