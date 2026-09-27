# Progress log

Newest first. One entry per meaningful step: what landed, what was measured,
what is next. Keep entries short; link to benchmarks and ADRs.

## 2026-09-27 — Reads from other hosts made fast (ADR-030…035)

- Spread reads (ADR-030): local first, rendezvous stripes, capped by
  measured disk rates; 39.6 GB/s aggregate when all seven hosts load a
  model at once (21.9 local only).
- Fixed: fabric reads were not cancellation-safe (a reused landing slot
  took a late answer: short reads, wrong bytes); unmount hung 90 s.
- Scattered reads (ADR-031/033): files whose readahead goes unused are read
  directly, recorded with the file, back to readahead only for long runs.
- Fabric (ADR-032/034/035): slot tiers (4 KiB / 128 KiB / 4 MiB), a byte
  budget, protocol 2; every served read through io_uring on the completion
  thread; readers of scattered files reply to FUSE from it; rendezvous
  affinity per 1 MiB region; only the RDMA functions a port needs; threads
  awake while hot. Cold 4 KiB rows from other hosts: 128 µs median with one
  reader (was 620+), 66k/s with 32; tensor loads 7.2 GiB/s
  (benchmarks/M9-FABRIC-READS.md).
- I/O page: latency by kind and size over 10 s / 1 min / 10 min, readahead
  waste; Overview: tracked direct reads, crackling bubbles; Jobs across
  hosts; hf import safe to run on all hosts at once.
- Next: a ds41rt round on this build; per-file readahead shared across
  opens; Spark NVMe/CPU power settings (root checklist).

## 2026-09-27 — Usage, goal-based plans, space treemaps, hf import, a new UI

- **Usage statistics (ADR-028).** Every host records per file and day:
  opens, last open, bytes read locally / from other hosts / from archives,
  in its own `usage.sqlite` (not replicated; 30 days). Handles count with
  one atomic add; the VFS folds counters every 5 s.
- **Plans have goals:** *free* (least recently used redundant copies
  first), *tidy* (not opened within 1d/1w/1m, down to one copy), *speedup*
  (copy files to hosts that pull them over the network). Every planned copy
  says why. Plans offload only into archives the user picks.
- **Space tree** (`/v1/space/tree`): by model (shared hf 2.0 blobs credited
  to their repo through the snapshot → repo-link → store chain) or by path;
  by size or by cost with copies; per host, per archive or the cluster.
  `/v1/hf/detail` for one repo: files by snapshot name, where each lives,
  revisions and refs, rules, per-host usage.
- **Jobs**: cancellable (no new files start; nothing is evicted after a
  cancelled offload or plan), timed, logged at start; imports mirror their
  progress into the job; failures name the path. A copy no longer fails on
  the first source that answers Stale when the file has not changed.
- **Logs** kept in memory on every node (20,000 lines), merged in the UI
  and `nest logs`.
- **Imports**: relative destinations are inside sparknest; `--copy` copies
  across filesystems straight into the store. **`nest hf import`** brings a
  Hugging Face cache (classic or hf 2.0 shared-blob layout) into the hub:
  blobs placed where huggingface_hub looks, `hf download` finishes each
  snapshot (only what the source never finished is fetched), offline the
  snapshot links are mirrored; refs copied, every file verified; `--move`
  swaps in a symlink to the hub. Live check on the trial: `models--gpt2`
  imported and finalized by hf in 1.1 s; `HF_HUB_OFFLINE=1 hf download`
  resolves it from sparknest.
- **Web UI rewritten** (Svelte 5 + Vite, one embedded file, no CDN): WebGL
  plasma backdrop and a host constellation whose lightning arcs follow real
  fabric traffic; WebGL treemaps with zoom and right-click / long-press
  actions; Models with % of bytes per host, live copy progress, one-click
  copy and a details drawer; Plans by goal with draggable free-space
  handles; upload (files, folders, drag and drop), download (folders as a
  streamed tar) and delete; phones get a top bar with a menu button.
- Tests: 87 (usage + speedup + tidy, space tree incl. hf 2.0 link chains,
  cancelled offload keeps live copies, hf import of both layouts with move,
  copy across filesystems via /dev/shm).

## 2026-09-27 — M8: relaxed durability, automatic recovery, faster metadata

- openraft 0.10 (alpha.35): leader replicates submitted entries, appends
  merge; Raft log and metadata in WAL/NORMAL with our own ~1 s checkpoints;
  votes fsynced (ADR-026). Metadata operations 3–5× faster on the cluster
  (benchmarks/M8-RELAXED-DURABILITY.md); `nest rm -r` removes ~20k
  entries/s.
- File data stays durable: finalize fdatasyncs; application fsync/fdatasync
  pass through plus a majority metadata barrier.
- Crash detection (boot id), recovery fsck before serving, automatic
  re-found after a full outage or a Raft-format upgrade, incarnations,
  /.lost+found/<run>/<host>/<path> (ADR-027). The trial cluster upgraded
  itself from format 1 in place.
- Boot integration: sd_notify readiness and status, `nest wait-ready`,
  mountpoint guard with automatic rescue, fabric re-probe.
- Chaos soak on the new build: 12 kills, 69 GB written, zero corruption.

## 2026-09-27 — M8: io_uring, soak, disk-full reserve

- FUSE over io_uring (ADR-025) on all hosts after the user enabled
  `fuse.enable_uring=1`: single-stream reads 2–8× faster on a Spark
  (benchmarks/M8-FUSE-IO-URING.md).
- 15-minute chaos soak with 21 SIGKILLs: 88 GB written, 84 GB read and
  verified, zero corruption (benchmarks/M8-SOAK.md). The cluster also came
  back intact from a reboot of all seven hosts.
- Data reserve protects metadata from a full disk (ADR-024); import adopts
  existing blobs without copying; the daemon clears dead FUSE mounts.
- The Homebrew formula passes build, test, style and linkage natively on
  raptor (x86_64) and ostrich (arm64).
- Next: cut v0.1.0 when the user wants it published (repo is private).

## 2026-09-27 — M8: release tooling, migration tooling

- Every CLI command prints plain text (`--json` for machines); the web UI
  has a Space view for plans and groups; plans revalidate rules per step.
- `scripts/release.sh` builds v0.1.0 from `git archive`: native x86_64 and
  aarch64 tarballs (max GLIBC_2.39, the tap's ceiling), checksums, and the
  Homebrew formula rendered from `packaging/homebrew/sparknest.rb.in`.
  `scripts/test-formula.sh` builds, tests (a one-node import, offload,
  snapshot and export round trip), audits style, linkage and ABI through a
  throwaway tap: passes on raptor.
- Import adopts blobs the namespace already has (hard link, no transfer),
  so each Spark's existing cache joins without copies or extra space.
- `scripts/cluster.sh` + `scripts/cluster-main.env` run the real cluster
  beside the trial; `cluster.sh bootstrap` founds it once and refuses when
  Raft state exists. `docs/MIGRATION.md` is the user-driven playbook;
  `docs/INSTALL.md` and `docs/RELEASING.md` cover new users and releases.
- Blocked on the user: making the GitHub repo public and publishing the
  release; the root checklist; the switch at 200G for benchmarks.

## 2026-09-27 — Backups, web UI, groups and free-space plans

- M6 finished: versioned backups and restore into archive stores, periodic
  metadata snapshots, and `sparknestd export` for offline recovery
  (ADR-021).
- M7: every daemon serves the web UI and the JSON API (ADR-020); views for
  nodes, models, files, rules and jobs.
- Host groups (`nest group set sparks ostrich,dodo,...`, then `@sparks`
  anywhere a host list is taken) and free-space plans
  (`nest plan --free raptor=800GiB`, `nest plan apply ID`), ADR-022.
- Next: M8 hardening and soak, release packaging for GitHub and the
  `local-ai-tap` formula, migration playbook; the M4 root-level tuning pass
  and the 200G benchmark need the user.

## 2026-09-26 — M6: archive stores on real storage

- Archive stores hold replicas behind gateways, guarded by a marker file
  (ADR-019): offload, reads through gateways, recall, invalidation.
- Trial stores on raptor's `/mnt/scratch` (ntfs3, one gateway) and the NAS
  share (SMB, seven gateways) both healthy; a 35 GiB model offloaded to
  scratch, read from moa through raptor, and recalled to ostrich in 9 s.
- Automatic rules (`--auto`) and `nest cluster` membership commands landed
  (M5 leftovers).
- Not yet: retained versioned backups and metadata snapshots to an archive.
- Next: M7 web UI.

## 2026-09-26 — M5: placement, import and the CLI on the trial cluster

- Directory seal policies (HF rename-from-incomplete, on-finalize); rules;
  selectors and manifests (path trees following symlinks; `hf:org/name@rev`);
  target-pulled whole-file replication over RDMA; eviction with last-copy
  protection; hard-link import; management API on a Unix socket; `nest`
  CLI (ADR-017).
- On-disk format versioning after a trial upgrade crash (ADR-018).
- Real model (`benchmarks/M5-REAL-MODEL.md`): 35 GiB imported in ~1 s,
  replicated at 4.2 GB/s (93% of rdmasync), loaded at 9.6 GB/s cold and
  85 GB/s warm on a node with a copy, 5 GB/s cold from a node without one.
- Still open in M5: automatic reconcile on finalize, `nest cluster`
  membership commands.
- Next: M6 stores/backup/archive, M7 web UI, then the M4 root-level tuning
  pass once the system unit is installed.

## 2026-09-26 — M4 in progress: RDMA fabric live on the trial cluster

- `nest-fabric`: C shim over libibverbs, rail discovery, pull protocol with
  WRITE_WITH_IMM, bounded pools, per-device pollers (ADR-016). Loopback
  20.3 GB/s (raptor), 14.1 GB/s (ostrich).
- Remote reads use RDMA with sequential readahead (stable generations only)
  and fall back to TCP. Fast paths on the FUSE thread after finding that
  Spark deep idle states cost ~0.5 ms per FUSE request.
- Seal from any mount: `user.sparknest.sealed` xattr.
- Numbers in `benchmarks/M4-FIRST-MEASUREMENTS.md`: warm sealed remote
  reads 22 GB/s from page cache; raptor reads Spark-owned files at 4.5 GB/s;
  Spark single streams through the daemon are latency-bound (1.4–2 GB/s).
- Blocked on root for the rest of M4: system unit (passthrough +
  readahead), optional deep-idle experiment, 200G switch setting.
- Next: M5 (placement, replication engine, HF plugin, CLI) while the root
  steps are pending; then return to M4 tuning with the new levers.

## 2026-09-26 — M3: multi-node data plane, first trial on all seven hosts

- Reads route to local copy, owner, or live holder by exact generation;
  mutations route to the owner (this node if it holds or discards the
  content, else a reachable holder); owners fence every member or wait out
  its read lease; leases renew by leader read barriers (ADR-015).
- M3 tests: 7 cross-node scenarios including stale-read regression under
  revocation (disabling fencing makes it fail) and lease-bounded fencing
  with a partitioned holder.
- Trial cluster deployed to raptor + 6 Sparks (native amd64 and arm64
  builds; arm64 on ostrich via rustup), mounted at `~/sparknest-test/mnt`.
  Cross-node create/read/mkdir work; a 1 GiB file written on raptor reads
  byte-identical on ostrich and moa. Hard-killing three Sparks left the
  cluster writable; restarted nodes caught up.
- Remote read over the M3 TCP path: ~640 MB/s single stream (one 1 MiB
  request in flight, latency-bound). M4 adds read-ahead and RDMA.
- Next: M4, the RDMA fabric and bounded prefetcher, with benchmarks against
  rdmasync's measured ceilings.

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
