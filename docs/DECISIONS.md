# Architecture Decision Records

Append-only. Reversing a decision gets a new ADR that references the old one.
Format: context → decision → alternatives → consequences.

## ADR-001 — Implementation language: Rust everywhere (2026-09-26)

**Context.** Candidates were Rust, Go, C++, and a split (C/C++/Rust core with a
Go CLI/web layer). The daemon holds registered RDMA memory, speaks the FUSE
kernel protocol, embeds SQLite and Raft, and must run identically on amd64
(Ubuntu 26.04, libfuse 3.18) and arm64 DGX Sparks (Ubuntu 24.04, libfuse
3.14).

**Decision.** One Cargo workspace, Rust for daemon, CLI, tests and tooling;
`unsafe` FFI limited to libibverbs bindings in `nest-fabric`. Web UI is
TypeScript (unavoidable) and talks only to the HTTP API.

**Why.**
- `fuser` (≥0.16) implements the FUSE wire protocol directly and no longer
  links libfuse by default. That sidesteps the libfuse 3.14-vs-3.18 gap: FUSE
  passthrough needs libfuse 3.17+ userspace support but only kernel ≥6.9 with
  fuser, and all seven kernels qualify. `fuser` 0.16 added passthrough
  BackingIds, 0.17 an async API.
- `rusqlite` with the `bundled` feature removes a system SQLite dependency.
- `openraft` (ADR-002) is Rust-native and async.
- The author's inference runtime `glmrt` is already Rust with tokio/axum/clap,
  so conventions and reviewability carry over.
- Memory safety matters for a long-running daemon juggling registered buffers,
  FUSE replies and async cancellation; Rust's ownership model is the right tool
  for buffer lifetime around RDMA completions.

**Alternatives.**
- *Go*: excellent FUSE (`go-fuse`) and HTTP story, but RDMA through cgo with
  GC-managed memory means pinning and copying around registered buffers, and
  the only mature Raft (hashicorp) would pull the whole daemon into Go. A
  two-language split doubles build/release machinery for a solo project.
- *C++*: no ecosystem gain for Raft/HTTP/CLI; the failure modes we most want to
  avoid (use-after-free of a slot still owned by the NIC) are exactly the ones
  C++ does not prevent.
- *Rust core + Go tools*: the CLI is thin API plumbing; a second toolchain buys
  nothing.

**Consequences.** Sparks need a Rust toolchain (rustup) for native arm64
builds, or a cross-build sysroot. verbs bindings are generated once per arch
with bindgen and vendored so libclang is not a runtime build requirement on
every host.

## ADR-002 — Consensus: openraft with a custom SQLite state machine (2026-09-26)

**Context.** Requirement: namespace, ownership and replica validity stay
correct and available when up to a few of the seven nodes are down, including
raptor. Metadata op rate is low (thousands/s during imports, otherwise tens/s);
partitions are rare on one rack; node reboots are frequent.

**Decision.** `openraft`, single Raft group, our own `RaftLogStorage`
(SQLite), `RaftStateMachine` (SQLite `meta.sqlite`, typed command enum,
transactional apply with `last_applied`), and `RaftNetwork` (tonic/gRPC on the
fabric IPs). Snapshots via SQLite backup/VACUUM INTO. All seven nodes voters
by default; learners supported and configurable.

**Alternatives and why not.**
- *raptor as primary, SQLite replicated by log shipping (no consensus).*
  Simplest by far, but raptor down = cluster down, which violates the stated
  requirement. Rejected; noted as the fallback if Raft integration proves
  disproportionate, but there is no sign it will.
- *raft-rs (TiKV).* Battle-tested core algorithm only; synchronous tick-driven
  API; we would write the log store, network, driving loop, snapshot
  orchestration and membership-change ergonomics ourselves. Same trust,
  strictly more glue.
- *hiqlite (openraft + rusqlite, replicated SQL).* Attractive shortcut, but it
  replicates SQL statements and owns the apply loop. We need per-node apply
  hooks (invalidation → immediate deletion; replica publish → inventory;
  lock grant → wake waiter) and conditional semantic commands with fencing
  (`AcquireOwner` must reject a stale epoch). Doing that via SQL affected-row
  checks and polling is fragile.
- *dqlite / cowsql (C, WAL-frame replication).* Right idea, wrong ecosystem:
  C library, unstable governance (fork), thin Rust bindings, leader-routed
  client protocol.
- *etcd/consul sidecar.* External process in Go, no local materialized view
  for fast FUSE lookups, another thing to deploy.
- *Write our own Raft.* Core is small; membership change, snapshot install,
  log compaction and the interaction between them are where the bugs live.
  Not worth it when openraft exists.
- *Leaderless/CRDT.* Rename, ownership and invalidation need a total order.

**Consequences.** Namespace reads are local with bounded staleness; the data
plane is generation-fenced so staleness can never surface as stale bytes.
openraft's API is pre-1.0; pin the exact version and isolate it behind
`nest-raft` so an upgrade is a one-crate change. Spike 0.9 vs 0.10-alpha in
Milestone 1 and record the result here as ADR-002a.

## ADR-003 — FUSE library: `fuser`, no libfuse (2026-09-26)

**Decision.** `fuser` with `libfuse` feature off; mount via `fusermount3`
(present on all hosts). Enable passthrough (`max_stack_depth ≥ 1`) for sealed
local replicas, kernel page cache for sealed/stable remote reads, direct I/O for
files under mutation, writeback cache off.

**Alternatives.** `fuse-backend-rs` (virtiofs-oriented, heavier), libfuse via
FFI (blocked by 3.14 on Sparks for passthrough), a kernel module (no).

**Consequences.** FUSE-over-io_uring is not available through `fuser` today;
treat it as a measured later optimization, possibly by contributing upstream.

## ADR-004 — Fabric: verbs FFI in Rust, design ported from rdmapipe/rdmasync (2026-09-26)

**Decision.** Thin `-sys` bindings to libibverbs (dynamic link; rdma-core is
installed everywhere), a safe RAII layer, and a transport trait with RDMA and
TCP implementations. Persistent RC QPs per node pair, per-rail; registered
slot rings; TCP bootstrap over the authenticated control connection. Start
with the SEND/RECV ring protocol already measured in `rdmapipe`, then evaluate
one-sided WRITE.

**Alternatives.** Link `rdmapipe/transport.c` via FFI (a stream, not a
request/response file server; would need a rewrite anyway), `libfabric`/UCX
(large dependencies, no measured benefit on this homogeneous mlx5 fabric),
existing Rust crates such as `ibverbs`/`rdma-core-sys` (usable as a starting
point for bindings but not for the transport design).

## ADR-005 — Backing layout: objects by identity, not by path (2026-09-26)

**Context.** The original musing wanted the backing store to be a
human-browsable mirror of the tree. Proposal A softened that to "local
materializations with hard-link anchors".

**Decision.** `objects/xx/<file_id>.<generation>`. Renames, hard links and
unlinks are metadata-only. Deletion addresses a specific generation instance.
`nest export` and `nest recover` materialize a subtree from `meta.sqlite` +
objects with the cluster down, so recoverability is preserved.

**Alternatives.** Path-mirrored tree (every replica host must replay renames,
offline nodes drift, anchors get messy, late deletes can hit the wrong file),
hybrid with anchors (two sources of truth on disk).

**Consequences.** `ls` on the raw store is meaningless without the DB; the
mount is the browsable view. Import by `rename(2)` is trivial and zero-copy.

## ADR-006 — Management API: HTTP+JSON with OpenAPI, SSE events (2026-09-26)

**Decision.** axum on a local Unix socket and on the fabric IP with bearer
auth; OpenAPI checked in; `nest` CLI and web use only this. gRPC is used for
Raft RPC only.

**Alternatives.** gRPC for management (worse for a browser SPA without a
proxy), embedding the UI as the only interface (rejected by the user; the
daemon exposes commands, tools consume them).

## ADR-007 — Identity model: single-tenant, fixed owner per host (2026-09-26)

**Decision.** Store mode bits, not uid/gid; present every file as the mounting
user on each host (uid 1000 on raptor, 1001 on Sparks). `allow_other` so root
and containers can read the mount.

**Alternatives.** Store and map uids (complexity for one user), require equal
uids (would force re-numbering accounts).

## ADR-008 — No hashing, no application checksums (carried from Proposal A)

Transfers are accepted on `(file_id, generation, expected size, all ranges
complete, I/O success)`. No digests in metadata or on the wire. HF blob names
are opaque. Import of equal-size same-path files from several hosts as one
generation requires an explicit flag.

## ADR-009 — Distribution: build script, systemd samples, Homebrew tap release (2026-09-26)

**Decision.** `scripts/build.sh` produces daemon, CLI and web bundle for the
host arch; `scripts/deploy-cluster.sh` builds natively on raptor (amd64) and
one Spark (arm64) and pushes binaries and sample configs to all nodes for
trial deployments. `packaging/` holds systemd units, `fuse.conf` guidance,
`node.toml` samples. Final release: GitHub release with per-arch tarballs and a
formula in `../local-ai-tap` alongside `rdmasync` and `rdmapipe`.

## ADR-010 — HF cache layout: shared blob store normative, legacy readable (2026-09-26)

**Context.** huggingface_hub ≥ 1.32 stores Xet payloads once at
`hub/blobs/<xx>/<xet_hash>` with a marker file and `.refs` hints, and makes
repo `blobs/<etag>` a relative symlink. Every existing cache on raptor and the
Sparks is the older repo-local layout (venvs run 1.24–1.30); the marker on
raptor was created on 2026-09-25 by a newer client.

**Decision.** The sparknest-managed hub targets the shared-blob layout.
Validation target is huggingface_hub 2.0.x (what the user runs via Homebrew);
the layout floor is 1.32, and 2.0 changed the HTTP stack and removed deprecated
APIs without touching the cache format. Venv pins may lag as long as they are
≥ 1.32 for anything that downloads into the mount. The resolver reads any layout by
following symlink chains to terminal files. `nest import --adopt` preserves
legacy layouts unchanged. Sealing triggers on rename from `*.incomplete` under
any `blobs/` directory. `trees/<commit>.json` drives completeness.

**Alternatives.** Rewriting legacy repos into the shared store (needs Xet
hashes we cannot compute without hashing; rejected), forcing
`HF_HUB_DISABLE_SHARED_BLOBS=1` for uniformity (loses cross-repo dedup that is
free for us), sealing on close instead of rename (Xet writes in parallel from
many threads, close is not a boundary).

**Consequences.** The write path is designed around concurrent random-offset
`pwrite` into one file. M5 must verify no silent fallback to repo-local
storage on the FUSE mount. User venvs should be upgraded to ≥ 1.32 before
migration.

## ADR-011 — Lifecycle refinements found while implementing nest-meta (2026-09-26)

1. **Replicated generation state is STABLE or OWNED only.** Revocation and
   finalization are owner-local phases: in both, every other node already
   routes to the owner, so replicating them buys nothing. `AcquireOwner`
   bumps generation and epoch together, invalidates every other replica in
   the same apply, and the owner withholds its first mutation until all
   nodes acknowledge applying the grant or their read lease has expired.
2. **Unsealed regular files are always opened FUSE direct-I/O.** Only sealed
   files get passthrough (local) or the kernel page cache (remote). A sealed
   file never changes, so its cache is valid forever and needs no revocation.
   This removes read grants for page caches entirely; generic large-file trees
   get the fast path through an auto-seal policy (M5) or `nest seal`.
3. **Unlink while open, cluster-wide, without per-open consensus.** When the
   last name goes, a regular file becomes an orphan held for every live
   session. Each node releases orphans it has no handles to, in batches;
   session expiry (including a node restart) releases the rest. Opens of an
   orphan fail with ENOENT, which matches POSIX for a removed name.
4. **Names are bytes.** Stored as SQLite BLOBs; only `/`, NUL, `.`, `..` are
   refused.
5. **Readdir offsets are per-entry cookies** assigned at insertion, so
   concurrent inserts and removals never skip or repeat entries.

## ADR-012 — Control-plane RPC: small framed protocol instead of gRPC (2026-09-26)

**Decision.** `nest-rpc`: one TCP connection per peer pair and direction,
length-prefixed frames with request id and service number, many concurrent
requests per connection, mutual HMAC-SHA256 challenge-response over the
cluster secret at connect. Payloads are postcard-encoded serde types shared
across crates. A reachability filter lets tests partition nodes. Supersedes
the "tonic/gRPC for Raft" line in ADR-006; the management API stays HTTP.

**Why.** tonic needs `protoc` on every build host and a second schema
language for types that already derive serde. The traffic is modest
(Raft, write forwarding, data-service control); bulk bytes go over
`nest-fabric`.

**Consequences.** No cross-language clients for the control plane, which is
fine: the CLI and web use the HTTP management API.

## ADR-002a — openraft 0.9.25 pinned (2026-09-26)

**Decision.** Pin `openraft = "=0.9.25"` with `storage-v2` and `serde`.

**Why.** 0.9.x is the maintained stable line (0.9.25 released 2026-07-28);
0.10 has published 35 alphas with breaking changes roughly weekly. The 0.9
v2 storage traits give everything we need: a direct `RaftStateMachine::apply`
(our synchronous effect hook), SQLite-backed `RaftLogStorage`, file-backed
snapshot data (`tokio::fs::File`, built with `VACUUM INTO`, installed with
the SQLite restore API), `get_read_log_id` for read barriers, and
learner/joint-consensus membership changes. All of it is exercised by
`crates/nest-raft/tests/cluster.rs`: forwarding with read-your-writes, leader
failover and rejoin, minority refusal, snapshot catch-up of a wiped node,
and exactly-once application of retried requests.

**Consequences.** Everything openraft-specific lives in `nest-raft`; moving
to 0.10 later is a one-crate change.
