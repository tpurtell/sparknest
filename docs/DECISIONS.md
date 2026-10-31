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
following symlink chains to terminal files. `nest import` preserves
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

## ADR-013 — Metadata write outcomes, rejoin, and reconcile-first recovery (2026-09-26)

1. **Definite vs unknown write failures.** A leader whose last quorum
   acknowledgement is older than the election timeout refuses proposals
   without appending them, so `NoQuorum` (EROFS) means "not applied". If an
   attempt may have reached a leader and the proposal deadline passes, the
   error is `Unavailable("outcome unknown")` (EIO): the entry may still
   commit later. Within the deadline, retries are safe because the state
   machine deduplicates `(client, seq)`.
2. **Nodes may lose their disk.** `loosen-follower-log-revert` is enabled so
   a re-imaged node whose log went backwards does not panic the leader. It
   rejoins and catches up by snapshot. Operationally, a node that lost its
   disk should be removed and re-added (`nest cluster rejoin`, M5) so it
   cannot cast a second vote in a term it already voted in.
3. **Reconcile against committed state, not an intents journal.** Objects
   keyed by `(file, generation)` let startup reconciliation resolve crash
   windows from metadata alone: unexpected objects are deleted, a missing
   working object is recovered from the previous generation and truncated
   to the recorded size, missing replicas are retired, leftover ownerships
   are finalized, and staging is discarded. Reconciliation waits until the
   local state machine has re-applied everything the log had committed
   before the crash (meta.sqlite uses synchronous=NORMAL). Only import by
   rename needs a real intent (M5).
4. **Caught-up gate.** A started node serves no object until it has passed
   a leader read barrier, so an invalidation committed while it was down is
   applied (fenced) before any read could use the stale copy.

## ADR-014 — FUSE frontend shape and kernel realities (2026-09-26)

1. **A FUSE-independent `Vfs`** (nest-data) holds all filesystem semantics;
   `nest-fuse` only translates requests, dispatching each onto tokio and
   replying from there. Cross-node behaviour is tested against `Vfs`
   without mounts; kernel behaviour is tested with real mounts.
2. **Kernel cache invalidation** comes from applied effects via a tap and
   runs on a dedicated thread, never on the apply path or a request path
   (`inval_entry` can wait on a directory lock an in-flight request holds).
3. **Passthrough needs `CAP_SYS_ADMIN`** (measured on kernel 7.0: backing
   registration fails with EPERM for an unprivileged daemon even though
   `FUSE_PASSTHROUGH` negotiates). The daemon falls back to page-cache mode
   automatically. Production runs the system unit `sparknestd@.service` as
   the user with that single ambient capability.
4. **flock release on close is asynchronous.** This kernel delivers the
   flock unlock with the RELEASE request after `close()` returns, so another
   descriptor may briefly still see the lock. Blocking lockers and polling
   lockers (huggingface's filelock) are unaffected.
5. **Ownership lingers 250 ms** after the last writer closes before the
   generation is finalized, so close/reopen/append patterns do not churn
   generations. Explicit time changes on an owned file are applied to the
   working object so they survive finalize.
6. **Not supported:** extended attributes (ENODATA/ENOTSUP), device nodes,
   FIFOs and sockets (EPERM), chown (accepted and ignored; ADR-007),
   ioctls (ENOTTY).

## ADR-015 — Multi-node data plane: routing, fencing, read leases (2026-09-26)

1. **Routing.** Reads use a local copy when this node may serve it, else the
   owner (while OWNED) or a node holding a live copy of the current
   generation. Every remote read names its generation; a server refuses a
   generation that is not current in its view (`Stale`) and the reader
   catches up and retries. Mutations go to the owner.
2. **Who owns.** On the first mutation of a STABLE file, the proposer picks
   the owner: itself if it holds the content or the mutation discards it
   (O_TRUNC, truncate to zero), otherwise a reachable holder. A 40 GB file
   is never moved to make a small edit.
3. **Fencing (revocation).** The owner, on observing its grant at log index
   `i`, sends `Fence(file, i)` to every member. A member answers once it has
   applied `i` (so its reads route to the new owner) and no older local read
   of the file is running, after dropping page caches. The owner withholds
   the first mutation until all members answer or, for any that do not, one
   read lease has passed since the grant. New files skip fencing.
4. **Read leases.** A node renews a lease with a leader read barrier every
   quarter period (default 2 s) and serves local copies only while it
   holds one. A partitioned node therefore stops serving within one lease,
   which is what makes (3) safe without its acknowledgement.
5. **Writers anywhere.** Remote writer handles join the owner's epoch on
   first write and leave on close; a node's session expiry drops its
   writers. Unused ownerships granted on someone's behalf finalize after
   the linger.
6. **Transport.** M3 carries bytes in `nest-rpc` messages over TCP;
   M4 moves ranged reads onto the RDMA fabric behind the same calls.

Tests (`crates/nest-testkit/tests/m3.rs`): visibility before close,
write-through-holder, truncating ownership, readers never regress to stale
bytes during revocation (disabling fencing makes this fail), unlink with a
remote open handle, owner partition fails closed, and a partitioned holder
forces the owner to wait out its lease while serving nothing stale.

## ADR-016 — RDMA fabric design as built (2026-09-26)

1. **C shim, not bindgen.** `crates/nest-fabric/csrc/nf_shim.c` wraps the
   inline verbs calls behind our own tiny ABI and is compiled against each
   host's installed headers (rdma-core 50 and 61 both work).
2. **Rails are RoCE v2 GIDs** whose IPv4 is configured on an UP netdevice;
   a link pairs rails by subnet, one RC QP per pair, so raptor's single port
   uses both subnets against a Spark's two functions.
3. **Pull protocol.** The reader SENDs a 64-byte request naming a landing
   slot (remote-writable registered memory); the server reads through the
   data service (`ReadSource`, where generation fencing lives) into a
   registered staging slot and answers with RDMA WRITE_WITH_IMM
   (imm = slot << 23 | len) or a small error SEND. TCP only sets links up.
4. **Bounded memory.** Per-device landing and staging pools (defaults 128
   and 64 × 4 MiB), per-lane windows sized so both directions fit the
   peer's receive ring. Readahead holds chunks in landing slots and only
   fetches speculatively while a quarter of the pool is spare.
5. **Readahead only for STABLE generations.** A file being written by its
   owner is read with exact, uncached ranges.
6. **Failure.** A failed completion puts the lane's QP into the error state
   (the NIC stops touching its buffers) and fails only its waiters; the
   next read reconnects the link; reads fall back to TCP meanwhile.
7. **Fast paths on the FUSE thread** (local reads, readahead hits, owner
   writes): measured necessary on the Sparks, whose deep idle states make
   each thread hand-off expensive (benchmarks/M4-FIRST-MEASUREMENTS.md).

## ADR-017 — Placement, import and seal policies as built (2026-09-26)

1. **Seal policies on directories**, inherited: `off`, `rename_from_incomplete`
   (seal a regular file renamed from `*.incomplete`; huggingface_hub's
   completion step, applied at finalize if the rename lands first), and
   `on_finalize`. Applied deterministically in apply.
2. **Selectors → manifests.** Path selectors walk a tree and follow symlinks
   inside the namespace, which is exactly how an HF snapshot reaches
   repo-local blobs and the shared blob store; `hf:org/name@rev` narrows to
   one revision (refs/ and trees/ included). Symlinks that leave the
   namespace are reported as dangling.
3. **Whole-file replication is pulled by the target** into staging over
   RDMA (TCP fallback), accepted on expected length and completed I/O,
   fsynced, moved into place, and published conditionally on the
   generation; a generation change discards the copy.
4. **Rules are durable, jobs are not.** Rules live in replicated metadata;
   replicate/reconcile jobs live on the coordinating node and are
   idempotent, so a lost job is re-run, not resumed. Eviction never removes
   the last live copy.
5. **Import by hard link.** Reserve ids, link each source into the store
   under its final id, commit STABLE entries, then (with `--move`) unlink
   sources. No data is copied and a crash never loses a source (this is the
   "intent" ADR-013 anticipated, without a journal). `*.incomplete` and
   `*.lock` files are skipped. Seal mode `auto` seals files under `blobs/`
   below a rename-from-incomplete policy (refs/ and trees/ stay writable
   because huggingface_hub rewrites them in place; so do the shared
   store's `.refs` hints and marker).
   **Adoption.** A blob under `blobs/` whose path the namespace already
   has, with the same size and a stable generation, becomes this node's
   copy of it (hard link, then a conditional `PublishReplica`): blob names
   are content hashes, so equal paths mean equal bytes. This is how each
   Spark's existing cache joins without copying (2026-09-27). A size
   mismatch is reported and the file left alone.
6. **Management API** on a 0600 Unix socket; `nest` CLI uses only it.
   Nodes register their names in the stores table at startup.

## ADR-018 — On-disk format stability (2026-09-26)

**Context.** A trial node crashed on restart after `Command` variants were
inserted mid-enum: postcard encodes enum variants by position and struct
fields in order, so old Raft log entries decoded as different commands.

**Decision.** `nest_meta::FORMAT_VERSION` covers every persisted encoding
(Raft log entries, deduplicated replies in `sm_dedup`, snapshots). The log
store records it and refuses to open state written in another version with
an actionable error. Persisted enums (`Command`, `Reply`, `NestError`) are
append-only; `crates/nest-meta/tests/format.rs` pins every variant's
position. Any other change (a field added to a persisted struct, a removed
variant) bumps the version and needs an upgrade procedure: drain, snapshot
with the old version, and install with the new one (to be built with the
first such change). Decode failures are storage errors, never panics.

## ADR-019 — Archive stores are stores that hold replicas (2026-09-26)

An archive store (a folder reachable from one or more gateway nodes: an SMB
share mounted everywhere, or a disk on one node) holds complete replicas in
the same object layout as a live store, with store ids from 2^20 up.
Everything else follows from existing rules:

- **Offload** = replicate into the archive, then evict the live copies (the
  last-copy rule allows it because the archive copy is live).
- **Reads** of archive-only files stream through a gateway (the gateway's
  data service serves from the archive object), after any live copy.
- **Recall** = ordinary replication; archive gateways are holders.
- **Writes** invalidate archive copies like any replica; the store's first
  gateway deletes them.
- **Health**: a marker file (`.sparknest-store`, cluster + store id) must be
  present; an unmounted share is never read from or written into.
- Each gateway stages into its own directory; renames fall back to
  check-then-rename where `RENAME_NOREPLACE` is unsupported (CIFS, ntfs3).

Backups as retained, versioned snapshots (separate from replicas) and
metadata snapshots to an archive remain to be built.

## ADR-020 — Web UI: one static page served by every daemon (2026-09-26)

**Decision.** The web UI is a single self-contained HTML/JS/CSS page
(`crates/nest-api/web/index.html`, embedded with `include_str!`), served at
`/` on each daemon's `api_listen` address. The same API routes are served
there behind a bearer token (HMAC of a fixed label under the cluster
secret, identical on every node); `nest ui` prints a link carrying it. The
page talks only to the API, like the CLI.

**Why not a Vite/TypeScript SPA (ADR-006's plan).** No npm build in the
release pipeline, nothing to deploy separately, and every node can serve
it. The page is small enough that a framework adds more than it saves; if
it grows, the API contract does not change.

**Views.** Overview (nodes, stores), Models (per-repo readiness matrix
across nodes and archive stores; clicking a cell copies there or removes
from there), Files (browse with copy locations, seal toggle, copy-to),
Rules (create, apply, delete), Space (free-space plans and host groups,
ADR-022), Jobs (live progress).

## ADR-021 — Backups, metadata snapshots and offline export (2026-09-26)

1. **Backups are not replicas.** A backup captures a selection's structure
   (directories, symlinks, files, modes, times) into an archive store:
   `backups/manifests/<id>.sqlite` plus content in `backups/objects/`, keyed
   by (file, generation) and shared by every backup that captured that
   generation. Live writes, deletes and invalidations never touch them.
   Catalog rows live in replicated metadata (`backups` table, schema v2).
2. **Consistency.** Each file is copied at its exact generation (reads are
   generation-fenced); a file being written is reported and the backup is
   not recorded rather than recorded inconsistently.
3. **Restore** recreates the capture under a destination as new files,
   relative to the capture's common base directory, so symlinks between an
   HF repo and its shared blobs keep resolving.
4. **Delete** removes the manifest and every object no remaining manifest
   in that store references.
5. **Metadata snapshots**: the leader writes `meta/meta-<unix>-<index>.sqlite`
   into every healthy archive store every 6 h (14 kept); `nest backup meta`
   does it on demand.
6. **Offline export**: `sparknestd export --meta SNAPSHOT --objects DIR...
   --out DIR [--path P] [--link]` rebuilds any subtree as a plain directory
   tree from a snapshot and any object directories (live stores, archive
   roots, backup areas), with no cluster running.

## ADR-022 — Host groups and free-space plans (2026-09-27)

1. **Groups** are named host sets kept in replicated metadata (`groups`
   table, schema v3). Anywhere a host list is accepted (rules, replicate,
   plans) `@name` expands to the group's members, resolved when used, so
   editing a group re-targets every rule that names it on the next
   reconcile.
2. **A plan** is computed, shown and then applied; nothing moves while
   planning. Input is a desired free-space floor per host (or per group
   member). For each host short of its floor the planner, largest copies
   first:
   - evicts copies that exist elsewhere and that no rule requires here;
   - then offloads sole copies to the healthiest archive store with room
     (copy there, then evict here);
   - never removes a last copy or a rule-required copy. A shortfall that
     only those could cover is reported as blocked, naming the rules.
3. **Apply revalidates every step.** Evictions name exact generations, so
   a file rewritten since planning keeps its new copy; the state machine
   refuses a last copy; and rules are re-read before each step, so a copy a
   rule made since planning requires is kept and reported. Plans are held
   in memory on the node that made them and expire with it.
4. **Groups a rule names cannot be deleted**, so rules and plans always
   resolve.
5. Both front ends: `nest plan` / `nest group` and the web UI's Space view
   (targets per host or group, the proposed steps with their files, the
   blockers, Apply, and group editing).

## ADR-023 — FUSE over io_uring: deferred behind the idle-state experiment (2026-09-27)

**Context.** On the Sparks, daemon-mediated I/O is bound by about 0.5 ms
per FUSE request (benchmarks/M4-FIRST-MEASUREMENTS.md). The suspected cause
is waking an idle core: Spark ACPI LPI states take 231–433 µs to exit.
FUSE over io_uring (kernel 6.14+) gives each CPU its own request queue, so
a request is answered on the core that issued it, which is awake, and
each round trip saves a read/write syscall pair.

**Findings.** The kernels have `CONFIG_FUSE_IO_URING=y`, but
`/sys/module/fuse/parameters/enable_uring` is off and only root can turn it
on (`options fuse enable_uring=1` in modprobe.d). fuser 0.18 defines the
`FUSE_OVER_IO_URING` init flag but has no ring transport: no
`FUSE_IO_URING_CMD_REGISTER` / commit-and-fetch loop. Adopting it means
writing that session layer (io_uring `URING_CMD` on `/dev/fuse`, one queue
per core, fixed buffers sized to `max_write`) under fuser's request
decoding: roughly 1–2k lines plus fallback to the read/write channel.

**Decision.** Deferred. The cheaper experiment tests the same hypothesis
first: `sudo cpupower idle-set -D 100` on one Spark and rerun the FUSE
single-stream benchmark (root, reversible with `idle-set -E`). If
latency drops toward raptor's (about 0.1 ms per request), core-affine
queues are worth building. Do it as a sparknest-owned transport behind a
config switch, keeping the /dev/fuse read path as the fallback. If latency
does not move, the bottleneck is elsewhere and io_uring is not the lever.
Sealed files already bypass the daemon via passthrough (ADR-014), so this
matters only for unsealed files and metadata-heavy work.

## ADR-024 — A data reserve protects metadata from a full disk (2026-09-27)

The metadata database and the Raft log live on the same filesystem as the
node's objects. A disk filled with object data would stop the node from
appending to its log; on the leader that stalls every metadata operation in
the cluster. So object data must leave `node.data_reserve_gib` (default 4)
free: owner writes (including the FUSE fast path) and incoming replicas get
`ENOSPC` past it, while namespace operations, deletes and evictions, which
free space, keep working. The store samples statvfs at most every 250 ms
and subtracts writes in between. Archive stores have no reserve.

## ADR-025 — FUSE over io_uring, built (2026-09-27; supersedes ADR-023's deferral)

The user enabled `fuse.enable_uring=1` on every host (kernel command line;
FUSE is built into these kernels, so modprobe options do not apply).
sparknest now negotiates `FUSE_OVER_IO_URING` at INIT whenever the kernel
offers it (`[fuse] io_uring = true`, the default) and runs one queue thread
per possible CPU, pinned, each with its own ring and two entries. It warns
and stays on /dev/fuse when the parameter is off.

- **fuser is vendored** (`vendor/fuser`, `SPARKNEST.md`) with a small patch:
  a reply target on `ChannelSender` and a `RingDispatcher`, so ring requests
  use fuser's decoding and our `Filesystem` unchanged. FORGET, INTERRUPT and
  notifications stay on /dev/fuse, as the kernel requires.
- **Every ring command is issued by its queue's thread.** Replies made on
  other threads are handed over through an eventfd, so completions never run
  as task work on tokio threads.
- **READ is answered in place** when the local fast path can serve it: the
  bytes are read straight into the entry's payload buffer. Without this,
  the extra copy cost 15–20% on parallel reads.
- `unsafe` outside `nest-fabric` is confined to `crates/nest-fuse/src/uring.rs`
  (SQE layout, buffers shared with the kernel, eventfd), each block with a
  safety comment. Buffers the kernel may still reference are never freed.

Measured on ostrich (benchmarks/M8-FUSE-IO-URING.md): 4 KiB reads 58–179 µs
→ ~20 µs, 128 KiB reads 0.4 → 2.3–3.4 GB/s, 1 MiB reads 1.5–2.2 → 3.1–3.3
GB/s, writes +55%, parallel reads level.

## ADR-026 — Relaxed metadata durability with automatic recovery (2026-09-27)

**Context.** A metadata commit waits for about three fsyncs in a row
(benchmarks/M8-METADATA-LATENCY.md); the software path is 0.1–0.4 ms. The
user prefers optimistic durability with automatic recovery: full power
outages are rare, and file data is what matters.

**Decisions.**

1. **openraft 0.10** (pinned alpha): the leader replicates entries once they
   are submitted, not flushed, and consecutive appends merge into one write.
   The commit index is stored without fsync (openraft allows it to lag).
2. **Raft log and metadata run SQLite WAL with synchronous=NORMAL**, with
   automatic checkpoints off; our own thread checkpoints about once a second
   when changes are pending, with a size cap as a backstop. That interval is
   the metadata loss window for a full outage. **Votes are always fsynced**:
   a host that forgot its vote could vote twice in a term.
3. **File data stays durable.** Replica, recall, offload and import copies
   are fsynced before they are recorded (as before). Finalize `fdatasync`s
   the owner's object before recording it stable, so a stable file is
   durable on every holder's disk. An application's `fsync` becomes an object
   `fsync` (its `fdatasync` an `fdatasync`) followed by a metadata barrier:
   the Raft log flushed on a majority up to the latest entry. `fsync` on a
   directory takes the barrier too.
4. **Dirty detection.** Each host records the kernel boot id and a
   clean-shutdown marker. A host whose OS crashed while the daemon ran is
   dirty. A dirty minority simply rejoins (the log repairs it). A dirty
   majority re-founds automatically from the most advanced metadata among a
   majority, after a short grace period for more hosts, then runs fsck.
5. **fsck** (a library used by the daemon's pre-mount recovery phase and by
   `nest fsck`, online through the daemon or offline) compares each host's
   objects with the metadata. With a healthy quorum everything resolves
   automatically: a damaged or missing copy is retired and re-copied from
   another holder; a file with no surviving copy is reported lost. An
   object the metadata does not know (metadata rolled back) is never
   deleted: it moves to `/.lost+found/<host>/`. Files mid-write on a crashed
   owner keep the size found on disk. Resolutions that would destroy data
   with no quorum to decide fail the mount until `nest fsck` (`-y` applies
   the standard non-destructive choices). Hugging Face blobs can be checked
   deeply against the SHA-256 in their names.
6. **Automatic upgrade and re-found on restart.** The Raft log is disposable
   across format changes; only our metadata schema migrates. Hosts exchange
   binary and format versions before mounting. A new format is adopted only
   when a majority of configured members run it; a host on the minority
   version waits unmounted and says why. So an accidental upgrade of a
   minority never takes the cluster. Lossless after a clean shutdown (a
   small decoder reads the old log's committed tail); after a crash it is
   the dirty path. `nest upgrade prepare` remains the recommended manual
   route. Tested with simulated in-process clusters, not real installs.
7. **Batch delete** in `nest`: many unlinks per Raft entry, in chunks.

## ADR-027 — Recovery as built (2026-09-27; implements ADR-026)

- **HELLO** (RPC service 6) runs from process start: build, Raft format,
  phase (recovering: dirty, disk format, applied position; running: leader),
  incarnation, and the plan and seed a host holds. Start-up decides *normal*,
  *join* (set Raft state aside under `pre-join-*`, adopt the running
  cluster's incarnation, catch up from a snapshot) or *re-found*.
- **Plans** are proposed by the lowest-id recovering host once a majority of
  voters recover (immediately if all do, else after a grace period, 10 s),
  accepted by each host at most once, and committed only with a majority of
  acceptances; an uncommitted acceptance expires after a minute.
- **Seeds**: the most advanced metadata (applied term, index; lowest id on
  ties), Raft rows cleared, marked with the new incarnation, SHA-256
  checked on every participant. Old state is kept under `pre-refound-<id>`.
- **Founding**: the seed host initializes the group as its only voter,
  snapshots and purges, then adds the other voters, so no host ever
  replays a log that lacks the seeded state.
- **Incarnations**: every Raft message carries one; a mismatch is refused.
  A clean host that returns to a re-founded cluster joins afresh.
- **fsck** runs on every host that did not trust its local state: after
  catching up, before admission (nothing is served until then). Unknown
  objects are deleted only when the host caught up with the incarnation it
  last reconciled; otherwise they go to `/.lost+found/<run>/<host>/<path>`,
  with paths recovered from set-aside metadata. Snapshot-install
  reconciliation stands down while a host recovers.
- **No mount-blocking decisions**: every resolution fsck applies keeps the
  bytes (lost+found) and records what happened, so none needs a person to
  choose before mounting; `nest fsck` lists anything a person may want to
  act on (e.g. an unknown object newer than the cluster's version of a
  file).
- **Tests** simulate all of it in process: `TestCluster::power_loss` rolls a
  host's disk back to a saved durable point and changes its boot id;
  `tests/recovery.rs` covers a full outage with uneven loss, a lossless
  whole-cluster upgrade, a lone upgraded host (waits, then joins) and a host
  absent during a re-found.

## ADR-028 — Per-host usage statistics, and goal-oriented plans (2026-09-27)

**Context.** Free-space plans removed copies by count and size alone, with
no idea which copies anyone used. The user wants plans that tidy what has
not been used for a day, a week or a month, and plans that add copies where
hosts keep pulling a file over the network.

**Decision.**
- Every host records, per file and per UTC day: opens, last open time, and
  bytes read from its own store, from other hosts, and from an archive it
  gateways. Rows live in `usage.sqlite` in the state directory, **not** in
  the replicated metadata, and are pruned after 30 days.
- Readers count bytes on their open handle (one relaxed atomic add, cheap
  enough for the io_uring fast path); the VFS folds handle counters into a
  pending batch every 5 s and on close, and writes it on a blocking thread.
  Passthrough reads (sealed, local) are served by the kernel and are not
  counted; their opens are.
- Keyed by **file id**, not path: renames keep their history, and a file
  deleted and re-created under the same name is new content (Hugging Face
  re-downloads land as new blobs) and starts afresh.
- The leader asks every host for its summary (`AdminReq::Usage`) when it
  plans. A host that does not answer is *unknown*, never *idle*: tidy leaves
  its copies alone.
- Plans have goals: **free** (desired free space; redundant copies least
  recently opened on that host first, then offload into chosen archives),
  **tidy** (copies not opened on their host within the window and not
  written within it, oldest use first, down to one copy anywhere; sole
  copies move to a chosen archive or stay) and **speedup** (copy files onto
  hosts that read them over the network at least a threshold within the
  window, most-read first, keeping a free-space floor). Every copy in a plan
  says why it is there.

**Alternatives.** A column on the metadata row: every open would be a Raft
commit on every host, for data only one host cares about. A path-keyed
table: survives delete-and-recreate, but loses history on rename and would
credit old usage to new content.

**Consequences.** Usage restarts from nothing when a host's state
directory is wiped, and passthrough-heavy workloads show opens but few
bytes. Plans remain proposals: reading never creates a replica (invariant
2); a person applies a speedup plan.

## ADR-029 — The web UI talks to its node over one WebSocket (2026-09-27)

**Context.** The UI polled several endpoints every two seconds; later it
held a Server-Sent Events stream. Over plain HTTP/1.1 a browser allows six
connections per server across all its tabs. Unanswered polls, then a few
tabs each holding an event stream, used them up, and every later request
queued until those tabs closed: the page animated but nothing worked.

**Decision.** Each page opens one WebSocket (`/v1/ws`, token in the query)
and does everything over it:
- API calls as `{type: "call", id, method, path, body}` frames, run through
  the same axum router as the HTTP endpoints (so handlers exist once) and
  answered as `reply` frames; calls run concurrently and time out on the
  page;
- pushed `state` (status, jobs, archive stores, groups, computed once per
  node while any page is connected), debounced `changed` notices when the
  namespace or placement changes (views refetch only then), and `lines`
  while the page follows logs.

Plain HTTP remains only for the page itself, file uploads and downloads
(streamed bodies), and the CLI over the local Unix socket. There is no
polling fallback: calls made while the socket reconnects wait for it.

**Alternatives.** Polling (the failure above); SSE (holds one of the six
connections per tab); HTTP/2 (needs TLS in browsers, so certificates on
every node).

**Consequences.** WebSockets do not count against the per-server
connection limit, so tabs no longer starve each other. The HTTP endpoints
stay for the CLI and scripts.

## ADR-030 — Reads spread over every copy: local first, rendezvous stripes, capped (2026-09-27)

**Context.** A model with a copy on every host was read only from the
local disk (passthrough or the daemon's pread). The Sparks' disks read
~5 GB/s against 23 GiB/s of RDMA links; raptor's links carry 93 GiB/s.
When all hosts boot the same model at once (ds41rt), every copy is being
read anyway, so their page caches can serve each other.

**Decision.**
- Passthrough policy `always | sole | never` (`[fuse] passthrough`): `sole`
  uses passthrough only where this host holds the only live copy, so the
  daemon can spread reads of files with several.
- A STABLE file with several copies is read through readahead in 4 MiB
  chunks, each from a source the node's balancer picks: this host's disk
  first while its latency is within 2× the best; otherwise peers ranked by
  rendezvous hash of (file, 32 MiB stripe), so hosts loading the same
  shards ask the same peer for the same stripe and hit its page cache.
- The peers used are capped to a prefix whose measured disk rates sum to
  125% of this host's link rate. Disk rates are measured with O_DIRECT
  after boot and daily while idle (`disk-bandwidth.json`); link rates come
  from sysfs. `nest io` and the I/O page show what each host learned.
- Speculative fabric chunks take landing slots only while slots are
  spare, and remote chunks are copied out of their slot on arrival, so a
  demanded read always gets a slot (invariant 9). Fabric reads are
  cancellation-safe: the pending table owns a read's slot until the peer
  answers.
- RoCE is lossy on the current switch: a 17 ms QP retransmit timeout (was
  67 ms) and at most 16 fabric reads in flight per host bound the cost of
  incast drops until PFC/ECN is configured.

**Measured** (cold, 164 GiB EXL3 model on all 7 hosts, 8 readers each): a
Spark alone 3.3 → 6.2–6.5 GB/s; all seven at once 21.9 → 39.6 GB/s
aggregate (each Spark ~54 s → ~29 s); raptor alone at 16 readers 8.9 →
14.5 GB/s. See benchmarks/.

**Consequences.** Reading never creates a replica; everything held is
transient. The in-flight cap is one number for hosts whose links differ
4×; it should scale with the reader's link (to be benchmarked).

## ADR-031 — Files read in scattered pieces are read directly, and the file remembers (2026-09-27)

**Context.** ds41rt maps engram tables (hundreds of GB of hash tables)
with MADV_RANDOM: every page fault is one 4 KiB FUSE read at an unrelated
offset. ADR-030's readahead fetched a 4 MiB chunk for each, from the disk
or a peer, and discarded almost all of it; a loader that opens a shard
per tensor did the same with the unread tail of each window. On raptor
the readahead read 2.3 TiB to deliver a few hundred GiB, and the first
request stalled behind it. FUSE never sees madvise or fadvise.

**Decision.**
- Every chunk readahead drops reports how much readers consumed. Per file
  on each host, once 64 MiB of dropped chunks were less than 25% consumed
  (a decaying tally, so a stream's unread tail does not count alone), the
  file's reads are *scattered*.
- Scattered files are read exactly as asked: from this host's copy when it
  has one (on the FUSE fast path), otherwise from whichever copy is least
  busy, over every copy (no stripes, no cap). New opens get passthrough
  when the policy is `sole` and a local copy exists.
- The verdict is recorded with the file (`Command::SetReadPattern`, a bit
  in `files.flags`) and seeds every host at open, so it is learned once
  per cluster and survives restarts. Hosts reading through the daemon keep
  judging: if 90% of 256 MiB of direct reads continue each other, the file
  streams again and the flag is cleared. A host using passthrough cannot
  see its reads and does not revise it. `nest read-pattern PATH
  scattered|stream [-r]` overrides.

**Alternatives.** Smaller chunks for everyone (hurts streams); passing the
application's advice (FUSE has no channel for it); per-host memory only
(every host and restart relearns, during the load that hurts most).

**Consequences.** One metadata write per file when first judged. The
kernel's own read-around still applies to mapped files opened without
MADV_RANDOM; only passthrough avoids FUSE for those entirely.

## ADR-032 — Fabric reads: size tiers, a byte budget, small reads served inline (2026-09-27)

**Context.** Engram rows over the network (ADR-031, a model's lookup
tables read from other hosts) came back in ~650 µs for 4 KiB. The serving
host spent ~400 µs per request: a task, a blocking-pool hop (each a thread
wake from the Sparks' deep idle states), two metadata queries, a stat and
an open per request. The reader waited ~400 µs more for one of 16 reads in
flight, a count sized for 4 MiB chunks. Every read, however small, held a
4 MiB slot at each end, and a lane allowed 32 outstanding requests. The
reply's immediate packed slot (9 bits) and length (23 bits), so slots were
capped at 512 per device. raptor's link rate was also counted twice (one
400 Gb/s port carrying two subnets is two rails, one link).

**Decision.**
- Landing and staging slots come in tiers per device: 4 KiB (2048 / 1024),
  128 KiB (512 / 256) and the 4 MiB chunk (128 / 64). A read takes the
  smallest tier it fits, or a larger free one. The small tiers add
  ~108 MiB registered per device.
- The immediate carries the whole slot id; the length is the completion's
  byte count. Hosts pair only when they speak the same read protocol (a new
  `ConnectV { protocol }` request, protocol 2); during a rolling upgrade
  mismatched hosts fall back to TCP rather than misread replies.
- The incast cap counts bytes, in 4 KiB units: by default 2 ms of the
  host's link rate, at least 64 MiB (`[fabric] inflight_mib`; the old
  `max_inflight` is read as that many 4 MiB chunks). The per-lane window
  is 128 (was 32); the completion queue holds 65536 entries.
- A server answers reads up to 128 KiB on the fabric's completion thread
  when the object is already open (`ReadSource::try_read_now`): one pread
  into a staging slot and the RDMA write, no hops. Served objects of STABLE
  generations stay open, fully re-checked every 2 s and against the lease
  and fences on every read; other reads take the async path, which opens
  them.
- Link rate counts each device port once.

**Consequences.** Rows and page faults no longer compete with bulk reads
for slots or the in-flight cap. A cold small read can stall a completion
thread for about one disk read (~0.1–0.2 ms); if that shows under load,
serve page-cache hits inline (RWF_NOWAIT) and send misses to the pool.
Tier sizes and the budget are starting points, to be tuned by benchmark.

## ADR-033 — Scattered files return to readahead only for long runs (2026-09-27)

**Context.** ADR-031 switched a scattered file back to readahead when 90%
of 256 MiB of direct reads continued the previous one. A loader that opens
a shard per tensor reads each tensor front to back, so the weight shards
looked like streams, went back to readahead, wasted it past every short
tensor's end, and were judged scattered again: files flipped several times
a minute, each flip a metadata write and a burst of 75–97% waste.

**Decision.** Judge by run length instead: a handle's sequential run of
direct reads ends at a read elsewhere or at close; a file returns to
readahead only when its runs (at least 4, 256 MiB in all) average 64 MiB
or more, well past readahead's window. Short sequential runs stay direct,
where the kernel's own readahead requests (up to 128 KiB) pipeline them
without waste.

## ADR-034 — One serve path through io_uring; readers never wait on a thread; scattered reads keep cache affinity (2026-09-27)

**Context.** Serving small reads inline (ADR-032) did each cold disk read
on the completion thread, one at a time per device: under load (a model
loading from other hosts) 150–330 µs disk reads became ~730 µs round
trips, while the drive could have run dozens at once. On the reader, a FUSE
thread that waited for its row (to save thread hops) blocked the other page
faults queued on its CPU's ring: one reader got faster, Fable's concurrent
faults got slower (53.7 → 34.1). Scattered reads went to the least busy
holder, so no holder kept any part of a table cached. And a holder whose
latency was measured high once was never tried, so never re-measured (dodo
sat unused at 36 ms).

**Decision.**
- Serving has one path: the object (open already, or checked off-thread
  once and reopened every 2 s), a queue until a staging slot is free, an
  io_uring read into the slot submitted from the device's completion
  thread, and the RDMA write when the read completes, on that same thread.
  No hand-offs, and the drive sees every read in flight. Short reads are
  continued, never taken for end of file. The poller sleeps on both the
  RDMA completion channel and an eventfd registered with the ring.
- A reader of a scattered file with no local copy reserves a lane, budget,
  a landing slot and window room without waiting (`Fabric::reserve`),
  sends, and returns; the completion thread runs its callback, which
  replies to FUSE. Reads that fail are retried through the ordinary path;
  unanswered ones fail after 30 s like any other.
- Scattered reads choose their holder by rendezvous hash of (file, 1 MiB
  region), falling to the next only when it is clearly busier, so each
  holder's page cache keeps its own part of a table.
- A source's latency fades back toward its prior while it is idle, so a
  slow sample cannot bench it for good.

**Consequences.** One serve path to reason about and measure; the async
serve and inline reads are gone. The fabric tests serve a real file.
Readahead's 4 MiB chunks still use the awaiting `read`.

## ADR-035 — Use only the RDMA functions a port needs; completion threads stay awake while hot (2026-09-27)

**Context.** A Spark's one cable reaches two PCIe functions (10.55.0.x and
10.55.1.x), each capped by its PCIe x4 link at ~115 Gb/s; both report the
port's rate. At 200 Gb both are needed (the dual-rail finding); at the
current 100 Gb one carries the port, and the second only costs a completion
thread, and its rate was counted twice (200 Gb), inflating the in-flight
budget and the source cap, as raptor's two subnets on one 400 Gb port had.
Separately, a lone reader's 4 KiB row took ~790 µs against ~150 µs when the
completion threads were busy: each request woke a sleeping thread on a
deep-idle core, several times per read.

**Decision.**
- Rails are grouped by physical port (same NIC switch id and PCI function
  number). Per port, the fewest functions (lowest subnet first, the same
  choice on every host) whose PCIe capacity (lanes × GT/s × 0.9) covers the
  port rate are used; the rest are not opened. A host's link rate counts
  each port once. An explicit `[fabric] devices` filter overrides this.
- A completion thread keeps spinning while it has reads outstanding and for
  10 ms after its last work, then sleeps as before. At most one core per
  used function spins, only while reads keep coming.

**Measured.** Lone reader, cold 4 KiB rows from other hosts: 790 → 127 µs
median (p99 299 µs); 32 readers: ~70k rows/s, median ~410 µs.

## ADR-036 — Spread imports: a cache larger than one host goes to whoever has room (2026-09-27)

**Context.** raptor's `/mnt/scratch` holds a 5.9 TB Hugging Face cache on a
drive about to be reformatted; raptor's store has ~2 TB free and the
Sparks ~3.3 TB each. Importing means copying (another filesystem), and the
copies cannot all stay on raptor.

**Decision.** `nest hf import SRC --spread`: blobs are copied into this
host's store (on its NVMe; never staged on the source drive or in /tmp) in
batches of ~64 GiB. Each batch is handed out while the next copies: each
file, largest first, goes to the host with the most free space after what
the batch already gave it (every host keeps 64 GiB or 5% free), that host
pulls it over the fabric (`StartReplicate`), and this host's copy is
removed once the target's is live (never the last copy: a failed transfer
leaves the file here). Files this host wins stay. Blobs the cluster
already has are skipped (adopting across filesystems is not possible, and
not needed). The source is only read, once.

**Consequences.** At most two batches are staged on the importing host.
The import runs at the source drive's read rate. Snapshots, refs and
verification are unchanged; placement afterwards is the placer's (rules,
plans).

## ADR-037 — Models are deleted by hf, on sparknest's hub (2026-09-27)

**Context.** Deleting a model's directory from the Files page removes paths,
but in huggingface_hub's current layout a repo's blobs are links into the
hub's shared `blobs/` directory, which other repos may use; only hf knows
which shared blobs are still referenced.

**Decision.** Models are deleted with `hf cache rm` run by the daemon on its
own mount's hub (`--cache-dir <mount>/hf-home/hub`, `HF_HOME` set to the
mount's hf-home), so every host's and archive's copy goes with the files.
The UI's model detail and `nest hf rm REPO... [--dry-run]` preview with hf's
dry run, then delete as a job. The daemon looks for `hf` on PATH, then in
Homebrew's and `~/.local/bin` (its system unit has a minimal PATH). Deleting
paths from the Files page stays as it is.

## ADR-038 — Downloads from the Hub, by hf, on the chosen host (2026-09-27)

**Context.** Models were only brought in by importing caches. Adding one
meant running hf by hand on some host with the right environment, and a
repo no host held completely (an unfinished download, files spread over
hosts) had no one-step fix.

**Decision.** `nest hf download REPO [--host H]` and the Models page's
Download dialog (search through `hf models ls` / `hf datasets ls`, or a
pasted id; size from `hf download --dry-run`; host defaulting to the most
free space) start a job on the chosen host that first copies there what
the cluster already has of the repo, then runs `hf download` into the hub
through that host's mount, so new files are written there. A repo no host
holds completely gets a Finish button that does the same on the host
holding most of it. hf runs with the hub as its cache and HF_HOME, and the
user's `~/.cache/huggingface/token` as HF_TOKEN_PATH when present.
Progress is what the host holds of the repo against hf's size.

## ADR-039 — Transfers into archive stores are paced (2026-09-28)

**Context.** Offloading a 400 GB model to the NAS archive (an SMB share on
raptor) made raptor crawl, with the kernel's CIFS flush thread at 100%.
The ext4 → CIFS copy is buffered (`copy_file_range` refuses to cross
filesystem types), nothing waited for the share, and on a host with 183 GB
of RAM the kernel lets tens of GB of dirty pages build up before
throttling, then flushes them through one SMB connection while everything
else waits for memory. The source was read through the page cache too,
evicting what raptor had cached.

**Decision.** Archive and backup stores are opened paced
(`ObjectStore::paced`, 64 MiB): `Staging::write_all_at` flushes the file
(`fdatasync`) every 64 MiB written, with writers waiting meanwhile, and
drops its pages (`POSIX_FADV_DONTNEED`), so a transfer never holds more
than ~64 MiB unflushed. The buffered copy also drops the source's pages as
it goes. Live stores on local NVMe are not paced.

**Alternatives.** Mounting the share with `cache=none` (every client of the
share loses its cache), or a lower global `vm.dirty_bytes` / a per-device
`strict_limit` (host-wide knobs, root; still useful as belt and braces).

## ADR-040 — Live stores are paced too, with a longer step (2026-09-28)

**Context.** ADR-039 paced archive stores only. Transfers into a host's own
store (replication, copies to `@all`, a spread import's staging copies)
were left to the kernel, but a Spark receives over the fabric (up to
11.6 GB/s) faster than its SSD writes, and with 128 GB of RAM tens of GB of
dirty pages build up before throttling, evicting the page cache loaders
depend on.

**Decision.** The live store is paced at 256 MiB (flush, drop the pages;
NVMe flushes are cheap), and the import's buffered copy writes through the
paced writer and drops its source's pages as it reads.

## ADR-041 — Transfers never park threads by the dozen; the apply path never waits on an archive disk (2026-09-28)

**Context.** With pacing (ADR-039/040), queued offloads into the scratch
disk (~150 MB/s) pinned raptor's Tokio blocking pool at its 512-thread cap:
each transfer wrote its 16 in-flight chunks from 16 blocking threads that
waited on the same file and a slow disk. Work needing a blocking thread
queued behind them, and the metadata apply, which checked the archive's
marker file on the saturated disk, fell behind the cluster; raptor lost its
session over and over (not serving, reads flapping off the fabric).

**Decision.**
- A transfer writes through one writer thread fed by a bounded channel.
- At most 2 transfers write into one archive store at a time; the rest
  wait as tasks, holding no thread.
- Hot paths (the apply, reads) use an archive's health as checked within
  the last 5 s; explicit checks (`nest store ls`) read the marker afresh.

**Next.** Completion-driven transfer writes through io_uring (`RWF_DSYNC`,
from the fabric's landing slots, a fixed number in flight per transfer and
per device) replace the writer and flusher threads.

## ADR-042 — Transfers are written with direct I/O through io_uring (2026-09-28)

**Context.** ADRs 039–041 paced transfer writes through the page cache
(flushes every window, a flusher thread, then a writer thread per
transfer): threads still waited on the disk, pacing needed syncs, and a
local copy to another filesystem used `copy_file_range`, which quietly
became an unpaced buffered copy and flooded the page cache (the scratch
disk offloads).

**Decision.** Transfers into a store write 4 KiB-aligned buffers to the
object's staging file opened with `O_DIRECT`, submitted to one io_uring
per process (`nest-store/src/uring.rs`). A direct write completes when the
device (or the SMB server) has the data, so each transfer keeps a fixed
number of writes in flight (`ObjectStore::window`: 16 for the live store
on NVMe, 4 for archives) and waits only for its oldest completion. No page
cache, no per-window syncs, no threads parked. Short writes are continued;
the last write is padded to the alignment and the file trimmed at commit,
which still syncs once to make the object durable. Filesystems refusing
`O_DIRECT` fall back to buffered writes; without io_uring, writes run on
the caller's thread. The fabric copy, the local copy to archives (no longer
`copy_file_range`) and import copies all use it. `unsafe` is allowed in
`nest-store/src/uring.rs` (aligned buffers, ring submissions), as for the
fabric's verbs layer and the FUSE io_uring transport.

**Measured.** raptor NVMe, 8 GiB: 5.7–6.0 GB/s at windows 4–16 (the
device's sustained rate), against 2.9 GB/s with inline flushing; the
buffered flusher's 7.1 GB/s on a 4 GiB file was flattered by the page
cache. The scratch disk is to be measured when idle.

## ADR-043 — Placing a selection by replica factor; plans are edited before they apply; Finish only downloads (2026-09-28)

**Context.** Moving a model meant separate one-shot actions ("copy to every
host", "move to 1", "move to…", offload), none of which could spread a
model evenly or keep, say, two copies of each file over four hosts. Plans
could only be applied whole. "Finish" on a partly downloaded model first
copied everything the cluster had onto one host, moving data the user never
asked to move.

**Decision.**
- A **place** goal: a selection, target hosts, and a replica factor from
  1/hosts to 1. Each file goes on `round(factor × hosts)` of them (at least
  one). Files are assigned largest first. Hosts still below their even
  share of the bytes come first; among those, the hosts that read that file
  most in the last 30 days win, then hosts already holding it (no
  transfer), then the emptiest. A host without the file must keep
  max(64 GiB, 5%) free after taking it. Copies outside the assignment are
  removed only once a host the file is assigned to (its keeper, per copy)
  holds it. Rule-required copies stay. One host with factor 1 is the old
  consolidate. The UI's Place dialog offers multi host (hosts and factor),
  single host (the host that reads it most, then holds most, preselected)
  and archive (offload, or copy while keeping the hosts' copies).
- **Plans are edited before they apply.** Every copy in a proposed plan can
  be skipped or taken back by host, model (org/name), directory, file or
  file id (`POST /v1/plans/{id}/select`, `nest plan skip|unskip`). Copies
  carry the model and snapshot name they belong to, for people. Apply runs
  what is left. The additions run on every host at once, then the
  removals. A removal whose keeper was skipped finds no copy there and is
  kept.
- **Finish downloads only what no host has.** `hf download --dry-run`
  against the hub cache (sizes of "-" are cached files) says what is
  missing cluster-wide. Finish appears only then and runs `hf download` on
  the chosen host, which writes the missing files there and moves nothing.
  With no revision given, downloads and dry runs use the revision the cache
  holds (`refs/main`), so finishing never pulls a newer snapshot.

**Consequences.** The factor makes "spread" and "everywhere" two ends of
one control. Placement may take several passes when hosts lack room, and
the plan says how many files fell short. Plans stay in memory on the node
that made them (as before).

## ADR-044 — One I/O mode and one backing file per open inode; passthrough opens carry no other flags (2026-09-28)

**Context.** Every open of a sealed file whose only copy was local failed
with EIO on its own host: 30–99% of each host's sole copies (on kiwi, the
drafter checkpoint a four-Spark launch needed). The FUSE frontend answered
passthrough opens with FOPEN_KEEP_CACHE, which the kernel refuses beside
FOPEN_PASSTHROUGH (fs/fuse/iomode.c accepts only direct I/O, parallel direct
writes and no-flush there), and registered a new backing file for every
open, while the kernel keeps one I/O mode per inode: every concurrent open
must name the same backing file, and an inode open through the page cache
refuses passthrough (and the reverse). Any refusal is EIO for the caller.
The bug dates from M2; it surfaced once many hub files were sealed with a
single copy. FUSE over io_uring is not involved (the same with it off).

**Decision.** The frontend keeps, per inode with open handles, the backing
file its passthrough opens share (or none) and a handle count. The first
open decides: passthrough when the VFS offers the local file, else through
the daemon. Later opens follow it until the last handle is released,
whatever the VFS would choose now (a new copy elsewhere, a scattered read
pattern). A writer opening a file that is passed through gets ETXTBSY (only
sealed files are). Passthrough answers carry no flags. Unit tests pin both
rules against the kernel's mask.

**Verified.** rhea, with io_uring: one open, four concurrent opens (same
bytes), mmap and a reopen of a sealed 8 MiB file; all 521 of rhea's sole
copies open (477 failed before).

## ADR-045 — Hugging Face downloads are planned, spread over hosts and fetched one file at a time (2026-10-01)

**Context.** `nest hf download` ran the `hf` command on one host. Progress
counted only finished files (a 5 GB shard showed nothing until done), every
file landed on that host, and hf fetched 8 files at once, which floods the
uplink (hf_xet already uses several connections per file).

**Decision.**
- A small Python driver, `sparknest-hf-fetch` (tools/hf-fetch, installed
  in libexec/sparknest), runs with the `hf` command's own interpreter, so
  it uses the same huggingface_hub and hf_xet. `plan` resolves the
  revision to a commit and lists the files with sizes; `fetch` downloads
  given files at that commit one after another and reports bytes as JSON
  lines (a tqdm subclass handed to `hf_hub_download`).
- The coordinator (the node taking the request) plans once, skips files
  the hub already has at that commit, and assigns the rest largest first
  to the least loaded chosen host with room (free space above max(64 GiB,
  5%); a host named alone needs only the space the files take). A file is
  fetched by the host it is assigned to, through that host's mount, so its
  first copy lands there: placement needs no hint in the filesystem.
- At most `in_flight` files download at once across the cluster (default
  1; benchmarks/M9-HF-DOWNLOAD.md). A host's small files (< 64 MiB) go as
  one batch. A failed file is retried on its host, then on another; a host
  that stops answering for a minute loses its files to others.
- One job with byte progress per host; per-host fetches are not jobs
  (`HfFetch` / `HfFetchStatus` admin requests, appended). Cancelling kills
  the drivers; files already fetched stay, and running it again fetches
  only what is missing. The branch ref (`refs/main`) is written to the
  planned commit; without `--revision` a repo the hub has is finished at
  its cached commit.
- Default: spread over every serving host. `--host H` puts every file on
  H; `--hosts a,b` limits the spread. The web UI's Download dialog and
  Finish offer the same.

**Verified.** Several hosts fetching different files of one repo at once
through the running cluster: each file on the host that fetched it,
contents matching their SHA-256 on other hosts. A one-node trial: byte
progress, cancel, resume fetching only the missing files, `refs/main`.

## ADR-046 — Downloads: four files in flight; stalled fetches and failing hosts hand their files on (2026-10-03)

**Context.** ADR-045 kept one file in flight across the cluster. In real
use that left the uplink underused (the one-file test was on raptor; four
in flight downloads faster in practice). And when emu lost its IPv4
address, each of its fetches hung about six minutes in DNS and then failed:
with one file in flight the whole download stalled behind it (the first
attempt sat at 0 until cancelled), and each of emu's files was tried twice
there before moving.

**Decision.** Four files in flight by default (`--in-flight`). A fetch
with no new bytes for 90 s before its first byte, or 120 s after, is
cancelled and counts as a failure. A host whose fetches fail twice in a
row is dropped for the rest of the download: it gets no new files and its
queued ones move at once to the least loaded good host with room; the job
says so. A file is retried once on its host, then on another, and given up
after three tries. The scheduling (`hfimport::Schedule`) is plain code
with unit tests.

## ADR-047 — Off-grid hosts: learners on the LAN that dial out (2026-10-12)

**Context.** New hosts join that are not on the RDMA fabric: kakapo (a
Spark) and later another, and oliver (WSL2 on a Windows PC, behind WSL's
NAT; mirrored networking drops connections there, so it stays on NAT). All
are on 10 GbE and always on. They must use every model on the cluster,
keep full copies of the models they use for local speed, and write and own
files, but not slow consensus down with TCP round trips. Seven fabric
hosts already vote.

**Decision.**
1. **Role.** An off-grid host is a Raft learner (`voter = false`) marked
   `role = "offgrid"` in membership. It applies the whole log into its own
   metadata, so lookups are local, and forwards proposals to the leader
   like any follower. It may own and create files. Quorum, elections and
   recovery count voters only, as today.
2. **It dials out.** Core hosts cannot reach a host behind NAT, so an
   off-grid host keeps a control connection open to every member it has an
   address for, and every control connection carries requests both ways:
   a node with no address for a peer sends its requests (Raft appends,
   fences, replication, data calls) over the connection that peer opened.
   Core hosts additionally listen on the LAN (`listen = 0.0.0.0:7400`); an
   off-grid host lists the core hosts at their LAN addresses in its own
   `node.toml`, and addresses learned from Raft membership (fabric IPs)
   never replace an address given there. No inbound port is needed on an
   off-grid host.
3. **Fencing is unchanged: every member.** Narrowing it to the holders of
   a file is not safe: a host that read the file before (remotely, sealed)
   has its pages in the kernel page cache, which only the fence drops
   synchronously. With hosts always on, the fence costs one parallel LAN
   round trip; the lease wait only applies when a member does not answer.
4. **Reads are local first.** An off-grid host reads its own copy whenever
   it has one, regardless of how many other copies exist, and otherwise
   reads over TCP from one core holder (never striped). Its copies are full
   replicas under the usual generation rules.
5. **Placement.** Place spreads only over fabric hosts and never counts
   or removes off-grid copies. An Off-grid goal picks which off-grid hosts
   hold a selection; offload accepts an off-grid host as its destination;
   a copy there is a copy, so unloading from the fabric hosts is not a
   deletion.
6. **Not now.** No encryption (public data, trusted LAN, performance
   first); no reads while disconnected; onboarding is explicit (plans and
   `nest import`), informed by usage, never triggered by reads (invariant 2).

The TCP bulk data path and the planner and UI work follow in their own
ADRs.

## ADR-048 — TCP lanes: the bulk path for off-grid hosts (2026-10-12)

**Context.** Off-grid hosts (ADR-047) read and copy whole models over
10GbE. Before this, a host without RDMA read one request at a time over its
control connection, which also carries Raft heartbeats and lease renewals:
a large transfer could delay a lease. Measurements (benchmarks/M10-LAN-TCP.md):
one TCP stream does not fill the link into WSL2 (4–5.7 Gb/s) and leaves
headroom on kakapo; four streams with eight 4 MiB chunks in flight reach
9.2–9.6 Gb/s on both; slow start after idle costs 20–40% of an idle
connection's first chunk.

**Decision.**
1. **Lanes.** Each off-grid host keeps `node.lanes` (default 4) TCP
   connections to every member it knows, dialled by it (it may sit behind
   NAT) at `listen` port + 2 (`lanes_listen`). Lanes are authenticated with
   the cluster secret and carry range requests both ways, so fabric hosts
   read from an off-grid host over the lanes it opened.
2. **Serving.** Answers are a header sent with MSG_MORE followed by
   `sendfile` from the object: no user-space copy on the serving host.
   Objects open through the same `ReadSource` as RDMA reads, so serving
   checks and fence guards are shared; a guard is held until the bytes are
   on the wire. TCP_NODELAY on every lane; socket buffers left to
   autotuning; `tcp_slow_start_after_idle=0` on every host.
3. **Use.** Readahead and replication fetch whole chunks through one
   interface (`Bulk`) with two transports: RDMA between fabric hosts, lanes
   when either end is off-grid. Reads in flight are capped in bytes
   (64 MiB) as on the fabric. Without a lane the old one-request path
   remains the fallback.
4. **Jumbo frames** stay off: the LAN switching drops frames over 1500
   bytes (ENVIRONMENT.md).

## ADR-049 — Placing models on off-grid hosts (2026-10-12)

**Decision.**
1. **Place** (ADR-043) spreads over fabric hosts only: an empty host list
   and `@all` mean the fabric hosts, an off-grid host named there is
   refused, and off-grid copies are neither counted toward the factor nor
   removed by it (as archive copies).
2. **Off-grid** is a fourth placement mode beside Multi host, Single host
   and Archive: the off-grid hosts picked (0..N, `@offgrid` for all) each
   get a full copy of the selection where it fits; the other off-grid
   hosts' copies are removed, unless a rule keeps them or no other copy
   would remain. Fabric hosts are left as they are. A one-shot plan,
   edited and applied like Place; rules remain the way to force copies.
3. **Offload** accepts an off-grid host as its destination. After the copy
   lands there, the fabric hosts' copies are removed; copies on other
   off-grid hosts stay (they are placed on purpose). The copy on the
   off-grid host counts like any other, so this is not a deletion.
4. **Hugging Face downloads** spread over fabric hosts only; an off-grid
   host is a valid single `--host`.

## ADR-050 — Moving a host between the fabric and off-grid (2026-10-12)

**Decision.** `nest cluster role HOST offgrid|fabric [--addr IP:PORT]`,
run while the host is stopped for its move. Off-grid: the host stops
voting (kept as a learner) and its membership address is cleared; every
node watches membership and stops dialling a member whose address is
cleared, so it is reached over the connections it opens. Fabric: its
address is set while it is still a learner, then it becomes a voter. At
least three voters must remain. The host then starts with the matching
`node.toml` (`role`, `listen`, members at fabric or LAN addresses). Its
copies stay where they are; only how they are counted by Place changes.

## ADR-051 — RDMA traffic counters for benchmarking (2026-10-15)

**Context.** The RDMA fabric is shared by sparknest (model loads,
replication), the LLM engine's engram table reads and its activations.
Engine benchmarks are sometimes confounded by sparknest traffic; the engine
measures the NIC totals on its own schedule and needs sparknest's share to
subtract.

**Decision.**
1. Each RDMA device port keeps cumulative payload bytes, operations and
   estimated packets (`ceil(bytes / path_mtu)` per operation), sent and
   received, as relaxed atomics added on the completion path where sizes
   are known (data at successful completion, control messages at post).
   They reset only when the daemon restarts; a random `start_id` marks
   each start. Reading them takes no lock.
2. Exposed read-only and unauthenticated, on loopback only: a JSON
   snapshot (`GET 127.0.0.1:7403/v1/rdma/counters`) and a binary stream on
   `<state_dir>/rdma-counters.sock` (mode 0666, for containers): a hello
   with the port table, then counters at 1 Hz or a rate the reader asks
   for with one byte (up to 20 Hz). Slow readers skip frames, never
   buffered. Layout in docs/RDMA-COUNTERS.md, pinned by a fixture both the
   encoder and the Python reference decoder are tested against.
3. Ports are named as `/sys/class/infiniband` names them, with their
   netdev, addresses and path MTU; timestamps are `CLOCK_MONOTONIC` and
   wall clock with the boot id.
4. Not included: per-peer or per-class splits, events, the NIC's own
   counters, TCP lanes, rates, file names, histograms, control actions.

## ADR-052 — The counters socket in a directory of its own (2026-10-15)

**Context.** ADR-051 put the counters stream at
`<state_dir>/rdma-counters.sock`, for containers to bind-mount. A bind
mount of a file pins that inode; the daemon unlinks and recreates the
socket at every start, so a container keeps a dead socket until it is
restarted itself. Mounting the whole state dir instead would hand
containers the metadata and the cluster secret.

**Decision.** The socket lives at `<state_dir>/run/rdma-counters.sock`.
The daemon creates `run/` (mode 0755) and nothing else goes in it;
containers mount the directory, which outlives the socket, and reconnect
after a restart. The 0.4.0 path is removed at start, not kept: nothing
depended on it yet. Protocol and permissions are unchanged.

## ADR-053 — Files marked always passthrough (2026-10-15)

**Context.** Fabric hosts run `passthrough = "sole"`: a file with copies on
other hosts is served by the daemon so its reads can be spread (ADR-030),
unless its reads are scattered (ADR-031, learned after 64 MiB of wasted
readahead, and cleared again by hosts that see it stream). The LLM
engine's engram tables want the kernel's path from the first read on
every host that holds them, whatever was learned. Separately, `sole`
counted off-grid hosts' copies as "elsewhere", though reads are never
spread to them: a fabric host sharing a file only with an off-grid host
gave up passthrough for nothing.

**Decision.**
1. A per-file flag (`Command::SetPassthrough`, bit 11 of `files.flags`,
   beside the scattered bit): a host with a complete local copy of a
   marked file hands it to the kernel at open, whatever its policy. A
   host without one reads it as usual. Set by people or tools (`nest
   passthrough SEL always|default`, `POST /v1/passthrough`, a toggle on a
   model's page and the Passthrough… menu item), never learned or cleared
   by hosts. A selector (a path, a tree or hf:org/name) marks its files
   as they are; files added later are not marked until it is set again.
2. `sole` counts only other fabric hosts' live copies.
3. Hosts that predate the command cannot decode it (ADR-018), so setting
   the flag is refused until every host answers with the version of the
   host taking the request, or later.

**Alternatives.** A selector rule evaluated at open (follows new files,
but costs a selector match per open and a second place to look); marking
the file scattered (also changes how hosts without a copy read it, and
hosts revise it).

## ADR-054 — Metadata reads answered on the ring thread; READDIRPLUS (2026-10-15)

**Context.** `du` over the production mount (24,210 entries) took 0.3–0.8 s
on raptor but 5–12 s on the Sparks, idle or not, off-grid or not: about
400 µs per `stat`. Every LOOKUP and GETATTR went from the ring thread
that received it to a tokio worker and back through an eventfd, several
cross-CPU wakeups for a sub-millisecond SQLite query. The daemon used
3.5 s of CPU of kakapo's 11 s, and `du` slept 30,000 times; disabling the
deep idle states (433 µs exit) only brought it to 8.7 s. And every entry
of a listing cost its own LOOKUP, since READDIR carries no attributes.

**Decision.**
1. LOOKUP and GETATTR are answered on the thread the request arrived on
   (the CPU's ring queue, or a /dev/fuse reader) whenever this host's
   replica answers them: always, except for a file another host owns,
   whose live size takes a call; those still go to the runtime. READDIR
   already worked this way, and READ has its own fast path (ADR-025).
2. READDIRPLUS (`FUSE_DO_READDIRPLUS`, not the adaptive mode): listings
   carry each entry's attributes from the same join, so stat after
   readdir (`du`, `ls -l`, `find -size`) needs no request per entry. An
   entry another host owns goes out with zero entry and attribute TTLs,
   so the kernel asks again for its live size on use.

Both read SQLite on the request thread, as READDIR did. The pages are
local and almost always in the page cache; a slow query delays that CPU's
queue, not the runtime.

**Alternatives.** Keeping metadata on the runtime and making the handoff
cheaper (busy-polling the eventfd burns a core per queue; pinning workers
does not remove the wakeups); a longer attribute TTL (helps repeated
scans only, and delays other hosts' changes); the adaptive READDIRPLUS
mode (plain listings would save the attribute columns, but listings of
large directories switch between the two and lose the point).

## ADR-055 — Hugging Face imports and downloads straight into an archive store (2026-10-15)

**Context.** A model meant for an archive (raptor's `/mnt/scratch`, the NAS
share at `/mnt/models`) had to land on a host first and then be offloaded:
the bytes crossed the host's NVMe twice, and a cache already sitting on the
archive's disk (`/mnt/scratch/hf`) was copied off it only to be copied back.

**Decision.**
1. `nest hf import SRC --archive NAME` and `nest hf download REPO --archive
   NAME` (the API's `archive` field; the Download dialog's Archive choice)
   run on a gateway of that archive and put the blobs in it, never on a
   host. Other hosts refuse, naming the gateways.
2. **Import.** Each blob goes into the archive's objects: hard-linked when
   the source is on the archive's filesystem (CIFS links within a share
   too), else copied with the archive's paced, direct writes. A blob the
   cluster already has gains the archive's copy the same way. Snapshot
   links are mirrored from the source (hf would write blobs through the
   mount onto this host). Refs are small mutable files and stay on the
   host, as any write through the mount.
3. **Recorded with existing commands.** `Import` records its copy on the
   importing host's store; a `Batch` of `Import`, `PublishReplica` into
   the archive and `RetireReplica` of the host's copy commits the entry
   with the archive's copy only, all or nothing. No new command, so it
   needs no version gate (ADR-018); retiring a copy that was never written
   deletes nothing.
4. **Download.** The files the hub lacks at the planned commit are fetched
   one at a time by `sparknest-hf-fetch` into a scratch hub on the
   archive's filesystem (`<archive>/hf-download-<node>/`), then imported
   from there as in 2 (hard links: nothing is written twice) and the
   scratch hub removed. The branch ref is written as for other downloads.
   A gateway clears what an earlier, interrupted run left in its scratch
   directory.

**Alternatives.** Downloading onto a host and offloading (two writes, and
the host needs the room); letting hf write into the archive through a
second FUSE mount (no such mount exists); a new `Import` variant naming a
store (needs every host upgraded first, for no gain over the batch).

## ADR-056 — Reads ride out elections; cluster settings; control-plane retransmits; archive SQLite built locally (2026-10-17)

**Context.** On 2026-10-16 a model load failed with EIO through the mount on
two hosts and sparknestd logged nothing. A switch upgrade had dropped every
fabric link twice, metadata quorum was gone for about three minutes, and a
read that found no servable copy got one lease-long barrier before failing.
Separately the leader logged thousands of heartbeat timeouts an hour during
heavy multi-host runs: the fabric switch tail-drops 1–5% of egress toward the
100G hosts under incast, Raft rides plain TCP on the same port, Linux waits
at least 200 ms to retransmit, and openraft gives a heartbeat only its
interval (100 ms) as budget. And the leader's 6-hourly metadata snapshot into
the `models` archive (an SMB share) had never succeeded: SQLite cannot lock a
file there ("database is locked"), leaving an empty `tmp-*.sqlite` per try.

**Decision.**
1. **Every EIO is logged**, rate-limited: the first of each (file, reason) in
   a 10 s window at WARN with the operation, path and reason; the rest are
   counted and reported as "N more EIO on M files in the last 10 s".
2. **Reads wait out a leader change or a lost quorum.** A read that ends in
   `Unavailable` (no servable copy: lease lapsed, holders unreachable)
   retries with backoff (50 ms doubling to 500 ms) until the cluster's read
   recovery wait is spent, then fails as before. The lease itself is not
   stretched: it still bounds how long a cut-off host serves its copies.
3. **Cluster settings.** `Command::SetSetting { name, value }` (appended,
   ADR-018) stores an integer under `setting:<name>` in the metadata `kv`
   table; `None` returns it to the default. Every host reads them from its
   replica, cached for 1 s, so a change applies everywhere within a second
   of being applied. `nest_types::Setting` names each one with its unit,
   default and range. The first is `read-recovery-wait` (ms, default 5000,
   0–120000). `nest config ls|get|set|reset`; `GET /v1/settings`,
   `PUT /v1/settings/{name}` `{"value": n}`, `DELETE /v1/settings/{name}`.
   Setting one is refused until every host runs this version or later
   (as for ADR-053). Raft timing stays in node config: changing election
   timing on a live cluster invites the elections it should prevent.
4. **Control-plane retransmits.** Peer connections (dialled and accepted)
   set `TCP_RTO_MIN_US` to 5 ms, so a lost segment is resent within a few
   milliseconds; kernels without it keep the default. The heartbeat
   interval goes from 100 to 150 ms with elections still at 500–1000 ms
   (more than three heartbeats): openraft wraps each heartbeat in a timeout
   of exactly the interval, so a floor inside our network layer could not
   extend it, and raising the election timeouts would slow failover.
5. **Archive SQLite files are built locally.** Metadata snapshots and backup
   manifests are written under `<state_dir>/tmp-sqlite/`, then copied into
   the archive under a temporary name, synced and renamed into place. Each
   run removes `tmp-*.sqlite` older than an hour from the archive directory.

**Alternatives.** A dedicated Raft connection (TCP head-of-line blocking
behind data frames costs milliseconds once retransmits do); hedging Raft
over the 10G LAN (more machinery; revisit if timeouts persist); lossless
RoCE with PFC (the user declined: ECN removed most drops, and priority
classes would complicate sharing the fabric with the LLM engine); stretching
the lease during elections (weakens the guarantee that a cut-off host stops
serving); SQLite on SMB with `nobrl` (unsafe if two writers ever meet).

## ADR-057 — A 10G standby for the control plane and data; remote writes over RDMA; leader handoff and a preferred leader; serve cache closed on fence (2026-10-17)

**Context.** ADR-056 shortened control-plane retransmits but left every
fabric host's control connection, and all of its non-RDMA data, on the one
CRS804 fabric switch: a switch reboot (2026-10-16) or a pulled cable takes
quorum with it although every host also sits on a 10G LAN (172.22.x).
Remote writes travel inside requests on the Raft connection, so a 1 MiB
write delays the heartbeats queued behind it. A daemon stop drops the
leader without telling anyone (an election per restart), and the leader
lands wherever the last election did, although raptor (400G, always on)
is the best placed. A daemon restart replaces the FUSE mount: containers
that bind `/mnt/sparknest` privately keep a dead mount until restarted.
The serve cache keeps an object open up to 2 s after its copy here was
retired, which holds the disk space.

**Decision.**
1. **A standby address per member.** `Member { lan: Option<SocketAddr> }`
   in `node.toml` (each host's 10G address, port as `addr`). Fabric hosts
   with a `lan` for a peer keep a second, idle control connection to it
   there, kept up by the dialler (lower node id dials).
2. **Fail over on link failure only, and come back.** Control requests use
   the fabric connection while it is up. When it breaks (closed, reset,
   connect refused) or a request to the peer gets no answer at all for
   1 s while the standby answers, the peer's requests switch to the
   standby; slowness alone never switches. Every 2 s the fabric is
   re-dialled; after 10 s connected it is primary again. Requests in
   flight at a switch fail as `Unreachable` and openraft resends them; a
   snapshot transfer restarts from offset 0, which the receiver handles.
   Nothing is sent twice on purpose (no hedging, ADR-056 stands).
3. **A standby data lane.** Fabric pairs keep one TCP lane (ADR-048) to
   each other on the LAN (`node.lanes_fabric`, default 1). Reads use RDMA
   while a link is up; when link setup fails or a link dies they use the
   lane, and RDMA is retried as today (`link_retry`, doubling to 30 s).
   The control connection no longer carries file data between fabric
   hosts except as a last resort.
4. **Remote writes over RDMA, pulled by the owner.** A writer on another
   host registers its buffer and sends a 64-byte `WriteReq` (file, epoch,
   writer, offset or append, length, buffer address and key). The owner
   takes a staging slot, RDMA-READs the bytes, applies the write exactly
   as `owner_mutate` does today (epoch check, append offset chosen there),
   and answers with a `WriteDone { n | error }` SEND. Pulling, not
   pushing, means nothing on the owner is reserved per writer and a
   receiver never runs out of posted buffers (RC has no multi-packet
   receive; `rnr_retry = 7` would stall the link). Without a link the
   write goes over the standby lane, and only then over the control
   connection. Read protocol 3; hosts on 2 still pair for reads and write
   over TCP.
5. **Leader handoff on shutdown.** `RaftReq::TransferLeader` (appended)
   carries openraft's `TransferLeaderRequest`; a stopping leader hands off
   to the most caught-up voter and waits up to 2 s for it to take over
   before stopping Raft. A peer that cannot decode it is skipped.
6. **A preferred leader.** Cluster setting `preferred-leader` (a node id,
   default none, ADR-056 settings). The leader hands off to it when it has
   been up, caught up and reachable on its fabric connection for 2 minutes
   and the last handoff was over 10 minutes ago, so a flapping host never
   bounces leadership.
7. **The serve cache follows fencing.** Fencing a key (deletion, eviction,
   invalidation, a new owner's generation) removes it from the serve
   cache at once; reads already holding the file finish and its space is
   freed with the last of them. Entries also expire 2 s after last use
   (sliding) and at most 30 s after opening.
8. **The mount survives a restart.** The daemon hands its `/dev/fuse`
   descriptor to systemd's file-descriptor store (`FileDescriptorStoreMax`)
   and a restarted daemon takes it back instead of mounting again, so
   processes and containers keep their mount. The FUSE session is
   re-initialised on the old descriptor; requests that arrive meanwhile
   wait in the kernel. Fall back to a fresh mount when there is no stored
   descriptor (first start, crash with a stale one).
9. **Health.** `nest net`: per peer, which path control and data use, the
   round-trip times our Raft network measures, failovers and returns, and
   the RoCE retransmit counters (`hw_counters`: `out_of_sequence`,
   `packet_seq_err`, `local_ack_timeout_err`). Each leader change and each
   failover is logged at WARN.
10. **Benchmark first.** `benchmarks/M12-CONTROL-PATH.md`: control call
    round trip and Raft commit latency on the fabric and on the LAN, idle
    and under load, with ADR-056's 5 ms retransmit floor on both. If the
    LAN proves as good as the fabric under load, revisit which is primary.

**Alternatives.** Hedged duplicate Raft sends on both networks (duplicate
snapshot chunks restart transfers; needs dedup; rejected in ADR-056);
the LAN as permanent primary (decided against until measured); pushing
writes into credited slots on the owner (one round trip faster, but needs
per-writer reservations and credit reclaim); a SEND carrying the write
(posted 1 MiB receives per connection; an empty queue stalls the link
under `rnr_retry = 7`); preferred leader in `node.toml` (could differ
between hosts; settings are set once and persist); rslave bind
propagation in containers instead of keeping the mount (needs every
container changed and still breaks open files).

## ADR-058 — Remote writes as built: mixed versions, lane probe, append lock (2026-10-17)

**Context.** ADR-057 item 4 as implemented needed three choices it left
open, and turned up one existing bug.

**Decision.**
1. **Protocol negotiation, not refusal.** A 0.5 host offers fabric
   protocol 3; a host that refuses with "read protocol …" gets a second
   offer of the version it names (at least 2). An accepting 0.5 host takes
   2 or 3. Each link records the protocol both speak; writes use RDMA only
   on protocol-3 links, so 0.4.4 hosts keep RDMA reads during a rolling
   upgrade. `FabricReq`/`FabricResp` are unchanged.
2. **Lane probe.** A 0.4.4 host closes a TCP lane on an unknown frame kind,
   so each lane opens with a read of `FileId(u64::MAX)`: a 0.5 host answers
   with its lane protocol, an older one with NotFound. Write frames (kind
   4) go only on lanes that answered 2.
3. **Unknown outcome is EIO.** A write sent but not answered (timeout, lane
   lost after the send) fails; it is never resent another way, since a
   resent append would be applied twice. Only a write known not to have
   been sent moves on to the next path.
4. **Appends hold a per-file lock** from choosing the end of file until the
   bytes are written. Before, concurrent appends from several writers on
   the owner could each read the same length and overwrite each other.
5. **A write that races another host's ownership grant is routed again**
   instead of failing "no reachable node holds this file".

## ADR-059 — Keeping the mount across a restart: spike findings; ADR-057 item 8 not built (2026-10-17)

**Context.** ADR-057 item 8 proposed handing `/dev/fuse` to systemd's fd
store so a restarted daemon resumes the same FUSE connection. A spike
(outside the tree, in `~/build/fusefd-spike` on raptor: vendored fuser
with a no-INIT `Session::resume`, a toy filesystem, our `uring.rs`)
tested it on raptor's kernel 7.0.0-38 against the v7.0 sources of
`fs/fuse/dev.c`, `dev_uring.c`, `iomode.c`, `backing.c`.

**Findings.**

1. **The fd store works.** A transient user unit (`Type=notify`,
   `FileDescriptorStoreMax=4`, default `FileDescriptorStorePreserve=
   restart`) got the fd back through `LISTEN_FDS`/`LISTEN_FDNAMES` on
   `systemctl restart` and on SIGKILL with `Restart=always`; `stop`
   empties the store and leaves a dead mount (our `ExecStopPost`
   detaches it). FDSTORE needs only `sendmsg` with `SCM_RIGHTS` (nix).
2. **On /dev/fuse (no io_uring) resuming is sound.** The connection
   stays initialised (no second INIT; the new process must reuse the
   negotiated values). Requests issued while no daemon runs wait in the
   kernel and are answered by the next one (measured: 2 s gap, no
   error). Requests the dead process had read but not answered stay on
   its device's processing list while the stored fd keeps the
   `struct file` alive; `FUSE_NOTIFY_RESEND` (in 7.0, `FUSE_HAS_RESEND`)
   requeues them with the same unique id marked `FUSE_UNIQUE_RESEND`.
   Stress: 10 SIGKILL + resume cycles under 32 parallel readers, 61 297
   reads, none failed, nothing left waiting. Open files, cwds, and
   containers with a default (rprivate) bind kept working.
3. **With FUSE over io_uring it is not.** On exit io_uring cancels only
   ring entries that are idle (`fuse_uring_cancel`: `FRRS_AVAILABLE`);
   a request already dispatched to an entry stays on that queue's own
   `fpq`, and `fuse_resend()` walks only `fc->devices`, never ring
   queues, so it is never answered. The new process can register its
   queues again (`fc->ring` and `ring->ready` persist; measured, 64
   queues), but requests queued while no daemon ran stay on their CPU's
   queue until that queue sees a commit. Stress: the first SIGKILL left
   8 requests stuck forever, readers in D state, and lookups of the same
   names piling up in `d_alloc_parallel`; only aborting the connection
   recovered. A graceful stop narrows the window but cannot close it: the
   kernel may hand an entry a request (task work) after our last look.
   Nothing in mainline (checked master, 2026-10) recovers ring requests.
4. **io_uring cannot be dropped for the resumed connection.** Once a
   ring is ready `fiq->ops` stays `fuse_io_uring_ops`; a /dev/fuse-only
   server on such a connection receives nothing (measured: every access
   timed out). Only a connection that never negotiated
   `FUSE_OVER_IO_URING` can be resumed.
5. **What the daemon would have to carry over** (none of it exists
   today): the kernel keeps file handles (`fh`) chosen by the old
   process and sends them unchanged (measured), so VFS handles must be
   re-derivable or persisted; inode numbers are already stable
   (`ino = FileId`) and lookup counts can be ignored (we ignore FORGET);
   a passthrough inode stays bound to its backing file in the kernel and
   every new open of it must answer FOPEN_PASSTHROUGH with a backing id
   the new process does not know (else EIO, `iomode.c`); the new
   process's `OpenSession` expires the old session, which drops its
   POSIX locks and deletes open-but-unlinked files, and write ownership
   of open writable files lives in memory. Each of these is lifecycle and
   fencing work that needs its own tests first.
6. **rslave does not rescue today's remount.** A container binding the
   mountpoint itself (`-v /mnt/sparknest:/mnt/sparknest:rslave`) keeps a
   bind of the old FUSE root: the new mount is a sibling on the host, not
   a child of that bind, so nothing propagates ("Socket not connected").
   Only binding a parent with rslave (`-v /mnt:/mnt:rslave`) shows the new
   mount, and open files and cwds inside the old one still break.
7. **A mount holder** (a small process that owns the connection and
   forwards requests to the daemon over a Unix socket) would keep
   io_uring between kernel and holder but adds a hop: 2–2.5 µs per
   round trip up to 4 KiB, 9 µs at 128 KiB, 44 µs at 1 MiB measured on
   raptor (memcpy across the socket), i.e. most of what io_uring saved
   on 4 KiB reads (58–179 → 19–21 µs, M8), plus a second process with its
   own restart and handle story. Passing the fd back instead of proxying
   brings back findings 3–5.

**Decision.** ADR-057 item 8 is not built in 0.5.0: FUSE over io_uring
stays required (the user's standing rule), and a resumed io_uring
connection can lose requests. The choice between
"the mount survives restarts" (no io_uring on the mount: 4 KiB reads back
to 58–179 µs, 128 KiB to 0.4 GB/s; plus the carry-over work in 5) and
"keep io_uring" (restarts replace the mount) is the user's. Until then
containers that must ride out a restart bind a parent directory with
`rslave` and are restarted if they held files open; a restart remains an
operation to schedule (memory: no restart while the mount is in use).

**Alternatives.** Build the resumable mode behind `fuse.keep_mount =
true` (forces `io_uring = false`): fd store, `Session::resume` in
vendored fuser, persisted INIT parameters, NOTIFY_RESEND, handle
re-derivation, lock and session carry-over, passthrough re-binding; a
week-scale change with fencing tests first. Wait for kernel support for
recovering ring requests (none proposed as of 2026-10).

## ADR-060 — A TCP lane that stops answering is closed; cancel stops a copy mid-file (2026-10-18)

**Context.** On 2026-10-18 the Kimi-K3 page in the web UI opened empty.
Its only live copy was on oliver (off-grid, WSL behind NAT), and every
read raptor sent oliver waited the full 30 s lane timeout before the
archive copy answered (the UI gives up at 20 s). Six of oliver's eight
lanes to raptor had stopped carrying raptor's bytes 3.2 h earlier, during
a large archive sync that pulled about 477 GB from oliver over them: on
raptor they held unacknowledged data in exponential backoff (rto 120 s,
backoff 12), while oliver saw them idle and healthy and its answers still
arrived, so neither end closed or redialled them. Closing raptor's ends
(`ss -K`) made oliver redial at once and the page load in 0.18 s. Two
gaps let it last: a read that timed out left its lane in use, and lanes
had no `TCP_USER_TIMEOUT`, so the kernel would retry for the default
~15 retransmits (tens of minutes to hours) before giving up.

**Decision.**
1. A read or write on a lane that gets no answer within `read_timeout`
   closes that lane: its other waiting requests fail at once (reads try
   the next source; writes report an unknown outcome as before), and the
   dialling end opens a fresh lane within half a second. An answer that
   is only slow costs one reconnect.
2. Every lane sets `TCP_USER_TIMEOUT` = 10 s (`LanesConfig::stall_timeout`)
   and TCP keepalive (idle 10 s, 3 probes 4 s apart), so the kernel itself
   drops a connection whose bytes go unacknowledged, and idle lanes keep
   NAT mappings refreshed.

The in-process test `a_lane_that_stops_answering_is_replaced` relays the
lanes through a proxy that silently drops one direction of every open
lane, and fails without item 1.

Also in 0.5.1: **a cancelled copy stops mid-file.** Cancelling a
replicate or offload used to skip only files not yet started; the ones
copying ran to the end. Cancelling Kimi-K3 to scratch (17 GB files pulled
from oliver over 10G) took about five minutes to settle. A file mid-copy
now stops within 200 ms of the cancel reaching its host: the copy is
dropped, nothing is published, and its staging file is removed once the
direct writes already queued land (the write ring owns their buffers and
files, nest-store/src/uring.rs). It counts as neither done nor failed.
Test: `cancel_stops_a_file_mid_copy`.

**Alternatives.** An application-level ping per lane (the control
connections' standby probe, ADR-057) would also find it but adds traffic
to every lane; the timeout already is that signal for a lane in use, and
the kernel options cover idle ones. Shorter `read_timeout`: a 4 MiB read
over a busy 10G link can legitimately take seconds.

## ADR-061 — Each cached branch of a Hugging Face repo is its own model (2026-10-18)

**Context.** Some publishers keep one quantisation per branch:
turboderp's `Qwen3.5-9B-exl3` has `2.00bpw`, `6.00bpw` and more, with
`main` holding only the README. huggingface_hub caches several revisions
of one repo side by side: `refs/<branch>` names a commit, each commit has
its own `snapshots/<commit>/` of symlinks into the repo's `blobs/`, and a
blob two revisions share is stored once. `hf cache rm <commit>` removes
one revision and only the blobs no other revision uses. sparknest showed
a repo as one row whatever it held, the download dialog had no way to
name a branch, and `hf rm` removed every revision.

**Decision.**
1. Wherever a repo is named (`/v1/hf/download`, `/v1/hf/size`,
   `/v1/hf/refs`, `nest hf download`, the dialog) `org/name@branch` and
   `huggingface.co/org/name/tree/<branch>` links name that branch;
   `revision` still overrides.
2. `/v1/hf` lists a repo with two or more cached revisions as one row per
   revision: `branch` is its first ref (main first) or its commit,
   `branches` every ref naming it, and `selector`
   `hf:org/name@<branch>`, which placement already accepted (the
   revision narrows a selector to that snapshot's files). Sizes,
   readiness, detail, Place, Copy and Remove-from all work per branch
   through that selector. A repo with one revision keeps its plain row
   and selector.
3. `hf:org/name@<branch or commit>` in `/v1/hf/remove` resolves a branch
   through `refs/` and removes that commit with `hf cache rm`, so blobs
   another branch shares stay.
4. `/v1/hf/refs` lists the Hub's branches and tags (huggingface_hub's
   `list_repo_refs`, through `sparknest-hf-fetch refs`), each marked
   `current`, `older` or not cached here; the dialog offers them as a
   branch picker, `nest hf refs` prints them.
5. Download job names carry `@branch` when one was asked for.

Tested on the trial cluster with turboderp/Qwen3-0.6B-exl3 `3.0bpw` and
`2.75bpw_H5`: both downloaded, listed as two rows of their own sizes
(611 MB, 578 MB), the second placed on kiwi without moving the first, and
removing it left `3.0bpw` whole.

**Alternatives.** One row with revisions underneath: a quant is chosen
and placed like a model, and a single row hides which one a host holds.
Treating each branch as a separate repo directory: huggingface_hub
would not find it.

Amended in 0.5.4 (this ADR's item 2): only snapshots a ref names get
rows, and only when two or more are named. Updates leave the previous
commit's snapshot behind with no ref, and listing those split about 25
ordinary repos into near-duplicate rows.

## ADR-062 — The OOM killer passes over the daemon (2026-10-19)

**Context.** On 2026-10-18 ostrich ran out of memory while a container
built with CUDA. The kernel killed sparknestd first, then each restart of
it, four times in 11 s, before reaching the python processes. On a DGX
Spark the GPU shares system memory, and the memory a CUDA context holds is
not charged to its process: the report showed about 9 GiB of process
memory and no page cache on a 121 GiB host. The largest charged task was
the daemon, whose ~960 MiB of registered RDMA slots (ADR-032) are
anonymous and pinned. Killing the daemon frees little and unmounts
`/mnt/sparknest` under every reader, including the job that needed the
memory.

**Decision.**
- The system unit sets `OOMScoreAdjust=-500`. The daemon is still a
  candidate when nothing else is left (not -1000).
- Children inherit the score, so hf and `sparknest-hf-fetch` are started
  back at 0 (writing their own `oom_score_adj`, which needs no
  privilege): a download is ordinary work.
- Hosts installed earlier pick this up by copying the rendered unit to
  `/etc/systemd/system/` again (root, `systemctl daemon-reload`); it
  applies from the next restart.


## ADR-063 — Push loads: each source disk streams once, each rank receives only its bytes (2026-10-21)

**Context.** The LLM engine (cuteafd) starts a model on 6 Spark expert
ranks and a coordinator (raptor). Each rank needs slices of many files,
and which bytes belong to which rank changes with the TP degree of each
run. Measured on 2026-10-20 (`benchmarks/M13-MODEL-LOAD.md`):
- **GLM 5.3 EXL3 K4:** each rank requested 248 GB to keep ~57 GB. EXL3
  trellis rows interleave every rank's columns (1 KiB runs every 6 KiB at
  TP6), so a reader must fetch whole rows.
- **Kimi K3 EXL3 K2:** 467 GB requested per rank for 109 GB owned; each
  Spark's NIC received ~545 GB; read+upload took ~315 s.
- **The pull path's limits:**
  - a per-rank receive ceiling near 5 GB/s (16 lanes of ≤1 MiB FUSE
    requests, each a fabric round trip of ~4 ms under load);
  - a source hot spot whenever all ranks read the same layer (one file,
    one holder);
  - each source disk re-reading its files for every rank once the page
    cache is lost.
- **What the engine fixes reached:** random layer order, open handles and
  coalesced reads brought GLM from 143 s to 102 s, with every source and
  disk still mostly idle.

Rearranging checkpoints per TP is not an option (no space for extra copies,
and TP changes run to run). Host memory on the Sparks is nearly all
weights (Kimi leaves 5–9 GB free), so nothing may hold whole layers.

**Decision.**
1. **Push groups.** An engine may load weights by *push* instead of
   reading through the mount. It is an accelerator, never a dependency:
   when the weights are not on sparknest, the socket is absent, or any
   step fails, the engine reads through the mount as before.
   - **Plan:** the coordinator describes the load in one plan: which bytes
     of which files go to which rank, at which offset in that rank's
     memory.
   - **Schedule:** sparknest does the rest: which copy to read, one stream
     per source store, flow control and failover.
   - **Load times:** one group per load time (the Spark experts, and the
     coordinator's own tensors, are separate groups).
   - **State:** groups live only in memory, on the hosts taking part. No
     Raft state, no persisted types.
2. **Plan format: N-dimensional strided slices.**
   - **Patterns:**
     `{run: u32, dims: [{count | indices[], src_stride: u64, dst_stride: u64}; ≤4]}`,
     outermost dimension first. A pattern copies the file bytes at
     `src_base + Σ iₖ·src_strideₖ` (`run` bytes each) to
     `dst_base + Σ jₖ·dst_strideₖ`, where `jₖ` is the step's position and
     `iₖ` the same, or the step's value for an index list.
   - **Entries:** `{file, pattern, src_base, dst_base, rank}`. Patterns
     are sent once and shared by every expert and layer.
   - **Index-list dimensions:** at most one dimension per pattern may be
     an index list (sorted, unique), in place of `count`. It selects from
     the source and packs into the destination: the source steps by the
     index's value, the destination by its position in the list (balanced
     EXL3 at TP6, mixed-K EXLR8: one entry per tensor and rank).
   - **Runs** may be as small as one byte (scalars, scale slices).
   - **Destination strides:** these let the engine land each rank's runs
     already packed in its own layout; the engine computes them from its
     TP and balance, and sparknest knows nothing about kernel layouts.
   - **Ordering rule:** within each dimension, steps are increasing and
     non-overlapping on both sides (each stride ≥ the extent of the
     dimensions inside it).
   - **One writer per destination range:** each entry's destination
     extent is disjoint from every other entry's for the same rank, which
     holds when a share lands packed and contiguous. Bytes sent to every
     rank are one entry per rank.
3. **Checked before any byte moves.** At start, the hub resolves each file
   to its current generation, which must be sealed, and pins it. It then
   checks, in closed form:
   - every entry lies within its file;
   - destination extents are disjoint per rank and within the rank's
     arena size;
   - byte totals per rank and per source.

   A bad plan is refused with the first bad entry and why. Plans are data
   that is checked, never code that is run.
4. **Sources: one stream per store, one pass in disk order.**
   - **Assigning files:** the hub gives each file to one live copy,
     balancing bytes per store. A host's own store comes first, then
     fabric hosts; an archive store only if no host holds the file, an
     off-grid host only if nothing else does.
   - **Reading:** each source sorts its files' entry extents by offset,
     merges them (gaps under 1 MiB are read through) and reads with
     O_DIRECT through io_uring in 16 MiB reads, a few files in flight, the
     object opened through `ReadSource` (fencing and in-flight guards as
     for every read).
   - **Packing:** for each buffer read, it walks the entries overlapping
     it (each pattern jumps to its first run in the buffer in O(dims))
     and copies each run into the destination rank's open send slot. Runs
     adjacent in the destination merge into one fragment.
   - **Sending:** a slot goes out when full or at the end of the stream.
     Every source streams at once: 8–9 streams with 6 Sparks, raptor and
     an archive.
5. **Delivery into a ring the engine owns.**
   - **The ring:** each rank creates a memfd ring (default 128 MiB, slots
     of `slot_bytes`, e.g. 4 MiB) and `cudaHostRegister`s it. It passes
     the fd to its local sparknestd, which maps it and registers it with
     its NICs.
   - **Slot contents:** `payload | fragment table | footer`. The footer is
     `{magic, n_frags, payload_len}` in the last 16 bytes; each fragment
     is `{dst_off: u64, len: u32, _: u32}`, with payloads in order.
   - **RDMA:** sources RDMA-WRITE_WITH_IMM a whole slot; the immediate
     names the slot, and the completion's byte count gives the length, as
     for reads (ADR-032).
   - **Checking:** the receiving sparknestd checks the footer and that
     every fragment lies in the rank's arena, then tells the engine
     `Filled{slot, len}`.
   - **Copying:** the engine copies, or converts, each fragment to
     `arena + dst_off`, and sends `Release{slot}` only after that copy has
     completed (a CUDA event).
   - **Ordering:** arrival order is arbitrary. The engine allocates its
     whole arena before the load and runs any format transform after
     `Done`, over complete tensors.
6. **Flow control from the receiver.** A free slot is a credit.
   - **Who gets credits:** the receiver hands credits to the source with
     the most bytes still owed to this rank.
   - **Incast:** it keeps the bytes granted and not yet filled within the
     fabric's incast budget (`[fabric] inflight_mib`, ADR-032). The
     switch is lossy without PFC.
   - **A slow rank:** it stalls only the sources' slots for that rank; a
     source keeps reading for other ranks until its read ring is full.
7. **Transports.**
   - **RDMA, per group:** queue pairs are made just for the group between
     each source and receiver host, so the read path's lanes, windows and
     protocol are untouched. They are torn down at the end, after the QPs
     are in the error state and flushed (as dead lanes are, ADR-060).
   - **TCP, a real transport:** used for off-grid or archive sources, not
     only for tests. A slot travels as a frame on the existing TCP lanes,
     and the receiver's lane thread reads it straight into the ring. Push
     moves 4–5× fewer bytes than pull, so slow links gain the most.
   - **Same host:** a source on the rank's own host packs straight into
     the ring.
8. **Who talks to whom.**
   - **Engine ↔ local sparknestd:** an abstract SEQPACKET socket
     `@sparknest-push` (the engine's containers share the host network
     namespace; `[push] socket` overrides it). Peers are checked with
     `SO_PEERCRED`; messages are postcard, defined in a small
     `nest-push` crate that engines may depend on.
   - **Coordinator:** it opens the group on its local sparknestd (the
     *hub*): `Create{ranks: [{rank, host, arena_bytes}]}` →
     `{group, token}`; then `Patterns`, `Entries` in batches, then
     `Start`. The hub answers with the predicted bytes and time per store
     and per rank (and which streams are slow: archive, TCP), then
     progress, then `Done` or `Failed{reason, missing}`.
   - **Each rank:** `Join{group, token, rank}` plus the ring fd, to its
     own sparknestd; then `Filled` events, its `Release`s, then `Done` or
     `Failed`.
   - **Start:** waits until every rank has joined (bounded by a timeout).
   - **Between sparknestds:** a new RPC service, `PUSH`, carries create,
     join, stream start and progress; each source sends the hub its slot
     counts per rank.
   - **Done:** the hub sends `Done` to a rank's host with those counts,
     and that host tells the engine once it has seen that many slots.
     A rank's `Done` therefore always follows every `Filled` of that rank.
   - **Several ranks per process:** a host may hold several ranks, and one
     process may join several (the coordinator's GPUs at TP2: one rank,
     arena and ring per GPU).
   - **Ring memory:** any fd that `mmap(MAP_SHARED)` accepts, a memfd or
     the engine's own exported allocation. sparknestd maps it and
     registers the mapping; a mapping the NIC cannot register fails the
     `Join`, and the engine falls back. The registration is charged to
     sparknestd, whose unit sets `LimitMEMLOCK=infinity`, not to the
     engine's container.
   - **Who may connect:** `SO_PEERCRED` must show sparknestd's own uid
     (the engines' containers run as the host user) or root. The abstract
     socket is reachable only from the host's network namespace, so a
     container without `--net=host` falls back to the mount.
9. **Failure.**
   - **Per group in phase 1:** on `Failed` the engine loads the whole
     group through the mount.
   - **Closing the socket** cancels: a coordinator closing ends the group,
     a rank closing fails it.
   - **A source that fails a file** (a dead host, an I/O error, a stale
     generation) hands it to another live copy if there is one, and
     *the whole file is sent again*. Resending is harmless: the same bytes
     land at the same addresses, and nothing is transformed before `Done`.
   - **No other copy:** the group fails, naming the files and ranks left
     incomplete. The engine falls back to the mount for them, or for
     everything.
   - **Stalls:** a group with no progress for 30 s fails.
   - **The hub restarting** fails the group.
   - **The engine** must drain its copies before freeing its arena, and
     never publish a partial load.
10. **Mixed versions.** Every host in a group (hub, ranks and sources)
    must run a version with `PUSH`; otherwise `Create` or `Start` is
    refused and the engine falls back. Nothing else changes for older
    hosts.
11. **Memory.**
    - **Source, while streaming:** a read ring of 4 × 16 MiB plus send
      slots of (credits + 1) per rank × `slot_bytes`: about 250 MiB with
      7 ranks and 4 MiB slots. It is registered at group start and freed
      at the end.
    - **Receiver:** sparknestd maps the engine's ring and holds nothing of
      its own beyond that mapping.
    - **Hub:** the plan, held during the load: tens of MB for Kimi.
    - **No checksums** on this path; tests verify bytes, and integrity
      comes from the up-front checks and per-rank totals.
12. **Phase 1 (0.6.0).**
    - **What's covered:** the Spark expert weights and the coordinator's
      tensors, which are whole source tensors, or each GPU's share, landed
      for the engine's own transforms.
    - **Sources:** host and archive stores.
    - **Transports and failover:** RDMA and TCP, failover between copies.
    - **Status and tools:** `nest push` and `GET /v1/push` show running
      groups (bytes and rates per store and per rank). `nest push bench`
      builds a synthetic plan (a file set and a TP split) so the trial
      cluster can measure push without the engine.

**Expected.** Each source reads its share once at disk speed (≈5–9 GB/s
on a Spark), so a load takes about the largest share divided by that
rate. For Kimi that is ~133 GB per source, about 15–27 s against ~315 s
now. For GLM, about 10–15 s against ~74 s. Ranks receive at a third of
their NIC.

**Alternatives.**
- **Rank-side reads through the mount, tuned:** these hit the per-rank
  round-trip ceiling and still move every byte that a rank's rows touch.
- **Per-TP copies of each checkpoint:** no space, and TP changes every
  run.
- **Lists of ranges instead of strided patterns:** half a million runs per
  rank per layer at TP6, not checkable in closed form.
- **A scripting language for plans:** it cannot be bounds-checked without
  running it.
- **RDMA straight into the engine's GPU memory:** it saves a copy the
  receiver makes anyway, and adds dma-buf registration and ordering
  problems. Rejected.
- **Gather WRITEs from the read buffer (no packing copy):** 30 SGEs per
  work request against 1 KiB runs is too many requests, and it pins read
  buffers until the write completes.
- **Reusing the read lanes:** it would mix credits into the read
  protocol's windows and immediates, and starve FUSE reads during a load.

**Amendment (2026-10-21): ranks may join before Start.** The hub tells
each rank's host to expect it when the group is created, before answering
`Created`, so a rank may `Join` any time after the coordinator hands it
the group and token: before `Start` (the engine's order, which lets it
know every ring is registered before it commits to the load) or after.
`Start` still waits, bounded, for the ranks that have not joined. A rank
host that cannot be told fails `Create`. (The first build sent the
expectation only at `Start`, refusing an earlier `Join`.)

**Amendment (2026-10-21): a finished source gives back its grants; rings
are reported.** The first production load (group 4092384301547520, 6
sources, 2 ranks, 128-slot rings) stopped after 9 s at 18.4 GB/s peak:
kiwi finished its share, but rank hosts kept it in their grant round
robin until every source was done, and within 0.3 s every free ring slot
was granted to a source that would fill none. Now, when a source
finishes, the hub sends each rank host it owed an updated `Sources`, and
the rank host takes back that source's outstanding grants. Rank hosts
report each rank's ring to the hub every 5 s (`RankState`: free,
granted, filled, released, when the engine last released a slot); the
load's record keeps it, and a stall's reason says whether a ring waits on
its engine or on sources. Same-host slots count as `local_slots`, apart
from fallbacks (`rpc_slots`). Push is not run across versions: hosts are
upgraded together before loads use it.

**Amendment (2026-10-21): one group per load.** The engine loads a model
as one group: the coordinator's own GPUs are ranks of the same group as
the Spark ranks (6 Sparks and 2 coordinator GPUs: 8 ranks, one `Create`,
one `Start`), not a separate group as item 1's example said. Nothing
changes in sparknestd: a host may hold several ranks (item 8), the hub's
host may be a source and a rank host at once, and slots from a host's own
store to its own ranks are handed over in memory. A test covers two ranks
on the hub's host beside ranks elsewhere.

**Amendment (2026-10-21): the order a rank's slots come in.** Engines
that hold slots until a band of rows is whole (the coordinator's GPUs
quantize per 128-row band) size their rings on this, so it is a contract;
changing it needs a new amendment and notice to the engine.
- **One source:** it streams its files one at a time, in plan order, and
  each file's reads in rising offset order. For each rank it packs one
  open slot at a time, in the order its bytes are read. Several files
  are never interleaved into one rank's slots. A slot may hold the end
  of one file and the start of the next, because the open slot carries
  over the file boundary.
- **Arrival:** a source keeps up to `in_flight` sends in flight (32 for
  1 MiB slots) over RDMA and lanes, and they may land out of order. For
  one rank, the window is never more than that source's grants for that
  rank. Fragments carry their arena offsets, so engines place by offset,
  never by arrival order.
- **Several sources:** slots from different sources interleave in a rank's
  ring (grants go round robin). A file, and so any tensor inside it,
  comes from one source.

**Amendment (2026-10-22): a floor of grants per source, outside the
budget.** The first group with two ranks on one host (120235912134667:
six Sparks, and raptor's two GPUs as ranks 6 and 7) stopped in its first
half second. Raptor's incast budget (95 MiB) covers both of its ranks.
Rank 6's grants took all of it, held unfilled by sources that were
blocked on rank 7 (each source streams in file order, and every file feeds
every rank), so rank 7 never got a grant. A rank host now first gives each
source owing a rank a floor of grants of that rank (`push-grant-floor`,
default 1), outside the budget, and only then grants round robin within
the budget. Freeing budget to the other rank alone still deadlocks (a CPU
model of the policy, cuteafd commit 48fe74c4a): the floor's grants must not
count against it. The cost is at most floor × sources × ranks on the host
extra slots in flight (12 MiB on raptor). The budget itself is now a
cluster setting too (`push-incast-mib`, 0: the fabric's read budget), so
both can be tuned without a redeploy. A rank host reads both when the rank
joins and keeps them for the group: a change applies to the next load,
never mid-load. A test with two ranks on one host and a 3-slot
budget fails without the floor and passes with it.

The same gap stalled a load with one rank per host (120235912134669:
ranks on ostrich, dodo and raptor; six Spark sources). A rank's grants go
round robin to every source owing it, needed yet or not. Kiwi was
streaming coordinator tensors only, so each grant of the Spark ranks
handed to it stayed unfilled; within 0.3 s all of both Spark ranks'
grants sat at kiwi, and all of raptor's at the five sources busy with
experts. Each side held what the other needed. The floor fixes this shape
too: each source always holds a grant of every rank it owes. A second
test reproduces it (a source opening with another rank's stretch) and
fails without the floor.

**Amendment (2026-10-22): sources pack into registered staging, and read
ahead across files.** A source packed each slot into a fresh zeroed
buffer and then copied it into a staging slot registered on the lane's
device. Staging is now one anonymous mapping registered on every device,
so a slot bound for another host is packed where the NIC sends it from:
no allocation, no zeroing, no copy. A write that fails hands its bytes
back for the RPC path. Slots for this host's own ranks stay plain
buffers. Staging grows by one slot per rank (each rank's slot being
packed). Read-ahead (`READ_AHEAD` reads of 16 MiB) used to restart per
file, so the queue drained at every file's end and waited on the next
open; it now runs over the source's reads of all its files in order, and
opens each file in its first read. On EXLR8 (load 1535845564284937)
rhea and moa pushed 8.0 GB/s from disks that read 13 GB/s, the slower
Sparks 4.6 from 5.7: one stream's ceiling, not the disk. Packing alone
(pack_bench, 20 GB of K4) went from 10 to 13.5–15 GB/s on a Spark and
18 to 36 GB/s on raptor.

## ADR-064 — Placement readable through the mount (2026-10-21)

**Context.** The engine's containers see the hub only through the mount;
widening their mounts to reach the API socket is unwanted. To read files
in an order that spreads load over their holders (the rotated order of
`benchmarks/M13-MODEL-LOAD.md`), a loader needs each file's holders, and
a tool needs the cluster's hosts, without running `nest`.

**Decision.**
- **Attributes:** three read-only extended attributes, answered by
  `getxattr` from metadata on the host (no RPC, so a busy peer never
  delays them):
  - `user.sparknest.hosts` on a file: the hosts holding a live copy of
    its current generation, this host first, archive stores left out;
  - `user.sparknest.where` on a file: JSON with size, generation, seal
    and every copy, each with its store name and kind (local, fabric,
    offgrid, archive);
  - `user.sparknest.cluster` on the mount's root: JSON with this host,
    the leader, the members (off-grid flagged) and archive stores with
    their gateways.
- **Never listed:** `listxattr` still lists only `user.sparknest.sealed`,
  so copying tools never carry them to another filesystem. Setting or
  removing them is `EPERM`. A directory or symlink target that is not a
  regular file has none (`ENODATA`).
- **Other filesystems** give `ENODATA` or `ENOTSUP` for these names,
  which is how an engine detects sparknest.

**Alternatives.** A hidden `.sparknest/cluster.json` file at the root:
needs a synthetic inode through lookup, open and read, and a stat-able
file that tree walkers may still reach. Live serving state in the
cluster attribute: needs an RPC per host, so a hung host would hang a
getxattr; `nest status` and `/v1/status` keep that.

## ADR-065 — The kernel reads ahead; the daemon's readahead is gone (2026-10-21)

**Context.** The daemon read files with copies elsewhere through its own
readahead (ADR-030): 4 MiB chunks kept in flight per handle, spread over
the copies, learning when a file is read randomly and stopping (ADR-031,
033). It was built for model weights. Two of its main users read in ways
it fits badly: the engine's tensor reads (33–35% of fetched bytes unused,
`benchmarks/M13-MODEL-LOAD.md`), and Engram lookups (a 4 MiB chunk per
4 KiB row until the file is judged random). Weights now load by push
(ADR-063). The kernel already reads ahead for FUSE files, follows
`madvise`/`fadvise`, and production mounts set `read_ahead_kb` to 16 MiB.

Measured over RDMA loopback on raptor, a remote sealed file read cold
through a real mount with `read_ahead_kb` = 16384:

| Daemon readahead | 1 GiB sequential | 2 GiB sequential | random 4 KiB |
|---|---|---|---|
| on | 2.39 GB/s | 2.32 GB/s | 66–71 µs |
| off | 2.40 GB/s | 2.67 GB/s | 58–59 µs |

With the kernel's default 128 KiB window the daemon's readahead was
faster (2.89 against 1.74 GB/s): the window, not the daemon, is what
matters, and every production mount has 16 MiB.

**Decision.** The daemon's readahead is removed, with everything that
existed for it:
- **Reads go out as asked.** A local copy is read from local disk (the
  fast path, or passthrough); a remote one is fetched exactly as asked
  from whichever copy is least busy (small reads on the completion thread,
  ADR-034), and the kernel's readahead decides what to fetch ahead.
- **The read-pattern detector goes** (it only judged when to stop reading
  ahead). Files' stored scattered flag stays in metadata (ADR-018) but no
  longer changes how they are read, and nothing sets it.
- **`passthrough = "sole"` behaves as `"always"`:** it kept local files
  away from the kernel only so their reads could be spread over other
  copies. `balance_reads` is accepted and ignored.
- **Status fields** that travel between hosts (`scattered_files`,
  `readahead_*_bytes`) stay, always 0, so hosts on either side of an
  upgrade read each other.

What is lost: a file on this host and others is read from the local disk
alone, not spread over every copy (one NVMe is 5–9 GB/s; weights load by
push). A remote-only file is still spread over its copies, per request.

**Alternatives.** A cluster setting to keep the readahead switchable:
measured and rejected, as it would keep 1,000 lines for a mode no workload
here wants. Raising the kernel's `max_pages` instead: needs a sysctl on
every host and helps only reads larger than 1 MiB.

## ADR-066 — Push reads and placement follow disk speed (2026-10-21)

**Context.** The Sparks' disks differ by two: rhea and moa read 13 GB/s,
ostrich, dodo, emu and kiwi 5.6 GB/s (raptor 12.3). A push load gave each
holder an even share of the bytes, so the fast disks finished in less than
half the time and the load ended on the four slow ones alone: the rate
held their sum (~22 GB/s) for its second half rather than the cluster's
(~48 GB/s). Where a model has one copy of each file, the hub has no
choice of holder at all, and an even spread makes the same tail.

**Decision.**
- **A push hub assigns each file to the holder that would finish it
  soonest** at its measured disk read rate (`disk_read_bps`, shared every
  30 s), largest files first; a holder with no rate counts at the median,
  and with no rates every holder counts the same (bytes balance, as
  before). With two or more copies this alone keeps every disk busy.
- **Place takes `by_speed`** (`nest plan --place … --by-speed`, a toggle
  in the Place dialog): each host's share of the bytes is sized by its
  disk read rate instead of evenly. It is per selection, so only the
  models where load time matters get uneven disk use. It changes nothing
  when every host holds everything.

**Alternatives.** Spreading every import by speed: disk use would go
uneven for files nobody pushes. Stealing work while the load runs (fast
sources take files from slow ones): needs a second copy anyway, which the
hub's choice already uses.

## ADR-067 — Bulk off the control connection: push slots on TCP lanes; copy progress within files (2026-10-21)

**Context.** Push slots that cannot go by RDMA (an off-grid source or
rank, an RDMA lane that failed, a ring the NIC could not register) went
as `PushMsg::Slot` RPCs on the control connection: 1–16 MiB messages
queued ahead of Raft heartbeats. Replication and reads never did (RDMA,
or TCP lanes: ADR-048, ADR-057). Separately, a replication job counted a
file's bytes only once the whole file was copied: with 8–10 GB shards and
eight files in flight per host, progress sat at 0 for minutes and then
jumped, and the user started more plans thinking the first ones were
queued.

**Decision.**
1. **Lane messages (lane protocol 3).** A lane frame `kind=5 | id | service
   u8 | len u32 | bytes`, answered as a write, hands a service's message to
   the `MessageSink` registered for it on the receiving host. The probe
   answer becomes 3; a peer that answers 2 or less is never sent one.
2. **Push slots take the lane.** A slot's fallback goes on a lane to the
   rank host when one takes messages (the standby lanes between fabric
   hosts, the lanes off-grid hosts dial), else on the control connection
   as before. A lane failure falls back to the control connection; a slot
   that may have arrived is safe to send again (its grant's `seq` drops a
   duplicate).
3. **Copy progress within files.** Each chunk a replication copies adds
   to a counter for its file; `JobProgress.partial_bytes` (appended,
   ADR-018) sums those of the files in flight when the status is read.
   The UI shows done plus partial, the percentage, a rate over the last
   few seconds and the time left. A host before this reports 0.
4. **Copies ride out a source that cannot serve.** A replication whose
   every source answers `Unavailable` (its lease lapsed while the cluster
   could not confirm it: on 2026-10-21 a burst of plans held leases off
   for two minutes and 109 files were skipped) waits and tries again, up
   to the read recovery wait or two minutes, whichever is longer, instead
   of skipping the file. The error says "not serving right now" when that
   is why, and a host logs when it stops serving and when it serves again.

**Alternatives.** A separate lane connection per service (more
connections and handshakes for one more frame kind); marking control
traffic with a higher DSCP class only (still needs the switch to map it,
and keeps bulk on one TCP stream with Raft; done on the switch side too).

## ADR-068 — Control traffic on the LAN, the fabric its fallback (2026-10-21)

**Context.** ADR-057 kept each fabric pair's control connection (Raft,
metadata, admin) on the fabric, with a LAN connection as an idle standby
used only when the fabric connection failed outright; slowness never
switched. The fabric switch (a CRS804, one egress queue, no pause) tail-
drops 1–5% under incast, and the control TCP shares that queue with RDMA:
on 2026-10-21 several placement plans at once dropped enough heartbeats
that the leader could not confirm leases for two minutes, and every host
stopped serving. ADR-067 took the last bulk data off the control
connection, so it is now only small messages; the 10G LAN switch carries
no RDMA.

**Decision.**
1. **A cluster setting, `control-on-lan`** (0/1, default 1): every host
   follows it within a second, live; `nest config set control-on-lan 0`
   puts control traffic back on the fabric. Path choice, not Raft timing
   (which stays in node config, ADR-056).
2. **On**, a peer's requests move to the LAN connection once it has
   answered probes for `return_after` (10 s); the fabric connection stays
   open, probed, and redialled if it drops. On the LAN, requests are
   watched as the fabric's are: one waiting `silence` (1 s) with nothing
   at all arriving falls back to the fabric, the LAN connection closed
   and dialled again; the LAN is taken again after `return_after` of
   answers. A LAN connection that closes falls back at once.
3. **Off**, ADR-057 as before.
4. Bulk data is unchanged: RDMA, and TCP lanes when RDMA is down.

**Amendment (2026-10-21, 0.6.2).** The preferred-leader handoff
(ADR-057) skips a peer whose link is degraded, and 0.6.0 took "requests on
the standby" to mean degraded. With the LAN preferred every peer is on the
standby, so the preferred leader never got leadership back after its
restart. Degraded now means on the fallback path: the standby when the
fabric is preferred, the fabric when the LAN is.

**Consequences.** Control latency rises by the LAN's round trip
(≈0.1 ms more), irrelevant at Raft's 150 ms heartbeat. A fabric outage no
longer moves control traffic at all. Traffic classes on the fabric switch
(control above inference above bulk) remain worth doing for the data.

## ADR-069 — Push: an archive is a source of its own; grants go to the source holding fewest (2026-10-23)

**Context.** ADR-063 item 4 lets a push load read a file from an archive
when no host holds it, but the hub resolved holders from hosts only: a
group whose plan named an archive-only file failed at Start with "no
live copy" (a GLM-5.3-Flash shard in October), and the engine had to
load it through the mount. Two more gaps sat behind that one:
- **One stream per host, not per store.** A source was a host; an
  archive's gateway (raptor, for `/mnt/scratch` and the NAS share) would
  have read archive files in the same single pass as its NVMe files,
  so the slow disk would hold up the fast one.
- **Round robin grants.** A rank host granted its incast budget round
  robin over the sources owing a rank. A source that fills its grants
  slowly (an archive disk, an off-grid host over TCP) keeps getting its
  turn while holding the ones it has, so within a second or two it holds
  nearly the whole budget and every fast source waits on its floor of
  one grant: one slow source slows the load to its pace.

**Decision.**
- **A source is a store.** Each stream reads one store: a host's own,
  or an archive, read by one of its gateways (the hub if it is one, else
  the first). A gateway runs its archive's stream beside its own, as
  another disk of the same host, each with its own read-ahead, slots and
  grants. Messages name the store (`Src {store, host}`): `Stream` says
  which store to read, `Credit` which stream it is for. The record and
  forecast list an archive by its name.
- **Nearest kind first.** A file is read from fabric hosts when any hold
  it, else from archives, else from off-grid hosts (over TCP lanes, as
  before); within a kind, the ADR-066 rule (soonest done at disk rate)
  picks. An archive has no measured rate and counts at the median.
- **Budget grants go to the owing source holding fewest** of the rank's
  grants (ties rotate). A slow source then holds an even share of the
  budget at most and streams at its own pace beside the rest; the floor
  still gives every source one grant.
- **Wire format.** `PushMsg` changed in place, which ADR-063's
  amendment allows: push is not run across versions.

**Alternatives.** Reading archive files after the gateway's own, in one
stream: the gateway's fast disk would wait on the slow one. A per-source
cap on grants: a fixed number is wrong for one source and for eight;
fewest-held adapts to however many there are.

## ADR-070 — Forgetting a lost host's copies (2026-10-26)

**Context.** oliver's disk failed (NTFS ran out of room for the VHDX's
extents) and the host is being rebuilt, its sparknest state not
recovered. The metadata still records 599 copies on it. Evicting a copy
asks its holder, which cannot answer, so the records stayed: reads and
plans treated oliver as a holder, and the last-copy check counted its
copy, so a later eviction elsewhere could have dropped the one real
copy (GLM-5.3-BF16 was left with only the `models` archive). `nest
cluster remove` changes only the Raft membership.

**Decision.** `nest cluster remove HOST --forget-copies` first retires
every copy recorded in HOST's store from the leader side, through the
ordinary `RetireReplica` (no new command), in batches. It is refused
while HOST answers (evict instead), and `RetireReplica` without
`allow_last` keeps any file's last live copy: the removal is then
refused, naming the count, so those files are removed (`nest rm`) or
restored first, a deliberate step. The host is removed only once
nothing was kept.

**Alternatives.** Dropping the records on `cluster remove` itself: a
host removed to be re-imaged with its disk intact would lose copies it
still holds. Retiring with `allow_last`: lost files would sit in the
namespace with no copy, failing every read instead of being absent.

## ADR-071 — Off-grid hosts on the LAN are dialled like any member (2026-10-28)

**Context.** ADR-047 had every off-grid host dial in, which only oliver
(WSL behind NAT) needed. kakapo and cassowary were registered the same
way, with no address in membership, and an off-grid host dials only
the fabric hosts in its `node.toml`. So two off-grid hosts had no
connection at all: a plan copying GLM-5.3-Flash-BF16 (held only on
kakapo and cassowary) to maxq failed every file with "no address, and
it has not connected here", after the two-minute copy wait for each. The
job counted each failed file's bytes as done, so its progress moved in
batches of files that had failed, and the plan ended with nothing on
maxq. Every host, fabric or not, is on the 10G LAN. Off-grid is about
how a host is shown and how copies pick sources, not about whether it
can be reached.

**Decision.**
1. **Registered at the LAN address.** An off-grid host on the LAN is a
   learner whose membership address is its LAN control address.
   `add-offgrid.sh join` registers it there; an empty address remains
   for a host behind NAT. `nest cluster role HOST offgrid --addr IP:PORT`
   sets it on a running host, without a restart (a voter stops voting
   first).
2. **Every host learns every address from membership.** A learner gets
   no Raft appends, so the leader's network factory never told the
   others where it is: each node now follows membership and sets every
   member's address (pinned `node.toml` addresses still win). Clearing
   an address still stops dialling it.
3. **Lanes between off-grid hosts.** An off-grid host also keeps lanes
   to the other off-grid hosts that have an address; of each pair the
   lower id dials, unless only one end has an address. Copy sources keep
   their order: fabric holders, then off-grid ones, then archives.
4. **Progress counts what landed.** A file that fails is listed in
   `failed` and not counted in `done_files` or `done_bytes` (replicate
   and backup jobs).
5. **No staging leftovers.** `commit_staging` removes its staging file
   when the rename fails (the object exists), as on every other error.
   The 10-26 GLM-5.3-BF16 copy to maxq had left 400 GB there.

**Alternatives.** Listing the other off-grid hosts in each `node.toml`:
works without a release, but every new host means editing every other
host's config and restarting it.

## ADR-072 — Deep fsck: shared-store blobs checked through their SHA-256 link, once (2026-10-28)

**Context.** `nest fsck --deep` hashed every object under a `/blobs/`
path whose name is 64 hex characters against that name. The shared
blob store (ADR-010) names a blob by its Xet hash, also 64 hex
characters, not by a SHA-256: every Xet-stored model file failed. A run
on maxq on 2026-10-26 reported all 184 GLM-5.3-BF16 copies there
corrupt; the two hashed by hand matched the SHA-256 their repo links
name. Only reported, nothing was retired, but `--deep -y` would have
dropped good copies wherever another copy existed. Each run also hashed
every blob again (about 1.5 TB for that model alone).

**Decision.**
1. **The right SHA-256.** A repo's own `models--…/blobs/<sha256>` is
   checked against its name. A shared-store `blobs/xx/<xet>` is checked
   against the name of the repo link pointing at it (one metadata query
   per run for all such links). A shared blob no repo links has no
   SHA-256 to check and is skipped. Xet hashes are not computed: Xet
   chunks and hashes them in a tree of its own, and the SHA-256 the repo
   names is the same guarantee, from the hub's own metadata.
2. **Each copy hashed once.** Objects never change once committed (a
   write makes a new generation), so a host records the copies it has
   verified in `<state>/fsck-verified` (`file.generation` per line,
   pruned to what is on disk). Later deep runs skip them; `--recheck`
   hashes everything again, for bit rot. A copy found bad is checked
   again every run until it is repaired; one that cannot be read is not
   recorded. The report gives `hashed` and `already_verified`.
3. Hashing drops the object's pages afterwards, as copies do.

## ADR-073 — Several disks per host: each extra disk is a store of its own (2026-10-29)

**Context.** raptor is getting two more 8 TB PCIe 5 NVMe drives beside its
3.7 TB system disk (90% full). Everything assumed one store per host, with
the store id equal to the node id: a copy's id named its host. A RAID0
stripe would hide the disks from sparknest but needs matching drives,
cannot take in the system disk, and loses every copy on it with either
drive. Push loads (ADR-063) already run one stream per store, and
placement by speed (ADR-066) already weighs sources by disk rate.

**Decision.**
1. **A disk is a store.** `nest disk add HOST PATH` registers a store of
   the new class `Disk` (`StoreClass` appended, ADR-018), with id
   `2^40 + (node << 8) + k`, `k` from a per-host counter that never reuses
   a number. Any host reads a copy's host from its id
   (`StoreId::host()`), with no lookup. The disk carries the same marker
   as an archive: an unmounted disk's empty mountpoint is never read or
   written. `DeleteStore` (appended) forgets a disk with nothing on it.
2. **A host holds a file once.** Copy counts, `@all` replication,
   readiness, plans and the UI count hosts; a disk is only where a host
   keeps its copy. A copy for a host goes on whichever of its stores has
   the most room after the copies already headed there
   (`Vfs::pick_store`), at most 8 transfers into each at once, so a job
   fills the disks side by side. A host with a copy on one disk is not
   given a second on another.
3. **Writes stay on the live store.** Working objects, ownership, the
   metadata database and the Raft log are on the host's live store as
   before; the data reserve (ADR-024) applies there only. Writing a file
   whose only local copy is on a disk first copies it into the live store
   on the host, then retires the disk's copy.
4. **Reads.** A host serves and reads its disk copies as it does its live
   store's: they rank as local, before any other host, and before an
   archive. Fences are per store, so a copy moving between a host's disks
   is never unservable.
5. **Push.** Each disk is a stream of its own, given files by its own
   measured rate (each disk is probed as the live store is). The planner
   sizes a host's share by its disks' rates together, capped at its link.
6. **Startup.** Each usable disk is reconciled against metadata: objects
   nothing lists are removed, listed copies missing from it are retired.
   A disk that is not mounted keeps its records and serves nothing until
   it is back.
7. **Drain and loss.** `nest disk drain NAME` moves a disk's copies to its
   host's other stores, on the host. `nest disk rm NAME` forgets an empty
   disk; `--forget-copies` first retires the copies of a lost one (never
   a file's last copy). `nest cluster remove --forget-copies` covers every
   store of the host.
8. **Seen as one host.** The host's info sums its stores' space and
   copies (as `statfs` does); `GET /v1/disks` lists each store and the UI
   shows them on the host's card and in the Place dialog. The API refuses
   `nest disk add` until every host runs 0.7.0, which decodes the new
   class and command.

**Alternatives.** RAID0 or LVM striping (above). Several live stores with
ids from a lookup table: every hot path that maps a copy to its host
would query it. Letting a host hold a file on two of its disks: twice the
space for no protection against losing the host, and every copy count
would need to tell hosts from disks.

## ADR-074 — A host's disks are places of their own, picked by name (2026-10-29; amends ADR-073)

**Context.** ADR-073 as first built kept "a host holds a file once" and
chose a disk for each copy by free space. TJ: nothing else in sparknest
places by itself, they want to choose which drive gets more and which is
left room, and a Place spread with several copies over a host's disks
gets confusing if hosts and disks count differently. 0.7.0 had not been
released.

**Decision.**
1. **A disk is a place, like a host.** It is named `raptor (D2)`, the
   host's own disk being D1 (`raptor:d2` is accepted too). Replicate,
   evict, Place, Consolidate, the free-space goal, rules and readiness
   take it wherever they take a host; the UI shows it as a host card
   and a Place tile of its own, and the Space view can scope to it.
2. **Nothing is automatic.** `raptor` means raptor's primary disk; a
   copy lands on D2 only when D2 is named or a plan over places that
   include D2 picks it. Free-space picking is gone. A drain moves a
   disk's copies to one disk named with `--to` (the host's primary by
   default).
3. **Copies count per place.** A spread over raptor, D2, D3 and the six
   Sparks puts each file on one of the nine; two copies over raptor's
   disks are two copies, both on raptor. No rule tells hosts from disks.
4. **Unchanged from ADR-073:** store ids naming their host, the marker,
   writes on the primary disk, local reads and push streams per disk,
   reconcile and the 0.7.0 gate. A host's info reports its primary disk
   only; `GET /v1/disks` gives each disk.

**Alternatives.** One copy per host with an automatic disk (ADR-073 as
first built): see Context.

## ADR-075 — Hub downloads pick places as Place does (2026-10-31; amends ADR-049 §4)

**Context.** After raptor's two 8 TB disks were added, TJ found the
Download dialog could not put a model on `raptor (D2)`, and its spread
left the off-grid hosts out. The request took host names only, and
ADR-049 §4 kept spreads fabric-only from when off-grid hosts were
dial-in only (since ADR-071 they are dialled like any member). TJ asked
for the same options as Place, without replicas.

**Decision.**
1. **Places, as in Place.** A download's `host`/`hosts` take places:
   hosts, a host's disks (`raptor (D2)`), off-grid hosts and `@groups`.
   Empty still means every serving fabric host. Each file is downloaded
   once and gets one copy: there is no replica factor.
2. **The dialog matches Place.** Multi host (fabric places, the hosts'
   own disks picked to start), Single host, Off-grid (a spread over the
   off-grid hosts picked) and Archive, as tiles with free space.
3. **Onto a disk by moving.** The Hub is fetched through the host's
   mount, so a file lands on its primary disk. A file for one of its
   extra disks is moved there (`MoveLocal`, as a drain) once its fetch
   finishes; only fetches in flight take room on the primary. A file
   that cannot be moved stays on the primary and the job says so.
4. **Room is the place's own:** a disk's free space, not its host's.

**Alternatives.** Writing straight into the chosen disk: faster, but it
changes where writes land (ADR-073 §4), so not for this.

## ADR-076 — The hub's refs stay true: sizing writes nothing, downloads write refs last, `hf repair` and `hf prune` (2026-10-31)

**Context.** TJ could not delete GLM-5.3-Flash-NVFP4: hf skipped the
repo ("not found"), and removing it host by host is refused (last
copies). hf ignores any repo whose ref names a commit with no snapshot,
and nine did. Three causes: (1) before ADR-043, Finish ran `hf download`
at the Hub's newest main, which rewrote `refs/main` though no file came
(five repos, all 2026-09-28 07:06–07:24); (2) hf's dry run, which the
Download dialog runs to size every repo picked, writes `refs/<branch>`
into the cache it reads, so searching for a model left a repo of nothing
but a ref (Muse-Spark, never downloaded); (3) a spread download wrote its
ref before fetching, so one that fetched nothing left the same.

**Decision.**
1. **Sizing resolves a branch to its commit on the Hub first** (no cache
   read) and gives hf the commit, where it writes nothing. A test checks
   sizing leaves an empty hub empty.
2. **A download writes its ref last,** and only once its snapshot exists.
3. **`nest hf repair`** (`POST /v1/hf/repair`) makes refs true from what
   the hub holds, never the Hub (whose main may have moved on): a ref to
   the repo's only snapshot when every ref is stale; a stale ref dropped
   beside a good one; a repo of nothing but refs removed; anything else
   (several snapshots, files without a snapshot) only reported. Model
   files are never touched. It shows the changes; `--apply` makes them.
4. **`nest hf prune`** (`POST /v1/hf/prune`) runs `hf cache prune`:
   revisions no ref points at and incomplete downloads go, with blobs only
   they used. hf decides what is still linked; sparknest removes each
   deleted file's copies at once, as for any deletion (no GC of its own,
   PROPOSAL §11). The Models view's "Tidy hub…" runs repair then prune,
   each shown and confirmed first.

**Alternatives.** Taking refs from the Hub: claims a commit whose files
the hub may not have. A delete that falls back to removing folders when
hf cannot see a repo: once repair makes refs true, hf sees and deletes it,
keeping the shared store's bookkeeping; a fallback would bypass that.
