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
