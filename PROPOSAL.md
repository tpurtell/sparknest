# sparknest — Official Architecture Proposal

Status: **accepted baseline for implementation** (2026-09-26).
Supersedes `docs/history/initial-proposal.md` where they conflict.
Decisions with alternatives are recorded in `docs/DECISIONS.md`; the concrete
machine inventory is in `docs/ENVIRONMENT.md`; sequencing is in `ROADMAP.md`.

## 0. What sparknest is

A **coherent, whole-file distributed filesystem with explicit placement** for a
small trusted cluster on a fast RoCE fabric. It is built to host a shared
Hugging Face cache across `raptor` (amd64, 400G) and six DGX Sparks (arm64,
2×100/200G), mounted at `/mnt/sparknest` everywhere, with `~/.cache/huggingface`
symlinked into it. Secondarily it is a good general-purpose shared folder.

The user-visible promise:

- One namespace on every node. Files live in full on the node that created
  them, plus wherever a **rule** says to replicate them. Nothing replicates
  because you read it.
- Reads of a file you do not hold locally stream over RDMA at fabric speed.
  Reads of a sealed local file are kernel passthrough at NVMe speed.
- Writes are coherent: first mutation picks one owner, everybody else routes
  to it, stale copies are removed immediately.
- Placement, free-space planning, backup/archive/recall, and HF-aware
  operations are driven through one management API used by both the `nest`
  CLI and the web UI.
- Archive stores (the SMB NAS, the SATA scratch disk) are ordinary folders
  reached through gateway nodes; sparknest routes the bytes over the fabric so
  a Spark never has to talk SMB/NFS itself.

Non-goals (first release): general POSIX for shared writable mmap, multi-user
uid/gid semantics, encryption on the data plane, content checksums, any form of
chunked/block storage.

## 1. Cluster and deployment shape

| Node | Arch | Fabric | Live store | Role |
|---|---|---|---|---|
| raptor | amd64 | 1×400G RoCE (`mlx5_0`), 10.55.0.22 / 10.55.1.22 | root NVMe (9100 PRO 4T) | always-on, HF download host, gateway for `/mnt/scratch` |
| ostrich, dodo, kiwi, emu, rhea, moa | arm64 | one ConnectX cable exposing two RoCE functions (rail 0 `10.55.0.N`, rail 1 `10.55.1.N`); switch currently at 100G per rail for fan noise, 200G available for benchmarks | root NVMe 4T | compute nodes; reboot/re-image often |
| aviary (NAS) | n/a | 10G, SMB | `/mnt/models` on all nodes | archive store, multi-gateway |
| raptor `/mnt/scratch` | SATA SSD 7.3T (NTFS now, ext4 later) | via raptor only | archive store, single gateway |

One binary, `sparknestd`, runs identically on every node (systemd unit, runs as
the user). It contains: FUSE frontend, metadata (Raft + SQLite), local object
store, fabric transport, data service, placement/HF engine, and the management
API. There are no separately deployed services. Any node may also run with no
live store (pure client / learner) later; not required for v1.

`nest` (CLI) and `web/` (SPA) are separate programs that speak **only** the
management API. The daemon may serve the built web bundle for convenience, but
the UI has no privileged path into the daemon.

Single-tenant by design: every file is presented as the mounting user's
uid/gid on each host (raptor is uid 1000, Sparks are 1001; we store mode bits,
not owners). Trust boundary is the fabric VLAN plus a shared cluster secret for
the control plane.

## 2. Consistency contract

| Property | Guarantee |
|---|---|
| Namespace | create/unlink/link/rename/mkdir/rmdir/setattr are serialized through Raft; atomic cluster-wide. |
| Namespace reads | Served from the local applied state with **bounded staleness** (≤ one heartbeat, ~100 ms worst case, typically sub-ms) plus kernel dentry/attr caches that the daemon invalidates on apply. Operations that need exactness are writes and go to the leader anyway. |
| Data reads | **Never stale.** Every data request carries `(file_id, generation)` and, for mutable files, the ownership epoch. A source refuses a request for a generation that is not current; the client refreshes and retries. Stale namespace cannot yield stale bytes. |
| Visibility | A completed write is visible to subsequent reads on any node without waiting for close (all readers of a file under mutation are routed to the owner). |
| Concurrent writers | Allowed; all writers of a mutable file submit to its single owner, which orders them. |
| Durability | `fsync` = data durable on the owner's local disk and the associated metadata committed. Redundancy only via a rule-driven replica or backup. |
| Locks | `flock`/`fcntl` advisory locks are cluster-wide (a Raft-managed lock table with session-bound leases). Required because HF's `filelock` coordinates concurrent downloads with them. |
| Quorum loss | Minority side: no mutations (EROFS-style errors), no new ownership. Reads of **sealed** files with a local replica continue from local applied state; everything else fails fast rather than lying. |
| Sole holder down | That file is unavailable (EIO after timeout); other files unaffected. No promotion of stale generations. |

## 3. Metadata: Raft over SQLite

### 3.1 Why consensus, and which one

The requirement is that when a few nodes are down (including raptor), the
others keep a correct namespace and can serve the files they hold. That is a
replicated state machine problem; the ordering needs for rename, ownership
hand-off and invalidation rule out leaderless/CRDT designs, and a
raptor-primary design fails the requirement outright. Alternatives considered
are in `docs/DECISIONS.md` (ADR-002). Summary of the choice:

**openraft** (Rust, Databend's Raft) with our own log storage, state machine
and network implementations. Decisive reasons:

1. We need **apply-time hooks on every node**: when an `InvalidateReplica`
   commits, the node holding that replica must start deletion inside the
   apply path, immediately, with no job engine involved. openraft's
   `RaftStateMachine::apply` gives exactly that. Replicated-SQL products
   (hiqlite, dqlite, rqlite) hide apply and would force polling/diffing.
2. Our commands are semantic and conditional (`AcquireOwner` must fail if the
   file is already owned in a newer epoch). A typed command enum applied
   transactionally is far easier to reason about and test than replicated SQL.
3. It provides the parts that are genuinely hard to get right (joint
   membership change, snapshot/install, log compaction, linearizable read
   index, leader lease) and is exercised in production.
4. Pure Rust and async; fits the single-binary daemon; no sidecar.

`raft-rs` (TiKV) is the credible runner-up: equally trusted, but it is only the
algorithm core with a synchronous tick-driven API; we would write more glue
(storage, network, driving loop, snapshot orchestration) for the same result.

Membership: default **all seven nodes as voters** (majority 4, tolerates any 3
down, including raptor). Voter count is configuration, not code; learners are
supported so a node that is often offline or has no live store can follow
without voting. The API exposes `cluster add/remove/promote`.

### 3.2 State layout per node (`/srv/sparknest` by default, configurable)

```text
/srv/sparknest/
  node.toml            identity, cluster secret, fabric prefs, store roots
  meta.sqlite          replicated state machine materialization (WAL)
  raft.sqlite          Raft log + hard state + membership (WAL)
  objects/             live store: xx/<file_id>.<generation>
  staging/             incomplete whole-file transfers (never advertised)
  intents.sqlite       local crash-recovery journal (create/publish/delete/rename intents)
  snapshots/           Raft snapshots = VACUUM INTO copies of meta.sqlite
```

Log and state machine are separate SQLite files so log fsync cadence and
state-machine batching are independent. `last_applied` is committed in the
same transaction as the applied commands. Snapshots use SQLite's backup/VACUUM
INTO at a known applied index; log compaction follows.

### 3.3 Entities (schema owned by `nest-meta`)

- **file** — `file_id`, kind (reg/dir/symlink), mode, times, nlink, size,
  `current_generation`, `sealed`, `policy_flags`.
- **dentry** — `(parent_id, name) -> file_id`. Hard links are multiple dentries.
- **generation** — `(file_id, gen)`, settled size, state
  `{STABLE, REVOKING, OWNED, FINALIZING}`, owner node, ownership epoch.
- **replica** — `(file_id, gen, store_id)`, state `{STAGING, LIVE, INVALID}`.
  Always a complete file. There is no partial residency anywhere in metadata.
- **store** — id, class `{live, archive}`, node(s) acting as gateway, root,
  capacity domain, capabilities (rename, symlink, hardlink, fsync semantics).
- **lock** — file, range, type, holder session, lease expiry.
- **session** — node + client identity + lease; owns handles and locks.
- **rule / manifest / plan / job** — placement (§7).
- **backup** — retained versions (§8).

Command enum examples: `Create`, `Link`, `Unlink`, `Rename`, `SetAttr`,
`AcquireOwner`, `FinalizeGeneration`, `Seal`, `PublishReplica`,
`InvalidateReplica`, `RetireReplica`, `Lock/Unlock`, `SessionHeartbeat`,
`ImportBatch`, `RuleUpsert`, `JobTransition`. Commands are validated at apply
against current state; the proposer receives the apply result. Bulk operations
(import, replication publish) are batched into single log entries.

### 3.4 Read path for the namespace

FUSE `lookup/getattr/readdir/readlink/open(O_RDONLY)` read local `meta.sqlite`
directly (prepared statements, no leader round trip). Kernel caches are enabled
with a short TTL (default 1 s) and the daemon issues `notify_inval_entry /
notify_inval_inode` when it applies a command touching a cached object. A
`nest fs barrier` API (read index) exists for tooling that needs a
linearizable view, and `open` for write uses it implicitly because it goes to
the leader.

## 4. Local object store (`nest-store`)

Objects are stored **by identity, not by path**: `objects/xx/<file_id>.<gen>`.
Renames and hard links are therefore pure metadata; offline nodes have nothing
to replay in their backing tree; deletion targets a specific `(file_id, gen)`
instance so a late callback can never remove a newer generation. This departs
from the original musing of a human-browsable backing tree; the mount itself is
the browsable view, and `nest export` / `nest recover` can materialize any
subtree from `meta.sqlite` plus objects without the cluster being up. See
ADR-005.

Every filesystem-side action that must agree with metadata is journaled in
`intents.sqlite` first (create object, publish, invalidate/delete, rename from
staging). Startup replays intents and reconciles inventory against
`meta.sqlite` **before** the node advertises any replica.

**Import/adopt** is a first-class operation: `nest import --adopt SRC DST`
moves files into the object store with `rename(2)` when on the same filesystem
(zero copy; this is how the existing 2.3 TB `hub/` on raptor and the 1–1.7 TB
caches on each Spark become sparknest content in seconds). Adopting the same
logical path from several hosts with equal size registers them as replicas of
one generation only under an explicit `--trust-equal-size` flag (we do not
hash; HF blob names are content addresses HF already verified).

Invalidation → deletion is direct: on applying `InvalidateReplica` for a local
object, mark ineligible, fence in-flight data sessions on that generation,
write a delete intent, spawn the unlink on the blocking-I/O pool. No debounce,
no GC pass, no job. Open descriptors keep bytes alive until closed, as normal.

## 5. Fabric transport (`nest-fabric`)

Ported in Rust from the proven design in `rdmapipe/transport.c` and
`rdmasync/rdma.c` (same author, measured at 167 Gb/s Spark↔Spark dual-rail and
~95 Gb/s single rail at 200G):

- Discovery from `/sys/class/infiniband`: active Ethernet-link-layer ports with
  an UP IPv4 netdev and matching RoCE v2 GID. No hostname logic.
- **Persistent** RC QPs between every node pair, established at join and
  re-established on failure; one QP per rail, auto rail selection (raptor's one
  400G port pairs against both Spark rails; Spark↔Spark maps distinct devices).
  On a Spark both rails are functions of one physical cable, and **dual rail is
  mandatory for throughput**: one rail is PCIe-capped near 116 Gb/s regardless
  of link speed, two rails reach ~195 Gb/s at 200G. Every Spark data stream
  therefore stripes across both rails by default.
- Registered memory: per-connection rings of fixed slots (start at 2 MiB ×
  depth 8, benchmark before changing), plus a node-wide registered pool budget.
- Bootstrap over the TCP control connection (QPN/PSN/GID/MTU exchange, shared
  secret + per-session token), then verbs only.
- A **TCP transport implementing the same trait** for tests, CI, and machines
  without RDMA. The correctness core is developed and tested against it.
- Bulk plane has no encryption or application checksum (trusted fabric, as
  with rdmasync). Control plane is authenticated.

Two message classes share a connection: small control RPCs (open/close
sessions, read-range requests, write submissions, revocations) and bulk payload
frames carrying `(session, request_id, offset, len)` headers. Start with the
SEND/RECV ring model that is already benchmarked; evaluate one-sided RDMA
WRITE into the requester's registered slots as an optimization once the ring
version is measured.

Raft RPC and management API use plain TCP (tonic/gRPC for Raft, HTTP for
management) on the fabric IPs; they are low-rate.

## 6. Data service and FUSE data paths (`nest-data`, `nest-fuse`)

### 6.1 File lifecycle

```text
STABLE(g) --first mutation--> REVOKING(g) --> OWNED(g+1, owner, epoch)
        --all writers closed & flushed--> FINALIZING(g+1) --> STABLE(g+1)
```

- `open(O_RDWR)` alone changes nothing. The first `write`, `truncate`,
  `O_TRUNC`, `fallocate`-style size change acquires ownership.
- Owner selection: writer if it holds a current replica; otherwise a current
  replica holder (do not move a 40 GB file to make a 1 KB edit); the creating
  node for new files.
- Acquire = Raft `AcquireOwner` → all other replicas `INVALID` (deletion
  starts on those nodes at apply) → read grants for gen `g` revoked
  (ack or lease expiry) → owner may mutate. Physical deletion is never on the
  writer's critical path.
- While `OWNED`, readers and writers everywhere route to the owner over the
  fabric. Requests carry session, op id, epoch; uncertain outcomes are resolved
  from owner-side op state or surfaced as errors, never replayed blindly.
- `FinalizeGeneration` when the last participating writer releases; sets
  settled size; the owner's object becomes the sole `LIVE` replica of `g+1`.
- **Seal**: an explicit, enforced immutability bit. In-place mutation of a
  sealed file is `EPERM`; replacement via rename of a new file is fine. The HF
  plugin seals a blob when the downloader renames `*.incomplete` to its final
  name (the download boundary in `huggingface_hub`); users can seal anything
  via `nest seal`. Sealing is what unlocks the fast paths below.

### 6.2 Read paths

| Situation | Path |
|---|---|
| Sealed, local replica | **FUSE passthrough** (`fuser` BackingId, kernel ≥ 6.9 on all nodes, both have `CONFIG_FUSE_PASSTHROUGH=y`). Reads and read-only mmap hit the backing NVMe file directly; the daemon is out of the loop. |
| Sealed, remote | FUSE with kernel page cache enabled; daemon serves `read` from a bounded, generation-fenced RDMA prefetcher (demanded range first, adaptive sequential read-ahead, one source per stream with failover to another `LIVE` replica). Read-only mmap works because pages come through the kernel cache. Whole small files are fetched in one request. Prefetched bytes are RAM/page cache, never a replica. |
| Unsealed, stable | Same as sealed paths but under a revocable read grant; cached kernel pages are dropped on revocation (`notify_inval_inode`). |
| Under mutation | Direct-I/O to the owner (no kernel caching), all nodes including the owner's own FUSE mount. |
| Archive-only | Streams through a gateway node exactly like a remote replica; the gateway does the SMB/NFS/local I/O. |

Budgets are node-wide: registered memory, in-flight ranges, read-ahead bytes,
outstanding replies. Loading 60 shards concurrently must not multiply
allocations.

### 6.3 Write path

Daemon-mediated, FUSE writeback cache off initially. Owner appends/pwrites to
its object, tracks size and mtime, replies. Remote writers ship data over the
fabric to the owner. `flush`/`fsync` are real: fsync the object, then commit a
`SyncPoint`/size update if required. Large application writes are not one
distributed transaction; the atomicity unit is the fabric request.

### 6.4 Model-loading reality

Inference engines here (glmrt, vLLM, exllama) load safetensors via mmap or
large sequential reads. Passthrough makes the local case native. For the remote
case, a Spark reading from raptor over 2×100G (~20 GB/s of fabric) can plausibly
outrun its own NVMe; replication is therefore about availability and avoiding
fan-in on raptor, not raw speed. The prefetcher must handle mmap fault
patterns (kernel readahead windows of 128 KB–2 MiB) by running well ahead of
the faulting position.

## 7. Placement engine and HF plugin (`nest-place`)

Three objects, kept separate: **rule** (desired), **manifest** (resolved file
ids + generations + link structure), **plan** (operations to converge).
Rules are the only authorization to create copies. Overlapping rules union
their requirements; removing one never removes a copy another still needs.
Eviction (remove a copy) and unlink (remove a name) are different operations,
and HF's own delete-cache tooling run through the mount deletes cluster-wide,
by design.

### 7.1 Hugging Face cache layout policy

**Normative layout is the huggingface_hub ≥ 1.32 shared blob store** (current
release line is 2.x; enabled by default, opt-out `HF_HUB_DISABLE_SHARED_BLOBS=1`).
Under `hub/`:

```text
hub/blobs/.huggingface-shared-blobs        marker; hf never adopts an unmarked blobs/ dir
hub/blobs/<xx>/<xet_hash>                  one payload per Xet file, shared across repos
hub/blobs/<xx>/<xet_hash>.refs             append-only hint listing repo blobs that reference it
hub/models--org--name/blobs/<etag>         relative symlink -> ../../blobs/<xx>/<xet_hash>  (Xet files)
hub/models--org--name/blobs/<etag>         regular file (small non-Xet files, git-sha etags)
hub/models--org--name/snapshots/<commit>/<path> -> ../../blobs/<etag>
hub/models--org--name/{refs/<branch>, trees/<commit>.json, .no_exist/}
hub/.locks/models--org--name/<etag>.lock   filelock (fcntl) used during downloads
```

Reading is layout-agnostic: the resolver follows any symlink chain inside the
hub to a terminal regular file and builds the dependency manifest on terminal
`file_id`s, so legacy repo-local blobs (every existing cache on raptor and the
Sparks), the shared store, and no-symlink copies all resolve. `trees/<commit>.json`
(path, size, hash per file at that commit) is the preferred source for the
intended selection and completeness check; the Hub API is the fallback.

Import adopts legacy caches **as they are**. sparknest never rewrites a
repo-local blob into the shared store: that would require the Xet hash, which
only the downloader knows (ADR-008, no hashing). Legacy content is still
deduplicated for placement and accounting by shared terminal identity within a
repo, and across hosts by path+size adoption. New downloads land in the shared
store because downloads use huggingface_hub 2.x (the `hf` CLI is 2.0.0 via
Homebrew; 1.32 is the floor for the layout, 2.0 changed only the HTTP stack).

Download mechanics that shape the FUSE/write design (from `file_download.py`):
`hf_xet` writes **directly into `<blob>.incomplete` with parallel random-offset
writes from several threads**; this is the dominant write workload and the
owner-routed write path must sustain multi-GB/s of concurrent `pwrite` to one
file on the download host. Completion is `os.replace(incomplete, blob)`, then
for Xet files `publish_blob_to_shared_store` moves the blob into
`hub/blobs/<xx>/<hash>` (another rename), appends and fsyncs `.refs`, and
creates the repo symlink. **Seal rule:** a regular file under any `blobs/`
directory that is renamed from a `*.incomplete` name is sealed at that rename.
Subsequent renames are metadata-only, which is exactly why objects are stored
by identity (ADR-005). `.refs` and `refs/<branch>` stay ordinary mutable files.

huggingface_hub probes capabilities with `os.symlink`, `os.link`, `os.replace`
and **silently falls back to repo-local storage on any failure**; M5 must
prove, on a trial mount, that a real download ends in the shared store. A model
is *ready* on a host only when every file of the resolved selection is sealed
and `LIVE` there. Reconciliation runs on seal/rename events with debounce at the
model level. `hf cache rm` / `prune` through the mount delete cluster-wide by
design (eviction is the API operation for local space). HF's mutable scratch
(`xet/` chunk cache, tool caches) stays on local disk by configuration.

Whole-file replication: reserve capacity → read grant on the stable generation
→ copy to `staging/` → verify expected `(file_id, gen, size)` and completion →
fsync per store policy → `PublishReplica` → release. Replication, migration,
backup, archive, recall share this engine. Write-triggered invalidation does
not.

Free-space planning models **capacity domains** (raptor root NVMe, each Spark
NVMe, the NAS share, the scratch disk), reservations, staging, bytes pending
reclaim, targets and emergency reserve. Two operations: **reconcile** (satisfy
rules) and **optimize** (propose rule changes to hit free-space targets; user
approves; plan executes copy-before-evict with revalidation before each
destructive step). Deterministic greedy planner with explicit costs; seven
nodes do not need a solver. Marginal reclaimable space is computed over the
shared-blob dependency graph.

## 8. Stores, backup, archive, recall

A store is a folder plus gateway node(s), capacity domain, and capabilities.
`/mnt/models` (SMB) is one store with all seven nodes as gateways; the planner
picks a gateway by load and never counts two gateways as two copies. raptor's
`/mnt/scratch` is a store with raptor as the sole gateway; Sparks reach it via
the fabric, not via their NFS mount. Store health is validated by a marker file
and mount identity so an unmounted path is never mistaken for storage.

- **Backup**: retained, versioned, immutable recovery records (versioned
  folder tree + SQLite manifest of paths/links/ids/generations/sizes). Later
  writes and unlinks never remove them; retention is explicit config.
- **Archive/offload**: the current namespace may have zero online copies if
  its current generation is committed to a reachable archive store. Reads
  stream through a gateway; **recall** creates online replicas on demand or
  by rule.
- Invalidated live replicas are never retained as implicit backups.
- Metadata backup: periodic `meta.sqlite` snapshot at a known applied index
  to an archive store; the DR test restores namespace + objects without the
  original cluster.

User plan for context: the existing `/mnt/scratch` NTFS content migrates into
sparknest, the disk is reformatted ext4, then registered as an archive store.

## 9. Management API, CLI, web

Every node serves the same versioned HTTP+JSON API (axum) on a Unix socket
locally and on the fabric IP with bearer auth; local reads answered locally,
mutations forwarded to the leader. Events via SSE. The OpenAPI document is the
contract and is checked in; the CLI, the web UI, and the integration tests
consume it.

Resources: `cluster`, `nodes`, `stores`, `fs` (stat/ls/barrier/seal/import),
`replicas`, `rules`, `manifests`, `plans`, `jobs`, `backups`, `locks`,
`events`, `metrics`. Mutations carry idempotency keys and expected revisions.
Destructive calls address `(file_id, gen, store)` instances.

`nest` CLI shape (illustrative):

```bash
nest status                                   # quorum, nodes, stores, capacity
nest ls -l /hf/hub/models--Qwen--Qwen3.6-35B-A3B-FP8   # per-host completeness
nest import --adopt ~/.cache/huggingface/hub /hf/hub
nest rule set qwen36 --hf Qwen/Qwen3.6-35B-A3B-FP8 --revision main --hosts raptor,@sparks
nest reconcile qwen36 --wait
nest plan --free raptor=800GiB --free @sparks=400GiB ; nest plan apply <id>
nest store add nas --gateway @all --path /mnt/models/sparknest --class archive
nest backup create qwen36 --store nas ; nest offload <rule> --store nas ; nest recall <rule> --hosts @sparks
nest bench read /hf/hub/... --from ostrich        # built-in benchmarks
```

Web UI: TypeScript SPA in `web/`, built with Vite, talking only to the API.
Views: cluster/capacity, per-model readiness matrix (hosts × required files),
rules, plans with live progress, stores/backups, events. Build output is
optionally embedded in `sparknestd` for zero-step deployment.

## 10. Code organization (Cargo workspace)

| Crate | Responsibility |
|---|---|
| `nest-types` | ids, enums, wire/API DTOs shared everywhere |
| `nest-meta` | schema, command enum, deterministic apply logic; unit-tested without I/O |
| `nest-raft` | openraft storage/state-machine/network adapters, snapshots, membership |
| `nest-store` | object store, intents journal, staging, deletion, inventory reconcile, import/adopt |
| `nest-fabric` | transport trait; RDMA (verbs FFI) and TCP implementations; rails; budgets |
| `nest-data` | sessions, read grants, ReadRange server/client, prefetcher, owner-routed writes, transfer engine |
| `nest-fuse` | fuser-based frontend, inode/handle tables, passthrough, cache invalidation, locks |
| `nest-place` | rules, manifests, HF resolver, planner, jobs, stores, backup/archive/recall |
| `nest-api` | axum server, OpenAPI, SSE, auth |
| `nest-client` | Rust API client used by CLI and tests |
| `sparknestd` | daemon binary, config, wiring, systemd integration |
| `nest` | CLI |
| `nest-testkit` | in-process multi-node cluster harness (TCP fabric, temp stores, optional real FUSE) |
| `web/` | SPA (TypeScript) |

Language is Rust throughout, with `unsafe` FFI confined to `nest-fabric`'s verbs
bindings. Rationale and rejected alternatives: ADR-001.

## 11. Invariants the implementation must keep

1. Metadata never contains partial-file residency, chunk maps, or hashes.
   Replicas are complete files or they are not replicas.
2. Reading never creates a replica. Only rules/plans/imports do.
3. Data requests are generation-fenced; a stale namespace can never return
   stale bytes.
4. First mutation → single owner → others invalid → deletion starts in the
   apply path. No GC, no debounce, no job for this path.
5. Passthrough/mmap fast paths only for sealed files; sealing is enforced.
6. Local object identity is `(file_id, gen)`; a late delete can never hit a
   newer object.
7. Every cross-layer (SQLite ↔ filesystem) step is intent-journaled and
   replayed at startup before advertising.
8. An unavailable store is never an empty directory to write into.
9. Budgets are node-wide and enforced; opening many shards is O(1) in
   registered memory.
10. Quorum loss degrades to read-only sealed-local service, never to a
    divergent namespace.

## 12. Open questions to settle by measurement (not by design)

- Slot size/depth and rail policy for the file-server pattern vs the stream
  pattern rdmasync tuned for; SEND/RECV rings vs one-sided WRITE.
- FUSE read size (`max_read`/`max_readahead`) and thread model for the remote
  path; whether FUSE-over-io_uring (both kernels have `CONFIG_FUSE_IO_URING=y`,
  `fuser` does not support it yet) is worth implementing after passthrough and
  RDMA are in place.
- Whether follower namespace reads need the read-index barrier anywhere in
  the FUSE path beyond `open` for write and locks.
- openraft line: 0.9 stable vs latest 0.10 alpha (spike, then pin).
- Exact HF downloader behavior (1.24–1.30, xet-backed) around `.incomplete`
  → rename → symlink and lock files; where to seal.
- Whether a Spark reading from raptor beats its own NVMe; this shapes default
  placement advice.
