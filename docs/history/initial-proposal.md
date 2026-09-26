# Distributed FUSE Filesystem: Original Request and Proposal A

## Original request

I want to build a FUSE based distributed filesystem that I am ultimately going to use to host my hugging face cache in a fully distributed fashion across my whole RDMA capable network.  My super server and 6 sparks will all leverage a folder on it as a shared caching space for hugging face.  But I guess it will be generally useful to me for all kinds of storage since it's a simple thing to use, however the properties are intended to maximize read performance and support proper consistency with extreme simplicity on top of that.

The backing store data for the filesystem will simply be a folder containing the files and directory trees it contains that happen to be on that host.  All metadata will be contained in an optimized sqlite database. &#x20;

Filesystem state would be managed in a consensus manner so that even if a few nodes were down, the other ones could access normally things that were stored in their own storage.

Rough general principle
When writing to a file, one owner would be established others replicas would be evicted (probably should happen lazily at the first actual write rather than open).  additional writer could be allowed based on the filesystem spec but then their writes would be directed to that owner until all the writes were closed.  Reads during this times would ovciously be directed to that host

When reading files, they could load from the local full replica.  If its not local, it would begin high speed rdma prefetching of the file on first read (not to replicate just to do optimized loading assuming these are large files that will be read in huge chunks)...  targeting the performance of [https://github.com/tpurtell/rdmasync](https://github.com/tpurtell/rdmasync)  small files will effectively be read entirely. &#x20;

all nodes of the service will serve a protocol that allows for visualization and management of the storage space.  This interface would allow triggering a replication for particular folders (intelligent content aware in particular for the hugging face model folder which has a special layout of shared blobs.   Adding files essentially always consumes local disk space only and no replication happens automatically, only through commands triggered by the web interfaces,  there would also be a command line tool to control replicas, convenient for preparation of using a particular model, to set replication set X to keep HF model or folder Y on host set Z.  The web interface would basically work through these same rules.  obviously writes can cause that to invalidate, so there would be a process/command that can apply the replication rules.  e.g. after hf update some model, i might run it since it changed shards, but the system can also schedule it automatically as file writes fully end. &#x20;

The front ends should also allow a storage management planning that lets you adjust the desired  free space to on the hosts and let it make a new plan for the replica definition and apply that plan, giving status along the way.

I also want the front end to support "backup" stores handling HF models or folders that we want to have a safe copy of OR if we want to completely offload that.  These would be ordinary folder definitions.  In my case I would use my nas over SMB and a giant but slow SSD in one of my workstations, basically these would be network shares for the most part.  Again the utilties and web frontend would need to speak the protocol to whatever node they run on to accomplish the tasks above and these tasks.

Make me a more complete design for this.

---

## Proposal A — revised

This is a proposed architecture for further refinement and implementation. The original request above is the product brief; the following clarifications supersede any conflicting choices in the earlier proposal.

### Revisions incorporated

1. **An inode is only a file's stable identity and metadata.** This design does not store file-block maps, chunk placement, or partial-file residency in the distributed metadata. A replica is always a complete file. Byte ranges used for ordinary I/O and temporary RDMA buffers are not storage-placement metadata.
2. **No file digests and no application-level checksums.** Do not hash file contents, add checksum fields to the transfer protocol, or scan files to validate their contents. Completion, length, generation identity, and successful I/O are the basis for accepting transfers. The filesystem does not add or attempt to disable checks inherent to the underlying network or storage hardware.
3. **Invalidation immediately initiates asynchronous deletion.** An invalidated live replica is removed from read eligibility immediately, and its local invalidation handler starts deletion immediately. This is not a management job, reconciliation task, periodic garbage-collection pass, or retention policy. Writers do not wait for physical deletion, although they must wait for the required read-authority revocation.

### 1. Architectural model and consistency contract

Build a **coherent, whole-file distributed filesystem with explicit placement**, not a distributed block store.

New files consume storage on the creating node. Stable files may have multiple complete replicas. The first mutation of an existing file establishes one owner; every subsequent writer and concurrent reader uses that owner until the write epoch ends. Explicit placement rules determine whether additional complete copies should later be created.

Consensus governs the namespace, ownership, and replica validity. It does not carry bulk file data and does not provide redundancy for a file stored on only one node.

| Property | Proposed guarantee |
|---|---|
| Namespace | Creates, unlinks, hard links, renames, and authoritative namespace changes are serialized through consensus. |
| Read/write visibility | A completed ordinary write is visible to a subsequent read, including on another node. Visibility does not wait for the writer to close. |
| Concurrent writers | Permitted where supported by the filesystem contract; all operations on one mutable file are ordered by its owner. |
| Stable reads | Any complete, valid replica of the current generation may serve the file under valid read authority. |
| Durability | `fsync` makes the relevant data durable on its owner and commits the associated durable metadata. Redundancy requires a separately completed replica or backup. |
| Partitions | Only the quorum side can make authoritative changes or grant new ownership. A minority cannot continue as an independent live filesystem. |
| Data availability | A file is available only while an appropriate current copy is reachable. Losing its only copy does not make unrelated files unavailable. |

For the super server and six Sparks, start by evaluating **one seven-voter Raft group**. A majority is four, so any three node failures can be tolerated for metadata progress. This is a proposed deployment choice, not a requirement that the implementation hard-code seven members. A smaller voting group trades lower consensus overhead for different failure tolerance.

With quorum available, local data reads should not be relayed through the metadata leader. With quorum unavailable, the service must not pretend its local namespace is necessarily current. Continuing already-authorized operations on immutable open files can be handled separately from serving new authoritative namespace operations.

Do not advertise unrestricted POSIX compatibility before it exists. Shared writable mappings across nodes are out of scope for the first implementation. Read-only model-loading mappings are an important target, with an explicit coherence strategy rather than accidental cache behavior.

### 2. One daemon per node; SQLite for metadata

Each node runs the same daemon, with these internal components:

| Component | Responsibility |
|---|---|
| FUSE frontend | Namespace operations, permissions, file handles, and selection of the correct data path. |
| Metadata service | Consensus participation and a local SQLite materialization of committed namespace and control state. |
| Data service | Backing-file access, owner-routed I/O, persistent RDMA sessions, and full-file transfer publication. |
| Placement and backup engine | Explicit replication, migration, backup, archive, recall, and free-space plans. |
| Management API | The common interface used by the CLI and web frontend. |

These should initially be modules in one process, not separately deployed services. The metadata leader is never a bulk-data proxy.

A suggested node layout is:

```text
/srv/dfs/
    tree/                       Ordinary partial directory tree
        hf/
        projects/
        datasets/

    .state/
        metadata.sqlite         Replicated namespace and control state
        node.sqlite             Local inventory, intents, and consensus persistence
        staging/                Incomplete full-file transfers
        anchors/                Stable local references to backing objects
        unlinked/               Objects detached from names while still held open
```

The two databases distinguish replicated state from node-local state. They are both local files. Do not place the live databases on SMB or the distributed mount itself. Use WAL, appropriate durable synchronization, prepared statements, indexed lookups, and batched state-machine application.

The implementation should choose a consensus library whose durable log/state storage can be integrated with the SQLite requirement, rather than quietly introducing another authoritative metadata database.

**The database defines the namespace; the backing directory trees are local materializations.** This is not a union of independent directory listings.

A directory rename can therefore be atomic in the shared namespace while offline nodes catch up later. A returning node must not resurrect names merely because old paths still exist on its disk. Backing paths may be repaired or renamed locally without changing the logical result.

Direct writes to backing directories while the service is running are unsupported. Provide explicit import and recovery tools for adopting ordinary files.

### 3. What an inode means here

An inode is the logical file object, not a block map. To reduce ambiguity, implementation APIs can call its identifier `file_id`.

A directory entry maps a name to that file object:

```text
(parent_file_id, name) -> file_id
```

The file object holds ordinary metadata such as type, permissions, timestamps, size, and link count. It also identifies its current generation and, when applicable, its owner. Hard links are multiple names for the same file object; an open handle refers to the object even if its pathname is renamed or removed.

A node's underlying Linux inode is a separate local implementation detail. Different hosts' replicas need not have the same local inode number.

The persistent entities are approximately:

| Entity | Essential information |
|---|---|
| File/inode | Stable file ID, type, permissions, timestamps, link count, current generation, and content policy. |
| Directory entry | Parent file ID, name, child file ID. |
| Generation | File ID, generation number, settled size, and lifecycle state. No digest. |
| Ownership | File ID, owner node, ownership epoch, participating sessions, and fencing state. |
| Replica | File ID, generation, store ID, local object locator, and whole-file validity/availability state. |
| Policy | Folder/HF selector, required placement, optional backup retention, and reconciliation mode. |
| Manifest | A resolved set of file IDs and generations, plus path/link relationships. |
| Job | Plan ID, operation, expected generations, destination reservations, and progress. |

**Content generation** and **ownership epoch** are different counters. The generation distinguishes successive contents of a logical file; the ownership epoch fences obsolete authorities and requests. Neither is derived from hashing the file. During a write epoch, the owner is authoritative for the changing contents; that epoch's generation is not treated as immutable until finalization.

There are no block-address tables, per-chunk hashes, erasure stripes, shared extent maps, or persistent partial replicas. The local filesystem handles its own physical blocks. Transient buffers may track requested byte ranges in RAM because ordinary reads and RDMA need offsets and lengths; that does not become distributed storage metadata.

Use semantic consensus commands such as `CreateFile`, `AcquireOwner`, `PublishReplica`, and `RenameEntry`, not arbitrary client-supplied SQL. Apply committed commands transactionally together with their applied-log position.

A follower's local SQLite view is not automatically fresh enough for a strong namespace read. Use a leader-validated read barrier or a correctly implemented delegation. Kernel attribute and directory-entry caching must follow the same contract; use conservative caching initially rather than promising strong consistency with uncontrolled TTLs.

### 4. Backing-object identity and ordinary files

Operations address a stable file ID and generation, not “whatever currently exists at this pathname.” This matters for rename, unlink, replacement, asynchronous deletion, and crash recovery.

On local filesystems that support them, private hard-link anchors can keep backing objects addressable through namespace changes. They do not duplicate the data and are not snapshots: in-place writes through either link modify the same local file.

Deleting a replica means removing its local materialized names and internal references, not unlinking the file from the shared namespace. The namespace remains present while another current replica or the owner holds the data.

A file unlinked from the namespace may remain physically allocated while an existing descriptor is open. Preserve normal open-file semantics, but do not mistake that necessary lifetime for a retention feature.

All local backing operations need recoverable intents. SQLite commits and filesystem operations are not a single atomic transaction. A restart must reconcile incomplete creates, renames, transfers, and deletions against committed state before advertising the affected objects.

### 5. Owner-on-first-mutation lifecycle

Use a lifecycle resembling:

```text
STABLE(g)
    -> REVOKING_READ_AUTHORITY(g)
    -> OWNED(g+1, owner, epoch)
    -> FINALIZING(g+1)
    -> STABLE(g+1)
```

Opening with `O_RDWR` alone does not establish ownership or invalidate other replicas. The first mutation does. Mutations include writes, truncation, `O_TRUNC`, hole punching, and other content/size changes. `O_TRUNC` is therefore a mutation even though it arrives through an open operation.

#### Acquiring ownership

1. Select the owner. Prefer the writing node when it already has a current full replica. Otherwise prefer a current replica holder instead of moving an enormous file solely to make a small edit. A new file is owned on the creating node.
2. Enter revocation through consensus and stop granting authority to read the old stable generation.
3. Revoke existing read grants, cancel or drain affected prefetch/read operations, and fence obsolete requests. Wait for acknowledgments or conservatively proven lease expiry.
4. Commit the new owner/epoch and invalidate all other live replicas. Nodes applying invalidation immediately start their local asynchronous deletion path, as specified below.
5. Permit the owner to mutate after the read-coherence conditions are satisfied. Do not wait for the invalidated replicas' physical deletion.

The implementation agent should refine the exact consensus transitions and acknowledgment ordering. The invariant is fixed: no old replica may serve a read that violates visibility after the new write completes, and physical unlink latency must not enter the writer's critical path.

#### While owned

All writers submit operations to the owner. All concurrent readers use the owner, including readers that opened the file before the mutation. Additional writers do not create additional owners.

The owner orders operations, allocates append offsets, and provides current size and timestamps. Consensus does not need a transaction for every write or file-length increase.

Initially avoid long speculative read-ahead on actively changing files. Explicitly define the atomicity boundary of large writes; an arbitrarily large application write is not automatically one distributed transaction.

Requests carry a session ID, operation ID, and ownership epoch. Do not blindly replay appends after an uncertain response. Resolve uncertain outcomes through recorded operation state where supported, or return an explicit error rather than silently duplicating data.

Implement distributed advisory locks. Per-node lock behavior is insufficient for a shared HF cache.

#### Finishing the epoch

When all participating writers have released their handles and pending mutations have finished, finalize the generation and publish stable metadata. No hashing or content scan is involved.

A writable handle that never participated in mutation need not reserve an owner indefinitely. If it writes later, it starts or joins the appropriate epoch.

Implement `flush` and `fsync` deliberately; do not rely on handle release as the sole place to report write errors or establish durability. A successful `fsync` should mean the relevant data was durably flushed on the owner and the associated durable sequence/metadata was committed. This still does not mean another host has a copy.

If an owner fails during a write epoch, do not promote a stale old generation as current. Wait for recovery, report that file unavailable, or perform an explicit restore. Other files remain usable.

### 6. Immediate asynchronous deletion on invalidation

Replica invalidation is a direct data-service operation, not work submitted to the placement engine.

On applying an invalidation, a reachable node must:

```text
Remove old generation from read eligibility immediately
    -> fence/cancel old-generation data sessions as required
    -> record a crash-recoverable local deletion intent
    -> start asynchronous deletion immediately
    -> complete local cleanup and clear the intent
```

“Start immediately” means the invalidation handler initiates the local operation using the service's ordinary asynchronous I/O execution mechanism. There is no debounce, grace period, scheduled reconciliation, management job, or periodic garbage collector before deletion begins. A stopped or busy replication planner must not prevent this path from running.

The acknowledgment of logical invalidation does not wait for physical deletion to complete. Reader revocation/draining and safe RDMA buffer lifetime are separate correctness requirements; they cannot be skipped merely because unlink has started.

Deletion must identify the invalidated **local replica instance**, not just a reusable path. Use generation-specific backing locators and local serialization or atomically detach the old backing name before unlinking it. An old asynchronous callback must never delete a newer replica that has since appeared at the same logical pathname.

Remove every local reference belonging to that stale replica, including private anchors. Storage held by already-open descriptors is reclaimed when those references close; close internal stale handles promptly once safe. This is unavoidable file-lifetime behavior, not intentional delayed cleanup.

An offline node cannot delete until it returns. On return, it catches up committed invalidations, immediately starts the same deletion path, and cannot advertise stale copies. Failed deletions retain local retry state and a visible error; retries are low-level recovery, not deferred normal operation.

**Do not retain invalidated live replicas as history, fallback copies, or implicit backups.** Explicitly requested backup snapshots are separate objects with separate retention, described later. Invalidation never converts an ordinary replica into a retained backup.

### 7. FUSE data paths and coherent caching

Use two explicit paths rather than applying every optimization to every file.

#### Coherent mutable files

For general mutable files, begin with daemon-mediated I/O, using FUSE direct-I/O mode where necessary so the kernel cannot serve stale file data without consulting the coherence layer. This is distinct from forcing the backing SSD reads to use `O_DIRECT`.

Stable generations can use revocable read authority so local reads do not require a metadata round trip for every block. Grant expiry, node suspension/resume, and partitions must be fenced correctly. Do not serve through expired authority.

Initially disable FUSE writeback caching. Kernel metadata caching also needs explicit invalidation/delegation treatment. Strong behavior should not depend on every application closing and reopening a file after another node changes it.

#### Explicitly immutable content

For finalized HF blobs and explicitly sealed ordinary files, consider a faster immutable path:

- A local full replica can use FUSE passthrough to its backing file.
- A remote immutable file can use cached FUSE reads supplied by RDMA prefetch, including supported read-only mappings.

Immutability must be enforced, not inferred merely from `O_RDONLY` or the absence of current writers. A sealed inode rejects in-place mutation, while replacement by a new inode through rename remains possible. Existing open handles continue referring to the old file object.

An HF plugin may propose automatic sealing at the appropriate publication boundary, but must be tested against actual downloader behavior and existing writable handles. Do not globally make ordinary files immutable as an unannounced change in filesystem semantics.

If a compatible immutable boundary cannot be established, retain the coherent path. Do not claim that cache invalidation alone makes arbitrary existing passthrough handles safe for owner switching.

The implementation agent must validate the actual kernel/libfuse capabilities on both the super server and Sparks. Shared writable mappings remain explicitly unsupported initially rather than silently bypassing owner routing.

### 8. Persistent RDMA service and bounded read-ahead

Use [rdmasync](https://github.com/tpurtell/rdmasync) as the transport and performance reference. Inspect and reuse suitable transport concepts or code where practical; do not spawn a separate copy process for each read.

A stable-file read follows:

```text
Resolve file ID and generation
    -> obtain valid read authority
    -> choose local full replica or remote source
    -> satisfy the demanded range first
    -> prefetch subsequent ranges into bounded memory
```

An internal operation can resemble:

```text
ReadRange(file_id, generation, offset, length, session)
```

Choose sources using locality, observed throughput, storage performance, and queue pressure. Initially use one source per stream. Another full replica of the same stable generation can take over after source failure. Never combine ranges from different generations.

Persistent connections and reusable registered-memory pools avoid per-read setup and registration. Use appropriate rail/topology selection based on the actual fabric. An RDMA completion, application acknowledgment, and durable destination flush are distinct events and must not be conflated.

Remote reads fetch small files entirely when useful and use adaptive sequential prefetch for large files. Stop or reduce speculative fetching when access becomes sparse. Share in-flight ranges between local readers of the same stable generation where practical.

**Reading is not replication.** Prefetched bytes live in bounded RAM/page-cache state, not a new persistent partial or full replica. Even a whole small file temporarily cached in memory is not advertised as durable storage.

Benchmark initial transfer-unit and look-ahead settings rather than hard-coding an assumed optimum. Enforce a node-wide budget for registered buffers, pending replies, in-flight ranges, and read-ahead. Opening many model shards must not multiply unbounded per-file allocations.

Transient range bookkeeping stays local and in memory. The metadata service tracks complete-file copies only. For the initial implementation, interrupted persistent transfers restart the incomplete file; multi-file jobs resume at the completed-file level. Persistent chunk-resume maps are not required.

Cancellation must drain or safely fence outstanding RDMA operations before registered buffers are reused. Do not assume that transport zero-copy makes the FUSE-to-application path zero-copy; measure each copy and storage stage.

#### No checksum or digest layer

The filesystem performs no full-file hashing, chunk hashing, payload checksum calculation, background content scrubbing, or byte-by-byte comparison pass. The application protocol contains no digest/checksum fields.

A transfer is accepted using the expected file ID/generation, expected length, completion of all requested ranges, successful I/O/completion status, and the required flush/publication ordering. These checks detect incomplete or misidentified transfers, not arbitrary same-length silent corruption.

That limitation applies equally to replicas and backups. “Complete” or “durable” must not be presented as “content verified by checksum.” Do not reintroduce hashing under an integrity or backup feature.

### 9. Hugging Face dependency-aware placement

Keep HF-specific knowledge in a selector/resolver plugin. The core filesystem understands files, links, generations, and stores.

The plugin resolves a model selection into a dependency manifest by following the actual cache layout and symlinks. Do not assume a snapshot directory is self-contained or that every terminal blob lies directly inside its repository directory.

Conceptually:

```text
Selected repository/revision files
    -> snapshot entries
    -> referenced blob paths, possibly through further links
    -> terminal ordinary file IDs and generations
```

Include the selected directory/link structure and all required regular files. Namespace metadata alone does not make small file contents local.

Deduplicate dependencies by their resolved file identity. Existing HF blob names are opaque names supplied by HF; the filesystem does not calculate or validate their apparent hashes. Do not attempt general content deduplication between independent files with identical bytes.

Support commit-pinned selections and explicitly configured tracking selections, plus include/exclude patterns. A tracking selection can follow a locally updated cache reference without the filesystem itself initiating Internet downloads.

#### Readiness and reconciliation

A snapshot directory existing, or one shard closing, does not prove the selected model is complete.

A model is ready on a host only when every file required by its resolved selection has a stable generation and a current complete replica on that host. The resolver should use available repository file listings and the chosen include/exclude patterns to establish the intended selection; unresolved completeness must remain visible.

Automatic reconciliation can respond to finalization and namespace events, with debounce at the **model reconciliation** level. This debounce never applies to invalidated-replica deletion.

Distributed locks are essential for simultaneous HF downloads. Implement the supported `flock` and `fcntl` semantics across nodes; copying lock-file contents is not lock coordination.

Initially share the Hub cache rather than all of HF's home directory:

```bash
export HF_HUB_CACHE=/mnt/dfs/hf/hub
```

Leave credentials and other tool-specific working caches local unless deliberately configured otherwise. Validate the exact HF version and downloader behavior during implementation.

### 10. Rules, manifests, and execution plans

Separate three objects:

| Object | Meaning |
|---|---|
| Rule | Persistent desired placement or retention intent. |
| Resolved manifest | The specific files and generations selected by that rule. |
| Plan | The operations needed to move actual state toward desired state. |

An illustrative rule is:

```yaml
name: coding-model
selector:
  kind: hf
  repo: organization/model
  revision: "<commit>"
  include: ["*"]
placement:
  required_hosts: [raptor, "@sparks"]
  additional_flexible_copies: 0
reconcile:
  mode: on_idle
retention:
  protect_namespace_deletion: false
```

The syntax is proposed, not an existing implementation. A `manual` mode should also be supported. Rules are the explicit authorization for replication; an optional on-idle scheduler simply reapplies those rules after writes finish. No unsolicited persistence happens merely because a remote file was read.

Overlapping rules combine their requirements. Removing one rule cannot remove a copy still required by another. Placement and retention are separate: requiring locality is not automatically a prohibition on an application deleting the shared file.

**Eviction is not unlink.** Eviction removes a physical copy from a store. Unlink removes a name from the shared namespace. Consequently, running HF deletion through this mount affects the shared cache, not just the invoking host. Local space reclamation uses the management API.

#### Full-file replication

```text
Reserve destination capacity
    -> obtain stable-generation read/copy authority
    -> copy the complete file to staging
    -> check expected generation, size, and completion status
    -> flush according to durability policy
    -> publish the complete replica through consensus
    -> release reservations
```

There is no content verification pass. Publication is conditional on current job authority and the intended generation still being eligible. Incomplete staging files are never advertised as replicas.

If a source mutates, the copy must either have been protected by valid generation authority or be canceled/restarted. It must not publish a mixture of generations. A crash after writing the destination but before publication is resolved through local intent state.

Replication, migration, backup, archive, and recall can share the same full-file transfer machinery. Their placement and retention policies differ. **Write-triggered invalidation/deletion does not use this job machinery.**

### 11. Free-space planning

Model **stores and physical capacity domains**, not only nodes. Two directories on one filesystem share capacity. Two gateways mounting the same NAS share do not represent independent physical copies or capacity pools.

Track actual free bytes, incoming reservations, staging allocations, bytes pending real reclamation, desired free-space targets, and an emergency reserve. Report local writable capacity through mount behavior consistent with local-only creation, and expose aggregate cluster capacity separately in management views.

Hard constraints include exact host requirements, surviving current-copy requirements outside intentional mutation semantics, protected backups, in-use objects, and emergency capacity. Preferences include locality, balanced utilization, and reduced copying.

Minimum replica targets are steady-state placement requirements. The user's owner-on-write behavior intentionally invalidates other live replicas during mutation. Policies must not silently override that behavior by retaining stale current replicas; an independent explicit backup is a different protection mechanism.

Offer two operations:

- **Reconcile:** Satisfy current rules without changing them.
- **Optimize placement:** Propose revisions to flexible/planner-managed rules, then present and apply an approved plan.

Never silently violate an exact placement rule to meet a free-space target. For seven nodes, begin with a deterministic greedy planner and explicit costs rather than a complex optimizer.

Compute marginal reclaimable space using the resolved dependency graph. Removing a model selection may free little if other retained selections still depend on the same blob files. Deduplication here is shared file identity, not content hashing.

A feasible final arrangement is not enough: account for peak intermediate storage. Replacement copies, staging, and still-open deleted files can prevent an otherwise attractive plan from executing.

For planned safe migration, use dependencies:

```text
Create replacement copy
    -> confirm completion/durability
    -> publish replacement
    -> retire old valid replica
    -> reclaim its space
```

This copy-before-evict ordering applies to planned migration. It is not a prerequisite for write-triggered invalidation, which immediately deletes stale replicas under the owner-on-write rules.

Before every destructive plan step, revalidate policies, generations, available copies, and reservations. Do not count space as free until it is actually reclaimed. If a plan is impossible, report the blocking constraints and the maximum safely reclaimable space.

### 12. Backup stores, archives, and recall

Treat a backup store as an ordinary folder served through one or more gateway nodes. A NAS need not run the daemon: a node mounts its SMB share and registers that path as a managed store. A slow local SSD is another store of the same general form.

Store definitions include a stable ID, gateway nodes, root path, capacity/failure domains, capabilities, and a tested durability policy. Several gateways to one share are multiple access paths to one store, not independent backups.

#### Backup versus archive

**Backup:** Retain an independently recoverable file generation or folder/model snapshot. Later live writes and deletes do not automatically remove it.

**Archive/offload:** Allow the current namespace to have no online-cluster data copy, provided its current contents have been successfully committed to a reachable archive store.

A read of an archive-only file normally streams through a gateway and remains non-persistent on the reader. Explicit recall creates complete online replicas. Reading alone does not change the placement policy.

Writing an archive-only file normally requires materializing it on an active owner first. A complete truncation/replacement can avoid reading the old contents. Preserve any separately retained backup according to its own policy.

Do not overwrite the only retained backup in place. Create a new backup version, commit its manifest, and only then apply explicitly configured retention cleanup.

#### Explicit backups are not stale live replicas

A backup object is separately cataloged and immutable as a recovery record. It is not an invalidated online replica kept opportunistically. No normal replica gains retention just because a write invalidated it.

An archive object may also be the source for a current live file. If it is independently protected by an explicit backup snapshot, dropping its live role must not destroy that protected backup. The implementation should represent these roles explicitly rather than confusing backup retention with replica validity. Separate physical backup objects are an acceptable simple initial implementation.

#### Ordinary recoverability

Use normal versioned folder trees plus a SQLite manifest describing paths, links, metadata, file IDs, generations, and lengths. No file-content digests are stored.

When SMB does not preserve particular POSIX features, encode them in the manifest for restoration or provide a materialized export at additional space cost. Validate the actual backend's rename, symlink, hard-link, and flush behavior rather than assuming local-filesystem semantics.

Detect an absent SMB mount through store identity and mount validation. Never silently write a “backup” into the empty local mountpoint directory after the share disappears.

#### Consistent folder capture

A namespace snapshot does not snapshot mutable file bytes. For immutable HF content, capture and retain the resolved dependency manifest and copy those exact generations.

For a general folder, capture a fixed manifest and obtain each selected generation safely. If a required generation changes before capture, retry or report that a consistent capture could not complete. If continuous writes require guaranteed progress, introduce explicit quiescing or a separately designed snapshot mechanism. Do not claim application-consistent backup from a recursive copy alone.

Back up the filesystem metadata as well. Associate a consistent SQLite snapshot with its applied consensus position and the relevant data manifests. A useful disaster-recovery test restores the namespace and opens the retained files without relying on the original live cluster.

### 13. Management API, CLI, and web frontend

Every node exposes the same versioned API. It can serve local information directly and forward authoritative changes to the leader. The CLI and browser use this API; neither independently orchestrates storage by shelling into hosts.

Core API resources:

```text
nodes / stores
files / replicas
rules / manifests
plans / jobs
backups / archives
events
```

Mutations accept idempotency keys and expected object revisions. Destructive actions identify specific file generations and replica instances, not just paths. Use an event stream for progress. Durable job transitions belong in authoritative state; high-rate byte counters need not pass through consensus.

Illustrative CLI:

```bash
dfsctl rule set coding-model \
  --hf organization/model --revision COMMIT \
  --hosts raptor,@sparks

dfsctl reconcile coding-model --wait

dfsctl plan --free raptor=500GiB --free @sparks=250GiB
dfsctl plan apply PLAN_ID

dfsctl store add nas \
  --gateway raptor --path /mnt/nas/dfs --kind archive

dfsctl backup create coding-model --store nas
dfsctl offload coding-model --store nas --wait
dfsctl recall coding-model --hosts raptor,@sparks
```

The web view should distinguish logical size, unique physical storage, per-host completeness, live validity, backup coverage, archive-only availability, and actually reclaimable bytes. It should show invalidation/deletion errors without turning routine invalidation deletion into a queued job.

For an HF model, report which hosts can load the entire selected revision locally, which rely on remote reads, and exactly which dependencies prevent readiness. “Three copies” is insufficient when those copies cover different shards or revisions.

### 14. Failure handling, durability, and security

| Event | Expected behavior |
|---|---|
| Several nodes fail; quorum remains | Reachable current files remain usable without routing their data through the leader. |
| Sole current data holder fails | That file is unavailable; no fabricated fallback to stale content. |
| Metadata quorum is lost | No new authoritative changes; only operations explicitly safe under existing authority can continue. |
| Stale replica node rejoins | Catch up invalidations before serving, immediately initiating required local deletion. |
| Replica invalidation occurs | Stop stale reads and start asynchronous unlink immediately, regardless of job-engine activity. |
| Physical unlink fails | Keep the replica invalid, expose the error, and retry through local recovery. |
| Transfer process crashes | Incomplete staging stays unpublished; restart the incomplete file or reclaim it. |
| File changes during copying | The old transfer cannot publish itself as the current generation. |
| NAS mount disappears | Mark the store unavailable; do not use the empty mountpoint as storage. |
| Space target is unsatisfiable | Report constraints rather than delete the last required copy. |

Persist enough local intent state to recover across the database/filesystem boundary. Do not infer global deletion from a missing local path. On startup, resolve pending intents and reconcile local inventory with committed metadata before advertising data.

Use authenticated control connections, explicit node/session identity, permission enforcement, and fenced transfer capabilities. Scope RDMA access to active buffers/sessions. “No checksums” is not permission to expose unauthenticated arbitrary-path or arbitrary-memory access; keep authentication/control authorization separate from bulk-payload checking.

Do not promise crash durability unsupported by a backing store. State whether a reported copy is transferred, published, locally durable, replicated, or retained as a backup; these are different states.

### 15. Implementation order and acceptance tests

| Stage | Deliverable |
|---|---|
| 1. Correct core | Whole-file metadata, namespace operations, consensus/SQLite persistence, local creation, owner-on-write, direct invalidation/deletion, locks, and crash recovery. A simple TCP path can establish correctness. |
| 2. Model-loading path | Persistent RDMA service, bounded prefetch, local fast path, supported read-only mappings, HF dependency resolution, and optional enforced immutability. |
| 3. Placement management | Explicit rules, safe full-file replication/eviction, recoverable multi-file jobs, free-space plans, CLI, and web progress. |
| 4. Backup and refinement | External stores, archive/recall, retained manifests, restore testing, capacity optimization, and performance tuning. |

Rust is a reasonable implementation candidate, but language and library choices remain proposals. Use an established consensus implementation rather than writing a new consensus algorithm. Confirm that FUSE and RDMA bindings expose the necessary low-level capabilities on both architectures.

Correctness tests should include concurrent readers/writers; an owner partition during mutation; a lost append response; rename/unlink with open handles; a returning stale node; simultaneous HF downloads; generation changes during copying; crashes between flush and publication; missing NAS mounts; and policy changes during a plan.

Specific tests for the revised requirements:

- Persistent metadata contains no chunk map or partial-file residency catalog. Published replicas are always complete files.
- No file hashing, payload checksum field, verification scan, or content-based deduplication appears in any read, replication, backup, or restore path.
- Pause the placement/job engine, then mutate a replicated file. Stale read eligibility must disappear and deletion must still start immediately.
- Delay an old unlink callback while a new generation is copied to the same logical path. The newer object must survive.
- A restarted/offline node must not serve a stale replica before replaying invalidation and initiating deletion.
- Reading a remote model must not create durable replicas or exceed the configured aggregate prefetch memory budget.

Performance tests should compare native local reads, local FUSE reads, remote streaming without destination writes, and full-file replica creation with destination writes. Use the actual rdmasync baseline and equivalent durability settings, not nominal link rates. Measure cold and warm storage/cache states, sequential and sparse reads, multiple simultaneous shard loads, CPU cost, registration overhead, and aggregate memory.

### 16. Handoff to the next design/implementation agent

Refine this proposal against the original request before implementing it. Focus the refinement on the precise ownership/read-revocation state machine, kernel/FUSE caching guarantees, cross-layer crash ordering, and HF downloader/lock behavior. Validate dependencies against the actual machines and current source code.

Preserve the non-negotiable boundaries: ordinary whole-file backing trees, SQLite metadata, local-only creation, lazy owner-on-mutation, explicit whole-file placement, non-persistent bounded remote prefetch, no digests or application checksums, and immediate asynchronous deletion directly from invalidation handling.

Keep the implementation small. Do not add a chunk store, automatic read-triggered replication, stale-replica retention, hashing-based deduplication, or a garbage-collection delay to solve a problem that whole-file identity and explicit lifecycle state can solve.

References carried forward for implementation review:

- Transport/performance reference: https://github.com/tpurtell/rdmasync
- Raft overview and literature: https://raft.github.io/
- SQLite WAL: https://sqlite.org/wal.html
- SQLite backup API: https://sqlite.org/backup.html
- Linux FUSE I/O modes: https://docs.kernel.org/filesystems/fuse/fuse-io.html
- libfuse low-level operations: https://libfuse.github.io/doxygen/structfuse__lowlevel__ops.html
- Hugging Face download implementation: https://github.com/huggingface/huggingface_hub/blob/main/src/huggingface_hub/file_download.py
- Hugging Face environment configuration: https://huggingface.co/docs/huggingface_hub/en/package_reference/environment_variables

These are implementation references, not a claim that the proposed combinations have already been implemented or benchmarked.
