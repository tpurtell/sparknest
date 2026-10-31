# Progress log

Newest first. One entry per meaningful step: what landed, what was measured,
what is next. Keep entries short; link to benchmarks and ADRs.

## 2026-10-31 — Hub downloads onto disks and off-grid hosts (ADR-075)

The Download dialog now has Place's modes (Multi host, Single host,
Off-grid, Archive) over places, so a model can go straight to
`raptor (D2)`/`(D3)` or be spread over off-grid hosts; one copy of each
file, no replicas. Files for an extra disk are fetched through the
host's mount and moved onto the disk as each fetch finishes. Test:
`a_download_for_a_disk_moves_onto_it` (nest-testkit `disks`).

## 2026-10-31 — 0.7.1: remote reads run no metadata queries

- cuteafd's remote random 4 KiB gathers on raptor capped at ~58K reads/s
  with sparknestd spinning on ~48 cores of system time. perf put it in
  SQLite's shared page-cache mutex: each remote read ran about six
  queries to learn there was no copy here. The open handle now remembers
  that per generation until a copy anywhere changes (or 1 s), and a
  remembered local copy is re-checked in memory on every read.
- Trial (`benchmarks/M15-REMOTE-RANDREAD.md`): 64 reader CPUs, every read
  async: 181K reads/s at 5,192% sys on 0.7.0; 1.84M at 21% with the fix.
  A fabric test now requires zero metadata queries per remote read.
- Rolls to every host but maxq tonight; maxq (Step-5 quant running)
  follows when the quant ends.

## 2026-10-29 — 0.7.0: several disks per host (ADR-073, ADR-074)

- For raptor's two new 8 TB PCIe 5 NVMe drives: each extra disk is a
  place of its own, named like `raptor (D2)` (`nest disk add|ls|drain|rm`,
  `/v1/disks`). Copies go to a disk only when it is named or a plan over
  it picks it; `raptor` is its primary disk. A Place spread over raptor,
  D2, D3 and the Sparks puts each file on one of them.
- Writes, ownership and metadata stay on the primary disk. Reads,
  passthrough and push treat a host's disks as local; push runs one
  stream per disk at its own measured rate.
- Startup reconciles each mounted disk; an unmounted disk (marker
  missing) is never read or written and keeps its records until it is
  back. A drain moves a disk's copies to a named disk on the same host.
- UI: each disk is a host card and a Place tile of its own.
- 0.6.7 off-grid fixes (ADR-071, ADR-072) are on kakapo, cassowary and
  maxq; raptor and the Sparks wait for cuteafd's V4 release window, then
  take 0.7.0 directly.
- Raptor's root disk (also /srv/sparknest) fell to 100 GB free from build
  artifacts; `target/` pruned back to 320 GB free.
- Next: install the drives, `nest disk add raptor /srv/sparknest-nvme1`
  (and nvme2), measure a push from raptor's three disks.

## 2026-10-21 — 0.6.0, 0.6.1, 0.6.2 released and deployed; first production push loads

- 0.6.0 on all ten hosts (cassowary joined as an off-grid host, node 10),
  then 0.6.1 and 0.6.2, each from published bottles, rolled host by host
  inside a cuteafd drain window; with bottles installed ahead, each
  restart took ~4 s.
- First production push load (0.6.0) stalled after 9 s: a finished source
  kept collecting ring-slot grants. Fixed in 0.6.1, with ring state in the
  load record and stall reasons. 0.6.2: the preferred-leader handoff
  counted control-on-the-LAN as a degraded link; leadership now returns
  to raptor (~2 min after its restart in production).
- 0.6.2 push of GLM-5.3-Flash EXL3 K3 to two ranks: 107.7 GiB in 7.2 s,
  16.1 GB/s overall, 23 GB/s peak, all RDMA. cuteafd: launch to ready
  47.0 s against 71.3 s through the mount, outputs identical
  (`benchmarks/M13-MODEL-LOAD.md`).
- Control traffic on the LAN: zero heartbeat timeouts since (hundreds per
  link per day before).

## 2026-10-21 — Bulk off the control connection; control on the LAN; hf share (0.6.0 in progress)

- Trial run 2 (6×6, 8 GiB each) stalled 11 s on a simultaneous-dial race
  (two RDMA links per pair; fixed: the lower id's link wins), then ran
  15–19 GB/s at 8 slots in flight per source; now 32. Trial run 3 failed
  outright: a slot whose RDMA write landed but reported failure was sent
  again by RPC and refused. Grants now carry a sequence number and a
  duplicate is dropped (tested with lost acks over real RDMA).
- Push assignment and Place follow disk speed (ADR-066): rhea, moa and
  raptor read 12–13 GB/s, the other Sparks 5.6; an even split left the
  slow disks to finish alone. `nest plan --place … --by-speed`.
- A burst of hand-applied placement plans (2026-10-21 05:12 UTC) starved
  Raft heartbeats on the lossy fabric switch for two minutes; leases
  lapsed cluster-wide and 109 copies were skipped. Now: copies wait out
  a source that cannot serve (up to 2 min), hosts log serving changes,
  push slots without RDMA travel on TCP lanes (ADR-067), and control
  traffic goes on the 10G LAN with the fabric as fallback (ADR-068,
  cluster setting `control-on-lan`, default on).
- Job progress moves within files (`partial_bytes`); the UI shows
  percent, recent rate and time left.
- `nest hf share` converts repos written by huggingface_hub < 1.32 to the
  hub's shared blob store in place: dry run on production finds 57 repos,
  2,361 blobs, 11.6 TiB, no data moved. Homebrew `hf` 2.2.0 on every host;
  cuteafd pinned `huggingface_hub>=2.2`.
- UI: dialogs stay on screen on phones; logs no longer log out on a
  dropped socket; the Logs page no longer crashes Safari.

## 2026-10-21 — Push loads built; the daemon's readahead removed (0.6.0 in progress)

- Push loads (ADR-063): the plan crate (`nest-push`), the hub, sources and
  rank hosts, slots written by the source's NIC straight into the engine's
  ring, O_DIRECT source reads four ahead. Over RDMA loopback on raptor,
  2 GiB from NVMe to two ranks at 4.7 GB/s. First run across the trial
  cluster (6 sources, 6 ranks, 8 GiB each): 6.2 GB/s in all, about
  1.1 GB/s per rank, below target; being worked on.
- Placement readable through the mount (ADR-064): `user.sparknest.hosts`,
  `.where`, `.cluster`.
- The daemon's readahead and read-pattern detector are gone (ADR-065):
  with the mount's 16 MiB kernel window, reading as asked was as fast
  sequentially (2.40–2.67 against 2.32–2.39 GB/s) and 12–17% faster at
  random 4 KiB. `nest read-pattern` and `/v1/read-pattern` removed;
  `passthrough = "sole"` now behaves as `"always"`.
- Fixed three flaky tests (two m8 races replicating before the target
  host saw the file stable; a data-reserve margin on a shared disk).

## 2026-10-21 — Model-load baseline; push loads designed (ADR-063)

- Measured the engine's model loads through the mount with a 1 Hz
  NIC/disk sampler on every host (`benchmarks/M13-MODEL-LOAD.md`): ranks
  move 2–5× the bytes they keep (EXL3 rows interleave every rank's
  columns), and reading through FUSE caps each rank near 5 GB/s peak
  while sources and disks idle. With the engine's random order and
  reader fix, GLM 5.3 K4 went from 143 s to 102 s; Kimi K3 still takes
  ~315 s.
- ADR-063: push loads. The engine sends a strided plan, each source
  store streams its share once in disk order, and each rank receives only
  its own bytes into a ring it owns. Planned for 0.6.0 with the read-only
  placement attributes and the end of the daemon's speculative readahead.

## 2026-10-19 — 0.5.5: the OOM killer passes over the daemon (ADR-062)

- ostrich's daemon was OOM-killed four times in a row while a container
  ran CUDA: Spark GPU memory is not charged to its process, so the
  daemon's pinned RDMA slots made it the largest task. The unit now sets
  `OOMScoreAdjust=-500`; hf children are started back at 0. Installed
  units need the rendered unit copied again.

## 2026-10-18 — 0.5.4: only named branches split a model

- 0.5.3 listed about 25 ordinary repos as several rows: a `main` row
  plus one per snapshot an update left behind (no ref names it, and
  they share most blobs, so the rows repeat sizes). A repo now splits
  only when two or more of its snapshots are named by a branch or tag;
  unnamed snapshots stay in the repo's own row. On production no repo
  splits; on the trial cluster Qwen3-0.6B-exl3 `3.0bpw` and
  `2.75bpw_H5` are still two rows.

## 2026-10-18 — 0.5.3: a snapshot no ref names is still a revision

- After the 0.5.2 rollout five wrldsuksgo2mars repos listed with errors:
  their extra snapshots are named by 64-hex ids that no ref points at,
  and `hf:org/name@<id>` was looked up as a branch in `refs/`. A
  revision that names a snapshot directory is now that snapshot
  (selector resolution and `hf rm`); tests in `m5` and
  `hf_removes_one_branch`.

## 2026-10-18 — 0.5.2: Hugging Face branches as models (ADR-061)

- Several branches of one repo can be downloaded, and each is its own
  model: its own row, size, detail page, placement and delete
  (`hf:org/name@branch`). The download dialog takes `/tree/<branch>`
  links and offers the Hub's branches; `nest hf refs` lists them.
  Checked on the trial cluster with turboderp/Qwen3-0.6B-exl3 `3.0bpw`
  and `2.75bpw_H5` (download both, place one on kiwi, delete one).
- `/v1/hf/refs` reads `refs/` through the mount off the async executor.

## 2026-10-18 — 0.5.1: a stuck TCP lane is replaced; cancel stops mid-file (ADR-060)

- Raptor's reads from oliver hung for 30 s each, three hours after an
  archive sync: six lanes had stopped carrying raptor's requests (NAT on
  the WSL host, most likely) while oliver's answers still flowed, so
  neither end redialled. The Kimi-K3 page (only live copy on oliver)
  opened empty. A read or write left unanswered now closes its lane, and
  lanes set `TCP_USER_TIMEOUT` 10 s plus keepalive. New test
  `a_lane_that_stops_answering_is_replaced` cuts one direction of the
  lanes through a relay; it fails without the fix.
- Cancelling a replicate or offload stops files mid-copy as well (it
  used to let them finish: Kimi-K3 to scratch took about five minutes to
  settle) and removes their partial copies (`cancel_stops_a_file_mid_copy`).

## 2026-10-17 — 0.5.0: a 10G standby, remote writes over RDMA, leader handoff (ADR-057–059)

- **10G standby for the control plane** (ADR-057 1–2). Each fabric pair
  keeps an idle, pinged control connection on the LAN (`Member.lan`; the
  lower id dials). Requests move there only when the fabric connection
  breaks, or nothing arrives on it for 1 s while the standby answers (a
  slow handler never counts: the connection reader answers a probe), and
  move back after 10 s healthy. In-process, cutting every fabric
  connection kept quorum with no election. Requests in flight at a move
  fail `RpcError::Lost` (maybe executed) and Raft resends them.
- **A standby data lane** (ADR-057 3): one TCP lane per fabric pair on the
  LAN (`node.lanes_fabric`); reads use it while an RDMA link is down or
  backing off, never because RDMA is slow.
- **Remote writes over RDMA, pulled by the owner** (ADR-057 4, ADR-058):
  the writer SENDs a 64-byte request naming a remote-readable slot, the
  owner RDMA-READs it into a staging slot (queued when none is free, never
  reserved per writer), applies it and answers. Fabric protocol 3, lane
  frame kind 4 after a probe; 0.4 hosts keep RDMA reads and get writes
  over the control connection. A write whose answer is lost is EIO, never
  resent. raptor loopback, 1 MiB at 8 in flight: 52 GB/s against 13 GB/s
  over TCP; a control call behind them waits p99 115 us instead of
  1.2–3.4 ms ([M12-REMOTE-WRITES](../benchmarks/M12-REMOTE-WRITES.md)).
- **Two data bugs fixed.** Concurrent appends from several hosts could
  overwrite each other at the owner: on the trial cluster a 0.4.4 owner
  kept 563–597 of 600 records, a 0.5.0 owner all 600 (append lock).
  Concurrent first writes to a stable file from two hosts failed with "no
  reachable node holds this file" (EIO); they route again now.
- **Leader handoff** (ADR-057 5–6): a stopping leader hands off to the
  most caught-up voter that can take it (`RaftReq::TransferLeader`, after
  a `Features` probe 0.4.4 refuses). Cluster setting `preferred-leader`
  (`nest config set preferred-leader raptor`): the leader moves there after
  2 min settled, not within 10 min of the last move, and never while that
  host's control requests are on the LAN. Trial cluster: handoff on stop
  23 ms, back to raptor after the settle in 31 ms, term +1 each, no
  election.
- **Served objects close on invalidation** (ADR-057 7): deletion, eviction
  from any host or a new owner's generation closes the cached open file at
  once; idle 2 s (sliding) or open 30 s closes it otherwise.
- **`nest net`** (`GET /v1/net`): per host and peer the control path and
  why it moved, the RDMA link and protocol, TCP lanes, Raft heartbeat
  round trips and lag; each RDMA port's NIC loss counters (out-of-sequence,
  sequence NAKs, RNR NAKs, CNPs). A new admin request rather than new
  `NodeInfo` fields: postcard cannot decode a struct missing trailing
  fields, so a 0.5 host asking a 0.4.4 one would have seen an error.
- **Control path benchmark** ([M12-CONTROL-PATH](../benchmarks/M12-CONTROL-PATH.md)):
  raptor to kiwi, 30 min side by side, fabric p50 246 us, LAN 372 us,
  neither near a heartbeat's 150 ms. The fabric stays primary.
- **Not done:** keeping the mount across a restart (ADR-059: requests on
  the FUSE io_uring ring are lost when the server exits); a live fabric
  link drop on real hardware (needs a quiet window).
- **The OpenAPI document** (ADR-006) is checked in at last:
  `crates/nest-api/openapi.yaml`, with a test that fails when a route is
  not documented.
- `sparknest-add-lan-standby` turns the standby on in `node.toml` after
  the upgrade (0.4.4 refuses to start with `lan`).

## 2026-10-17 — 0.4.4: reads ride out elections, EIO logged, archive snapshots on SMB (ADR-056)

- Investigated a silent EIO during a model load (2026-10-16): the fabric
  switch's upgrade reboot dropped every link twice and metadata quorum was
  lost for ~3 min. Reads now retry through a leader change or lost quorum
  for `read-recovery-wait` (default 5 s; `nest config set
  read-recovery-wait 10s`, a cluster setting every host reads live), and
  every EIO the mount returns is logged, rate-limited.
- Leader heartbeat-timeout storms during heavy runs: the switch tail-drops
  1–5% of egress to 100G hosts under incast; Raft's lost segments waited
  Linux's 200 ms retransmit floor against a 100 ms heartbeat budget. Peer
  connections now set a 5 ms floor (`TCP_RTO_MIN_US`) and heartbeats go
  every 150 ms (elections unchanged).
- Metadata snapshots into the `models` archive (SMB) had never succeeded
  (SQLite cannot lock there); snapshots and backup manifests are now built
  on local disk and copied in, and stale temporaries are swept.

## 2026-10-15 — 0.4.3: Hugging Face imports and downloads into archive stores (ADR-055)

- `nest hf import SRC --archive NAME` and `nest hf download REPO --archive
  NAME` (and the Download dialog's Archive choice) put a model in an
  archive store without it touching a host's disk; a cache on the
  archive's own disk is hard-linked in. Verified with a one-node trial
  daemon: a real download (hf-internal-testing/tiny-random-gpt2) and an
  import of a cache on the archive's filesystem (blobs linked, link count
  2), both reading back through the mount with matching hashes.

## 2026-10-15 — 0.4.2: fast metadata scans (ADR-054)

- LOOKUP and GETATTR are answered on the ring thread that received them
  unless another host owns the file, and listings use READDIRPLUS, so
  stat after readdir costs no request per entry. Before: `du` over the
  mount (24,210 entries) took 0.3–0.8 s on raptor and 5–12 s on the
  Sparks (~400 µs per `stat`, mostly cross-CPU wakeups). After, on the
  fleet: raptor 0.3 s, Sparks and off-grid hosts 0.6–1.6 s.

## 2026-10-15 — 0.4.1: always-passthrough files, counters socket directory, UI fixes

- Files can be marked always passthrough (ADR-053): `nest passthrough
  SEL always|default`, a toggle on a model's page, the Passthrough… menu
  item. Hosts with a complete local copy hand them to the kernel whatever
  their policy. `sole` no longer gives up passthrough over an off-grid copy.
- The counters stream moved to /srv/sparknest/run/rdma-counters.sock so
  containers mount the directory and survive daemon restarts (ADR-052).
- The web UI reloads itself when its node starts serving a different UI,
  and no longer reflows under the pointer when hover content or a
  scrollbar appears (stable scrollbar gutters, fixed-size treemap details,
  no animation on resize).

## 2026-10-15 — 0.4.0: RDMA counters for benchmarking (ADR-051)

- Per RDMA port: payload bytes, operations and estimated packets, sent and
  received, for subtracting sparknest's share from the NIC totals in LLM
  engine benchmarks. JSON snapshot on 127.0.0.1:7403, binary stream on
  /srv/sparknest/rdma-counters.sock (1–20 Hz); docs/RDMA-COUNTERS.md;
  `sparknest-rdma-counters` prints it.

## 2026-10-12 — 0.3.0: off-grid hosts (ADR-047…050)

- Hosts on the LAN only (kakapo, oliver behind WSL NAT) join as Raft
  learners with `role = "offgrid"`: they dial the fabric hosts at pinned
  LAN addresses and every control connection now carries requests both
  ways, so nothing needs to reach them inbound. They never vote, never
  seed a re-found, read their own copies first and never spread reads;
  fabric hosts rank them after fabric holders and leave them out of
  spreads. `nest cluster add ID --offgrid`; sample config in
  `packaging/config/node-offgrid.toml.sample`. Fencing stays cluster-wide.
- RDMA link setup backs off after a failure (1 s doubling to 30 s) and
  logs why, instead of re-dialling on every read.
- LAN TCP measured (benchmarks/M10-LAN-TCP.md): Linux Sparks fill 10GbE
  with 1–2 streams, oliver needs 4–8; oliver tuned (NIC buffers, WSL MTU
  1500); `tcp_slow_start_after_idle=0` on every host. Jumbo frames do not
  pass the LAN switching yet.
- TCP lanes (ADR-048): 4 authenticated connections per peer, dialled by
  the off-grid host, answers sent with MSG_MORE + sendfile; readahead and
  replication use them through a `Bulk` interface beside RDMA. 1.15–1.20
  GB/s (line rate) to oliver and kakapo.
- Placement (ADR-049): Place over fabric hosts only; an Off-grid mode in
  the Place dialog and `nest plan --offgrid`; offload to an off-grid host;
  HF spread downloads skip them. `nest cluster role HOST offgrid|fabric`
  moves a host (ADR-050). `scripts/add-offgrid.sh` onboards one.
- Released 0.3.0 (bottles-20261012-1), installed on nine hosts, fabric
  hosts restarted onto it listening on the LAN. 2026-10-13: kakapo (node
  8) and oliver (node 9, WSL behind NAT) joined off-grid, both with FUSE
  passthrough and io_uring. Reads from fabric NVMe: kakapo 0.84 GB/s one
  stream, 1.13 GB/s two; oliver 0.53 and 0.79 (its quantization job was
  running).
- Next: single-stream reads through the filesystem off the fabric (one
  holder per stream, two copies per chunk), and oliver's WSL path.

## 2026-10-11 — 0.2.2: RDMA lanes no longer leak

- Lanes of replaced or half-built links held their QP and registered ring
  for the life of the process: after a week dodo, emu and kiwi each had
  ~130k QPs and rhea gathered ~8k within hours of a restart, starving other
  RDMA users. Dead lanes are now released by the expiry sweep once a 10 s
  grace has passed (`replaced_links_release_their_lanes`).
- `cargo test` mounts ten FUSE test clusters at once, each with 64 io_uring
  queues: under a 1024 open-file soft limit (an ssh login) queues fail with
  EMFILE and the tests hang. `scripts/build.sh --check` now raises the soft
  limit to the hard one first; the daemon runs with LimitNOFILE=1048576.

## 2026-09-28 — The real cluster; transfers written directly; placing models

- Installed with Homebrew from a local tap on all seven hosts
  (`scripts/install-cluster.sh`); cluster `sparknest` at /mnt/sparknest,
  archives `models` (NAS) and `scratch` (SATA SSD).
- Transfers (ADR-039…042): paced, then written with O_DIRECT through one
  io_uring per process, a fixed window of writes in flight. No page-cache
  floods on SMB, no blocking-pool exhaustion stalling the metadata apply.
  raptor NVMe 5.7–6.0 GB/s (benchmarks/M9-DIRECT-WRITES.md).
- Place (ADR-043): hosts plus a replica factor (1/hosts spreads, 1 copies
  everywhere), the host reading a file most chosen first. Plans are
  edited as a host → model → file tree before applying (UI and
  `nest plan show|skip|unskip`). Finish downloads only files no host has.
- UI: dialogs float on the screen (not the page), the model window fits the
  screen with a treemap, hover details and zoom-out, the animated backdrop
  shows through everywhere, nav links can no longer start a native drag.
- Fixed (ADR-044): opens of sealed files with a local sole copy failed with
  EIO on that host (passthrough answered with a flag the kernel refuses, and
  a new backing file per open). One mode and backing file per open inode.
- Released v0.1.0 (https://github.com/tpurtell/sparknest/releases/tag/v0.1.0)
  and added it to tpurtell/local-ai-tap with Linux ARM64/AMD64 bottles
  (bottles-20260928-2). All seven hosts now install the published bottle
  (`brew install tpurtell/local-ai/sparknest`) instead of the local tap.
- Hugging Face downloads (ADR-045): planned, spread over the hosts by
  default (each file fetched by the host it is assigned to), one file in
  flight across the cluster, byte progress per host, cancel and resume.
- 0.2.1 (ADR-046): four files in flight; stalled fetches are given up on
  and a host that keeps failing (emu without IPv4) hands its files on.
- Next: measure the scratch disk idle; daily fstrim (root checklist).

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
- ds41rt from other hosts' copies only now matches a local copy: cold
  117.02 (was 72.23), warm 136.75 (local 136–137), Fable 69.7 / 71.1.
- Next: per-file readahead shared across opens; Spark NVMe/CPU power
  settings (root checklist).

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
