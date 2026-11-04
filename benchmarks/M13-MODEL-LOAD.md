# M13 — Model loads by the LLM engine, through the mount (2026-10-20)

Baseline for push loads (ADR-063). The engine (cuteafd) loaded models on
raptor (coordinator, 2× RTX PRO 6000) and six Sparks (expert ranks 0–5 =
ostrich, dodo, emu, kiwi, rhea, moa), with weights read through
`/mnt/sparknest` on sparknest 0.5.5. Every model has one copy per file,
spread over the Sparks; none on raptor.

**Method.** `tools/loadprobe-sample.sh` ran on raptor and every Spark,
recording once a second:
- NIC bytes sent and received (port counters ×4);
- NVMe bytes read;
- sparknestd CPU time.

`tools/loadprobe-report.py FROM TO LOGS…` gives per-host means, peaks and
totals over a window; `tools/loadprobe-series.py` gives per-second send
rates. Load windows and readiness times come from the engine's logs (UTC).
"Rank ready" is the mean over the six Spark ranks.

## Load order: GLM 5.3 EXL3 K4 (`wrldsuksgo2mars/GLM-5.3-EXL3-K4-v1`)

Model: 55 files, 367 GiB; 64 GiB on each of five Sparks, 47 GiB on dodo.
Each rank requested 248.47 GB.

| Order | Rank ready | Read+upload | API ready | Per-rank rx | Disk/byte sent |
|---|---|---|---|---|---|
| In order | 142.8 s | 113.4 s | 176.0 s | ~2.3 GB/s | ~0.2 |
| Random per rank | 115.2 s | 85.0 s | 148.7 s | ~3.3 GB/s | ~0.4 |
| Interleaved by holder, all ranks in step | 149.7 s | 120.9 s | 184.0 s | ~2.0 GB/s | ~0.2 |
| Rotated by holder, rank r starts r ahead | 113.2 s | — | 148.8 s | 3.0–3.2 GB/s | ~0.27 |

- **In order:** one or two sources ran at line rate (11.7–12.4 GB/s) while
  the rest idled. A layer lives in one file, and all ranks walk the same
  layer.
- **Random and rotated:** every source sends at once at 2–9 GB/s, but the
  per-rank receive never passed ~5.6 GB/s in any second.
- **Disk:** low in every arm. Each Spark's NIC received 276–300 GB against
  248 GB requested (readahead overfetch).

## Reader change (cuteafd #566: handles kept open, reads coalesced)

| Order | Arm | Rank ready | Read+upload | NIC rx per Spark | Heavy-phase rx, cluster |
|---|---|---|---|---|---|
| In order | before | 150.8 s | 120.3 s | 271–300 GB | 12.8 GB/s |
| In order | after | 131.2 s | 100.1 s | 214–234 GB | 11.8 GB/s |
| Random | before | 123.5 s | 93.4 s | 275–299 GB | 16.1 GB/s |
| Random | after | 102.4 s | 73.5 s | 215–234 GB | 15.7 GB/s |

- **The gain is fewer bytes, not faster ones.** The overfetch went away
  (about 60 GB per rank); the receive rate is unchanged.
- **Where the limit is:** `nest io` on the Sparks shows fabric reads over
  128 KiB at p50 4.1 ms and p90 8.2 ms, against a serve time of p50
  1.0 ms. 16 readers × 1 MiB / 4 ms ≈ 4 GB/s, the per-rank ceiling.

## Kimi K3 EXL3 K2 (`wrldsuksgo2mars/Kimi-K3-EXL3-K2`), in order, before #566

- **The model:** 113 files, 798.8 GB, 129–137 GB per Spark.
- **What each rank needed:** 109.2 GB.
- **What it read:** 466.8 GB of pread (whole packed rows) per rank, and
  539–552 GB over each Spark's NIC.
- **Time:** read+upload took 273–318 s per rank, at a cluster rx of about
  10 GB/s (1.6 GB/s per rank) over the 334 s heavy phase.
- **Disk:** sources read 166–209 GB each for ~540–590 GB sent.
- **The coordinator** loaded concurrently and was not on the critical path.

## GLM 5.3 EXLR8, random order (cuteafd #570)

| Order | Rank ready | Heavy phase | rx per rank | rx rate per rank (mean/peak) | Disk per host |
|---|---|---|---|---|---|
| In order | 115.9 s | ~41 s | 51–52 GB | 1.0 / 1.6–2.2 GB/s | 57–62 GB |
| Random | 104.6 s | ~33 s | 42–51 GB | 1.0–1.2 / 1.8–2.2 GB/s | 40–55 GB |

- **No hot spot even in order:** 4–6 sources at once. The ~1 GB/s per rank
  is the EXLR8 reader's own limit.

## What the baseline says

- **Wasted bytes:** each rank moves 2–5× the bytes it keeps (EXL3 trellis
  rows interleave all ranks' columns).
- **Per-rank ceiling:** reading through FUSE caps each rank near 5 GB/s
  peak and 2–3 GB/s mean, with sources and disks mostly idle.
- **What push should reach:** each source reading its share once, about
  133 GB at 5–9 GB/s (Kimi) or about 64 GB (GLM), so 10–30 s instead of
  74–315 s.

## Push trials on the trial cluster (synthetic shards, `benchmarks/tools/push-trial.sh`)

Six sources (one 8 GiB shard each), six ranks (one per Spark), 1 MiB
slots, ranks' arenas in memfd rings.

| Run | Change | Result |
|---|---|---|
| 1 | first build | 6.2 GB/s overall; RDMA lanes failing (status 12) |
| 2 | per-host incast budget | 48 GiB in 14.3 s: an 11 s stall, then 15–19 GB/s |
| 3 | dial race fixed, 32 slots in flight | all-sources run failed (duplicate slot after a lost ack); one source alone 2.9 GB/s over RPC |

The stall in run 2 was two RDMA links per pair from simultaneous dials,
not the switch. Steady state was about 2.6 GB/s per source with 8 slots
in flight; the disks read 5.6–13 GB/s. Run 4 (all fixes, disk-speed
assignment) is next.

## First production push loads (0.6.x, 2026-10-21)

`wrldsuksgo2mars/GLM-5.3-Flash-EXL3-K3-v1@1e4abd26` from the production
hub, sources on all six Sparks, two ranks (dodo, emu) plus the RTX
coordinator on raptor; 128-slot rings of 1 MiB per rank.

| Release | Group | Result |
|---|---|---|
| 0.6.0 | 4092384301547520 | 69.2 GiB of 107.7 in 9 s (peak 18.4 GB/s), then stalled: kiwi finished first and rank hosts kept granting it slots (fixed in 0.6.1) |
| 0.6.2 | 120235912134656 | **107.7 GiB in 7.20 s: 16.1 GB/s overall, 23.0 GB/s peak (1 s), ~20 GB/s sustained**; 100% RDMA over the network, 0 fallbacks; both ranks done at 7.20 s |

Engine side (cuteafd, 0.6.2 run): launch to ready **47.0 s against 71.3 s
through the mount (−34%)**; push committed 42 layers per rank ~23 s after
the workers came up (transfer plus the engine's own setup); the four
greedy outputs are byte-identical to the mount reference. What remains is
mostly the coordinator's own load, graph capture and container start.

Per source (0.6.2): ostrich 21.4, rhea 21.8, emu 19.9, dodo 19.0, moa 14.2,
kiwi 11.4 GiB; finished between 4.9 s (kiwi) and 7.2 s (rhea). Two ranks
took ~10 GB/s each, so this load was bound by the ranks' intake rather
than the six disks (~48 GB/s together): a load over more ranks is the
next measurement.


## Source packing on real plans (2026-10-22)

`crates/nest-push/examples/pack_bench.rs` replays one source's packing loop
on one core against synthetic bytes (no disk, no network) for a real
engine plan (`{plan, file_sizes, arenas}` from cuteafd's builder): files
dealt to 6 sources by size, source 0 packs 20 GB into 1 MiB slots,
including the copy into RDMA staging. `0.6.2` is the loop as released:
every 16 MiB read walked all of its file's entries, and direct reads were
copied out of their aligned buffer. Now a read visits only the entries
overlapping it (`plan::FileEntries`) and packs from the aligned buffer
in place (`PushRead`).

| Plan | Entries | Runs (avg) | Host | 0.6.2 | now |
|---|---|---|---|---|---|
| GLM-5.3 K4 TP6 | 1,963,008 | 13.2 M in 20 GB (1.5 KB) | Spark (ostrich) | 5.5 GB/s | **10.7 GB/s** |
| | | | raptor | 5.6 GB/s | 13.4 GB/s |
| Kimi K3 TP6 | 8,902,656 | 6.7 M in 20 GB (3.0 KB) | Spark (ostrich) | 5.4 GB/s | **11.3 GB/s** |
| | | | raptor | 6.2 GB/s | 19.8 GB/s |

Entries visited per 16 MiB read: 46,745 → 101 (K4), 96,909 → 223 (Kimi).
On a Spark, 20 GB of K4 took 2.41 s walking and packing + 0.79 s copying
reads + 0.41 s into staging under 0.6.2; now 1.38 + 0 + 0.48 s.

Production sources ran at 3.8–4.0 GB/s (K4, load 120235912134657), so
packing at 5.5 GB/s was most of a source's time per byte; grant waits are
the rest. EXLR8 (load 120235912134668, 2.0 GB/s per source) has ~100×
fewer, longer runs and stalls on every source at once: not packing.

### Packing into registered staging (2026-10-22)

The same bench, `0.6.3` (each slot packed into a fresh zeroed 1 MiB
buffer, then copied into staging) against packing into reused staging
slots in place (`current`), 20 GB from source 0 of 6, two runs each:

| Plan | Host | 0.6.3 | in place |
|---|---|---|---|
| GLM-5.3 K4 TP6 | Spark (ostrich) | 10.2, 9.6 GB/s | **13.5, 15.1 GB/s** |
| | raptor | 18.1, 19.3 GB/s | 35.1, 36.8 GB/s |
| Kimi K3 TP6 | Spark (ostrich) | 12.4, 11.3 GB/s | **17.2, 23.8 GB/s** |
| | raptor | 16.8, 17.7 GB/s | 35.1, 44.5 GB/s |

The staging copy was 0.29–0.50 s of each run; the rest is the zeroing
and allocation of a fresh slot buffer, which in place packing skips (a
reused slot's old bytes past the payload and table are never read).

### One source alone (2026-10-22)

`nest push bench` on production 0.6.4, rhea the only holder, ranks
ostrich, dodo, emu, kiwi, GLM-5.3-Flash K3.25 shards 1–8 (68.7 GB),
RoCE counters sampled every 250 ms on every fabric host
(loads 4132356212719628 and …629):

| Run | Rate | Steady | ECN marks | Seq errors | Pauses |
|---|---|---|---|---|---|
| after dropping rhea's cache | 8.0 GB/s | ~9 GB/s | 56 | 0 | 0 |
| straight after | 4.35 GB/s | 3.3 GB/s for 11 s, then ~9 | 0 | 0 | 0 |

Every ring kept 60–64 of its 64 grants free, so the ranks never held the
source back. Sources read with `O_DIRECT`, so the second run was not warm:
its 11 s at 3.3 GB/s was rhea's disk (likely the SSD settling after
137 GB had just been written to it), not the fabric.

Alone, rhea pushes ~9 GB/s with a clean fabric; in a full EXLR8 load it
pushes 7.5–7.8 GB/s while each Spark records 15–27 M ECN marks and
13–24 K sequence errors. Incast costs a source ~15%; the end of a load
is set by the slower Spark disks (4.2–4.4 GB/s), which only placement
(fewer bytes on them) can change.

### MiMo V2.6 Pro after the plan fix (2026-10-22)

The first MiMo V2.6 Pro TP6 push stalled at ~0.27 GB/s per Spark rank: its
down-projection scales landed as 10–11 B runs 12 B apart, which never
merge, so each 1 MiB slot carried ~1,877 fragments and the engine copied
them out one at a time. CuteAFD made the scales contiguous (2.9 fragments
per Spark slot through the real packer, full 1 MiB payload, 51 GB/s
packing per source) and batched the copy-out. Load 4132356212719654,
production 0.6.4, 475 GB over 7 sources to 8 ranks:

| | Bytes | Done | Rate |
|---|---|---|---|
| each Spark rank (6) | 74.4–74.6 GB | 15.15 s | 4.9 GB/s |
| raptor ranks 6, 7 | 15.6, 13.0 GB | 15.15 s | |
| raptor source (10 files) | 63.5 GB | 8.5 s | 7.5 GB/s |
| ostrich, dodo, emu, kiwi (14 files each) | 48.9 GB each | 9.0–10.0 s | 4.9–5.4 GB/s |
| rhea (32 files) | 111.7 GB | 15.15 s | 7.4 GB/s |
| moa (30 files) | 104.7 GB | 14.8 s | 7.1 GB/s |

All 475 GB in 15.15 s (31.4 GB/s; 42–46 GB/s while all seven sources ran).
Every ring kept 61–63 of its 64 grants free, so no rank held a source back.
The last 5 s are rhea and moa alone at ~8 GB/s each: they hold more than
twice the bytes of the other Sparks. Placed in proportion to each source's
rate here, the same 475 GB would end in ~11 s; that is placement, not push. Two dips (at 4.0
and 6.0 s) slowed every source but emu at once, to 1–4 GB/s for a bin or two.

### An archive as a source of its own (2026-10-23, ADR-069)

Trial cluster, `PUSH_TRIAL_ARCHIVE=/mnt/scratch/sparknest-trial-archive
benchmarks/tools/push-trial.sh 8`: the six Sparks' 8 GiB shards plus one
8 GiB shard held only in an archive store on raptor's `/mnt/scratch`
(a Samsung 870 SATA SSD), six ranks (load 3206424498470912):

| Source | Done | Rate |
|---|---|---|
| ostrich, dodo, emu, kiwi, rhea, moa | 1.7–3.3 s | 2.6–5.0 GB/s (~20 GB/s together) |
| trial-archive (raptor, its own stream) | 16.8 s | 0.51 GB/s |

Every rank received its 10.0 GB exactly. The archive streamed beside the
Sparks from the start and never held them back: they finished at their
own disks' pace while it held its share of the grants, then it ran on
alone at the SATA disk's rate. The load's end is that disk: 8.6 GB at
0.5 GB/s.

## Muse max before and after SparkNest's fabric class (ADR-084, 2026-11-04)

The same Muse max plan (429.0 GB, eight sources, six Spark ranks and two on
raptor, lookahead 1024, default incast budget), from the hub's load records,
the hosts' NIC counters and the switch's per-queue counters.

| | 0.7.5, DSCP 0 (…432, …433, …434) | 0.8.0, DSCP 18 (3868415327535105) |
|---|---|---|
| Load | 21.0 / 25.0 / 20.1 s | **13.8 s** |
| Aggregate | 20.4 / 17.2 / 21.3 GB/s | **31.1 GB/s** |
| Per Spark rank, steady | ~3 GB/s | 6.0–6.6 GB/s |
| Per source | ~2.3 GB/s | 4.5–6.6 GB/s |
| RoCE retransmits per Spark, whole load | 6,000–9,000 | 7–12 |
| Out-of-sequence per Spark | 13,000–22,000 | 400–800 |
| Switch queue carrying it | 1 (lossy) | 2 (PFC) |
| Switch drops per Spark port | (queue 1: 3–6.6 G over its life) | 5.6–20 k in queue 2 |

cuteafd's own measure, engine join to group commit: 29.24 s → 18.75 s.
Its class 3 saw no pauses and no retransmits during the load; its
readiness (93.5 s) is now bound by drafter staging, not the push.

Packing is not the limit: on the Muse plan one Spark core packs 11 GB/s
(Cortex-A725) to 20 GB/s (X925), and the busiest packing thread stayed
under 40% of a core. Ports still run 4–5.7 GB/s each way of 12.5; each
Spark's single RDMA poller thread is busy throughout, and queue 2 still
drops a few thousand packets per port under eight-way incast (the PFC
thresholds are `auto`).

## Muse max: credits, incast and ECN (ADR-085, ADR-086; 2026-11-04)

The same Muse max push (429.0 GB, eight sources, six Spark ranks and two on
raptor, engine build dev 2312a49d0, slot `muse-max-qos:whole`), one load per
row. "Push" is the hub's load window; "join → commit" is cuteafd's. Stalls
are 250 ms bins under 15 GB/s. Drops are the switch's `tx-drop-queue2` per
Spark port, resends `roce_adp_retrans` per host, pauses `rx_prio2_pause`
per Spark, all over the load.

| Group | SparkNest | Class 2 | ECN q2 | Incast | Push | Join → commit | Stalls | Drops / port | Resends / host | Pauses / Spark | Rank spread |
|---|---|---|---|---|---|---|---|---|---|---|---|
| …120 | 0.8.0 | lossless | off | 64 MiB | 15.7 s | — | 18 / 63 | — | — | — | — |
| …880 | 0.8.1 | lossless | off | 64 MiB | 15.3 s | 19.99 s | 12 / 62 | 1.6–2.3 M (four ports) | 1.5–2.4 k | 685 k | 31 ms |
| …888 | 0.8.1 | lossless | off | 8 MiB | 11.2 s | 15.79 s | 0 / 45 | 9–12 k | 5–16 | 235 k | 3 ms |
| …891 | 0.8.1 | lossless | on | 64 MiB | 11.7 s | 16.39 s | 0 / 47 | 0 | 0 | 90 k | 26 ms |
| **…894** | 0.8.1 | **lossless** | **on** | **8 MiB** | **11.6 s** | **16.30 s** | 0 / 47 | **0** | **0** | 70 k | **2 ms** |
| …896 | 0.8.1 | lossy | on | 8 MiB | 11.6 s | 16.18 s | 0 / 47 | 1.5–5.9 k | 0–4 | 0 | 3 ms |
| …897 | 0.8.1 | lossy | on | 16 MiB | 11.9 s | 16.52 s | 1 / 48 | 280–490 k | 1.2–1.9 k | 0 | 2 ms |

With 64 MiB in flight per rank host and PFC alone, each burst filled the
switch's lossless pool (4 MiB of its 8 MiB): it paused every sender on class
2 at once, about 78 k pauses into raptor per 250 ms during each stall, and
still dropped. The engine held 0–7 ring slots throughout, every rank's
grants were all out, and the engine's class 3 was idle, so neither the
engine nor SparkNest's credits held the stalls (0.8.1's credit change,
ADR-085, took stall bins from 18 to 12, not to 0). The four ports that
dropped were the same model, cable, speed, FEC and PFC profile as the two
that did not, with no receive errors; in Kimi loads the drops moved to
other ports.

With ECN, each Spark received 1.1–1.4 M ECN-marked packets and sent 87–113 k
CNPs per load, and raptor's NIC handled 131–154 k: DCQCN slowed the senders
before the queue overflowed. 8 MiB per rank host is about two grants per
source (each Spark ring had 8–16 granted); the push was no slower for it.

V4.1 Flash max (306.6 GB, heavier to raptor's ranks) moved the same way,
bound by raptor's D2 disk, which finished 5–9 s after every other source:
20.1 s (64 MiB) → 19.2 s (8 MiB) → 16.3 and 16.7 s (64 MiB, ECN) → 15.9 and
16.4 s (8 MiB, ECN), the ECN runs with 0 drops and 0 resends.

Cuteafd's readiness moved 81.0–92.0 s across the arms, bound after the push
by its drafter staging (+47 to +50 s after commit).
