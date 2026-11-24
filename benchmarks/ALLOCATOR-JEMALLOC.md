# sparknestd's allocator: glibc vs jemalloc (ADR-093, 2026-11-24)

## Production, glibc (0.9.2)

ostrich's sparknestd after 4 d 6 h serving: RSS 3.70 GB. `malloc_info`
(gdb, via agent-sudo):

| | |
|---|---|
| Arenas | 160 (glibc's cap: 8 × 20 cores) |
| Heap mapped | 2.86 GB |
| Free inside the arenas | 2.72 GB |
| Live heap | 160 MB |
| mmap chunks (pinned RDMA regions) | 24, 1.04 GB |

`malloc_trim(0)` by hand: RSS 3.70 → 2.44 GB, stayed there. The other
Sparks were 3.1-3.8 GB with the same 0.98 GB `VmPin`.

## Trial cluster, push trial, both builds

`benchmarks/tools/push-trial.sh 16` with `PUSH_TRIAL_RUNS="all all all"`,
under the sparks/rhea/moa hwlock: six 16 GiB synthetic shards, one per
Spark, pushed to one rank per Spark (17.2 GB each), three loads in a row,
then the cluster held idle 60 s. Same commit (b92ea66) built twice, with
and without jemalloc; each run starts fresh daemons. RSS of each trial
sparknestd sampled every second. Load records in
`~/build/push-loads/trial-all-{141546125852672..4,172845005864960..2}.json`.

| | glibc | jemalloc |
|---|---|---|
| Load 1 / 2 / 3, aggregate | 8.8 / 13.4 / 13.5 GB/s | 13.2 / 16.8 / 15.0 GB/s |
| Spark RSS before the first load | 1.03-1.04 GB | 1.04-1.05 GB |
| Spark RSS peak | 1.52-1.63 GB | 1.36-1.45 GB |
| Spark RSS 30 s+ after the last peak | 1.32-1.43 GB (flat) | 1.06 GB |
| raptor (coordinator) RSS | 1.05 GB | 1.07 GB |

With glibc each Spark kept 290-400 MB above its floor after three loads,
and it would grow with the peak (what production shows after days). With
jemalloc every Spark returned to within 20 MB of its pre-load RSS. The
floor is the registered RDMA memory (~1.0 GB), the same in both.

Push rate: jemalloc no slower. The first glibc load (8.8 GB/s) was the
cold one (shards just written); the later loads (13.4-13.5 vs 15.0-16.8
GB/s) are the like-for-like comparison. One trial per build: the gain is
within the run-to-run spread seen before (M13), so read it as "no cost",
not as a speed-up.
