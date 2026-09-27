# M9 — Reads from other hosts: spread loads, scattered rows, io_uring serving

2026-09-27. raptor (400 Gb, one port, both subnets) reading
deepseek-ai/DeepSeek-V4.1-Flash (475 GB; engram tables in shards 47–48,
~203 GB) with copies on the six Sparks. Sparks at 100 Gb per cable. Cold
means `nest drop-caches` on every host first. Rows: 4 KiB `pread`s at
random offsets with `POSIX_FADV_RANDOM` (as the engram's `MADV_RANDOM`
faults arrive). Tensor loads: 16 threads, 6 MiB runs read front to back in
128 KiB requests, from flagged weight shards (ds41rt's open-per-tensor
pattern). Tools: python snippets in the session; ADRs 030–035.

## 4 KiB rows from other hosts (raptor holds no copy)

| Build | 1 reader, median | 32 readers |
|---|---|---|
| raptor's own NVMe, O_DIRECT (reference) | 62 µs | — |
| ADR-032 tiers + inline serve | 418 µs (p99 1.5 ms) | 23.2k/s, median 864 µs |
| + FUSE thread waits for its row | 259 µs (p99 723 µs) | 19.7k/s, median 802 µs |
| ADR-034 io_uring serving, callback replies | 595–850 µs | 62–70k/s, median ~410–450 µs |
| + threads awake while hot (ADR-035), cold | **128 µs** (p90 265, p99 360) | **66k/s**, median 435 µs, p99 843 µs |

Where a row's time went before ADR-035: server io_uring read ~145 µs,
queue ~6 µs, the rest thread wake-ups on deep-idle cores (a lone reader
measured 153 µs median while 8 other readers kept threads busy).

Raw single 4 KiB read latency on the Sparks' own disks at queue depth 1:
moa (Samsung PCIe 5) 199 µs median, raptor 62 µs; Sparks with more traffic
answered faster (129–162 µs) than idle ones (279–327 µs), consistent with
power states.

## Tensor loads from other hosts (cold)

| Build | Rate |
|---|---|
| During ds41rt, ADR-032/033 (inline serving queued disk reads) | ~0.6–1.2 GiB/s, 128 KiB reads ~730 µs |
| ADR-034/035 | **7.2 GiB/s** (4.6 GiB, 16 threads) |

## ds41rt (weighted score; categories in the session)

| Run | Setup | Weighted | Fable |
|---|---|---|---|
| r1/r3 | local copy, fresh launch | 115.18 / 118.10 | — |
| r2/r4 | local copy, second battery | 136.08 / 137.48 | 74.12 / 74.61 |
| r5 warmup / r6 / r7 | network only, before ADR-032 | 49.16 / 72.12 / 122.13 (r7 128.04) | 40.63 |
| r8 warmup / r9 | network only, ADR-032 | 75.63 / 130.87 | 53.74 |
| r10 cold / r11 warm | + FUSE thread waiting (regressed) | 72.23 / 116.23 | 36.96 / 34.10 |
| r12 cold / r13 warm | network only, ADR-034/035 | **117.02 / 136.75** | **69.71 / 71.08** |

The second battery runs mostly from raptor's page cache; the cold runs are
where the network path shows. With ADR-034/035, network only matches a
local copy: cold 117.02 against 115–118 (local, after a warmup), warm
136.75 against 136.08–137.48. Loading was ~3× faster than r10, with ~9%
readahead waste (the tails of the few unflagged files' streams; the 46
flagged shards, 508 GB, are read exactly). All samples passed; acceptance
67.7% / 69.4%. r13's natural JSON (135.85 against ~155) looks like noise.

r12/r13 by category: code 178.93 / 187.83, code with reasoning 132.58 /
151.84, math 137.77 / 170.47, Fable 69.71 / 71.08, hello 97.68 / 119.23,
topic 92.37 / 110.85, natural JSON 128.26 / 135.85, schema JSON 144.45 /
170.13, multilingual 85.47 / 110.24.

## Readahead waste

Before ADR-031 raptor's readahead read ~2.3 TiB to deliver a few hundred
GiB (open-per-tensor loads and `MADV_RANDOM` faults each fetching 4 MiB
chunks). With per-file detection (ADR-031) and run-length switch-back
(ADR-033): ~0% waste once the shards are flagged.
