# M5: a real model through the trial cluster (2026-09-26)

Model: `Qwen/Qwen3.6-35B-A3B-FP8` (62 files, 34.9 GiB), copied from raptor's
HF cache into a scratch source (the real cache untouched). Same conditions
as `M4-FIRST-MEASUREMENTS.md`: no `CAP_SYS_ADMIN` (no passthrough), FUSE
readahead 128 KiB, Spark switch ports at 100G per rail.

| Step | Result |
|---|---|
| `nest import --move` into `/hub` (hard links, HF seal policy) | ~1 s for 34.9 GiB; blobs sealed, refs writable |
| `nest replicate … --hosts ostrich` (8 files in parallel) | 12.0 s ≈ 3.1 GB/s, into NVMe with fsync |
| `nest replicate … --hosts dodo --parallel 16` | 9.0 s ≈ 4.2 GB/s |
| rdmasync of the same blobs raptor → emu (baseline) | 8.4 s ≈ 4.5 GB/s |
| Model read on ostrich (local copy), 8 parallel `cat` of all shards, first | 9.6 GB/s |
| same, second pass (kernel page cache, sealed files keep it) | 84.8 GB/s |
| Model read on moa (no copy, remote over RDMA), first | 5.0 GB/s |
| same, second pass | 6.0 GB/s (moa had <1 GB free RAM: another workload holds its memory, so the page cache could not keep 35 GB; a single 805 MiB shard re-reads at 9–11 GB/s) |

Replication is at 93% of rdmasync on the same pair class, above the M4
target of 80%; both appear bound by Spark NVMe writes at this link speed.
Cold remote model loads are bound by per-request latency (128 KiB kernel
readahead through FUSE, Spark deep idle states); the root-level levers in
`docs/ENVIRONMENT.md` are the next measurement.

## M6: the same model through archive stores (2026-09-26)

Stores: `scratch` = `/mnt/scratch/sparknest-trial-archive` (raptor's 870 SATA
SSD, ntfs3; raptor sole gateway) and `nas` = `/mnt/models/sparknest-trial-archive`
(SMB share, all seven nodes gateways). Both healthy from every gateway.

| Step | Result |
|---|---|
| `nest offload hf:Qwen/Qwen3.6-35B-A3B-FP8 --store scratch` (copy, then drop live copies on raptor, ostrich, dodo) | 194 s ≈ 180 MB/s (ntfs3 on SATA, fsync per file); only the archive copy remains |
| moa reads an archived 798 MiB shard through raptor (gateway, page cache warm) | 1.5 GB/s |
| `nest replicate … --hosts ostrich --parallel 16` (recall from the archive) | 9.0 s |
