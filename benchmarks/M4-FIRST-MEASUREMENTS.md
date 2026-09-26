# M4 first measurements (2026-09-26)

Trial cluster: raptor (x86_64, 1×400G mlx5, kernel 7.0) and six DGX Sparks
(arm64 Cortex-X925, 2 RoCE functions on one cable, switch at 100G/rail,
kernel 7.0-nvidia). Daemons run as the user (no `CAP_SYS_ADMIN`, so no FUSE
passthrough; FUSE readahead at the kernel default 128 KiB). Files were in
the page cache of the node holding them; `dd` single streams unless noted.

## Transport

| Path | Result |
|---|---|
| RDMA loopback through the NIC, synthetic source, 256 concurrent 4 MiB reads (raptor) | 20.3 GB/s |
| same on ostrich | 14.1 GB/s |
| In-process nodes, fabric reads of a real object, 8 × 4 MiB in flight (raptor) | 26.5–28.1 GB/s |
| In-process full Vfs read path (1 MiB reads + readahead), 1 GiB (raptor) | 15.2–15.8 GB/s |

The fabric and the Vfs read path are not the bottleneck.

## Through FUSE on the real cluster

| Case | bs | Result |
|---|---|---|
| raptor, local unsealed file | 1 MiB / 4 MiB | 3.9 / 4.9 GB/s |
| raptor reading a Spark-owned file over RDMA | 1 MiB | 4.4–4.6 GB/s |
| ostrich, local unsealed, before fast paths | 1 MiB | 0.9 GB/s |
| ostrich, local unsealed, after fast paths | 1 MiB / 4 MiB | 1.6 / 2.0 GB/s |
| ostrich, local unsealed, 4 parallel readers | 4 MiB | 6.5 GB/s aggregate |
| ostrich reading raptor-owned unsealed file over RDMA | 1 MiB | 1.4 GB/s |
| same, 4 parallel readers | 4 MiB | 4.0 GB/s aggregate |
| ostrich reading raptor-owned **sealed** file, cold | 4 MiB | 1.2 GB/s |
| same, warm (page cache kept across opens) | 4 MiB | 21.4–22.8 GB/s |
| raptor, local sealed, page-cache mode (no passthrough) | 4 MiB | 6.4 GB/s |
| ostrich native read of the backing object (page cache) | 1 MiB | 30 GB/s |

## What limits the Sparks

A FUSE request costs ~0.5 ms on the Sparks regardless of size (128 KiB
requests: 209 MB/s; 1 MiB: 1.2 GB/s), and parallel readers scale almost
linearly, so it is latency, not bandwidth. Spark cores idle in ACPI LPI
states with 231 µs and 433 µs exit latency (raptor: 1 µs / 100 µs); every
thread wake on the request path can pay that. Moving local reads, readahead
hits and owner writes onto the FUSE thread (one wake instead of three)
improved local reads 0.9 → 1.6 GB/s and writes 0.54 → 1.3 GB/s.

Cold sealed reads go through kernel readahead, which the kernel caps at
128 KiB for this mount unless root raises `read_ahead_kb`.

## Levers that need root (not yet measured)

1. `CAP_SYS_ADMIN` (system unit): FUSE passthrough for sealed local files,
   expected ≈ native (NVMe 6–7 GB/s cold, page cache ~30 GB/s warm).
2. `read_ahead_kb` 16 MiB on the FUSE bdi (system unit does this): larger,
   more parallel requests for cold sealed remote reads.
3. `cpupower idle-set -D 100`: removes the deep-idle wake cost.
4. Switch at 200G per Spark rail for fabric-bound cases.

## Next measurements

With the system unit installed: sealed local passthrough vs native (cold
and warm), sealed remote cold reads with 16 MiB readahead, then the same
with deep idle states disabled. Compare remote reads against rdmasync on
the same pair.
