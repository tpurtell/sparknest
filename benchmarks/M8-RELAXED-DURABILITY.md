# After relaxed durability, openraft 0.10 and batch delete (2026-09-27)

Trial cluster at 100G per Spark rail, io_uring on, new build (ADR-026/027).
Same scripts as the earlier runs (benchmarks/M8-METADATA-LATENCY.md,
M8-FUSE-IO-URING.md); leader dodo.

## Metadata operations through the mount (one client, in sequence)

| Operation | Leader before → now | Spark follower before → now | raptor before → now |
|---|---|---|---|
| rmdir | 5.3 → 1.5 ms | 8.2 → 2.9 ms | 7.5 → 1.7 ms |
| rename | 5.6 → 2.0 ms | 8.2 → 3.1 ms | 7.4 → 1.8 ms |
| mkdir | 7.6 → 2.2 ms | 8.5 → 3.5 ms | 7.8 → 1.7 ms |
| create empty file | 7.7 → 2.5 ms | 8.6 → 3.4 ms | 10.0 → 1.9 ms |
| unlink | 8.7 → 1.7 ms | 12.5 → 2.6 ms | 14.9 → 2.1 ms |
| write 1 byte to a new file (3 commits) | 20 → 5.0 ms | 21 → 6.3 ms | 20 → 4.1 ms |

In process on raptor's NVMe: one commit 11 ms → 0.1–0.25 ms; 32 in flight
~350 → 60–80 k commits/s.

## Removing a tree of 2,021 entries on ostrich

| | Time | Per entry |
|---|---|---|
| `rm -rf` through the mount, before | ~32 s | ~16 ms |
| `rm -rf` through the mount, now | 5.5 s | 2.7 ms |
| `nest rm -r`, now | 0.11 s | 0.05 ms |

## Data path at 100G

| | Before | Now |
|---|---|---|
| ostrich create + write + close, per file | 10 ms | 3.3 ms |
| ostrich read, 1 MiB blocks (io_uring) | 3.1–3.3 GB/s | 4.3 GB/s |
| moa reading the model from ostrich, 8 readers | 5.0 GB/s | 7.9 GB/s |
| replicate the model ostrich → dodo | 9.0 s | 8.0 s (disk-bound) |

## Upgrade in place

Deploying this build onto the format-1 trial state re-founded all seven
hosts automatically: plan agreed ~10 s after the majority appeared, seed from
emu, 71 entries and the 35 GiB model intact, identical bytes on three hosts.
