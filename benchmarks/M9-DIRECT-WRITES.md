# M9 — Transfer writes: direct I/O through io_uring (ADR-042)

2026-09-28, raptor's NVMe (Samsung 9100 PRO 4TB, PCIe 5, `max_sectors_kb`
512), ext4, `nest-store`'s `pace_bench` (one transfer, O_DIRECT through the
write ring, commit sync included). `NEST_PACE_BENCH=DIR NEST_PACE_WINDOW=N
NEST_PACE_BLOCK_KIB=K NEST_PACE_GIB=G cargo test -p nest-store pace_bench --
--ignored --nocapture`.

## Before: page-cache pacing (4 GiB, 64 MiB step)

| Pacing | Rate |
|---|---|
| inline `fdatasync` every 64 MiB | 2,926 MB/s |
| background flusher | 7,123 MB/s (flattered by the page cache) |

## The drive's state matters most

Measured the same night with the drive untrimmed after it had been ~90%
full (2 TB freed but not yet trimmed): after ~100 GB of writes it sat at
~1.9 GB/s (its post-cache TLC rate) for minutes, recovering to 6–7 GB/s
only after rest. `fstrim -v /` trimmed 434.9 GiB; afterwards every run
below stayed at cache speed. Daily `fstrim` is on the root checklist.

## After trim: block size × window (8 GiB each, pre-sized)

| Block \ window | 1 | 2 | 4 | 8 | 16 |
|---|---|---|---|---|---|
| 512 KiB | 4,934 | 6,432 | 5,908 | 5,520 | 5,056 |
| 1 MiB | 5,357 | 5,268 | 5,549 | 5,029 | 6,173 |
| 4 MiB | 5,767 | 6,076 | 5,763 | 5,472 | 6,062 |
| 16 MiB | 4,874 | 5,381 | 5,128 | 5,069 | 4,753 |

MB/s. Block size and window barely matter on this drive; 4 MiB blocks
(the fabric chunk) with a window of 4 are kept. Not pre-sizing (writes
extending the file, serialized by ext4): 5,189 / 5,336 MB/s at windows
4 / 16, about 10% slower. A 48 GiB run at window 16 (drive recovered, not
yet trimmed): 6,179 MB/s.

The scratch disk (Samsung 870 SATA, one 4 MiB request per write, NCQ 32)
is to be measured once `ext4lazyinit` has finished.
