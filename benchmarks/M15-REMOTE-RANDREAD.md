# Remote random reads: no metadata queries per read (0.7.1)

**Problem.** cuteafd's Engram gathers read random 4 KiB pages of files held
only on Sparks, from raptor through the mount. They capped at ~58K
reads/s, and sparknestd took ~48 of raptor's 64 cores of system time,
however many reads were in flight (96 to 1024). With the reader on 8 CPUs
it was 144K/s at under one core. A kernel profile (perf, 64 reader CPUs)
put 83.5% of the machine in `native_queued_spin_lock_slowpath` under
`futex_wake`/`futex_wait`, called from SQLite's `pcache1Unpin`
(identified against the 0.6.6 binary's line tables): every remote read
ran about six SQLite queries to learn the host had no copy, and SQLite
built with `SQLITE_ENABLE_MEMORY_MANAGEMENT` shares one page cache, behind
one mutex, across all connections.

**Fix** (a8032d8): the open handle remembers where a generation is read
from, until a copy anywhere changes or one second passes. A new fabric
test counts metadata lookups over 1,999 remote reads: 7,996 before, 0
after.

**Trial** (`benchmarks/tools/remote-randread.sh`, 2026-10-31): raptor
reading a sealed 4 GiB file held on ostrich and dodo; fio io_uring,
iodepth 128, `force_async=1`, 15 s, caches dropped before each run.
Sparks on the fix in both runs; raptor on 0.7.0, then the fix.

| reader CPUs | 0.7.0 reads/s | 0.7.0 sparknestd usr / sys | fix reads/s | fix usr / sys |
|---|---|---|---|---|
| 8 | 1.68M | 115% / 38% | 1.79M | 59% / 14% |
| 16 | 1.40M | 171% / 279% | 1.77M | 60% / 16% |
| 64 | 181K | 374% / 5,192% | 1.84M | 55% / 21% |

The 4 GiB file fits raptor's page cache, so after warm-up most reads hit
it: reads/s here are mostly cache, and the result is the CPU. On 0.7.0 the
daemon's share collapsed the 64-CPU run to 181K/s at 52 cores of spin;
with the fix it stays under a core at every width.

Without forced async (fio's default inline submission) neither build
showed the spin (64 CPUs: 64K reads/s at 23% sys before, 73K at 11%
after): the contention needs reads issued from many CPUs at once, as
io-wq does.
