# FUSE over io_uring on a DGX Spark (2026-09-27)

ostrich (DGX Spark, kernel 7.0.0-1019-nvidia, `fuse.enable_uring=1`), trial
cluster, `scripts/bench/fuse-io.sh`. Unsealed files use direct I/O, so every
read and write is a FUSE round trip to the daemon; the 4 GiB test file is
local to ostrich. Same build, `[fuse] io_uring` toggled; two or three runs
each, ranges shown.

| Workload | /dev/fuse | io_uring (replies copied) | io_uring (reads straight into the ring) |
|---|---|---|---|
| write, 1 MiB blocks, fsync | 1.06–1.15 GB/s | 1.45–1.74 GB/s | 1.69–1.75 GB/s |
| read, 1 MiB blocks | 1.54–2.24 GB/s | 2.70–2.86 GB/s | 3.05–3.26 GB/s |
| read, 128 KiB blocks | 0.39–0.40 GB/s | 1.64–1.97 GB/s | 2.33–3.36 GB/s |
| read, 4 KiB blocks | 58–179 µs/op | 19–32 µs/op | 19–21 µs/op |
| 4 parallel readers, 1 MiB | 10.9–11.8 GB/s | 8.7–9.7 GB/s | 9.6–12.0 GB/s |
| create + write 1 byte + close | 9.6–9.9 ms | 10.0–10.6 ms | 10.2–10.5 ms |
| stat (attribute cache warm) | 314–398 µs | 133–389 µs | 273–345 µs |

Single-stream I/O was bound by waking a daemon thread on an idle core
(ADR-023). Answering on the issuing CPU's queue removes most of that: 4 KiB
reads drop from 58–179 µs to about 20 µs, 128 KiB reads go up 6–8×.
Copying replies into the ring first cost parallel throughput; reading local
data directly into the ring payload (the READ fast path in
`crates/nest-fuse/src/uring.rs`) brings it back level with /dev/fuse.

File creation is unchanged at about 10 ms: it waits for a Raft commit
(fsync on a quorum), not for FUSE. Sealed files do not touch the daemon at
all (passthrough), so model loading is unaffected either way.
