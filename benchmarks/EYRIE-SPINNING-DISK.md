# eyrie: copies onto one spinning disk (2026-11-26)

eyrie: QNAP TS-X64 (Celeron N5095, 16 GB), QuTS hero 5.2.9, 10 GbE. Its
copies (`data_dir`) are on one Seagate IronWolf Pro 32 TB (zpool3/zfs20,
compression, dedup and encryption off, recordsize 128K); its state on the
NVMe mirror. StorageReview's fio numbers for the drive: 284 MB/s sequential
read, 283 MB/s write. sparknest 0.9.4 in Container Station (no io_uring in
the container: transfer writes run on the blocking pool).

Load: `nest replicate hf:Qwen/Qwen3.8-27B-FP8 --hosts eyrie` (83 files,
28.8 GiB: 64 shards of ~366 MiB, one 5.6 GiB file, small files), from its
only complete copy on raptor's D3 (NVMe) over the LAN; evicted between runs.
Disk rate: `zpool iostat -p zpool3 5` on eyrie, median over the samples
writing more than 50 MB/s.

| Run | dataset `sync` | `disk_transfers` | copy | disk writes (median) |
|---|---|---|---|---|
| 1 | standard | 2 | 355 s, **87 MB/s** | 135 MB/s at ~1,090 writes/s, disk 100 % busy |
| 2 | disabled | 2 | 138 s, **224 MB/s** | 225 MB/s |
| 3 | disabled | 1 | 135 s, **229 MB/s** | 226 MB/s |
| 4 | disabled | 2 | 134 s, **230 MB/s** | 233 MB/s |

Baselines on the same dataset (GNU dd in the container, 8 GiB, one stream):
O_DIRECT 1 MiB writes 236 MB/s; buffered writes with one fdatasync 190
MB/s; O_DIRECT 8 MiB writes 188 MB/s; read 251 MB/s.

Reading the copies back off the disk (the 66 files over 300 MB, 29.4 GiB,
dd O_DIRECT 8 MiB, ZFS cache much smaller than the data): one file at a
time 259 MB/s, two at a time 259 MB/s. Two copies written at once are laid
out no worse for reading than one.

## What it says

- With `sync=standard`, each copy's syncs sent its data through the ZFS
  intent log on the same disk before its final place: the disk was full at
  ~1.5x the bytes landing. With `sync=disabled` the copy runs at the disk's
  own rate (about 230 vs 236 MB/s for a bare O_DIRECT stream). The dataset
  holds only copies: the metadata and Raft log are on the NVMe mirror with
  sync left on, and a copy lost to a power cut in its last seconds is
  re-fetchable (as on aviary's ZFS24, also `sync=disabled`).
- `disk_transfers` 1 vs 2 is a tie on this load; 2 stays (smoother in the
  web UI's rate graph, and one slow source does not idle the disk).
- A copy from eyrie to another host is only made when no faster source has
  the file: with raptor's D3 holding the model, a copy to kakapo read
  from D3 at 1.19 GB/s and never touched eyrie.
