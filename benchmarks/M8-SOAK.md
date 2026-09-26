# Chaos soak on the trial cluster (2026-09-27)

`scripts/soak/run.sh 900`: all seven hosts run `scripts/soak/worker.py`
against their own mounts for 15 minutes. Workers write files named by the
SHA-256 of their contents (1 B–64 MiB; 40% of large files written at
shuffled offsets from four threads, like hf_xet), delete their own old
files, and read random files written by any host, checking every byte. A
rule keeps copies of `/soak` on raptor and rhea (`--auto`). Every 20–50 s a
random daemon is SIGKILLed (a second one 20% of the time) and restarted
5–25 s later.

| | |
|---|---|
| Daemon SIGKILLs | 21 |
| Files written / bytes | 15,747 / 87.9 GB |
| Files read and verified / bytes | 15,250 / 84.0 GB |
| Corrupt reads | 0 |
| Files at the end, re-read from raptor and from moa | 14,332 each, 0 corrupt, 0 errors |

Errors seen, all expected: the killed host's own mount drops
("transport endpoint is not connected") until restart; reads of a file whose
only live copy was on a killed host fail with EIO until it returns (321
write and 883 read errors over the run). Afterwards all seven hosts were
rebooted together; the cluster came back with its state intact.

Not yet exercised: pulling a cable (a real partition; the in-process suite
covers partitions) and a host reboot under load.
