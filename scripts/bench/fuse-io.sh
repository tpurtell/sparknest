#!/usr/bin/env bash
# FUSE path microbenchmarks on one host's mount (trial data only).
#   scripts/bench/fuse-io.sh MOUNT [LABEL]
# Unsealed files bypass the page cache (direct I/O), so every read and write
# is a FUSE round trip: this measures the daemon path, not the disk cache.
set -euo pipefail
mnt=$1; label=${2:-run}
d="$mnt/bench-$(hostname)"; rm -rf "$d"; mkdir -p "$d"
t() { local s e; s=$(date +%s.%N); "$@" >/dev/null 2>&1; e=$(date +%s.%N); echo "$e - $s" | bc; }
gib=4
w1=$(t dd if=/dev/zero of="$d/seq" bs=1M count=$((gib*1024)) conv=fsync)
r1=$(t dd if="$d/seq" of=/dev/null bs=1M)
r128=$(t dd if="$d/seq" of=/dev/null bs=128k)
r4k=$(t dd if="$d/seq" of=/dev/null bs=4k count=65536)
par=$(t bash -c "for i in 0 1 2 3; do dd if='$d/seq' of=/dev/null bs=1M skip=\$((i*1024)) count=1024 & done; wait")
meta=$(t python3 -c "
import os
for i in range(1000):
    with open('$d/f%d' % i, 'w') as f: f.write('x')")
st=$(t python3 -c "
import os
for r in range(10):
    for i in range(1000): os.stat('$d/f%d' % i)")
rm -rf "$d"
awk -v l="$label" -v h="$(hostname)" -v g=$gib -v w1=$w1 -v r1=$r1 -v r128=$r128 -v r4k=$r4k -v par=$par -v meta=$meta -v st=$st 'BEGIN {
  printf "%-8s %-10s write1M %5.2f GB/s | read1M %5.2f GB/s | read128k %5.2f GB/s | read4k %6.1f us/op | 4x read1M %5.2f GB/s | create+write %5.1f us/file | stat %5.1f us\n",
    h, l, g*1.073741824/w1, g*1.073741824/r1, g*1.073741824/r128, r4k/65536*1e6, g*1.073741824/par, meta/1000*1e6, st/1000*1e6 }'
