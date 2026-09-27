#!/usr/bin/env bash
# Every host reads the same snapshot at once (a model loaded cluster-wide);
# prints each host's time and the aggregate. Run from the coordinator.
#   read-all-hosts.sh SNAPSHOT_DIR PARALLEL HOST...
set -euo pipefail
dir=$1 par=$2; shift 2
tmp=$(mktemp -d)
for h in "$@"; do
  ssh "$h" "~/.local/lib/sparknest/read-snapshot.sh $dir $par" > "$tmp/$h" 2>&1 &
done
start=$(date +%s.%N); wait; end=$(date +%s.%N)
for h in "$@"; do printf '%-8s %s\n' "$h" "$(tail -1 "$tmp/$h")"; done
bytes=$(grep -ho '[0-9.]* GiB' "$tmp"/* | awk '{s+=$1} END {print s}')
awk -v g="$bytes" -v s="$start" -v e="$end" 'BEGIN { printf "all: %.1f GiB in %.1f s: %.2f GB/s aggregate\n", g, e - s, g*2^30/(e-s)/1e9 }'
rm -rf "$tmp"
