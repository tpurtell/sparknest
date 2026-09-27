#!/usr/bin/env bash
# Read every file of a Hugging Face snapshot through the mount, N at a time
# (like a model loader), and print bytes, seconds and GB/s.
#   read-snapshot.sh DIR [PARALLEL] [BLOCK]
set -euo pipefail
dir=$1 par=${2:-8} bs=${3:-16M}
mapfile -t files < <(find -L "$dir" -type f | sort)
bytes=$(du -cbL "${files[@]}" | tail -1 | cut -f1)
start=$(date +%s.%N)
printf '%s\0' "${files[@]}" | xargs -0 -P "$par" -I{} dd if={} of=/dev/null bs="$bs" status=none
end=$(date +%s.%N)
awk -v b="$bytes" -v s="$start" -v e="$end" -v n="${#files[@]}" -v p="$par" \
  'BEGIN { t = e - s; printf "%d files %.1f GiB in %.1f s: %.2f GB/s (parallel %d)\n", n, b/2^30, t, b/t/1e9, p }'
