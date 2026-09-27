#!/usr/bin/env bash
set -u
cd /home/tj/Developer/sparknest
N=~/.local/lib/sparknest/nest
S='~/sparknest-test/mnt/hf-home/hub/models--brandonmusic--GLM-5.3-Flash-EXL3-4bpw/snapshots/4739eb1bcfd478e8a32da6358908567bc3a9ac51'
HOSTS="raptor ostrich dodo emu kiwi rhea moa"
restart() { timeout 120 scripts/trial.sh stop >/dev/null; timeout 60 scripts/trial.sh start >/dev/null; timeout 90 $N wait-ready >/dev/null; sleep 5; }
echo "== spread on (local first + rendezvous)"
SPARKNEST_BALANCE_READS=true ./scripts/deploy-cluster.sh --no-build >/dev/null 2>&1; restart
for p in 8 16 32; do $N drop-caches >/dev/null; echo -n "dodo alone p$p: "; timeout 300 ssh dodo "~/.local/lib/sparknest/read-snapshot.sh $S $p" 2>&1 | tail -1; done
$N io | sed -n '/^dodo/,/^emu/p' | head -9
$N drop-caches >/dev/null; echo "all 7 at once p8:"; timeout 600 benchmarks/tools/read-all-hosts.sh "$S" 8 $HOSTS
echo "== spread off (each host its own disk)"
SPARKNEST_BALANCE_READS=false ./scripts/deploy-cluster.sh --no-build >/dev/null 2>&1; restart
$N drop-caches >/dev/null; echo "all 7 at once p8:"; timeout 600 benchmarks/tools/read-all-hosts.sh "$S" 8 $HOSTS
$N drop-caches >/dev/null; echo -n "raptor alone p16 (spread off): "; timeout 300 ssh raptor "~/.local/lib/sparknest/read-snapshot.sh $S 16" 2>&1 | tail -1
echo "== spread on again"
SPARKNEST_BALANCE_READS=true ./scripts/deploy-cluster.sh --no-build >/dev/null 2>&1; restart
$N drop-caches >/dev/null; echo -n "raptor alone p16 (spread on): "; timeout 300 ssh raptor "~/.local/lib/sparknest/read-snapshot.sh $S 16" 2>&1 | tail -1
echo done
