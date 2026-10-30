#!/bin/bash
# Remote 4 KiB random reads on raptor of a sealed trial file held only on
# Sparks (ostrich, dodo): fio io_uring, iodepth 128, every read async (as
# cuteafd's reader), reader on 8, 16, 64 CPUs. Run under the sparks hwlock.
# Usage: remote-randread.sh LABEL BIN_DIR_X86 BIN_DIR_ARM|-
set -u
L=$1; X=$2; A=$3
D=$(cd "$(dirname "$0")" && pwd)
cd ~/Developer/sparknest
export SSH_AUTH_SOCK=/home/tj/.ssh/agent/s.KCgk0ZT1TX.sshd.0ESOfXHf8l
OUT=~/build/remote-randread-$L.txt; : > $OUT
scripts/trial.sh stop >/dev/null 2>&1
# Binaries: raptor gets $X, every Spark gets $A.
cp $X/sparknestd $X/nest ~/.local/lib/sparknest/
[ "$A" = - ] || for h in ostrich dodo emu kiwi rhea moa; do rsync -a $A/sparknestd $A/nest $h:.local/lib/sparknest/; done
scripts/trial.sh start >/dev/null
M=~/sparknest-test/mnt
C=~/sparknest-test/state/node.toml
for i in $(seq 90); do mountpoint -q $M && SPARKNEST_CONFIG=$C nest status >/dev/null 2>&1 && break; sleep 1; done
export SPARKNEST_CONFIG=$C
F=bench-sqlfix/table
if ! [ -e $M/$F ]; then
  ssh ostrich "mkdir -p ~/sparknest-test/mnt/bench-sqlfix && head -c 4G /dev/urandom > ~/sparknest-test/mnt/$F && sync"
  nest seal $M/$F >/dev/null
  nest replicate --hosts dodo --wait /$F >/dev/null
fi
nest evict --hosts raptor /$F >/dev/null 2>&1 || true
nest where /$F 2>&1 | tail -3 >> $OUT
echo "== $L $(SPARKNEST_CONFIG=$C nest status --json | python3 -c 'import json,sys;print(json.load(sys.stdin)["me"])' 2>/dev/null)" >> $OUT
P=$(pgrep -f "sparknestd --config $C")
for cpus in 0-7 0-15 0-63; do
  nest drop-caches >/dev/null 2>&1
  t0=$(awk '{print $14, $15}' /proc/$P/stat)
  r=$(taskset -c $cpus fio --name=rr --filename=$M/$F --rw=randread --bs=4k --ioengine=io_uring --force_async=1 --iodepth=128 \
      --numjobs=1 --time_based --runtime=15 --ramp_time=2 --norandommap --group_reporting \
      --output-format=json 2>/dev/null | python3 "$D/fio-parse.py")
  t1=$(awk '{print $14, $15}' /proc/$P/stat)
  cpu=$(echo "$t0 $t1" | awk -v hz=$(getconf CLK_TCK) '{printf "sparknestd usr %.0f%% sys %.0f%%", ($3-$1)/hz/17*100, ($4-$2)/hz/17*100}')
  echo "cpus $cpus: $r  $cpu" >> $OUT
done
scripts/trial.sh stop >/dev/null 2>&1
cat $OUT
