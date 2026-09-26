#!/usr/bin/env bash
# Chaos soak on the trial cluster. Every host runs scripts/soak/worker.py
# against its mount while this loop SIGKILLs one daemon (sometimes two) at a
# random moment and restarts it. At the end every file is re-read and
# checked from two hosts. Trial data only (<mount>/soak).
#   scripts/soak/run.sh [SECONDS]      default 900
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/cluster.env
secs=${1:-900}
nest=${NEST:-$HOME/.local/lib/sparknest/nest}
out="dist/soak/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$out"
log() { echo "$(date +%T) $*" | tee -a "$out/events.log"; }

hosts=(); declare -A MNT STATE
for h in "${SPARKNEST_HOSTS[@]}"; do
  IFS=: read -r name _ <<<"$h"; hosts+=("$name")
  home=$(ssh "$name" 'echo $HOME')
  MNT[$name]=${SPARKNEST_MOUNT/#\~/$home}; STATE[$name]=${SPARKNEST_STATE_DIR/#\~/$home}
done

"$nest" rule set soak /soak --hosts raptor,rhea --auto >/dev/null
mkdir -p "${MNT[raptor]}/soak"
for h in "${hosts[@]}"; do
  ssh "$h" "mkdir -p ~/sparknest-test/soak && rm -f ~/sparknest-test/soak/result.json"
  scp -q scripts/soak/worker.py "$h:sparknest-test/soak/worker.py"
  ssh "$h" "setsid -f python3 ~/sparknest-test/soak/worker.py '${MNT[$h]}' $h $secs ~/sparknest-test/soak/result.json > ~/sparknest-test/soak/worker.log 2>&1 < /dev/null"
done
log "workers started on ${hosts[*]} for ${secs}s"

kill_one() { ssh "$1" "pkill -KILL -f '[s]parknestd --config ${STATE[$1]}/node.toml'" || true; log "SIGKILL $1"; }
end=$(( $(date +%s) + secs ))
kills=0
while [ "$(date +%s)" -lt $(( end - 30 )) ]; do
  sleep $(( 20 + RANDOM % 30 ))
  v1=${hosts[RANDOM % ${#hosts[@]}]}; kill_one "$v1"; kills=$((kills+1))
  if [ $(( RANDOM % 5 )) = 0 ]; then
    v2=${hosts[RANDOM % ${#hosts[@]}]}
    [ "$v2" != "$v1" ] && { sleep $(( RANDOM % 4 )); kill_one "$v2"; kills=$((kills+1)); }
  fi
  sleep $(( 5 + RANDOM % 20 ))
  scripts/trial.sh start > /dev/null; log "restarted"
done
log "chaos done ($kills kills); waiting for workers"
for h in "${hosts[@]}"; do
  for _ in $(seq 180); do ssh "$h" "test -s ~/sparknest-test/soak/result.json" && break; sleep 2; done
  scp -q "$h:sparknest-test/soak/result.json" "$out/$h.json" || log "no result from $h"
done
scripts/trial.sh start > /dev/null
for _ in $(seq 60); do
  [ "$("$nest" status | awk 'NR>2 && $2=="yes"' | wc -l)" = "${#hosts[@]}" ] && break; sleep 2
done
"$nest" status | tee -a "$out/events.log"

verify='
import hashlib, os, sys, json
root = sys.argv[1]; ok = bad = err = 0; bad_list = []
for h in sorted(os.listdir(root)):
    for name in sorted(os.listdir(os.path.join(root, h))):
        if name.startswith("."): continue
        p = os.path.join(root, h, name)
        try:
            d = hashlib.sha256(open(p, "rb").read()).hexdigest()
        except OSError as e:
            err += 1; bad_list.append(f"{p}: {e}"); continue
        if d[:32] == name.split("-", 1)[1]: ok += 1
        else: bad += 1; bad_list.append(p)
print(json.dumps(dict(ok=ok, corrupt=bad, errors=err, problems=bad_list[:20])))
'
for h in raptor moa; do
  log "verify from $h: $(ssh "$h" "python3 -c '$verify' '${MNT[$h]}/soak'")"
done
python3 - "$out" <<'PY'
import json, sys, glob, os
tot = {}
for f in sorted(glob.glob(os.path.join(sys.argv[1], "*.json"))):
    r = json.load(open(f))
    for k, v in r["stats"].items(): tot[k] = tot.get(k, 0) + v
    for c in r["corrupt"]: print("CORRUPT", r["host"], c)
    for k, v in sorted(r["errors"].items(), key=lambda x: -x[1])[:4]: print(f"  {r['host']:8} {v:5}  {k}")
print(json.dumps(tot))
PY
