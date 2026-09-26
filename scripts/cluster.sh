#!/usr/bin/env bash
# Run a cluster's daemons on all hosts without systemd (detached over SSH).
# The cluster is described by $SPARKNEST_ENV (default scripts/cluster.env, the
# trial); scripts/cluster-main.env is the real deployment.
#   scripts/cluster.sh start      start every daemon (the trial bootstraps itself)
#   scripts/cluster.sh stop       stop every daemon and unmount
#   scripts/cluster.sh status     process, mount and last log lines per host
#   scripts/cluster.sh logs HOST  tail a host's daemon log
#   scripts/cluster.sh bootstrap  found the cluster on the first host, once
#   scripts/cluster.sh wipe       stop, then delete all cluster state (asks first)
set -euo pipefail
cd "$(dirname "$0")/.."
source "${SPARKNEST_ENV:-scripts/cluster.env}"

paths() { # host -> "bin state mnt"
  local home; home=$(ssh "$1" 'echo $HOME')
  echo "${SPARKNEST_BIN_DIR/#\~/$home} ${SPARKNEST_STATE_DIR/#\~/$home} ${SPARKNEST_MOUNT/#\~/$home}"
}

each() { for h in "${SPARKNEST_HOSTS[@]}"; do IFS=: read -r name id ip arch <<<"$h"; "$@" "$name" "$id"; done; }

start_one() {
  local name=$1 id=$2 bin state mnt boot=""
  read -r bin state mnt <<<"$(paths "$name")"
  [ "$id" = 1 ] && [ "${SPARKNEST_AUTO_BOOTSTRAP:-0}" = 1 ] && boot="--bootstrap"
  [ "${3:-}" = bootstrap ] && boot="--bootstrap"
  if ssh "$name" "pgrep -f '[s]parknestd --config $state/node.toml' >/dev/null"; then
    echo "$name: already running"; return
  fi
  # setsid -f forks and returns at once; the redirects keep SSH's streams
  # out of the daemon so the session can close.
  ssh "$name" "fusermount3 -uz '$mnt' 2>/dev/null; cd '$state' && RUST_LOG=\${RUST_LOG:-info,openraft=warn} setsid -f '$bin/sparknestd' --config '$state/node.toml' $boot >> '$state/daemon.log' 2>&1 < /dev/null"
  echo "$name: started"
}

stop_one() {
  local name=$1 bin state mnt
  read -r bin state mnt <<<"$(paths "$name")"
  ssh "$name" "pkill -TERM -f '[s]parknestd --config $state/node.toml' || true; for i in 1 2 3 4 5 6 7 8 9 10; do pgrep -f '[s]parknestd --config $state/node.toml' >/dev/null || break; sleep 0.5; done; pkill -KILL -f '[s]parknestd --config $state/node.toml' || true; fusermount3 -uz '$mnt' 2>/dev/null || true"
  echo "$name: stopped"
}

status_one() {
  local name=$1 bin state mnt
  read -r bin state mnt <<<"$(paths "$name")"
  ssh "$name" "printf '%-8s ' $name; if pgrep -f '[s]parknestd --config $state/node.toml' >/dev/null; then printf 'running '; else printf 'STOPPED '; fi; mount | grep -q ' $mnt ' && printf 'mounted ' || printf 'unmounted '; tail -1 '$state/daemon.log' 2>/dev/null | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-120"
}

case "${1:-}" in
  start) each start_one ;;
  stop) each stop_one ;;
  status) each status_one ;;
  logs) read -r bin state mnt <<<"$(paths "$2")"; ssh "$2" "tail -n ${3:-50} '$state/daemon.log'" ;;
  bootstrap)
    IFS=: read -r name id _ <<<"${SPARKNEST_HOSTS[0]}"
    read -r bin state mnt <<<"$(paths "$name")"
    if ssh "$name" "test -e '$state/raft.sqlite'"; then
      echo "$name already has Raft state in $state; refusing to found a second cluster" >&2; exit 1
    fi
    start_one "$name" "$id" bootstrap
    for i in $(seq 60); do
      ssh "$name" "grep -q 'sparknestd running' '$state/daemon.log'" && break
      sleep 1
    done
    ssh "$name" "grep -q 'sparknestd running' '$state/daemon.log'" \
      || { echo "bootstrap did not come up; see: $0 logs $name" >&2; exit 1; }
    stop_one "$name"
    echo "cluster $SPARKNEST_CLUSTER founded on $name; start the services now" ;;
  wipe)
    read -r -p "Delete all $SPARKNEST_CLUSTER state on every host? [y/N] " ok
    [ "$ok" = y ] || exit 1
    each stop_one
    for h in "${SPARKNEST_HOSTS[@]}"; do IFS=: read -r name _ <<<"$h"; read -r bin state mnt <<<"$(paths "$name")"; ssh "$name" "rm -rf '$state'/*.sqlite* '$state/objects' '$state/staging' '$state/snapshots' '$state/daemon.log'"; done ;;
  *) sed -n '2,11p' "$0"; exit 2 ;;
esac
