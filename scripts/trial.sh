#!/usr/bin/env bash
# Run the trial cluster on all hosts without systemd (detached over SSH).
#   scripts/trial.sh start      start every daemon (raptor bootstraps if new)
#   scripts/trial.sh stop       stop every daemon and unmount
#   scripts/trial.sh status     process, mount and last log lines per host
#   scripts/trial.sh logs HOST  tail a host's daemon log
#   scripts/trial.sh wipe       stop, then delete all trial state (asks first)
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/cluster.env

paths() { # host -> "bin state mnt"
  local home; home=$(ssh "$1" 'echo $HOME')
  echo "${SPARKNEST_BIN_DIR/#\~/$home} ${SPARKNEST_STATE_DIR/#\~/$home} ${SPARKNEST_MOUNT/#\~/$home}"
}

each() { for h in "${SPARKNEST_HOSTS[@]}"; do IFS=: read -r name id ip arch <<<"$h"; "$@" "$name" "$id"; done; }

start_one() {
  local name=$1 id=$2 bin state mnt boot=""
  read -r bin state mnt <<<"$(paths "$name")"
  [ "$id" = 1 ] && boot="--bootstrap"
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
  wipe)
    read -r -p "Delete all trial state on every host? [y/N] " ok
    [ "$ok" = y ] || exit 1
    each stop_one
    for h in "${SPARKNEST_HOSTS[@]}"; do IFS=: read -r name _ <<<"$h"; read -r bin state mnt <<<"$(paths "$name")"; ssh "$name" "rm -rf '$state'/*.sqlite* '$state/objects' '$state/staging' '$state/snapshots' '$state/daemon.log'"; done ;;
  *) sed -n '2,8p' "$0"; exit 2 ;;
esac
