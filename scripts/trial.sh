#!/usr/bin/env bash
# The trial cluster (scripts/cluster.env): start|stop|status|logs HOST|wipe.
exec "$(dirname "$0")/cluster.sh" "$@"
