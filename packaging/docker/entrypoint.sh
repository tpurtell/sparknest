#!/bin/sh
# Start sparknestd from the state dir's node.toml, or run what was asked
# (`docker run IMAGE nest status`, `... sparknestd fsck`).
# SPDX-License-Identifier: MIT OR Apache-2.0
set -eu

if [ "$#" -gt 0 ]; then
  exec "$@"
fi

cfg="${SPARKNEST_CONFIG:-/srv/sparknest/node.toml}"
if [ ! -f "$cfg" ]; then
  cat >&2 <<EOF
sparknest: no $cfg.
Write it on the host, in the folder mounted at /srv/sparknest, from
/opt/sparknest/share/sparknest/config/node-container.toml.sample:
  docker run --rm IMAGE cat /opt/sparknest/share/sparknest/config/node-container.toml.sample
See packaging/docker/README.md.
EOF
  exit 64
fi
sparknestd --config "$cfg" --check || {
  echo "sparknest: $cfg does not check out (above)" >&2
  exit 78
}
secret=$(sed -n 's/^[[:space:]]*secret_file[[:space:]]*=[[:space:]]*"\(.*\)".*/\1/p' "$cfg" | head -1)
if [ -n "$secret" ] && [ ! -r "$secret" ]; then
  echo "sparknest: no cluster secret at $secret: copy the cluster's cluster.secret there (mode 0600)" >&2
  exit 78
fi
exec sparknestd --config "$cfg"
