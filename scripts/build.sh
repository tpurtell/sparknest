#!/usr/bin/env bash
# Build sparknest for the host architecture.
#   scripts/build.sh            release build of daemon + CLI (+ web if present)
#   scripts/build.sh --debug    debug build
#   scripts/build.sh --check    fmt check, clippy -D warnings, tests
# Output: dist/<arch>/{sparknestd,nest}
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -x /home/linuxbrew/.linuxbrew/bin/brew ]; then
  eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"
fi

mode=release
case "${1:-}" in
  --debug) mode=debug ;;
  --check)
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace
    exit 0 ;;
  "") ;;
  *) echo "usage: $0 [--debug|--check]" >&2; exit 2 ;;
esac

arch=$(uname -m)
flags=()
[ "$mode" = release ] && flags+=(--release)
cargo build "${flags[@]}" -p sparknestd -p nest-cli
out="dist/$arch"
mkdir -p "$out"
install -m 0755 "target/$mode/sparknestd" "target/$mode/nest" "$out/"

if [ -f web/package.json ] && command -v npm >/dev/null; then
  (cd web && npm ci --silent && npm run build --silent)
  rm -rf "$out/web" && cp -r web/dist "$out/web"
fi
echo "built $mode binaries in $out"
