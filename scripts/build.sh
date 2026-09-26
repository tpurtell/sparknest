#!/usr/bin/env bash
# Build sparknest for the host architecture.
#   scripts/build.sh            release build of daemon + CLI (web UI is embedded)
#   scripts/build.sh --debug    debug build
#   scripts/build.sh --check    fmt check, clippy -D warnings, tests
# Output: dist/<arch>/{sparknestd,nest}
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -x /home/linuxbrew/.linuxbrew/bin/brew ]; then
  eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"
fi
# rustup toolchains (used on arm64 build hosts) take precedence when present.
[ -d "$HOME/.cargo/bin" ] && export PATH="$HOME/.cargo/bin:$PATH"
command -v cargo >/dev/null || { echo "cargo not found (see docs/TOOLING.md)" >&2; exit 1; }

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

echo "built $mode binaries in $out"
