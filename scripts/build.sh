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

# The web UI (crates/nest-api/ui, Svelte) builds to one self-contained
# crates/nest-api/web/index.html that the daemon embeds. It is committed, so
# building without node works; with node, it is rebuilt when sources change.
ui=crates/nest-api/ui
web_ui() {
  command -v npm >/dev/null || return 0
  [ -d "$ui/node_modules" ] || npm --prefix "$ui" ci --no-audit --no-fund
  if [ -n "$(find "$ui/src" "$ui/index.html" "$ui/package.json" "$ui/vite.config.ts" -newer crates/nest-api/web/index.html -print -quit 2>/dev/null)" ]; then
    npm --prefix "$ui" run build
  fi
}

mode=release
case "${1:-}" in
  --debug) mode=debug ;;
  --check)
    if command -v npm >/dev/null; then
      web_ui
      npm --prefix "$ui" run check
    fi
    if command -v python3 >/dev/null && command -v cc >/dev/null; then
      python3 -m unittest discover -s tools/drop-page-cache -p 'test_*.py'
    fi
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace
    exit 0 ;;
  "") ;;
  *) echo "usage: $0 [--debug|--check]" >&2; exit 2 ;;
esac

web_ui
arch=$(uname -m)
flags=()
[ "$mode" = release ] && flags+=(--release)
cargo build "${flags[@]}" -p sparknestd -p nest-cli
out="dist/$arch"
mkdir -p "$out"
install -m 0755 "target/$mode/sparknestd" "target/$mode/nest" "$out/"

echo "built $mode binaries in $out"
