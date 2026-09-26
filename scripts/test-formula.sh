#!/usr/bin/env bash
# Build, test and audit the formula rendered by scripts/release.sh from the
# local source tarball, through a throwaway local tap. Installs into Homebrew
# temporarily; the keg and the tap are removed on exit.
#   scripts/test-formula.sh dist/release/v<version>
set -euo pipefail
[ $# = 1 ] || { echo "usage: $0 dist/release/v<version>" >&2; exit 2; }
dir=$(realpath "$1")
eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"
export HOMEBREW_NO_AUTO_UPDATE=1 HOMEBREW_NO_ENV_HINTS=1

src=$(find "$dir" -maxdepth 1 -name 'sparknest-*.tar.gz' ! -name '*-linux-*' | head -1)
[ -f "$src" ] && [ -f "$dir/sparknest.rb" ] || { echo "no release in $dir" >&2; exit 1; }
if brew list --formula sparknest >/dev/null 2>&1; then
  echo "sparknest is already installed from a tap; not touching it" >&2; exit 1
fi

tap=tpurtell/sparknest-local
cleanup() {
  brew uninstall --force "$tap/sparknest" >/dev/null 2>&1 || true
  brew untap --force "$tap" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup
brew tap-new --no-git "$tap" >/dev/null
formula="$(brew --repository "$tap")/Formula/sparknest.rb"
sed "s#^  url .*#  url \"file://$src\"#" "$dir/sparknest.rb" > "$formula"

brew style "$formula"
brew install --build-from-source "$tap/sparknest"
brew test "$tap/sparknest"
brew linkage --test "$tap/sparknest"
abi=$(dirname "$0")/../../local-ai-tap/scripts/check-bottle-abi
[ -x "$abi" ] && "$abi" "$tap/sparknest"
echo "formula OK"
