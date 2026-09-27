#!/usr/bin/env bash
# Build a release from the committed HEAD:
#   dist/release/v<version>/
#     sparknest-<version>.tar.gz               source (git archive; the formula builds this)
#     sparknest-<version>-linux-x86_64.tar.gz  native build on this host
#     sparknest-<version>-linux-aarch64.tar.gz native build on $SPARKNEST_ARM_BUILDER
#     SHA256SUMS, NOTES.md, sparknest.rb       (formula rendered for this version)
#
#   scripts/release.sh           build and verify only (nothing leaves this machine
#                                except the source sent to the arm builder)
#   scripts/release.sh --draft   also tag v<version>, push the tag and create a
#                                *draft* GitHub release with the tarballs.
# Publishing the draft is deliberately manual; see docs/RELEASING.md.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$(pwd)
source scripts/cluster.env
if [ -x /home/linuxbrew/.linuxbrew/bin/brew ]; then
  eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"
fi
[ -d "$HOME/.cargo/bin" ] && export PATH="$HOME/.cargo/bin:$PATH"

draft=0
for a in "$@"; do
  case "$a" in
    --draft) draft=1 ;;
    *) echo "usage: $0 [--draft]" >&2; exit 2 ;;
  esac
done
die() { echo "release: $*" >&2; exit 1; }

GLIBC_CEILING=2.39   # Homebrew's Linux bottle baseline (local-ai-tap policy)
REPO_URL=https://github.com/tpurtell/sparknest

version=$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -1)
[ -n "$version" ] || die "no workspace version in Cargo.toml"
tag="v$version"
name="sparknest-$version"
[ -z "$(git status --porcelain)" ] || die "working tree is not clean; commit first"
commit=$(git rev-parse HEAD)
out="dist/release/$tag"
rm -rf "$out" && mkdir -p "$out/stage"

echo "== source $name.tar.gz ($commit)"
git archive --format=tar --prefix="$name/" HEAD | gzip -n > "$out/$name.tar.gz"

# Newest GLIBC_x.y symbol version an ELF file requires.
glibc_max() { readelf --version-info "$1" | grep -o 'GLIBC_[0-9.]*' | sed 's/GLIBC_//' | sort -uV | tail -1; }

package() {
  local arch=$1 bindir=$2 pkg="$name-linux-$1"
  local stage="$out/stage/$pkg"
  mkdir -p "$stage/bin" "$stage/libexec/sparknest" "$stage/share/sparknest" "$stage/share/doc/sparknest"
  install -m 0755 "$bindir/sparknestd" "$bindir/nest" "$stage/bin/"
  install -m 0755 tools/drop-page-cache/sparknest-drop-page-cache "$stage/bin/"
  install -m 0644 tools/drop-page-cache/drop-page-cache.c "$stage/libexec/sparknest/"
  cp -r packaging/config packaging/systemd "$stage/share/sparknest/"
  cp README.md docs/INSTALL.md LICENSE-MIT LICENSE-APACHE "$stage/share/doc/sparknest/"
  for f in "$stage"/bin/sparknestd "$stage"/bin/nest; do
    local m; m=$(glibc_max "$f")
    [ "$(printf '%s\n%s\n' "$m" "$GLIBC_CEILING" | sort -V | tail -1)" = "$GLIBC_CEILING" ] \
      || die "$f requires GLIBC_$m (ceiling $GLIBC_CEILING)"
    echo "   $(basename "$f") ($arch): GLIBC_$m"
  done
  tar -C "$out/stage" --owner=0 --group=0 --sort=name -czf "$out/$pkg.tar.gz" "$pkg"
}

echo "== build x86_64 (here)"
[ "$(uname -m)" = x86_64 ] || die "run on the x86_64 host (raptor)"
src="dist/release/build-x86_64"
rm -rf "$src" && mkdir -p "$src"
tar -C "$src" -xzf "$out/$name.tar.gz"
( cd "$src/$name" && CARGO_TARGET_DIR="$root/target/release-pkg" \
    cargo build --release --locked -p sparknestd -p nest-cli )
bin_x86="target/release-pkg/release"
"$bin_x86/nest" --version | grep -q "$version" || die "x86_64 nest --version mismatch"
package x86_64 "$bin_x86"

echo "== build aarch64 (on $SPARKNEST_ARM_BUILDER)"
rdir="$SPARKNEST_ARM_BUILD_DIR-release"
ssh "$SPARKNEST_ARM_BUILDER" "mkdir -p $rdir && rm -rf $rdir/$name"
scp -q "$out/$name.tar.gz" "$SPARKNEST_ARM_BUILDER:$rdir/"
ssh "$SPARKNEST_ARM_BUILDER" "set -e
  [ -x /home/linuxbrew/.linuxbrew/bin/brew ] && eval \"\$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)\"
  export PATH=\$HOME/.cargo/bin:\$PATH
  cd $rdir && tar -xzf $name.tar.gz && cd $name
  CARGO_TARGET_DIR=$rdir/target cargo build --release --locked -p sparknestd -p nest-cli
  $rdir/target/release/nest --version | grep -q '$version'"
mkdir -p "$out/stage/bin-aarch64"
for b in sparknestd nest; do
  scp -q "$SPARKNEST_ARM_BUILDER:$rdir/target/release/$b" "$out/stage/bin-aarch64/"
done
package aarch64 "$out/stage/bin-aarch64"

echo "== checksums, formula, notes"
( cd "$out" && sha256sum -- *.tar.gz > SHA256SUMS )
src_sha=$(sha256sum "$out/$name.tar.gz" | cut -d' ' -f1)
sed -e "s#@URL@#$REPO_URL/releases/download/$tag/$name.tar.gz#" -e "s#@SHA256@#$src_sha#" \
  packaging/homebrew/sparknest.rb.in | sed '1,3d' > "$out/sparknest.rb"
cat > "$out/NOTES.md" <<NOTES
sparknest $version ($commit)

Install with Homebrew (Linux x86_64 and arm64):

    brew install tpurtell/local-ai/sparknest

or unpack a binary tarball; binaries need glibc $GLIBC_CEILING or newer and
the system libibverbs (rdma-core). Setup: share/doc/sparknest/INSTALL.md.

SHA-256:

$(sed 's/^/    /' "$out/SHA256SUMS")
NOTES
rm -rf "$out/stage"
ls -l "$out"

if [ "$draft" = 1 ]; then
  echo "== tag $tag and draft release"
  git tag -a "$tag" -m "sparknest $version" "$commit"
  git push origin "$tag"
  gh release create "$tag" "$out"/*.tar.gz "$out/SHA256SUMS" --verify-tag --draft \
    --title "sparknest $version" --notes-file "$out/NOTES.md"
  echo "draft created; review it, then: gh release edit $tag --draft=false"
fi
