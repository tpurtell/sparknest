# Releasing sparknest

Releases are cut from a clean, pushed `main` on raptor (x86_64); the arm64
build runs natively on `SPARKNEST_ARM_BUILDER` (`scripts/cluster.env`).

1. Bump `version` in the workspace `Cargo.toml`, run `scripts/build.sh --check`,
   commit and push.
2. `scripts/release.sh` builds `dist/release/v<version>/`: the source tarball
   (what Homebrew builds), a binary tarball per architecture, `SHA256SUMS`,
   `NOTES.md` and `sparknest.rb` (the formula rendered from
   `packaging/homebrew/sparknest.rb.in`). It refuses a binary that needs a
   glibc newer than 2.39, Homebrew's bottle baseline.
3. `scripts/test-formula.sh dist/release/v<version>` builds and tests that
   formula from the local source tarball through a throwaway local tap, then
   uninstalls it.
4. **Publishing (owner's decision).** The GitHub repository must be public
   for Homebrew to download the source. Then `scripts/release.sh --draft`
   tags `v<version>` and uploads a draft release; review it and publish
   with `gh release edit v<version> --draft=false`.
   When ostrich is busy, build arm64 elsewhere, and beside running jobs
   at low priority: `SPARKNEST_ARM_BUILDER=rhea SPARKNEST_ARM_NICE=19
   SPARKNEST_ARM_JOBS=4 scripts/release.sh --draft`.
5. Copy `sparknest.rb` to `Formula/` in `tpurtell/local-ai-tap` and follow
   that tap's `RELEASING.md`: native bottles on raptor and a Spark, a
   bottles release, `brew bottle --merge`, and install tests on the fleet.
   To bottle only sparknest, pass `sparknest` after the output directory to
   `build-bottles-native` and `--formula sparknest` to `prepare-bottles`.
   A host with sparknest from the local tap (`scripts/install-cluster.sh`)
   must drop that keg first (`HOMEBREW_NO_AUTOREMOVE=1 brew uninstall
   --ignore-dependencies sparknest`; without the variable brew also removes
   rust). A running daemon keeps its binary until it restarts.
