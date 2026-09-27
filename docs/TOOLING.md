# Required tooling

Everything the build, test loop, or deployment needs, with how to install it.
Add a row the moment a new tool is introduced.

| Tool | Where | Install | Purpose |
|---|---|---|---|
| Rust 1.98.1 | raptor | Homebrew `rust` (already installed) | native amd64 builds |
| Rust 1.98.1 | ostrich (arm64 build host) | rustup: `curl -sSf https://sh.rustup.rs \| sh -s -- -y --profile minimal --default-toolchain 1.98.1 -c clippy -c rustfmt` | native arm64 builds. Homebrew's `rust` bottle depends on a multi-hundred-MB `llvm@22` bottle that downloaded too slowly from ghcr.io, so rustup is used here. |
| libibverbs-dev, librdmacm-dev | all nodes | already installed (rdma-core) | verbs FFI link target |
| fusermount3 | all nodes | already installed (`fuse3`) | mounting as user |
| node ≥ 24, npm | raptor | already installed | web UI: `crates/nest-api/ui` (Svelte 5 + Vite) builds to the committed `crates/nest-api/web/index.html`; `scripts/build.sh` runs `npm ci` and rebuilds when sources change |
| bindgen (cargo) + libclang | raptor only | `cargo install bindgen-cli`; libclang-21 present | regenerate vendored verbs bindings |
| rdmasync, rdmapipe | all nodes | `brew install tpurtell/local-ai/rdmasync rdmapipe` or existing `~/.local/bin` | moving test data, throughput baselines |
| perftest (`ib_write_bw`) | all nodes | already installed on raptor; `apt`/brew on Sparks if missing | raw fabric ceilings |

Homebrew (`/home/linuxbrew/.linuxbrew`) is the preferred way to add tools.

## When ghcr.io downloads stall

Homebrew bottles come from ghcr.io, which sometimes resets long transfers
(seen for the 514 MB `llvm@22` bottle that brew's `rust` needs). Fetch the
blob with resume, then place it in the cache under the name brew expects
(the `.incomplete` file's name without that suffix):

```sh
curl -sSL --http1.1 -C - --retry 5 --retry-all-errors -H "Authorization: Bearer QQ==" \
  -o bottle.tar.gz "https://ghcr.io/v2/homebrew/core/<name>/<version>/blobs/sha256:<digest>"
```
