# Required tooling

Everything the build, test loop, or deployment needs, with how to install it.
Add a row the moment a new tool is introduced.

| Tool | Where | Install | Purpose |
|---|---|---|---|
| Rust 1.98.1 | raptor | Homebrew `rust` (already installed) | native amd64 builds |
| Rust 1.98.1 | ostrich (arm64 build host) | rustup: `curl -sSf https://sh.rustup.rs \| sh -s -- -y --profile minimal --default-toolchain 1.98.1 -c clippy -c rustfmt` | native arm64 builds. Homebrew's `rust` bottle depends on a multi-hundred-MB `llvm@22` bottle that downloaded too slowly from ghcr.io, so rustup is used here. |
| libibverbs-dev, librdmacm-dev | all nodes | already installed (rdma-core) | verbs FFI link target |
| fusermount3 | all nodes | already installed (`fuse3`) | mounting as user |
| node ≥ 24, npm | raptor | already installed | build `web/` bundle |
| bindgen (cargo) + libclang | raptor only | `cargo install bindgen-cli`; libclang-21 present | regenerate vendored verbs bindings |
| rdmasync, rdmapipe | all nodes | `brew install tpurtell/local-ai/rdmasync rdmapipe` or existing `~/.local/bin` | moving test data, throughput baselines |
| perftest (`ib_write_bw`) | all nodes | already installed on raptor; `apt`/brew on Sparks if missing | raw fabric ceilings |

Homebrew (`/home/linuxbrew/.linuxbrew`) is the preferred way to add tools.
