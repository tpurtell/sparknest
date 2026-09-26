# Required tooling

Everything the build, test loop, or deployment needs, with how to install it.
Add a row the moment a new tool is introduced.

| Tool | Where | Install | Purpose |
|---|---|---|---|
| rustup + stable toolchain | raptor, ostrich (build hosts) | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh -s -- -y` | native amd64/arm64 builds; toolchain pinned by `rust-toolchain.toml` |
| libibverbs-dev, librdmacm-dev | all nodes | already installed (rdma-core) | verbs FFI link target |
| fusermount3 | all nodes | already installed (`fuse3`) | mounting as user |
| node ≥ 24, npm | raptor | already installed | build `web/` bundle |
| bindgen (cargo) + libclang | raptor only | `cargo install bindgen-cli`; libclang-21 present | regenerate vendored verbs bindings |
| rdmasync, rdmapipe | all nodes | `brew install tpurtell/local-ai/rdmasync rdmapipe` or existing `~/.local/bin` | moving test data, throughput baselines |
| perftest (`ib_write_bw`) | all nodes | already installed on raptor; `apt`/brew on Sparks if missing | raw fabric ceilings |

Homebrew (`/home/linuxbrew/.linuxbrew`) is the preferred way to add tools.
