# sparknest

A distributed filesystem for a small RDMA cluster, built first to hold one
shared Hugging Face cache for every machine and second to be a good shared
folder. One namespace is mounted on every host; files are whole objects,
and you decide where copies live.

- **Whole-file copies, explicit placement.** A file lives on the host that
  wrote it. Rules, host groups and one-off commands put full copies
  elsewhere. Reading never silently replicates.
- **Fabric speed.** Remote reads and replication stream over RoCE v2 with
  one queue pair per rail, which suits dual-rail DGX Spark NICs.
  Sealed local files are served by FUSE passthrough at NVMe speed.
- **Coherent.** The first writer becomes the file's owner and every host
  sees its writes; stale copies are deleted, never served. Metadata is
  Raft-replicated SQLite, so a seven-host cluster keeps working with three
  hosts down.
- **Hugging Face aware.** Understands both cache layouts, seals blobs when
  a download completes, selects by `hf:org/model@revision`, and adopts
  existing caches without copying.
- **Archives and space.** NAS shares and spare disks become archive
  stores for offload, recall, versioned backups and metadata snapshots.
  Free-space plans say what to remove or offload, and apply it safely.
- **One daemon, one API.** `sparknestd` on every host; the `nest` CLI and
  the web UI use the same management API.

Measured on raptor plus six DGX Sparks at 100G per rail
(`benchmarks/`): a 35 GiB model replicates host to host in 9 s (93% of
rdmasync), reads from a host without a copy at 5 GB/s over the fabric, and
from a local copy at 9.6 GB/s cold and 85 GB/s warm.

## Install

```sh
brew install tpurtell/local-ai/sparknest
```

Linux x86_64 and arm64. Binary tarballs are on the releases page. Setup,
configuration and service units: [`docs/INSTALL.md`](docs/INSTALL.md).

## A short tour

```sh
nest status                                   # hosts, stores, leader
nest ls /hf-home/hub                          # where each file's copies are
nest where hf:Qwen/Qwen3-8B                   # hosts with a complete copy
nest replicate hf:Qwen/Qwen3-8B --hosts @sparks --wait
nest rule set qwen hf:Qwen/Qwen3-8B --hosts @sparks --auto
nest offload hf:old/model --store nas --wait  # archive, then free local space
nest plan --free @sparks=400GiB               # propose; then: nest plan apply ID
nest ui                                       # link to the web UI
```

## Documentation

- [`docs/INSTALL.md`](docs/INSTALL.md): installing and running a cluster.
- [`docs/MIGRATION.md`](docs/MIGRATION.md): moving existing caches in.
- [`PROPOSAL.md`](PROPOSAL.md): the design.
  [`docs/DECISIONS.md`](docs/DECISIONS.md): why it is built this way.
- [`ROADMAP.md`](ROADMAP.md), [`docs/PROGRESS.md`](docs/PROGRESS.md),
  [`benchmarks/`](benchmarks/): plan, history and measurements.
- [`AGENTS.md`](AGENTS.md): working on the code;
  [`docs/RELEASING.md`](docs/RELEASING.md): cutting a release.

Rust workspace; `scripts/build.sh --check` runs formatting, lints and the
test suite. Licensed under MIT or Apache-2.0, at your option.
