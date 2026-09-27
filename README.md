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

## Example: moving a real cluster in

How sparknest went into service on raptor (an x86 workstation with a
400 Gb port) and six DGX Sparks, starting from their existing Hugging Face
caches: each host's `~/.cache/huggingface`, and a 5.9 TB cache on a drive of
raptor's that was about to be reformatted. Hosts, ids and fabric addresses
live in `scripts/cluster.env`.

**1. Install and configure.** From a clean checkout on raptor (hosts reached
over SSH; nothing published, the formula builds a tarball of `HEAD` from a
local tap on each host):

```sh
scripts/install-cluster.sh all    # brew-build on every host, write /srv/sparknest/node.toml
```

Once per host, as root: the directories, and the unit that runs the daemon
as you with `CAP_SYS_ADMIN` (for passthrough):

```sh
sudo mkdir -p /srv/sparknest /mnt/sparknest && sudo chown "$USER": /srv/sparknest /mnt/sparknest
sudo cp ~/.config/sparknest/sparknestd@sparknest.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable sparknestd@sparknest
```

Bootstrap the first host once, then start everything:

```sh
sparknestd --config /srv/sparknest/node.toml --bootstrap   # on raptor; Ctrl-C at "sparknestd running"
sudo systemctl start sparknestd@sparknest                  # every host
nest status                                                # 7/7 serving
nest store add models /mnt/models/sparknest --gateways raptor   # archive store (a NAS share)
nest group set sparks ostrich dodo emu kiwi rhea moa
```

**2. Bring each host's cache in, without touching it.** On every host at
once (safe to run in parallel); blobs are hard-linked into the store, so
nothing is copied:

```sh
nest hf import ~/.cache/huggingface --wait
export HF_HOME=/mnt/sparknest/hf-home      # try the models from sparknest
```

**3. Make it permanent.** The same import with `--move`: already-imported
blobs are recognized, everything is verified again, and each cache is
replaced by a symlink into sparknest's hub:

```sh
nest hf import ~/.cache/huggingface --move --wait
```

**4. One copy of each model.** The hosts' caches overlapped, so most models
now had several copies. A free-space target above what any host can reach
removes every redundant copy (never the last); nothing is archived without
`--to`:

```sh
nest plan --free @all=8TiB     # review the proposal, then:
nest plan apply ID --wait
```

**5. The big cache, spread over the cluster.** raptor's 5.9 TB cache did
not fit on raptor. `--spread` copies blobs into raptor's store in ~64 GiB
batches and hands each to the host with the most free space; the drive is
only read, once, and blobs the cluster already has are skipped:

```sh
nest hf import /mnt/scratch/hf_cache --spread --wait
```

**6. Copies where they pay.** After a week or so of using the models,
the usage each host recorded (what it opened, and what it read over the
network) drives the speedup planner, which proposes copies on the hosts
that keep reading a model from elsewhere:

```sh
nest plan --speedup 1w         # review, then: nest plan apply ID --wait
```

**7. Later.** After reformatting that drive it becomes a second archive
store (`nest store add scratch /mnt/scratch/sparknest --gateways raptor`),
and the models on the NAS are imported, spread, then offloaded back to its
archive store (`nest offload SELECTOR --store models`), leaving the cluster
free to pull any of them back on demand.
