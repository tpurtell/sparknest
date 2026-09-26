# Environment inventory and prerequisites

Surveyed 2026-09-26 from raptor via passwordless SSH to all Sparks. Keep this
file current when hosts change; it is the agent's source of truth for what is
installed where.

## Hosts

| Host | Arch | OS | Kernel | FUSE lib | rdma-core | Root NVMe (free) | HF hub cache |
|---|---|---|---|---|---|---|---|
| raptor | amd64 | Ubuntu 26.04.1 | 7.0.0-34-generic | 3.18.2 | 61.0 | 3.6T (652G) | 2.3T, 23 repos |
| ostrich | arm64 | Ubuntu 24.04.5 | 7.0.0-1019-nvidia | 3.14.0 | 50.0 | 3.6T (1.5T) | 1.7T |
| dodo | arm64 | Ubuntu 24.04 | 7.0.0-1019-nvidia | 3.14.0 | 50.0 | (1.6T) | 1.7T |
| kiwi | arm64 | Ubuntu 24.04 | 6.17.0-1026-nvidia | 3.14.0 | 50.0 | (1.5T) | 1.7T |
| emu | arm64 | Ubuntu 24.04 | 6.17.0-1026-nvidia | 3.14.0 | 50.0 | (1.7T) | 1.6T |
| rhea | arm64 | Ubuntu 24.04 | 7.0.0-1019-nvidia | 3.14.0 | 50.0 | (2.4T) | 1.1T |
| moa | arm64 | Ubuntu 24.04 | 7.0.0-1019-nvidia | 3.14.0 | 50.0 | (2.4T) | 1.1T |

All kernels: `CONFIG_FUSE_FS=y`, `CONFIG_FUSE_PASSTHROUGH=y`,
`CONFIG_FUSE_IO_URING=y`, `CONFIG_FUSE_DAX=y`. `/dev/fuse` is 0666 everywhere.
`fusermount3` present everywhere. Sparks: 20 cores, 121 GiB unified memory.

User account: `tj`, uid 1000 on raptor, uid 1001 on Sparks. `sudo` needs a
password on every host, so the agent cannot perform root steps; see the
one-time root checklist below.

## Fabric

- raptor: `mlx5_0`, RoCE (Ethernet link layer), 400G, `enp1s0np0` with
  10.55.0.22 and 10.55.1.22 on one port. rdmasync docs mention 10.55.0.12 for
  an older raptor address; use `.22`.
- Sparks: one physical cable exposing two RoCE functions: rail 0
  `rocep1s0f0`/`enp1s0f0np0` = 10.55.0.N, rail 1 `roceP2p1s0f0`/`enP2p1s0f0np0`
  = 10.55.1.N, N = 1..6 (ostrich, dodo, emu, kiwi, rhea, moa in IP order:
  .1 ostrich, .2 dodo, .3 emu, .4 kiwi, .5 rhea, .6 moa). The `f1` functions
  are DOWN.
- Switch is currently set to **100G per Spark rail** to keep fans quiet. The
  user can raise it to 200G on request for a benchmark pass. A single Spark
  rail is PCIe-capped near 116 Gb/s regardless; dual rail is required for
  ~195 Gb/s.
- Measured reference (rdmasync benchmarks, 2026-08, 200G): Spark dual-rail raw
  196 Gb/s, rdmasync 168 Gb/s, one rail 94 Gb/s; raptor two-flow raw 160 Gb/s.
- Management/LAN: 172.22.x.x (raptor 172.22.2.12, ostrich 172.22.2.1). NAS
  `aviary` at 172.22.1.10 over 10G.

## Existing mounts relevant to sparknest

- `/mnt/models`: `//aviary/models` CIFS 3.1.1 on all nodes (6.0T, 1.4T free).
  → archive store, all nodes as gateways.
- `/mnt/scratch` on raptor: `/dev/sda2` Samsung 870 7.3T, **ntfs3** (1.5T
  free). Exported as `10.55.0.22:/mnt/scratch` and mounted on Sparks at
  `/mnt/nfs/raptor-scratch` (NFSv4.2 over RDMA). → archive store, raptor as
  the only gateway; user plans to drain it, reformat ext4, re-register.
- Every node exports `/srv/nfs/<host>`; cross-mounted at `/mnt/nfs/<host>` over
  NFS/RDMA. Useful for moving test data around during trials; not part of the
  design.
- `~/.cache/huggingface/hub` on raptor: classic layout (`models--*/blobs`,
  `snapshots`, `refs`, `.no_exist`, `.locks/`), plus a new-style shared-blobs
  root `hub/blobs/` with marker `.huggingface-shared-blobs` and one entry
  `blobs/09/<sha>` + `.refs` (created 2026-09-25 by a client ≥ 1.32; the
  referencing repo has since been deleted). One repo has a `trees/` dir.
  `huggingface_hub` in venvs: 1.24, 1.26, 1.30 (predate shared blobs; the Homebrew
  `hf` CLI is 2.0.0 as of 2026-09-26; venvs that download into the mount need
  ≥ 1.32). `~/.cache/huggingface/xet/` exists (chunk cache,
  disabled by default since hf_xet 1.2; keep local).

## Toolchains

| Tool | raptor | Sparks | Notes |
|---|---|---|---|
| rustc/cargo | 1.98.1 (Homebrew, no rustup) | missing | install rustup on raptor and on at least one Spark (native arm64 builds) |
| gcc | 15.2 | 13.3 | fine |
| clang/libclang | clang 23 (brew), libclang-21 | libclang-18 only | bindgen runs on raptor; vendor generated bindings per arch |
| cmake / ninja / make | yes | cmake yes | not needed by our build |
| node/npm | 24 / 11 | 26 | web bundle built on raptor only |
| python3 | 3.14 | 3.14 | HF validation scripts |
| libibverbs-dev / librdmacm-dev | yes | yes | dynamic link target |
| verbs headers | `/usr/include/infiniband/verbs.h` | yes | |
| rdmasync / rdmapipe | source in `~/Developer` | `~/.local/bin` | benchmark baselines |
| libfuse3-dev, libsqlite3-dev | not needed | not needed | fuser speaks the protocol; rusqlite bundled |

Homebrew is available and preferred for tools (`local-ai-tap` at
`~/Developer/local-ai-tap` is the release channel). Log every tool the build
needs in `docs/TOOLING.md` as it is added.

## One-time root checklist (user runs; agent cannot sudo)

On every node (raptor + six Sparks):

```bash
# 1. FUSE: let root/containers see a user-owned mount
echo user_allow_other | sudo tee -a /etc/fuse.conf
# 2. Mountpoints and state dir owned by tj
sudo mkdir -p /mnt/sparknest /mnt/sparknest-test /srv/sparknest /srv/sparknest-test
sudo chown tj:tj /mnt/sparknest /mnt/sparknest-test /srv/sparknest /srv/sparknest-test
# 3. Allow user services to keep running when logged out
sudo loginctl enable-linger tj
```

For native-speed reads of sealed files (FUSE passthrough), the daemon needs
`CAP_SYS_ADMIN`. Once the deploy script has staged the unit, on every node:

```bash
sudo cp ~/.config/sparknest/sparknestd@trial.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now sparknestd@trial
```

(Use this instead of the user unit; do not run both.) Without it everything
works, and sealed files are served through the page cache instead. The
system unit also raises the mount's kernel readahead from 128 KiB to 16 MiB,
which cold reads of sealed files need.

Other root-level performance levers, measured or suspected on the Sparks
(record results in `benchmarks/` before adopting any):

- **CPU idle states.** Spark cores idle in ACPI LPI states with 231–433 µs
  exit latency (raptor: ≤100 µs). Every FUSE request wakes a thread, so
  single-stream daemon-mediated I/O is latency-bound (~0.5 ms/request).
  `sudo cpupower idle-set -D 100` (disable states slower than 100 µs) is the
  obvious experiment; it costs idle power.
- **FUSE over io_uring.** Compiled in on all kernels but disabled
  (`/sys/module/fuse/parameters/enable_uring` = N). Not yet supported by
  `fuser`; a later optimization.

Optional later: raise switch ports to 200G for the benchmark pass; create
`/mnt/models/sparknest` on the NAS share; reformat `/mnt/scratch` after drain.

## Trial deployment conventions

Trial clusters currently run from user-owned paths, `~/sparknest-test/state`
and `~/sparknest-test/mnt`, so they need no root (switch `scripts/cluster.env`
to `/srv/sparknest-test` and `/mnt/sparknest-test` after the root checklist).
They use a distinct cluster name, secret and ports (7410/7411), so they can
coexist with a later real deployment. `scripts/deploy-cluster.sh` builds and
pushes; `scripts/trial.sh start|stop|status|logs|wipe` runs the daemons
detached over SSH (no systemd, since linger is off). Ports 7410/7411 on the
10.55.0.0/24 fabric subnet pass the hosts' ufw rules. Test data is generated (synthetic files) or copied from
existing HF caches; never move the user's real caches during trials. The
user's real caches are adopted only in the final migration milestone, on the
user's go.
