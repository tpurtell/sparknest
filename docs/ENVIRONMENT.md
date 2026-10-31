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

Every host: `/etc/sysctl.d/90-sparknest-tcp.conf` sets
`net.ipv4.tcp_slow_start_after_idle = 0` (benchmarks/M10-LAN-TCP.md);
the original nine on 2026-10-12, cassowary and maxq on 2026-10-26 (cassowary
had been missed). `scripts/add-offgrid.sh check` now requires it.

### Off-grid hosts (LAN only, ADR-047)

| Host | Arch | OS | Kernel | LAN | Notes |
|---|---|---|---|---|---|
| kakapo | arm64 | Ubuntu 24.04.5 | 7.0.0-1019-nvidia | 172.22.2.7, RTL8127 (r8127), 10GbE | Spark without the fabric cable; `fuse.enable_uring` still N (set at runtime when it joins, kernel command line at next reboot) |
| cassowary | arm64 | Ubuntu 24.04.5 | 7.0.0-1019-nvidia | 172.22.2.8 (`cassowary.birds.vimoin.com`), `enP7s7`, 10GbE | Spark without the fabric cable (node 10, 4 lanes; added 2026-10-21); 20 cores, 119 GiB, 3.4 TiB free; `fuse.enable_uring` N |
| maxq | amd64 | Ubuntu 24.04 on WSL2 (bridged) | 6.18.54.1-microsoft-standard-WSL2 | 172.22.2.11 (`maxq.birds.vimoin.com`; bare `maxq` resolves to Tailscale), WSL eth0 bridged, 10GbE | oliver's PC rebuilt (node 11, 8 lanes; added 2026-10-26). A learner with its LAN address in the membership, so the others dial it. 24 cores, 94 GiB; `/srv` on btrfs `/dev/sdc1` (3.8 TB); `fuse.enable_uring` Y. TCP from ostrich 6.8 Gbit/s; cold 5 GB reads through the mount 550–590 MB/s |
| oliver (retired 2026-10-26; disk lost, rebuilt as maxq) | amd64 | Ubuntu 24.04.5 on WSL2 (NAT) | 6.18.40.1-microsoft-standard-WSL2 | Windows RTL8127, 10GbE; WSL eth0 172.25.172.80 behind NAT | 24 cores, 62 GiB, ext4 VHD 3.6T |

oliver: never use WSL mirrored networking (it drops inbound connections
there); stays on NAT and dials out. Tuned 2026-10-12 (benchmarks/M10-LAN-TCP.md):
Windows adapter "Ethernet 7" Receive/Transmit Buffers 4096; WSL eth0 MTU
1500 via `/etc/systemd/system/wsl-lan-mtu.service` (WSL copies the Windows
Tailscale adapter's 1280; a route keeps 1280 for 100.64.0.0/10);
`.wslconfig` has `kernelCommandLine=fuse.enable_uring=1`. All LAN NICs
support jumbo frames (atlantic max 16334, r8127 max 9194, Windows 9014),
but the LAN switching between oliver's PC and raptor does not pass them
(2026-10-12: with both ends at MTU 9000, a 4000-byte don't-fragment ping
from Windows to raptor was dropped, and so was one between raptor and
kakapo, both Linux at 9000). Everything stays at 1500 until the
switch ports allow jumbo; a host at 9000 talking TCP to it would hang.

## Fabric

- raptor: `mlx5_0`, RoCE (Ethernet link layer), 400G, `enp1s0np0` with
  10.55.0.22 and 10.55.1.22 on one port. rdmasync docs mention 10.55.0.12 for
  an older raptor address; use `.22`.
- Sparks: one physical cable exposing two RoCE functions: rail 0
  `rocep1s0f0`/`enp1s0f0np0` = 10.55.0.N, rail 1 `roceP2p1s0f0`/`enP2p1s0f0np0`
  = 10.55.1.N, N = 1..6 (ostrich, dodo, emu, kiwi, rhea, moa in IP order:
  .1 ostrich, .2 dodo, .3 emu, .4 kiwi, .5 rhea, .6 moa). The `f1` functions
  are DOWN.
- Switch set to **200G per Spark rail** on 2026-09-27 for the benchmark pass
  (benchmarks/M8-200G.md); it was 100G before to keep the fans quiet. A single Spark
  rail is PCIe-capped near 116 Gb/s regardless; dual rail is required for
  ~195 Gb/s.
- Measured reference (rdmasync benchmarks, 2026-08, 200G): Spark dual-rail raw
  196 Gb/s, rdmasync 168 Gb/s, one rail 94 Gb/s; raptor two-flow raw 160 Gb/s.
- Management/LAN: 172.22.x.x (raptor 172.22.2.12, ostrich 172.22.2.1). NAS
  `aviary` at 172.22.1.10 over 10G.
- raptor's 10.55.0.22 comes from DHCP; 10.55.1.22 is static. At 100G a
  Spark offers only rail 0 (10.55.0.N), so a daemon started before the DHCP
  address arrives has no rail in common with any Spark and every RDMA link
  is refused until a restart (2026-10-31, after the NVMe install reboot).
  `/etc/systemd/system/sparknestd@sparknest.service.d/wait-fabric.conf` on
  raptor makes the daemon wait up to 60 s for both addresses, then start
  anyway.
- raptor's 10G LAN port is the Aquantia (`atlantic`) one. Adding PCIe
  devices renames it (`enp172s0` became `enp174s0` with the two 8 TB NVMe
  drives): its MAC is unchanged, but systemd-networkd's DHCP client ID
  follows the name, so the 172.22.2.12 reservation missed and it came up
  on 172.22.220.3 until TJ rebound it at the switch. The off-grid hosts
  reach raptor at 172.22.2.12, so their sessions fail while it is wrong.
- raptor's extra disks (ADR-074): `raptor (D2)` = `/mnt/fast8t/srv/sparknest`
  (Samsung 9100 PRO 8 TB, PCIe 5 x4), `raptor (D3)` =
  `/mnt/slow8t/srv/sparknest` (same model in a PCIe 4 slot, 16 GT/s). Both
  btrfs, `nofail` in fstab, added 2026-10-31.

### LAN standby for the fabric hosts (ADR-057, 0.5.0)

Every fabric host is also on the 10G LAN: raptor 172.22.2.12, ostrich .1,
dodo .2, emu .3, kiwi .4, rhea .5, moa .6. Given each member's `lan`,
sparknest keeps a standby control connection and one TCP data lane there
and uses them when the fabric fails (switch reboot, pulled cable). Every
host already listens on `0.0.0.0:7400`/`:7402` and the LAN reaches
those ports (checked 2026-10-17 from raptor to ostrich), so nothing else is
needed. To turn it on, once every host runs 0.5.0, add `lan` to each
fabric member in `cluster.members` of `/srv/sparknest/node.toml` on all
seven fabric hosts (the same list everywhere), then restart each daemon in
turn (ask first: the mounts are in use):

```toml
members = [
  { id = 1, name = "raptor", addr = "10.55.0.22:7400", lan = "172.22.2.12:7400" },
  { id = 2, name = "ostrich", addr = "10.55.0.1:7400", lan = "172.22.2.1:7400" },
  { id = 3, name = "dodo", addr = "10.55.0.2:7400", lan = "172.22.2.2:7400" },
  { id = 4, name = "emu", addr = "10.55.0.3:7400", lan = "172.22.2.3:7400" },
  { id = 5, name = "kiwi", addr = "10.55.0.4:7400", lan = "172.22.2.4:7400" },
  { id = 6, name = "rhea", addr = "10.55.0.5:7400", lan = "172.22.2.5:7400" },
  { id = 7, name = "moa", addr = "10.55.0.6:7400", lan = "172.22.2.6:7400" },
]
```

`tools/lan-standby/sparknest-add-lan-standby` makes this edit on one host
(installed with the formula; `--dry-run` shows the diff): it refuses
while the installed sparknestd is older than 0.5.0, keeps
`node.toml.pre-lan`, and validates the result with `sparknestd --check`
before replacing the file. It leaves off-grid hosts alone and does nothing
twice. `sparknestd --config /srv/sparknest/node.toml --check` validates a
hand edit. A 0.4.4 daemon rejects the unknown `lan` key, so do not add it
before the upgrade. Mixed versions are fine: a 0.5.0 host keeps using the
fabric connection to a 0.4.4 peer and retries the standby once a minute.
Off-grid hosts (kakapo, oliver) are unchanged: their members are already
LAN addresses and need no `lan`. `scripts/install-cluster.sh configure`
and `scripts/deploy-cluster.sh` write `lan` from `SPARKNEST_LAN`.

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

For cold-cache benchmarks (`nest drop-caches` drops the page cache on every
host), install the fixed-function helper once per node; it asks for sudo:

```bash
~/.local/lib/sparknest/sparknest-drop-page-cache --install   # or from a checkout:
tools/drop-page-cache/sparknest-drop-page-cache --install
```

It lands at `/usr/local/libexec/sparknest/drop-page-cache` (root, setuid,
mode 4750, your group only) and does exactly one thing: `sync` and
`drop_caches=1`. See `tools/drop-page-cache/README.md`.

To restart the daemon after a deploy without a password (for agents
iterating on sparknest), install the restart rule once per node; it asks for
sudo on each host that lacks it:

```bash
tools/allow-restart/sparknest-allow-restart --hosts        # from a checkout on raptor
sudo systemctl restart sparknestd@sparknest                  # then, passwordless
```

It adds `/etc/sudoers.d/sparknest-restart-sparknest` allowing exactly that
command line for your user, nothing else (`--print` shows it, `--check`
verifies it; delete the file to revoke).

Other root-level performance levers, measured or suspected on the Sparks
(record results in `benchmarks/` before adopting any):

- **CPU idle states.** Spark cores idle in ACPI LPI states with 231–433 µs
  exit latency (raptor: ≤100 µs). Every FUSE request wakes a thread, so
  single-stream daemon-mediated I/O is latency-bound (~0.5 ms/request).
  `sudo cpupower idle-set -D 100` (disable states slower than 100 µs) is the
  obvious experiment; it costs idle power.
- **NVMe interrupt coalescing (moa, rhea).** An NVIDIA tuning script had
  enabled interrupt coalescing on these hosts' drives by default. It
  delayed completions, so small scattered reads there (the engram rows)
  lagged the other hosts. TJ disabled it permanently on 2026-09-30. Treat
  moa/rhea read-latency numbers from before then with care; if a Spark's
  small reads lag again, check `nvme get-feature -f 8` (Interrupt
  Coalescing) first.
- **FUSE over io_uring.** Enabled on all hosts on 2026-09-27 with
  `fuse.enable_uring=1` on the kernel command line (FUSE is built in, so
  `/etc/modprobe.d` options are ignored). sparknest uses it automatically
  (ADR-025).

Optional later: raise switch ports to 200G for the benchmark pass; create
`/mnt/models/sparknest` on the NAS share; reformat `/mnt/scratch` after drain.

## Trial deployment conventions

Trial clusters currently run from user-owned paths, `~/sparknest-test/state`
and `~/sparknest-test/mnt`, so they need no root (switch `scripts/cluster.env`
to `/srv/sparknest-test` and `/mnt/sparknest-test` after the root checklist).
They use a distinct cluster name, secret and ports (7410/7411), so they can
coexist with a later real deployment. `scripts/deploy-cluster.sh` builds and
pushes; `scripts/trial.sh start|stop|status|logs|wipe` (a wrapper for
`scripts/cluster.sh` with the trial env) runs the daemons
detached over SSH (no systemd, since linger is off). Ports 7410/7411 on the
10.55.0.0/24 fabric subnet pass the hosts' ufw rules. Test data is generated (synthetic files) or copied from
existing HF caches; never move the user's real caches during trials. The
user's real caches are adopted only in the final migration milestone, on the
user's go.
