# Installing sparknest

sparknest runs one daemon (`sparknestd`) per host. Together they form a
cluster that mounts one namespace everywhere. `nest` and the web UI manage it.
This guide sets up a new cluster; `docs/ENVIRONMENT.md` records the author's
own hosts.

## Requirements

- Linux with glibc 2.39 or newer (Ubuntu 24.04+), x86_64 or arm64.
- FUSE 3 from the distribution (`sudo apt install fuse3`, for `fusermount3`).
- RDMA (RoCE v2 or InfiniBand) with rdma-core for fabric-speed transfers.
  Without it, set `fabric.mode = "tcp"`; everything else works.
- An odd number of voting hosts is usual; three tolerate one failure, seven
  tolerate three.

## Install

```sh
brew install tpurtell/local-ai/sparknest
```

Or unpack a release tarball and put `bin/sparknestd` and `bin/nest` on your
PATH; the tarball's binaries use the system `libibverbs`. Homebrew core has
an unrelated `nest` formula that also installs a `nest` executable; the two
cannot be linked together.

## One-time root setup (every host)

```sh
echo user_allow_other | sudo tee -a /etc/fuse.conf   # if fuse.allow_other = true
sudo mkdir -p /mnt/sparknest /srv/sparknest
sudo chown "$USER": /mnt/sparknest /srv/sparknest
sudo loginctl enable-linger "$USER"                   # only for the user unit
```

RDMA pins registered memory: `ulimit -Hl` should print `unlimited` (add
`<user> hard memlock unlimited` under `/etc/security/limits.d/` if not).

## Configure

1. Make one secret and copy it to every host (mode 0600). It authenticates
   all cluster traffic and derives the web UI token.

   ```sh
   head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > /srv/sparknest/cluster.secret
   chmod 600 /srv/sparknest/cluster.secret
   ```

2. Write `/srv/sparknest/node.toml` on each host from `node.toml.sample`
   (Homebrew: `$(brew --prefix sparknest)/share/sparknest/config/`). Only
   `[node]` differs between hosts; `cluster.members` must be identical
   everywhere. The state directory holds metadata and this host's objects,
   so put it on the fast local disk, and keep its path under about 90
   characters (the management socket lives inside it).

3. Check it: `sparknestd --config /srv/sparknest/node.toml --check`.

## Start

Bootstrap the cluster exactly once, on one host, before the services run:

```sh
sparknestd --config /srv/sparknest/node.toml --bootstrap
# wait for "sparknestd running", then Ctrl-C
```

Never pass `--bootstrap` again, and never put it in a unit: a host whose
state was wiped would found a second cluster.

Then install one unit per host from `share/sparknest/systemd/` (Homebrew
has already filled in the binary path; replace `@STATE_DIR@`, `@MOUNT@`,
and for the system unit `@USER@`/`@GROUP@`):

- `sparknestd.service`, a **user** unit
  (`~/.config/systemd/user/`, `systemctl --user enable --now`). No root at
  run time; sealed files are served through the page cache.
- `sparknestd@.service`, a **system** unit running as your user with
  `CAP_SYS_ADMIN` as its only capability. The kernel requires it for FUSE
  passthrough, which reads sealed files at local NVMe speed. The unit also
  raises the mount's readahead. Recommended.

`nest status` on any host shows every node; `nest ui` prints a link to the
web UI with its token (`api_listen` must be set for the web UI).

## Hugging Face cache

Give the hub a seal policy, adopt the existing cache, and point the hub
directory at the mount (tokens and `xet/` stay local):

```sh
mkdir /mnt/sparknest/hub && nest policy /hub incomplete
nest import --move --wait ~/.cache/huggingface/hub /hub
mv ~/.cache/huggingface/hub ~/.cache/huggingface/hub.old   # leftovers only
ln -s /mnt/sparknest/hub ~/.cache/huggingface/hub
```

Import hard-links, so the cache must share a filesystem with the state
directory. On further hosts, blobs the cluster already has become that
host's copies without any transfer. Either Hugging Face cache layout is
accepted. Use `huggingface_hub` 1.32 or newer (2.0 recommended).
`docs/MIGRATION.md` is the full walk-through for an existing fleet.

## Placement, archives and space

```sh
nest group set gpus hostA,hostB,hostC
nest rule set qwen hf:Qwen/Qwen3-8B --hosts @gpus --auto   # keep copies there
nest store add nas /mnt/nas/sparknest --gateways @all       # archive store
nest offload hf:old/model --store nas                       # free local space
nest plan --free @gpus=500GiB && nest plan apply <id>       # hit free-space targets
nest backup create /projects --name nightly --store nas
```

`nest <command> --help` documents each command.

## Upgrades

Stop every daemon, replace the binaries, start them again. Our metadata
schema migrates on start. When a release changes the Raft format, the
hosts notice on start, agree once a majority of members run the new build,
and re-found the cluster from the most advanced metadata; nothing needs
doing by hand, and after a clean stop nothing is lost. A host still on the
old build (or down) never counts toward that majority, so upgrading a
minority by accident just leaves those hosts waiting unmounted. Upgrade all
hosts together.

## Power loss and fsck

Metadata commits are not fsynced (ADR-026); an application's `fsync` still
makes its file and the metadata it depends on durable on a majority. A host
that went down while running (it notices from the kernel boot id) catches
up with the cluster before it mounts, and checks its objects against the
metadata: copies it lost are retired, damaged copies are replaced from
another host, and files no host holds any more are logged as lost. If most
hosts went down at once, they re-found the cluster from the most advanced
surviving metadata when a majority is back.

Nothing that might be the only copy of data is deleted: objects the
metadata no longer knows go to `/.lost+found/<date-time>/<host>/<path>`.

- `nest fsck` checks this host while running (`-y` applies the standard,
  non-destructive fixes; `--deep` verifies Hugging Face blobs against the
  SHA-256 in their names).
- `sparknestd fsck` reports the same without a daemon.

## Disaster recovery

Archive stores receive metadata snapshots every six hours
(`<store>/meta/`). Without any daemon running, `sparknestd export --meta
SNAPSHOT --objects DIR... --out DIR` rebuilds the namespace, or a subtree
of it, as ordinary files from any surviving object directories.
