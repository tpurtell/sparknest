# sparknest in a container

The image runs `sparknestd` as an **off-grid host** (ADR-047): a member of a
sparknest cluster on the LAN only, not on the RDMA fabric. It is meant for a
machine that should store and serve but is not one of the cluster's compute
hosts, a NAS above all: its disks hold copies (an archive of models, say),
and the other hosts read them straight from it over TCP.

It joins as a non-voting learner, so it never holds up the cluster: a NAS
rebooting for an update is a host going away for a while, nothing more.

- Image: `ghcr.io/tpurtell/sparknest:<version>` (amd64 and arm64), built
  from the release tarballs by `.github/workflows/container.yml`.
- Files here: `Dockerfile`, `entrypoint.sh`, `compose.yaml`.
- Config sample: `packaging/config/node-container.toml.sample`
  (in the image at `/opt/sparknest/share/sparknest/config/`).

You need a running sparknest cluster (`docs/INSTALL.md`). Any Docker host
works; the QNAP notes below are for one we run.

## What the container needs

- **A folder for the state dir**, bind-mounted at `/srv/sparknest`. It holds
  `node.toml`, `cluster.secret`, the metadata and this host's objects, so put
  it on the disks that should serve.
- **The host's network** (`network_mode: host`). The cluster dials the host
  at its LAN address on port 7400 (7401 for the API and web UI); there is no
  port mapping to keep in step with `node.toml`.
- **The user that owns the folder's files** (`user:` in compose). On a NAS
  that is usually the share's user.
- **Nothing privileged.** The default `node.toml` has no `mountpoint`: the
  host stores and serves, and the namespace is mounted on the hosts that use
  it. Mounting it in the container too needs `/dev/fuse`, `CAP_SYS_ADMIN`
  and an AppArmor profile that allows `mount(2)` (commented in
  `compose.yaml`).

## A new host

On the NAS, with the folder you give sparknest (here `/share/sparknest`):

1. Write the config and copy the cluster's secret:

   ```sh
   mkdir -p /share/sparknest
   docker run --rm ghcr.io/tpurtell/sparknest:0.9.0 \
     cat /opt/sparknest/share/sparknest/config/node-container.toml.sample \
     > /share/sparknest/node.toml
   scp a-host:/srv/sparknest/cluster.secret /share/sparknest/
   chmod 600 /share/sparknest/cluster.secret
   ```

   In `node.toml` set `id`, `name`, and `members`. The id must be one no
   host has ever had, above every id in `nest cluster ls` and any host
   removed before: a removed host's id stays retired (re-adding it brings
   back the old host's name). For `members`, copy the
   cluster's list and add this host as `voter = false` at its LAN address.
   Add the same line to `members` in every other host's
   `node.toml`; it takes effect at their next restart, and nothing needs it
   sooner.

2. From any running host, add it:

   ```sh
   nest cluster add 12 172.22.1.10:7400 --offgrid
   ```

3. Start it: copy `compose.yaml` next to the folder, set `STATE`,
   `SPARKNEST_UID`/`SPARKNEST_GID` and `SPARKNEST_VERSION` (in a `.env`
   file or the environment), then

   ```sh
   docker compose up -d
   docker exec sparknest nest status     # the new host serving, among the others
   ```

From then on it is a host like any other off-grid one: `nest replicate SEL
--hosts aviary` puts copies there, `nest offload SEL --store aviary` moves
files there and drops their copies elsewhere, and placement never spreads
onto it uninvited.

## Moving an SMB archive onto the NAS that holds it

A common first setup is a NAS share mounted over SMB on one host and
registered as an archive store (`nest store add models /mnt/models/sparknest
--gateways raptor`). Every byte to or from it then goes through that host's
SMB client. Running sparknest on the NAS itself removes that hop. And the
objects are already where the NAS host keeps its own: the archive's folder
*is* the new host's state dir. Only the metadata changes.

Every host must run 0.9.0 or later. With the archive `models` at
`/mnt/models/sparknest` on the gateway (`/share/models/sparknest` on the NAS):

1. **Put the config in the archive's folder.** It becomes the host's state
   dir as it is: leave its `objects/`, `.sparknest-store` and the rest where
   they are.

   ```sh
   # on the NAS
   cd /share/models/sparknest
   docker run --rm ghcr.io/tpurtell/sparknest:0.9.0 \
     cat /opt/sparknest/share/sparknest/config/node-container.toml.sample > node.toml
   # edit id, name, members; copy cluster.secret here (mode 0600)
   ```

2. **Add the host and start it** (as for a new host, with `STATE` set to the
   archive's folder and the user owning its files). Its first start finds
   objects in a folder it has no metadata for, so it **holds** itself: it
   removes nothing that is not listed as its own. `nest status` shows it
   `held`, with the number of objects it keeps.

3. **Check, then adopt:**

   ```sh
   nest store adopt models aviary --dry-run
   nest store adopt models aviary
   ```

   The adoption first checks that the new host sees the archive's marker in
   its state dir (the same folder the gateway reaches over SMB) and that a
   gateway finds every object the archive records, at its size. Any miss
   stops it with nothing changed. Then one metadata step makes every copy
   the host's and removes the archive store. The host drops the marker,
   is released, and from then on sweeps like any host.

4. **Retire the SMB path.** Nothing in sparknest uses the share now: unmount
   it from the hosts (or keep it for other uses). The archive's other
   folders (`meta/`, `backups/`, `staging-*`) are no longer read and can be
   removed.

Files that live only in that archive cannot be read between step 3's
command and the host serving them; with the host already running and
held, that gap is a moment.

## QNAP (QuTS hero / QTS 5, Container Station)

Tested on a TS-855X (Atom C5125, 128 GB, QuTS hero 5.2, kernel 6.6).

- **Shares** live at `/share/<Share>` (a link to
  `/share/ZFS<n>_DATA/<Share>` on QuTS hero). Use the real path in `STATE`
  if Container Station does not follow the link.
- **Docker** is Container Station's: `docker` is in
  `/share/<pool>/.qpkg/container-station/bin/` and needs an administrator
  account (or the Container Station UI: Applications → Create, paste
  `compose.yaml`).
- **Ownership.** Files written over SMB belong to the share's user. Find its
  ids with `id <user>` over SSH and set `SPARKNEST_UID`/`SPARKNEST_GID` to
  them, so the container can read and write what the share already holds.
- **Start at boot**: `restart: unless-stopped` is enough; Container Station
  starts its containers when the NAS boots.
- **ZFS**: a dataset with `sync=disabled` (common for a models share) is
  fine for objects (each copy is checked against its size and the cluster
  keeps its own checks); with a UPS it is fine for the metadata too.
  Compression gains nothing on model weights.
- **No io_uring.** QuTS hero 5.2's kernel is built without it (the
  syscalls return ENOSYS on the host itself, seccomp or not), so the log's
  `io_uring unavailable` warning is expected: transfer writes run on the
  receiving thread. Loosening the container's seccomp profile changes
  nothing.
- **Network**: the container uses the NAS's address. Keep the NAS's IP fixed
  (DHCP reservation or a static address): the cluster dials it there.

## Building the image

From a published release:

```sh
docker buildx build --platform linux/amd64,linux/arm64 \
  --build-arg VERSION=0.9.0 -t sparknest:0.9.0 packaging/docker
```

From local tarballs (`scripts/release.sh` output), for this machine's
platform:

```sh
docker build --network=host --build-arg VERSION=0.9.0 \
  --build-context dist=dist/release/v0.9.0 -t sparknest:0.9.0 packaging/docker
```

The base is `ubuntu:24.04`: its glibc is the one the release binaries are
built against. `--network=host` helps where the default build network
cannot reach the Ubuntu mirrors.

## Running it

- `docker exec sparknest nest status` (or any `nest` command): the
  management socket is in the state dir.
- `docker logs -f sparknest`: the daemon's log; `nest logs --host aviary`
  from any host shows it too.
- `docker compose pull && docker compose up -d`: upgrade. `docker stop`
  shuts it down cleanly (60 s grace), so it restarts without a check of its
  objects.
- `docker run --rm -v STATE:/srv/sparknest IMAGE sparknestd fsck`: check
  its objects with the daemon stopped.
- A held host that should not be (a folder brought in by hand, say):
  `nest cluster release aviary --dry-run`, then without `--dry-run`.
