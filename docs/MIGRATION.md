# Migration playbook: from local caches to sparknest

The user drives this; nothing here runs without their go. Steps marked
**root** need sudo on the host. Steps marked **destructive** remove data
that is not yet somewhere else; each is preceded by a check. The trial
cluster can keep running throughout: the real cluster `main` has its own
name, secret, ports (7400/7401) and paths.

## 0. Before starting

- **root**, every host: the checklist in `docs/ENVIRONMENT.md`
  (`user_allow_other`, `/srv/sparknest` and `/mnt/sparknest` owned by `tj`).
- Every venv that downloads models: `pip install -U "huggingface_hub>=1.32"`
  (2.0 recommended). Older clients cannot write the shared-blob layout.
- Pause downloads and training jobs that read `~/.cache/huggingface` on the
  host being migrated.
- On any host: `mkdir /mnt/models/sparknest` (the NAS archive area).

## 1. Bring up the real cluster

```sh
SPARKNEST_ENV=scripts/cluster-main.env scripts/deploy-cluster.sh
SPARKNEST_ENV=scripts/cluster-main.env scripts/cluster.sh bootstrap
```

**root**, every host (the deploy staged the rendered unit):

```sh
sudo cp ~/.config/sparknest/sparknestd@main.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now sparknestd@main
```

Check with `nest status`, which reads `/srv/sparknest/node.toml` first. All
seven hosts should show `serving`. Then prepare the Hugging Face area and the
archive and host groups:

```sh
mkdir /mnt/sparknest/hub
nest policy /hub incomplete          # seal blobs when a download completes
nest group set sparks ostrich,dodo,emu,kiwi,rhea,moa
nest store add nas /mnt/models/sparknest --gateways @all
```

## 2. Adopt raptor's cache (2.3 TB, 23 repos)

The cache and `/srv/sparknest` share raptor's root filesystem, so import
hard-links: nothing is copied and no space is needed.

```sh
nest import --move --wait ~/.cache/huggingface/hub /hub
```

`--move` unlinks each source once the namespace holds it. What stays in
`~/.cache/huggingface/hub` is only what was not imported: partial
downloads, lock files, and anything listed as an error. Review it, then:

```sh
mv ~/.cache/huggingface/hub ~/.cache/huggingface/hub.before-sparknest
ln -s /mnt/sparknest/hub ~/.cache/huggingface/hub
```

Keep `~/.cache/huggingface/token` and `xet/` local; only `hub` moves.
Resume jobs, and check that a model loads from the new path.

## 3. Adopt each Spark's cache (1.1 to 1.7 TB each)

On each Spark in turn, the same command:

```sh
nest import --move --wait ~/.cache/huggingface/hub /hub
```

Blobs the namespace already has, from raptor or an earlier Spark, are
**adopted**: the Spark's own file becomes its copy of that blob, with no
transfer and no extra space. New repos and blobs are imported. A blob whose
size differs from the cluster's is reported and left in place for review.
A repo's `refs/` stay as the first import wrote them. Then swap in the
symlink as in step 2.

When all six are done, `nest where hf:<org>/<model>` shows which hosts
hold a complete copy of each model.

## 4. Placement

Make the copies you want explicit, and let the planner free space:

```sh
nest rule set qwen35 hf:Qwen/Qwen3.6-35B-A3B-FP8 --hosts @sparks --auto
nest plan --free raptor=800GiB --free @sparks=400GiB
nest plan apply <id> --wait
```

Plans remove redundant copies first and offload sole copies to the NAS.
They never remove a last copy or a copy a rule requires. The web UI's
Space view does the same.

## 5. Drain `/mnt/scratch` and make it an archive

`/mnt/scratch` (NTFS, 5.9 TB used) is a different filesystem, so its
contents are copied in, not linked. Raptor's own disk cannot hold them all,
and a file lands on the host that writes it. So copy from the Sparks, a
share each sized to its free space, through the existing NFS/RDMA mount:

```sh
# on a Spark, for its share of the top-level directories:
rsync -a /mnt/nfs/raptor-scratch/<dir>/ /mnt/sparknest/scratch/<dir>/
```

Check each copy (`rsync -anc` should list nothing), and let `nest plan`
move cold data to the NAS if a Spark gets tight.

**destructive, root**, only once every directory is verified:
reformat `/dev/sda2` as ext4, mount it at `/mnt/scratch` (fstab), and give
`tj` `/mnt/scratch/sparknest`. Then register it and send the cold data back
as archived copies:

```sh
nest store add scratch /mnt/scratch/sparknest --gateways raptor
nest offload /scratch/<cold-dir> --store scratch --wait
```

Offloaded files stay in the namespace and stream from raptor when read;
`nest replicate` brings one back to fast storage.

## Afterwards

- Retire the trial: `scripts/trial.sh wipe` removes only trial state.
- Metadata snapshots land in each archive every six hours; `docs/INSTALL.md`
  describes recovery with `sparknestd export`.
