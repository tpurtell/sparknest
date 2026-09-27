# drop-page-cache — cold-cache benchmarks

`nest drop-caches` asks every host (or `--host` ones) to sync and drop its
clean page cache before a cold-read measurement. Each host does it through a
small fixed-function setuid helper, installed once per host:

```bash
sparknest-drop-page-cache --install   # builds the helper; asks for sudo
sparknest-drop-page-cache --check     # installed and executable by you?
sparknest-drop-page-cache             # drop this host's cache now
nest drop-caches                      # every host, at once
```

(From a checkout: `tools/drop-page-cache/sparknest-drop-page-cache`.)

The helper lives at `/usr/local/libexec/sparknest/drop-page-cache`, owned by
root, mode `4750`, with the installing user's primary group: only root and
that group can run it. Every parent directory must be root-owned and not
group/world-writable. It is replaced atomically (new inode, then rename),
and nothing privileged is ever placed in a writable checkout. Each install or
update needs ordinary sudo authorization; no sudo rule is installed. Homebrew
cannot install setuid root files, so the package ships this wrapper and the C
source (`libexec/sparknest/drop-page-cache.c`); the wrapper builds and
installs the helper on first use (a C compiler is needed then).

The helper takes no paths, commands, environment or alternate values: it runs
`sync`, then writes `1` to `/proc/sys/vm/drop_caches` (checked to be procfs).
That drops clean page-cache pages system-wide, including other workloads'.
Active or dirty pages can stay: for cold-cache numbers, check residency
(`mincore`, `fincore`) and record physical reads rather than assuming a cold
cache. It does not drop dentries/inodes or anything on GPUs.

To remove it: `sudo rm /usr/local/libexec/sparknest/drop-page-cache`.

Tests (unprivileged; they never install anything or drop caches):

```bash
python3 -m unittest discover -s tools/drop-page-cache -p 'test_*.py'
```

Adapted from the helper in ds41rt (same author, MIT).
