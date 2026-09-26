# Packaging and host setup

- `config/node.toml.sample`: annotated node configuration.
- `systemd/sparknestd.service`: user unit template (`@BIN_DIR@`, `@STATE_DIR@`, `@MOUNT@`
  substituted by `scripts/deploy-cluster.sh`). No root needed; FUSE
  passthrough is unavailable, so sealed files are served via the page cache.
- `systemd/sparknestd@.service`: system unit running as the user with
  `CAP_SYS_ADMIN` as its only ambient capability, which the kernel requires
  for FUSE passthrough. The deploy script stages a rendered copy in
  `~/.config/sparknest/` on each host; installing it takes one sudo step.

One-time root setup per host is in `docs/ENVIRONMENT.md`. `LimitMEMLOCK` in a
user unit cannot exceed the user's hard limit; if `ulimit -Hl` is not
`unlimited`, add `tj hard memlock unlimited` to `/etc/security/limits.d/`
(RDMA registration pins memory).
