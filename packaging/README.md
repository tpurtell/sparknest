# Packaging and host setup

- `config/node.toml.sample`: annotated node configuration.
- `systemd/sparknestd.service`: user unit template (`@BIN_DIR@`, `@STATE_DIR@`, `@MOUNT@`
  substituted by `scripts/deploy-cluster.sh`).

One-time root setup per host is in `docs/ENVIRONMENT.md`. `LimitMEMLOCK` in a
user unit cannot exceed the user's hard limit; if `ulimit -Hl` is not
`unlimited`, add `tj hard memlock unlimited` to `/etc/security/limits.d/`
(RDMA registration pins memory).
