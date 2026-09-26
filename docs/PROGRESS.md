# Progress log

Newest first. One entry per meaningful step: what landed, what was measured,
what is next. Keep entries short; link to benchmarks and ADRs.

## 2026-09-26 — Design accepted

- Environment surveyed on all seven nodes (`docs/ENVIRONMENT.md`).
- Official proposal written (`PROPOSAL.md`), ADR-001…009 recorded.
- Language: Rust throughout. Consensus: openraft with custom SQLite state
  machine. FUSE: fuser without libfuse. Fabric: verbs FFI ported from
  rdmapipe/rdmasync design.
- Blocking on user: one-time root checklist in `docs/ENVIRONMENT.md`
  (fuse.conf `user_allow_other`, mountpoints, linger).
- Next: M0 skeleton (workspace, build/deploy scripts, packaging samples,
  rustup on raptor + ostrich, vendored verbs bindings).
