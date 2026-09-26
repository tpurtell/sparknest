# sparknest

A coherent, whole-file distributed filesystem with explicit placement for a
small RDMA cluster: one super server plus six DGX Sparks, one namespace at
`/mnt/sparknest`, built first to host a shared Hugging Face cache at fabric
speed and second to be a good general shared folder.

- Files are whole objects on the node that created them; **rules** decide
  where else full copies live. Reading never replicates.
- Remote reads stream over RoCE (dual-rail on Sparks); sealed local files are
  FUSE passthrough at NVMe speed.
- First write picks one owner; stale copies are removed immediately.
- Raft-replicated SQLite metadata; keeps working with up to three nodes down.
- Archive stores (SMB NAS, slow SATA disk) are folders behind gateway nodes;
  backup, offload and recall are explicit operations.
- One daemon (`sparknestd`), one management API, a CLI (`nest`) and a web UI
  that only use that API.

Start with [`PROPOSAL.md`](PROPOSAL.md). Decisions: [`docs/DECISIONS.md`](docs/DECISIONS.md).
Machines and prerequisites: [`docs/ENVIRONMENT.md`](docs/ENVIRONMENT.md).
Plan: [`ROADMAP.md`](ROADMAP.md). How to work here: [`AGENTS.md`](AGENTS.md).

Status: M1–M7 done and running on the trial cluster; M8 (hardening, release) in progress. Rust workspace.
