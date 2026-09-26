# Vendored fuser 0.18.0

Upstream: https://crates.io/crates/fuser/0.18.0 (MIT, see LICENSE.md).
Trimmed to the library (no examples, tests or CI files) and patched for
sparknest:

- `src/ring.rs` (new): `RingSink`, `RingDispatcher` and
  `Session::ring_dispatcher`, so a FUSE-over-io_uring transport
  (`crates/nest-fuse/src/uring.rs`) can dispatch requests through fuser
  and route each reply back to its ring entry.
- `src/channel.rs`: `ChannelSender` carries an optional ring target;
  `send` hands the reply to it instead of writing /dev/fuse.
- `Cargo.toml`: example and dev-dependency sections removed; dead-code
  warnings for unused ABI structs allowed.

Drop the patch if upstream fuser gains an io_uring transport.
