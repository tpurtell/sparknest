# Hugging Face downloads: files in flight (2026-10-01)

Four ~650 MB files of `openai-community/gpt2` from the Hugging Face CDN
(hf_xet, huggingface_hub 2.0) to raptor's NVMe over its gigabit uplink,
through `sparknest-hf-fetch` (one file at a time per process):

| files in flight | throughput |
|---|---|
| 1 | 82 MB/s (32.4 s) |
| 2 | 100 MB/s (26.7 s) |
| 4 | 103 MB/s (25.9 s) |

One file already uses ~2/3 of the link (hf_xet spreads a file over several
connections); two saturate it. Spread downloads therefore default to one
file in flight across the cluster (`--in-flight 2` for the whole link):
spreading decides where files land, not how fast they arrive. `hf download`
itself defaults to 8 files at once, which floods the uplink.

Through a one-node trial's FUSE mount (no passthrough, TCP fabric) the same
download ran at ~80 MB/s with byte-level progress per file.

Update (2026-10-03, ADR-046): in real use four files in flight downloaded
faster than one, so the default is now four.
