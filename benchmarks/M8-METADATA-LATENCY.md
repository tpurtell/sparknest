# Where a metadata commit's time goes (2026-09-27)

## On the trial cluster (one client, operations in sequence)

`metaops.py` on the leader (emu) and a follower (ostrich), through the mount:

| Operation | Leader | Follower | Commits |
|---|---|---|---|
| rmdir, rename | 5.3–5.6 ms | 8.2 ms | 1 |
| mkdir, create empty file | 7.6 ms | 8.5 ms | 1 |
| unlink | 8.7 ms | 12.5 ms | 1–2 |
| open existing, write 1 byte, close | 20 ms | 21 ms | about 3 |

Fabric round trip (ping, ostrich to emu): 0.24 ms minimum, 0.75 ms average
(idle Spark cores wake slowly). fsync of 4 KiB: 0.66 ms on a Spark,
2.5 ms on raptor. TCP_NODELAY is set on every RPC socket.

## In process (`commit_latency_breakdown` in crates/nest-testkit/tests/m8.rs)

Loopback, raptor. State on tmpfs (fsync free) versus raptor's NVMe:

| | tmpfs | NVMe (2.5 ms fsync) |
|---|---|---|
| empty commit, 3 nodes, on leader / follower | 0.12 / 0.09 ms | 11.0 / 9.3 ms |
| mkdir, 7 nodes, on leader / follower | 0.19 / 0.37 ms | 11.5 / 11.5 ms |
| 32 commits in flight, 7 nodes | 25–50 k/s | 320–350 /s |

strace over 1,200 sequential commits on 3 nodes: 7,299 fsyncs of
`raft.sqlite-wal` (2.0 per node per commit), 69 on `meta.sqlite`.

## Reading

The consensus software, RPC and SQLite apply cost 0.1–0.4 ms. The rest is
fsyncs in series: each node fsyncs twice per commit (the log append and the
commit index), and a commit waits for about three of them in a row: the
leader's append (replication reads the entry back from SQLite, so it cannot
leave before that commit), a follower's append, then the leader's
commit-index write before applying. A request from a follower also waits
for that node to learn and apply the commit. On the Sparks that is three or
four 0.7 ms fsyncs plus three or four 0.25–0.75 ms hops, which is the 5–8 ms
per commit measured. Concurrency barely helps (350/s with 32 in flight on
raptor's disk) because appends are not grouped.

Levers, in order of payoff: make the commit-index write non-durable
(openraft allows it to lag), replicate from an in-memory copy of new
entries instead of after the leader's fsync, group concurrent appends into
one fsync, and a `nest` batch delete that removes a tree in a few commits.
