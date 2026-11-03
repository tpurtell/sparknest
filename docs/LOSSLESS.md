# A lossless fabric class for SparkNest

SparkNest's RDMA (remote reads, replication, push loads) is marked
**DSCP 18** (TOS byte 72, `[fabric] traffic_class`; ADR-084). A fabric
tuned for it carries that traffic in a class of its own:

| | Inference engine (e.g. cuteafd) | SparkNest |
|---|---|---|
| DSCP / TOS | 26 / 104 | 18 / 72 |
| NIC priority, switch class | 3 | 2 |
| Switch scheduling | high-priority group | low-priority group: sent only when the high group is empty |
| Loss | PFC on priority 3 | PFC on priority 2, its own NIC buffer |

So SparkNest's traffic is not dropped under incast (eight sources into
one host), and it always yields to the engine's: the switch serves class 2
only when class 3 has nothing to send, and PFC pauses each priority on its
own, so a full SparkNest buffer never pauses the engine. ECN and WRED stay
off: neither class sets ECN bits, so marking cannot slow it, only drop it.

Untuned, nothing breaks: a NIC trusting PCP or a switch ignoring DSCP puts
DSCP 18 in the default lossy queue, where SparkNest ran before 0.8. Then a
loaded fabric drops packets, and every drop stalls an RDMA lane for a
go-back-N resend: on 2026-11-04, Muse push loads (eight sources, eight
ranks) moved about 3 GB/s per rank on 100G ports while the switch's lossy
queue had dropped 3–6.6 billion packets per Spark port.

## Hosts

`tools/fabric-qos/sparknest-fabric-qos` sets each ConnectX port on the
fabric (an active RoCE port with a 10.55.0.x/10.55.1.x address):

- trust DSCP (the fabric links are untagged, so PCP says nothing);
- DSCP 18 → priority 2, 26 → 3, 48 (CNP) → 6, every other DSCP → 0;
- PFC on priorities 2 and 3, each in its own receive buffer (the port's
  receive memory split three ways: lossy, SparkNest, engine; the driver
  sizes headroom from speed, MTU and cable length);
- global 802.3x pause off: it stops the whole port, every class at once.

It saves each NIC's state to `/var/lib/sparknest/fabric-qos/` first and
refuses to run over a saved state; `rollback` restores it. It needs
`mlnx_qos` (`mlnx-tools`), `dcb` (iproute2) and `ethtool`. Changing a NIC's
buffers drops its link for a moment, so stop loads first.

**One tool owns a NIC.** The policy includes the engine's priority 3, so on
a cluster tuned this way the engine's own NIC tool (cuteafd's
`fabric-lossless`) only checks. `apply` refuses while that tool holds saved
state: roll it back first, so the state saved here is the NIC's original.

**Surviving a reboot.** The settings live in the NIC, and a reboot or NIC
reset loses them (raptor's on 2026-10-31 did). `persist` installs a
root-owned copy in `/usr/local/sbin` and `sparknest-fabric-qos.service`, a
oneshot after `network-online.target` (and before `sparknestd`). At boot it
leaves a tuned NIC alone, reapplies over its saved original, and refuses
(the unit fails, visibly) on any other state rather than overwrite it.

```sh
T=tools/fabric-qos/sparknest-fabric-qos
$T check                       # what each fabric NIC has now
$T apply --dry-run             # the commands apply would run
agent-sudo --agent-context "Tune this host's RoCE NICs for SparkNest's DSCP 18 class" $T apply
agent-sudo --agent-context "Reapply SparkNest's RoCE NIC tuning at boot" $T persist
$T check                       # every port: tuned
```

## Switch (MikroTik CRS8xx)

`tools/fabric-qos/sparknest-qos.rsc` adds to a switch already set up for
the engine's DSCP 26 / class 3 (as RouterOS 7.24.5 on a CRS804-4DDQ):

- a profile mapping DSCP 18 to traffic class 2 (DSCP only, `automap=no`);
- one PFC profile on classes 2 **and** 3 (`traffic-class=2,3`): a port
  takes one PFC profile, so this replaces the class-3-only one on the
  seven fabric ports;
- queue 2's wire rate on each port, which PFC uses to time pauses it
  receives;
- nothing else: queue 2 is already in the low-priority group, queue 3 in
  the high one, and ECN and WRED stay off everywhere.

Check the port names and rates against `/interface ethernet switch qos
export verbose` first; the file names this cluster's ports.
`sparknest-qos-rollback.rsc` restores exactly the fields it changes.

```sh
scp tools/fabric-qos/sparknest-qos*.rsc admin@switch:
ssh admin@switch '/import file-name=sparknest-qos.rsc verbose=yes dry-run'
ssh admin@switch '/import file-name=sparknest-qos.rsc verbose=yes'
```

## Checking it

With a load running, on a host:

```sh
ethtool -S enp1s0f0np0 | grep -E 'prio2_(bytes|pause)'    # SparkNest's traffic
cat /sys/class/infiniband/*/ports/1/hw_counters/roce_adp_retrans
```

`tx_prio2_bytes` grows with SparkNest traffic and retransmits stay near
zero. On the switch, `/interface ethernet switch qos port print stats`
shows queue 2 carrying it and `tx-drop-queue2-packet` at zero. Pauses on
priority 2 are normal under incast; pauses on priority 3 that rise only
with SparkNest loads would mean a buffer is shared, which apply avoids.
