# SparkNest's fabric class on a MikroTik CRS8xx (ADR-084; docs/LOSSLESS.md).
# RouterOS 7.24.5 / CRS804-4DDQ. Adds to the engine's lossless DSCP 26 /
# traffic class 3 setup and changes nothing of it but the PFC profile name.
#   DSCP 18 (SparkNest RDMA) -> traffic class 2: low-priority group, sent
#     only when the high-priority group (class 3, the engine) is empty.
#   PFC on classes 2 and 3 together: a port takes one PFC profile, and
#     PFC pauses each class on its own, so SparkNest's pauses never stop 3.
# ECN marking on queue 2 (ADR-086): the NICs mark RoCE ECN-capable and slow
#   a sender on each CNP, so the queue stays short and PFC rarely fires.
#   WRED stays off on every queue: it drops, and a RoCE drop costs a
#   go-back-N resend. Queue 3 (the engine) is left as it is.
# Import with serving stopped; dry-run first. Undo: sparknest-qos-rollback.rsc.
/interface ethernet switch qos profile add name=sparknest-rdma dscp=18 pcp=2 traffic-class=2 automap=no
/interface ethernet switch qos map ip add dscp=18 map=default profile=sparknest-rdma
/interface ethernet switch qos priority-flow-control add name=pfc-2-3 traffic-class=2,3 rx=yes tx=yes
/interface ethernet switch qos tx-manager queue set [find traffic-class=2] ecn=yes wred=no
# moa: classes 2 and 3 lossless; queue 2 wire rate for received pause time.
/interface ethernet switch qos port set qsfp56-dd-1-1 pfc=pfc-2-3 egress-rate-queue2=100.0Gbps
# rhea: classes 2 and 3 lossless; queue 2 wire rate for received pause time.
/interface ethernet switch qos port set qsfp56-dd-1-5 pfc=pfc-2-3 egress-rate-queue2=100.0Gbps
# raptor: classes 2 and 3 lossless; queue 2 wire rate for received pause time.
/interface ethernet switch qos port set qsfp56-dd-2-1 pfc=pfc-2-3 egress-rate-queue2=400.0Gbps
# ostrich: classes 2 and 3 lossless; queue 2 wire rate for received pause time.
/interface ethernet switch qos port set qsfp56-dd-3-1 pfc=pfc-2-3 egress-rate-queue2=100.0Gbps
# emu: classes 2 and 3 lossless; queue 2 wire rate for received pause time.
/interface ethernet switch qos port set qsfp56-dd-3-5 pfc=pfc-2-3 egress-rate-queue2=100.0Gbps
# kiwi: classes 2 and 3 lossless; queue 2 wire rate for received pause time.
/interface ethernet switch qos port set qsfp56-dd-4-1 pfc=pfc-2-3 egress-rate-queue2=100.0Gbps
# dodo: classes 2 and 3 lossless; queue 2 wire rate for received pause time.
/interface ethernet switch qos port set qsfp56-dd-4-5 pfc=pfc-2-3 egress-rate-queue2=100.0Gbps
# Class 3's own profile is now unused; it stays, so the rollback can return to it.
