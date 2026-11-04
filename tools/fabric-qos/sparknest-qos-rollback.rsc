# Undo sparknest-qos.rsc: the switch's QoS as exported before it (2026-11-04,
# the engine's DSCP 26 / class 3 lossless setup, ECN and WRED off).
# Restores every changed field exactly, then removes only SparkNest's entries.
/interface ethernet switch qos port set qsfp56-dd-1-1 !egress-rate-queue2 pfc=cuteafd-pfc3
/interface ethernet switch qos port set qsfp56-dd-1-5 !egress-rate-queue2 pfc=cuteafd-pfc3
/interface ethernet switch qos port set qsfp56-dd-2-1 !egress-rate-queue2 pfc=cuteafd-pfc3
/interface ethernet switch qos port set qsfp56-dd-3-1 !egress-rate-queue2 pfc=cuteafd-pfc3
/interface ethernet switch qos port set qsfp56-dd-3-5 !egress-rate-queue2 pfc=cuteafd-pfc3
/interface ethernet switch qos port set qsfp56-dd-4-1 !egress-rate-queue2 pfc=cuteafd-pfc3
/interface ethernet switch qos port set qsfp56-dd-4-5 !egress-rate-queue2 pfc=cuteafd-pfc3
/interface ethernet switch qos tx-manager queue set [find traffic-class=2] ecn=no
/interface ethernet switch qos priority-flow-control remove [find name=pfc-2-3]
/interface ethernet switch qos map ip remove [find profile=sparknest-rdma]
/interface ethernet switch qos profile remove [find name=sparknest-rdma]
