# Runtime configurations

Files use flat `key: value` lines. Existing node1–node3 files keep legacy fixed-leader behavior.

`transition/node1.yaml` through `transition/node3.yaml` run a separate loopback election cluster on ports 7201–7203 with `consensus_engine: transition`. Start each with `choreoos-node CONFIG`. They use separate data directories; they do not migrate or deploy an existing cluster.

See [runtime integration](../docs/transition-runtime.md) for activation, `snapshot_every`, ownership, stopped-cluster migration and limits. Remote transition peers require the [mTLS configuration](../docs/peer-tls.md).
