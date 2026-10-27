# Schedule input for the compiled transition core

`choreoos-raft-conformance --replay-inputs` reads a bounded text schedule from
stdin and emits v2 NDJSON transitions, including pre/post durable projections,
persistence tokens and effects. Existing default scenarios and snapshot mode
are unchanged. This is the input driver for forthcoming TLC normalization;
it does not interpret TLC states or establish refinement.

Each line is a command; node names are `a`, `b`, `c`. Empty lines and unknown
commands fail. A failed line exits nonzero; earlier transitions remain in the
output. Callers must check the process exit before accepting the trace.

| Command | Meaning |
|---|---|
| `timeout a` | Election timeout |
| `heartbeat a` | Heartbeat timeout |
| `propose a "value with spaces"` | Proposal; C++ quoted-string escaping |
| `complete a` / `fail a` | Complete/fail the actor's current pending token |
| `restart a` | Core restart from acknowledged durable state |
| `advance a` / `apply a` / `compact a` | Commit, application or compaction input |
| `deliver a b RVReq` | Deliver the oldest matching actual emitted message |
| `drop a b RVReq` | Remove the oldest matching message |
| `duplicate a b RVReq` | Append a copy, preserving its exact request identity |

Delivery kinds: `RVReq`, `RVResp`, `AEReq`, `AEResp`, `SnapshotReply`.
The driver captures the real `Effects.send_now` objects. It cannot inject an
arbitrary reply or release a deferred message before persistence. An unavailable
message or missing pending write fails rather than inventing an input. Different
message kinds/destinations can be delivered in any scheduled order. The same
source/destination/kind bucket uses oldest-first selection; arbitrary reordering
within that bucket is not yet supported. No automatic delivery or fsync occurs.
Drop/duplicate only alter the harness network and emit no core transition row.

Example:

```sh
printf '%s\n' 'timeout a' 'complete a' 'deliver a b RVReq' \
  'complete b' 'deliver b a RVResp' | \
  build/linux-gcc/cpp-runtime/tests/choreoos-raft-conformance --replay-inputs
```

Schedules are limited to 10,000 lines and 65,536 bytes per line. They are local
test inputs, not a production network endpoint. Snapshot assembly/install,
read rounds, explicit stale callbacks, torn physical storage and arbitrary
initial durable states are outside this mode. `restart` exercises the pure
core's acknowledged state; it does not reopen real storage.

CTest runs eight integration checks against the compiled runner: durable
election, prevention of invented/early deliveries, drop, duplicate identity,
failed-write halt/restart, quoted payload, replication through durable
application/compaction, and malformed schedules. No runtime code changed.

Next: normalize actual TLC behaviors into schedules, compare an explicitly
stated projection at justified correspondence points, and require divergence
mutations to fail. The [term/vote and heartbeat gaps](raft-model-mapping-gaps.md)
must be resolved explicitly; this driver does not hide them.

## Two-term election and restart replay (draft)

The `raft-replay-input-tests` suite now includes a bounded **compiled-core** two-term election schedule: elect `a` in term 1, then durably elect `b` in term 2 using `a`'s higher-term vote, and restart `a` from acknowledged disk. It checks that the higher-term request does not release a vote before fsync, the reply preserves the actual request ID, `b` becomes leader, and `a` retains durable term 2/vote `b` after restart. Thirteen independently corrupted trace observations and a failed-write/no-reply schedule are negative cases. This is a precursor to T1-B.2, **not** a TLC model check, exhaustive fault exploration, or refinement proof. Exact-head CI is pending.
