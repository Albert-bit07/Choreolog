# Snapshot-aware diagnostic traces (v2)

`trace_line_v2` supplements the existing v1 serializer. V1's bytes, checker and
refusal of snapshot/read actions are unchanged. No node wire or disk format
changes. Neither trace contract establishes TLC-to-C++ refinement.

V2 records the full staged and durable term/vote, absolute commit/applied
indexes, snapshot boundary and retained suffix. Unlike v1's durable log length,
this lets the checker compare a completed write with the exact admitted target.
Input objects retain vote/append/snapshot request context, including exact
matched coverage, callback token and incomplete-snapshot readiness. Read rounds
remain unsupported and throw rather than silently omitting authority.

The snapshot contract runs through the existing compiled pure-core harness:

```sh
python scripts/check_raft_conformance.py \
  --runner build/linux-gcc/cpp-runtime/tests/choreoos-raft-conformance \
  --schema formal/raft/trace-schema.v2.json --snapshot-contract --self-test
```

CTest `raft-snapshot-trace-contract` runs on normal and sanitizer builds and
Windows/Linux CI. Five deterministic families contain 26 transitions:
compaction with stale completion/restart; installation retaining a matching
suffix; installation discarding a conflicting uncommitted suffix; failed
compaction; failed install. Installation covers incomplete transfer term
persistence and a retried final acknowledgement. Failed operations preserve
acknowledged disk state and halt; no real disk write is simulated here.

The independent Python contract checks exact staged/completed targets,
absolute-index bounds, continuity, persistence tokens, unchanged disk before
completion, no premature reply, fail-stop and reply correlation. Eight trace
mutations must fail at their intended safety checks: early durable boundary,
lost compacted suffix, wrongly retained conflicting suffix, premature ack,
continued availability after write failure, wrong ack request ID and wrong ack
index. An eighth mutation removes the actual installation while keeping a
continuous term-only transfer/restart, so coverage cannot pass vacuously. The v1 checker still rejects its original six mutations. The committed
NDJSON golden and C++ tests freeze v2 output and v1 refusal/compatibility.

## Limits and next gate

This is a **diagnostic contract prerequisite**, not completion of T1-B. It does
not ingest TLC-generated behaviors, compare peer progress or network bags to a
model, verify snapshot application payloads/receipts, or justify a stuttering
relation. It checks only the named snapshot fixtures, not arbitrary v2 traces.
The schema describes serializer output; the Python checker adds fixture-specific
safety/coverage checks without a third-party JSON Schema runtime dependency.

Existing TLA+ models still omit snapshots. Before claiming refinement, define
snapshot/compaction model transitions and the host/application correspondence,
export real TLC behaviors, normalize message identity and persistence context,
replay them against C++, and reject deliberately divergent projections. Failed
I/O recovery must use actual recovered disk truth; the pure harness's old-disk
failure case is only one possible outcome. No default-backend flip, public
linearizable read, general proof or new model-check state count is claimed.
