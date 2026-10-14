# Choreography WAL ↔ Raft entry projection (PR #13, first slice)

## Scope

This is a **strictly read-only correspondence bridge** from the existing
`state::FileEngine` (typed choreography events in `wal.bin`, durable
`meta.bin`, recovered Engine state) to the experimental pure
`raft::Durable` log. Production `replication::Replica` and the existing
write paths are unchanged.

A Raft entry is represented as:

- `Entry.term` = the immutable typed event's term.
- `Entry.command` = the **exact canonical serialization** produced by
  `state::canonical_event(event)`; it includes the event's log index,
  term, operation ID, schema version and payload.

The bridge validates **both** the Raft index/term and the event envelope on
decode; noncanonical or ambiguous encodings are rejected rather than being
silently normalized. It explicitly handles all event types through the
existing canonical event parser (including `NoOp`).

`audit_domain_store()` propagates the checked
`FileEngine::consensus_metadata()` result. Neither the projection nor the
production replica substitutes an initial term/vote after a metadata read
failure; that would invalidate Raft vote durability.

## Replay and applied-state correspondence

For a **complete, uncompacted** log beginning at index 1:

1. Verify every WAL index is contiguous, event terms never decrease and are
   no newer than the recovered metadata term, and all typed entries round-trip
   canonically. This includes the uncommitted suffix.
2. Check the metadata commit index equals the engine commit index and the
   recovered choreography `last_applied` index.
3. Reconstruct the deterministic applied choreography state by replaying only
   entries `1..commit`. Uncommitted suffixes are decoded and validated but
   **never applied**.
4. Compare `state_hash(replayed)` to the authoritative recovered
   `state_hash(FileEngine.engine().state())`; refuse projection on mismatch.

The resulting `raft::Durable.applied` records **observed recovered**
application progress, not a new independently persisted Raft applied record.
PR #12's standalone `CheckpointStore` still deliberately rejects nonzero
applied progress, so **do not write this projection into that checkpoint**.

## Explicit exclusions / remaining gates

A separate [snapshot-aware recovery projection](raft-recovery-projection.md)
now checks compacted production stores without manufacturing a full
`raft::Durable` log. The exclusions below continue to apply to this original
full-WAL API and to production migration.

- **Compacted logs and snapshots are rejected.** The projection cannot
  manufacture missing entries from a snapshot. The next production bridge
  needs a `lastIncludedIndex`/`lastIncludedTerm` offset model, snapshot
  state and receipt correspondence, and updated Raft transition semantics.
- This module never calls `FileEngine::append_event`,
  `commit_through`, `truncate_after` or `persist_consensus`. It is not a
  writable WAL adapter and does not turn on the pure Raft core in production.
- It does not verify Raft leadership or an arbitrary network/disk failure
  schedule. Bounded TLC safety checks and previous C++ trace-contract tests
  have distinct documented coverage.
- Recovery depends on the existing `FileEngine::open` logic. This checker
  compares recovered state and WAL data **after** open, not power-loss
  durability across every filesystem or all snapshot install crash points.
- There is no independent cryptographic authentication; canonical encoding
  and `state_hash` are deterministic consistency checks, not signatures.

## Test gate

`RaftDomainBridgeTest` covers real WAL restart, typed event encoding,
uncommitted log suffix isolation, metadata corruption, noncanonical envelopes,
term/index mismatch and refusal to guess missing snapshot prefixes. Linux,
Windows, sanitizer/static analysis, formatting and management API jobs must
pass on the exact pull request head before merge.

An experimental [writable FileEngine adapter](raft-file-engine-adapter.md) now
executes prevalidated plans with term/vote/WAL/commit sequencing, snapshot
offsets and fail-stop/reopen handling. It remains separate from the pure core
and production Replica. Snapshot maintenance, the core's commit/apply mapping,
host effect discipline and formal correspondence remain migration gates.
