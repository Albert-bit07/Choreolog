# Experimental Raft durable checkpoint adapter (PR #12, first slice)

## Status and safety boundary

This adapter persists the **standalone pure Raft transition core** using real
filesystem flushes and atomic whole-file replacement. It is not connected to
the production `replication::Replica`, `FileEngine`, choreography event WAL,
snapshots, network transport or gRPC gateway. It does **not** read or overwrite
`meta.bin` or `wal.bin`. The adapter owns only `raft-core-v1.bin` in a
separate test/experimental storage directory.

The entry strings in `raft::Durable.log` are not serializable
`choreoos::state::Event` objects. Reusing the production WAL would require a
verified mapping of operation IDs, terms, indexes, payloads and snapshot offsets.

## Implementation

- `raft::CheckpointStore` persists term, voted-for, the complete log and
  commit index as one versioned, CRC-32 protected checkpoint. It uses the
  existing `storage::write_atomic` utility with requested fsync of the file
  and parent directory. A checkpoint update is an **O(log size)** full rewrite;
  it is intentionally a correctness-first experimental adapter, **not** the
  production append-only WAL design.
- `CheckpointStore::persist` rejects term/commit regressions, vote changes
  in the same term, and any truncation or replacement of previously committed
  log entries.
- On **every write failure** (including an injected failure after a successful
  durable replacement), storage enters an uncertain state and refuses further
  writes until recovery rereads the actual disk image.
- `raft::DurableNode` accepts protocol inputs, invokes the pure
  `step()`, executes persistence synchronously, and **only after a
  successful write** dispatches `PersistCompleted(token)` and returns the
  dependent network effects. A write failure instead produces
  `PersistFailed(token)`, halts the node and emits **no** dependent messages.
- External callers cannot forge `PersistCompleted` or `PersistFailed`.
  `Restart` discards staged and volatile state and reloads from filesystem
  recovery, rather than trusting an in-memory term after a failed write.
- Fault injection requires explicit `CheckpointOptions{test_mode=true}`.
  The two deterministic cases simulate failures before and after replacement.

## Deliberate fail-closed limitations

**Applied-state atomicity:** `ApplyCommitted` is rejected and checkpoints
with nonzero `applied` are invalid. Updating applied progress durably before
persisting the actual choreography state would permit a restart to skip an
unapplied command. The next integration slice must pair a durable state hash,
snapshot or replay checkpoint with applied progress. This design is not yet
sufficient for production consensus migration.

**One atomic record, not a write-ahead log:** `raft-core-v1.bin` is a single
atomic checkpoint. The existing `ChoreoRaft.tla` torn-write model includes
multiple write stages and is not refined by this file. The correctness
boundary assumes atomic replacement on the supported filesystem and
successful sync operations; it does not claim arbitrary power-loss resilience
on all filesystems or rigorous Windows directory-durability proof.

**No production liveness or convergence claim:** `DurableNode` is a
synchronous, single-threaded test host. It performs no socket I/O, scheduling,
message delivery, snapshot installation, or real application. It is not a
replacement for `replication::Replica`. Per-node directory ownership and
transport identity verification must be enforced by the future host.

## Test and merge requirements

`RaftDurableTest` covers durable elections and votes, follower replication
and majority commitment, reopening real checkpoint files, corruption
rejection, committed-prefix preservation, explicit fault injection before/after
replacement, recovery of ambiguous outcomes, forged completion rejection and
disabled applied-progress advancement. Passing these tests demonstrates
behavior of this **experimental** adapter only.

Do not merge PR #12 until the exact head passes Windows and Linux builds and
tests, formatting, sanitizers/static analysis, existing management API tests,
and bounded TLC CI. Passing this slice does not authorize switching production
storage or claiming TLA+-to-C++ refinement.
