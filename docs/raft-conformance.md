# Raft transition conformance (PR #11, bounded test phase)

**Status:** experimental C++ core + independent safety-contract checker. The
deployed TCP `replication::Replica` does not use the new transition core.
This suite is **not** a mechanically verified refinement of
`formal/raft/ChoreoRaft.tla`.

## What runs

`cpp-runtime/tests/integration/raft_conformance_scenarios.cpp` executes the
real compiled `choreoos::raft::step` implementation over five deterministic,
repeatable scenario families and emits NDJSON. Each row includes a versioned
action and post-state projection, the previous state, the originating scenario,
the requested persistence callback token (when applicable), staged-write token
before/after, and outbound/application effects.

`scripts/check_raft_conformance.py` checks the trace independently of the
C++ assertions. It loads the v1 action alphabet from
[`formal/raft/trace-schema.v1.json`](../formal/raft/trace-schema.v1.json) and
checks term and durable monotonicity, per-scenario sequence continuity,
per-node state continuity, committed-log prefix preservation, durable
write/application ordering, matching persistence tokens, no effects on a
failed/halted node, and the scenario-specific terminal witnesses.

The fixture coverage is:
- Election with a persist-before-RequestVote self-vote, a durably granted
  follower vote, durable leader/follower log entries, majority commitment,
  and durably acknowledged application.
- Pending write failure, ignored stale callback, fail-stop and restart.
- Committed-prefix conflict refusal and short-heartbeat suffix preservation.
- Repair of an **uncommitted** suffix (a shorter staged log than the old
  durable log is allowed until the write finishes).
- Stale AppendEntries reply handling; an outdated request ID cannot inflate
  `matchIndex`.

The checker also mutates six passing traces, inserting premature disk
changes, committed-log corruption, premature vote emission, application
ahead of commit, broken per-node continuity, and an unsupported snapshot offset.
Each mutation must fail. The v1 serializer/checker explicitly refuses nonzero
snapshot boundaries until a snapshot-aware trace/model relation is defined.
This is mutation detection of the checker, not a protocol safety proof.

The CTest name is `raft-transition-trace-contract`. It runs on Linux and
Windows alongside existing tests, as well as in the sanitizer CI build.

## What this does *not* prove

The checker enforces a **hand-reviewed safety contract influenced by the
TLA+ model**. It does not ingest TLC-produced behaviors, establish a formal
projection/stuttering simulation relation to TLA+, cover arbitrary asynchronous
interleavings, or prove liveness. Some action names abstract over multiple
internal steps. It does not authenticate network peers, test snapshot
checkpoint atomicity, or establish that a failed write was torn exactly as
the real WAL recovery would be. No production node uses these test traces.

**Before claiming model-to-code refinement:** export bounded TLC behavior
traces with full request/persistence context; normalize TLA+ and C++ actions;
support zero/multiple model steps per implementation step; independently
validate a refinement mapping and negative tests against that relation.
The scope must include meaningful multi-term replication, log repair and
crash/restart paths, which the currently completed TLC profiles check
separately, not exhaustively in one combined state space.

Storage and application checkpoint obligations also remain open:
[storage audit](raft-storage-audit.md) and
[transition core notes](raft-transition-core.md).
