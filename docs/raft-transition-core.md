# Experimental Raft transition core (PR #10, phase A)

Status: the pure C++20 core is available through the selectable
[transition runtime](transition-runtime.md), including real storage, TCP/mTLS,
snapshots and management. Legacy `replication::Replica` remains the default.
The core function still performs no I/O and has no machine-checked production
refinement proof.

## Pure transition boundary

`choreoos::raft::step(config, state, input)` returns a new logical node
state and a collection of intended effects, without accessing a clock,
filesystem, RNG, socket or mutable process-wide state.

The source is `cpp-runtime/src/raft/transition.cpp`, the interface is
`cpp-runtime/include/choreoos/raft/transition.hpp`, and characterization
tests are `cpp-runtime/tests/unit/raft_transition_test.cpp`.

- `Node.durable` represents the latest **acknowledged** durable Raft metadata.
- `Node.pending` stages one proposed durable update, with a unique token.
- `Effects.persist` requests a durable write. A real host must not claim
  completion until it has finished all required flush and recovery-safe
  operations.
- Only matching `PersistCompleted(token)` installs the staged `Durable`
  state and releases dependent `send_now`/application effects.
- `PersistFailed(token)` is fail-stop. The node must stop interacting with
  peers and clients, then reload from real disk. That disk may contain an
  ambiguous partial or complete write; it cannot be assumed identical to
  `Node.durable` after a failed operation.
- `Restart` resets volatile state. The host **must first overwrite the
  durable snapshot with the result of filesystem recovery**. Calling
  `Restart` on an arbitrary in-memory old snapshot is not recovery.
- Replication uses one entry per send and per-peer in-flight request IDs.
  Acknowledged match positions are computed from the original request
  context, never from a follower's unverified extra suffix.

## Important implementation gaps

1. **Runtime integration is selectable.** RuntimeHost dispatches validated
   FileEngine writes, application confirmation and maintenance; CoreReplica maps
   clocks and framed messages. The default remains legacy during rollout.
2. **Application mapping is explicit.** FileEngine applies at durable commit;
   core apply effects confirm that result and are never replayed into Engine.
   Effects/client completion wait for verified persistence and application.
   Recovery derives both commit and applied from actual disk replay. This is a
   tested host contract, not a formal atomicity/refinement proof.
3. **Snapshot lifecycle is integrated.** Recovered offsets, local compaction,
   persisted installation and correlated chunk catch-up are supported. The
   snapshot-aware formal model/relation remains open. See the runtime guide.
4. **No model-to-code proof.** New unit tests characterize the transition
   contract but do not mechanically replay `ChoreoRaft.tla` behaviors. The
   model's larger multi-term replication failure space is also not exhaustively
   verified.
5. **No authentication or Byzantine defense.** The core now rejects
   message identities outside the configured peer set before updating terms or
   processing log data. This is only a membership check on a **claimed** ID;
   the real host must authenticate the transport sender and bind that identity
   to the decoded message. A member spoofing another member is out of scope.
6. **No liveness guarantee.** ElectionTimeout and HeartbeatTimeout are supplied
   by the driver. The core intentionally has no fairness or clock assumptions.

## Merge discipline

Linux and Windows builds/tests, formatting, sanitizers/static analysis, and
existing model checks passed on PR #10 before merge. Merging phase A means only that an
**experimental** component and its characterization tests are available; it
does not activate the new Raft behavior. Merged PR #11 adds bounded
trace-contract tests, and merged PR #12 adds an isolated atomic checkpoint
adapter with real filesystem recovery tests. Neither is wired to the
production `Replica` or proves TLA+ refinement; real WAL/event mapping,
the selectable runtime integration is implemented separately through FileEngine,
while snapshot-aware formal refinement remains outstanding. See [verification scope](raft-verification.md) and
[storage correspondence audit](raft-storage-audit.md).

## Deterministic trace projection (first PR #10 conformance prerequisite)

`cpp-runtime/include/choreoos/raft/trace.hpp` exposes `trace_action` and
`trace_line(config, node, input, seq)`. The resulting JSON line follows the
basic post-state fields and action vocabulary in
[`formal/raft/trace-schema.v1.json`](../formal/raft/trace-schema.v1.json).
An externally supplied monotonically increasing sequence and node ID make
this side-effect-free; the production runtime does **not** yet emit it.
`post.term`, `post.log` and `post.commit` project the *staged* state
(`pending.target`) when a write is outstanding; `post.durable` reports
acknowledged persistence. `post.alive` maps `!halted`.

The trace serializer is covered by deterministic output, JSON escaping and
staged-versus-acknowledged durability tests. **It is only a projection**, not
a trace-refinement checker: it omits the full inbound request payload, network
bag, host write boundaries and a formal stuttering-step matcher. The concrete
`Entry.command` value also needs a reviewed mapping to the TLA+ value
alphabet. No code-to-model conformance claim follows from producing a valid
trace line.
