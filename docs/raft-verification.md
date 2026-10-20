# Raft conformance and verification program

Status: **active; not yet Raft-verified**. The deployed runtime is still
Raft-like. PRs #7–#9 merged bounded TLA+ election, replication and unified
asynchronous models; merged PR #10 introduces a separate, experimental C++ pure
transition core. The selectable [transition runtime](transition-runtime.md) connects that core
to real nodes, storage and authenticated transport. Neither model-to-code
refinement nor a machine-checked C++ proof has been established.

## Scope and trust boundary

The initial verification target is a three-node, static-membership cluster
running the C++20 runtime in election mode. The choreography state machine
remains deterministic and applies **only committed** log entries. The goal is
a specification of Raft plus a correspondence argument for the executable
implementation, including durable metadata and recovery.

Distinguish four claims:

1. **Tests pass:** finite workloads, fault schedules, and platform builds pass.
2. **Protocol model checked:** a formal model satisfies properties under
   stated finite bounds and assumptions.
3. **Refinement checked:** recorded implementation transitions can be mapped
   to the model, with differential tests and counterexample traces.
4. **Implementation proved:** a machine-checked proof covers the implementation
   or a verified core with an explicit connection to deployed C++ behavior.

Claims 1–3 do **not** imply claim 4. Never describe this project as a
“fully verified Raft implementation” until the proof boundary is explicit,
checked, and documented. The transport, filesystem, OS, compiler, clock and
crash model form a trusted/assumed boundary unless covered separately.

## Architecture and protocol boundary

- `cpp-runtime/src/replication/replica.cpp`: roles, terms, votes, append
  consistency, leader replication, commit, and snapshot transfer. Currently
  also performs I/O through `FileEngine` and sends network messages.
- `cpp-runtime/src/replication/simulator.cpp`: deterministic frame transport
  and enabled fault scenarios; supports replayable schedules.
- `cpp-runtime/src/state/store.cpp`: uncommitted versus committed log;
  metadata persistence, snapshot installation, and replay.
- `cpp-runtime/src/storage/{wal,metadata,snapshot,file_util}.cpp`: disk
  durability, CRC validation, atomic replacement and crash behavior.
- `cpp-runtime/src/network/node_server.cpp`: real asynchronous TCP,
  peer gating, timer ticks, client acknowledgements and failover.
- `cpp-runtime/src/state/machine.cpp`: deterministic domain state
  transitions and state hashing; must not decide Raft commitment.

**Refactor status (merged PR #10):** a new `cpp-runtime/src/raft/transition.cpp`
implements a pure `step(Config, Node, Input)` with persistence, messaging and
application effect intents and corresponding GoogleTests. It is compiled
separately from legacy `replication::Replica`; the selectable runtime now hosts
it with real I/O, without establishing a formal refinement mapping. RuntimeHost persists and verifies dependent term/vote/log changes before
releasing effects; failure requires reopening disk. See the
[field and persistence inventory](consensus-field-inventory.md) for the current
source audit and executable boundary regressions.

## Raft safety properties

The specification must state and check:

- **Election safety:** at most one leader per term.
- **Leader append-only:** a leader never rewrites its own log.
- **Log matching:** identical index and term imply identical preceding entries.
- **Leader completeness:** committed entries appear in all later leaders' logs.
- **State-machine safety:** different commands are never applied at one index.
- **Durable-vote discipline:** term and vote survive restart before a vote
  response can grant election support.
- **Commit discipline:** a leader advances commit using a majority-replicated
  entry from its current term; followers apply only through leader commit.
- **Client safety:** a successful response denotes a committed and applied
  command, and retry IDs must not produce duplicate effects.
- **Snapshot safety:** installation/compaction cannot erase committed truth,
  restore conflicting state, or break log-matching at the snapshot boundary.

Prove or model-check properties under explicit **crash/restart**, message
loss/duplication/delay/reordering, partitions and healing, stale leaders,
conflicting uncommitted suffixes, and snapshot catch-up. Include fail-stop
filesystem assumptions and the specified behavior of write failures.

## Liveness and feature scope

Liveness is conditional: a quorum must be reachable, storage must make
progress, and some sufficiently stable interval of message delivery and
election timeouts is assumed. A permanently partitioned minority need not
elect a leader. Safety must not depend on timing.

Initial scope: fixed three-node membership, write-only replicated commands,
leader elections, majority commit, crash recovery, log repair and snapshots.
Not included initially: dynamic membership/joint consensus, leadership
transfer, lease-based or linearizable reads, Byzantine peers, or production
security. Each future addition extends the specification before code.

## Implementation plan and completion gates

### V0 — Freeze a reliable baseline
- [x] Three two-versus-one partitions tested; PR #3 merged after CI success.
- [x] Run Windows and Linux CI, sanitizer builds and repeatable fault tests
      on main (earlier CI passes). Preserve failing seed and network schedule.
- [x] Inventory consensus-changing fields and persist-before-reply edges in
      [the source audit](consensus-field-inventory.md).
- [x] Map current deviations and refusals to executable regression tests in
      the inventory; missing features remain unfinished gates.

### V1 — Specify the protocol
- [x] Add `formal/raft/Election.tla`, `Replication.tla` and
      `ChoreoRaft.tla` with finite TLC configurations (PRs #7–#9).
- [x] Model terms, votes, logs, commit/applied indexes, crashed nodes,
      messages and persistence in the unified specification.
- [x] Run TLC in CI with recorded **bounded** profiles and explicit
      reachability witnesses; record expected mutation counterexamples.
- [ ] Complete an exhaustive configuration combining **multi-term committed
      log replication**, stale messages, network loss/duplication and restarts.
- [ ] Strengthen Figure 8, snapshot and storage-correspondence obligations;
      establish proof obligations beyond bounded exploration.

### V2 — Make the implementation auditable
- [x] Introduce an **experimental** pure transition core with explicit
      persistence and send effects alongside the existing `Replica`
      (PR #10 core; PR #22 real-node integration, still opt-in).
- [ ] Add protocol trace events with term, role, log indices, message cause
      and durable completion boundaries.
- [x] Add a **bounded C++ trace-contract checker and seeded negative cases**
      (merged PR #11), separate from the production `Replica`.
- [ ] Build a *TLC behavior-to-C++* trace refinement checker with explicitly
      justified projection and stuttering. The current independent contract
      tests are not a machine-checked refinement proof. See
      [PR #11 verification boundary](raft-conformance.md).
- [ ] Extend fault tests across elections, conflict repair, snapshots and
      restart. Verify specific command IDs and values, not only commit counts.

### V3 — Establish machine-checked proof
- [ ] Choose a proof framework and clearly define a verified algorithmic
      core or refinement mapping with the actual C++ implementation.
- [ ] Prove protocol safety obligations and preservation across transitions.
- [ ] Document assumptions, unchecked operations and trusted code.
- [ ] Run proof checking in CI and report coverage honestly.

### V4 — Expand and integrate
- [ ] Check serialization, snapshot boundaries, real TCP processes and
      crash-ordering behavior against the verified model's assumptions.
- [ ] Expose metrics/management APIs only after write acknowledgement
      semantics are stable.
- [ ] Revisit dashboard and Temporal orchestration; neither owns Raft state.

## Review rules

Every consensus PR must describe: invariant affected, model transitions
affected, test schedule/seed, persistence ordering, recovery behavior,
and proof/model-check result. No weakening a safety assertion solely to
make CI green. Do not merge while required checks fail.

References:
- Ongaro and Ousterhout, *In Search of an Understandable Consensus
  Algorithm* (Raft).
- Ongaro, *Consensus: Bridging Theory and Practice*, extended Raft description.
- TLA+ and TLC for bounded model checking; model checking a design does not
  establish verified C++ refinement.

## Model-checking status (merged PR #9)

The unified model completes three **separate** CI profiles: core election-to-commit (5,090 distinct states), crash/restart recovery (88,966) and two-term election without client logs (46,897). The original broader combined state space did **not** exhaust TLC under its timeout. The negative non-durable-vote mutation is caught by `ElectionSafety`. See [the detailed results](../formal/raft/ChoreoRaft-README.md) and [storage audit](raft-storage-audit.md). The PR #10 C++ core is not refinement-checked against this model.
