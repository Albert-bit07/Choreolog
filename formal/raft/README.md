# Raft formal models: elections, replication and unified asynchronous protocol

This directory contains three distinct specifications maintained under the [Raft verification program](../../docs/raft-verification.md). `Election.tla` abstracts voting, `Replication.tla` abstracts bounded log/commit safety, and `ChoreoRaft.tla` combines asynchronous election and AppendEntries with durability modeling. None is a mechanically checked proof of the C++ runtime.

## `Election.tla`: what the election-only model contains

- Three nodes with nondeterministic election starts, candidate self-votes, and
  quorum-based leadership.
- Explicit vote request/response messages. Messages may be delivered in any
  order, lost, or indefinitely delayed. A permanently unreachable quorum makes
  no progress; liveness is deliberately not asserted.
- Crash/restart actions that erase volatile roles and vote-response collections
  while preserving the term and vote. The model atomically records a term and
  a granted vote **before** exposing a corresponding message.
- Historical vote and leadership records, so a crash or leader step-down cannot
  erase evidence of **double voting** or **two leaders in the same term**.

TLC checks `TypeOK`, `NoDoubleVote`, `ElectionSafety`,
`CandidateOwnsVote`, `LeaderHasQuorum`, and `GrantsWereRecorded`.
`Election.cfg` checks three nodes and **one nonzero election term**;
`ElectionTwoTerms.cfg` checks **two** bounded terms, including stale-term
messages and vote persistence after restart, and applies an explicit
`MessageBudget` TLC state constraint: at most two messages in flight.
This prunes behaviors in which message queues accumulate beyond two;
it is a tractability bound, **not** a safety assumption of Raft. The
unconstrained two-term model generated over 88 million successor states
before its 240-second CI limit and was not exhaustively checked.
Both CI configurations are finite-state checks, not proofs for unbounded
terms, message queues, or executions.

## What `Election.tla` does NOT contain

Log entries, up-to-date-log voting, AppendEntries, quorum **commit**, current-term
commit restrictions, snapshots, compaction, real disk I/O, networking identity,
message duplication, dynamic membership, client responses, and state-machine
application are not modeled here. In particular, this first model cannot
establish log matching, leader completeness, state-machine safety, or full Raft
conformance. Those behaviors are modeled only in the separate `Replication.tla` and/or `ChoreoRaft.tla` modules described below. No C++ refinement is established.

## Run locally

Requires Java 11+ and `tla2tools.jar` from the immutable
[TLA+ tools v1.7.4 release](https://github.com/tlaplus/tlaplus/releases/tag/v1.7.4).

```bash
cd formal/raft
java -jar /path/to/tla2tools.jar -config Election.cfg Election.tla
java -jar /path/to/tla2tools.jar -config ElectionTwoTerms.cfg Election.tla
```

The CI workflow downloads the exact v1.7.4 release asset and verifies its
SHA-256 before running TLC. The pinned digest is
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.

## Correspondence targets for the next PRs

| Model transition | C++ implementation boundary |
| --- | --- |
| `StartElection` | `Replica::start_election` and `FileEngine::persist_consensus` |
| `DeliverRequest` | `Replica::on_vote` and `persist_vote` |
| `DeliverResponse` | `Replica::on_vote_response` |
| `BecomeLeader` | `Replica::become_leader` (model omits its log no-op) |
| `Crash / Restart` | `Replica::open` and durable metadata recovery |
| `Drop` | `SimulatedNetwork` loss/partition behavior |

The unified `ChoreoRaft.tla` now covers several of these transitions under separately scoped bounds. PR #10 adds an experimental C++ transition core; implementation trace conformance is still future work. Do not represent a green TLC run as proof of the C++ implementation.

## Bounded replication model (next V1 slice)

`Replication.tla` is a **separate, intentionally simplified** log/commit
model. Run `java -jar /path/to/tla2tools.jar -config Replication.cfg
Replication.tla`. The configuration models three nodes, two terms, two
possible commands and at most two entries per node. TLC checks `TypeOK`,
`LogMatching`, `CommittedPrefix`, `CommittedHistoryOnLeader`, and
`CommittedIndexesBounded`.

The model includes log-freshness-gated quorum election, append-only leader
writes, atomic follower prefix repair, majority commitment restricted to
current-term entries, follower commit propagation and crash/restart with
durable log and commit indices. The `committed` variable is a specification
history oracle, **not** a production node or a distributed source of truth.

**Important limitations:** Elections are atomic and unique, not the
asynchronous message protocol of `Election.tla`; AppendEntries is abstracted
as a successful prefix transfer, with no message queue or per-follower
`nextIndex`/`matchIndex`; leadership changes cannot overlap with old-leader
network actions. Thus checking these invariants does **not** verify the
interaction between elections and replication, including delayed RPCs,
misleading acknowledgments or durable write ordering. Snapshotting, log
compaction, application, client responses, network identity and any
correspondence to C++ are also out of scope.

A green finite TLC run is only evidence for this bounded abstraction.
The next step is to strengthen the existing unified asynchronous model's combined failure bounds and connect implementation traces to it.

## Unified asynchronous model (PR #9)

`ChoreoRaft.tla` models elections, vote durability, AppendEntries, delayed/dropped/duplicated messages, crash/restart, and torn-write outcomes. A single exhaustive run of the unrestricted combined model **has not completed**, so passing restricted profiles must not be extrapolated to all failure schedules. See [the model's report](ChoreoRaft-README.md) for the exact configurations, measured state counts, and remaining proof obligations.

The CI workflow successfully exhausted three **separate** finite profiles: election/replication/commit (5,090 distinct states), crash/restart recovery (88,966), and two-term elections **without logs** (46,897). Expected-failure reachability and the non-durable-vote negative test passed. This is *scoped model checking*, not a proof of arbitrary-size Raft or production C++ behavior.

## Experimental C++ transition boundary (PR #10)

`cpp-runtime/include/choreoos/raft/transition.hpp` and `cpp-runtime/src/raft/transition.cpp` introduce a pure, testable state transition interface. The selectable [transition runtime](../../docs/transition-runtime.md) hosts the core on real storage/TCP/mTLS; legacy `replication::Replica` remains the default. Model-generated trace replay, snapshot-aware formal mapping, combined multi-term replication faults and implementation conformance remain unfinished. See [the persistence audit](../../docs/raft-storage-audit.md).

## Model-generated behavior artifacts

CI now exports its seven existing witness/counterexample behaviors as typed
JSON artifacts for subsequent replay work. See [the export format and limits](../../docs/tlc-behavior-export.md)
and [observed model/core mapping gaps](../../docs/raft-model-mapping-gaps.md).
Export preserves context and provenance; it does not establish C++ replay or
refinement. The model bounds and invariants above are unchanged.
