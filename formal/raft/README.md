# Bounded Raft election model (V1, first slice)

This directory begins the verification program in [docs/raft-verification.md](../../docs/raft-verification.md).
`Election.tla` is an executable **abstraction of elections**, not yet a complete
Raft model and not a mechanically checked proof of the C++ implementation.

## What this model contains

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
messages and vote persistence after restart. Both are finite-state models,
not proofs for unbounded terms or executions.

## What this model does NOT contain

Log entries, up-to-date-log voting, AppendEntries, quorum **commit**, current-term
commit restrictions, snapshots, compaction, real disk I/O, networking identity,
message duplication, dynamic membership, client responses, and state-machine
application are not modeled here. In particular, this first model cannot
establish log matching, leader completeness, state-machine safety, or full Raft
conformance. These are the next specification expansions. The distinction
between this abstract voting model and deployed C++ is unproved.

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

Next: introduce bounded log contents and log-freshness voting, then
AppendEntries and current-term majority commitment in `Spec.tla`.
Add a trace mapping that can distinguish durable writes from messages sent,
and eventually verify that the C++ transition core refines the model.
Do not represent a green TLC run as proof of the C++ implementation.
