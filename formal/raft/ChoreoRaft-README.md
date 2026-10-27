# ChoreoRaft (PR #9) - unified asynchronous model

Run (needs Java and tla2tools.jar):

    java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -config ChoreoRaft.cfg -workers 4 ChoreoRaft.tla

## Results from the authoring session (TLC 2026.10.13, 1 core, 4 workers)
| Config | Outcome |
|---|---|
| `ChoreoRaft.cfg` (all Mut* FALSE) | **Incomplete.** Stopped at 270 s: ~7.8M states generated, ~932k distinct, depth 21, no violation. Not an exhaustive result. |
| `neg_MutTruncateOnShorter` | Violates `MatchIndexSound` (the PR #8 bug class) |
| `neg_MutVoteNotDurable` | Violates `ElectionSafety` |
| `neg_IgnoreEpochAndVote` | Violates `ElectionSafety` (not informative about epochs) |
| `neg_IgnoreEpochAlone` | No violation within 120 s (inconclusive, not exhaustive) |
| `MutCommitOldTerm` | Not run to a result: the Figure 8 hazard needs 5 servers; with 3 it is not expected to be observable |

Bounds: 3 servers, 1 value, MaxTerm 2, MaxLogLen 2, MaxMsgs 3, MaxEpoch 3.
No liveness is checked. No snapshots, membership change, timers or client dedupe.
## Revision status

This review revision adds a separate offline/crash/restart lifecycle and prefix-equality matchIndex checks. Persistence is still modeled as a sequence of whole-field stages; term and vote share an atomic stage. These assumptions need a separate WAL/metadata correspondence audit. The ghost committed history is generated from the modeled commit decision and therefore is not an independent proof of that decision. The author's original wide TLC exploration was incomplete. A green, *complete* bounded TLC exploration is required for merge.


## CI decomposition (PR #9, October 13)

The broad `Spec`/`ChoreoRaft.cfg` explores competing elections, message
duplication/drop, offline crashes/restarts, torn-write outcomes and the
asynchronous message bag. **It remains exploratory** until a complete TLC run
is recorded. A timeout is a failure, not a safety pass.

`CoreSpec`/`ChoreoRaftCore.cfg` is a separate **restricted safety profile**,
not a state-space-preserving reduction of `Spec`. It retains vote requests,
durable self-votes and vote grants, elections, client appends, AppendEntries
handling, quorum commitment and durable apply, with all original invariants.
It restricts send behavior to one in-flight AppendEntries exchange per
leader/follower pair, omits redundant heartbeat messages, and excludes
crashes, persistence failures, network drops and message duplication. These
behaviors must be checked separately. Symmetry uses interchangeable fixed
three-node identifiers and must not be used for asymmetric scenarios.

The CI workflow additionally runs `ReachLeader.cfg`,
`ReachReplication.cfg`, `ReachCommit.cfg` and `ReachApplication.cfg`.
Each deliberately negates a witness predicate and **must** produce a TLC
invariant violation with the expected invariant name and exit code. This
establishes that the core configuration can actually elect a leader, durably
replicate an entry, commit an entry and durably apply one. An unreachable
witness fails CI even if the safety run succeeds. Reachability is existential,
not a liveness claim.

The old `ChoreoRaftSmall.cfg` (one epoch, two queued messages) repeatedly
hit the 240-second limit, exploring roughly 1.5–1.6 million distinct states
without completing. An earlier one-message variant finished quickly but did
not allow a quorum election, so it must not be presented as meaningful
coverage. The broad author's two-term configuration also remained incomplete.
All these unfinished runs remain evidence of exploration only.

### Outstanding acceptance gates

- Record a *complete* `CoreSpec` TLC result, including actual distinct states,
  depth, and zero remaining queue.
- Confirm all four reachability witnesses in CI, without interpreting the
  expected counterexamples as ordinary safety failures.
- Check crash/restart, write failure, network duplication/drop and multiple
  terms in separately scoped model-checking configurations.
- Revalidate negative mutation fixtures on the revised model and inspect
  their actual counterexample properties.
- Audit WAL/metadata write ordering against `Torn`; term and vote currently
  form one atomic stage, and the log replacement step is atomic.
- Do not claim refinement or machine-checked C++ verification from these
  bounded TLC results.


## Verified CI profiles (bounded TLC, October 13, 2026)

Each of the following runs used the three-node unified `ChoreoRaft.tla`
model with **one value**, at most two messages in flight, and the safety
invariants/action properties declared in its respective configuration.
All listed runs **exhausted their reachable state space**, with zero states
remaining in the TLC queue and no invariant violations:

| Configuration | Terms | Distinct states | Depth | Checked behavior |
| --- | ---: | ---: | ---: | --- |
| `ChoreoRaftCore.cfg` | 1 | 5,090 | 35 | Durable election, replication, majority commit and apply |
| `ChoreoRaftRecovery.cfg` | 1 | 88,966 | 38 | Core plus crash, torn-write failure and restart |
| `ChoreoRaftTwoTermElection.cfg` | 2 | 46,897 | 31 | Competing elections, durable votes, crash/restart and dropped vote messages; **no client logs** |

See [Raft model-checking workflow](../../.github/workflows/raft-model.yml).
The expected-failure reachability configurations demonstrate leader election,
durable replication, commit, application, a node offline and leadership changes
across different terms. An expected witness counterexample is *not* a safety
violation of the main configuration.

**Unverified combinations:** The two-term model intentionally does not
include committed log entries or AppendEntries. The full `Spec` with
unrestricted message drops, duplication, retransmissions, multi-term
replication and crash/restart is still too large for the previous CI limit.
Therefore **leader completeness across an actual committed log and later
leader is not yet exhaustively checked in this unified model**. The
`NegativeNonDurableVote.cfg` mutation has been confirmed in CI to produce an
`ElectionSafety` counterexample; its expected-failure test passes. This
establishes detection of this seeded durability bug within the scoped
multi-term election profile, not coverage of every persistence fault.

In all these configurations, `CHECK_DEADLOCK FALSE` is appropriate because
liveness is explicitly out of scope and a bounded election can end without a
quorum. This option does not disable any safety invariant. The specialized
`CoreSpec`, `RecoverySpec` and `ElectionSpec` intentionally restrict enabled
actions; they are **not** sound state-space-preserving reductions of full
`Spec`. Do not generalize their successful results to omitted behaviors.

Persistence mapping caveats are recorded in
[docs/raft-storage-audit.md](../../docs/raft-storage-audit.md).

## Proposed bounded combined profile (draft; CI evidence pending)

`ChoreoRaftCombined.cfg` and `ReachCombinedHandoff.cfg` use the existing
asynchronous `ChoreoRaft.tla` state machine with two terms, one command value,
one entry per log, two queued messages, and two leadership epochs. The new
`CombinedSpec` is an **explicit action-subset profile**, not a
state-space-preserving reduction of unrestricted `Spec`.

The profile designates a term-one proposer and a term-two successor via
`CHOOSE`; **symmetry is disabled** because the actors are not interchangeable.
It permits real staged/durable votes, one follower AppendEntries exchange,
majority commit and application, loss of a third-node vote request, a
higher-term election, and former-leader crash, failed persistence, and restart.
It deliberately excludes arbitrary proposer selection, multiple client
entries, arbitrary AppendEntries retries, duplicated packets, crashes of other
nodes, and arbitrary message loss. Those remain separate obligations.

The CI gate runs all existing safety invariants and action properties under
this bounded profile, then requires an actual counterexample to the **negated**
`NoCombinedHandoffWitness` predicate. That witness must show a committed
term-one entry and a later term-two leader with the entry durably present.
The witness does **not** itself prove crash/restart occurred; those actions are
only enabled in the safety profile. Until both hosted TLC runs complete, there
is no claim of exhaustive coverage even for this restricted profile. A timeout
or absent witness is a failure, not a successful safety result.

This model is separate from the compiled C++ fixed-schedule test merged in
PR #38. No implementation refinement, production filesystem equivalence, or
unbounded protocol proof is claimed.
