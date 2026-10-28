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

The initial safety run on PR #39 head `d850a22677b083387774aef34ec4ea27092a56a9` exhausted **230 distinct states**, depth 30, with zero remaining and no invariant violation. The handoff witness reached the negated invariant in 25 states, showing the committed entry in the later leader's durable log. An earlier head was correctly rejected because its four-state profile could not process the initial higher-term vote request; `UpdateTerm` for the first follower was restored without weakening the witness. The follow-up `ReachCombinedCrashAfterApply.cfg` now also requires a durable apply and former-leader crash *after* the handoff. **This new reachability gate is not yet CI-verified.** Neither witness establishes an actual restart, and no full refinement is claimed.

## Typed combined witness export (proposed follow-up, CI pending)

The formal workflow now exports `ReachCombinedHandoff.cfg.json` and
`ReachCombinedCrashAfterApply.cfg.json` from the actual pinned TLC witness
logs using the existing typed trace exporter. A new Python validator checks
source SHA-256 provenance, complete state-variable sets, distinct term-one and
term-two leaders, a committed entry matching the later leader's durable log,
and (for the crash witness) durable apply plus an actual alive-to-offline
transition after leadership handoff. Sixteen negative mutations are exercised
across the two witnesses, including provenance, committed-entry, durable-log,
leader-role, applied-index and offline-state corruption. The checker fails
closed on missing or malformed evidence; a TLC timeout or missing witness is
still a CI failure. This is **not** a C++ correspondence or refinement proof.

These assertions and the generated JSON artifact require exact-head hosted CI
before the follow-up PR may be accepted. The existing older draft paragraphs
above are retained as dated historical records.

## Combined TLC split higher-term write envelope (draft, CI pending)

The typed 25-state handoff and 30-state durable-apply/crash witnesses are now
checked for four **ordered** transitions of the original leader: `UpdateTerm`
stages term 2 with a nil vote; the first `PersistCompleted` makes only that
term durable; `HandleRVReq` stages the vote for the successor while keeping the
positive response deferred; and the second `PersistCompleted` durably writes
the vote and releases exactly one corresponding network reply. The checker
compares typed model-state deltas and the message bag, not TLC source-line
labels. Eight new negative mutations per witness deliberately corrupt these
intermediate states, pending targets or message release. The model witness
validator now rejects 32 mutations total if the proposed branch passes CI.

**Boundary:** This tests the actual model-generated witness transitions, not
C++ execution. The compiled core combines the term/vote durable write, so
these intermediate states cannot be equated one-for-one. A separately reviewed
checkpoint relation remains necessary before claiming implementation
correspondence or refinement. Exact-head CI is required before merge.

## Three-message combined safety bound (verified October 28, 2026)

`ChoreoRaftCombinedThreeMessages.cfg` retains the restricted asymmetric
`CombinedSpec`, its three servers, two terms, one value and one log entry,
all safety invariants, and both temporal properties. It increases
`MaxMsgs` from 2 to 3, without enabling new protocol actions.

The exact-head formal workflow
https://github.com/Albert-bit07/Choreolog/actions/runs/40629355645
completed successfully: the two-message profile generated **485** states,
found **230 distinct**, reached depth **30**, and left **0** on the queue;
the three-message profile generated **2,267** states, found **873 distinct**,
reached depth **32**, and left **0** on the queue. No safety/property
violation was reported in either bounded safety run. All five regular
exact-head jobs also passed in
https://github.com/Albert-bit07/Choreolog/actions/runs/40629355614
(Ubuntu/Windows 295/295, sanitized 293/293, formatting and management API).
PR #56 was merged at `86fe7a775cfa4a0831ab0f1909339078fac252ca`.
The successful bound does **not** imply unrestricted network-fault coverage,
full Raft safety, physical power-loss correctness, or C++ refinement.

## Bounded election vote-message faults (draft, CI pending)

`CombinedVoteFaultSpec` adds only vote-request/response drops and duplicates
from the designated term-one proposer and term-two successor to the existing
restricted `CombinedNext` action set. The new
`ChoreoRaftCombinedVoteFaults.cfg` keeps the three-message, two-term,
one-entry bound and the original safety/temporal checks. Two separately
scoped expected-failure reachability configurations require an actual
duplicated vote message and the absence of a previously durably sent
third-peer vote request (a dropped request in this action subset).

A successful duplicate/drop witness demonstrates only that the bounded
fault actions are reachable. A successful safety run checks only the
restricted reachable state space, not arbitrary packet loss, AppendEntries
duplication, crashes of every node, multiple client commands, liveness,
or a model-to-compiled-core refinement relation. PR #57 is a draft until
hosted TLC and all five regular exact-head CI jobs pass; a timeout,
missing witness, or safety counterexample must be investigated.
