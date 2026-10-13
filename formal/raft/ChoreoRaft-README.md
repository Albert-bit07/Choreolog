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
