# Observed model-to-core mapping gaps

These are obligations for T1-B replay, not accepted stuttering rules or proof
results. Preserve the discrepancy rather than weakening state comparisons.

## Higher-term vote staging: actual witness versus compiled C++

The exported baseline `ReachLeader.cfg` behavior contains nine states. Node
`s2` receives the vote request from `s1` in term 1. Renaming `s1 -> a`,
`s2 -> b`, `s3 -> c` does not align the intermediate durable/staged pairs:

| Step | Model staged term/vote | Model durable term/vote |
|---|---|---|
| State 3, before request handling | 0 / Nil | 0 / Nil |
| State 4, UpdateTerm staged | 1 / Nil | 0 / Nil |
| State 5, term persisted | 1 / Nil | 1 / Nil |
| State 6, vote staged | 1 / s1 | 1 / Nil |
| State 7, vote persisted | 1 / s1 | 1 / s1 |

A GCC-compiled probe of the real transition core with
`RecvRequestVote{RequestVote{1, "a", 0, 0, 41}}` instead produced:

| C++ input | Staged term/vote | Acknowledged durable term/vote |
|---|---|---|
| RecvRequestVote | 1 / a | 0 / null |
| Matching PersistCompleted | 1 / a | 1 / a |

The v2 serializer from PR #26 exposed both votes for this observation. The
request grant is released only at the second row. Existing
`RaftTransitionTest.VoteGrantAndHigherTermWaitForPersistCompletion` covers the
combined target/persist-before-reply behavior; no runtime change is proposed.

The current model has separate `UpdateTerm`, term completion, `HandleRVReq`
and vote completion actions. The combined C++ staged pair appears in no
intermediate model state above under a literal full-state projection. The
endpoints agree, but calling the intervening term/vote changes **stuttering**
would require an explicit abstraction argument; these fields did change.

Next: choose and test a relation that represents pending-write granularity,
or add a reviewed compound model action corresponding to the C++ target.
Keep negative non-durable-vote schedules effective. Do not split production
metadata writes or omit vote comparisons solely to make a trace pass.

## Other obligations visible in current source

- `BecomeLeader` updates role/epoch/progress without sending heartbeats. C++
  promotes during vote-reply handling and immediately emits two AppendEntries.
  The actual nine-state leader witness ends before any model `SendAE`; role
  and network correspondence therefore need explicit extra model steps, not
  silent message dropping.
- The model gives AppendEntries replies a leadership epoch; C++ correlates
  exact outstanding probes. Normalize message identity/coverage explicitly.
- Model `AdvanceCommit` updates knowledge, then `PersistCommit` persists it;
  the C++ core stages a durable target. Compare knowledge versus durability at
  justified boundaries rather than treating staged commit as disk truth.
- Model `Apply` advances one entry; core `ApplyCommitted` can release a batch.
  A multi-step correspondence must account for every entry and completion.
- Model `Torn` can change disk on failure/crash. The core retains acknowledged
  disk state until a production host reopens actual storage. Recovery input and
  observed disk must be represented separately from the old core projection.
- Models and v1 traces use full index-1 logs. Snapshot install/compaction and
  authoritative application/receipt payloads need a new mapping/model scope.

The exporter preserves enough source information to investigate these gaps;
it does not resolve them. None is marked BLOCKED merely for being difficult.
