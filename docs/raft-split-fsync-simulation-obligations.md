# Split-fsync versus combined-write: candidate observation boundaries

**Status:** T1-B.1b research obligation; **not** an accepted refinement map.

This document states exactly what the selected SHA-pinned 32-state dual-fault
TLC witness and compiled C++ transition schedule can and cannot establish.
The executable check is in
`scripts/tests/test_tlc_dual_fault_split_fsync_relation.py`. It does not
modify the original TLA+ specification or production persistence ordering.

## Three distinct observation domains

1. **Wire-grant projection (deliberately lossy).** Observe only the first
   positive RequestVote reply from third peer `s3/c`, including its destination,
   term and grant bit. For this *selected prefix*, TLC's separate term stage,
   term fsync and vote stage have no positive grant; vote fsync releases one
   grant to `s2` in term 2. The C++ combined-write stage emits none and its
   successful persistence completion releases one to `b` in term 2.
   The TLC network *bag* contains the reply while the C++ trace records a
   *send effect*: these are not globally interchangeable observations.
   This prefix comparison is only a bounded event-correspondence check.

2. **Memory-plus-durable projection (not equivalent).** Include the in-memory
   and acknowledged durable term/vote pair. The compiled combined-write
   staging observation `(memory=2/b, durable=0/Nil)` does not appear in
   the selected model's separate-write intermediate states. The fully
   persisted `(2/b, 2/b)` endpoint does match. A literal full-state
   stuttering claim is therefore falsified on this witness.

3. **Crash/restart-sensitive durable projection (not equivalent).** A
   model-side failed vote write can leave `(term=2, vote=Nil)` durable,
   while the compiled combined-write failure retains its prior acknowledged
   `(term=1, vote=a)` record. A projection that hides this difference is
   insufficient for crash/restart refinement. Different durable terms can
   affect subsequent stale-term decisions and reply terms.

The wire-only test **intentionally demonstrates its own blind spots**:
altering the model's term-only durable state, corrupting the compiled
RequestVote correlation ID, or altering the failed C++ durable term can leave
the normalized grant event unchanged. The existing strict relation checker
must reject each altered trace. Passing the wire-only test is **never** a
substitute for passing the strict persistence and request-identity checks.

## Candidate compound-action relation — obligations, not a proof

A possible future relation would group a TLC sequence of `UpdateTerm`,
term-fsync, vote staging and vote-fsync into one abstract successful
term/vote persistence operation. Before accepting that proposal:

- **Initial/terminal states:** Specify the exact correspondence for
  term, vote, log, commit/applied indices, pending write and visible messages
  at both ends of every grouped sequence. Include the log-freshness check.
- **Intermediate steps:** Prove which intermediate TLC actions are hidden,
  why they cannot release positive grants, and whether crashes, timeouts,
  network messages or other inputs can interleave while they are hidden.
- **Failure paths:** A compound success action alone cannot simulate the
  model's term-only durable state after vote-fsync failure. Either extend
  the implementation/model with justified failure semantics or exhibit a
  different relation that preserves every relevant post-crash observation.
  Do not silently identify `2/Nil` with `1/a`.
- **Messages:** Preserve source, destination, term, candidate, log freshness,
  grant bit and request correlation; justify any model-to-C++ request-ID
  normalization. Account for TLC network-bag delivery separately from C++
  emitted effects and immediate leader heartbeats.
- **Fairness and liveness:** State scheduling assumptions separately from
  safety. A finite successful trace does not establish liveness.
- **Recovery:** Include actual reopened WAL/metadata and process restart
  observations, not just the transition core's acknowledged-durable record.
- **Validation:** Require negative counterexamples for wrong durable vote,
  early grant, wrong term/destination, stale request ID, missing callback,
  lost log, and mismatched crash/restart state. Check bounded interleavings,
  not only one hand-selected successful schedule.

A legitimate refinement claim would require a formal relation `R` and a
transition-by-transition (or justified weak/compound-step) simulation
argument across the stated state/input/failure space. These tests establish
only the finite observations described above. They do **not** establish
unrestricted Raft safety, complete TLA+-to-C++ refinement, liveness, or
physical power-loss correctness.

## Third-peer RequestVote causality (proposed bounded check)

The selected dual-failure witness also permits a more precise check than the
wire-only grant projection: the term-two RequestVote from `s2` to `s3`
must retain its candidate, term and last-log index/term while the model
performs `UpdateTerm` and term fsync. `HandleRVReq` must consume exactly
one copy before staging a single deferred positive response; vote fsync
releases that response. In the compiled schedule, the request from `b` to
`c` must carry matching candidate and log-freshness fields, and the reply
must retain its originating request ID. Negative mutations exercise request
content, multiplicity, early removal, deferred reply term, and compiled
correlation.

The comparison deliberately does **not** equate a TLC network-bag entry to
an actual C++ transport receive. It does not supply a crash/restart witness
for the model's term-only durable state, establish a global message mapping,
or justify compound-action stuttering. The existing strict durable-state
checker remains mandatory.

## Next experiments

1. Generate a TLC witness with a crash/restart *after* the term-only fsync
   and before the vote fsync, then compare subsequent stale-term replies to
   compiled restart behavior. A synthetic predicted reply is not evidence.
2. Model a reviewed combined metadata-write action with explicit success
   and failure branches, keeping the original split-write model intact for
   differential analysis.
3. Extend bounded two-term replication/handoff witnesses with network
   delivery, duplicate/lost messages, and real storage reopen outcomes.
4. Revisit the original snapshot and commit/apply mapping gaps before
   claiming any general refinement.
