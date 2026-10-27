# Conservative TLC witness election-prefix normalization

This is an incremental **T1-B.1b input-normalization slice**, not a TLC-to-C++
refinement check. `scripts/tlc_witness_prefix.py` reads the existing version-1
**typed** witness JSON from `scripts/export_tlc_trace.py` and produces schedule
commands accepted by `choreoos-raft-conformance --replay-inputs`.

Only two model steps are currently supported:

1. `Timeout(i)` with exactly one staged candidate term/self-vote and a pending
   durable target, two deferred vote requests, no early network release, and no
   unrelated model-state change. Maps `s1/s2/s3` to `a/b/c` and emits `timeout`.
2. `PersistCompleted(i)` with a matching pending target, no extra disk writes,
   exactly the deferred messages released to the model bag, and no unrelated
   state changes. Emits `complete`.

The normalizer compares TLA+ sets and function mappings semantically, not by
printed ordering; sequences retain their order. It checks all 15 model
variables at every **included** state. An unsupported action or ambiguous
transition fails with nonzero status. A prefix may be selected only with an
**explicit** `--max-transitions` bound; remaining states are not checked. The
output is written only after successful normalization of the selected steps.

## Reproduce a bounded witness prefix

Use the SHA-verified `tlc-witness-behaviors` artifact from the earlier model
workflow (run 39504905569, artifact 12302079273; ZIP SHA-256
`e7dc846907ecb8d2eaa0d5f9485aa2862c0c9fe721ee487312db712d2c5bf1aa`).
After extracting the artifact, run:

```sh
python3 scripts/tlc_witness_prefix.py \
  --witness ReachLeader.cfg.json --max-transitions 2 \
  --output leader-prefix.schedule
cat leader-prefix.schedule
# timeout a
# complete a
```

Local evidence on 2026-10-26: this explicit two-step prefix normalized for
six of seven archived TLC witnesses. `ReachOffline.cfg.json` correctly rejected
its unsupported action; the entire nine-state `ReachLeader` witness is **not**
normalized. The archived leader witness also rejected deliberately mutated
premature message release, altered durable vote, and forged pending callback.
The Python unit tests passed (4 test methods, including 8 distinct negative
mutations); CI additionally runs them through CTest on supported platforms.

## Strictly unverified boundaries

- This tool does **not** run C++, compare any C++ projection, validate the
  exported witness's provenance, or prove refinement. A generated schedule
  being accepted by a runner is not proof of matching states.
- The next `ReachLeader` step is `UpdateTerm` on a received vote request.
  The model separates higher-term staging, term persistence, vote staging,
  and vote persistence, whereas the C++ transition core combines the
  staged term/vote target. Do not erase or label these differences as
  stuttering without a reviewed, tested relation.
- Message identity, election response delivery, immediate C++ leader
  heartbeats versus model `BecomeLeader`, commit/apply granularity, torn
  storage, snapshots, and all non-election action mappings remain TODO.
- Do not infer protocol actions from generic TLC source locations (line 410).
  Semantic deltas and exact side effects are required.

Next: implement a reviewed mapping for `UpdateTerm` and durable term/vote
completion, then connect normalized schedules to the compiled runner and
compare explicitly defined correspondence checkpoints with negative mutants.

## Bounded compiled-core projection check (follow-up branch)

The new `tlc-witness-core-prefix-tests` CTest entry invokes the **compiled**
`choreoos-raft-conformance --replay-inputs` runner with the two commands
produced by the synthetic typed witness fixture. It checks the actor's staged
term/vote, role, availability, durable term/vote, commit/apply indexes, empty
logs, pending persistence-token continuity, no premature vote requests, and
the two vote requests released after completion. Twelve independently
corrupted C++ output mutations must be rejected. The normalizer's existing
typed-state checks remain unchanged.

**Scope:** The fixture is synthetic, not an archived TLC witness. This test
exercises an actual compiled transition core but does **not** establish that
any real TLC witness was replayed, that messages match by identity/destination,
or that the higher-term term/vote split corresponds. CI results must be
recorded against the exact PR head before accepting this follow-up.

## Outbound vote identity comparison (next bounded slice)

The v2 replay harness now exposes each emitted message's destination, term,
request ID, and, for `RVReq`, the candidate and last-log index/term. These are
diagnostic-only fields added to the existing `effects.send` objects; the
production wire protocol and v1 trace format are unchanged. The compiled-core
prefix checker compares the two deferred model vote requests against the actual
emitted destination/payload set, rejects duplicate destinations and request IDs,
and requires positive integral request IDs. Eight new negative mutations cover
wrong/duplicate destinations, wrong candidate/term/freshness and invalid or
repeated IDs. The model does not assign C++ request IDs, so only uniqueness and
presence are asserted, not an invented numeric equality.

**Verification boundary:** This remains a synthetic two-transition election
fixture. No actual TLC witness, higher-term staged-vs-durable split, heartbeat,
or full-network identity mapping is claimed. Exact-head CI must pass before
merging this slice.

### Existing snapshot contract compatibility

Adding v2 outbound destination/term fields also changes diagnostic snapshot
reply objects. The existing snapshot trace contract is kept **strict**:
acknowledgements must match the admitted request's destination, term, index,
and request ID. Two additional negative mutants reject a wrong destination
or reply term (ten snapshot mutations total). This is a diagnostic schema
extension, not a change to snapshot installation or production messaging.

## Archived real TLC leader checkpoint bridge (draft)

`scripts/tests/fixtures/reach_leader_tlc_20261020.zlib.b64` stores a compressed,
SHA-256-verified copy of the actual archived nine-state `ReachLeader.cfg.json`
witness (artifact 12302079273; uncompressed SHA-256
`e5273d42f856bd8dbdc265bfc5cef6b0fcce0f18f10c091894a6a78b7ce4015c`).
`scripts/tests/test_tlc_real_leader_bridge.py` verifies the model's separate
term-staging, term-fsync, vote-staging, and vote-fsync states. It then drives the
**compiled** core with `timeout a`, `complete a`, `deliver a b RVReq`,
`complete b`, and `deliver b a RVResp`. The checker compares the initial
candidate prefix, the durable granted-vote checkpoint, and the leader role,
including vote reply destination/term and the two immediate core heartbeats.
Four model-side and fifteen core-side deliberately corrupted observations must
fail. It is registered with CTest on Linux, Windows and sanitizer jobs.

**This is checkpoint correspondence, not full TLC refinement.** Model states
4–6 have no literal matching compiled row because the model stages and fsyncs
term/vote separately while C++ combines them. Model state 8 is a candidate
before `BecomeLeader`, while C++ promotes on `HandleRVResp` and sends
heartbeats immediately. No model network bag is silently equated to C++ wire
messages. Actual real witness provenance is pinned, but no proof of an
abstraction relation for intermediate states is asserted. Hosted exact-head CI
is pending; do not merge without all five green jobs.

## Real witness network-bag conservation (draft follow-up)

The archived nine-state ReachLeader test now additionally compares the model
network bag across UpdateTerm, term fsync, vote staging, vote fsync, reply
delivery and BecomeLeader. The exact s1→s2 RequestVote remains in the bag through
UpdateTerm and term fsync; HandleRVReq consumes it while staging the vote; durable
vote completion must add exactly one s2→s1 term-1 VoteReply without changing
other messages; reply delivery removes it; and model BecomeLeader must not
silently invent heartbeats. The compiled reply must echo the request ID of the
actual vote request sent to node b. Six model-side and one compiled-side
negative mutations exercise these new assertions (ten model and sixteen core
mutations overall). These are scoped network conservation checks, **not** an
abstraction proof for split term/vote durability or a model-to-wire identity
mapping. Hosted exact-head CI is required before merging this follow-up.


## Explicit split-write / combined-fsync envelope (draft)

The archived nine-state leader witness now has an explicit *bounded
write-collapse envelope* in `check_write_collapse`. It checks **all five**
model snapshots surrounding UpdateTerm, term fsync, HandleRVReq, and vote fsync,
including the separate pending term-only and term-plus-vote targets. Against
the compiled core it requires a single staged term/vote target, old durable
state until the persistence callback, one continuous pending token, and
atomic installation of the durable term/vote before the granted reply is
released. Twelve negative mutations independently corrupt intermediate model
writes, pending targets, staged/durable core values, persistence tokens and
premature/missing replies.

This is a reviewed **envelope for one archived election witness**, not a
general forward simulation, stuttering proof, or a claim that model states
4–6 equal C++ states. Hosted exact-head CI must pass before merge.
