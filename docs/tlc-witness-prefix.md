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
