# T1-B: typed TLC witness export — 2026-10-20

Scope: preserve actual full TLC witness states as typed JSON, add parser
regressions and publish all seven existing witnesses from model CI. This is a
prerequisite for C++ replay, not a refinement result. No model bounds,
invariants, runtime behavior or disk formats changed.

## Validation

- Combined main (PRs #25/#26) normal GCC Debug suite: **286/286**, 34.33s.
  Local Debug used `-g0` to fit constrained storage; assertions, optimization
  level and warnings-as-errors retained. Benchmark targets disabled locally.
- Combined ASan/UBSan suite: **284/284**, 51.44s. Existing CI
  excludes only `^CatchUpTest` (two stress cases). CI leak detection unchanged.
- Six Python parser regressions pass; Black 100 check and Python compilation
  pass. Reject malformed/truncated witnesses, duplicate/missing state fields,
  wrong invariants, gaps and nesting/state limits; preserve nested record keys,
  sequences, sets, functions, model values and quoted strings.
- All seven actual pinned TLC 1.7.4 behaviors exported, preserving 15 variables
  per state: negative vote 18 states, different-term leaders 18, leader 9,
  replication 14, commit 16, application 20, offline 2.
- Fresh application witness produced by TLC: expected exit 12 and exact
  invariant, all 20 states exported. Exact workflow export loop run against
  actual logs produced seven JSON artifacts. Shell syntax check passed.
- No production C++ source changed in this slice. Existing snapshot diagnostics
  from merged #26 included in combined broad gates.

Local LSan cannot inspect task threads in this environment (minimal ASan
program reproduces it); ASan/UBSan used `detect_leaks=0:halt_on_error=1`, with
UBSan halt/stack traces. Hosted CI retains leak checking. Dependencies reuse
pinned installed packages with `VCPKG_MANIFEST_INSTALL=OFF`; manifests unchanged.
Disk-full attempts are retained in ignored raw logs, not counted as passes.
Normal reproducible object/binary products removed after the successful suite
to make room for sanitizer linking. Source and installed dependencies retained.

## Acceptance boundary

Exporter parses full typed values and records model/config/log hashes plus TLC
version/source locations. CI binds artifacts to its controlled invocation.
No input normalization, C++ replay, trace projection, snapshot model or justified
stuttering is implemented. See `docs/raft-model-mapping-gaps.md` for the concrete
higher-term term/vote staging discrepancy; do not drop fields to force agreement.
Next: resolve that relation explicitly, then replay behaviors with divergence
mutations. Remaining T1-B and later roadmap gates remain unfinished.


Hosted PR #27 TLC run 39504905569 passed on head 19cad22. Downloaded artifact
12302079273, SHA256 e7dc846907ecb8d2eaa0d5f9485aa2862c0c9fe721ee487312db712d2c5bf1aa:
all seven typed JSON behaviors retain 15 variables/state. Hosted offline witness
has three states (local baseline two); other witness lengths match above.
Exact-head broad CI run 39504905349 passed all five jobs. PR #27 merged
as f24073b126d41d99761ecf13ffa8221894ea4d1c under conditional authorization.
