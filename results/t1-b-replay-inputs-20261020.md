# T1-B: compiled-core schedule input — 2026-10-20

Adds `--replay-inputs` to the existing C++ conformance harness. Schedules drive
actual transitions, emitted messages and pending tokens, without synthesizing
network replies. Normal and snapshot scenario modes remain unchanged. No
production runtime, persistence format, wire format, model or invariant changed.

| Check | Result |
|---|---|
| Full GCC Debug suite | 287/287, 34.66s |
| Full selected ASan/UBSan suite | 285/285, 52.99s |
| Driver integration checks | 8 pass against normal and sanitizer runners |
| clang-tidy 18 | Strict CI bugprone/performance options pass on modified harness |
| clang-format 18 / Black 100 | Pass |
| Python discovery | 6 exporter tests pass; runner-dependent class explicitly skipped; CTest executes it with runner |

Only the existing two `^CatchUpTest` stress cases are excluded locally under
sanitizers. Local LSan remains unavailable (`detect_leaks=0`); CI keeps leak
checking. Normal Debug uses -g0 for disk constraints, retaining assertions/O0
and warnings-as-errors. Benchmarks disabled locally; pinned installed packages
reused with manifest install off. No source gate, seed or timeout weakened.

Checks exercise delayed vote effects, unavailable/early-message rejection,
loss, exact duplicate identity, failed-write halt/restart, quoted command values,
replication through durable commit/application/compaction and malformed commands.
A pre-rebuild negative run caught missing advance/apply/compact operations;
implementation then passed. Static analysis caught an optional access behind a
custom helper; an explicit guard fixed it. Final source passed strict analysis
with only the repository's existing enum-width/signature check exclusions.
An earlier sanitizer run passed 285 tests (59.30s) before that equivalent guard
cleanup; the final complete rerun above is the accepted result.

See docs/raft-replay-inputs.md for grammar, bounds and limitations. This is a
schedule executor, not a TLC normalizer or refinement checker. The model's split
term/vote persistence, separate leader promotion/heartbeats, message identity,
recovery and snapshots still need an explicit correspondence. No stuttering
claim is made and no projection field was dropped to force agreement.

PR preparation: current base main f24073b (merged export #27). Hosted exact-head
CI is required before merging this slice under existing user authorization.
