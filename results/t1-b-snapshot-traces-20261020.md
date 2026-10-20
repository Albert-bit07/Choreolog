# Snapshot diagnostic trace evidence — 2026-10-20

Branch `agent/t1-b-snapshot-traces-20261020`, base main
`332b11b681a6f2f5d007daa25ba67f0b127e5280`. Same GCC/vcpkg environment as
PR #25's baseline; no runtime consensus/storage/network behavior changed.

| Gate | Actual result |
|---|---|
| Full normal CTest | 285/285, 35.20s |
| Local ASan/UBSan CTest | 283/283, 50.35s; only existing CatchUp exclusions |
| Focused trace unit suite | 8/8, including 5 new v2 tests and 3 unchanged v1 tests |
| V1 checker | 32 rows, 5 scenario families, all 6 original mutants rejected |
| V2 checker | 26 rows, 5 scenario families, all 8 targeted mutants rejected at expected checks |
| Schema validation | 26 generated v2 rows and committed golden accepted by jsonschema |
| clang-tidy 18 | Strict bugprone/performance checks pass on trace.cpp and scenario harness |
| clang-format 18 | Touched C++ dry-run/Werror passes |
| Python syntax / diff | py_compile and git diff --check pass |

Full CTest includes the existing property workloads' seeds 1–8, real TCP/mTLS,
crash-ordering, snapshot redo and process-recovery tests. The diagnostics add
no clock, filesystem or socket access to the core. No new persistence ordering,
format migration or default selection is introduced.

## Reproduction

```sh
cmake --build --preset linux-gcc --parallel 4
ctest --preset linux-gcc --timeout 240
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 cmake --build --preset linux-sanitizers --parallel 2
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1 \
  ctest --preset linux-sanitizers --timeout 240 -E '^CatchUpTest'
clang-tidy-18 -p build/linux-gcc \
  --checks='-*,bugprone-*,performance-*,-performance-enum-size,-bugprone-easily-swappable-parameters' \
  cpp-runtime/src/raft/trace.cpp cpp-runtime/tests/integration/raft_conformance_scenarios.cpp \
  --warnings-as-errors='bugprone-*,performance-*'
```

The local executable is `clang-tidy` version 18.1.8 from pip; the command above
uses CI's executable name. Local LeakSanitizer is unsupported by this sandbox's
task/ptrace restrictions, as reproduced and documented on PR #25. CI retains
`detect_leaks=1`. Normal and final sanitizer evidence were completed in separate
runs; an earlier sanitizer log ended without a summary and is **not** counted.
No assertions/timeouts/seeds or repository sanitizer options were relaxed.

The first static-analysis attempt found unchecked optional accesses in the
scenario harness (including two pre-existing ones). A checked `persist_token`
helper now throws on missing intents; no analyzer suppression was added.

## Acceptance and limits

V2 preserves full staged/durable suffixes and votes, absolute snapshot indexes,
input readiness and request context; v1 output/refusals remain unchanged.
Compaction/install checks bind completion and acknowledgements to the exact
admitted target. An extra mutation ensures a term-only incomplete transfer
cannot satisfy installation coverage.

The [contract document](../docs/raft-snapshot-traces.md) states the precise
scope. These are deterministic diagnostic traces, **not TLC-generated behavior
replay**, payload/receipt validation, liveness evidence or a formal refinement.
T1-B remains unfinished. Snapshot-aware TLA+ transitions, real TLC behavior
export/replay, projection/stuttering arguments and combined multi-term checking
remain required before migration gates/default changes.

Broad remote CI on the PR's exact head is pending. No PR-ready, integrated or
formal-verification claim is made until its corresponding evidence exists.
Raw local logs are gitignored under `results/raw/t1-b-traces-20261020/`.
