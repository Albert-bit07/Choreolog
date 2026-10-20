# T1-A baseline — 2026-10-20

Runtime source: `332b11b681a6f2f5d007daa25ba67f0b127e5280` (same runtime as PR #23).
Documentation branch: `agent/t1-a-baseline-20261020`, PR #25. No runtime change.
Linux 6.18.55, GCC 13.3.0, CMake 4.4.4, Ninja 1.13.2, Python 3.12.14,
clang-format/clang-tidy 18.1.8. vcpkg baseline `6e856794a` from manifest;
Boost 1.92.0, GoogleTest 1.18.0, OpenSSL 3.6.4. Java 17.0.20, pinned TLC 1.7.4.
These are correctness checks in a shared container, not performance benchmarks.

## C++ checks

| Check | Result |
|---|---|
| Full normal CTest | 100% tests passed out of 279; Total Test time (real) =  36.53 sec |
| ASan + UBSan (local LSan unavailable) | 100% tests passed out of 277; Total Test time (real) =  50.85 sec |
| Repository clang-format 18 | All C++ files pass dry-run/Werror |
| Markdown link targets | Changed docs' local targets exist |
| Existing trace checker | Five scenarios, 32 rows, six mutations rejected |

The normal suite includes seeds 1–8 in PropertyTest, real TCP/mTLS, crash
ordering, snapshot redo faults and `transition-runtime-process-recovery`.
The sanitizer selection excludes only the two existing CatchUp stress tests,
matching CI; those passed normally. No timeout, assertion or seed was changed.

Commands (after bootstrapping pinned vcpkg and exporting PATH/VCPKG_ROOT):

```sh
cmake --preset linux-gcc
cmake --build --preset linux-gcc --parallel 4
ctest --preset linux-gcc --timeout 240
cmake --preset linux-sanitizers -DCHOREOOS_BUILD_BENCHMARKS=OFF \
  -DVCPKG_INSTALLED_DIR="$PWD/build/linux-gcc/vcpkg_installed"
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 cmake --build --preset linux-sanitizers --parallel 2
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1 \
  ctest --preset linux-sanitizers --timeout 240 -E '^CatchUpTest'
```

**LeakSanitizer limitation:** default build failed at GoogleTest discovery:
`Can't open /proc/296/task for reading` followed by
`LeakSanitizer has encountered a fatal error` and the ptrace hint.
Retrying with `LSAN_OPTIONS=verbosity=1:log_threads=1` reproduced it. A minimal
`int main() { return 0; }` built with `g++ -fsanitize=address` failed identically
(`/proc/2/task`). Local runs therefore cover ASan/UBSan, **not leak checking**.
CI retains `detect_leaks=1`; repository sanitizer settings were not changed.

## Existing bounded models reproduced

TLC jar SHA-256: `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.
All positive runs finished with zero states left on the queue at the existing
checked-in bounds. No configuration/model was edited.

| Configuration | Distinct states | TLC reported duration |
|---|---:|---|
| Election.cfg | 32,315 | 01s |
| ElectionTwoTerms.cfg | 185,762 | 04s |
| Replication.cfg | 62,904 | 02s |
| ChoreoRaftCore.cfg | 5,090 | 01s |
| ChoreoRaftRecovery.cfg | 88,966 | 09s |
| ChoreoRaftTwoTermElection.cfg | 46,897 | 05s |

Each of these expected counterexamples returned exit 12 and its named invariant:
`NegativeNonDurableVote`, `ReachTwoLeadersDifferentTerms`, `ReachLeader`,
`ReachReplication`, `ReachCommit`, `ReachApplication`, `ReachOffline`.
A successful witness search is not a safety proof.

From `formal/raft`, use the exact CI commands with the verified jar; local
runs used `-Xmx1g -workers 2` and separate metadirs. The scope is the existing
individual profiles: **no combined multi-term replication/snapshot model or
model-to-C++ refinement has been established**.

## CI and remaining evidence

Main `332b11b` passed all five jobs in
[run 39497829946](https://github.com/Albert-bit07/Choreolog/actions/runs/39497829946):
Linux, Windows, format, management API and API audit. Management/.NET and
Windows results are CI-only, not run locally. Audit includes leak checking.
TLC was reproduced locally; the workflow's path filter does not rerun it on
prose-only changes.

This baseline does not complete T1-B–F. No three-process terminal recording,
new benchmark, public linearizable read, receipt retention or management auth
is claimed. The [inventory](../docs/consensus-field-inventory.md) links current
boundaries and persistence edges to executable regression filters. Raw local
logs are gitignored under `results/raw/t1-a-baseline-20261020/`; the reproducible
commands and measured summaries above are the committed evidence.
