# Agent progress ledger

## Environment facts
Session 2026-10-20. Linux development terminal and GitHub connector accessible.
GCC 13.3, CMake 4.4.4, Ninja 1.13.2, clang-format 18.1.8.
Pinned vcpkg `6e856794a` bootstrapped; baseline configure in progress.
.NET SDK, Docker and Java not initially installed. No test result claimed yet.

## Repo state at session start
Main: `2eb118e46c56275fce64fd2c9ed66bb8373b0c37` (PR #23 merged).
PR #24 is open, draft, head `0a4471ddcb7dd2a483195c5a043ada3f348c1a58`;
it owns README, technical overview and guided demo prose. Do not duplicate it.
PR #24 was merged externally during the session as
`332b11b681a6f2f5d007daa25ba67f0b127e5280`; branch fast-forwarded to it.
Runtime source is unchanged. No merges performed by this agent or authorized. Latest ledger lives on `agent/t1-a-baseline-20261020`, not main.

## Task table
| ID | Title | Status | Implemented | Tested | Validated | Verified | Acceptance criteria | Evidence |
|---|---|---|---|---|---|---|---|---|
| T1-A | Baseline, CI, truthful docs | DOING | no | no | no | no | Full baseline, inventory, executable deviations, current docs | Pending |
| T1-B | Consensus/conformance | TODO | partial | no | no | no | Trace v2, bounded models, migration gates | Existing core; evidence reinspection pending |
| T1-C | Public read consistency | TODO | partial | no | no | no | Public quorum reads and fault history checker | PR #23 provides primitive only |
| T1-D | Benchmarks/profile | TODO | no | no | no | no | Reproducible report and optimization decision | Pending |
| T1-E | Durability/security | TODO | no | no | no | no | Bounded receipts, power-loss tests, management auth | Pending |
| T1-F | Observability/demo | TODO | no | no | no | no | Real three-process failover and convergence assertions | Pending |
| T2-A/B/C | Product/orchestration/packaging | TODO | no | no | no | no | Tier 1 gate first | Deferred by charter |
| T3 | Research/stretch | TODO | no | no | no | no | Precisely scoped proof artifacts | Deferred by charter |

## Decisions log
- 2026-10-20: Start from live main; keep PR #24's presentation work independent.
- Evidence columns describe this session's evidence; prior PR claims need reinspection.

## Blockers
No roadmap blocker established. Environment setup attempts:
- Plain `apt-get update` failed with `setgroups (1: Operation not permitted)`.
- Root-sandbox apt update succeeded; broad install did not progress downloads and
  was terminated before installation. No system permission escalation requested.
- Installed CMake/Ninja/format from pip; pinned vcpkg configure initially failed
  `Could not find pkg-config`. Extracted pkgconf Debian packages locally and
  restarted configure with a local executable wrapper. Dependency build ongoing.

## PR integration queue
No new PR yet. PR #24 was merged externally; no merge authorization for this agent.
Main CI on `2eb118e` succeeded: [run 39168201024](https://github.com/Albert-bit07/Choreolog/actions/runs/39168201024).
PR #23 reviewed head `dca5f46` passed all five jobs in run 39042874728;
this is historical CI evidence, not local execution or a new formal result.
Latest main `332b11b` CI was still in progress when inspected.

## Next 3 actions
1. Establish toolchain and run baseline build/full tests.
2. Inspect CI and inventory consensus fields/persistence edges against source.
3. Complete T1-A documentation/regression evidence, prepare PR, then next eligible Tier 1 task.
