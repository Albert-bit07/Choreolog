# Agent progress ledger

Latest implementation checkpoint: `agent/t1-b-snapshot-traces-20261020`, based
on main `332b11b681a6f2f5d007daa25ba67f0b127e5280`; not integrated on main.
No merges authorized. User preference: idiomatic, reviewable code matching
repository style; focused functions and comments that explain why.

## Execution charter and environment
The full user charter and baseline source inventory are preserved on
[PR #25](https://github.com/Albert-bit07/Choreolog/pull/25), branch
`agent/t1-a-baseline-20261020` (`docs/agent-execution-charter.md`). Read Sections
0–4 before resuming. This branch is independent of that unmerged prose work.
Linux, GCC 13.3, CMake 4.4.4, Ninja 1.13.2; clang-format/tidy 18.1.8;
pinned vcpkg 6e856794a; Java 17 and SHA-verified TLC 1.7.4. .NET/Docker absent.
Git clone/fetch and GitHub connector work; terminal push lacks credentials.
Save with connected tree/commit/ref APIs; never expose credentials.

## Repo state at session start
Main started at 2eb118e (PR #23). PR #24 was merged externally as 332b11b;
only presentation docs changed. This agent performed no merge.
Main 332b11b passed all five CI jobs in run 39497829946. No open implementation
PR conflicted when this slice began. Inspect live main/PRs again before resuming.

## Task table
| ID | Title | Status | Implemented | Tested | Validated | Verified | Acceptance criteria | Evidence |
|---|---|---|---|---|---|---|---|---|
| T1-A | Baseline and source audit | DONE | yes | yes | yes | no | Baseline/full suite, inventory, truthful docs | PR #25 report; 279/279 normal, 277/277 local ASan/UBSan; not merged |
| T1-B.1a | Snapshot diagnostic traces | DOING | yes | yes | no | no | V2 golden, v1 compatibility, five fixtures, eight negative cases, full gates | [Contract](raft-snapshot-traces.md); [Evidence](../results/t1-b-snapshot-traces-20261020.md); local broad gates pass, remote CI pending |
| T1-B.1b | TLC trace replay/refinement | TODO | no | no | no | no | Actual TLC behavior ingestion and justified projection/stuttering with negative tests | Separate from diagnostic contracts |
| T1-B.2 | Combined bounded models | TODO | no | no | no | no | Multi-term replication/loss/dup/restart, Figure 8 and snapshots | Existing scoped profiles only |
| T1-B.3 | Default transition backend | TODO | no | no | no | no | Dual-backend parity, process failover, 1000 seeded schedules, benchmark gate | Legacy remains default |
| T1-C | Public linearizable reads/checker | TODO | partial | no | no | no | Public quorum path, minority-old-leader/election tests, known-bad histories | Primitive from PR #23 only |
| T1-D | Benchmarks/profile | TODO | no | no | no | no | Release baseline/profile and evidence-based optimization decision | No new performance claim |
| T1-E | Durability/security | TODO | no | no | no | no | Bounded receipts, lost-write harness, management auth | Unfinished |
| T1-F | Metrics and real failover demo | TODO | no | no | no | no | One command, three processes, recovery/state assertions and captured output | Unfinished |
| T2-A/B/C | Dashboard/Temporal/packaging | TODO | no | no | no | no | Tier 1 gate first | Deferred |
| T3 | Research/stretch | TODO | no | no | no | no | Precise proof scope before implementation | Deferred |

## Decisions log
- Risk: high, because diagnostics/checkers can misstate correctness.
- Invariant: only matching durable completion publishes snapshot boundaries and
  releases dependent replies; compaction preserves exact suffix, install retains
  it only for an agreeing anchor; durable absolute indexes never regress.
- Add v2 alongside v1, preserving v1 bytes and refusal behavior. No persistence,
  node wire, storage or consensus behavior change; pure core remains untouched.
- Public read diagnostics stay unsupported. Snapshot trace checking does not
  imply TLC refinement, snapshot payload validation or general safety proof.

## Blockers and limitations
No roadmap blocker established. Local LSan cannot inspect sandbox task
threads. Three reproductions: default gtest discovery, verbose LSAN retry,
minimal ASan `int main(){return 0;}`. All fail with LSan fatal task/ptrace errors.
Local ASan/UBSan runs use detect_leaks=0; CI retains detect_leaks=1. See PR #25
baseline report. This does not justify skipping normal tests or weakening CI.

## PR integration queue
- PR #25, head 19478b723a1afc344df50ca439d8583d50ba1d49, base 332b11b:
  baseline accepted on branch, latest CI pending, draft, not merged.
- Snapshot trace slice: prepared for a draft PR; 285/285 normal and 283/283
  local ASan/UBSan pass. Exact-head remote CI required before ready status.
- No branch depends on an unmerged code change; no merge authorization.

## Next 3 actions
1. Run full normal/sanitizer gates and targeted clang-tidy on the snapshot slice;
   preserve any failure and compare current main before preparing its PR.
2. Implement TLC behavior export/replay and a precise model projection, including
   snapshots/compaction and justified stuttering; diagnose mismatches honestly.
3. Complete bounded-model and migration gates in order, then public read work.

## Implementation checkpoint
[Evidence](../results/t1-b-snapshot-traces-20261020.md): full normal 285/285
(35.20s), local ASan/UBSan 283/283 (50.35s), 8 trace unit tests, v1 six and
v2 eight mutations, strict affected-source clang-tidy and format pass.
No refinement claim; diagnostic slice waits for exact-head remote CI.
Next independent eligible work is TLC behavior export/replay on a separate
branch from current main. It need not depend on merging the diagnostic API.
