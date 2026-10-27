# Agent progress ledger

> **T1-B.2 combined TLC safety slice — 2026-10-27 (DRAFT, UNVERIFIED).** PR #38 exact head `e146fc779418744ac3b752f0fa3f33e53a1d6868` passed all five required jobs in run 40469313051 (Ubuntu, Windows, format, management API, API audit), no submitted reviews or unresolved threads; main was unchanged at `a9a51663f107194c5e1bc1eb01ece3f99db5ae49`, and PR #38 merged using `expected_head_sha` as `315f5354496b6db1040a8936a578009eeeeb2130`. The new isolated branch `agent/t1-b-combined-tlc-profile-20261027` proposes a restricted two-term, one-entry asynchronous `CombinedSpec` that includes replication, commit/apply, one lost vote request, higher-term handoff and former-leader crash/restart or failed persistence. Two TLC configurations check all existing safety invariants and require a negated-invariant committed-entry handoff witness. Symmetry is disabled for designated actors. **No TLC run has yet passed for this branch; it is not merged.** A timeout or missing witness must fail CI. This is not unrestricted model exploration, full implementation refinement, or a crash/restart witness. Next: inspect formal CI, fix soundness/tractability defects without weakening invariants, then expand recovery/witness coverage. Historical checkpoints below are retained.\n\n> **T1-B.2 bounded combined compiled-core schedule — 2026-10-27 (DRAFT, CI NOT YET VERIFIED).** PR #37 head `da29b1134efa91bdcb7cdfca56f79844028cd1dd` passed all five exact-head jobs in run 40463929575 (Ubuntu, Windows, format, management API, API audit), no submitted reviews or unresolved threads, and merged after checking unchanged main as `a9a51663f107194c5e1bc1eb01ece3f99db5ae49`. The isolated branch `agent/t1-b-replicated-handoff-20261027` adds a fixed compiled-core schedule combining a dropped election message, durable replication, durable commit/apply, higher-term leader handoff and restart, plus twelve negative observation mutations. This branch is unmerged and has not yet passed hosted CI; no local C++ compilation was possible because github.com DNS resolution failed. This is not a combined TLC model, exhaustive interleaving check, or refinement proof. Next: exact-head CI, then bounded TLC combined model/config and formal correspondence. Older checkpoints below remain dated history.

> **T1-B.2 two-term compiled replay precursor — 2026-10-27 (DRAFT, NOT CI-VERIFIED).** PR #36 head `2c6eba54f3ac246eb9f46d2ea923e1ea0a7c6e32` passed all five exact-head CI jobs in run 40459600894 (Ubuntu, Windows, formatting, management API, API audit), with no submitted reviews or unresolved threads; it merged as `e6fedc4fef47537e48a651665f826156e93e4ff1` after unchanged-main verification. The new isolated branch `agent/t1-b-two-term-replay-20261027` adds a compiled-core two-term leader-change schedule: node a becomes leader in term 1, node b campaigns in term 2, the old leader stages and durably grants the higher-term vote, node b becomes leader, and restart of a must retain term 2/vote b. Thirteen negative trace mutations and a failed-fsync negative schedule reject premature/unpersisted vote replies. **Unverified until exact-head hosted CI; not a TLA+ bounded combined model or full refinement.** Next: validate CI, then add actual TLC bounded multi-term election/replication scenarios and formal correspondence. Older checkpoints remain historical.

> **T1-B.1b explicit split-write envelope — 2026-10-27 (DRAFT, NOT CI-VERIFIED).** PR #35 head `750b828d39db36261b1dd57afc5479250795e72c` passed all five exact-head jobs in run 40455691843, with no review submissions or unresolved threads, and merged as `a796c57e2a964815b7b0f6cccd66136fcabca536` after verifying main unchanged. The new isolated branch `agent/t1-b-split-write-envelope-20261027` adds an explicit bounded relation between all five model term/vote split phases and the C++ combined staged/durable write, including pending-target consistency, callback token continuity, and reply-after-fsync; twelve negative mutations. It has **not** passed hosted CI, has **not** merged, and does not prove full TLC refinement. Next: verify CI, then develop bounded combined models and broader network correspondence. Older checkpoints retained.

> **T1-B.1b network-bag conservation follow-up — 2026-10-27 (DRAFT, NOT CI-VERIFIED).** PR #34 exact head `6d3e43b1fe267f315872ce3e9880ce453ca63605` passed all five jobs in run 40449335274 and merged as `37de6db190e8ed5fb25a5faced95de5752544fba`. This isolated branch `agent/t1-b-real-witness-network-bag-20261027` tightens the archived nine-state TLC witness check with model network-bag conservation: UpdateTerm and term fsync retain the request, HandleRVReq consumes it, vote fsync releases the reply, then reply delivery consumes it, plus C++ reply/request ID correlation and seven negative mutations (six model, one compiled). The initial hosted Ubuntu run exposed an incorrect test assumption that UpdateTerm consumes the vote request; this was repaired to require consumption at HandleRVReq without relaxing network conservation. It is a draft, not merged or locally C++-compiled. **No full refinement claimed**; explicit pending-write abstraction and bounded combined models remain TODO. Older dated entries retained.\n\n> **T1-B.1b real witness checkpoint bridge — 2026-10-27 (DRAFT; CI NOT YET VERIFIED).** PR #33 merged as `f5b35c7f016260eba32c4e0b9b42015c57733a37` after all five exact-head jobs succeeded (run 40445054148). This isolated branch adds a SHA-256-pinned copy of the actual nine-state 2026-10-20 `ReachLeader.cfg.json` TLC witness (uncompressed SHA-256 `e5273d42f856bd8dbdc265bfc5cef6b0fcce0f18f10c091894a6a78b7ce4015c`), a CTest-registered compiled C++ schedule with five core steps, explicit selected staged/durable checkpoint comparison, vote-reply destination/term and immediate heartbeat checks, four model and fifteen core negative mutations. It intentionally **does not** equate model states 4–6 to any C++ row or claim full refinement: split model term/vote persistence and combined C++ durable write remain different, as do leader-promotion/heartbeat timing and model net bags. No exact-head CI for this branch yet; local C++ compilation unavailable. Historical entries retained.\n\n> **PR #33 CI repair checkpoint — 2026-10-27 (DRAFT, UNMERGED).** The first exact-head runs caught formatting, a C++ `quoted` name-resolution error, and a `-Wshadow` error; all were repaired without changing test assertions. At head `c381d91b074096176fc23f1080c3cccc3867c92a`, Ubuntu built and the new compiled prefix test passed, but existing `raft-snapshot-trace-contract` failed because the additive v2 send destination/term fields changed strict snapshot acknowledgement objects. This was an actual contract regression. The branch now updates `scripts/raft_snapshot_contract.py` to check destination and term explicitly, adds two negative mutations (ten snapshot mutants total), and updates the expected mutation count; no validation was weakened. Windows on an earlier head independently failed vcpkg benchmark download due to github.com DNS resolution. Current head must pass all five exact-head CI jobs before merge; real TLC witness refinement remains TODO. Historical entries below are preserved.

> **T1-B.1b vote-message projection slice — 2026-10-26 (draft branch, pending CI).** PR #32 head `c6586c4d485b8b5814210c05f4490ac494477550` passed all five exact-head CI jobs in [run 40437681391](https://github.com/Albert-bit07/Choreolog/actions/runs/40437681391); Ubuntu 289/289 CTests and API-audit sanitizer 287/287 CTests passed. PR #32 merged as `ca63e589412f18dd3f2e17887ffc44c14fa9bf74` after confirming unchanged main and exact head. The new isolated branch `agent/t1-b-vote-message-projection-20261026` adds outbound v2 diagnostic destination/term/request ID and RequestVote candidate/log-freshness fields, plus explicit model-to-core vote request payload comparison and eight negative mutations. **This branch has not passed hosted CI and has not been merged**; no local C++ build is available (GitHub DNS failure). This is synthetic prefix correspondence only, not real TLC witness replay/refinement. Next: verify exact-head CI and then map real witness UpdateTerm and split term/vote persistence without dropping model states. Historical checkpoints below are retained as dated records.

> **T1-B.1b compiled-prefix follow-up — 2026-10-26 (feature branch, unmerged).** PR #31 head `2012e75422da89e7aef233a3b1d2c0d4ce5dec97` passed all five exact-head jobs in [CI run 40433007973](https://github.com/Albert-bit07/Choreolog/actions/runs/40433007973), including 288/288 Ubuntu CTests and 286/286 sanitizer CTests; it merged as `12d26172930d815eb445ede4d5a44939eb191a8b`. This isolated branch `agent/t1-b-compiled-prefix-check-20261026` adds a CTest-registered comparison of two **synthetic** typed model steps to real compiled C++ replay rows, with twelve negative projection mutations. The branch has not yet passed CI; do not claim real TLC witness refinement or full trace correspondence. Remaining: real witness execution, higher-term split/combined durability relation, message identity and heartbeat comparison. Local git clone still fails DNS; use exact-head hosted CI as the validation gate. Preserve older checkpoint history below.

> **T1-B.1b witness-prefix implementation checkpoint — 2026-10-26 (feature branch, not merged).** PR #30 was merged after five exact-head CI jobs passed (PR head `f4cf59cf959dba62948aede87966ecc7357922bd`, merge `a995fe75187cb6007f6f59fb6681e7df08942751`). This branch `agent/t1-b-witness-prefix-20261026` adds a conservative typed-TLC election prefix normalizer, unit mutations, CTest registration, and scoped documentation. Four Python unit methods passed locally, including eight deliberately invalid synthetic mutations; six of seven archived real TLC witnesses normalized their first two steps, while the offline witness was rejected as unsupported. Archived artifact ZIP SHA-256 `e7dc846907ecb8d2eaa0d5f9485aa2862c0c9fe721ee487312db712d2c5bf1aa`. This is **partial input normalization only**, not C++ replay/refinement. No C++ compilation was possible locally because git clone DNS failed; exact-head hosted CI is required before merge. Next: map UpdateTerm and the split durable term/vote behavior without weakening state comparisons, then compare compiled C++ projections with real witnesses and negative divergences. Historical checkpoints below remain for provenance.

> **CURRENT VERIFIED CHECKPOINT — 2026-10-26 (independent audit).** Live `main` is `72c7a0280dadd0f369f124801035f5a53745eebf`, unchanged from the preceding verified baseline; GitHub compare reports **0 commits and 0 changed files**. The authenticated open-PR listing reports **zero open PRs**. The most recent identified main push CI run [39999355294](https://github.com/Albert-bit07/Choreolog/actions/runs/39999355294) completed successfully on 2026-10-23. This is historical hosted evidence, **not** a new local build or test run. No source defects were established by this audit. T1-B.1b-replay remains TODO: semantic TLC witness-to-C++ mapping, term/vote durability, heartbeat/message identity, and negative divergence tests are not yet validated. The dated checkpoints below are retained for provenance. This audit does not claim to have established whether a separate engineering agent is currently editing an unpublished branch; avoid overlapping code changes until ownership is established.

> **CURRENT VERIFIED CHECKPOINT — 2026-10-23 17:21 UTC (repository audit).** Live `main` is `dcd96bf63b1dd480e95d92d8502e58ca23ae7371` and there are **zero open PRs**. PR #28 was merged on 2026-10-20 at this main SHA; its former head `3e80913ec0d1968107c3d83bdaa3b8f55705f034` is historical. All five exact-main-head CI checks passed in [push run 39509654255](https://github.com/Albert-bit07/Choreolog/actions/runs/39509654255) (Linux, Windows, management API, API audit, formatting). T1-B.1b schedule-driver implementation is integrated and checked; **TLC-to-C++ replay/refinement remains TODO**, as do bounded combined models, default migration and T1-C onward. This audit did not rerun tests locally. Old draft/CI-pending lines below are retained as dated historical checkpoints, not current state. Next engineering action: normalize actual TLC witnesses to schedule inputs with explicit term/vote and heartbeat correspondence, retaining negative divergence tests.


Latest checkpoint: `agent/t1-b-replay-20261020`, based on export PR #27, from main
`f24073b126d41d99761ecf13ffa8221894ea4d1c`. User now authorizes merges after all exact-head checks are green and task
acceptance is verified. PR #25 merged under that authorization as
`c19169ed3538aa3d513c2ef5691b12d90947227e`; no direct main push/deployment authorized.
Follow the complete user charter on PR #25 (`docs/agent-execution-charter.md`,
Sections 0–4) before resuming. Keep code idiomatic, focused and reviewable.

## Environment and repo state
Linux GCC 13.3, CMake 4.4.4/Ninja 1.13.2; format/tidy 18.1.8; pinned vcpkg
6e856794a. Java 17, SHA-verified TLC 1.7.4 available. .NET/Docker absent.
Terminal git fetch/clone works, push has no credentials; GitHub tree/commit/ref
connector saves branches without exposing credentials.
Main initially 2eb118e. PR #24 merged externally as 332b11b, prose only.
Main all-five CI passed in run 39497829946. Recheck live main/PRs before work.

## Task table
| ID | Title | Status | Implemented | Tested | Validated | Verified | Acceptance criteria | Evidence |
|---|---|---|---|---|---|---|---|---|
| T1-A | Baseline/inventory/docs | DONE | yes | yes | yes | yes | Full baseline/source audit/current docs | PR #25 merged c19169e; 279 normal/277 local ASan+UBSan; all exact-head CI green |
| T1-B.1a | Snapshot diagnostics | DONE | yes | yes | yes | yes | Golden/v1 compatibility/negative cases/full gates | PR #26 merged 3265f4a; 285 normal/283 local ASan+UBSan; all synced-head CI/TLC green |
| T1-B.1b-export | Actual TLC behavior export | DONE | yes | yes | yes | yes | Typed complete real witnesses, parser tests, CI artifacts | [Exporter](tlc-behavior-export.md); six tests/seven real logs pass; 286 normal/284 local ASan+UBSan; PR #27 all exact-head CI/TLC green; merged f24073b |
| T1-B.1b-driver | Compiled-core schedule input | DONE (MERGED) | yes | yes | yes (bounded driver) | yes (CI + acceptance) | Actual message delivery and persistence tokens; malformed-input regressions | PR #28 merged dcd96bf; 287 normal/285 selected ASan+UBSan (author report); exact-main-head CI run 39509654255 all five green |
| T1-B.1b-replay | TLC-to-C++ mapping/replay | TODO | no | no | no | no | Normalize inputs, durable completion, message IDs and justified stuttering; negative divergence tests | Export is a prerequisite only |
| T1-B.2 | Combined bounded models | TODO | no | no | no | no | Multi-term replication/loss/dup/restart, Figure 8/snapshots | Existing scoped profiles only |
| T1-B.3 | Default migration | TODO | no | no | no | no | Parity, real failover, >=1000 fault seeds, benchmark gate | Legacy remains default |
| T1-C | Public read consistency/history checker | TODO | partial | no | no | no | Quorum read API, partition/election tests, stale-read mutation | Primitive only from PR #23 |
| T1-D | Benchmarks/profile | TODO | no | no | no | no | Reproduced baseline/profile/optimization decision | Unfinished |
| T1-E | Retention/power-loss/management auth | TODO | no | no | no | no | Specs, formats/fixtures, seeded/crash/security tests | Unfinished |
| T1-F | Metrics/three-process terminal demo | TODO | no | no | no | no | One-command failover, recovery/hash assertions and captured output | Unfinished |
| T2-A/B/C | Dashboard/Temporal/packaging | TODO | no | no | no | no | Tier 1 gate first | Deferred |
| T3 | Research/stretch | TODO | no | no | no | no | Precise proof boundary first | Deferred |

## Decisions and scope
- High risk: correctness evidence parser and CI can misstate what was checked.
- Preserve full TLC states/types and provenance; reject ambiguous/truncated
  syntax rather than invent missing values. No eval, no inferred action mapping.
- This adds export/CI artifacts on integrated main after PRs #25/#26;
  no production runtime changes.
- No model/invariant/bounds, runtime, format or persistence ordering changed.
- Export does not satisfy C++ replay or refinement; snapshots remain unmodeled.

## Blockers and limitations
No roadmap blocker established. Local LSan cannot inspect /proc task threads:
default discovery, verbose retry and minimal ASan program reproduce fatal
ptrace/task errors. Local runs use ASan/UBSan without leaks; CI retains
`detect_leaks=1`. No checks/assertions/seeds/timeouts weakened. See PR #25.

## PR integration queue
- #25: `19478b723a1afc344df50ca439d8583d50ba1d49`, all five exact-head checks
  green in run 39499897103. Merged as c19169ed3538aa3d513c2ef5691b12d90947227e; integrated on main.
- #26 synced head 4c657543cd7a0f3571ab154ccdcb922a1263e098 passed five CI
  jobs (39502431789) and TLC (39502432094). Merged under user authorization
  as 3265f4af76f9f3eae7daf825a24c9c6a31bbd5c1; integrated, diagnostic scope only.
- Export slice: PR #27 head 19cad222cefdb04a9207faafb2824029436c68c1.
  Local gates pass. TLC run 39504905569 passed; artifact 12302079273
  downloaded and verified: all seven behaviors, 15 variables/state. Archive
  SHA256 e7dc846907ecb8d2eaa0d5f9485aa2862c0c9fe721ee487312db712d2c5bf1aa.
  CI run 39504905349 passed all five exact-head jobs; merged f24073b
  under existing authorization. Hosted leak checks passed.
- Conditional merge authorization received: green exact-head checks plus acceptance
  verification. Recheck live main before merge and validate any changed integration.

## Next 3 actions (current as of 2026-10-23)
1. Implement actual TLC witness-to-C++ schedule normalization, explicitly model pending term/vote writes and leader-heartbeat emission, and add negative divergence tests. Do not claim stuttering without a tested abstraction.
2. Extend bounded model profiles for multi-term replication, loss/duplication/restarts, Figure 8 and snapshot boundaries, retaining exact witnesses and CI artifacts.
3. Complete default-engine parity/migration gates and then T1-C public read consistency; maintain separate evidence for runtime TCP/disk recovery and model correspondence.

## Mapping investigation checkpoint
Actual nine-state leader witness versus compiled C++ shows different staged/
durable term+vote granularity; see [paired observations](raft-model-mapping-gaps.md).
No comparison was weakened and no stuttering claim was invented. The next
replay change must explicitly represent/justify this relation (or review a
compound model action), preserving negative durable-vote detection.

## Current integration and scheduling checkpoint
Merged live main 3265f4a into export branch. Resolved CTest append conflict by
keeping both the snapshot contract and exporter tests, including v2 unit cases.
Consolidated this ledger; full charter and prior evidence are now on main.
User's 2026-10-20 conditional merge authorization supersedes the charter's
original per-PR merge prohibition: merge only after exact-head green checks and
verified scope acceptance. No direct main push, deployments or releases.
A two-hour resume-check automation is configured. It skips active work and
resumes from the latest ledger when idle can be confirmed; if activity cannot
be reliably determined it provides a resume reminder instead of duplicating
writers. Do not promise uninterrupted execution beyond a run.

Local export-only baseline (before this integration): 280 normal (34.50s),
278 ASan/UBSan (52.96s), six parser regressions, seven real behaviors and a fresh
20-state application witness. Combined validation passed: 286 normal (34.33s), 284 ASan/UBSan (51.44s),
only the two existing ^CatchUpTest exclusions. Normal Debug used -g0 for disk
space; assertions/strict warnings retained. Benchmarks disabled locally.
See results/t1-b-tlc-export-20261020.md. Raw logs are ignored; summary committed.
Disk-full failures are preserved: sanitizer process recovery hit non-executable
built node/CLI artifacts. After freeing only reproducible build products,
rebuilding and restoring executable mode on those owned ELF binaries, the
single process check and full 278-test sanitizer run passed. No source check or
assertion was removed. Local LSan remains unsupported; CI leak checks retained.


## Replay branch resume details
Current checkout `agent/t1-b-replay-20261020` starts at PR #27's exact head.
The schedule-driven C++ input harness is implemented; model normalization and
projection correspondence are not. Local export merge commit e7d53c0 is
preserved under checkpoint/tlc-export-local-e7d53c0; its tree equals canonical
19cad22 (92583644725d6b2c01e3a752f77e2d1041787f14). No unique code discarded.
Normal build products were removed after validation to free disk. Rebuild with
-g0 and manifest install disabled, reusing build/linux-gcc/vcpkg_installed.
Sanitizer build remains available; ASAN_OPTIONS=detect_leaks=0 is required for
local discovery and tests. Disk currently nearly full; never delete source,
checkpoints or installed dependencies. Full accepted combined log summaries:
results/raw/t1-b-tlc-export-20261020/ctest-combined.log (286,34.33s) and
ctest-sanitizers-final.log (284,51.44s). An earlier overly broad CatchUp regex
selected 283; final rerun used exact ^CatchUpTest exclusion and includes TCP.
Hosted ZIP is in the same ignored raw directory, verified against artifact digest.

A fresh leader witness has generic source locations at line 410 for every
noninitial step: locations alone cannot select protocol actions. Derive and
validate semantic deltas, or introduce reviewed action annotation, before input
normalization. Term/vote write granularity and separate BecomeLeader remain
explicit mapping obligations. No field dropping or fabricated stuttering.


## Schedule driver checkpoint
`--replay-inputs` runs a user-provided schedule against actual C++ nodes. It
captures emitted WireMessage objects, supports delivery/drop/duplicate without
inventing replies, uses actual pending persistence tokens, and emits v2 rows.
Eight checks cover delayed effects, packet identity, halt/restart, quoting and
replication through durable commit/application/compaction; malformed input fails.
See docs/raft-replay-inputs.md. No production runtime changes. TLC action
normalization, full message projection, arbitrary same-bucket reordering,
physical torn storage and snapshot install inputs remain outside this driver.
A pre-rebuild regression caught missing advance/apply/compact commands; after
implementation all eight checks pass. Static analysis first caught a helper-
guarded optional dereference; explicit guard fixed it. Rechecked with exact CI
bugprone/performance options (existing enum-size/signature exclusions), strict
warnings-as-errors: zero reported user-code warnings. Black/clang-format pass.
Normal full suite 287/287 (34.66s); final sanitizer suite 285/285 (52.99s). Earlier driver
suite 285/285 (59.30s) passed before the equivalent optional-guard cleanup.
Normal standalone driver checks rerun after unittest discovery integration fix:
eight pass. Generic Python discovery runs six exporter tests and explicitly skips
runner-dependent class; CTest supplies a runner and executes all eight.


## Historical work-session handoff (superseded by current checkpoint above)
At this older handoff PR #28 was open as a draft: https://github.com/Albert-bit07/Choreolog/pull/28.
Initial published head 615b32506d957b861af42db01824ac83d777ec66 has tree
f5576da4704e413e17f485da3648b868c6e25ca1, identical to locally validated source.
This final checkpoint updates only this ledger; resolve current head from the
PR rather than relying on the initial SHA. Hosted CI must pass on that current
head before merge. No failures may be waived. Local broad gates and static
analysis are complete; their commands/limits/results are in the committed report.
No replay normalization code is partially staged. Next code work is semantic
TLC action identification (generic line-410 locations are insufficient), a
reviewed term/vote and heartbeat correspondence, then actual replay comparisons
with divergence mutations. Never report the schedule driver as refinement.

Workspace checkout retains local equivalent commits; remote API commits have
identical trees but different metadata SHAs. Verify trees before any reset;
preserve unique local commits on checkpoint branches. Terminal push lacks
credentials; use GitHub tree/commit/ref APIs with expected-head leases. No main
push, force push, deployment, release or settings changes authorized.

Automation remains enabled every two hours: skip active work; resume from main,
open PR and branch ledgers only when idle can be established. If active/idle
cannot be reliably determined, leave a reminder rather than run duplicate writers.
The roadmap is not complete: T1-B mapping/model/migration, T1-C–F and later tiers
remain unfinished. Stop/pause recurring checks once the whole roadmap is done.
