# Documentation index

- [Engineering overview](project-overview.md)
- [Single-node process recovery walkthrough](demo.md)

## Build and operate

- [Development environment and commands](development.md)
- [Transition runtime: ownership, rollout and migration](transition-runtime.md)
- [Protocol and local management semantics](protocol.md)
- [Persistence and flush semantics](persistence.md)
- [Peer mTLS and identity boundary](peer-tls.md)
- [Benchmark suites and measurement limits](../cpp-runtime/benchmarks/README.md)

## Correctness and evidence

- [Verification gates and claim boundaries](raft-verification.md)
- [Consensus fields and persist-before-reply inventory](consensus-field-inventory.md)
- [Consensus hardening audit](consensus-hardening-audit.md)
- [Historical storage audit and remaining assumptions](raft-storage-audit.md)
- [Pure transition core](raft-transition-core.md)
- [Trace contract and refinement limits](raft-conformance.md)
- [Bounded TLA+ models](../formal/raft/README.md)
- [Actual TLC behavior export](tlc-behavior-export.md)
- [Observed model/core projection gaps](raft-model-mapping-gaps.md)
- [Quorum read primitive; public integration unfinished](raft-read-barriers.md)

## Storage correspondence and snapshots

- [Isolated checkpoint test adapter](raft-durable-adapter.md)
- [Typed domain/WAL projection](raft-domain-bridge.md)
- [Snapshot-aware recovered projection](raft-recovery-projection.md)
- [Fail-stop FileEngine writer](raft-file-engine-adapter.md)
- [Snapshot offsets](raft-snapshot-offsets.md)
- [Persistence-gated local compaction](raft-local-compaction.md)

## Roadmap and session continuity

- [Project plan](../PLAN.md)
- [Tasks](../TASKS.md)
- [Agent progress and next actions](AGENT_STATE.md)
- [Master execution charter and full prioritized roadmap](agent-execution-charter.md)

Tests, reproduced process behavior and formal properties are separate evidence.
A green seed is evidence, not a proof. The current runtime is Raft-like;
bounded model results do not establish C++ refinement.
