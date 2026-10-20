# Choreo Log — engineering overview

**Author:** Albert Mejooli · **Scope:** independently built C++20 distributed systems project, with choreography events as the deterministic workload.

## Problem and design

A distributed service cannot safely equate “written somewhere” with “committed and visible everywhere.” Choreo Log experiments with recovering a consistent event-derived state when nodes crash, lag, lose connections or hold divergent *uncommitted* tails. It is not a database, a message broker, or a complete production Raft distribution.

The shipped node selects between a legacy replica (default) and an integrated transition core (`consensus_engine: transition`). The latter separates a pure `raft::step` transition from network/storage effects. The host executes persistent changes, verifies the resulting FileEngine state and confirms application before dependent acknowledgments are released. Storage uses WAL records, metadata, snapshots and recovery transactions. Optional peer mTLS binds peer identity to certificates; default plaintext is not authenticated against spoofing.

## Hard decisions and tradeoffs

1. **Persisted ≠ committed ≠ applied.** WAL durability survives restart, but only matching majority/term evidence authorizes commitment; applying uncommitted tails would expose events later eligible for rollback. See [protocol](protocol.md) and [runtime mapping](transition-runtime.md).
2. **Preserve committed prefixes while reconciling.** The follower may remove a conflicting *uncommitted* suffix; retained committed state must not be replaced. Snapshot installation is verified before publication, with crash recovery handled by a redo transaction. See [persistence](persistence.md).
3. **Bound memory and messages.** Catch-up uses bounded entry batches and snapshot chunks, and networking applies session and queue budgets. This trades unlimited throughput/liveness for predictable resource pressure and fail-closed behavior. See [protocol limits](protocol.md#status-state-pages-and-capacity).
4. **Select, don't silently replace.** The new transition backend is opt-in; existing legacy stores/clients remain in scope. Neither backend establishes a model-to-production proof.

## Failure-and-recovery example

Suppose three nodes agree that entries through index 40 are committed, while follower C also retains an entry at 41 in its WAL that was **never committed**. C crashes. A valid leader later commits a different entry at 41 with a majority. On restart, C must recover durable metadata, establish the leader's matching prefix, and replace only its conflicting uncommitted suffix; it must **not** apply its old WAL entry 41 as authoritative state. After replay/catch-up, state hashes should agree for the same committed applied prefix.

This is an *illustrative index example*, not a captured trace. The matching invariants are exercised in [cluster integration tests](../cpp-runtime/tests/integration/cluster_test.cpp), [transition cluster tests](../cpp-runtime/tests/integration/transition_cluster_test.cpp), and implemented in [replication](../cpp-runtime/src/replication/replica.cpp), [transition step](../cpp-runtime/src/raft/transition.cpp), and [durable host](../cpp-runtime/src/raft/runtime_host.cpp). **Does not establish:** safe behavior for all partitions, arbitrary crash sequences, Byzantine actors or a mechanically proved Raft refinement.

## Evidence and verification

The project contains GoogleTests for state, recovery and distributed schedules, plus real TCP process integration and bounded TLA+ model-checking profiles. The latter are scoped checks, not proof that the C++ runtime refines the model. Use `cmake --preset linux-gcc`, `cmake --build --preset linux-gcc`, and `ctest --preset linux-gcc --output-on-failure` with the vcpkg toolchain available. For a real-process crash/receipt test, use [the guided demo](demo.md).

## Performance evidence

The previously reported **45.5 ms to 17.1 ms** median commit latency corresponds to **62.4% less latency**; before/after raw samples, machine details, disk policy and precise commits were not recovered during this documentation review, and the result was not independently reproduced. The checked `bench_cluster.cpp` commit benchmark operates on a simulated network and reports logical ticks, not wall-clock TCP latency. [run_benchmarks.sh](../scripts/run_benchmarks.sh) stores current benchmark JSON and host metadata but is not itself a before/after experiment. Keep the historical number qualified until an instrumented real-node, identical-workload A/B study checks commit receipts and hashes, archives raw output and reports sample counts, medians and distribution.

## Next engineering work

Integrate bounded, partition-tested public quorum reads; strengthen snapshot-aware model-to-code correspondence; explicitly test more combined faults, resource exhaustion and crash windows; document receipt retention, recovery operations and certificate lifecycle. The project provides concrete, inspectable engineering evidence, **not** unrestricted production guarantees.
