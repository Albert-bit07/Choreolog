# ChoreoOS — Implementation Checklist

This checklist implements `PLAN.md` in correctness-first order. Finish each exit criterion before beginning the next milestone. Record randomized seeds and retain raw benchmark data.

## Milestone 0 — Replace the obsolete scaffold

### Step 1: Remove superseded implementation files

- [x] Remove the Go module, Go source, and Go-specific CI.
- [x] Remove obsolete Go-specific configuration and developer commands.
- [x] Preserve the Temporal Server setup for later refactoring into an independent orchestration stack.
- [x] Remove any implication that Temporal or PostgreSQL stores authoritative ChoreoOS state.
- [x] Rename remaining Choreo Log references to ChoreoOS.
- [x] Preserve Git history and the new planning documents.

Done when no build or documentation path implies that Go, Temporal, or PostgreSQL implements the ChoreoOS core, while Temporal remains documented as a separate future orchestration layer.

Completed: Go sources are gone. Temporal Compose and scripts now live under `temporal/` and persist workflow history only.

### Step 2: Verify development prerequisites

- [x] Install a C++20-capable compiler.
- [x] Install CMake and Ninja.
- [x] Install or bootstrap vcpkg.
- [ ] Install a supported modern .NET SDK.
- [x] Verify Python 3 and virtual-environment support.
- [ ] Install Docker Desktop or another supported container runtime before the Temporal milestone.
- [x] Record exact tool versions in `docs/development.md`.

Required checks:

```powershell
cmake --version
ninja --version
cl
dotnet --info
python --version
```

Docker Desktop is not required for the initial C++ runtime, but a container runtime is required for the planned local Temporal Server.

### Step 3: Create repository skeleton

- [x] Add root `CMakeLists.txt`.
- [x] Add `CMakePresets.json`.
- [x] Add `vcpkg.json`.
- [x] Add `cpp-runtime/include/choreoos`.
- [x] Add `cpp-runtime/src`.
- [x] Add `cpp-runtime/apps/node`, `cli`, and `replay`.
- [x] Add C++ test, benchmark, and simulator directories.
- [x] Add `proto/internal` and `proto/management`.
- [x] Add .NET dashboard, client, workflow, worker, and test project directories.
- [x] Add `temporal`, `python-tools`, `configs`, and `results`.
- [x] Update `.gitignore` for CMake, vcpkg, .NET, Python, node data, and benchmark artifacts.

### Step 4: Establish automated quality checks

- [x] Enable strict compiler warnings.
- [x] Treat warnings as errors in project code.
- [x] Add `clang-format`.
- [x] Add `clang-tidy` with an initially focused rule set.
- [x] Add GoogleTest through vcpkg.
- [x] Add sanitizer presets where supported.
- [x] Add CI for Windows and Linux builds.
- [x] Run unit tests through CTest.

Done when a trivial C++ library and test build cleanly from a fresh checkout.

## Milestone 1 — Deterministic single-node state machine

### Step 5: Define deterministic value types

- [x] Define strongly typed node, choreography, dancer, command, and event IDs.
- [x] Define `LogIndex`, `Term`, and `MusicalTick`.
- [x] Define fixed-point `StageCoordinate`.
- [x] Define `Position` and `StageBounds`.
- [x] Avoid authoritative floating-point fields.
- [x] Add checked construction and explicit validation errors.

Tests:

- [x] Integer overflow and range limits.
- [x] Stage-boundary inclusivity.
- [x] ID equality, ordering, and serialization.
- [x] Musical tick conversion.

### Step 6: Define command and event models

- [x] Add versioned command envelopes.
- [x] Add versioned event envelopes.
- [x] Add create choreography.
- [x] Add/remove dancer.
- [x] Move dancer.
- [x] Define/change formation.
- [x] Trigger music cue.
- [x] Trigger lighting cue.
- [x] Separate authoritative logical ordering from observation timestamps.

### Step 7: Implement authoritative state and invariants

- [x] Define immutable-input/mutable-state application boundaries.
- [x] Enforce valid stage dimensions.
- [x] Enforce dancer lifecycle.
- [x] Enforce stage bounds.
- [x] Enforce configured overlap policy.
- [x] Enforce formation membership.
- [x] Enforce cue dependencies and ordering.
- [x] Reject unsupported event versions and types.
- [x] Return structured errors without partial mutation.

### Step 8: Implement command handling

- [x] Validate commands against committed state.
- [x] Produce events without applying them.
- [x] Deduplicate command IDs.
- [x] Guarantee invalid commands emit no events.
- [x] Keep I/O, time, and randomness outside command logic.

### Step 9: Implement deterministic event application

- [x] Apply committed events in contiguous index order.
- [x] Track the last-applied index.
- [x] Make duplicate event delivery a no-op or explicit prior result.
- [x] Reject gaps and conflicting event IDs.
- [x] Serialize authoritative state canonically.
- [x] Compute a deterministic state hash.

Core properties:

- [x] Two state machines replaying the same events produce identical bytes and hash.
- [x] Replaying the same committed event twice does not mutate state twice.
- [x] Invalid sequences fail at the same index on every instance.

### Step 10: Build the single-node CLI

- [x] Create choreography.
- [x] Add/remove/move dancers.
- [x] Define/change formations.
- [x] Trigger cues.
- [x] Show current state.
- [x] Show ordered events.
- [x] Replay through a target index or musical tick.
- [x] Validate state and print its hash.
- [x] Support human-readable and JSON output.

Exit criterion: two independent CLI replay runs over the same event fixture produce identical state and hash.

Verified: two `choreoos-replay` runs over the same store both produced hash `b4475e4686e8e6ce`.

## Milestone 2 — Durable log and snapshots

### Step 11: Specify disk formats

- [x] Document log-record framing.
- [x] Document snapshot framing.
- [x] Select and document checksums.
- [x] Define format and schema version behavior.
- [x] Define maximum record sizes.
- [x] Define flush/durability modes.
- [x] Create golden compatibility fixtures.

### Step 12: Implement append-only event log

- [x] Append framed versioned records.
- [x] Validate checksums while reading.
- [x] Detect incomplete tail records.
- [x] Reject corrupt middle records.
- [x] Read by index.
- [x] Scan contiguous ranges.
- [x] Flush at the documented durability boundary.
- [x] Support safe uncommitted suffix truncation.
- [x] Expose instrumentation for append and flush duration.

Tests:

- [x] Clean reopen.
- [x] Restart after every possible partial-record length.
- [x] Checksum failure.
- [x] Oversized and malformed record rejection.
- [x] Concurrent caller behavior at the public boundary.

### Step 13: Implement persistent metadata

- [x] Persist current term.
- [x] Persist voted-for node.
- [x] Persist known commit index safely.
- [x] Use checksummed versioned data.
- [x] Write through temporary file and atomic replacement.
- [x] Flush before acknowledging dependent protocol actions.

### Step 14: Implement snapshots

- [x] Serialize state canonically.
- [x] Store last-included index and term.
- [x] Store state hash and payload checksum.
- [x] Write, flush, and atomically replace.
- [x] Ignore incomplete temporary snapshots.
- [x] Select the newest valid compatible snapshot.
- [x] Trigger snapshots at a configurable threshold.

### Step 15: Implement recovery and replay tool

- [x] Load persistent metadata.
- [x] Restore latest valid snapshot.
- [x] Validate state hash.
- [x] Scan and validate the log.
- [x] Replay committed post-snapshot entries.
- [x] Re-run invariants.
- [x] Print a recovery report.
- [x] Add standalone log inspection and replay commands.

Exit criterion: recovery from snapshot plus log produces the same state hash as full replay across crash/truncation tests.

Verified: `DurabilityTest.SnapshotRecoveryMatchesFullReplay` and `DurabilityTest.GoldenV1FixtureRecoversKnownHash`. The golden store recovers hash `dd22fb677cc125bf`. All 26 unit tests passed.

## Milestone 3 — Protocol and fixed-leader replication

### Step 16: Define Protocol Buffer schemas

- [x] Add handshake and protocol-version messages.
- [x] Add `AppendEntries` and response.
- [x] Add vote messages for the next milestone.
- [x] Add snapshot installation messages.
- [x] Add structured protocol errors.
- [ ] Generate C++ types through CMake. The schema is `proto/internal/choreoos.proto`. The runtime writes that protobuf wire format directly. `protoc` was not generated here because building Abseil ran out of disk space.
- [x] Add serialization compatibility tests.

### Step 17: Implement framed TCP transport

- [x] Use Boost.Asio asynchronous TCP.
- [x] Implement bounded length-prefixed framing.
- [x] Handle partial reads and writes.
- [x] Add connection lifecycle and reconnection.
- [x] Add correlation IDs.
- [x] Apply backpressure and bounded queues.
- [x] Reject malformed, oversized, and incompatible frames.
- [x] Add transport metrics.

### Step 18: Implement deterministic network simulator

- [x] Deliver typed messages without real sockets.
- [x] Control logical time explicitly.
- [x] Seed all randomized scheduling.
- [x] Delay, drop, duplicate, and reorder messages.
- [x] Partition node sets.
- [x] Save failing schedules and seeds.

Consensus logic must run against both simulated and real transports through one narrow interface.

### Step 19: Implement fixed-leader replication

- [x] Configure one leader and two followers.
- [x] Validate previous log index and term.
- [x] Track `next_index` and `match_index`.
- [x] Retry rejected append requests safely.
- [x] Repair conflicting uncommitted suffixes.
- [x] Advance commit index only after majority replication.
- [x] Apply entries only through commit index.
- [x] Propagate leader commit index to followers.
- [x] Deduplicate repeated append messages.

### Step 20: Add client command semantics

- [x] Followers redirect or reject commands with leader information.
- [x] Leader deduplicates command IDs.
- [x] Serialize initial proposals for simple correctness.
- [x] Return success only after commit and local apply.
- [x] Return prior result for a retried committed command.
- [x] Return unavailable when quorum cannot be reached.

### Step 21: Run a three-process cluster

- [x] Add node configuration files.
- [x] Assign unique IDs, ports, and data directories.
- [x] Add cluster start/stop scripts.
- [x] Submit commands through the CLI.
- [x] Query every node's term/index/hash.
- [x] Demonstrate equal state hashes.
- [x] Stop one follower and preserve quorum progress.
- [x] Restart and catch up that follower.

Exit criterion: fixed-leader replication survives one follower loss without losing committed entries. Do not call this consensus yet.

Verified: `ReplicationTest.OneFollowerLossStillCommitsThenCatchesUp` and `ClusterTest.ThreeTcpNodesCommitAndCatchUp`. All 37 unit tests passed. This is fixed-leader replication, not consensus. Vote messages are rejected. Snapshot installation is defined and not used for catch-up yet.

## Milestone 4 — Leader election and recovery

### Step 22: Implement consensus roles and terms

- [ ] Add follower, candidate, and leader roles.
- [ ] Persist term transitions before responses.
- [ ] Step down on any higher term.
- [ ] Ignore stale-term messages safely.
- [ ] Publish role and term metrics.

### Step 23: Implement voting

- [ ] Add randomized election timeouts.
- [ ] Use injectable deterministic randomness in tests.
- [ ] Persist one vote per term.
- [ ] Enforce up-to-date-log voting rule.
- [ ] Require majority to become leader.
- [ ] Reset election timers only under valid conditions.

### Step 24: Implement heartbeats and leader initialization

- [ ] Send empty AppendEntries as heartbeat.
- [ ] Initialize follower replication indexes.
- [ ] Establish leadership with a current-term no-op entry if required.
- [ ] Prevent stale leaders from committing.
- [ ] Commit prior-term entries only under documented safe rules.

### Step 25: Test election safety

- [ ] One leader maximum per term in simulated scenarios.
- [ ] Minority partitions cannot elect or commit.
- [ ] Split votes retry with new randomized timeouts.
- [ ] Old leaders step down after healing.
- [ ] Conflicting uncommitted entries are repaired.
- [ ] Committed entries are never overwritten.

### Step 26: Implement restart and catch-up

- [ ] Restart as follower from persisted term/vote/log/snapshot.
- [ ] Catch up incrementally through AppendEntries.
- [ ] Install snapshots for compacted history.
- [ ] Verify snapshot chunks and final checksum.
- [ ] Resume log replication after snapshot index.
- [ ] Rejoin an old leader safely as follower.

### Step 27: Run process-level failover tests

- [ ] Kill the current leader.
- [ ] Observe a real majority election.
- [ ] Commit through the new leader.
- [ ] Restart the old leader.
- [ ] Verify all state hashes converge.
- [ ] Test each two-versus-one partition arrangement.
- [ ] Measure detection, election, and resumed-commit time.

Exit criterion: documented election and log-safety properties pass simulator and process tests. Describe the system as “Raft-like” until a compliance review justifies stronger wording.

## Milestone 5 — Fault injection and randomized correctness

### Step 28: Build explicit failure controls

- [x] Delay messages by type, source, and destination.
- [x] Drop and duplicate selected messages.
- [x] Disconnect peers.
- [x] Pause, terminate, and restart nodes.
- [x] Create and heal partitions.
- [x] Inject log and snapshot test corruption.
- [x] Inject storage failures around flush boundaries.
- [x] Require explicit test mode.

### Step 29: Build seeded workload generation

- [x] Generate valid command streams.
- [x] Generate invalid commands.
- [x] Generate concurrent client retries.
- [x] Generate network and node failures.
- [x] Record seed, configuration, and message schedule.
- [x] Save minimized or reduced failing scenarios.

### Step 30: Assert system properties

- [x] Deterministic replay.
- [x] Replica convergence.
- [x] Idempotent command and message retries.
- [x] Invariant preservation.
- [x] Snapshot/full-replay equivalence.
- [x] No lost committed events under documented durability.
- [x] No conflicting committed entry at one index.
- [x] No minority commits.
- [x] Eventual catch-up after faults heal.

Exit criterion: seeded fault injection and property tests pass. A passing seed is evidence, not a proof over every schedule. Storage faults stay off unless test mode is set. Describe elections as Raft-like.

Verified: `StorageFaultTest`, `FaultTest`, and `PropertyTest` (seeds 1–8 for replay, seeds 2, 5, and 11 for a dropped follower that heals). A corrupt log is rejected. A corrupt snapshot is skipped and the write-ahead log is replayed.

## Milestone 6 — Benchmarking and performance engineering

### Step 31: Create benchmark harnesses

- [ ] Single-node apply throughput.
- [ ] Serialization and hashing.
- [ ] Log append and flush.
- [ ] Full replay.
- [ ] Snapshot creation and restore.
- [ ] One-node and three-node command throughput.
- [ ] Command-to-commit latency.
- [ ] Catch-up and failover recovery.

### Step 32: Produce trustworthy measurements

- [ ] Capture p50, p95, and p99.
- [ ] Record warmup and sample counts.
- [ ] Record CPU, memory, OS, compiler, build mode, and commit.
- [ ] Save raw JSON/CSV results.
- [ ] Use fixed workloads and seeds for comparisons.
- [ ] Add Python aggregation and plotting.
- [ ] Never commit fabricated result data.

### Step 33: Profile before optimizing

- [ ] Capture CPU profiles.
- [ ] Measure allocations.
- [ ] Inspect lock contention.
- [ ] Inspect network and storage timing.
- [ ] Identify a measured bottleneck.
- [ ] Make one controlled optimization.
- [ ] Re-run correctness tests.
- [ ] Report before/after data and trade-offs.

Candidates only after profiling:

- [ ] Batching or group commit
- [ ] Reduced serialization copies
- [ ] Buffer reuse
- [ ] Reduced lock scope
- [ ] More efficient state indexes

## Milestone 7 — Management API and C# dashboard

### Step 34: Define the management API

- [ ] Submit command.
- [ ] Accept stable operation/command IDs and return prior committed results for retries.
- [ ] Query authoritative choreography state.
- [ ] Query node role, term, indexes, and health.
- [ ] Stream or page recent events.
- [ ] Query runtime metrics.
- [ ] Query snapshot and recovery status.
- [ ] Apply explicit test-mode failure controls.
- [ ] Generate C++ server and C# client types.

### Step 35: Scaffold Avalonia application

- [ ] Create solution and application.
- [ ] Separate models, generated transport, services, view models, and views.
- [ ] Use dependency injection where it clarifies boundaries.
- [ ] Use `async`/`await` and cancellation tokens.
- [ ] Keep consensus and state-machine logic out of C#.

### Step 36: Build dashboard views

- [ ] Cluster topology.
- [ ] Node roles, health, terms, and indexes.
- [ ] Leader transition timeline.
- [ ] Recent committed events.
- [ ] Two-dimensional stage.
- [ ] Snapshot/recovery status.
- [ ] Throughput and latency charts.
- [ ] Controlled failure actions.

### Step 37: Test the control plane

- [ ] Test API client error handling.
- [ ] Test reconnect and cancellation.
- [ ] Test view-model state transitions.
- [ ] Verify UI state comes from real C++ responses.
- [ ] Demonstrate leader kill and recovery visibly.

## Milestone 8 — Temporal durable orchestration

Temporal starts only after the C++ management API and typed C# client are stable enough for retry-safe Activities.

### Step 38: Provision an independent Temporal stack

- [ ] Move/refactor Temporal Compose files under `temporal/`.
- [ ] Run Temporal Server with its own persistence.
- [ ] Pin and document image versions.
- [ ] Add health checks and namespace initialization.
- [ ] Add start, stop, reset, and health commands.
- [ ] Confirm resetting Temporal does not delete ChoreoOS node data.
- [ ] Confirm resetting ChoreoOS node data does not delete Temporal history.

### Step 39: Scaffold the C# Temporal layer

- [ ] Add the Temporal .NET SDK.
- [ ] Create `ChoreoOS.Workflows`.
- [ ] Create a separately executable `ChoreoOS.TemporalWorker`.
- [ ] Register workflows and activities explicitly.
- [ ] Add graceful worker shutdown.
- [ ] Configure task queues, retry policies, and timeouts.
- [ ] Keep workflow code deterministic and free of direct I/O.

### Step 40: Implement retry-safe ChoreoOS Activities

- [ ] Query choreography version and state.
- [ ] Query cluster leader, health, term, and commit index.
- [ ] Validate cluster readiness.
- [ ] Submit commands with stable operation IDs.
- [ ] Request a snapshot.
- [ ] Collect runtime and benchmark metrics.
- [ ] Classify retryable and permanent errors.
- [ ] Return explicit committed outcomes rather than assuming submission succeeded.

Idempotency requirements:

- [ ] Derive each mutating operation ID from workflow and logical step identity.
- [ ] Reuse the same ID for every Activity retry.
- [ ] Verify ChoreoOS returns the prior result for a duplicate committed command.
- [ ] Reject an operation ID reused with conflicting request data.

### Step 41: Implement initial workflows

- [ ] `RehearsalWorkflow`
- [ ] `PerformanceWorkflow`
- [ ] `ChoreographyDeploymentWorkflow`
- [ ] Add `RecoveryWorkflow` only for high-level coordination.
- [ ] Use durable timers for long waits and scheduled phases.
- [ ] Use Signals for readiness, completion, cancellation, and operator input.
- [ ] Use Queries for non-mutating workflow status.
- [ ] Produce correlated workflow and ChoreoOS operation IDs.

`RehearsalWorkflow` exit scenario:

1. Load a specific choreography version.
2. Validate stage and cluster readiness.
3. Wait for participant readiness Signals.
4. Execute timed rehearsal phases.
5. Submit idempotent ChoreoOS commands.
6. Wait for committed outcomes.
7. Collect metrics.
8. Generate a final report.

### Step 42: Integrate workflow state into the dashboard

- [ ] List workflow ID, type, run ID, and status.
- [ ] Show current logical step and pending Activity.
- [ ] Show retry attempts, timers, and cancellation state.
- [ ] Correlate workflow with ChoreoOS cluster and command IDs.
- [ ] Show current leader and commit index beside workflow state.
- [ ] Visually distinguish Temporal state from authoritative ChoreoOS state.
- [ ] Start, signal, query, cancel, and terminate workflows through explicit controls.

### Step 43: Test independent and combined failures

- [ ] Retry an Activity after an ambiguous response without duplicating an event.
- [ ] Deliver a duplicate Activity attempt and return the prior result.
- [ ] Kill the ChoreoOS leader during a workflow.
- [ ] Recover a ChoreoOS node during a workflow.
- [ ] Kill and restart the Temporal Worker while ChoreoOS remains healthy.
- [ ] Start a replacement worker and resume workflow progress.
- [ ] Stop Temporal Server and verify ChoreoOS remains available.
- [ ] Remove ChoreoOS quorum and verify the workflow waits/retries safely.
- [ ] Heal infrastructure and verify successful continuation.
- [ ] Test workflow cancellation and timeout.
- [ ] Measure node recovery separately from worker recovery.

Exit criterion: demonstrations clearly identify whether recovery came from C++ replication/consensus or Temporal workflow history.

## Milestone 9 — Documentation and final demonstrations

### Step 44: Document implemented guarantees

- [ ] Architecture and ownership boundaries.
- [ ] ChoreoOS versus Temporal guarantee boundary.
- [ ] State-machine determinism.
- [ ] Event and protocol formats.
- [ ] Persistence and flush semantics.
- [ ] Consensus algorithm and deviations from Raft.
- [ ] Failure model.
- [ ] Recovery procedure.
- [ ] Testing strategy and property limits.
- [ ] Benchmark methodology.
- [ ] Known limitations.

### Step 45: Create reproducible demonstrations

- [ ] Deterministic replay.
- [ ] Invalid invariant rejection.
- [ ] Crash and snapshot recovery.
- [ ] Three-node replication.
- [ ] Follower catch-up.
- [ ] Leader failure and election.
- [ ] Partition and quorum behavior.
- [ ] Seeded randomized failure run.
- [ ] Benchmark and profile comparison.
- [ ] Avalonia dashboard walkthrough.
- [ ] Temporal Worker kill and workflow resumption.
- [ ] ChoreoOS leader kill during an active Temporal workflow.
- [ ] Combined dashboard view of workflow and cluster recovery.

### Step 46: Final validation

- [ ] Build from a clean Windows checkout.
- [ ] Build from a clean Linux checkout.
- [ ] Run unit, property, integration, and process tests.
- [ ] Run Temporal workflow and cross-layer integration tests.
- [ ] Run supported sanitizers.
- [ ] Reproduce final benchmark report.
- [ ] Verify every public claim against a test or measurement.

## Deferred work

- Dynamic membership and joint consensus
- Pre-vote and leadership transfer
- Linearizable read optimization
- WAN/multi-region behavior
- Transport alternatives
- Audio synchronization
- Rich choreography editing
- Production authentication and authorization

## Start here

Milestones 0–3 are on main. Milestone 3 is fixed-leader replication, not consensus. Milestone 4 election code is on main and is Raft-like, not production Raft; a few Step 27 boxes (every two-versus-one process partition, multi-chunk snapshots) are still open. Milestone 5 seeded fault injection and property tests now pass locally. A green seed is evidence, not a proof.

The next implementation session should begin Milestone 6 at Step 31: benchmark harnesses. Do not start the dashboard or Temporal workflows until the C++ management API and C# client can return retry-safe committed results.
