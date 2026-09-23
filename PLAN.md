# ChoreoOS — Project Plan

## 1. Mission

ChoreoOS is an educational distributed runtime for live-performance systems. Dance is the concrete domain; the engineering focus is deterministic state machines, event sourcing, durable storage, replication, consensus, concurrency, networking, recovery, correctness testing, and measured performance.

ChoreoOS is not an operating-system kernel, dance-management CRUD application, trading system, financial exchange, or production-grade consensus product. Claims must match implemented and tested guarantees.

The governing rule is:

> C++ owns authoritative state and distributed behavior. C# observes and controls it and hosts durable workflow workers. Temporal orchestrates long-running operations above the runtime. Python measures and analyzes it.

## 2. Language and component ownership

### C++20+ runtime

C++ owns all authoritative and performance-critical behavior:

- Command validation and event creation
- Deterministic choreography state machine
- Domain invariants
- Append-only event log
- Snapshot creation and recovery
- Node-to-node networking
- Replication and commit tracking
- Leader election and heartbeats
- Failure injection
- Runtime metrics
- CLI and management API

Use modern C++ deliberately: RAII, value types, smart pointers, `std::thread`, mutexes, condition variables, atomics, futures/promises where useful, `std::span`, `std::chrono`, and explicit ownership. Avoid abstraction that hides ordering, durability, or synchronization behavior.

### C#/.NET control plane

C# with Avalonia UI owns:

- Cluster topology and health dashboard
- Leader/follower and term visualization
- Log, commit, snapshot, and recovery status
- Event timeline
- Two-dimensional stage visualization
- Command submission
- Controlled failure-injection actions
- Metrics and benchmark presentation
- Temporal workflow definitions, workers, clients, and activity implementations
- Combined visualization of cluster state and workflow state

The dashboard never implements consensus or derives authoritative state independently. It uses asynchronous API clients and renders state reported by the C++ runtime.

Run the Temporal Worker as a separately terminable .NET process even though it belongs to the C# control-plane solution. This makes worker-crash recovery independently testable and prevents the desktop UI process from becoming a workflow availability requirement.

### Temporal orchestration

Temporal owns durable execution for high-level, long-running processes:

- Workflow history and deterministic workflow state
- Durable timers and waits
- Activity scheduling and retry policy
- Signals, queries, cancellation, and workflow timeout
- Recovery after a C# Temporal Worker process terminates

Temporal does not own choreography truth, log order, replication, quorum, commit semantics, leader election, snapshots, replay, or node recovery. A Temporal workflow interacts with ChoreoOS as an external client through the same explicit management API used by other clients.

### Python tooling

Python owns:

- Experiment orchestration
- Workload generation at the process level
- Benchmark aggregation
- Statistical analysis
- Plotting and report generation

Python does not implement runtime, consensus, or dashboard logic.

## 3. Initial technology stack

- C++20 or newer
- CMake with CMake Presets
- vcpkg manifest mode for third-party dependencies
- Boost.Asio for asynchronous TCP networking
- Protocol Buffers for explicit, versioned messages
- gRPC for the C++ management/client API consumed by C#
- GoogleTest for C++ unit and integration tests
- Google Benchmark for microbenchmarks
- Avalonia UI on a supported modern .NET release
- Temporal .NET SDK and a C# Temporal Worker
- Temporal Server with its own persistence
- xUnit for .NET service/view-model tests
- Python with pandas, NumPy, and Matplotlib for analysis

Internal node traffic uses length-prefixed Protocol Buffer messages over Boost.Asio TCP. The management API may use gRPC, but node-to-node consensus remains an explicit C++ transport rather than hidden behind gRPC.

Dependency choices must be benchmarked when they enter hot paths. Unsafe raw dumps of C++ object memory are prohibited for network and disk formats.

## 4. Top-level architecture

```text
                  C# / Avalonia dashboard and control plane
            cluster | stage | workflows | metrics | fault controls
                         |                         |
                gRPC management API        Temporal client API
                         |                         |
          +-----------------------+-----------------------+
          |                       |                       |
     C++ Node A              C++ Node B              C++ Node C
     leader/follower         leader/follower         leader/follower
          |                       |                       |
          +------ versioned Boost.Asio TCP protocol -----+
          |                       |                       |
      local WAL               local WAL               local WAL
      snapshots               snapshots               snapshots

                         C# Temporal Worker
                    workflows | activities | retries
                                  |
                           Temporal Server
                    workflow history | timers | tasks

                   metrics and experiment artifacts
                                  |
                          Python analysis tools
```

Each node is a separate process with its own:

- Node ID
- TCP peer port
- Management API port
- Static peer configuration
- Data directory
- Write-ahead event log
- Snapshot files
- Persistent consensus metadata
- Runtime role and metrics

The first cluster target is three nodes with static membership. Dynamic membership and joint consensus are not MVP features.

ChoreoOS node data and Temporal persistence are separate. Losing or restarting one layer must not silently reset the other.

## 5. Node architecture

```text
Client/management endpoint
           |
Command coordinator
           |
Consensus / replication state
           |
Durable append-only log
           |
Commit index
           |
Deterministic state machine
           |
Snapshot manager

Peer transport <--> protocol codec <--> consensus mailbox/event loop
Failure injector wraps peer transport and process orchestration boundaries
```

Subsystem boundaries:

- `state`: commands, events, state, invariants, deterministic apply
- `log`: log entries, append/read/truncate, checksums
- `storage`: files, durable metadata, snapshots, atomic replacement
- `protocol`: schemas, framing, version negotiation, error handling
- `network`: asynchronous connections and message delivery
- `consensus`: terms, roles, voting, replication, commit advancement
- `runtime`: lifecycle, scheduling, orchestration, configuration
- `api`: command/query/metrics/failure-control endpoints
- `simulator`: deterministic transport and fault scenarios for tests

Dependencies point inward toward pure domain/state types. The state machine must not depend on networking, consensus, storage, UI, or wall-clock time.

## 6. Deterministic choreography model

### Time and coordinates

Use integer representations:

- Musical time is an integer tick count.
- Stage coordinates use fixed-point integer units such as millimeters.
- Tempo and wall-clock timestamps are metadata unless explicitly represented by committed events.

Avoid floating-point values in authoritative state transitions so replay remains bit-for-bit stable across nodes and platforms.

### Commands

Initial commands:

- Create choreography and stage
- Add dancer
- Remove dancer
- Move dancer
- Define or change formation
- Trigger music cue
- Trigger lighting cue

Every client command includes:

- Command ID for deduplication
- Choreography/stream ID
- Expected logical context where required
- Explicit command type and versioned payload

Commands are validated only by the leader against committed authoritative state. A valid command produces one or more events; an invalid command produces none.

### Event envelope

Every committed event contains:

- Event ID
- Command ID / causation ID
- Choreography ID
- Event type
- Schema version
- Structured payload
- Musical tick
- Log term
- Log index
- Optional non-authoritative observation timestamp

Initial event types:

- `CHOREOGRAPHY_CREATED`
- `DANCER_ADDED`
- `DANCER_REMOVED`
- `DANCER_MOVED`
- `FORMATION_DEFINED`
- `FORMATION_CHANGED`
- `MUSIC_CUE_TRIGGERED`
- `LIGHTING_CUE_TRIGGERED`

### State-machine invariants

- Stage dimensions are valid.
- Active dancers exist before being referenced.
- Positions remain inside stage bounds.
- Overlap policy is explicit and enforced.
- Formation membership references active dancers.
- Musical and lighting cues satisfy declared ordering/dependency rules.
- Event IDs and command IDs are not applied twice.
- Applied log indexes are contiguous.
- An event is applied only after commitment.

Replay, replication, snapshot restoration, and normal command execution all use the same event-application function and invariant checks.

### Deterministic apply contract

Given identical initial state and identical ordered committed events, every healthy node must produce:

- Identical serialized authoritative state
- Identical last-applied index
- Identical deterministic state hash

I/O, random generation, system time, and thread scheduling cannot affect event application.

## 7. Single-node event lifecycle

Before distribution, implement and test:

```text
command
  -> validate against committed state
  -> construct event
  -> serialize and append durable log record
  -> mark locally committed
  -> apply exactly once
  -> expose updated state
```

The CLI must support command submission, state queries, event inspection, replay, snapshot creation, and validation without the dashboard.

## 8. Durable log and snapshot design

### Event log

Use a framed append-only file format:

```text
magic | format version | record length | term | index | payload | checksum
```

Requirements:

- Detect truncated and corrupt tail records.
- Reject invalid lengths and unsupported versions.
- Preserve ordering and contiguous indexes.
- Define configurable flush policy.
- Use `fsync`/`FlushFileBuffers` at the durability boundary.
- Never report a durability guarantee stronger than the configured flush mode.
- Support safe suffix truncation for conflict repair.

Start with correctness-oriented synchronous durability, then benchmark batching and group commit.

### Persistent consensus metadata

Persist term and vote before sending responses that depend on them. Use a checksummed, versioned metadata record with atomic replacement.

### Snapshots

A snapshot contains:

- Format and state schema versions
- Last included log index and term
- Serialized deterministic state
- State hash
- Payload checksum

Write to a temporary file, flush it, then atomically replace the prior snapshot. Keep enough metadata to reject incomplete or incompatible snapshots.

Recovery:

1. Load and validate persistent metadata.
2. Find the newest valid snapshot.
3. Restore state and verify its hash.
4. Scan and validate later log records.
5. Replay committed entries after the snapshot.
6. Re-check invariants and state hash.
7. Rejoin the cluster as a follower.

## 9. Network protocol

Every frame includes:

- Protocol magic
- Protocol major/minor version
- Message type
- Request/correlation ID
- Payload length
- Serialized payload
- Frame checksum where appropriate

Initial message types:

- `APPEND_ENTRIES`
- `APPEND_RESPONSE`
- `REQUEST_VOTE`
- `VOTE_RESPONSE`
- `INSTALL_SNAPSHOT`
- `SNAPSHOT_RESPONSE`
- `CLIENT_COMMAND`
- `CLIENT_RESPONSE`
- `STATUS_QUERY`
- `STATUS_RESPONSE`
- Protocol error/handshake messages

Heartbeats are empty `APPEND_ENTRIES` messages rather than a separate safety mechanism. Reject oversized, malformed, unsupported, and context-invalid messages without unsafe allocation.

Duplicate and reordered delivery must not duplicate state transitions. TCP provides ordered bytes per connection, but reconnection and retries still require message and log-level idempotency.

## 10. Incremental consensus plan

Do not begin with elections. Build safety in observable stages.

### Stage A: fixed-leader replication

- Static three-node membership
- One configured leader
- Followers validate `prev_log_index` and `prev_log_term`
- Leader tracks per-follower `next_index` and `match_index`
- Entries commit only after majority replication
- Followers apply only through the leader-announced commit index
- Duplicate AppendEntries is idempotent
- Conflicting uncommitted suffixes are repaired

### Stage B: leader election

- Persistent `current_term` and `voted_for`
- Follower, candidate, and leader roles
- Randomized election timeouts from deterministic injectable randomness in tests
- One vote per term
- Up-to-date-log voting rule
- Majority election
- Immediate step-down on a higher term
- Heartbeats and election reset rules

### Stage C: recovery and catch-up

- Restart from metadata, snapshot, and log
- Incremental follower catch-up
- Snapshot installation for followers behind compacted history
- Leader failure and re-election
- Old leader rejoins as follower

### Explicit initial limitations

- Static membership only
- No Byzantine fault tolerance
- No cross-region assumptions
- No linearizable follower reads
- No membership changes
- No pre-vote or leadership transfer initially
- No claim of full Raft compliance until the implementation passes documented safety and liveness tests

Client success is returned only after commitment and local application. Client retries use command IDs so an accepted command can return its prior result rather than append a duplicate event.

## 11. Concurrency model

Prefer one serialized consensus state owner per node:

- Asio I/O threads parse frames and enqueue typed messages.
- A consensus event loop owns role, term, indexes, and replication state.
- Storage operations expose explicit completion points.
- State-machine application occurs in committed-index order.
- Read-only snapshots of status are published safely to the API layer.

Do not allow multiple network callbacks to mutate consensus state directly. Start with a simple correct model, profile it, then add parallelism only where measurements justify it.

Every shared object must have documented ownership and synchronization. Run sanitizers and stress tests in supported environments.

## 12. Failure injection

The runtime/simulator must support seeded, reproducible controls:

- Delay selected messages
- Drop selected messages
- Duplicate selected messages
- Disconnect peers
- Partition node groups
- Pause a node
- Terminate and restart a node
- Corrupt or truncate test copies of logs/snapshots
- Inject storage failure before/after flush boundaries

Production code paths must not silently activate faults. Failure controls require explicit test configuration or management authorization.

## 13. Testing strategy

### Unit tests

- Commands, events, and invariants
- Deterministic serialization
- Log framing, checksums, and tail recovery
- Snapshot validation and atomic replacement
- Consensus state transitions
- Protocol framing and malformed input

### Model and property tests

- Replay is deterministic.
- Applying duplicates changes state at most once.
- Snapshot recovery equals full replay.
- Rejected commands append no events.
- Committed entries are never lost after configured durability succeeds.
- Two nodes never consider different entries committed at the same index.
- Healthy replicas converge after faults heal.
- Seeds and minimized scenarios are saved on failure.

Maintain a small executable reference/model state machine for comparison. The deterministic network simulator should test consensus logic without real time or sockets before equivalent process tests.

### Process/system tests

- Three local processes replicate commands.
- One follower failure preserves quorum progress.
- Leader failure causes a real election.
- Restarted nodes catch up.
- Partitions do not allow minority commits.
- Healed partitions converge.
- Corrupt/truncated persistence is detected.

### .NET tests

Test API clients, activity idempotency, workflow behavior, transport error handling, view models, and asynchronous cancellation. Do not over-invest in pixel-level UI tests.

### Cross-layer recovery tests

- An Activity retry reuses the same stable operation ID and creates no duplicate ChoreoOS event.
- A duplicate Activity attempt returns the prior committed result.
- ChoreoOS elects a new leader while a workflow remains active.
- A recovering ChoreoOS node catches up while a workflow continues.
- A Temporal Worker terminates while the ChoreoOS cluster remains healthy.
- A replacement worker resumes workflow progress from Temporal history.
- Workflow cancellation and timeout do not corrupt ChoreoOS state.
- A workflow blocked on temporary cluster unavailability continues after quorum recovers.

These tests must report which layer supplied each recovery guarantee.

## 14. Measurement and profiling

Record raw machine-readable results before producing plots.

Measure:

- Events committed per second
- Command-to-commit p50/p95/p99 latency
- Append, flush, replication, commit, and apply time
- Replay events per second
- Snapshot creation duration and size
- Snapshot recovery versus full-replay duration
- Memory usage and allocation behavior
- CPU usage
- Network bytes per committed event
- Catch-up and leader-failover time
- Temporal Activity attempts and retry counts
- Temporal Worker restart-to-resumed-progress time
- End-to-end workflow-step-to-ChoreoOS-commit latency
- Behavior with one, three, and later five nodes
- Behavior under delay, loss, duplication, and partitions

Use warmup, fixed seeds, repeated samples, environment metadata, and confidence intervals where useful. Profile before optimizing. Document whether improvements come from batching, allocation reduction, serialization changes, lock reduction, or I/O policy changes.

## 15. C# dashboard architecture

```text
Views
  -> ViewModels
    -> application services
      -> generated ChoreoOS gRPC clients -> C++ node management API
      -> Temporal clients -> Temporal Server
```

Dashboard features, in order:

1. Node list, role, health, term, commit index, last-applied index
2. Leader changes and recent event timeline
3. Stage and dancer visualization from authoritative state
4. Snapshot/recovery state
5. Throughput and latency metrics
6. Controlled fault actions
7. Benchmark result views
8. Workflow list, status, current step, history summary, retries, and timers
9. Workflow-to-cluster correlation, including current leader and commit index

The runtime remains fully operable through CLI and tests when the dashboard is absent.

## 16. Temporal durable orchestration

Initial workflows:

- `RehearsalWorkflow`
- `PerformanceWorkflow`
- `ChoreographyDeploymentWorkflow`
- `RecoveryWorkflow` only where it coordinates operational recovery rather than replacing node recovery

`RehearsalWorkflow` should demonstrate:

1. Load an explicit choreography version from ChoreoOS.
2. Validate stage and cluster readiness.
3. Wait for participant/readiness signals.
4. Schedule rehearsal phases with durable timers.
5. Submit idempotent commands to the current ChoreoOS leader.
6. Wait for committed outcomes or completion signals.
7. Collect runtime and benchmark metrics.
8. Produce a final report.

Activities represent external operations:

- Query ChoreoOS state and cluster health.
- Submit a command using a stable operation/command ID.
- Request a ChoreoOS snapshot.
- Register rehearsal metadata.
- Notify participants through a fake idempotent provider.
- Collect benchmark artifacts.

Activities may run more than once. Every mutating Activity must derive a stable operation ID from workflow ID, run ID where appropriate, and logical step identity. ChoreoOS command deduplication remains the final protection against duplicate authoritative effects.

Workflow code follows Temporal determinism rules and performs no direct network or filesystem I/O. Activities perform I/O and classify errors as retryable or permanent. Durable timers handle long waits. Signals modify workflow intent, while Queries expose status without mutation.

### Independent failure domains

```text
ChoreoOS node crash
  -> replicated log and C++ consensus recover authoritative runtime state

Temporal Worker crash
  -> Temporal history retains workflow state
  -> another/restarted C# worker resumes workflow tasks

Temporal Server outage
  -> workflows pause
  -> ChoreoOS cluster continues serving independently

ChoreoOS quorum loss
  -> mutating Activities retry or wait
  -> Temporal workflow remains durable
```

No Temporal workflow state may be treated as proof that a ChoreoOS command committed. Activities must query or receive an explicit committed result from ChoreoOS.

## 17. Repository structure

```text
ChoreoOS/
  CMakeLists.txt
  CMakePresets.json
  vcpkg.json
  cpp-runtime/
    include/choreoos/
      api/
      consensus/
      log/
      network/
      protocol/
      runtime/
      state/
      storage/
    src/
      api/
      consensus/
      log/
      network/
      protocol/
      runtime/
      state/
      storage/
    apps/
      node/
      cli/
      replay/
    tests/
      unit/
      integration/
      distributed/
      property/
      recovery/
    benchmarks/
    simulator/
  proto/
    internal/
    management/
  dotnet-dashboard/
    ChoreoOS.ControlPlane.sln
    src/
      ChoreoOS.Dashboard/
      ChoreoOS.Workflows/
      ChoreoOS.TemporalWorker/
      ChoreoOS.Client/
    tests/
      ChoreoOS.Dashboard.Tests/
      ChoreoOS.Workflows.Tests/
      ChoreoOS.Integration.Tests/
  temporal/
    docker-compose.yml
    dynamicconfig/
  python-tools/
    experiments/
    analysis/
    plotting/
  configs/
    node1.yaml
    node2.yaml
    node3.yaml
  scripts/
  docs/
    architecture.md
    state-machine.md
    protocol.md
    consensus.md
    persistence.md
    recovery.md
    failure-model.md
    temporal-boundary.md
    benchmarks.md
  results/
```

## 18. Delivery milestones

### Milestone 1: deterministic single-node core

Pure C++ state machine, invariants, replay, CLI, unit tests, and state hashes.

### Milestone 2: durable storage

Checksummed event log, crash/tail recovery, snapshots, replay tooling, and storage benchmarks.

### Milestone 3: fixed-leader cluster

Boost.Asio transport, protocol framing, three processes, majority commit, follower catch-up, and replica convergence.

### Milestone 4: elections and recovery

Terms, voting, heartbeats, election, leader failure, restart, snapshot installation, and partition tests.

### Milestone 5: fault simulation and property testing

Deterministic simulator, seeded randomized scenarios, duplicate/drop/delay/partition injection, and saved counterexamples.

### Milestone 6: performance engineering

End-to-end benchmarks, profiling, evidence-based optimization, and reproducible result reports.

### Milestone 7: C# control plane

gRPC management API, typed C# client, Avalonia cluster dashboard, stage view, failure controls, and metrics visualization.

### Milestone 8: Temporal orchestration

Temporal Server, C# Worker, retry-safe Activities, rehearsal/performance/deployment workflows, workflow views, and cross-layer recovery tests.

### Milestone 9: final hardening

Cross-platform runs, sanitizers, documentation, scripted demonstrations, limitations, and final benchmark results.

## 19. Scope and honesty rules

- Correctness precedes distribution, UI, and optimization.
- A fixed-leader replicated log is not called consensus.
- A partial Raft-like implementation is not called production Raft.
- Passing randomized tests is evidence, not proof.
- Acknowledged durability depends on actual flush semantics.
- Availability requires quorum; a minority must reject writes.
- Metrics come from real runs and retain raw artifacts.
- The dashboard cannot fake node transitions or recovered state.
- Temporal workflow completion is not a substitute for ChoreoOS commit acknowledgement.
- Temporal retries imply Activities may execute more than once; idempotency is mandatory.
- Temporal Worker recovery and ChoreoOS node recovery are demonstrated and measured separately.

This is larger than a credible two-week full implementation. Each milestone must be independently demonstrable so the portfolio remains valuable before every final feature is complete.

## 20. Definition of done

- C++ is the sole source of authoritative cluster and choreography state.
- Three local node processes replicate and commit an ordered event log.
- Committed events survive tested crashes under the documented durability mode.
- Healthy nodes deterministically converge to the same state hash.
- A follower can recover from snapshot plus later log entries.
- A leader can fail and be replaced by a real majority election.
- Minority partitions cannot commit new entries.
- Duplicate delivery and client retries do not duplicate state transitions.
- Seeded randomized tests reproduce failures.
- Benchmarks report real throughput, latency percentiles, replay, snapshot, and recovery measurements.
- The Avalonia dashboard displays and controls the real C++ cluster through an explicit API.
- Temporal runs long-lived workflows through a separately terminable C# Worker.
- Retried Activities do not duplicate committed ChoreoOS effects.
- Worker failure, leader failure, and combined cross-layer recovery scenarios pass integration tests.
- The dashboard visibly distinguishes authoritative cluster state from orchestration workflow state.
- Documentation accurately states guarantees, assumptions, and known limitations.

## 21. First implementation slice

1. Remove the obsolete Go scaffold and separate the existing Temporal setup from runtime infrastructure.
2. Establish C++ toolchain, CMake, vcpkg, formatting, testing, and CI.
3. Define fixed-point domain value types.
4. Define versioned commands and events.
5. Implement the pure deterministic state machine and invariants.
6. Add deterministic serialization and state hashing.
7. Build an in-memory CLI demonstration.
8. Add unit and seeded property tests before persistence or networking.

The first exit criterion is simple: two independent C++ state-machine instances given the same event sequence produce identical serialized state and hash, while invalid commands produce no events.
