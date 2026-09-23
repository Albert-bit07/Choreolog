# ChoreoOS

ChoreoOS is an educational distributed runtime for live-performance systems. It uses choreography as a concrete domain for implementing and measuring event sourcing, deterministic state machines, durable persistence, replication, leader election, recovery, failure injection, and performance engineering.

This is not an operating-system kernel, trading system, or production-grade consensus product. Guarantees are documented only when the implementation and tests support them.

See [PLAN.md](PLAN.md) for the architecture and [TASKS.md](TASKS.md) for the ordered implementation checklist.

## Responsibility boundaries

### C++20+ — authoritative runtime

The C++ runtime owns:

- Ordered immutable event log
- Deterministic choreography state machine
- Domain invariants
- Durable log and snapshots
- Node-to-node networking
- Replication, quorum, and commit semantics
- Leader election and node recovery
- Failure injection and runtime metrics

Only committed C++ runtime state is authoritative choreography state.

### C#/.NET — control plane and workflow workers

The .NET solution owns:

- Avalonia desktop dashboard
- Typed clients for the C++ management API
- Cluster, stage, event, and metric visualization
- Temporal workflow definitions and Activities
- A separately executable Temporal Worker

The dashboard and worker do not implement consensus or independently derive authoritative choreography state.

### Temporal — durable orchestration

Temporal coordinates long-running workflows above ChoreoOS, including:

- `RehearsalWorkflow`
- `PerformanceWorkflow`
- `ChoreographyDeploymentWorkflow`
- High-level recovery coordination where appropriate

Temporal provides workflow history, durable timers, Activity retries, Signals, Queries, cancellation, and worker recovery. It does not provide ChoreoOS replication, commit semantics, snapshots, or leader election.

Activities interacting with ChoreoOS must use stable operation IDs because an Activity may execute more than once. A workflow treats an operation as successful only after ChoreoOS returns an explicit committed result.

### Python — experiments and analysis

Python supports benchmark orchestration, statistical analysis, plotting, and reproducible experiments. It does not implement the distributed runtime.

## Independent failure domains

- A ChoreoOS node crash is recovered through the C++ replicated log, consensus, and snapshot mechanisms.
- A Temporal Worker crash is recovered through Temporal workflow history and another or restarted .NET worker.
- A Temporal outage pauses workflows but does not stop a healthy ChoreoOS cluster.
- ChoreoOS quorum loss prevents authoritative writes while Temporal retains workflow progress and retries safely.

Cross-layer tests deliberately exercise these failures independently and together.

## Development order

1. Deterministic single-node C++ state machine and CLI
2. Durable event log, snapshots, and replay
3. Fixed-leader three-node replication
4. Leader election and recovery
5. Failure simulation and randomized correctness testing
6. Benchmarking and profiling
7. C++ management API and C# Avalonia dashboard
8. C# Temporal Worker and durable workflows
9. Cross-layer recovery testing and final documentation

The CLI and automated tests remain usable without the dashboard or Temporal.

## Current status

Milestone 3 is implemented: three nodes, one configured leader, and majority commit. A command succeeds only after a majority has stored it and the leader has applied it. Stopping one follower still allows commits. Restarting that follower catches it up to the same hash. This is replication, not consensus. The protocol is described in [docs/protocol.md](docs/protocol.md).

Milestone 2 remains underneath: a checksummed write-ahead log (`wal.bin`), atomic node metadata, and snapshots. Disk formats are in [docs/persistence.md](docs/persistence.md).

Milestone 1 remains the deterministic state machine underneath that log. Two
independent replays of the same store still produce the same hash.

Milestone 0 also provides:

- A C++20 CMake project
- Node, CLI, and replay executable foundations
- vcpkg and GoogleTest integration
- Strict compiler warnings
- clang-format and clang-tidy configuration
- Windows and Linux CI
- Linux sanitizer presets

Configure, build, and test on Windows:

```powershell
.\scripts\dev.ps1 check
.\scripts\dev.ps1 run-cli
```

Temporal Compose files now live under `temporal/` as an independent
orchestration stack. They do not store authoritative ChoreoOS state. The .NET
project scaffold is pending installation of a .NET SDK.

See [docs/development.md](docs/development.md) for verified tool versions and
environment details.
