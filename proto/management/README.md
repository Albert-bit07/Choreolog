# ChoreoOS management API v1

The management service in `choreoos_management.proto` is implemented by a separate
C++ gRPC gateway at `cpp-runtime/apps/management/main.cpp`. It proxies the
C++ node's existing framed TCP protocol. **The gateway never owns consensus,
state, event ordering, or durable storage.** The C# client stub is generated
from the same schema by `dotnet-dashboard/src/ChoreoOS.Client`.

## Build and run

Build the C++ node normally. For the gateway, enable
`-DCHOREOOS_BUILD_MANAGEMENT_API=ON` (and if using vcpkg, enable manifest
feature `management-grpc` or install equivalent gRPC/protobuf development
packages). Run a node, then start:

```text
choreoos-management <node-tcp-port> <management-grpc-port>
```

The gateway connects only to `127.0.0.1:<node-tcp-port>` and binds to
`127.0.0.1:<management-grpc-port>` using insecure, local-only gRPC.
**Do not expose either endpoint to an untrusted network.** Production
remote access needs mutual TLS, authorization, and request-size/rate policies.

Build the C# client with:

```text
dotnet build dotnet-dashboard/src/ChoreoOS.Client/ChoreoOS.Client.csproj
```

## API guarantees and limitations

- `SubmitCommand` requires an operation ID equal to the canonical command ID.
  Only a leader can accept writes. Success means committed and locally applied.
  Timeouts may hide a successful commit; repeat with the identical operation ID.
  Conflicting reuses are rejected as `CommandConflict` (gRPC `ALREADY_EXISTS`).
- The compacted WAL may discard an original event. The new snapshot-state
  payload format v3 retains compact idempotency receipts (operation ID,
  deterministic 64-bit command fingerprint, index and term) and reconstructs
  the original event from a matching retry. Format-v2 canonical receipts are
  migrated on restore; v1 snapshots are readable, but commands with no
  recoverable receipt return `OutcomeUnavailable` (gRPC `FAILED_PRECONDITION`).
  The fingerprint is a collision-prone 64-bit FNV-1a checksum, **not** a
  cryptographic authentication boundary. IDs and receipts still scale with
  committed command count, though the serialized snapshot is much smaller
  than full event history. Restore/retry semantics depend on an unchanged
  canonical command/event schema.
- `GetState` and `GetNodeStatus` query the C++ node. A leader's state is
  authoritative for its locally applied log, but **is not a linearizable
  quorum-confirmed read**. A follower's state is diagnostic only.
- `ListEvents` pages committed log events only, with a 128-event maximum.
  Requests for compacted history return an out-of-range error; clients must
  bootstrap from a snapshot instead. A page is a local observation and may
  be superseded by later commits.
- `GetMetrics` returns counters, not a full telemetry subsystem;
  `GetRecoveryStatus` reflects the node's completed startup recovery and
  latest snapshot, not an ongoing stream of recovery progress.
- `InjectTestFault` is denied unless the node configuration explicitly sets
  `test_mode: true` and the caller connects to that node from loopback.
  `DROP_PEER`, `DELAY_PEER`, `HEAL_PEERS`, and `PAUSE_NODE` are test-only.
  `HEAL_PEERS` also resumes a paused node. Do not enable test mode in
  production.

## Verification

Normal CI runs C++ unit and TCP integration tests on Linux and Windows,
including all three two-versus-one partition arrangements. The separate
`management-api` job builds the generated native gRPC server and generated
C# client. The `management-api` CI job also runs the generated C# client against a
live C++ node through the gRPC gateway. Production-grade remote authentication,
quorum-linearizable reads, richer metrics, and broader multi-node gRPC
integration coverage remain follow-ups.

## Cluster-aware local client and error metadata

For operations that may outlive a leader, use `ChoreoOS.Client.ChoreoClusterClient`
with the local management ports of every configured node. Its
`SubmitCommittedAsync(request, timeout, cancellationToken)` method discovers
the current leader and retries only the **identical** canonical command and
operation ID. It stops on permanent errors and honors cancellation.

A timeout from this method means **outcome unknown**, not failure: the
command may already be committed. Persist the original operation ID and exact
canonical command before submitting and reuse them on a later retry. The client
does not generate IDs or automatically change the request between attempts.

The gateway returns `choreoos-error-code` in gRPC trailing metadata for
runtime errors such as `NotLeader`, `CommandConflict`, and
`OutcomeUnavailable`. Transport failures and runtime `Unavailable` errors
during command submission include `choreoos-outcome: unknown`. Routing uses
these machine-readable codes rather than parsing the English error message.
A follower's diagnostic state remains non-linearizable regardless of what
the client observes.

The `ChoreoOS.ClusterSmoke` project starts three real C++ node processes
and three local gateways using ephemeral ports. It exercises leader
replacement, reconnects, safe retries after an uncertain outcome, quorum
loss and recovery, malformed requests, and fault-control denial. CI also
runs the core under ASan/UBSan and analyzes selected protocol/network files
with clang-tidy. These checks do not establish full Raft compliance or
production security.
