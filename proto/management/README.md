# ChoreoOS management API (proposed v1)

`choreoos_management.proto` defines the future C++ management server / C# dashboard
client boundary. **This is a contract only, not a running gRPC service.**
The existing C++ TCP protocol already supports command submission and node status
(including canonical state), and its committed-command deduplication is the basis
for the future `SubmitCommand` implementation.

## Safety and retry rules

- `operation_id` must equal the canonical command's ID. The server must reject
  mismatches and any ID reused with different command content; it must not
  silently reinterpret a conflicting retry.
- Successful submission means the event was durably majority-committed and
  applied by the leader. A timeout is **ambiguous**, not proof of failure;
  retry with the same operation ID and exact command.
- A duplicate committed command returns its original event and index, not a
  second event. Clients should not rely on the current state hash being the
  historical hash at the time of the first response.
- A follower cannot accept mutations. The client should discover and retry
  the current leader without changing the operation ID.
- `GetState(require_leader=true)` must reject a nonleader. Even a leader's
  state read is not automatically linearizable without a quorum/read barrier;
  document the consistency level before exposing this as a production API.
- `ListEvents` must bound `page_size` and never return uncommitted entries.
  A compacted range may require a snapshot/bootstrap response instead.
- `InjectTestFault` must be disabled by default and require explicit test
  mode plus access control. Never expose it unauthenticated on a public port.

## Implementation gates

1. Complete all three partition tests and verify CI on Linux and Windows.
2. Choose the gRPC/protobuf build toolchain and generate C++ server and C#
   client stubs reproducibly. The existing internal protobuf C++ generation
   prerequisite is still incomplete.
3. Adapt the C++ runtime's existing command/status operations to the service,
   then implement bounded event pagination, metrics, and recovery status.
4. Add authenticated transport, explicit test-only fault authorization,
   idempotency conflict tests, and end-to-end gRPC tests before calling Step 34
   complete.
