# Transition-core node runtime

`choreoos-node` can run the pure transition core through its real TCP/mTLS,
FileEngine storage and management interfaces. Select it explicitly:

```yaml
consensus_engine: transition
elections: true
snapshot_every: 100
```

`consensus_engine: legacy` is the default. Existing configurations keep their
current behavior. The transition backend requires elections, valid static
membership and timing bounds. Plaintext listeners/peers must be numeric
loopback addresses; remote replication requires the existing
[peer TLS configuration](peer-tls.md). Client and management listeners retain
the existing local boundary. No production deployment is performed by this
change, and runtime integration does not establish formal verification.

## Runtime ownership

- `network::NodeServer` retains sessions, authentication, framed I/O, queue
  budgets, deadlines, request waiters and management RPC handling.
- `replication::RuntimeReplica` selects the legacy or transition implementation.
- `replication::CoreReplica` owns election timing, canonical proposals, protocol
  conversion and bounded/correlated snapshot streaming.
- `raft::RuntimeHost` owns synchronous durable execution and application
  confirmation. It emits effects only after successful storage verification.
- `raft::step` remains free of clocks, filesystem calls and sockets.

Node startup and the `ready` file name the selected backend. The recovery
management reply includes `consensus_engine` and a bounded `halt_reason`.
A ready process still needs to hear a leader or win an election before serving
writes. Status/state queries remain local observations, not quorum read barriers.
The [core read-barrier primitive](raft-read-barriers.md) is available to the
storage host; public management API integration remains a separate step.

## Durable/application mapping

The storage formats remain the existing `meta.bin`, `wal.bin` and snapshots.
FileEngine recovers committed choreography state and durable retry receipts.
RuntimeHost obtains a cheap Raft view without serializing the entire application
payload for each metadata/log operation.

A persistence request executes as follows:

1. Ordinary updates use `DomainWriteDelta`: preserve the shared common prefix,
   persist changed term/vote, repair only an uncommitted suffix, append new typed
   events, and commit through the requested index. Shared chunks let common
   prefix discovery skip unchanged payloads. Unchanged metadata is not flushed
   again just to append/commit.
2. FileEngine applies the committed batch as part of that durable commit.
   Physical application progress equals physical commit. The core's `applied`
   index records the host's subsequent confirmation of that durable result.
3. Before completing persistence, compare observed metadata, boundary, suffix
   and commit with the target, and check that durable application covers the
   requested applied prefix. `ApplyCommitted` effects **confirm** this result;
   they must never be replayed into Engine a second time. Apply-only completion
   requires verification, without a second metadata write.
4. Drain application confirmations before releasing network/client completion.
   NodeServer additionally checks the actual durable command receipt and its
   index/term before completing a pending client request.
5. Any write error or correspondence failure halts the host. Queued dependent
   effects are discarded. In-memory old state does not describe ambiguous disk
   outcomes; reopening derives truth from real recovery and starts as follower.

This is the implemented host mapping and tested correspondence boundary. It is
not a machine-checked refinement relation. A client disconnected after commit
can retry its original operation ID and recover the original receipt.

## Elections and replication

The runtime drives election and heartbeat inputs from the existing node timer.
Each new leader durably appends a canonical current-term no-op before relying
on majority commitment of older entries. Match positions derive from the exact
outstanding request. The wire response's matched prefix is separate from its
untrusted extra-suffix hint. Established transport identity must agree with the
claimed member before a message reaches the core.

Matching replies pump the next available entry immediately rather than waiting
one heartbeat per entry. Caught-up empty acknowledgements do not create an
unbounded empty-heartbeat loop. Retransmissions keep their original request ID
and payload. Higher-term messages persist the new term before dependent replies.

## Snapshot maintenance and catch-up

`snapshot_every` defaults to 100 and is bounded to 0..100000; 0 disables automatic
maintenance. Once application catches up, local maintenance runs the
[compaction intent](raft-local-compaction.md) through a real application snapshot,
WAL compaction and verified completion. The runtime checks resulting boundary,
metadata and retained uncommitted suffix before acknowledging the core change.
Newest-snapshot compaction retains retry receipts but gives up the older
snapshot fallback coverage provided by the legacy automatic compaction policy.

A lagging peer gets a correlated snapshot probe. The host pins the exact durable
snapshot payload for that probe, even if another quorum commits more entries
and creates a newer local snapshot. Multiple peers can share one immutable
payload. Each chunk is at most 256 KiB by default; a complete transfer is capped
at 64 MiB. One incoming stream is bound to leader, term, index, snapshot term and
hash. Duplicate chunks must agree with retained bytes; gaps/mismatches restart
the stream. The complete payload is verified before installation.

The core stages a snapshot-install intent. FileEngine's existing snapshot redo
transaction governs disk replacement and recovery. A matching boundary term
retains the later uncommitted suffix; a mismatch discards only the uncommitted
suffix covered by the new snapshot. An older/equal snapshot cannot roll back
committed application. A new boundary whose term predates known committed truth
is rejected. Only verified persistence releases final success.

Snapshot chunk responses must match the current chunk correlation, term and
expected end offset. Only final completion of the corresponding root probe can
advance core peer match. Lost final acknowledgements are safely retried against
an already installed boundary. Subsequent ordinary suffix replication resumes.

The retained core window remains 100000 entries. Client admission leaves one
slot for a leadership barrier. This is backpressure, not permission to discard
uncommitted history. Arbitrarily many failed leadership barriers can still
exhaust a finite uncommitted window; a node unable to establish its barrier
fails closed with a diagnostic rather than reporting successful writes. The
same applies to absolute index exhaustion. Configure adequate timing/capacity
and retain operational recovery options; no unconditional liveness is claimed.
Application data and retained retry receipts still grow with the workload.

## Store ownership and migration

Node backends, offline CLI and replay hold an OS-backed exclusive directory
lease. A second shipped writer/recovery tool refuses a live directory. The OS
releases ownership on process termination; a stale lock filename is harmless
and must not be treated as proof of a live owner or manually deleted to bypass
an active node. Low-level FileEngine/adapter callers and benchmarks still need
external ownership and synchronization.

Use the cluster protocol for cluster writes. Offline CLI mutation is for
standalone stores, never out-of-band editing of a replicated cluster history.
For backend migration, stop all cluster nodes, back up the complete stores,
retain node identities/membership, then change the selector consistently and
restart. Do not infer a rolling-upgrade or dynamic-membership guarantee from
wire/storage format compatibility. Reopen with the legacy backend is tested,
but a rollback must preserve files and follow the same stopped-cluster discipline.

The loopback example configurations are under `configs/transition/`. For a
multi-machine deployment, configure peer mTLS and the existing certificate
identity/CA rules; this change adds no automatic certificate issuance/rotation.

## Evidence and remaining scope

Tests cover persisted term/vote before replies, majority commit, apply-once
confirmation, uncertain I/O/reopen, local maintenance, snapshot suffix matching,
pinned generations, stale probes, missing snapshot material and index limits.
Real-node TCP tests cover snapshot catch-up and original retry receipts. The
transition backend also runs certificate rejection/identity binding/plaintext
bypass and authenticated leader failover/recovery tests. A separate executable
process check terminates a live node, verifies OS lease release, retries the
original receipt after recovery, excludes concurrent node/offline tools, and
reopens compatible storage with the legacy backend. CI runs three-process gRPC
client failover/retry checks against both backends.

The v1 trace serializer/checker and TLA+ models still have no snapshot transition
mapping. New snapshot inputs are explicitly refused by v1 trace serialization,
even if they have no effects. Snapshot-aware formal modeling/refinement,
linearizable reads, dynamic membership, Byzantine defense, certificate lifecycle
and a durable receipt retention policy remain separate work. Passing these
integration tests does not prove production consensus safety under every schedule.
