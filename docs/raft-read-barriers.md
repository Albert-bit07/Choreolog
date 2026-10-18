# Transition-core quorum read barriers

This is a consensus/storage-host primitive, not yet a public read API.
`GetState`, state pagination and status queries retain their existing local
observation semantics. `require_leader` still checks local role only; it is
not a linearizable-read flag. The legacy backend is unchanged.

## Contract and safety boundary

`BeginRead{context}` admits at most one volatile round. It captures the current
committed index and term, counts the leader itself, and requires a committed
entry in the leader's current term. A new leader must first commit its existing
leadership no-op. An empty log or an old-term commit returns `NotReady`.
The current-term check also recognizes a compacted snapshot boundary.

Only successful, exact replies to append probes **created after admission**
count toward the read quorum. The core records one eligible probe ID per peer.
A probe already in flight retains its original correlation and replication
purpose, but cannot authorize the new read. Once answered, a subsequent
`HeartbeatTimeout` creates a fresh eligible probe. Retries retain exact IDs;
they do not create additional votes. Distinct configured peers count once.
Replies must match the current term, active probe ID and exact covered prefix.
Snapshot replies, follower hints and accumulated replication `match` indices
do not count. The existing authenticated transport identity check remains a
host obligation; this is a fixed-membership, non-Byzantine Raft contract.

A fresh quorum establishes a point after read admission at which the leader
still has quorum authority. `Ready{context, term, index}` is emitted only when
local durable application covers the captured committed prefix. The host must
read the applied state after this result, on the same owner strand. `index`
is a minimum applied prefix, not a promise that later writes are absent.
The caller must not reuse a completion as authority for a later independent
request. No clock lease, local role observation or successful socket write
substitutes for the fresh quorum.

## Lifetime, bounds and integration

- A second read returns `Busy`, preserving the first round. Reads during an
  outstanding persistence operation also return `Busy`.
- A follower/candidate returns `NotLeader`; a halted node returns `Unavailable`.
- Step-down discards the round and returns `NotLeader`, even while a higher
  term is staged for persistence. Failed persistence returns `Unavailable`.
- `Restart` drops volatile authority and returns `Cancelled`. No read state is
  persisted or recovered. Host-level storage failures return an error and
  discard the entire dispatch output.
- `CancelRead{context}` removes only the matching round, including while a
  durable write is pending. Late replies may still advance replication but
  cannot complete the cancelled read or a later round.
- There is no wall clock in the pure core. The future public API must schedule
  deadlines and cancellation and must bound its own request queue. A partition
  may leave the single round waiting until cancellation or step-down.
- Additional memory is O(members), with one round and one probe ID per peer.
  Admission sends at most one ordinary append per available peer; existing
  in-flight requests are preserved. The round uses the existing heartbeat
  schedule and does not create an immediate heartbeat feedback loop.

`RuntimeHost::dispatch` collects typed results in `HostEffects.reads` while it
drains persistence, application confirmation and maintenance. If any of these
operations fails, it returns an error rather than a partial successful output.
A read introduces no extra WAL event, command receipt or metadata write.
`CoreReplica` forwards the actual matched prefix on append responses, preparing
for the public API integration; it does not yet admit client read rounds.

The next integration must own bounded client waiters/deadlines, map typed
failures to the management API, and assemble a hash-bound state response whose
application metadata corresponds to the returned state. It must exercise a
partitioned former leader through the actual TCP/gRPC path before claiming
linearizable public reads. This document does not claim that work is complete.

## Verification scope

Tests use explicit deterministic schedules for stale pre-read replies,
inter-round reuse, duplicate/unknown/failed/wrong-prefix responses, five-member
quorums, isolation, cancellation, higher/same-term step-down, restart,
application completion/failure, admission backpressure and snapshot-boundary
current-term commits. Storage-host tests cover real FileEngine application,
maintenance and recovered follower behavior.

Neither the existing TLA+ model nor the v1 trace contract models read rounds.
The v1 serializer refuses read actions and active read state. Diagnostic action
names are not evidence of refinement. No model checking or machine-checked
production refinement claim is added by this change.
