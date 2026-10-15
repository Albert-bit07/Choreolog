# Recovered snapshot offsets in the experimental Raft core

The pure `raft::step` core now supports a recovered snapshot boundary and a
retained suffix. This does not activate the core in production, implement
snapshot transfer/install or extend the existing TLA+ model.

## Representation and transitions

`Durable.snapshot_index` and `snapshot_term` identify the last included entry.
Both default to 0 for a complete log. `log[0]` is the entry at
`snapshot_index + 1`; the absolute last index is `snapshot_index + log.size()`.
The host retains the verified snapshot application state and retry receipts;
the pure core does not reconstruct them from these two numbers.

Recovered/pending states must satisfy:

```
snapshot_index <= applied <= commit <= last_index
```

A nonzero boundary needs a positive term no greater than the current term.
Retained entry terms are positive, nondecreasing from the snapshot term, and
no newer than metadata. Invalid state halts without releasing effects. Index
arithmetic is checked; `UINT64_MAX` is reserved to prevent next-index overflow.

- Vote freshness and election requests use the absolute last index and the
  last retained term, or the snapshot term for a snapshot-only log.
- Append requests use absolute previous indexes and the snapshot term at the
  boundary. Vector offsets subtract the boundary before indexing.
- Followers reject requests whose previous prefix is compacted away or whose
  boundary term does not match; they do not invent missing history. Higher
  terms are still persisted before replies to valid-but-unmatched requests.
- Conflict repair preserves committed absolute indexes. A short heartbeat
  does not truncate a suffix or commit entries beyond its matched prefix.
- Majority commit uses absolute match indexes and still requires a current-term
  entry. Apply advances through the retained suffix and releases each entry
  only after its matching persistence completion.

Append envelopes with decreasing/future terms or overflowing index ranges are
rejected before changing term/log state. Vote requests must provide a coherent
last-index/last-term pair. Persistence completion/failure retains the existing
token and fail-stop discipline.

## When a peer needs a snapshot

If a leader's next index for a peer is at/below the local boundary, ordinary
AppendEntries cannot supply that history. `Effects.snapshot_needed` reports the
peer and required snapshot index/term. It does not send a snapshot, acknowledge
installation, advance match indexes or authorize a client receipt. A future
host must verify/transfer the material and correlate installation responses.

Snapshot work discovered while staging a client proposal is retained with that
pending write and released only after matching `PersistCompleted`. Failed
persistence drops it. A heartbeat can request an already durable snapshot
directly. There is no retry/fairness or catch-up liveness claim for a host that
does not yet implement this work request.

## Explicit compatibility boundaries

- `raft-core-v1.bin` cannot serialize these fields. Its CheckpointStore rejects
  nonzero boundaries instead of dropping them; ordinary zero-offset checkpoints
  remain unchanged.
- `replay_domain_committed(Durable)` still requires a complete log. Use the
  separate recovered-domain projection for snapshot application/receipts.
- The v1 trace serializer throws `std::invalid_argument` for a snapshot state.
  The independent v1 checker also rejects nonzero offset fields in supplied
  rows; its sixth negative mutation tests this refusal. Existing zero-offset
  traces and five earlier mutation checks remain supported.
- `ChoreoRaft.tla` and its bounded profiles still model complete logs. No
  snapshot-model mapping or TLC replay/refinement result is claimed here.

## Evidence and next gates

`RaftSnapshotOffsetTest` covers election freshness, snapshot-only logs, outgoing
suffix/boundary indexing, follower repair and committed conflict protection,
short heartbeats, absolute quorum commit/apply, snapshot-needed effects and
failed persistence, invalid/overflowing states/messages, and legacy refusals.
A real FileEngine snapshot plus FileEngineAdapter drives an election metadata
write before the test releases its vote requests. This is a narrow test host;
it does not map every protocol persistence/application transition to production.

Remaining work includes durable local compaction and snapshot installation
transitions, host snapshot transfer/correlation, the FileEngine commit/apply
mapping, production host integration, a reviewed snapshot-aware formal model,
and independently checked state/action/stuttering correspondence. Shared-code
tests establish bounded behavior, not a production consensus proof.
