# Durable local compaction in the experimental Raft core

The experimental core accepts `CompactCommitted` as explicit local maintenance.
This supplies a bounded-log lifecycle prerequisite; it does not activate the
core in `Replica`, transfer/install snapshots, or extend the formal model.

## Contract and persistence ordering

Compaction does work only when `applied == commit > snapshot_index`. The host
must already have durably established the application state corresponding to
`applied`; a number alone is insufficient. When application lags commit, or the
boundary is already current, the input has no effects. All roles may compact.
Halted nodes and nodes with another pending write retain the existing refusal
rules; the host retries maintenance after completing the pending operation.

The returned `PersistRequest.kind` is `PersistKind::LocalCompaction`. Ordinary
writes remain `StateUpdate`. The target:

- moves the boundary to the absolute committed index and its actual log term;
- preserves the current term, vote, commit and applied indexes;
- preserves every uncommitted entry at its original absolute index;
- removes only the committed prefix from the retained in-memory log.

The acknowledged `Node.durable` retains the old boundary until matching
`PersistCompleted(token)`. Stale tokens do nothing. Compaction releases no
messages, application callbacks or client success. A matching `PersistFailed`
halts the core without publishing the staged state. The old in-memory state
cannot establish what survived on disk.

A host must dispatch this intent to **application snapshot plus WAL
maintenance**, not a metadata-only or ordinary suffix write. For the isolated
FileEngine path, under exclusive directory ownership and synchronized access:

1. Verify that the adapter's current projection agrees with the acknowledged
   core metadata, snapshot boundary, suffix and committed/applied state.
2. Execute `FileEngineAdapter::checkpoint()` successfully. It snapshots the
   committed choreography state and durable retry receipts, compacts the WAL,
   and audits the resulting recovered projection.
3. Verify that the resulting projection agrees with the requested target,
   including its boundary term and retained suffix. Preserve the application
   payload/hash and receipt correspondence; index equality alone is insufficient.
4. Only then feed the matching `PersistCompleted` to the core.
5. On failure, uncertainty or disagreement, feed `PersistFailed`, stop the
   node, recover from actual storage, replace `Node.durable` from that verified
   projection, and only then use `Restart`.

This is a documented contract and a narrow test driver, not a general host
implementation. A completed checkpoint with a lost callback can recover the
new boundary; a failure before disk work can recover the old boundary. Existing
FileEngine recovery and atomic replacement/redo contracts govern intermediate
I/O failures. This PR adds no all-files atomic transaction or new fault model.

## Replication and capacity

Maintenance does not advance peer match indexes or acknowledge snapshot
installation. Existing outstanding probes retain their exact request context,
including any individual entry that was compacted locally. Replies still use
that original context rather than a follower's unverified suffix. A peer whose
next index requires removed history receives the existing `snapshot_needed`
work request on a later heartbeat; snapshot transport/correlation is still
required for catch-up.

At the 100,000-entry retained-window limit, proposals report capacity pressure.
After sufficient committed state has been applied and compaction has completed
durably, proposals can resume. An entirely uncommitted full window cannot be
fixed by discarding it. The host must schedule apply/maintenance and provide
backpressure; this input does not add an automatic timer or scheduler.

Ordinary transitions retain shared chunked logs. Explicit maintenance copies
the retained uncommitted suffix once, so its cost is proportional to the suffix
payloads rather than independent of them. It temporarily holds old and new
suffix storage until completion/ownership release. Application data and durable
retry receipts retain their existing growth policies.

## Verification boundary

`RaftLocalCompactionTest` exercises all roles, stale completion/failure tokens,
application-lag/no-op cases, pending-write exclusion, fail-stop behavior,
lagging peers, retained probes, capacity recovery and repeated snapshot-only
lifecycles. `FileEngineAdapterTest` drives the actual core request through a
real checkpoint/reopen, checks complete recovered Raft state and application
payload/receipts, and checks lost completion before/after disk work.

`trace_action` names the new action for diagnostics, but the v1 trace serializer
rejects `CompactCommitted` even when it is a no-op. It also retains its rejection
of snapshot states. The v1 schema/checker and `ChoreoRaft.tla` have no compaction
mapping; no TLC-to-code refinement or new model-checking result is claimed.
The v1 checkpoint store still rejects nonzero snapshot boundaries.

Next gates are core snapshot installation and transfer/correlation, a general
FileEngine-backed host with reviewed commit/apply semantics, snapshot-aware
formal modeling/refinement, then staged production integration.
