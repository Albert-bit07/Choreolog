# Experimental writable FileEngine adapter

`raft::FileEngineAdapter` executes validated writes using the production
`meta.bin`, `wal.bin` and existing snapshot formats. It is an isolated library
component: neither production `replication::Replica` nor the experimental pure
transition core calls it. It replaces neither runtime.

## Write contract

One synchronous host exclusively owns the directory. Do not open this adapter
on a live node's directory or concurrently access it from other threads or
processes. There is no filesystem ownership lock or authenticated membership
policy here; the eventual host must enforce those boundaries.

`open` and `recover` use FileEngine recovery and the snapshot-aware
[recovered-store audit](raft-recovery-projection.md). The adapter forces sync
durability, disables commit-on-append and automatic snapshots/compaction, and
keeps its mutable FileEngine private. A write plan supplies:

- The current snapshot's absolute index and term, or both 0 without a snapshot.
- Target metadata term/vote, the complete canonical suffix strictly after the
  snapshot boundary, and an absolute target commit index.

Before writing, validate the boundary, term/commit monotonicity, same-term vote
preservation, vote ID syntax, immutable committed prefix, canonical entry
envelopes and nondecreasing/non-future terms. Decode all entries and validate
application of the entire committed prefix, including domain semantics and
retry receipts. Invalid plans return an error without writing or poisoning the
adapter. Plans are capped at 100,000 suffix entries.

This plan intentionally has no independently writable `applied` index.
Recovered application progress equals commit because FileEngine replays the
committed prefix onto the snapshot. Its serialized state includes retry
receipts. A future pure-core adapter must explicitly relate this rule to the
core's separate commit/apply steps; do not silently feed `raft::Durable` here.

## Execution and failure boundaries

| Phase | Durable action and recovery consequence |
| --- | --- |
| Metadata | Persist target term/vote, retaining old commit. A later failure can still leave the new vote durable. |
| Conflict repair | Keep the common exact suffix prefix; truncate only uncommitted conflicting/excess records. |
| Append | Append and sync each new canonical event. A failure can leave a recoverable partial suffix. |
| Commit | Advance metadata commit and publish application state through FileEngine's staged commit path. |
| Audit | Compare actual recovered-store projection to the validated target before reporting success. |

The adapter emits no network or application effects itself. Only a successful
`persist` return is a completion signal for a future host to release dependent
effects. Any returned storage error, post-write audit disagreement or caught
standard I/O exception sets `uncertain()`. Then `current()` and further writes
return errors until actual `recover()` succeeds. Cached observations from before
an error must not be treated as the current durable state.

Recovery discards the old FileEngine, opens real files again (including WAL tail
repair and snapshot redo), and checks application/receipt correspondence.
Failure to recover leaves the adapter blocked. A failed persist can have
committed: an injected error after commit returns failure, while reopening
observes the new commit and receipts. Retry logic must treat errors as ambiguous
outcomes, not proof that the requested events are absent.

## Tests

`FileEngineAdapterTest` covers metadata-only elections, durable higher term/vote,
committed replay/receipts and real reopen, uncommitted suffix isolation/repair/
shortening, preflight refusal of invalid/domain-invalid plans, compacted snapshot
boundaries, and explicit test-only injection. Six failure cases interrupt after
metadata, truncation, first append or commit, or use FileEngine's actual
before/after-flush append faults. Each blocks the old adapter, verifies actual
disk outcome after reopen, and resumes to the target. Another case checks an
after-flush failure at a compacted absolute boundary. Replacing `meta.bin` with
a directory exercises real I/O failure/exception handling and failed recovery.

## Remaining limits

This is ordered multi-operation persistence, not an atomic transaction over all
files. Crash recovery may retain a higher term/vote and only a prefix of the
requested uncommitted suffix. Tests inject API/phase failures and reopen files;
they do not simulate every torn sector, filesystem, power loss or OS sync
failure. Existing platform directory-durability assumptions still apply.

No snapshot install, concurrent dispatch, transport authentication, automatic
maintenance scheduling or production migration is implemented by this adapter.
Explicit checkpoint/compaction maintenance is described below. The authoritative snapshot and receipt-enrichment limitations of the
read-only projection still apply.

The pure core now supports recovered offsets, but production refinement still
requires core snapshot-install/compaction semantics and a snapshot-aware formal
model, an explicit commit/application mapping, host completion/effect
discipline, production input/effect traces, and an independently checked
model-state/action and stuttering relation. No TLA+-to-production proof or
liveness guarantee follows from this adapter.

## Incremental writes and maintenance

The stabilization update adds `persist_delta(DomainWriteDelta)` for the future
host: keep a validated prefix through `keep_index`, append only changed entries,
and advance commit after validating the newly committed application batch.
Snapshot index/term tokens reject stale plans. Existing committed history is
immutable. Metadata-only updates do no suffix replay/copy; full observed audits
remain on recovery and checkpoint maintenance. The original `persist(plan)` is
retained as a bounded full-plan validation/audit API, with its higher cost.

`current_shared()` returns a cached immutable projection; old views remain
valid across writes. Payload/receipt serialization happens only when a view is
explicitly requested. `current()` keeps its value-returning compatibility API.
Shared log chunks retain immutable payload aliases, with constant-time term
summaries and copy-on-write chunk mutation. The supported retained window is
100000 entries. At that boundary the core emits `capacity_exhausted` for a client
proposal, allowing the host to apply backpressure instead of halting.

`checkpoint()` now saves committed state and compacts the production WAL to its
snapshot boundary, preserving uncommitted suffix entries and durable retry
receipts. Call it before exhausting the retained window; stale boundary plans
must be rebuilt afterwards. Its explicit compaction uses the newest snapshot
rather than preserving older-snapshot fallback coverage. Failure requires real
recovery. This supplies a storage lifecycle API; it does not implement a core
snapshot-install input, snapshot RPC, automatic host scheduling or production
activation. The remaining formal/host integration limits above still apply.
