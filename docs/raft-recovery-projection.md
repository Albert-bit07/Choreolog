# Snapshot-aware production recovery correspondence

This is a read-only, executable storage correspondence check. It does not
activate the experimental Raft core or prove refinement to the TLA+ model.

The earlier `audit_domain_store` produces `raft::Durable`, whose log starts at
index 1. It still rejects compacted stores. The new
`audit_recovered_domain_store` produces a separate `RecoveredDomainProjection`:

| Field | Meaning |
| --- | --- |
| `term`, `voted_for` | Checked production metadata, including a term newer than the last event |
| `snapshot` | Optional verified production snapshot; its index/term are the absolute boundary |
| `suffix` | Canonical Raft entries strictly after that boundary, starting at boundary + 1 |
| `commit`, `applied` | Absolute production indexes, equal for a successfully recovered FileEngine |
| `applied_state_hash`, `applied_state_payload` | Observed application state plus compact retry receipts |

Without a snapshot the boundary is index/term 0, and the suffix starts at 1.
An installed snapshot with an empty WAL is valid. No placeholder entries are
invented for compacted history. The new type cannot be passed as `Durable` to
the existing transition core or checkpoint adapter.

## Executable relation

Run the audit only on a quiescent store or under the host's external
synchronization. It does not take FileEngine's private mutex and is not a
concurrent disk monitor. Prefer a freshly recovered store for crash evidence.

1. Reject a pending snapshot-install redo journal or an unreadable journal
   status. FileEngine must finish recovery before projection.
2. Read checked metadata; require its commit to equal FileEngine's commit.
3. Validate every retained WAL event, including records at/before the snapshot:
   canonical envelope, positive contiguous indexes within the retained range,
   nondecreasing terms and no future terms. Retained prefix terms cannot exceed
   the snapshot term, and an available boundary anchor must match that term.
4. Independently verify the snapshot payload/hash/index/term through the
   existing snapshot decoder. Require boundary <= commit = applied <= last
   projected index. Check index arithmetic before adding the suffix length.
5. Decode every suffix entry at its absolute index, including uncommitted
   entries. Terms cannot decrease from the snapshot boundary or exceed the
   metadata term. Replay only the committed suffix onto the snapshot state.
6. Compare the resulting applied index, domain hash and complete serialized
   checkpoint to the observed FileEngine state. `state_hash` excludes retry
   receipts, so a matching domain hash alone is insufficient. The older full-WAL
   audit now also compares the checkpoint payload.

`replay_recovered_domain` checks a captured projection without accessing disk.
It uses the production event/snapshot parsers and state machine; it is not an
independent implementation of the domain semantics. The snapshot is the
authoritative base: this cannot reconstruct or prove the missing event history,
nor detect a consistently changed base and observed state without an external
source of truth. FNV consistency hashes and receipt fingerprints are not
cryptographic authentication.

When a complete WAL is retained, FileEngine can rebuild receipts from that
history. If this repairs receipt data that disagrees with the snapshot base,
the full-WAL audit can succeed while the snapshot-based audit refuses it. This
also conservatively refuses older payloads whose missing receipts would need a
separate validated enrichment rule. A failed audit does not itself declare the
store unrecoverable or modify it.

## Regression gate

`RaftRecoveryProjectionTest` exercises real compaction/restart with an
uncommitted suffix, absolute commit advancement, installed snapshot/empty WAL,
receipt-based retries, full/empty stores, malformed snapshot/index/envelope/term
mutations, receipt drift with an unchanged domain hash, corrupt metadata,
retained anchor disagreement, and recovery after all four snapshot-install redo
boundaries. The uncertain old writer must fail projection; a reopened store
must complete redo and satisfy correspondence.

## Remaining refinement obligations

- Define snapshot offsets and compaction transitions in the experimental core
  and formal model; neither currently supports this projected representation.
- Define a recovery-safe writable adapter and application/receipt checkpoint
  contract, including operation-specific ambiguous persistence outcomes.
- Capture production inputs, persistence outcomes, authenticated senders and
  effects; define an explicit model-state/action projection and stuttering
  relation. The existing trace-contract checker is for the experimental core.
- Validate bounded model-produced behavior replay and negative mutations before
  stronger refinement claims. Arbitrary interleavings, liveness and exhaustive
  filesystem/power-loss behavior remain unproved.

This milestone removes the read-only snapshot correspondence gap. It does not
complete the writable adapter, pure-core migration, or machine-checked
production refinement program.
