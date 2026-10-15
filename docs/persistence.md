# Persistence formats

ChoreoOS stores one directory per node. The state machine never reads these files. Recovery reads them, then calls the same `apply()` function used by a live node.

Durability mode `sync` flushes the file and asks the operating system to commit it (`FlushFileBuffers` on Windows, `fsync` elsewhere) before a command returns success. Buffered mode skips that flush and must not be described as crash-safe.

WAL/metadata format version `1` and state schema version `1` are the versions used here. A different version, magic, or checksum is a hard error. There is no silent migration. A checked-in fixture at `cpp-runtime/tests/fixtures/golden-v1` is the compatibility sample: opening it must recover hash `dd22fb677cc125bf`.

## Write-ahead log (`wal.bin`)

Little-endian. Checksum is CRC-32/ISO-HDLC (polynomial `0xEDB88320`).

File header, 16 bytes:

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | Magic `CHOSWAL1` |
| 8 | 2 | Format version `1` |
| 10 | 2 | Reserved `0` |
| 12 | 4 | CRC-32 of the first 12 bytes |

Each record:

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 4 | Magic `LOG1` (`0x31474F4C`) |
| 4 | 2 | Record version `1` |
| 6 | 2 | Flags `0` |
| 8 | 4 | Payload length |
| 12 | 8 | Term |
| 20 | 8 | Log index |
| 28 | N | Payload: one canonical event line, no newline |
| 28+N | 4 | CRC-32 of all preceding record bytes |

The maximum payload is 1 MiB. A larger length is rejected.

If the file ends before a record's declared bytes are present, that tail is incomplete. Recovery truncates back to the last record whose checksum matched. A checksum mismatch, bad magic, or bad version on a fully present record is corruption and recovery stops. A corrupt record in the middle is not skipped.

## Metadata (`meta.bin`)

Written to `meta.bin.tmp`, flushed, then atomically replaced over `meta.bin`. In sync mode the parent directory is fsynced after the rename, including commit-index updates. Application state and retry receipts are published only after successful metadata persistence. Submission and recovery preserve a higher persisted election term and vote.

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | Magic `CHOSMETA` |
| 8 | 2 | Format version `1` |
| 10 | 2 | `voted_for` length (0 to 64) |
| 12 | 8 | Current term |
| 20 | 8 | Commit index |
| 28 | N | `voted_for` node id, empty until elections |
| 28+N | 4 | CRC-32 of the preceding bytes |

On a single node every checksum-valid log record is committed. If the metadata commit index is behind the log, recovery advances it to the last valid record after the log is scanned.

## Snapshots (`snapshots/<index>.snap`)

`<index>` is the 16-digit last included log index. The file is written under a `.tmp` name, flushed, renamed, and the directory is fsynced. A `.tmp` file is ignored. Two snapshots are kept: the newest and one fallback.

A snapshot is state plus compact deduplication metadata, not full event history. The outer snapshot file remains format 2, but its text payload is now `snapshot-state 3`: dancers, formations, cues, and small operation-ID/fingerprint receipt rows with implicit indexes and run-length encoded terms. Earlier `snapshot-state 1` and `snapshot-state 2` payloads remain readable. Outer file format 1 (full event history) remains readable for compatibility. Receipt count still grows with command history; the goal is a much smaller constant factor than full event bodies.

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | Magic `CHOSSNAP` |
| 8 | 2 | Format version `2` (`1` is read-only legacy) |
| 10 | 2 | State schema version `1` |
| 12 | 8 | Last included log index |
| 20 | 8 | Last included term |
| 28 | 16 | State hash, 16 hex characters |
| 44 | 4 | Payload length |
| 48 | N | Format 2: `snapshot_payload()` text, one space-delimited line per table row. Format 1: canonical event lines. |
| 48+N | 4 | CRC-32 of the preceding bytes |

Recovery tries snapshots newest first and stops at the first whose checksum, schema, and state hash all verify. It restores that state directly, then applies only log records with a higher index. If no snapshot is valid, recovery replays the whole log; if the log was compacted, that is an error (see below), never a silent partial state.

A command accepted before a snapshot is still recognised after it, because the dedup table is part of the snapshot. For snapshot-state 3, a matching retry reconstructs the original event from the caller's command and the persisted compact receipt without appending another event. Conflicting content reusing an operation ID returns `CommandConflict`. Legacy commands without reconstructible receipts return `OutcomeUnavailable`. The 64-bit FNV-1a fingerprint is deterministic but not collision-proof and should not be used as authentication.

## Compaction

Cluster nodes set `compact_log`. After each snapshot the log is cut back to the older of the two retained snapshots; that snapshot's own record stays as an anchor so the next `AppendEntries` can still check its previous term. This gives a follower a lag margin of one to two snapshot intervals before it needs a snapshot, and guarantees the fallback snapshot still joins up with the log if the newest one is damaged. The single-node CLI does not compact, so replay to any index keeps working.

If the log starts after index 1 and no valid snapshot covers the gap, `open()` fails. That is data loss and is reported as such.

## Crash ordering

Each multi-file change is ordered so every intermediate on-disk state is one `open()` can reconcile:

- **Rewriting the log** builds the replacement in memory and swaps it in with an atomic replace. The old file is never removed first.
- **Installing a snapshot** commits a complete redo journal first; recovery completes the snapshot, metadata and WAL replacement before opening any of them. Legacy intermediate states without a journal retain the previous metadata-based recovery rules. See the transaction contract below.
- **Truncating a conflicting log suffix** fsyncs the shortened file so the removed records cannot reappear.

## Recovery-atomic snapshot installation

`snapshot-install.txn` is the commit decision for one snapshot install. It holds
the exact encoded replacement snapshot, metadata and WAL, including an anchor
and uncommitted suffix only when the local anchor term matches the snapshot.
Existing on-disk file formats and golden fixtures remain unchanged.
Do not downgrade a node with a pending journal: complete recovery with the
journal-aware binary first, since older binaries do not recognize the decision.

The installer validates the snapshot index, term and state hash and encodes all
three images before touching the authoritative files. It atomically writes and
syncs the journal, then replaces/syncs the snapshot, metadata and WAL. Finally it
removes the journal and syncs the node directory. Snapshot installation always
uses sync writes, even for a buffered store. Directory ancestry is synced before
committing a new journal so newly created node directories are retained too.

Recovery resolves a journal **before** scanning the WAL or selecting snapshots:

- Without a journal, no transaction decision exists. A leftover `.txn.tmp` is
  ignored and the prior store is opened normally.
- With a valid journal, recovery replays every replacement and retires the
  journal. Replay is idempotent; an interrupted recovery can retry it again.
- A corrupt, truncated, oversized or unsupported journal blocks opening the
  store. It is never silently deleted or treated as an aborted transaction.
- After an uncertain installation error, the live FileEngine rejects further
  mutations until reopen; the production Replica also halts as in PR #14.

The journal is CRC-32 protected and bounded to 128 MiB including all images:

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | Magic `CHOSTXN1` |
| 8 | 4 | Format version `1` |
| 12 | 8 | Snapshot index, positive |
| 20 | 4 | Snapshot image length |
| 24 | 4 | Metadata image length |
| 28 | 4 | WAL image length |
| 32 | variable | Snapshot, metadata and WAL image bytes, in that order |
| end | 4 | CRC-32 of all preceding journal bytes |

This provides atomicity **at FileEngine recovery**, not simultaneous visibility
to arbitrary readers of the three raw files. The node directory remains a
single-writer store; concurrent processes sharing it are unsupported. Guarantees
depend on the existing platform atomic-replace and sync primitives and supported
filesystem behavior. CRC-32 detects accidental corruption, not malicious writes.
Fault tests simulate interruption after each durable boundary, interrupted redo,
unavailable old WAL data, malformed journals and retained uncommitted suffixes;
they are not exhaustive hardware power-loss testing.

This is the snapshot-install transaction milestone. Ordinary append, term/vote
and commit operations still have their existing separate persistence boundaries;
there is no general multi-operation transaction API. mTLS and production
model-to-code refinement remain separate work.

## Recovery and mutation failure contract

Recovery rejects metadata commits beyond reconstructable snapshot/WAL history;
it never lowers a durable commit to hide missing records. A torn uncommitted
tail may still be discarded. Production Replica also verifies recovered term
ordering and agreement with durable metadata and the snapshot anchor.

FileEngine blocks further mutations after an actual/ambiguous storage failure
until a new instance is reopened from disk. Validation failures before writes
and the deterministic test-only before-flush refusal do not poison the instance.
Expected filesystem/stream failures return typed errors. Commit staging retains
metadata-before-publication while copying only application-domain state and new
history/receipt records, not historical event maps. Cluster compaction trims
Engine/replica full history and command indexes to the retained log window;
compact retry receipts and legacy outcome tombstones remain durable. The CLI
keeps its default full-history behavior. Historical replay explicitly refuses
compacted/incomplete history instead of replaying a suffix as a complete log.
