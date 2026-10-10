# Persistence formats

ChoreoOS stores one directory per node. The state machine never reads these files. Recovery reads them, then calls the same `apply()` function used by a live node.

Durability mode `sync` flushes the file and asks the operating system to commit it (`FlushFileBuffers` on Windows, `fsync` elsewhere) before a command returns success. Buffered mode skips that flush and must not be described as crash-safe.

Format version `1` and state schema version `1` are the only versions this runtime reads. A different version, magic, or checksum is a hard error. There is no silent migration. A checked-in fixture at `cpp-runtime/tests/fixtures/golden-v1` is the compatibility sample: opening it must recover hash `dd22fb677cc125bf`.

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

Written to `meta.bin.tmp`, flushed, then atomically replaced over `meta.bin`. On POSIX the parent directory is fsynced after the rename, so a term or vote change survives power loss. A commit-index-only update skips that directory sync: any earlier durable copy still carries the latest term and vote.

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

A snapshot is state, not history. Format 2 (written today) holds the serialized state at one index: dancers, formations, cues, and one short line per accepted command id (the dedup table). Its size follows the state plus the number of accepted commands, not the length of the event log. Format 1 (every event that produced the state) is still read, so existing stores and the golden fixtures keep working.

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

A command accepted before a snapshot is still recognised after it, because the dedup table is part of the snapshot. A retry of a command whose event was compacted away gets `DuplicateCommand` rather than the original event, since the event text is history.

## Compaction

Cluster nodes set `compact_log`. After each snapshot the log is cut back to the older of the two retained snapshots; that snapshot's own record stays as an anchor so the next `AppendEntries` can still check its previous term. This gives a follower a lag margin of one to two snapshot intervals before it needs a snapshot, and guarantees the fallback snapshot still joins up with the log if the newest one is damaged. The single-node CLI does not compact, so replay to any index keeps working.

If the log starts after index 1 and no valid snapshot covers the gap, `open()` fails. That is data loss and is reported as such.

## Crash ordering

Each multi-file change is ordered so every intermediate on-disk state is one `open()` can reconcile:

- **Rewriting the log** builds the replacement in memory and swaps it in with an atomic replace. The old file is never removed first.
- **Installing a snapshot** saves the snapshot file, then the metadata commit index, then rewrites the log, then swaps the in-memory state. A snapshot newer than the metadata commit index is ignored on restart; once the metadata covers it, recovery restores from it and skips log records at or below it.
- **Truncating a conflicting log suffix** fsyncs the shortened file so the removed records cannot reappear.
