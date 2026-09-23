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

Written to `meta.bin.tmp`, flushed, then atomically replaced over `meta.bin`.

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

`<index>` is the 16-digit last included log index. The file is written under a `.tmp` name, flushed, then renamed. A `.tmp` file is ignored.

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | Magic `CHOSSNAP` |
| 8 | 2 | Format version `1` |
| 10 | 2 | State schema version `1` |
| 12 | 8 | Last included log index |
| 20 | 8 | Last included term |
| 28 | 16 | State hash, 16 hex characters |
| 44 | 4 | Payload length |
| 48 | N | Canonical event lines that built this state, separated by `\n` |
| 48+N | 4 | CRC-32 of the preceding bytes |

Recovery loads the newest snapshot whose checksum and schema match, replays its payload, and checks the state hash. It then applies only log records with a higher index. If every snapshot is invalid, recovery replays the whole log.
