# ChoreoRaft persistence correspondence audit (PR #9)

Status: **design review only, not a verified refinement of the deployed C++ runtime.**
Reviewed the `main` implementations of `storage/{metadata,wal,file_util}.cpp`
and `state/store.cpp` on 2026-10-13. This does **not** establish that all
reachable failures, operating systems or filesystems implement the model.

## Concrete storage paths

| Actual C++ operation | Observed persistence behavior | Consequence for ChoreoRaft |
| --- | --- | --- |
| `FileEngine::persist_consensus` | Writes term and votedFor together in `meta.bin` using a temp file, flush, replace and (POSIX when sync requested) parent-directory fsync | The model's combined term/vote stage is a useful abstraction for an individual successful sync-mode metadata update |
| `FileEngine::append_event` / `WriteAheadLog::append` | Appends a CRC-protected record; a torn record can survive as an incomplete tail, which recovery truncates | A log write is not inherently an atomic whole-log replacement. The model's all-or-nothing log stage needs a recovered-WAL-prefix interpretation and test support |
| `WriteAheadLog::rewrite` | Rewrites to a new file and atomically replaces the WAL after syncing | Whole-file replacement is a distinct operation from append and truncation |
| `WriteAheadLog::truncate_after` | Shrinks the WAL and syncs its file before accepting the new suffix | Requires its own crash-boundary test for conflicting suffix repair |
| `FileEngine::commit_through` | Applies events **in memory first**, then updates `meta.bin` commit index; a snapshot may follow | The committed application state is not durably advanced per event just because the in-memory engine applied it |
| `FileEngine::take_snapshot_unlocked` / `install_snapshot` | Saves durable state snapshots. Install orders snapshot save, metadata commit update, then WAL rewrite | `disk.applied` is an abstract durability variable and **does not yet directly map** to a per-command persisted C++ field |

## Assumptions that remain unproved

1. `Torn(old,new)` assumes the permitted crash outcomes are the *prefixes*
   term+vote, log, commit, applied. The C++ operations are not one universal
   multi-file transaction. Each API call has its own write sequence and
   explicit persistence barrier. In particular, term/vote/commit share the
   metadata file rather than being independent disk records.
2. Sync-mode durability depends on successful OS flush/sync and atomic rename
   semantics. Buffered mode is explicitly weaker and must be excluded from
   crash-safety conformance claims.
3. `mem` vs. `disk` and `PersistCompleted` are **proposed V2 interfaces**.
   They are not evidence that every existing C++ network reply or client
   acknowledgement is currently gated on a recorded disk completion.
4. The model does not include snapshots, compaction, WAL checksums, file
   replacement failures, write reordering across independent operations, or
   retained `meta.bin.tmp` files.

## Next implementation tests (not yet claimed passing)

- Restart after injected pre-flush and post-flush errors for term/vote writes.
- Crash around WAL append: complete record, truncated tail and checksum
  corruption; compare recovered log prefix to modeled outcomes.
- Crash around conflict truncation and WAL rewrite; assert committed prefix
  preservation and up-to-date-log voting.
- Crash after in-memory application but before metadata commit replacement;
  assert no acknowledgment was released and recovery replays correctly.
- Crash at each snapshot-install step (snapshot saved, metadata committed,
  WAL replaced); verify reconciliation and application progress.
- Run these under sync mode on Linux and Windows; document platform-specific
  parent-directory durability assumptions.

Do not merge a claim that `Torn` formally captures deployed crash behavior
without the missing tests, trace correspondence and revised abstraction. The
new bounded TLC profiles verify the **modeled algorithm under its documented
assumptions**, not implementation/storage refinement.
