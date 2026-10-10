# Fixed-leader protocol

Milestone 3 copies one node's log to the others. The leader is chosen in the node config. There is no election, so this is replication, not consensus.

A client may send a command to any node. A follower answers `NotLeader` and names the leader. The leader appends the command to its log and sends `AppendEntries` to the followers. The entry commits only after a majority has stored it. Only then does every node apply it to the choreography state. The client receives success after that commit.

An empty `AppendEntries` is a heartbeat. It carries the leader's commit index so a follower that already has the entries can apply them.

If a follower is missing earlier entries, or has a different uncommitted suffix, the leader retries from an earlier index. The follower deletes only the uncommitted suffix, then stores the leader's entries. A committed prefix is never deleted.

Stopping one follower of three still leaves a majority. Those commands remain committed. When the follower starts again, it copies the missing entries and reaches the same state hash.

A catch-up `AppendEntries` is bounded by entry count and bytes (`max_append_entries`, `max_append_bytes`), so it always fits in one frame. After each acknowledgement the leader sends the next batch immediately instead of waiting a heartbeat. A replica never emits a message over the frame limit; it counts it in `messages_dropped_oversize` instead of failing silently.

## Snapshots

When a follower is behind the leader's compacted log, the leader sends its snapshot with `InstallSnapshot`. The payload is split into chunks of at most `max_snapshot_chunk_bytes`. Each chunk carries its `offset` and a `more` flag; the follower acknowledges with `next_offset`, the offset it expects next, and the leader continues from there. A chunk the follower already holds is acknowledged again without effect, an out-of-order or mismatched chunk answers `next_offset = 0` so the leader restarts, and a newer snapshot on the leader restarts the transfer. Nothing is applied until the final chunk arrives and the whole payload verifies against the snapshot's state hash. A message with offset 0 and `more` false carries the entire payload.

Votes are granted only when `elections` is enabled in the node config.

## Frame

Every TCP message is one frame: magic `CHOS`, protocol version 1.0, message type, correlation id, payload length, protobuf payload, and a CRC-32. A payload larger than 1 MiB is rejected. A bad checksum closes the connection.

Message layouts are declared in `proto/internal/choreoos.proto`. The runtime writes that protobuf wire format itself, so the node does not link a separate protobuf library. Zero-valued proto3 fields are omitted.
