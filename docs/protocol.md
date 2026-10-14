# Replication protocol: fixed-leader and optional Raft-like elections

By default (`elections: false`), the configured leader copies its log to the other nodes. There is no leader election in this mode; it is fixed-leader replication, **not** consensus. With `elections: true`, nodes instead maintain follower/candidate/leader roles, persisted terms and votes, and a Raft-like election protocol. Neither mode is claimed to be fully Raft verified.

A client may send a command to any node. A follower answers `NotLeader` and names the leader. The leader appends the command to its log and sends `AppendEntries` to the followers. The entry commits only after a majority has stored it. Only then does every node apply it to the choreography state. The client receives success after that commit.

An empty `AppendEntries` is a heartbeat. It carries the leader's commit index so a follower that already has the entries can apply them.

If a follower is missing earlier entries, or has a different uncommitted suffix, the leader retries from an earlier index. The follower deletes only the uncommitted suffix, then stores the leader's entries. A committed prefix is never deleted.

Stopping one follower of three still leaves a majority. Those commands remain committed. When the follower starts again, it copies the missing entries and reaches the same state hash.

A catch-up `AppendEntries` is bounded by entry count and bytes (`max_append_entries`, `max_append_bytes`), so it always fits in one frame. After each acknowledgement the leader sends the next batch immediately instead of waiting a heartbeat. A replica never emits a message over the frame limit; it counts it in `messages_dropped_oversize` instead of failing silently.

## Snapshots

When a follower is behind the leader's compacted log, the leader sends its snapshot with `InstallSnapshot`. The payload is split into chunks of at most `max_snapshot_chunk_bytes`. Each chunk carries its `offset` and a `more` flag; the follower acknowledges with `next_offset`, the offset it expects next, and the leader continues from there. A chunk the follower already holds is acknowledged again without effect, an out-of-order or mismatched chunk answers `next_offset = 0` so the leader restarts, and a newer snapshot on the leader restarts the transfer. Nothing is applied until the final chunk arrives and the whole payload verifies against the snapshot's state hash. A message with offset 0 and `more` false carries the entire payload.

## Optional Raft-like election mode

Votes are granted only when `elections` is enabled in the node config.
Election attempts durably record the new term and self-vote before RequestVote messages are sent. A follower checks the candidate's last log term and index and persists a granted vote before replying. On leadership, the current production `Replica` appends a no-op entry in its new term. Commit advancement counts a majority only for entries from the current term; prior-term entries may become committed indirectly through a later current-term entry.

The C++ runtime in `cpp-runtime/src/replication/replica.cpp` currently owns election decisions, disk operations and send effects. PR #10's standalone `choreoos::raft::step` is **experimental** and is not called by the production TCP node. Formal model checking applies to scoped TLA+ configurations, not to the deployed C++ implementation; see [verification status](raft-verification.md).

The TCP peer-config gate is not cryptographic peer authentication. Do not assume the protocol resists spoofing or Byzantine participants.


## Frame

Every TCP message is one frame: magic `CHOS`, protocol version 1.0, message type, correlation id, payload length, protobuf payload, and a CRC-32. A payload larger than 1 MiB is rejected. A bad checksum closes the connection.

Message layouts are declared in `proto/internal/choreoos.proto`. The runtime writes that protobuf wire format itself, so the node does not link a separate protobuf library. Zero-valued proto3 fields are omitted.
