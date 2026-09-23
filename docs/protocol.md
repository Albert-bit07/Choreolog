# Fixed-leader protocol

Milestone 3 copies one node's log to the others. The leader is chosen in the node config. There is no election, so this is replication, not consensus.

A client may send a command to any node. A follower answers `NotLeader` and names the leader. The leader appends the command to its log and sends `AppendEntries` to the followers. The entry commits only after a majority has stored it. Only then does every node apply it to the choreography state. The client receives success after that commit.

An empty `AppendEntries` is a heartbeat. It carries the leader's commit index so a follower that already has the entries can apply them.

If a follower is missing earlier entries, or has a different uncommitted suffix, the leader retries from an earlier index. The follower deletes only the uncommitted suffix, then stores the leader's entries. A committed prefix is never deleted.

Stopping one follower of three still leaves a majority. Those commands remain committed. When the follower starts again, it copies the missing entries and reaches the same state hash.

Vote and snapshot-install messages are part of the schema. Votes are not granted yet. Snapshot installation waits until log compaction exists. Until then, a restarted follower catches up from the log.

## Frame

Every TCP message is one frame: magic `CHOS`, protocol version 1.0, message type, correlation id, payload length, protobuf payload, and a CRC-32. A payload larger than 1 MiB is rejected. A bad checksum closes the connection.

Message layouts are declared in `proto/internal/choreoos.proto`. The runtime writes that protobuf wire format itself, so the node does not link a separate protobuf library. Zero-valued proto3 fields are omitted.
