#pragma once

// Pure Raft transition core, hosted by RuntimeHost/CoreReplica for real nodes.
// The host owns I/O and snapshot payloads; legacy Replica remains separate.
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "choreoos/raft/entry_log.hpp"

namespace choreoos::raft {

using PersistToken = std::uint64_t;
using RequestId = std::uint64_t;

enum class Role { Follower, Candidate, Leader };

struct Durable {
  std::uint64_t term = 0;
  std::optional<std::string> voted_for;
  EntryLog log;
  std::uint64_t commit = 0;
  std::uint64_t applied = 0;
  // log contains ONLY entries after this recovered snapshot boundary.
  // The host owns the actual verified snapshot state and retry receipts.
  std::uint64_t snapshot_index = 0;
  std::uint64_t snapshot_term = 0;
  bool operator==(const Durable&) const = default;
};

struct RequestVote {
  std::uint64_t term;
  std::string candidate;
  std::uint64_t last_index;
  std::uint64_t last_term;
  RequestId request_id;
};
struct VoteReply {
  std::uint64_t term;
  std::string voter;
  bool granted;
  RequestId request_id;
};
struct AppendEntries {
  std::uint64_t term;
  std::string leader;
  std::uint64_t prev_index;
  std::uint64_t prev_term;
  std::vector<Entry> entries;
  std::uint64_t leader_commit;
  RequestId request_id;
};
struct AppendReply {
  std::uint64_t term;
  std::string follower;
  bool success;
  std::uint64_t hint;
  RequestId request_id;
  std::uint64_t matched = 0;  // Exact prefix proved by the original request.
};

// The host validates/assembles the application snapshot. Incomplete chunks
// only establish term/leadership; ready requests install the verified boundary.
struct SnapshotRequest {
  std::uint64_t term;
  std::string leader;
  std::uint64_t index;
  std::uint64_t snapshot_term;
  RequestId request_id;
  bool ready = true;
};
struct SnapshotReply {
  std::uint64_t term;
  std::string follower;
  bool success;
  std::uint64_t index;
  RequestId request_id;
};

using WireMessage = std::variant<RequestVote, VoteReply, AppendEntries, AppendReply, SnapshotReply>;
struct Outbound {
  std::string to;
  WireMessage message;
};

// Work request to the future host, not an authenticated snapshot RPC or ack.
// Never fabricate compacted entries when ordinary AppendEntries cannot help.
struct SnapshotNeeded {
  std::string peer;
  std::uint64_t index = 0;
  std::uint64_t term = 0;
  RequestId request_id = 0;
};

struct Probe {
  RequestId request_id = 0;
  std::uint64_t term = 0;
  std::uint64_t prev = 0;
  std::uint64_t count = 0;
  std::optional<AppendEntries> request = {};  // Exact context retained across retries.
};
struct PeerProgress {
  std::uint64_t next = 1;
  std::uint64_t match = 0;
  std::optional<Probe> in_flight;
  std::optional<SnapshotNeeded> snapshot_in_flight = std::nullopt;
};

struct Volatile {
  Role role = Role::Follower;
  std::set<std::string> votes;
  std::map<std::string, PeerProgress> peers;
  std::uint64_t leadership_epoch = 0;
};

struct PendingWrite {
  PersistToken token;
  Durable target;
  std::vector<Outbound> after;
  // Applied commands are observable only after their application checkpoint.
  std::vector<Entry> apply_after;
  bool self_vote = false;
  std::vector<SnapshotNeeded> snapshot_after = {};
};

struct Node {
  Durable durable;  // Last acknowledged on-disk state, never staged.
  Volatile volatile_state;
  std::optional<PendingWrite> pending;
  PersistToken next_token = 1;
  RequestId next_request_id = 1;
  bool halted = false;
};

struct Config {
  std::string self;
  std::vector<std::string> peers;  // Other members; fixed configuration.
};

struct ElectionTimeout {};
struct HeartbeatTimeout {};
struct ClientPropose {
  std::string command;
};
struct RecvRequestVote {
  RequestVote message;
};
struct RecvVoteReply {
  VoteReply message;
};
struct RecvAppend {
  AppendEntries message;
};
struct RecvAppendReply {
  AppendReply message;
};
struct RecvSnapshot {
  SnapshotRequest message;
};
struct RecvSnapshotReply {
  SnapshotReply message;
};
struct AdvanceCommit {};
struct ApplyCommitted {};
// Local maintenance, only once application has durably caught up to commit.
struct CompactCommitted {};
struct PersistCompleted {
  PersistToken token;
};
struct PersistFailed {
  PersistToken token;
};
struct Restart {};

using Input =
    std::variant<ElectionTimeout, HeartbeatTimeout, ClientPropose, RecvRequestVote, RecvVoteReply,
                 RecvAppend, RecvAppendReply, AdvanceCommit, ApplyCommitted, CompactCommitted,
                 RecvSnapshot, RecvSnapshotReply, PersistCompleted, PersistFailed, Restart>;

enum class PersistKind { StateUpdate, LocalCompaction, SnapshotInstall };

struct PersistRequest {
  PersistToken token;
  Durable target;
  // LocalCompaction requires the matching application snapshot and WAL
  // maintenance, not just persistence of the Raft metadata/log suffix.
  PersistKind kind = PersistKind::StateUpdate;
};
struct Effects {
  std::optional<PersistRequest> persist;
  std::vector<Outbound> send_now;
  std::vector<Entry> apply;         // Released after PersistCompleted only.
  bool halt = false;                // Fail-stop on failed durable write.
  bool capacity_exhausted = false;  // Host must checkpoint before accepting more proposals.
  std::vector<SnapshotNeeded> snapshot_needed = {};
};

[[nodiscard]] Node initial(const Config& config);
[[nodiscard]] std::pair<Node, Effects> step(const Config& config, Node state, const Input& input);

}  // namespace choreoos::raft
