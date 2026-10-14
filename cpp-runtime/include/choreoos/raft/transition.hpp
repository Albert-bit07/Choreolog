#pragma once

// Experimental, pure Raft transition core. This does NOT yet drive Replica.
// The host owns storage, networking, clocks and snapshot installation.
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace choreoos::raft {

using PersistToken = std::uint64_t;
using RequestId = std::uint64_t;

enum class Role { Follower, Candidate, Leader };

struct Entry {
  std::uint64_t term = 0;
  std::string command;
  bool operator==(const Entry&) const = default;
};

struct Durable {
  std::uint64_t term = 0;
  std::optional<std::string> voted_for;
  std::vector<Entry> log;
  std::uint64_t commit = 0;
  std::uint64_t applied = 0;
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
};

using WireMessage = std::variant<RequestVote, VoteReply, AppendEntries, AppendReply>;
struct Outbound {
  std::string to;
  WireMessage message;
};

struct Probe {
  RequestId request_id = 0;
  std::uint64_t term = 0;
  std::uint64_t prev = 0;
  std::uint64_t count = 0;
};
struct PeerProgress {
  std::uint64_t next = 1;
  std::uint64_t match = 0;
  std::optional<Probe> in_flight;
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
struct AdvanceCommit {};
struct ApplyCommitted {};
struct PersistCompleted {
  PersistToken token;
};
struct PersistFailed {
  PersistToken token;
};
struct Restart {};

using Input = std::variant<ElectionTimeout, HeartbeatTimeout, ClientPropose, RecvRequestVote,
                           RecvVoteReply, RecvAppend, RecvAppendReply, AdvanceCommit,
                           ApplyCommitted, PersistCompleted, PersistFailed, Restart>;

struct PersistRequest {
  PersistToken token;
  Durable target;
};
struct Effects {
  std::optional<PersistRequest> persist;
  std::vector<Outbound> send_now;
  std::vector<Entry> apply;  // Released after PersistCompleted only.
  bool halt = false;         // Fail-stop on failed durable write.
};

[[nodiscard]] Node initial(const Config& config);
[[nodiscard]] std::pair<Node, Effects> step(const Config& config, Node state, const Input& input);

}  // namespace choreoos::raft
