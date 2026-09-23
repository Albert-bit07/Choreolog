#pragma once

// Fixed-leader replication. This is not consensus and does not elect a leader.
// The configured leader appends to its log, copies entries to followers, and
// advances the commit index only after a majority has stored them. Every node
// applies an entry only once that commit index covers it.

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "choreoos/protocol/frame.hpp"
#include "choreoos/protocol/messages.hpp"
#include "choreoos/state/store.hpp"

namespace choreoos::replication {

struct ReplicaConfig {
  std::string id;
  std::string leader_id;
  std::vector<std::string> peers;
  std::filesystem::path directory;
};

struct EnqueueResult {
  choreoos::state::Event event;
  bool duplicate = false;
  bool committed = false;
  std::string leader_id;
};

struct NodeStatus {
  std::string node_id;
  std::string leader_id;
  std::string role;
  std::uint64_t term = 1;
  std::uint64_t commit_index = 0;
  std::uint64_t last_log_index = 0;
  std::string state_hash;
  std::string canonical_state;
};

class Replica {
 public:
  using Sender =
      std::function<void(const std::string& peer_id, const choreoos::protocol::Frame& frame)>;

  static choreoos::state::Result<Replica> open(ReplicaConfig config);

  void set_sender(Sender sender);
  void set_commit_hook(std::function<void()> hook);
  [[nodiscard]] const std::string& id() const noexcept { return config_.id; }
  [[nodiscard]] bool is_leader() const noexcept { return config_.id == config_.leader_id; }
  [[nodiscard]] const choreoos::state::FileEngine& store() const noexcept { return store_; }

  // Leader: append the command if it is new. Follower: return NotLeader.
  // `committed` is true only when a majority has the entry and this node applied it.
  choreoos::state::Result<EnqueueResult> enqueue(const choreoos::state::Command& command);
  void handle(const choreoos::protocol::Frame& frame);
  void heartbeat();
  [[nodiscard]] NodeStatus status() const;

 private:
  Replica(ReplicaConfig config, choreoos::state::FileEngine store,
          choreoos::state::ChoreographyState speculative);

  struct Peer {
    std::string id;
    std::uint64_t next_index = 1;
    std::uint64_t match_index = 0;
  };

  void replicate(Peer& peer);
  void replicate_all();
  void on_append(const choreoos::protocol::AppendEntries& message, std::uint64_t correlation);
  void on_append_response(const choreoos::protocol::AppendEntriesResponse& message);
  void advance_commit();
  void rebuild_speculative();
  void send(const std::string& peer, choreoos::protocol::MessageType type,
            std::uint64_t correlation, std::string payload);
  [[nodiscard]] const choreoos::state::Event* find_index(std::uint64_t index) const;
  [[nodiscard]] std::uint64_t last_index() const;
  [[nodiscard]] int majority() const;

  ReplicaConfig config_;
  choreoos::state::FileEngine store_;
  choreoos::state::ChoreographyState speculative_{};
  std::vector<Peer> peers_;
  Sender sender_;
  std::function<void()> commit_hook_;
  std::uint64_t next_correlation_ = 1;
};

}  // namespace choreoos::replication
