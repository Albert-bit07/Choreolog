#pragma once

// Replication plus a Raft-like election.
//
// elections == false keeps the Milestone 3 fixed leader: config.leader_id never
// changes, votes are refused, and an entry commits once a majority has stored
// it. That mode is not consensus.
//
// elections == true adds follower / candidate / leader, a persisted term and
// vote, and a current-term no-op so older entries commit only under a term the
// leader itself wrote. This is Raft-like. It is not a claim of full Raft.

#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "choreoos/protocol/frame.hpp"
#include "choreoos/protocol/messages.hpp"
#include "choreoos/state/store.hpp"

namespace choreoos::replication {

enum class Role { Follower, Candidate, Leader };

struct ReplicaConfig {
  std::string id;
  std::string leader_id;
  std::vector<std::string> peers;
  std::filesystem::path directory;
  // Default false so existing {id, leader, peers, directory} values stay
  // fixed-leader replicas.
  bool elections = false;
  std::uint64_t rng_seed = 1;
  int election_timeout_min = 3;
  int election_timeout_max = 6;
  std::uint64_t snapshot_every = 100;
  // A catch-up batch is bounded so one AppendEntries always fits in a frame
  // (protocol::kMaxFramePayload is 1 MiB). At least one entry is always sent.
  std::size_t max_append_entries = 512;
  std::size_t max_append_bytes = 256 * 1024;
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

// Counters a test or a status poll can read. They are not a metrics system.
struct ConsensusMetrics {
  std::uint64_t role_changes = 0;
  std::uint64_t elections_started = 0;
  std::uint64_t votes_granted = 0;
  std::uint64_t step_downs = 0;
  // Messages the replica refused to send because they exceed the frame limit.
  // A non-zero value means a peer cannot make progress through that message.
  std::uint64_t messages_dropped_oversize = 0;
};

class Replica {
 public:
  using Sender =
      std::function<void(const std::string& peer_id, const choreoos::protocol::Frame& frame)>;

  static choreoos::state::Result<Replica> open(ReplicaConfig config);

  void set_sender(Sender sender);
  void set_commit_hook(std::function<void()> hook);
  void set_election_bounds(int min_ticks, int max_ticks);
  [[nodiscard]] const std::string& id() const noexcept { return config_.id; }
  [[nodiscard]] bool is_leader() const noexcept;
  [[nodiscard]] const choreoos::state::FileEngine& store() const noexcept { return store_; }
  [[nodiscard]] choreoos::state::FileEngine& store() noexcept { return store_; }

  // Leader: append the command if it is new. Anyone else: return NotLeader.
  // `committed` is true only when a majority has the entry and this node applied it.
  choreoos::state::Result<EnqueueResult> enqueue(const choreoos::state::Command& command);
  void handle(const choreoos::protocol::Frame& frame);
  // One logical tick. Leaders replicate. Followers and candidates may campaign.
  void tick();
  void heartbeat();
  [[nodiscard]] NodeStatus status() const;
  // Cheap accessors for hot paths. status() serializes and hashes the whole state.
  [[nodiscard]] const std::string& leader_id() const noexcept { return config_.leader_id; }
  [[nodiscard]] const std::string& committed_state_hash() const noexcept {
    return store_.recovery().state_hash;
  }
  // O(1): the log entry carrying this command id, or nullptr.
  [[nodiscard]] const choreoos::state::Event* find_command(const std::string& command_id) const;
  [[nodiscard]] const ConsensusMetrics& metrics() const noexcept { return metrics_; }

 private:
  Replica(ReplicaConfig config, choreoos::state::FileEngine store,
          choreoos::state::ChoreographyState speculative);

  struct Peer {
    std::string id;
    std::uint64_t next_index = 1;
    std::uint64_t match_index = 0;
    std::uint64_t snapshot_index = 0;
  };

  void replicate(Peer& peer);
  void replicate_all();
  void on_append(const choreoos::protocol::AppendEntries& message, std::uint64_t correlation);
  void on_append_response(const choreoos::protocol::AppendEntriesResponse& message);
  void on_vote(const choreoos::protocol::RequestVote& message, std::uint64_t correlation);
  void on_vote_response(const choreoos::protocol::RequestVoteResponse& message);
  void on_install(const choreoos::protocol::InstallSnapshot& message, std::uint64_t correlation);
  void on_install_response(const choreoos::protocol::InstallSnapshotResponse& message);
  void advance_commit();
  void rebuild_speculative();
  // Followers never propose, so they only mark speculative state stale. It is
  // rebuilt when this node needs it (becoming leader, or a restamped term).
  void ensure_speculative();
  void rebuild_command_index();
  void start_election();
  void become_leader();
  bool step_down(std::uint64_t term);
  [[nodiscard]] choreoos::state::Result<void> persist_vote();
  [[nodiscard]] bool log_is_up_to_date(std::uint64_t last_term, std::uint64_t last_index) const;
  [[nodiscard]] int draw_timeout();
  [[nodiscard]] std::uint64_t next_random();
  void send(const std::string& peer, choreoos::protocol::MessageType type,
            std::uint64_t correlation, std::string payload);
  [[nodiscard]] const choreoos::state::Event* find_index(std::uint64_t index) const;
  [[nodiscard]] std::uint64_t last_index() const;
  [[nodiscard]] std::uint64_t last_term() const;
  [[nodiscard]] int majority() const;
  [[nodiscard]] const char* role_name() const noexcept;

  ReplicaConfig config_;
  choreoos::state::FileEngine store_;
  choreoos::state::ChoreographyState speculative_{};
  bool speculative_dirty_ = false;
  std::unordered_map<std::string, std::uint64_t> command_index_;  // command id -> log index
  std::vector<Peer> peers_;
  Sender sender_;
  std::function<void()> commit_hook_;
  std::uint64_t next_correlation_ = 1;
  Role role_ = Role::Follower;
  choreoos::state::Term current_term_ = choreoos::state::Term::initial();
  std::optional<std::string> voted_for_;
  std::vector<std::string> votes_from_;
  int election_elapsed_ = 0;
  int election_timeout_ = 3;
  std::uint64_t rng_ = 1;
  ConsensusMetrics metrics_{};
};

}  // namespace choreoos::replication
