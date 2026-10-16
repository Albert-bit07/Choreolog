#pragma once

#include <map>
#include <unordered_map>

#include "choreoos/raft/runtime_host.hpp"
#include "choreoos/replication/replica.hpp"

namespace choreoos::replication {

// Production transport facade for the transition-core backend. Synchronous
// durable/application ownership lives in RuntimeHost; this layer owns clocks,
// canonical client proposals and correlated bounded snapshot streams.
class CoreReplica {
 public:
  static state::Result<CoreReplica> open(ReplicaConfig config);
  using Sender = Replica::Sender;
  void set_sender(Sender sender) { sender_ = std::move(sender); }
  void set_commit_hook(std::function<void()> hook) { commit_hook_ = std::move(hook); }
  void heartbeat();
  void handle(const protocol::Frame& frame, const std::string& transport_peer);
  state::Result<EnqueueResult> enqueue(const state::Command& command);
  [[nodiscard]] const state::FileEngine& store() const { return host_.store(); }
  [[nodiscard]] const std::string& leader_id() const noexcept { return leader_id_; }
  [[nodiscard]] const std::string& committed_state_hash() const {
    return store().recovery().state_hash;
  }
  [[nodiscard]] NodeStatus status() const;
  [[nodiscard]] std::string failure_reason() const {
    return failure_ ? failure_->message() : std::string{};
  }
  [[nodiscard]] const ConsensusMetrics& metrics() const noexcept { return metrics_; }

 private:
  struct Transfer {
    raft::PinnedSnapshot pinned;
    protocol::Frame frame;
    std::uint64_t end = 0;
    bool final = false;
  };
  struct Incoming {
    std::string leader;
    std::uint64_t term = 0;
    std::uint64_t index = 0;
    std::uint64_t snapshot_term = 0;
    std::string hash;
    std::string bytes;
  };
  CoreReplica(ReplicaConfig config, raft::RuntimeHost host);
  state::Result<raft::HostEffects> drive(const raft::Input& input,
                                         const storage::Snapshot* installing = nullptr);
  void release(raft::HostEffects output, std::uint64_t snapshot_ack_bytes = 0);
  void emit(const raft::Outbound& outbound, std::uint64_t snapshot_ack_bytes);
  void send(const std::string& peer, const protocol::Frame& frame);
  void send_chunk(const std::string& peer, std::uint64_t offset);
  void on_snapshot(const protocol::InstallSnapshot& message, std::uint64_t correlation);
  void on_snapshot_reply(const protocol::InstallSnapshotResponse& message,
                         std::uint64_t correlation);
  void reply_chunk(const std::string& peer, std::uint64_t correlation, bool success,
                   std::uint64_t next_offset);
  state::Result<state::Event> noop();
  state::Result<void> ensure_speculative();
  void synchronize_log(const raft::Durable& before);
  int draw_timeout();
  bool known_peer(const std::string& peer) const;
  bool is_leader() const;
  ReplicaConfig config_;
  raft::RuntimeHost host_;
  std::string leader_id_;
  Sender sender_;
  std::function<void()> commit_hook_;
  ConsensusMetrics metrics_;
  std::uint64_t rng_ = 1;
  std::uint64_t snapshot_correlation_ = 1;
  int elapsed_ = 0;
  int timeout_ = 3;
  bool speculative_dirty_ = true;
  bool index_initialized_ = false;
  state::ChoreographyState speculative_;
  std::unordered_map<std::string, std::uint64_t> command_index_;
  std::map<std::string, Transfer> outgoing_;
  std::optional<Incoming> incoming_;
  std::optional<state::Error> failure_;
};

}  // namespace choreoos::replication
