#pragma once

#include <variant>

#include "choreoos/replication/core_replica.hpp"

namespace choreoos::replication {

// NodeServer depends on this small stable facade, rather than owning either
// implementation's consensus rules. Legacy remains the rollout default.
class RuntimeReplica {
 public:
  static state::Result<RuntimeReplica> open(ReplicaConfig config, bool transition_core);
  void set_sender(Replica::Sender sender);
  void set_commit_hook(std::function<void()> hook);
  void heartbeat();
  void handle(const protocol::Frame& frame, const std::string& peer);
  state::Result<EnqueueResult> enqueue(const state::Command& command);
  [[nodiscard]] const state::FileEngine& store() const;
  [[nodiscard]] const std::string& leader_id() const;
  [[nodiscard]] const std::string& committed_state_hash() const;
  [[nodiscard]] NodeStatus status() const;
  [[nodiscard]] const ConsensusMetrics& metrics() const;
  [[nodiscard]] const char* engine_name() const;
  [[nodiscard]] std::string failure_reason() const;

 private:
  RuntimeReplica(Replica replica, storage::DirectoryLease lease)
      : legacy_lease_(std::move(lease)), replica_(std::move(replica)) {}
  explicit RuntimeReplica(CoreReplica replica) : replica_(std::move(replica)) {}
  std::optional<storage::DirectoryLease> legacy_lease_;
  std::variant<Replica, CoreReplica> replica_;
};

}  // namespace choreoos::replication
