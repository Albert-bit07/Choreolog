#include "choreoos/replication/runtime_replica.hpp"

#include <type_traits>

namespace choreoos::replication {

state::Result<RuntimeReplica> RuntimeReplica::open(ReplicaConfig config, bool transition_core) {
  if (transition_core) {
    auto replica = CoreReplica::open(std::move(config));
    if (!replica) return replica.error();
    return RuntimeReplica(std::move(replica.value()));
  }
  auto lease = storage::DirectoryLease::acquire(config.directory);
  if (!lease) return lease.error();
  auto replica = Replica::open(std::move(config));
  if (!replica) return replica.error();
  return RuntimeReplica(std::move(replica.value()), std::move(lease.value()));
}
void RuntimeReplica::set_sender(Replica::Sender sender) {
  std::visit([&](auto& replica) { replica.set_sender(std::move(sender)); }, replica_);
}
void RuntimeReplica::set_commit_hook(std::function<void()> hook) {
  std::visit([&](auto& replica) { replica.set_commit_hook(std::move(hook)); }, replica_);
}
void RuntimeReplica::heartbeat() {
  std::visit([](auto& replica) { replica.heartbeat(); }, replica_);
}
void RuntimeReplica::handle(const protocol::Frame& frame, const std::string& peer) {
  std::visit([&](auto& replica) { replica.handle(frame, peer); }, replica_);
}
state::Result<EnqueueResult> RuntimeReplica::enqueue(const state::Command& command) {
  return std::visit([&](auto& replica) { return replica.enqueue(command); }, replica_);
}
const state::FileEngine& RuntimeReplica::store() const {
  return std::visit([](const auto& replica) -> const state::FileEngine& { return replica.store(); },
                    replica_);
}
const std::string& RuntimeReplica::leader_id() const {
  return std::visit([](const auto& replica) -> const std::string& { return replica.leader_id(); },
                    replica_);
}
const std::string& RuntimeReplica::committed_state_hash() const {
  return std::visit(
      [](const auto& replica) -> const std::string& { return replica.committed_state_hash(); },
      replica_);
}
NodeStatus RuntimeReplica::status() const {
  return std::visit([](const auto& replica) { return replica.status(); }, replica_);
}
const ConsensusMetrics& RuntimeReplica::metrics() const {
  return std::visit(
      [](const auto& replica) -> const ConsensusMetrics& { return replica.metrics(); }, replica_);
}
std::string RuntimeReplica::failure_reason() const {
  return std::visit(
      [](const auto& replica) -> std::string {
        if constexpr (std::is_same_v<std::decay_t<decltype(replica)>, Replica>)
          return replica.halted() ? "legacy replica halted after storage failure" : "";
        else
          return replica.failure_reason();
      },
      replica_);
}
const char* RuntimeReplica::engine_name() const {
  return std::holds_alternative<CoreReplica>(replica_) ? "transition" : "legacy";
}

}  // namespace choreoos::replication
