#include "choreoos/raft/durable_node.hpp"

#include <set>
#include <string>
#include <utility>

namespace choreoos::raft {
namespace {

state::Error error(std::string why) { return {state::ErrorCode::StoreError, std::move(why)}; }

bool valid_config(const Config& config) {
  if (config.self.empty()) return false;
  std::set<std::string> names{config.self};
  for (const auto& peer : config.peers) {
    if (peer.empty() || !names.insert(peer).second) return false;
  }
  return true;
}

bool acceptable_durable_vote(const Config& config, const Durable& durable) {
  if (!durable.voted_for) return true;
  if (*durable.voted_for == config.self) return true;
  for (const auto& peer : config.peers) {
    if (peer == *durable.voted_for) return true;
  }
  return false;
}

}  // namespace

state::Result<DurableNode> DurableNode::open(Config config, std::filesystem::path directory,
                                             CheckpointOptions options) {
  if (!valid_config(config)) return error("Raft durable driver has invalid cluster IDs");
  auto checkpoint = CheckpointStore::open(std::move(directory), options);
  if (!checkpoint) return checkpoint.error();
  if (!acceptable_durable_vote(config, checkpoint.value().current())) {
    return error("Recovered Raft vote does not belong to configured cluster");
  }
  auto node = initial(config);
  node.durable = checkpoint.value().current();
  return DurableNode{std::move(config), std::move(checkpoint.value()), std::move(node)};
}

DriveResult DurableNode::dispatch(const Input& input) {
  DriveResult result;
  if (std::holds_alternative<Restart>(input)) {
    auto recovered = checkpoint_.recover();
    if (recovered && !acceptable_durable_vote(config_, recovered.value())) {
      node_.pending.reset();
      node_.halted = true;
      result.effects.halt = true;
      result.error = error("Recovered Raft vote does not belong to configured cluster");
      return result;
    }
    if (!recovered) {
      node_.pending.reset();
      node_.halted = true;
      result.effects.halt = true;
      result.error = recovered.error();
      return result;
    }
    // Reconstruct from disk; no staged state, old request IDs or queued effects.
    node_ = initial(config_);
    node_.durable = std::move(recovered.value());
    return result;
  }

  // Never permit the caller to forge a disk completion or write-failure
  // callback. Only the internal checkpoint executor may generate those.
  if (std::holds_alternative<PersistCompleted>(input) ||
      std::holds_alternative<PersistFailed>(input)) {
    result.error = error("Persistence callbacks are private to the Raft storage driver");
    return result;
  }

  if (std::holds_alternative<ApplyCommitted>(input)) {
    // This checkpoint contains Raft metadata/log, not a choreography state
    // snapshot. Committing applied progress before state checkpoint is unsafe.
    result.error = error("ApplyCommitted requires atomic choreography checkpoint integration");
    return result;
  }

  auto [next, effects] = step(config_, std::move(node_), input);
  node_ = std::move(next);
  if (!effects.persist) {
    result.effects = std::move(effects);
    return result;
  }
  const auto request = *effects.persist;
  if (!effects.send_now.empty() || !effects.apply.empty()) {
    auto [halted, stopped] = step(config_, std::move(node_), PersistFailed{request.token});
    node_ = std::move(halted);
    result.effects = std::move(stopped);
    result.error = error("Raft core emitted dependent effects before durable write");
    return result;
  }
  auto saved = checkpoint_.persist(request.target);
  if (!saved) {
    auto [halted, stopped] = step(config_, std::move(node_), PersistFailed{request.token});
    node_ = std::move(halted);
    result.effects = std::move(stopped);
    result.error = saved.error();
    return result;
  }
  auto [durable, released] = step(config_, std::move(node_), PersistCompleted{request.token});
  node_ = std::move(durable);
  result.effects = std::move(released);
  return result;
}

}  // namespace choreoos::raft
