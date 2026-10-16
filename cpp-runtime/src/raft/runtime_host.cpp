#include "choreoos/raft/runtime_host.hpp"

#include <exception>
#include <iterator>
#include <set>

namespace choreoos::raft {
namespace {
state::Error invalid(std::string message) {
  return {state::ErrorCode::StoreError, std::move(message)};
}
}  // namespace

state::Result<RuntimeHost> RuntimeHost::open(Config config, std::filesystem::path directory,
                                             std::uint64_t snapshot_every, bool test_mode) {
  std::set<std::string> members;
  if (!state::NodeId::parse(config.self) || directory.empty())
    return invalid("runtime host requires a valid local ID and data directory");
  for (const auto& peer : config.peers)
    if (peer == config.self || !state::NodeId::parse(peer) || !members.insert(peer).second)
      return invalid("runtime host requires distinct valid peer IDs");
  auto lease = storage::DirectoryLease::acquire(directory);
  if (!lease) return lease.error();
  auto adapter = FileEngineAdapter::open(std::move(directory), test_mode);
  if (!adapter) return adapter.error();
  auto recovered = adapter.value().current_raft();
  if (!recovered) return recovered.error();
  RuntimeHost host(std::move(config), std::move(lease.value()), std::move(adapter.value()),
                   snapshot_every);
  host.node_ = initial(host.config_);
  host.node_.durable = std::move(recovered.value());
  auto [validated, effects] = step(host.config_, host.node_, Restart{});
  if (effects.halt) return invalid("recovered state violates transition-core invariants");
  host.node_ = std::move(validated);
  return host;
}

void RuntimeHost::halt() {
  if (node_.pending) {
    const auto token = node_.pending->token;
    auto failed = step(config_, std::move(node_), PersistFailed{token});
    node_ = std::move(failed.first);
  }
  node_.halted = true;
  node_.volatile_state = initial(config_).volatile_state;
  pinned_.clear();
  published_.reset();
}

state::Result<void> RuntimeHost::persist(const PersistRequest& request,
                                         const storage::Snapshot* installing) {
  const auto& target = request.target;
  if (request.kind == PersistKind::LocalCompaction) {
    auto checkpoint = adapter_.checkpoint();
    if (!checkpoint) return checkpoint.error();
  } else if (request.kind == PersistKind::SnapshotInstall) {
    if (!installing || installing->index.value() != target.snapshot_index ||
        installing->term.value() != target.snapshot_term)
      return invalid("snapshot intent lacks its matching verified application snapshot");
    auto installed = adapter_.install_snapshot(*installing, target.term, target.voted_for);
    if (!installed) return installed.error();
  } else {
    auto same = target;
    same.applied = node_.durable.applied;
    // FileEngine durably applies at commit. ApplyCommitted merely confirms
    // that existing durable result; it must not write or apply the event twice.
    if (same != node_.durable) {
      if (target.snapshot_index != node_.durable.snapshot_index ||
          target.snapshot_term != node_.durable.snapshot_term)
        return invalid("ordinary persistence cannot change the snapshot boundary");
      const auto common = node_.durable.log.common_prefix_size(target.log);
      DomainWriteDelta delta;
      delta.base_index = target.snapshot_index;
      delta.base_term = target.snapshot_term;
      delta.term = target.term;
      delta.voted_for = target.voted_for;
      delta.keep_index = target.snapshot_index + common;
      delta.commit = target.commit;
      for (auto offset = common; offset < target.log.size(); ++offset)
        delta.append.push_back(target.log[offset]);
      auto written = adapter_.persist_delta(delta);
      if (!written) return written.error();
    }
  }
  auto observed = adapter_.current_raft();
  if (!observed) return observed.error();
  auto expected = target;
  expected.applied = observed.value().applied;
  if (observed.value() != expected || observed.value().applied != target.commit ||
      target.applied > observed.value().applied)
    return invalid("runtime persistence disagrees with the requested consensus/application state");
  return {};
}

state::Result<void> RuntimeHost::collect(Effects effects, HostEffects& output) {
  if (effects.halt) return invalid("transition core halted");
  output.capacity_exhausted = output.capacity_exhausted || effects.capacity_exhausted;
  output.send.insert(output.send.end(), std::make_move_iterator(effects.send_now.begin()),
                     std::make_move_iterator(effects.send_now.end()));
  for (const auto& needed : effects.snapshot_needed) {
    auto found = pinned_.find(needed.peer);
    if (found == pinned_.end() || found->second.request.request_id != needed.request_id) {
      const auto& snapshot = store().latest_snapshot();
      if (!snapshot || snapshot->legacy() || snapshot->index.value() != needed.index ||
          snapshot->term.value() != needed.term || snapshot->state_payload.size() > (64u << 20))
        return invalid("snapshot work has no matching bounded durable application snapshot");
      if (!published_ || published_->index != snapshot->index ||
          published_->state_hash != snapshot->state_hash)
        published_ = std::make_shared<const storage::Snapshot>(*snapshot);
      found = pinned_.insert_or_assign(needed.peer, PinnedSnapshot{needed, published_}).first;
    }
    output.snapshots.push_back(found->second);
  }
  return {};
}

state::Result<void> RuntimeHost::advance(const Input& input, HostEffects& output,
                                         const storage::Snapshot* installing) {
  auto [next, effects] = step(config_, std::move(node_), input);
  node_ = std::move(next);
  while (effects.persist) {
    const auto request = *effects.persist;
    auto immediate = collect(std::move(effects), output);
    if (!immediate) return immediate.error();
    auto written = persist(request, installing);
    if (!written) return written.error();
    auto completed = step(config_, std::move(node_), PersistCompleted{request.token});
    node_ = std::move(completed.first);
    effects = std::move(completed.second);
  }
  return collect(std::move(effects), output);
}

state::Result<HostEffects> RuntimeHost::dispatch(const Input& input,
                                                 const storage::Snapshot* installing) try {
  if (node_.halted) return invalid("runtime host halted; reopen from actual disk recovery");
  HostEffects output;
  auto applied = advance(input, output, installing);
  if (applied) applied = advance(AdvanceCommit{}, output, nullptr);
  while (applied && node_.durable.applied < node_.durable.commit)
    applied = advance(ApplyCommitted{}, output, nullptr);
  if (applied && snapshot_every_ != 0 &&
      node_.durable.commit - node_.durable.snapshot_index >= snapshot_every_)
    applied = advance(CompactCommitted{}, output, nullptr);
  if (!applied) {
    halt();
    return applied.error();
  }
  for (auto it = pinned_.begin(); it != pinned_.end();) {
    const auto peer = node_.volatile_state.peers.find(it->first);
    bool active = false;
    if (peer != node_.volatile_state.peers.end()) {
      const auto& probe = peer->second.snapshot_in_flight;
      active = probe && probe->request_id == it->second.request.request_id;
    }
    if (!active)
      it = pinned_.erase(it);
    else
      ++it;
  }
  return output;
} catch (const std::exception& error) {
  halt();
  return invalid(std::string("runtime host failure: ") + error.what());
}

}  // namespace choreoos::raft
