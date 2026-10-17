#pragma once

#include <map>
#include <memory>

#include "choreoos/raft/file_engine_adapter.hpp"
#include "choreoos/storage/directory_lease.hpp"

namespace choreoos::raft {

struct PinnedSnapshot {
  SnapshotNeeded request;
  std::shared_ptr<const storage::Snapshot> snapshot;
};
struct HostEffects {
  std::vector<Outbound> send;
  std::vector<PinnedSnapshot> snapshots;
  bool capacity_exhausted = false;
  std::vector<ReadResult> reads;
};

// Synchronous storage/application host. One node strand owns this object.
// apply effects confirm already persisted FileEngine application; never replay
// them into Engine a second time. The network layer receives effects only after
// successful writes and application confirmation have been fully drained.
class RuntimeHost {
 public:
  static state::Result<RuntimeHost> open(Config config, std::filesystem::path directory,
                                         std::uint64_t snapshot_every = 100,
                                         bool test_mode = false);
  state::Result<HostEffects> dispatch(const Input& input,
                                      const storage::Snapshot* installing = nullptr);
  [[nodiscard]] const Node& node() const noexcept { return node_; }
  [[nodiscard]] const state::FileEngine& store() const { return adapter_.inspection_store(); }
  state::Result<void> inject_next_fault_for_test(DomainWriteFault fault) {
    return adapter_.inject_next_fault_for_test(fault);
  }

 private:
  RuntimeHost(Config config, storage::DirectoryLease lease, FileEngineAdapter adapter,
              std::uint64_t snapshot_every)
      : config_(std::move(config)),
        lease_(std::move(lease)),
        adapter_(std::move(adapter)),
        snapshot_every_(snapshot_every) {}
  state::Result<void> advance(const Input& input, HostEffects& output,
                              const storage::Snapshot* installing);
  state::Result<void> persist(const PersistRequest& request, const storage::Snapshot* installing);
  state::Result<void> collect(Effects effects, HostEffects& output);
  void halt();
  Config config_;
  storage::DirectoryLease lease_;
  FileEngineAdapter adapter_;
  Node node_;
  std::uint64_t snapshot_every_;
  std::map<std::string, PinnedSnapshot> pinned_;
  std::shared_ptr<const storage::Snapshot> published_;
};

}  // namespace choreoos::raft
