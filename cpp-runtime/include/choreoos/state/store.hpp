#pragma once

// Durable directory store.
// submit() appends a checksummed log record and flushes it before returning.
// open() restores the newest valid snapshot, then replays only later records.

#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "choreoos/state/machine.hpp"
#include "choreoos/storage/metadata.hpp"
#include "choreoos/storage/snapshot.hpp"
#include "choreoos/storage/wal.hpp"

namespace choreoos::state {

struct StoreOptions {
  std::uint64_t snapshot_every = 100;  // 0 disables automatic snapshots
  storage::Durability durability = storage::Durability::Sync;
  // Single-node CLI commits every appended record. A cluster node sets this
  // false so entries stay uncommitted until a majority has them.
  bool commit_on_append = true;
  // Fault injection stays off unless a test opts in. The node binary never sets this.
  bool test_mode = false;
};

// One-shot storage fault. Armed only while test_mode is true, then cleared.
enum class StorageFault { None, FailBeforeFlush, FailAfterFlush };

struct RecoveryReport {
  bool used_snapshot = false;
  std::uint64_t snapshot_index = 0;
  std::uint64_t replayed_after_snapshot = 0;
  std::uint64_t truncated_tail_bytes = 0;
  std::uint64_t commit_index = 0;
  std::string state_hash;
};

[[nodiscard]] Result<Event> parse_event(std::string_view line);

class FileEngine {
 public:
  static Result<FileEngine> open(std::filesystem::path directory, StoreOptions options = {});

  Result<SubmitResult> submit(Command command);
  Result<void> checkpoint();

  // Cluster log. These do not apply an entry until commit_through().
  [[nodiscard]] const std::vector<Event>& log_events() const;
  [[nodiscard]] LogIndex commit_index() const noexcept { return commit_index_; }
  Result<void> append_event(const Event& event);
  Result<void> truncate_after(LogIndex index);
  Result<void> commit_through(LogIndex index);

  // Election term and vote. Written before a node answers a vote or steps down.
  // A higher term is kept even when the applied log term is still behind.
  [[nodiscard]] storage::NodeMetadata consensus_metadata() const;
  Result<void> persist_consensus(Term term, std::optional<NodeId> voted_for);

  // Snapshot catch-up. The payload is the same event list a .snap file stores.
  [[nodiscard]] const std::optional<storage::Snapshot>& latest_snapshot() const noexcept {
    return snapshot_;
  }
  Result<void> install_snapshot(const storage::Snapshot& snapshot);
  // Drop log records covered by the newest snapshot. Applied state stays.
  Result<void> discard_compacted_prefix();

  [[nodiscard]] const Engine& engine() const noexcept { return engine_; }
  [[nodiscard]] const RecoveryReport& recovery() const noexcept { return recovery_; }
  [[nodiscard]] const std::filesystem::path& directory() const noexcept { return directory_; }
  [[nodiscard]] const storage::WalStats& wal_stats() const;

  // Returns an error when test_mode is false. The next append consumes the fault.
  Result<void> arm_storage_fault(StorageFault fault);

 private:
  Result<void> fail_before_flush_unlocked();
  Result<void> fail_after_flush_unlocked();
  FileEngine(std::filesystem::path directory, StoreOptions options);

  std::filesystem::path directory_;
  StoreOptions options_;
  std::unique_ptr<std::mutex> mutex_;
  std::unique_ptr<storage::WriteAheadLog> wal_;
  Engine engine_{};
  RecoveryReport recovery_{};
  LogIndex commit_index_ = LogIndex::none();
  std::optional<storage::Snapshot> snapshot_;
  StorageFault fault_ = StorageFault::None;
};

[[nodiscard]] std::filesystem::path event_log_path(const std::filesystem::path& directory);

}  // namespace choreoos::state
