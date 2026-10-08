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
  // After each snapshot, drop WAL records below the OLDER of the two retained
  // snapshots (that snapshot's own record stays as the anchor for prev-term
  // lookups). Keeping the log back to the older snapshot gives followers a lag
  // margin of one to two snapshot intervals, and guarantees that if the newest
  // snapshot is damaged the fallback still joins up with the log. Cluster nodes
  // set this; the CLI keeps the full history so replay and inspection can reach
  // any index.
  bool compact_log = false;
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
  // O(1) lookups. The log is contiguous, so an index maps to a vector offset.
  [[nodiscard]] const Event* log_entry(std::uint64_t index) const;
  // Last log index, or the applied index when the log holds no records.
  [[nodiscard]] std::uint64_t last_log_index() const;
  // First index whose record is still in the log. Anything below it was
  // compacted and is only available through the snapshot.
  [[nodiscard]] std::uint64_t log_first_index() const;
  // Term of the entry at `index`, including the snapshot boundary after the
  // log was emptied. Empty when that term is no longer known (compacted).
  [[nodiscard]] std::optional<std::uint64_t> log_term_at(std::uint64_t index) const;
  [[nodiscard]] LogIndex commit_index() const noexcept { return commit_index_; }
  Result<void> append_event(const Event& event);
  Result<void> truncate_after(LogIndex index);
  Result<void> commit_through(LogIndex index);

  // Election term and vote. Written before a node answers a vote or steps down.
  // A higher term is kept even when the applied log term is still behind.
  [[nodiscard]] storage::NodeMetadata consensus_metadata() const;
  Result<void> persist_consensus(Term term, std::optional<NodeId> voted_for);

  // Snapshot catch-up. latest_snapshot() is the state a lagging peer installs.
  [[nodiscard]] const std::optional<storage::Snapshot>& latest_snapshot() const noexcept {
    return snapshot_;
  }
  Result<void> install_snapshot(const storage::Snapshot& snapshot);
  // Explicitly drop log records below the newest snapshot (its own record
  // stays). Unlike automatic compaction this leaves no fallback snapshot
  // coverage. Applied state stays.
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
  Result<void> maybe_snapshot_unlocked();
  Result<void> take_snapshot_unlocked();
  // Drop log records below `anchor`; the anchor's own record stays.
  Result<void> compact_unlocked(std::uint64_t anchor);

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
