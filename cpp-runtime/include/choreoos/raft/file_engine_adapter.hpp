#pragma once

// Synchronous writer for production storage formats, used by RuntimeHost.
// The pure step function performs no I/O. One host owns the directory.
#include <memory>

#include "choreoos/raft/domain_bridge.hpp"

namespace choreoos::raft {

struct DomainWritePlan {
  std::uint64_t base_index = 0;
  std::uint64_t base_term = 0;
  std::uint64_t term = 1;
  std::optional<std::string> voted_for;
  EntryLog suffix;
  std::uint64_t commit = 0;
};

// Incremental host write: preserve through keep_index, then append only the
// changed suffix. Boundary tokens reject plans prepared before maintenance.
struct DomainWriteDelta {
  std::uint64_t base_index = 0;
  std::uint64_t base_term = 0;
  std::uint64_t term = 1;
  std::optional<std::string> voted_for;
  std::uint64_t keep_index = 0;
  std::vector<Entry> append;
  std::uint64_t commit = 0;
};

enum class DomainWriteFault {
  None,
  AfterMetadata,
  AfterTruncate,
  AfterAppend,
  AfterCommit,
  AppendBeforeFlush,
  AppendAfterFlush
};

class FileEngineAdapter {
 public:
  [[nodiscard]] static state::Result<FileEngineAdapter> open(std::filesystem::path directory,
                                                             bool test_mode = false);
  [[nodiscard]] state::Result<RecoveredDomainProjection> current() const;
  // Cheap consensus view; application payload serialization is unnecessary.
  [[nodiscard]] state::Result<Durable> current_raft() const;
  // Read-only management/proposal inspection. Consensus must use current_raft
  // and successful writes, never infer durability from this after a failure.
  [[nodiscard]] const state::FileEngine& inspection_store() const;
  [[nodiscard]] state::Result<std::shared_ptr<const RecoveredDomainProjection>> current_shared()
      const;
  [[nodiscard]] state::Result<void> persist_delta(const DomainWriteDelta& delta);
  // Explicit lifecycle boundary: snapshot committed state and compact to it.
  // Uncommitted entries and all durable retry receipts survive.
  [[nodiscard]] state::Result<void> checkpoint();
  [[nodiscard]] state::Result<void> install_snapshot(const storage::Snapshot& snapshot,
                                                     std::uint64_t term,
                                                     const std::optional<std::string>& vote);
  [[nodiscard]] bool uncertain() const noexcept { return uncertain_ || !store_; }

  // Validate completely before writing. A successful return is the only write
  // completion signal. Any storage/verification failure requires recover().
  [[nodiscard]] state::Result<void> persist(const DomainWritePlan& plan);
  [[nodiscard]] state::Result<RecoveredDomainProjection> recover();
  [[nodiscard]] state::Result<void> inject_next_fault_for_test(DomainWriteFault fault);

 private:
  FileEngineAdapter(std::filesystem::path directory, bool test_mode)
      : directory_(std::move(directory)), test_mode_(test_mode) {}
  std::filesystem::path directory_;
  bool test_mode_ = false;
  bool uncertain_ = true;
  DomainWriteFault fault_ = DomainWriteFault::None;
  std::unique_ptr<state::FileEngine> store_;
  RecoveredDomainProjection current_;
  mutable std::shared_ptr<const RecoveredDomainProjection> view_;
};

}  // namespace choreoos::raft
