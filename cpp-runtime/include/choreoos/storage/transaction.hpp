#pragma once

// Recovery-atomic snapshot install. One durable redo record owns all three
// replacement images; recovery completes it before interpreting any store file.
#include <cstdint>
#include <filesystem>
#include <string>

#include "choreoos/state/error.hpp"

namespace choreoos::storage {

enum class InstallFault { None, AfterJournal, AfterSnapshot, AfterMetadata, AfterWal };

struct SnapshotTransaction {
  std::uint64_t index = 0;
  std::string snapshot;
  std::string metadata;
  std::string wal;
};

// Always syncs the decision and replacement files, including in buffered mode.
// A failure after the decision requires reopen/recovery, never further writes.
[[nodiscard]] state::Result<void> commit_snapshot_transaction(
    const std::filesystem::path& directory, const SnapshotTransaction& transaction,
    InstallFault fault = InstallFault::None);
[[nodiscard]] state::Result<void> recover_snapshot_transaction(
    const std::filesystem::path& directory);

}  // namespace choreoos::storage
