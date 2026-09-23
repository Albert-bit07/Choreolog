#pragma once

// Checksummed append-only log.
// A command is durable only after append() returns in sync mode.
// Incomplete tails are truncated. A bad checksum in a complete record is an error.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "choreoos/state/model.hpp"

namespace choreoos::storage {

inline constexpr std::uint32_t kMaxPayloadBytes = 1u << 20;

enum class Durability { Sync, Buffered };

struct WalStats {
  std::uint64_t append_ns = 0;
  std::uint64_t flush_ns = 0;
  std::uint64_t appends = 0;
  std::uint64_t truncated_tail_bytes = 0;
};

struct WalScan {
  std::vector<choreoos::state::Event> events;
  WalStats stats;
};

class WriteAheadLog {
 public:
  // Create wal.bin if needed, drop a torn tail, and load every valid record.
  [[nodiscard]] static choreoos::state::Result<WriteAheadLog> open(std::filesystem::path path);

  // Append one canonical event and, in Sync mode, flush it to disk.
  [[nodiscard]] choreoos::state::Result<void> append(const choreoos::state::Event& event,
                                                     Durability durability);

  // Drop every record with an index greater than `index`. Used for conflict repair.
  [[nodiscard]] choreoos::state::Result<void> truncate_after(choreoos::state::LogIndex index);

  [[nodiscard]] const std::vector<choreoos::state::Event>& events() const noexcept {
    return events_;
  }
  [[nodiscard]] const WalStats& stats() const noexcept { return stats_; }
  [[nodiscard]] choreoos::state::Result<choreoos::state::Event> at(
      choreoos::state::LogIndex index) const;

 private:
  explicit WriteAheadLog(std::filesystem::path path) : path_(std::move(path)) {}

  [[nodiscard]] choreoos::state::Result<void> load();

  std::filesystem::path path_;
  std::vector<choreoos::state::Event> events_;
  std::vector<std::uint64_t> record_ends_;
  WalStats stats_{};
};

}  // namespace choreoos::storage
