#pragma once

// Isolated Raft-core durability checkpoint. This does not replace FileEngine
// or the production choreography WAL/snapshot representation.
#include <filesystem>

#include "choreoos/raft/transition.hpp"
#include "choreoos/state/error.hpp"

namespace choreoos::raft {

enum class CheckpointFault { None, BeforeReplace, AfterReplace };

struct CheckpointOptions {
  bool test_mode = false;
};

class CheckpointStore {
 public:
  [[nodiscard]] static state::Result<CheckpointStore> open(std::filesystem::path directory,
                                                          CheckpointOptions options = {});

  [[nodiscard]] const Durable& current() const noexcept { return current_; }
  [[nodiscard]] bool uncertain() const noexcept { return uncertain_; }
  [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

  // Replaces ONE checksummed and synced checkpoint atomically. On a failure
  // the disk state is UNKNOWN: no further write until recover() succeeds.
  [[nodiscard]] state::Result<void> persist(const Durable& target);
  [[nodiscard]] state::Result<Durable> recover();

  // Test-only injection; failures after replacement may have reached disk.
  [[nodiscard]] state::Result<void> inject_next_fault_for_test(CheckpointFault fault);

 private:
  CheckpointStore(std::filesystem::path path, CheckpointOptions options, Durable initial)
      : path_(std::move(path)), options_(options), current_(std::move(initial)) {}

  std::filesystem::path path_;
  CheckpointOptions options_;
  Durable current_;
  bool uncertain_ = false;
  CheckpointFault fault_ = CheckpointFault::None;
};

}  // namespace choreoos::raft
