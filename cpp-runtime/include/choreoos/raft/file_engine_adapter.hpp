#pragma once

// Experimental synchronous writer for production storage formats. It is not
// called by Replica or the pure Raft core. One host owns the directory.
#include <memory>

#include "choreoos/raft/domain_bridge.hpp"

namespace choreoos::raft {

struct DomainWritePlan {
  std::uint64_t base_index = 0;
  std::uint64_t base_term = 0;
  std::uint64_t term = 1;
  std::optional<std::string> voted_for;
  std::vector<Entry> suffix;
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
};

}  // namespace choreoos::raft
