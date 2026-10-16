#pragma once

#include <filesystem>
#include <memory>

#include "choreoos/state/error.hpp"

namespace choreoos::storage {

// Process-lifetime writer exclusion. The OS releases ownership after a crash;
// the persistent lock filename itself is never evidence that a writer is live.
class DirectoryLease {
 public:
  static state::Result<DirectoryLease> acquire(const std::filesystem::path& directory);
  DirectoryLease(DirectoryLease&&) noexcept = default;
  DirectoryLease& operator=(DirectoryLease&&) noexcept = default;
  DirectoryLease(const DirectoryLease&) = delete;
  DirectoryLease& operator=(const DirectoryLease&) = delete;

 private:
  struct Impl;
  explicit DirectoryLease(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}
  std::shared_ptr<Impl> impl_;
};

}  // namespace choreoos::storage
