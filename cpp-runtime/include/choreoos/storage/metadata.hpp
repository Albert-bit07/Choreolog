#pragma once

// Checksummed node metadata. Term and vote are persisted before any later
// consensus response may depend on them. The file is replaced atomically.

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

#include "choreoos/state/types.hpp"

namespace choreoos::storage {

struct NodeMetadata {
  choreoos::state::Term term = choreoos::state::Term::initial();
  choreoos::state::LogIndex commit_index = choreoos::state::LogIndex::none();
  std::optional<choreoos::state::NodeId> voted_for;
};

[[nodiscard]] choreoos::state::Result<NodeMetadata> load_metadata(
    const std::filesystem::path& path);
// `sync_dir` makes the rename durable. Term and vote changes need it, because a
// reverted vote could be cast twice. A commit-index-only update does not: any
// earlier version of the file still carries the latest durable term and vote.
[[nodiscard]] choreoos::state::Result<void> store_metadata(const std::filesystem::path& path,
                                                           const NodeMetadata& metadata, bool sync,
                                                           std::optional<bool> sync_dir = std::nullopt);

}  // namespace choreoos::storage
