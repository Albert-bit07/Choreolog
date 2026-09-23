#pragma once

// A snapshot is a checksummed checkpoint of the events that produced a state.
// Recovery replays the snapshot, checks its hash, then applies only later log records.

#include <filesystem>
#include <optional>
#include <vector>

#include "choreoos/state/machine.hpp"

namespace choreoos::storage {

struct Snapshot {
  choreoos::state::LogIndex index = choreoos::state::LogIndex::none();
  choreoos::state::Term term = choreoos::state::Term::initial();
  std::string state_hash;
  std::vector<choreoos::state::Event> events;
};

[[nodiscard]] choreoos::state::Result<void> save_snapshot(
    const std::filesystem::path& directory, const std::vector<choreoos::state::Event>& events,
    const choreoos::state::ChoreographyState& state, bool sync);

// Highest-index snapshot with a valid checksum and matching state hash.
// Missing or invalid snapshots return an empty optional rather than failing recovery.
[[nodiscard]] choreoos::state::Result<std::optional<Snapshot>> load_latest_snapshot(
    const std::filesystem::path& directory);

}  // namespace choreoos::storage
