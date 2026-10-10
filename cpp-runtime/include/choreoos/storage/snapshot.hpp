#pragma once

// A snapshot is a checksummed checkpoint of the state at one log index.
//
// Format 2 (written today) stores the serialized state: dancers, formations,
// cues and the applied-command table. It does not contain event history, so its
// size follows the state, not the length of the log, and recovery restores it
// directly instead of replaying events.
//
// Format 1 (legacy, read-only) stored every event that produced the state.
// It is still loaded so existing stores and the golden fixtures keep working.
//
// Either way the restored state must hash to the recorded state_hash.

#include <filesystem>
#include <optional>
#include <vector>

#include "choreoos/state/machine.hpp"

namespace choreoos::storage {

struct Snapshot {
  choreoos::state::LogIndex index = choreoos::state::LogIndex::none();
  choreoos::state::Term term = choreoos::state::Term::initial();
  std::string state_hash;
  std::vector<choreoos::state::Event> events;  // format 1 only
  std::string state_payload;                   // format 2 only (snapshot_payload text)

  [[nodiscard]] bool legacy() const noexcept { return state_payload.empty(); }
};

// Writes a format 2 snapshot named by the state's last_applied index.
[[nodiscard]] choreoos::state::Result<void> save_snapshot(
    const std::filesystem::path& directory, const choreoos::state::ChoreographyState& state,
    bool sync);

// Highest-index snapshot with a valid checksum and matching state hash. Files
// are tried newest first and the search stops at the first valid one. Missing
// or invalid snapshots return an empty optional rather than failing recovery.
[[nodiscard]] choreoos::state::Result<std::optional<Snapshot>> load_latest_snapshot(
    const std::filesystem::path& directory);

// Delete all but the `keep` highest-index snapshot files. Returns how many
// snapshots remain and the index of the oldest one kept (0 when none).
struct PruneResult {
  std::size_t remaining = 0;
  std::uint64_t oldest_index = 0;
};
[[nodiscard]] choreoos::state::Result<PruneResult> prune_snapshots(
    const std::filesystem::path& directory, std::size_t keep);

// Validate a snapshot received from a peer or read from disk. Format 2 parses
// the state and checks its hash and index; format 1 replays its events.
[[nodiscard]] choreoos::state::Result<choreoos::state::ChoreographyState> verify_snapshot(
    const Snapshot& snapshot);

}  // namespace choreoos::storage
