#include "choreoos/storage/snapshot.hpp"

#include <iomanip>
#include <sstream>

#include "choreoos/state/store.hpp"
#include "choreoos/storage/crc32.hpp"
#include "choreoos/storage/file_util.hpp"

namespace choreoos::storage {
namespace {

using choreoos::state::Error;
using choreoos::state::ErrorCode;
using choreoos::state::Result;

std::filesystem::path snapshot_path(const std::filesystem::path& directory, std::uint64_t index) {
  std::ostringstream name;
  name << std::setw(16) << std::setfill('0') << index << ".snap";
  return directory / "snapshots" / name.str();
}

Result<Snapshot> read_snapshot(const std::filesystem::path& path) {
  auto bytes = read_file(path);
  if (!bytes) {
    return bytes.error();
  }
  const auto& file = bytes.value();
  if (file.size() < 52) {
    return Error{ErrorCode::StoreError, "snapshot is incomplete"};
  }
  if (std::string(file.begin(), file.begin() + 8) != "CHOSSNAP") {
    return Error{ErrorCode::StoreError, "snapshot magic is invalid"};
  }
  if (read_u16(file, 8) != 1 || read_u16(file, 10) != choreoos::state::kCurrentSchemaVersion) {
    return Error{ErrorCode::StoreError, "snapshot version is unsupported"};
  }
  const std::uint32_t payload_len = read_u32(file, 44);
  if (file.size() != static_cast<std::size_t>(52 + payload_len)) {
    return Error{ErrorCode::StoreError, "snapshot length is invalid"};
  }
  if (crc32(file.data(), file.size() - 4) != read_u32(file, file.size() - 4)) {
    return Error{ErrorCode::StoreError, "snapshot checksum mismatch"};
  }

  Snapshot snapshot;
  auto index = choreoos::state::LogIndex::parse(read_u64(file, 12));
  auto term = choreoos::state::Term::parse(read_u64(file, 20));
  if (!index || !term) {
    return Error{ErrorCode::StoreError, "snapshot index or term is invalid"};
  }
  snapshot.index = index.value();
  snapshot.term = term.value();
  snapshot.state_hash.assign(reinterpret_cast<const char*>(file.data() + 28), 16);

  std::string payload(reinterpret_cast<const char*>(file.data() + 48), payload_len);
  std::istringstream lines{payload};
  std::string line;
  while (std::getline(lines, line)) {
    if (line.empty()) {
      continue;
    }
    auto event = choreoos::state::parse_event(line);
    if (!event) {
      return event.error();
    }
    snapshot.events.push_back(event.value());
  }
  auto replayed = choreoos::state::replay(snapshot.events);
  if (!replayed) {
    return replayed.error();
  }
  if (choreoos::state::state_hash(replayed.value()) != snapshot.state_hash ||
      replayed.value().last_applied != snapshot.index) {
    return Error{ErrorCode::StoreError, "snapshot state hash does not match its events"};
  }
  return snapshot;
}

}  // namespace

Result<void> save_snapshot(const std::filesystem::path& directory,
                           const std::vector<choreoos::state::Event>& events,
                           const choreoos::state::ChoreographyState& state, bool sync) {
  std::error_code ec;
  std::filesystem::create_directories(directory / "snapshots", ec);
  if (ec) {
    return Error{ErrorCode::StoreError, "unable to create snapshot directory"};
  }
  std::string payload;
  for (const auto& event : events) {
    if (!payload.empty()) {
      payload.push_back('\n');
    }
    payload += choreoos::state::canonical_event(event);
  }
  const std::string hash = choreoos::state::state_hash(state);
  if (hash.size() != 16) {
    return Error{ErrorCode::StoreError, "state hash must be 16 hex characters"};
  }
  std::string bytes(52 + payload.size(), '\0');
  bytes.replace(0, 8, "CHOSSNAP");
  write_u16(bytes, 8, 1);
  write_u16(bytes, 10, choreoos::state::kCurrentSchemaVersion);
  write_u64(bytes, 12, state.last_applied.value());
  write_u64(bytes, 20, state.term.value());
  bytes.replace(28, 16, hash);
  write_u32(bytes, 44, static_cast<std::uint32_t>(payload.size()));
  if (!payload.empty()) {
    bytes.replace(48, payload.size(), payload);
  }
  write_u32(bytes, bytes.size() - 4, crc32(bytes.substr(0, bytes.size() - 4)));
  return write_atomic(snapshot_path(directory, state.last_applied.value()), bytes, sync);
}

Result<std::optional<Snapshot>> load_latest_snapshot(const std::filesystem::path& directory) {
  const auto folder = directory / "snapshots";
  if (!std::filesystem::exists(folder)) {
    return std::optional<Snapshot>{};
  }
  std::optional<Snapshot> newest;
  for (const auto& entry : std::filesystem::directory_iterator(folder)) {
    if (entry.path().extension() != ".snap") {
      continue;
    }
    auto snapshot = read_snapshot(entry.path());
    if (!snapshot) {
      continue;
    }
    if (!newest || newest->index < snapshot.value().index) {
      newest = snapshot.value();
    }
  }
  return newest;
}

}  // namespace choreoos::storage
