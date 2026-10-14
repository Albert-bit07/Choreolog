#include "choreoos/storage/transaction.hpp"

#include <cstddef>
#include <iomanip>
#include <sstream>
#include <vector>

#include "choreoos/storage/crc32.hpp"
#include "choreoos/storage/file_util.hpp"

namespace choreoos::storage {
namespace {

constexpr std::size_t kMaximumBytes = 128u * 1024u * 1024u;
constexpr std::size_t kHeaderBytes = 32;
constexpr const char* kJournal = "snapshot-install.txn";

state::Error invalid(const std::string& why) {
  return {state::ErrorCode::StoreError, "snapshot transaction: " + why};
}

state::Result<bool> journal_exists(const std::filesystem::path& path) {
  std::error_code ec;
  const bool found = std::filesystem::exists(path, ec);
  if (ec) return invalid("stat failed: " + ec.message());
  return found;
}

state::Result<std::string> encode(const SnapshotTransaction& value) {
  // Bound before concatenation and narrowing; subtraction avoids overflow.
  std::size_t remaining = kMaximumBytes - kHeaderBytes - 4;
  for (const auto* part : {&value.snapshot, &value.metadata, &value.wal}) {
    if (part->size() > remaining) return invalid("replacement images exceed 128 MiB");
    remaining -= part->size();
  }
  if (value.index == 0 || value.snapshot.empty() || value.metadata.empty() || value.wal.empty()) {
    return invalid("missing index or replacement image");
  }
  std::string bytes(kHeaderBytes, '\0');
  bytes.replace(0, 8, "CHOSTXN1");
  write_u32(bytes, 8, 1);
  write_u64(bytes, 12, value.index);
  write_u32(bytes, 20, static_cast<std::uint32_t>(value.snapshot.size()));
  write_u32(bytes, 24, static_cast<std::uint32_t>(value.metadata.size()));
  write_u32(bytes, 28, static_cast<std::uint32_t>(value.wal.size()));
  bytes += value.snapshot;
  bytes += value.metadata;
  bytes += value.wal;
  const auto checksum = crc32(bytes);
  const auto end = bytes.size();
  bytes.resize(end + 4);
  write_u32(bytes, end, checksum);
  return bytes;
}

state::Result<SnapshotTransaction> decode(const std::vector<std::uint8_t>& bytes) {
  if (bytes.size() < kHeaderBytes + 4 || bytes.size() > kMaximumBytes) {
    return invalid("invalid journal size");
  }
  if (std::string(bytes.begin(), bytes.begin() + 8) != "CHOSTXN1" || read_u32(bytes, 8) != 1 ||
      read_u64(bytes, 12) == 0 ||
      crc32(bytes.data(), bytes.size() - 4) != read_u32(bytes, bytes.size() - 4)) {
    return invalid("invalid journal header or checksum");
  }
  SnapshotTransaction value;
  value.index = read_u64(bytes, 12);
  std::size_t cursor = kHeaderBytes;
  std::size_t field = 20;
  for (auto* part : {&value.snapshot, &value.metadata, &value.wal}) {
    const auto length = read_u32(bytes, field);
    if (length == 0 || length > bytes.size() - 4 - cursor) {
      return invalid("invalid replacement image length");
    }
    part->assign(reinterpret_cast<const char*>(bytes.data() + cursor), length);
    cursor += length;
    field += 4;
  }
  if (cursor != bytes.size() - 4) return invalid("trailing journal data");
  return value;
}

state::Result<void> apply(const std::filesystem::path& directory, const SnapshotTransaction& value,
                          InstallFault fault) {
  if (fault == InstallFault::AfterJournal) return invalid("injected interruption after journal");
  std::error_code ec;
  std::filesystem::create_directories(directory / "snapshots", ec);
  if (ec) return invalid("cannot create snapshot directory: " + ec.message());
  if (auto synced = sync_directory(directory); !synced) return synced.error();
  std::ostringstream name;
  name << std::setw(16) << std::setfill('0') << value.index << ".snap";
  if (auto saved = write_atomic(directory / "snapshots" / name.str(), value.snapshot, true); !saved)
    return saved.error();
  if (fault == InstallFault::AfterSnapshot) return invalid("injected interruption after snapshot");
  if (auto saved = write_atomic(directory / "meta.bin", value.metadata, true); !saved) {
    return saved.error();
  }
  if (fault == InstallFault::AfterMetadata) return invalid("injected interruption after metadata");
  if (auto saved = write_atomic(directory / "wal.bin", value.wal, true); !saved) {
    return saved.error();
  }
  if (fault == InstallFault::AfterWal) return invalid("injected interruption after WAL");
  if (!std::filesystem::remove(directory / kJournal, ec) || ec) {
    return invalid("cannot remove completed journal");
  }
  return sync_directory(directory);
}

}  // namespace

state::Result<void> commit_snapshot_transaction(const std::filesystem::path& directory,
                                                const SnapshotTransaction& value,
                                                InstallFault fault) {
  auto pending = journal_exists(directory / kJournal);
  if (!pending) return pending.error();
  if (pending.value()) return invalid("pending install requires recovery");
  auto bytes = encode(value);
  if (!bytes) return bytes.error();
  // Persist newly created node-directory ancestors before the decision file
  // can make any install durable. Windows sync_directory is a documented no-op.
  std::error_code ec;
  auto ancestor = std::filesystem::absolute(directory, ec);
  if (ec) return invalid("cannot resolve store directory");
  for (;;) {
    if (auto synced = sync_directory(ancestor); !synced) return synced.error();
    const auto parent = ancestor.parent_path();
    if (parent == ancestor) break;
    ancestor = parent;
  }
  if (auto committed = write_atomic(directory / kJournal, bytes.value(), true); !committed) {
    return committed.error();
  }
  return apply(directory, value, fault);
}

state::Result<void> recover_snapshot_transaction(const std::filesystem::path& directory) {
  auto pending = journal_exists(directory / kJournal);
  if (!pending) return pending.error();
  // Also finish a previously uncertain journal deletion before admitting
  // fresh writes. Otherwise an old decision could reappear after a later crash.
  if (!pending.value()) return sync_directory(directory);
  std::error_code ec;
  const auto size = std::filesystem::file_size(directory / kJournal, ec);
  if (ec || size > kMaximumBytes) return invalid("unreadable or oversized journal");
  auto bytes = read_file(directory / kJournal);
  if (!bytes) return bytes.error();
  auto value = decode(bytes.value());
  if (!value) return value.error();
  return apply(directory, value.value(), InstallFault::None);
}

}  // namespace choreoos::storage
