#include "choreoos/storage/metadata.hpp"

#include "choreoos/storage/crc32.hpp"
#include "choreoos/storage/file_util.hpp"

namespace choreoos::storage {
namespace {

using choreoos::state::Error;
using choreoos::state::ErrorCode;
using choreoos::state::Result;

}  // namespace

Result<NodeMetadata> load_metadata(const std::filesystem::path& path) {
  if (!std::filesystem::exists(path)) {
    return NodeMetadata{};
  }
  auto bytes = read_file(path);
  if (!bytes) {
    return bytes.error();
  }
  const auto& file = bytes.value();
  if (file.size() < 32) {
    return Error{ErrorCode::StoreError, "metadata file is incomplete"};
  }
  if (std::string(file.begin(), file.begin() + 8) != "CHOSMETA") {
    return Error{ErrorCode::StoreError, "metadata magic is invalid"};
  }
  if (read_u16(file, 8) != 1) {
    return Error{ErrorCode::StoreError, "metadata version is unsupported"};
  }
  const std::uint16_t voted_len = read_u16(file, 10);
  if (voted_len > 64 || file.size() != static_cast<std::size_t>(32 + voted_len)) {
    return Error{ErrorCode::StoreError, "metadata length is invalid"};
  }
  if (crc32(file.data(), file.size() - 4) != read_u32(file, file.size() - 4)) {
    return Error{ErrorCode::StoreError, "metadata checksum mismatch"};
  }
  auto term = choreoos::state::Term::parse(read_u64(file, 12));
  auto commit = choreoos::state::LogIndex::parse(read_u64(file, 20));
  if (!term || !commit) {
    return Error{ErrorCode::StoreError, "metadata contains an invalid term or index"};
  }
  NodeMetadata metadata{term.value(), commit.value(), std::nullopt};
  if (voted_len > 0) {
    const std::string voted(reinterpret_cast<const char*>(file.data() + 28), voted_len);
    auto parsed = choreoos::state::NodeId::parse(voted);
    if (!parsed) {
      return parsed.error();
    }
    metadata.voted_for = parsed.value();
  }
  return metadata;
}

Result<void> store_metadata(const std::filesystem::path& path, const NodeMetadata& metadata,
                            bool sync) {
  const std::string voted = metadata.voted_for ? metadata.voted_for->value() : "";
  if (voted.size() > 64) {
    return Error{ErrorCode::StoreError, "voted_for exceeds 64 characters"};
  }
  std::string bytes(32 + voted.size(), '\0');
  bytes.replace(0, 8, "CHOSMETA");
  write_u16(bytes, 8, 1);
  write_u16(bytes, 10, static_cast<std::uint16_t>(voted.size()));
  write_u64(bytes, 12, metadata.term.value());
  write_u64(bytes, 20, metadata.commit_index.value());
  if (!voted.empty()) {
    bytes.replace(28, voted.size(), voted);
  }
  write_u32(bytes, bytes.size() - 4, crc32(bytes.substr(0, bytes.size() - 4)));
  return write_atomic(path, bytes, sync);
}

}  // namespace choreoos::storage
