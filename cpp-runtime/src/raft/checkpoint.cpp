#include "choreoos/raft/checkpoint.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "choreoos/storage/crc32.hpp"
#include "choreoos/storage/file_util.hpp"

namespace choreoos::raft {
namespace {

using state::Error;
using state::ErrorCode;
using state::Result;

constexpr std::size_t kMaximumFileBytes = 16u * 1024u * 1024u;
constexpr std::size_t kMaximumCommandBytes = 1024u * 1024u;
constexpr std::uint32_t kMaximumEntries = 100000;
constexpr std::uint32_t kNoVote = std::numeric_limits<std::uint32_t>::max();

Error invalid(std::string why) { return {ErrorCode::StoreError, std::move(why)}; }

void add_u32(std::string& data, std::uint32_t value) {
  const auto offset = data.size();
  data.resize(offset + 4);
  storage::write_u32(data, offset, value);
}

void add_u64(std::string& data, std::uint64_t value) {
  const auto offset = data.size();
  data.resize(offset + 8);
  storage::write_u64(data, offset, value);
}

struct Reader {
  const std::vector<std::uint8_t>& bytes;
  std::size_t cursor = 0;

  bool take_u32(std::uint32_t& value) {
    if (cursor > bytes.size() || bytes.size() - cursor < 4) return false;
    value = storage::read_u32(bytes, cursor);
    cursor += 4;
    return true;
  }

  bool take_u64(std::uint64_t& value) {
    if (cursor > bytes.size() || bytes.size() - cursor < 8) return false;
    value = storage::read_u64(bytes, cursor);
    cursor += 8;
    return true;
  }

  bool take_string(std::uint32_t length, std::string& value) {
    if (cursor > bytes.size() || length > bytes.size() - cursor) return false;
    value.assign(reinterpret_cast<const char*>(bytes.data() + cursor), length);
    cursor += length;
    return true;
  }
};

Result<void> valid_state(const Durable& value) {
  if (value.commit > value.log.size() || value.applied > value.commit) {
    return invalid("Raft checkpoint indexes violate applied <= commit <= log length");
  }
  // PR #12 intentionally lacks an atomic choreography application checkpoint.
  // An applied index must not be marked durable ahead of durable state.
  if (value.applied != 0) {
    return invalid("Raft checkpoint cannot persist applied progress without state-machine snapshot");
  }
  if (value.voted_for && value.voted_for->size() > 64) {
    return invalid("Raft checkpoint vote ID too large");
  }
  if (value.log.size() > kMaximumEntries) {
    return invalid("Raft checkpoint log too long");
  }
  for (const auto& entry : value.log) {
    if (entry.term == 0 || entry.term > value.term || entry.command.empty() ||
        entry.command.size() > kMaximumCommandBytes) {
      return invalid("Raft checkpoint entry has invalid term or command length");
    }
  }
  return {};
}

Result<void> validate_transition(const Durable& old, const Durable& next) {
  auto shape = valid_state(next);
  if (!shape) return shape;
  if (next.term < old.term || next.commit < old.commit) {
    return invalid("Raft checkpoint cannot roll back term or committed index");
  }
  if (next.term == old.term && old.voted_for && next.voted_for != old.voted_for) {
    return invalid("Raft checkpoint cannot change a vote in the same term");
  }
  if (next.log.size() < old.commit) {
    return invalid("Raft checkpoint cannot truncate committed entries");
  }
  for (std::size_t i = 0; i < old.commit; ++i) {
    if (next.log[i] != old.log[i]) {
      return invalid("Raft checkpoint cannot rewrite committed prefix");
    }
  }
  return {};
}

Result<std::string> encode(const Durable& value) {
  const auto shape = valid_state(value);
  if (!shape) return shape.error();
  std::string data("CHRAFT01", 8);
  add_u32(data, 1);
  add_u32(data, 0);  // payload length filled after encoding
  add_u64(data, value.term);
  add_u64(data, value.commit);
  add_u64(data, value.applied);
  add_u32(data, value.voted_for ? static_cast<std::uint32_t>(value.voted_for->size()) : kNoVote);
  if (value.voted_for) data.append(*value.voted_for);
  add_u32(data, static_cast<std::uint32_t>(value.log.size()));
  for (const auto& entry : value.log) {
    add_u64(data, entry.term);
    add_u32(data, static_cast<std::uint32_t>(entry.command.size()));
    data.append(entry.command);
  }
  if (data.size() > kMaximumFileBytes - 4) {
    return invalid("Raft checkpoint exceeds maximum encoded size");
  }
  storage::write_u32(data, 12, static_cast<std::uint32_t>(data.size() - 16));
  add_u32(data, storage::crc32(data));
  return data;
}

Result<Durable> decode(const std::vector<std::uint8_t>& bytes) {
  if (bytes.size() < 16 + 8 * 3 + 4 + 4 + 4 || bytes.size() > kMaximumFileBytes) {
    return invalid("Raft checkpoint has invalid file size");
  }
  if (std::string(bytes.begin(), bytes.begin() + 8) != "CHRAFT01" ||
      storage::read_u32(bytes, 8) != 1 ||
      storage::read_u32(bytes, 12) != bytes.size() - 20) {
    return invalid("Raft checkpoint magic, version, or length invalid");
  }
  if (storage::crc32(bytes.data(), bytes.size() - 4) !=
      storage::read_u32(bytes, bytes.size() - 4)) {
    return invalid("Raft checkpoint checksum mismatch");
  }
  Reader reader{bytes, 16};
  Durable value;
  std::uint32_t vote_len = 0;
  if (!reader.take_u64(value.term) || !reader.take_u64(value.commit) ||
      !reader.take_u64(value.applied) || !reader.take_u32(vote_len)) {
    return invalid("Raft checkpoint has truncated metadata");
  }
  if (vote_len != kNoVote) {
    if (vote_len > 64) return invalid("Raft checkpoint vote ID too long");
    std::string vote;
    if (!reader.take_string(vote_len, vote)) return invalid("Raft checkpoint vote incomplete");
    value.voted_for = std::move(vote);
  }
  std::uint32_t count = 0;
  if (!reader.take_u32(count) || count > kMaximumEntries) {
    return invalid("Raft checkpoint log count invalid");
  }
  for (std::uint32_t index = 0; index < count; ++index) {
    Entry entry;
    std::uint32_t length = 0;
    if (!reader.take_u64(entry.term) || !reader.take_u32(length) ||
        length > kMaximumCommandBytes || !reader.take_string(length, entry.command)) {
      return invalid("Raft checkpoint log entry malformed");
    }
    value.log.push_back(std::move(entry));
  }
  if (reader.cursor != bytes.size() - 4) {
    return invalid("Raft checkpoint trailing data");
  }
  const auto shape = valid_state(value);
  if (!shape) return shape.error();
  return value;
}

Result<Durable> read_checkpoint(const std::filesystem::path& path) {
  std::error_code ec;
  const bool exists = std::filesystem::exists(path, ec);
  if (ec) return invalid("Raft checkpoint stat failed: " + ec.message());
  if (!exists) return Durable{};
  auto data = storage::read_file(path);
  if (!data) return data.error();
  return decode(data.value());
}

}  // namespace

state::Result<CheckpointStore> CheckpointStore::open(std::filesystem::path directory,
                                                    CheckpointOptions options) {
  std::error_code ec;
  std::filesystem::create_directories(directory, ec);
  if (ec) return invalid("Raft checkpoint directory creation failed: " + ec.message());
  auto snapshot = read_checkpoint(directory / "raft-core-v1.bin");
  if (!snapshot) return snapshot.error();
  return CheckpointStore{directory / "raft-core-v1.bin", options, std::move(snapshot.value())};
}

state::Result<void> CheckpointStore::persist(const Durable& target) {
  if (uncertain_) return invalid("Raft checkpoint requires recovery after previous write failure");
  const auto checked = validate_transition(current_, target);
  if (!checked) return checked;
  auto encoded = encode(target);
  if (!encoded) return encoded.error();
  const auto fault = fault_;
  fault_ = CheckpointFault::None;
  if (fault == CheckpointFault::BeforeReplace) {
    uncertain_ = true;
    return invalid("injected Raft checkpoint failure before replacement");
  }
  auto written = storage::write_atomic(path_, encoded.value(), true, true);
  if (!written) {
    uncertain_ = true;
    return written.error();
  }
  if (fault == CheckpointFault::AfterReplace) {
    uncertain_ = true;
    return invalid("injected Raft checkpoint failure after durable replacement");
  }
  current_ = target;
  return {};
}

state::Result<Durable> CheckpointStore::recover() {
  auto recovered = read_checkpoint(path_);
  if (!recovered) {
    uncertain_ = true;
    return recovered.error();
  }
  current_ = recovered.value();
  uncertain_ = false;
  fault_ = CheckpointFault::None;
  return current_;
}

state::Result<void> CheckpointStore::inject_next_fault_for_test(CheckpointFault fault) {
  if (!options_.test_mode) return invalid("Raft checkpoint fault injection requires test mode");
  fault_ = fault;
  return {};
}

}  // namespace choreoos::raft
