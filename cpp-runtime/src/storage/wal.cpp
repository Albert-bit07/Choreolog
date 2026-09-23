#include "choreoos/storage/wal.hpp"

#include <fstream>

#include "choreoos/state/store.hpp"
#include "choreoos/storage/crc32.hpp"
#include "choreoos/storage/file_util.hpp"

namespace choreoos::storage {
namespace {

constexpr char kFileMagic[8] = {'C', 'H', 'O', 'S', 'W', 'A', 'L', '1'};
constexpr std::uint32_t kRecordMagic = 0x31474F4Cu;  // bytes 'L','O','G','1'
constexpr std::uint16_t kVersion = 1;
constexpr std::size_t kFileHeaderBytes = 16;
constexpr std::size_t kRecordPrefix = 28;

using choreoos::state::Error;
using choreoos::state::ErrorCode;
using choreoos::state::Result;

Error corrupt(const std::string& detail) { return Error{ErrorCode::StoreError, detail}; }

}  // namespace

Result<WriteAheadLog> WriteAheadLog::open(std::filesystem::path path) {
  WriteAheadLog log{std::move(path)};
  if (!std::filesystem::exists(log.path_)) {
    if (auto header = write_new_header(log.path_); !header) {
      return header.error();
    }
  }
  if (auto loaded = log.load(); !loaded) {
    return loaded.error();
  }
  return log;
}

Result<void> WriteAheadLog::append(const choreoos::state::Event& event, Durability durability) {
  const auto started = std::chrono::steady_clock::now();
  const std::string payload = choreoos::state::canonical_event(event);
  if (payload.size() > kMaxPayloadBytes) {
    return corrupt("event payload exceeds 1 MiB");
  }

  std::string record(kRecordPrefix + payload.size() + 4, '\0');
  write_u32(record, 0, kRecordMagic);
  write_u16(record, 4, kVersion);
  write_u16(record, 6, 0);
  write_u32(record, 8, static_cast<std::uint32_t>(payload.size()));
  write_u64(record, 12, event.term.value());
  write_u64(record, 20, event.index.value());
  record.replace(kRecordPrefix, payload.size(), payload);
  const std::uint32_t sum =
      crc32(reinterpret_cast<const std::uint8_t*>(record.data()), kRecordPrefix + payload.size());
  write_u32(record, kRecordPrefix + payload.size(), sum);

  if (auto written = append_bytes(path_, record, durability == Durability::Sync); !written) {
    return written.error();
  }
  const auto finished = std::chrono::steady_clock::now();
  stats_.append_ns += static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(finished - started).count());
  if (durability == Durability::Sync) {
    stats_.flush_ns += stats_.append_ns;
  }
  ++stats_.appends;
  events_.push_back(event);
  const auto previous = record_ends_.empty() ? kFileHeaderBytes : record_ends_.back();
  record_ends_.push_back(previous + record.size());
  return {};
}

Result<void> WriteAheadLog::truncate_after(choreoos::state::LogIndex index) {
  std::size_t keep = 0;
  while (keep < events_.size() && events_[keep].index <= index) {
    ++keep;
  }
  const std::uint64_t end = keep == 0 ? kFileHeaderBytes : record_ends_[keep - 1];
  std::error_code ec;
  std::filesystem::resize_file(path_, end, ec);
  if (ec) {
    return corrupt("unable to truncate write-ahead log");
  }
  events_.erase(events_.begin() + static_cast<std::ptrdiff_t>(keep), events_.end());
  record_ends_.erase(record_ends_.begin() + static_cast<std::ptrdiff_t>(keep), record_ends_.end());
  return {};
}

Result<choreoos::state::Event> WriteAheadLog::at(choreoos::state::LogIndex index) const {
  for (const auto& event : events_) {
    if (event.index == index) {
      return event;
    }
  }
  return corrupt("log index was not found");
}

Result<void> WriteAheadLog::load() {
  auto bytes = read_file(path_);
  if (!bytes) {
    return bytes.error();
  }
  auto& file = bytes.value();
  if (file.size() < kFileHeaderBytes) {
    return corrupt("write-ahead log header is incomplete");
  }
  if (std::string(file.begin(), file.begin() + 8) != std::string(kFileMagic, kFileMagic + 8)) {
    return corrupt("write-ahead log magic is invalid");
  }
  if (read_u16(file, 8) != kVersion) {
    return corrupt("write-ahead log version is unsupported");
  }
  if (crc32(file.data(), 12) != read_u32(file, 12)) {
    return corrupt("write-ahead log header checksum mismatch");
  }

  std::uint64_t offset = kFileHeaderBytes;
  std::uint64_t good_end = kFileHeaderBytes;
  events_.clear();
  record_ends_.clear();
  while (offset < file.size()) {
    const std::uint64_t available = file.size() - offset;
    if (available < kRecordPrefix + 4) {
      stats_.truncated_tail_bytes += available;
      break;
    }
    if (read_u32(file, offset) != kRecordMagic || read_u16(file, offset + 4) != kVersion) {
      // A torn write can leave garbage at the end. That is a tail only when no
      // later record magic exists. A bad record followed by a real record is corruption.
      bool later_magic = false;
      for (std::uint64_t probe = offset + 1; probe + 4 <= file.size(); ++probe) {
        if (file[probe] == 'L' && file[probe + 1] == 'O' && file[probe + 2] == 'G' &&
            file[probe + 3] == '1') {
          later_magic = true;
          break;
        }
      }
      if (later_magic) {
        return corrupt("corrupt write-ahead record at offset " + std::to_string(offset));
      }
      stats_.truncated_tail_bytes += available;
      break;
    }
    const std::uint32_t payload_len = read_u32(file, offset + 8);
    if (payload_len > kMaxPayloadBytes) {
      return corrupt("write-ahead record exceeds the 1 MiB limit");
    }
    const std::uint64_t total = kRecordPrefix + payload_len + 4;
    if (available < total) {
      stats_.truncated_tail_bytes += available;
      break;
    }
    const std::uint32_t expected = crc32(file.data() + offset, kRecordPrefix + payload_len);
    const std::uint32_t actual = read_u32(file, offset + kRecordPrefix + payload_len);
    if (expected != actual) {
      return corrupt("write-ahead record checksum mismatch at offset " + std::to_string(offset));
    }
    const std::string payload(reinterpret_cast<const char*>(file.data() + offset + kRecordPrefix),
                              payload_len);
    auto event = choreoos::state::parse_event(payload);
    if (!event) {
      return corrupt("write-ahead payload is not a canonical event");
    }
    if (event.value().index.value() != read_u64(file, offset + 20) ||
        event.value().term.value() != read_u64(file, offset + 12)) {
      return corrupt("write-ahead envelope does not match its payload");
    }
    events_.push_back(event.value());
    offset += total;
    good_end = offset;
    record_ends_.push_back(good_end);
  }

  if (good_end < file.size()) {
    std::error_code ec;
    std::filesystem::resize_file(path_, good_end, ec);
    if (ec) {
      return corrupt("unable to truncate a torn write-ahead tail");
    }
  }
  return {};
}

}  // namespace choreoos::storage
