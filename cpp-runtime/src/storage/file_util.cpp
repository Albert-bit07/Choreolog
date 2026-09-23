#include "choreoos/storage/file_util.hpp"

#include <cstdio>
#include <fstream>
#include <iterator>
#include <system_error>

#ifdef _WIN32
#include <io.h>
#include <windows.h>
#else
#include <unistd.h>
#endif

#include "choreoos/storage/crc32.hpp"

namespace choreoos::storage {
namespace {

using choreoos::state::Error;
using choreoos::state::ErrorCode;
using choreoos::state::Result;

Error io_error(const std::string& detail) { return Error{ErrorCode::StoreError, detail}; }

Result<void> sync_handle(FILE* file) {
  if (std::fflush(file) != 0) {
    return io_error("flush failed");
  }
#ifdef _WIN32
  const int fd = _fileno(file);
  if (_commit(fd) != 0) {
    return io_error("commit failed");
  }
  const HANDLE handle = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
  if (handle == INVALID_HANDLE_VALUE || !FlushFileBuffers(handle)) {
    return io_error("FlushFileBuffers failed");
  }
#else
  if (fsync(fileno(file)) != 0) {
    return io_error("fsync failed");
  }
#endif
  return {};
}

}  // namespace

void write_u16(std::string& out, std::size_t offset, std::uint16_t value) {
  out[offset] = static_cast<char>(value & 0xFFu);
  out[offset + 1] = static_cast<char>((value >> 8) & 0xFFu);
}

void write_u32(std::string& out, std::size_t offset, std::uint32_t value) {
  for (int i = 0; i < 4; ++i) {
    out[offset + static_cast<std::size_t>(i)] = static_cast<char>((value >> (8 * i)) & 0xFFu);
  }
}

void write_u64(std::string& out, std::size_t offset, std::uint64_t value) {
  for (int i = 0; i < 8; ++i) {
    out[offset + static_cast<std::size_t>(i)] = static_cast<char>((value >> (8 * i)) & 0xFFu);
  }
}

std::uint16_t read_u16(const std::vector<std::uint8_t>& in, std::uint64_t offset) {
  return static_cast<std::uint16_t>(in[offset] | (in[offset + 1] << 8));
}

std::uint32_t read_u32(const std::vector<std::uint8_t>& in, std::uint64_t offset) {
  std::uint32_t value = 0;
  for (int i = 0; i < 4; ++i) {
    value |= static_cast<std::uint32_t>(in[offset + static_cast<std::uint64_t>(i)]) << (8 * i);
  }
  return value;
}

std::uint64_t read_u64(const std::vector<std::uint8_t>& in, std::uint64_t offset) {
  std::uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value |= static_cast<std::uint64_t>(in[offset + static_cast<std::uint64_t>(i)]) << (8 * i);
  }
  return value;
}

Result<std::vector<std::uint8_t>> read_file(const std::filesystem::path& path) {
  std::ifstream in{path, std::ios::binary};
  if (!in) {
    return io_error("unable to open " + path.string());
  }
  return std::vector<std::uint8_t>(std::istreambuf_iterator<char>(in),
                                   std::istreambuf_iterator<char>());
}

Result<void> write_new_header(const std::filesystem::path& path) {
  std::string header(16, '\0');
  header.replace(0, 8, "CHOSWAL1");
  write_u16(header, 8, 1);
  write_u16(header, 10, 0);
  write_u32(header, 12, crc32(header.substr(0, 12)));
  FILE* file = nullptr;
#ifdef _WIN32
  if (fopen_s(&file, path.string().c_str(), "wb") != 0) {
    file = nullptr;
  }
#else
  file = std::fopen(path.string().c_str(), "wb");
#endif
  if (file == nullptr) {
    return io_error("unable to create write-ahead log");
  }
  const bool wrote = std::fwrite(header.data(), 1, header.size(), file) == header.size();
  auto synced = wrote ? sync_handle(file) : Result<void>{io_error("unable to write log header")};
  std::fclose(file);
  return synced;
}

Result<void> append_bytes(const std::filesystem::path& path, const std::string& bytes, bool sync) {
  FILE* file = nullptr;
#ifdef _WIN32
  if (fopen_s(&file, path.string().c_str(), "ab") != 0) {
    file = nullptr;
  }
#else
  file = std::fopen(path.string().c_str(), "ab");
#endif
  if (file == nullptr) {
    return io_error("unable to append to " + path.string());
  }
  const bool wrote = std::fwrite(bytes.data(), 1, bytes.size(), file) == bytes.size();
  Result<void> synced{};
  if (!wrote) {
    synced = io_error("short write to " + path.string());
  } else if (sync) {
    synced = sync_handle(file);
  } else if (std::fflush(file) != 0) {
    synced = io_error("flush failed");
  }
  std::fclose(file);
  return synced;
}

Result<void> write_atomic(const std::filesystem::path& destination, const std::string& bytes,
                          bool sync) {
  const auto temporary = destination.string() + ".tmp";
  FILE* file = nullptr;
#ifdef _WIN32
  if (fopen_s(&file, temporary.c_str(), "wb") != 0) {
    file = nullptr;
  }
#else
  file = std::fopen(temporary.c_str(), "wb");
#endif
  if (file == nullptr) {
    return io_error("unable to create " + temporary);
  }
  const bool wrote = std::fwrite(bytes.data(), 1, bytes.size(), file) == bytes.size();
  auto synced = wrote ? (sync ? sync_handle(file) : Result<void>{})
                      : Result<void>{io_error("short atomic write")};
  std::fclose(file);
  if (!synced) {
    return synced.error();
  }

#ifdef _WIN32
  const auto dest_wide = destination.wstring();
  const auto temp_wide = std::filesystem::path(temporary).wstring();
  if (!MoveFileExW(temp_wide.c_str(), dest_wide.c_str(),
                   MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
    return io_error("atomic replace failed for " + destination.string());
  }
#else
  std::error_code ec;
  std::filesystem::rename(temporary, destination, ec);
  if (ec) {
    return io_error("atomic replace failed for " + destination.string());
  }
#endif
  return {};
}

}  // namespace choreoos::storage
