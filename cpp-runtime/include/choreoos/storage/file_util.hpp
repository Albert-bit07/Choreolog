#pragma once

// Little-endian binary helpers and crash-safe file replacement.
// Every on-disk integer is written explicitly so the format does not depend
// on the compiler's struct layout.

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "choreoos/state/error.hpp"

namespace choreoos::storage {

void write_u16(std::string& out, std::size_t offset, std::uint16_t value);
void write_u32(std::string& out, std::size_t offset, std::uint32_t value);
void write_u64(std::string& out, std::size_t offset, std::uint64_t value);

[[nodiscard]] std::uint16_t read_u16(const std::vector<std::uint8_t>& in, std::uint64_t offset);
[[nodiscard]] std::uint32_t read_u32(const std::vector<std::uint8_t>& in, std::uint64_t offset);
[[nodiscard]] std::uint64_t read_u64(const std::vector<std::uint8_t>& in, std::uint64_t offset);

[[nodiscard]] choreoos::state::Result<std::vector<std::uint8_t>> read_file(
    const std::filesystem::path& path);
[[nodiscard]] choreoos::state::Result<void> write_new_header(const std::filesystem::path& path);
[[nodiscard]] choreoos::state::Result<void> append_bytes(const std::filesystem::path& path,
                                                         const std::string& bytes, bool sync);
// Write to destination.tmp, flush it, then rename over destination. With
// `sync`, the file is fsynced before the rename. `sync_dir` additionally
// fsyncs the parent directory so the rename itself survives power loss; it
// defaults to `sync`. Skip it only for files whose previous version is an
// acceptable result after a crash.
[[nodiscard]] choreoos::state::Result<void> write_atomic(const std::filesystem::path& destination,
                                                         const std::string& bytes, bool sync,
                                                         std::optional<bool> sync_dir = std::nullopt);

// Make a directory entry change (create, rename) durable. POSIX fsyncs the
// directory. Windows relies on MOVEFILE_WRITE_THROUGH for renames and the
// journal for creation, so this is a no-op there.
[[nodiscard]] choreoos::state::Result<void> sync_directory(const std::filesystem::path& directory);

// fsync an existing file by path, for changes such as a size reduction.
[[nodiscard]] choreoos::state::Result<void> sync_file(const std::filesystem::path& path);

// The fixed 16-byte header at the start of every write-ahead log file.
[[nodiscard]] std::string wal_file_header();

}  // namespace choreoos::storage
