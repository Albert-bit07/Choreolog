#pragma once

// Little-endian binary helpers and crash-safe file replacement.
// Every on-disk integer is written explicitly so the format does not depend
// on the compiler's struct layout.

#include <cstdint>
#include <filesystem>
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
[[nodiscard]] choreoos::state::Result<void> write_atomic(const std::filesystem::path& destination,
                                                         const std::string& bytes, bool sync);

}  // namespace choreoos::storage
