#pragma once

// CRC-32/ISO-HDLC. Same bytes must produce the same checksum on every machine.
// Used by the write-ahead log, metadata file, and snapshots.

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace choreoos::storage {

[[nodiscard]] std::uint32_t crc32(const std::uint8_t* data, std::size_t size) noexcept;
[[nodiscard]] std::uint32_t crc32(std::string_view bytes) noexcept;

}  // namespace choreoos::storage
