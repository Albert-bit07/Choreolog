#include "choreoos/storage/crc32.hpp"

namespace choreoos::storage {

std::uint32_t crc32(const std::uint8_t* data, std::size_t size) noexcept {
  // Standard init and final XOR. The table-free bit loop keeps the code obvious.
  std::uint32_t crc = 0xFFFFFFFFu;
  for (std::size_t i = 0; i < size; ++i) {
    crc ^= data[i];
    for (int bit = 0; bit < 8; ++bit) {
      const std::uint32_t low_bit = crc & 1u;
      crc >>= 1;
      if (low_bit != 0) {
        crc ^= 0xEDB88320u;
      }
    }
  }
  return ~crc;
}

std::uint32_t crc32(std::string_view bytes) noexcept {
  return crc32(reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size());
}

}  // namespace choreoos::storage
