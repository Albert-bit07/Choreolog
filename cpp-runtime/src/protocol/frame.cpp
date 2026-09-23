#include "choreoos/protocol/frame.hpp"

#include "choreoos/storage/crc32.hpp"
#include "choreoos/storage/file_util.hpp"

namespace choreoos::protocol {
namespace {

using choreoos::state::Error;
using choreoos::state::ErrorCode;
using choreoos::state::Result;

std::uint16_t read_u16(const std::uint8_t* data) {
  return static_cast<std::uint16_t>(data[0] | (data[1] << 8));
}

std::uint32_t read_u32(const std::uint8_t* data) {
  return static_cast<std::uint32_t>(data[0]) | (static_cast<std::uint32_t>(data[1]) << 8) |
         (static_cast<std::uint32_t>(data[2]) << 16) | (static_cast<std::uint32_t>(data[3]) << 24);
}

std::uint64_t read_u64(const std::uint8_t* data) {
  std::uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value |= static_cast<std::uint64_t>(data[i]) << (8 * i);
  }
  return value;
}

Result<FrameHeader> header_from(const std::uint8_t* data) {
  if (data[0] != kFrameMagic[0] || data[1] != kFrameMagic[1] || data[2] != kFrameMagic[2] ||
      data[3] != kFrameMagic[3]) {
    return Error{ErrorCode::ProtocolError, "frame magic is invalid"};
  }
  if (data[4] != kProtocolMajor) {
    return Error{ErrorCode::ProtocolError, "protocol major version is unsupported"};
  }
  const std::uint32_t length = read_u32(data + 16);
  if (length > kMaxFramePayload) {
    return Error{ErrorCode::ProtocolError, "frame payload exceeds 1 MiB"};
  }
  FrameHeader header;
  header.type = static_cast<MessageType>(read_u16(data + 6));
  header.correlation = read_u64(data + 8);
  header.payload_length = length;
  return header;
}

}  // namespace

Result<std::string> encode_frame(const Frame& frame) {
  if (frame.payload.size() > kMaxFramePayload) {
    return Error{ErrorCode::ProtocolError, "frame payload exceeds 1 MiB"};
  }
  std::string bytes(kFrameHeaderBytes + frame.payload.size() + 4, '\0');
  bytes[0] = kFrameMagic[0];
  bytes[1] = kFrameMagic[1];
  bytes[2] = kFrameMagic[2];
  bytes[3] = kFrameMagic[3];
  bytes[4] = static_cast<char>(kProtocolMajor);
  bytes[5] = static_cast<char>(kProtocolMinor);
  choreoos::storage::write_u16(bytes, 6, static_cast<std::uint16_t>(frame.type));
  choreoos::storage::write_u64(bytes, 8, frame.correlation);
  choreoos::storage::write_u32(bytes, 16, static_cast<std::uint32_t>(frame.payload.size()));
  if (!frame.payload.empty()) {
    bytes.replace(kFrameHeaderBytes, frame.payload.size(), frame.payload);
  }
  choreoos::storage::write_u32(
      bytes, bytes.size() - 4,
      choreoos::storage::crc32(reinterpret_cast<const std::uint8_t*>(bytes.data()),
                               bytes.size() - 4));
  return bytes;
}

Result<FrameHeader> peek_header(const std::uint8_t* data, std::size_t size) {
  if (size < kFrameHeaderBytes) {
    return Error{ErrorCode::ProtocolError, "frame header is incomplete"};
  }
  return header_from(data);
}

Result<Frame> decode_frame(const std::uint8_t* data, std::size_t size) {
  if (size < kFrameHeaderBytes + 4) {
    return Error{ErrorCode::ProtocolError, "frame is incomplete"};
  }
  auto header = header_from(data);
  if (!header) {
    return header.error();
  }
  const std::size_t total = kFrameHeaderBytes + header.value().payload_length + 4;
  if (size < total) {
    return Error{ErrorCode::ProtocolError, "frame is incomplete"};
  }
  if (size != total) {
    return Error{ErrorCode::ProtocolError, "frame length does not match its header"};
  }
  const std::uint32_t actual = choreoos::storage::crc32(data, total - 4);
  const std::uint32_t expected = read_u32(data + total - 4);
  if (actual != expected) {
    return Error{ErrorCode::ProtocolError, "frame checksum mismatch"};
  }
  Frame frame;
  frame.type = header.value().type;
  frame.correlation = header.value().correlation;
  frame.payload.assign(reinterpret_cast<const char*>(data + kFrameHeaderBytes),
                       header.value().payload_length);
  return frame;
}

}  // namespace choreoos::protocol
