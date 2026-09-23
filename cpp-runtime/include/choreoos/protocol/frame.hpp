#pragma once

// Length-prefixed frame around one protobuf payload.
// Layout, little-endian:
//   0  magic "CHOS" (4)
//   4  major (1)
//   5  minor (1)
//   6  message type (2)
//   8  correlation id (8)
//  16  payload length (4)
//  20  payload
//  end CRC-32 of every preceding byte
//
// TCP can split a frame across reads. Callers read 20 bytes, reject an
// oversized length, then read the payload and checksum.

#include <cstddef>
#include <cstdint>
#include <string>

#include "choreoos/state/error.hpp"

namespace choreoos::protocol {

inline constexpr char kFrameMagic[4] = {'C', 'H', 'O', 'S'};
inline constexpr std::uint8_t kProtocolMajor = 1;
inline constexpr std::uint8_t kProtocolMinor = 0;
inline constexpr std::size_t kFrameHeaderBytes = 20;
inline constexpr std::uint32_t kMaxFramePayload = 1u << 20;

enum class MessageType : std::uint16_t {
  Handshake = 1,
  AppendEntries = 2,
  AppendEntriesResponse = 3,
  RequestVote = 4,
  RequestVoteResponse = 5,
  InstallSnapshot = 6,
  InstallSnapshotResponse = 7,
  ProtocolError = 8,
  ClientCommand = 9,
  ClientResponse = 10,
  StatusQuery = 11,
  StatusResponse = 12,
};

struct Frame {
  MessageType type = MessageType::ProtocolError;
  std::uint64_t correlation = 0;
  std::string payload;
};

struct FrameHeader {
  MessageType type = MessageType::ProtocolError;
  std::uint64_t correlation = 0;
  std::uint32_t payload_length = 0;
};

[[nodiscard]] choreoos::state::Result<std::string> encode_frame(const Frame& frame);
[[nodiscard]] choreoos::state::Result<FrameHeader> peek_header(const std::uint8_t* data,
                                                               std::size_t size);
[[nodiscard]] choreoos::state::Result<Frame> decode_frame(const std::uint8_t* data,
                                                          std::size_t size);

}  // namespace choreoos::protocol
