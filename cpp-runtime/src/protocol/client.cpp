#include "choreoos/protocol/client.hpp"

#include <boost/asio.hpp>
#include <chrono>
#include <stdexcept>

#include "choreoos/protocol/codec.hpp"
#ifndef _WIN32
#include <poll.h>

#include <cerrno>
#endif

namespace choreoos::protocol {
namespace {

using choreoos::state::Error;
using choreoos::state::ErrorCode;
using choreoos::state::Result;

void set_timeout(boost::asio::ip::tcp::socket& socket, std::chrono::milliseconds timeout) {
  const auto milliseconds = static_cast<int>(timeout.count());
#if defined(_WIN32)
  const DWORD value = static_cast<DWORD>(milliseconds);
  ::setsockopt(socket.native_handle(), SOL_SOCKET, SO_RCVTIMEO,
               reinterpret_cast<const char*>(&value), sizeof(value));
  ::setsockopt(socket.native_handle(), SOL_SOCKET, SO_SNDTIMEO,
               reinterpret_cast<const char*>(&value), sizeof(value));
#else
  struct timeval value;
  value.tv_sec = milliseconds / 1000;
  value.tv_usec = (milliseconds % 1000) * 1000;
  ::setsockopt(socket.native_handle(), SOL_SOCKET, SO_RCVTIMEO, &value, sizeof(value));
  ::setsockopt(socket.native_handle(), SOL_SOCKET, SO_SNDTIMEO, &value, sizeof(value));
#endif
}

}  // namespace

namespace {

// Read exactly `length` bytes or fail at the deadline. Boost.Asio's blocking
// reads ignore SO_RCVTIMEO on POSIX (they poll with no timeout), so a server
// that never replies would block the client forever. Poll with the time left
// before each read instead. Windows honours SO_RCVTIMEO, so it keeps the plain
// read.
void read_exact(boost::asio::ip::tcp::socket& socket, char* data, std::size_t length,
                std::chrono::steady_clock::time_point deadline) {
#ifdef _WIN32
  (void)deadline;
  boost::asio::read(socket, boost::asio::buffer(data, length));
#else
  std::size_t received = 0;
  while (received < length) {
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                               deadline - std::chrono::steady_clock::now())
                               .count();
    if (remaining <= 0) {
      throw std::runtime_error("timed out waiting for a reply");
    }
    pollfd waiting{socket.native_handle(), POLLIN, 0};
    const int ready = ::poll(&waiting, 1, static_cast<int>(remaining));
    if (ready == 0) {
      throw std::runtime_error("timed out waiting for a reply");
    }
    if (ready < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("poll failed while waiting for a reply");
    }
    received += socket.read_some(boost::asio::buffer(data + received, length - received));
  }
#endif
}

}  // namespace

Result<Frame> transact(const std::string& host, std::uint16_t port, const Frame& request,
                       std::chrono::milliseconds timeout) {
  try {
    boost::asio::io_context io;
    boost::asio::ip::tcp::socket socket{io};
    boost::asio::ip::tcp::resolver resolver{io};
    auto endpoints = resolver.resolve(host, std::to_string(port));
    boost::asio::connect(socket, endpoints);
    set_timeout(socket, timeout);
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    auto encoded = encode_frame(request);
    if (!encoded) {
      return encoded.error();
    }
    boost::asio::write(socket, boost::asio::buffer(encoded.value()));
    std::uint8_t header[kFrameHeaderBytes];
    read_exact(socket, reinterpret_cast<char*>(header), kFrameHeaderBytes, deadline);
    auto peeked = peek_header(header, kFrameHeaderBytes);
    if (!peeked) {
      return peeked.error();
    }
    std::string frame(kFrameHeaderBytes + peeked.value().payload_length + 4, '\0');
    frame.replace(0, kFrameHeaderBytes, reinterpret_cast<const char*>(header), kFrameHeaderBytes);
    read_exact(socket, frame.data() + kFrameHeaderBytes, peeked.value().payload_length + 4,
               deadline);
    return decode_frame(reinterpret_cast<const std::uint8_t*>(frame.data()), frame.size());
  } catch (const std::exception& error) {
    return Error{ErrorCode::Unavailable, error.what()};
  }
}

Result<std::string> fetch_state(const std::string& host, std::uint16_t port,
                                const std::string& expected_hash, std::chrono::milliseconds timeout,
                                std::size_t maximum_bytes) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::string assembled;
  std::string hash = expected_hash;
  do {
    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        deadline - std::chrono::steady_clock::now());
    if (remaining.count() <= 0) return Error{ErrorCode::Unavailable, "state pagination timed out"};
    ManagementQuery query;
    query.operation = "state";
    query.after_index = assembled.size();
    query.expected_state_hash = hash;
    auto payload = encode(query);
    if (!payload) return payload.error();
    Frame frame;
    frame.type = MessageType::ManagementQuery;
    frame.payload = std::move(payload.value());
    auto wire = transact(host, port, frame, remaining);
    if (!wire) return wire.error();
    if (wire.value().type != MessageType::ManagementReply)
      return Error{ErrorCode::ProtocolError, "unexpected state page response"};
    auto page = decode_management_reply(wire.value().payload);
    if (!page) return page.error();
    const auto& value = page.value();
    if (!value.error_code.empty()) return Error{ErrorCode::Unavailable, value.error_message};
    if (value.state_size > maximum_bytes)
      return Error{ErrorCode::ProtocolError,
                   "state exceeds unary response limit; use paginated state queries"};
    if ((hash.empty() ? value.snapshot_state_hash.empty() : hash != value.snapshot_state_hash) ||
        value.state_chunk.size() > maximum_bytes - assembled.size() ||
        value.next_after_index != assembled.size() + value.state_chunk.size() ||
        value.next_after_index > value.state_size ||
        value.has_more != (value.next_after_index < value.state_size) ||
        (value.has_more && value.state_chunk.empty()))
      return Error{ErrorCode::ProtocolError, "invalid state page cursor or hash"};
    hash = value.snapshot_state_hash;
    assembled += value.state_chunk;
    if (!value.has_more) return assembled;
  } while (true);
}

}  // namespace choreoos::protocol
