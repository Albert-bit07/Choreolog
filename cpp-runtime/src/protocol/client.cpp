#include "choreoos/protocol/client.hpp"

#include <boost/asio.hpp>

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

Result<Frame> transact(const std::string& host, std::uint16_t port, const Frame& request,
                       std::chrono::milliseconds timeout) {
  try {
    boost::asio::io_context io;
    boost::asio::ip::tcp::socket socket{io};
    boost::asio::ip::tcp::resolver resolver{io};
    auto endpoints = resolver.resolve(host, std::to_string(port));
    boost::asio::connect(socket, endpoints);
    set_timeout(socket, timeout);
    auto encoded = encode_frame(request);
    if (!encoded) {
      return encoded.error();
    }
    boost::asio::write(socket, boost::asio::buffer(encoded.value()));
    std::uint8_t header[kFrameHeaderBytes];
    boost::asio::read(socket, boost::asio::buffer(header, kFrameHeaderBytes));
    auto peeked = peek_header(header, kFrameHeaderBytes);
    if (!peeked) {
      return peeked.error();
    }
    std::string frame(kFrameHeaderBytes + peeked.value().payload_length + 4, '\0');
    frame.replace(0, kFrameHeaderBytes, reinterpret_cast<const char*>(header), kFrameHeaderBytes);
    boost::asio::read(socket, boost::asio::buffer(frame.data() + kFrameHeaderBytes,
                                                  peeked.value().payload_length + 4));
    return decode_frame(reinterpret_cast<const std::uint8_t*>(frame.data()), frame.size());
  } catch (const std::exception& error) {
    return Error{ErrorCode::Unavailable, error.what()};
  }
}

}  // namespace choreoos::protocol
