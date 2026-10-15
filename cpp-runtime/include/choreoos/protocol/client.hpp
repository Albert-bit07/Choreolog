#pragma once

// Blocking client used by the CLI. The node server is asynchronous; this
// connection sends one frame and waits for one reply.

#include <chrono>
#include <cstdint>
#include <string>

#include "choreoos/protocol/frame.hpp"
#include "choreoos/state/error.hpp"

namespace choreoos::protocol {

[[nodiscard]] choreoos::state::Result<Frame> transact(const std::string& host, std::uint16_t port,
                                                      const Frame& request,
                                                      std::chrono::milliseconds timeout);

// Bounded convenience assembly for CLI/gRPC unary callers. Larger consumers
// use ManagementQuery(operation="state") directly with a hash-bound cursor.
[[nodiscard]] choreoos::state::Result<std::string> fetch_state(
    const std::string& host, std::uint16_t port, const std::string& expected_hash,
    std::chrono::milliseconds timeout, std::size_t maximum_bytes = 3u << 20);

}  // namespace choreoos::protocol
