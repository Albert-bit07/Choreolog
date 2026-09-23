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

}  // namespace choreoos::protocol
