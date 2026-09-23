#pragma once

// Build/version identity for CLI and node processes. This is not choreography
// state and must never enter the deterministic hash.

#include <string_view>

namespace choreoos::core {

[[nodiscard]] constexpr std::string_view version() noexcept { return "0.1.0"; }

}  // namespace choreoos::core
