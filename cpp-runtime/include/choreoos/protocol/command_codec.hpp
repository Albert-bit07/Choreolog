#pragma once

// Text form of a client command, carried inside ClientCommand.canonical.
// Example:
//   schema=1;type=ADD_DANCER;cmd=c-alice;choreo=opening;tick=480;dancer=alice;x=1000;y=1000

#include <string>
#include <string_view>

#include "choreoos/state/model.hpp"

namespace choreoos::protocol {

[[nodiscard]] std::string canonical_command(const choreoos::state::Command& command);
[[nodiscard]] choreoos::state::Result<choreoos::state::Command> parse_command(
    std::string_view text);

}  // namespace choreoos::protocol
