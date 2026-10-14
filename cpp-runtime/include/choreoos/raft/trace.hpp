#pragma once

// Deterministic trace projection for the experimental Raft transition core.
// Serializes a state snapshot, not a proof that this step refines TLA+.
#include <cstdint>
#include <string>

#include "choreoos/raft/transition.hpp"

namespace choreoos::raft {

// Action names intentionally follow formal/raft/trace-schema.v1.json.
[[nodiscard]] const char* trace_action(const Input& input) noexcept;

// One valid v1 NDJSON line; the caller owns sequencing and durable I/O.
[[nodiscard]] std::string trace_line(const Config& config, const Node& state, const Input& input,
                                     std::uint64_t sequence);

}  // namespace choreoos::raft
