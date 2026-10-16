#pragma once

// Deterministic trace projection for the experimental Raft transition core.
// Serializes a state snapshot, not a proof that this step refines TLA+.
#include <cstdint>
#include <string>

#include "choreoos/raft/transition.hpp"

namespace choreoos::raft {

// V1 actions follow the schema; unmapped maintenance has a diagnostic name.
// std::visit may throw std::bad_variant_access for a valueless input.
[[nodiscard]] const char* trace_action(const Input& input);

// One valid v1 NDJSON line; the caller owns sequencing and durable I/O.
// Throws std::invalid_argument for snapshot boundaries or CompactCommitted,
// including no-op compaction: the v1 trace/model has no relation for either.
[[nodiscard]] std::string trace_line(const Config& config, const Node& state, const Input& input,
                                     std::uint64_t sequence);

}  // namespace choreoos::raft
