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
// Throws std::invalid_argument for snapshot boundaries, CompactCommitted or
// snapshot/read inputs or active read state, including no-ops: the v1 model
// has no relation for these.
[[nodiscard]] std::string trace_line(const Config& config, const Node& state, const Input& input,
                                     std::uint64_t sequence);

// Snapshot-aware diagnostic contract. Includes complete staged/durable suffixes
// and replayable input fields. Does not claim a TLC refinement mapping; reads
// remain unsupported. V1 is unchanged and continues to reject snapshot inputs.
[[nodiscard]] std::string trace_line_v2(const Config& config, const Node& state, const Input& input,
                                        std::uint64_t sequence);

}  // namespace choreoos::raft
