#pragma once

// Shared command/event generation for the Step 31 benchmark harnesses.
// Workloads are deterministic: a fixed seed drives every generated id and
// coordinate so runs are comparable. Nothing here touches the clock, disk,
// or the network; callers own all I/O.

#include <cstdint>
#include <string>
#include <vector>

#include "choreoos/state/model.hpp"

namespace choreoos::bench {

using choreoos::state::ChoreographyId;
using choreoos::state::Command;
using choreoos::state::CommandId;
using choreoos::state::CommandType;
using choreoos::state::CreateChoreographyPayload;
using choreoos::state::DancerId;
using choreoos::state::DancerPayload;
using choreoos::state::Event;
using choreoos::state::kCurrentSchemaVersion;
using choreoos::state::MusicalTick;
using choreoos::state::OverlapPolicy;
using choreoos::state::Position;
using choreoos::state::StageBounds;

inline ChoreographyId show_id() { return ChoreographyId::parse("opening").value(); }

inline Command make_create(std::uint64_t seq) {
  const std::string id = "bench-create-" + std::to_string(seq);
  // OverlapPolicy::Allow: these harnesses measure throughput of the state
  // machine, log, and replication paths, not the overlap validator (which the
  // correctness suites already cover). Allow keeps generated positions
  // trivially collision-free at any scale.
  // The 1M x 1M stage is the maximum StageCoordinate accepts.
  return Command{CommandId::parse(id).value(),
                 show_id(),
                 CommandType::CreateChoreography,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(0).value(),
                 CreateChoreographyPayload{show_id(), StageBounds::from_mm(1000000, 1000000).value(),
                                           OverlapPolicy::Allowed, 5000}};
}

inline Command make_add_dancer(std::uint64_t seq) {
  // Grid layout: unique positions at any scale, always inside the stage.
  const auto col = static_cast<std::int32_t>(seq % 1000);
  const auto row = static_cast<std::int32_t>(seq / 1000);
  const auto x = static_cast<std::int32_t>(500 + col * 900);
  const auto y = static_cast<std::int32_t>(500 + row * 900);
  const std::string id = "bench-add-" + std::to_string(seq);
  return Command{CommandId::parse(id).value(),
                 show_id(),
                 CommandType::AddDancer,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(1).value(),
                 DancerPayload{DancerId::parse(id).value(), Position::from_mm(x, y).value()}};
}

inline Command make_move_dancer(std::uint64_t seq, std::uint64_t dancer_seq) {
  // Shift the dancer one grid cell (+900mm x), well under the 5000mm travel
  // limit. OverlapPolicy::Allow (see make_create) keeps this valid.
  const auto col = static_cast<std::int32_t>(dancer_seq % 1000);
  const auto row = static_cast<std::int32_t>(dancer_seq / 1000);
  const auto x = static_cast<std::int32_t>(500 + col * 900 + 900);
  const auto y = static_cast<std::int32_t>(500 + row * 900);
  const std::string id = "bench-move-" + std::to_string(seq);
  const std::string dancer = "bench-add-" + std::to_string(dancer_seq);
  return Command{CommandId::parse(id).value(),
                 show_id(),
                 CommandType::MoveDancer,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(2).value(),
                 DancerPayload{DancerId::parse(dancer).value(), Position::from_mm(x, y).value()}};
}

// A create followed by `adds` dancer additions. All commands are valid
// against an empty state, so every submit produces exactly one event.
inline std::vector<Command> make_workload(std::uint64_t adds) {
  std::vector<Command> commands;
  commands.reserve(static_cast<std::size_t>(adds) + 1);
  commands.push_back(make_create(0));
  for (std::uint64_t i = 0; i < adds; ++i) {
    commands.push_back(make_add_dancer(i));
  }
  return commands;
}

}  // namespace choreoos::bench
