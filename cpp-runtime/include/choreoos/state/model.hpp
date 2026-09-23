#pragma once

// Commands are requests. Events are facts.
// A command may be rejected and then no event exists. Once an event is
// created and committed, replay treats it as history.

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "choreoos/state/types.hpp"

namespace choreoos::state {

// What a client asked the runtime to do.
enum class CommandType {
  CreateChoreography,
  AddDancer,
  RemoveDancer,
  MoveDancer,
  DefineFormation,
  ChangeFormation,
  TriggerMusicCue,
  TriggerLightingCue,
};

// What actually happened after validation. Names stay stable in the log.
enum class EventType {
  ChoreographyCreated,
  DancerAdded,
  DancerRemoved,
  DancerMoved,
  FormationDefined,
  FormationChanged,
  MusicCueTriggered,
  LightingCueTriggered,
  // Leadership marker. It changes no dancers or cues. A leader appends one
  // in its own term so older replicated entries can commit under that term.
  NoOp,
};

struct CreateChoreographyPayload {
  ChoreographyId choreography_id;  // name of the piece
  StageBounds stage;               // floor size
  OverlapPolicy overlap;           // stacked marks allowed?
  std::int32_t max_travel_mm;      // max step between counts
};

struct DancerPayload {
  DancerId dancer_id;  // who
  Position position;   // where they stand or move to
};

struct RemoveDancerPayload {
  DancerId dancer_id;  // who leaves the piece
};

struct FormationPayload {
  FormationId formation_id;       // name such as "front"
  std::vector<DancerId> members;  // sorted before hashing
};

struct CuePayload {
  CueId cue_id;                     // name such as "intro"
  std::optional<CueId> depends_on;  // must already exist and be earlier
};

using CommandPayload = std::variant<CreateChoreographyPayload, DancerPayload, RemoveDancerPayload,
                                    FormationPayload, CuePayload>;
using EventPayload = CommandPayload;

// Client request. command.id is the idempotency key for retries.
struct Command {
  CommandId id;                    // retry key; same id => same result
  ChoreographyId choreography_id;  // which piece
  CommandType type;                // what was requested
  std::uint16_t schema_version;    // must be 1 today
  MusicalTick tick;                // when in the music
  CommandPayload payload;          // type-specific data
};

// Immutable log record. index/term come from the log, not the client.
struct Event {
  EventId id;                      // unique fact id
  CommandId command_id;            // which command produced this
  ChoreographyId choreography_id;  // which piece
  EventType type;                  // what happened
  std::uint16_t schema_version;    // must be 1 today
  MusicalTick tick;                // when in the music
  LogIndex index;                  // 1, 2, 3, ...
  Term term;                       // 1 until elections exist
  EventPayload payload;            // type-specific data
};

[[nodiscard]] const char* command_type_name(CommandType type) noexcept;
[[nodiscard]] const char* event_type_name(EventType type) noexcept;
[[nodiscard]] const char* overlap_policy_name(OverlapPolicy policy) noexcept;
[[nodiscard]] Result<CommandType> parse_command_type(std::string_view name);
[[nodiscard]] Result<EventType> parse_event_type(std::string_view name);
[[nodiscard]] Result<OverlapPolicy> parse_overlap_policy(std::string_view name);
[[nodiscard]] EventType event_type_for(CommandType type) noexcept;
// One-line deterministic encoding used for hashes, file storage, and compare.
[[nodiscard]] std::string canonical_event(const Event& event);

}  // namespace choreoos::state
