// Invariants, propose/apply, replay, and hashing.
// apply() copies state first; a failed event never leaves a partial update.

#include "choreoos/state/machine.hpp"

#include <algorithm>
#include <iomanip>
#include <sstream>

namespace choreoos::state {
namespace {

// FNV-1a 64-bit over canonical_state(). Same bytes => same hash on every node.
constexpr std::uint64_t kFnvOffset = 14695981039346656037ull;
constexpr std::uint64_t kFnvPrime = 1099511628211ull;

Error unsupported_schema(std::uint16_t version) {
  return Error{ErrorCode::UnsupportedVersion,
               "unsupported schema version " + std::to_string(version)};
}

const DancerState* find_dancer(const ChoreographyState& state, const DancerId& id) {
  const auto it = state.dancers.find(id.value());
  if (it == state.dancers.end()) {
    return nullptr;
  }
  return &it->second;
}

bool mark_occupied(const ChoreographyState& state, const Position& position,
                   const std::optional<DancerId>& except) {
  for (const auto& [id, dancer] : state.dancers) {
    if (!dancer.active) {
      continue;
    }
    if (except && id == except->value()) {
      continue;
    }
    if (dancer.position == position) {
      return true;
    }
  }
  return false;
}

Result<void> require_created(const ChoreographyState& state) {
  if (!state.created || !state.id || !state.stage) {
    return Error{ErrorCode::ChoreographyNotFound, "choreography has not been created"};
  }
  return {};
}

Result<void> require_schema(std::uint16_t version) {
  if (version != kCurrentSchemaVersion) {
    return unsupported_schema(version);
  }
  return {};
}

std::vector<DancerId> unique_sorted(std::vector<DancerId> members) {
  std::sort(members.begin(), members.end());
  members.erase(std::unique(members.begin(), members.end()), members.end());
  return members;
}

Result<void> validate_formation_members(const ChoreographyState& state,
                                        const std::vector<DancerId>& members) {
  if (members.empty()) {
    return Error{ErrorCode::InvalidFormation, "formation must contain at least one dancer"};
  }
  for (const auto& member : members) {
    const DancerState* dancer = find_dancer(state, member);
    if (dancer == nullptr) {
      return Error{ErrorCode::DancerNotFound,
                   "formation references unknown dancer " + member.value()};
    }
    if (!dancer->active) {
      return Error{ErrorCode::DancerInactive,
                   "formation references inactive dancer " + member.value()};
    }
  }
  return {};
}

const CueState* find_cue(const ChoreographyState& state, const CueId& id) {
  const auto music = state.music_cues.find(id.value());
  if (music != state.music_cues.end()) {
    return &music->second;
  }
  const auto light = state.lighting_cues.find(id.value());
  if (light != state.lighting_cues.end()) {
    return &light->second;
  }
  return nullptr;
}

Result<void> validate_cue_dependency(const ChoreographyState& state, const CuePayload& cue,
                                     MusicalTick tick) {
  if (!cue.depends_on) {
    return {};
  }
  const CueState* dependency = find_cue(state, *cue.depends_on);
  if (dependency == nullptr) {
    return Error{ErrorCode::CueDependencyUnmet,
                 "cue depends on missing cue " + cue.depends_on->value()};
  }
  if (tick < dependency->tick) {
    return Error{ErrorCode::CueDependencyUnmet, "cue occurs before its dependency"};
  }
  return {};
}

Result<Event> make_event(const Command& command, LogIndex index, Term term, EventPayload payload) {
  auto event_id = EventId::parse(command.id.value());
  if (!event_id) {
    return event_id.error();
  }
  return Event{
      event_id.value(),
      command.id,
      command.choreography_id,
      event_type_for(command.type),
      command.schema_version,
      command.tick,
      index,
      term,
      std::move(payload),
  };
}

Result<void> apply_create(ChoreographyState& state, const Event& event,
                          const CreateChoreographyPayload& payload) {
  if (state.created) {
    return Error{ErrorCode::ChoreographyExists, "choreography already exists"};
  }
  if (payload.max_travel_mm < 0) {
    return Error{ErrorCode::InvalidTravel, "maximum travel must be non-negative"};
  }
  state.created = true;
  state.id = payload.choreography_id;
  state.stage = payload.stage;
  state.overlap = payload.overlap;
  state.max_travel_mm = payload.max_travel_mm;
  state.term = event.term;
  return {};
}

Result<void> apply_add(ChoreographyState& state, const DancerPayload& payload) {
  if (auto created = require_created(state); !created) {
    return created.error();
  }
  if (find_dancer(state, payload.dancer_id) != nullptr) {
    return Error{ErrorCode::DancerExists, "dancer already exists"};
  }
  if (!state.stage->contains(payload.position)) {
    return Error{ErrorCode::OutOfBounds, "dancer is outside the stage"};
  }
  if (state.overlap == OverlapPolicy::Forbidden &&
      mark_occupied(state, payload.position, std::nullopt)) {
    return Error{ErrorCode::PositionOccupied, "mark is already occupied"};
  }
  state.dancers.insert({payload.dancer_id.value(), DancerState{payload.position, true}});
  return {};
}

Result<void> apply_remove(ChoreographyState& state, const RemoveDancerPayload& payload) {
  if (auto created = require_created(state); !created) {
    return created.error();
  }
  DancerState* dancer = nullptr;
  const auto it = state.dancers.find(payload.dancer_id.value());
  if (it == state.dancers.end()) {
    return Error{ErrorCode::DancerNotFound, "dancer does not exist"};
  }
  dancer = &it->second;
  if (!dancer->active) {
    return Error{ErrorCode::DancerInactive, "dancer is already removed"};
  }
  dancer->active = false;
  return {};
}

Result<void> apply_move(ChoreographyState& state, const DancerPayload& payload) {
  if (auto created = require_created(state); !created) {
    return created.error();
  }
  const auto it = state.dancers.find(payload.dancer_id.value());
  if (it == state.dancers.end()) {
    return Error{ErrorCode::DancerNotFound, "dancer does not exist"};
  }
  if (!it->second.active) {
    return Error{ErrorCode::DancerInactive, "removed dancers cannot move"};
  }
  if (!state.stage->contains(payload.position)) {
    return Error{ErrorCode::OutOfBounds, "move is outside the stage"};
  }
  if (!travel_within_limit(it->second.position, payload.position, state.max_travel_mm)) {
    return Error{ErrorCode::TravelTooFar, "move exceeds the configured travel limit"};
  }
  if (state.overlap == OverlapPolicy::Forbidden &&
      mark_occupied(state, payload.position, payload.dancer_id)) {
    return Error{ErrorCode::PositionOccupied, "mark is already occupied"};
  }
  it->second.position = payload.position;
  return {};
}

Result<void> apply_formation(ChoreographyState& state, EventType type,
                             const FormationPayload& payload) {
  if (auto created = require_created(state); !created) {
    return created.error();
  }
  auto members = unique_sorted(payload.members);
  if (auto valid = validate_formation_members(state, members); !valid) {
    return valid.error();
  }
  const bool exists = state.formations.find(payload.formation_id.value()) != state.formations.end();
  if (type == EventType::FormationDefined && exists) {
    return Error{ErrorCode::FormationExists, "formation already exists"};
  }
  if (type == EventType::FormationChanged && !exists) {
    return Error{ErrorCode::FormationNotFound, "formation does not exist"};
  }
  state.formations.insert_or_assign(payload.formation_id.value(),
                                    FormationState{std::move(members)});
  return {};
}

Result<void> apply_cue(ChoreographyState& state, std::map<std::string, CueState>& cues,
                       const CuePayload& payload, MusicalTick tick) {
  if (find_cue(state, payload.cue_id) != nullptr) {
    return Error{ErrorCode::CueExists, "cue already exists"};
  }
  if (auto dep = validate_cue_dependency(state, payload, tick); !dep) {
    return dep.error();
  }
  cues.insert({payload.cue_id.value(), CueState{tick, payload.depends_on}});
  return {};
}

}  // namespace

// Build an event after validating against a throwaway copy of state.
Result<Event> propose(const ChoreographyState& state, const Command& command) {
  if (auto schema = require_schema(command.schema_version); !schema) {
    return schema.error();
  }

  const auto seen = state.applied_commands.find(command.id.value());
  if (seen != state.applied_commands.end()) {
    return Error{ErrorCode::DuplicateCommand, "command id has already been accepted"};
  }

  if (command.type != CommandType::CreateChoreography) {
    if (auto created = require_created(state); !created) {
      return created.error();
    }
    if (command.choreography_id != *state.id) {
      return Error{ErrorCode::ChoreographyNotFound, "command targets a different choreography"};
    }
  } else if (state.created) {
    return Error{ErrorCode::ChoreographyExists, "choreography already exists"};
  }

  EventPayload payload = command.payload;
  if (auto* formation = std::get_if<FormationPayload>(&payload)) {
    formation->members = unique_sorted(formation->members);
  }

  ChoreographyState candidate = state;
  auto tentative = make_event(command, state.last_applied.next(), state.term, payload);
  if (!tentative) {
    return tentative.error();
  }
  if (auto applied = apply(candidate, tentative.value()); !applied) {
    return applied.error();
  }
  return tentative.value();
}

// Commit path: check schema, duplicates, index, then apply one event type.
Result<void> apply(ChoreographyState& state, const Event& event, IndexRule index_rule) {
  if (auto schema = require_schema(event.schema_version); !schema) {
    return schema.error();
  }

  const auto seen = state.applied_events.find(event.id.value());
  if (seen != state.applied_events.end()) {
    if (seen->second == canonical_event(event)) {
      return {};
    }
    return Error{ErrorCode::ConflictingEvent, "event id was reused with different content"};
  }

  if (index_rule == IndexRule::Contiguous && event.index != state.last_applied.next()) {
    return Error{ErrorCode::IndexGap, "event index is not contiguous"};
  }
  if (index_rule == IndexRule::Monotonic && !(state.last_applied < event.index)) {
    return Error{ErrorCode::IndexGap, "event index must increase during filtered replay"};
  }

  ChoreographyState candidate = state;
  Result<void> applied{Error{ErrorCode::UnsupportedType, "unhandled event type"}};
  switch (event.type) {
    case EventType::ChoreographyCreated:
      if (const auto* payload = std::get_if<CreateChoreographyPayload>(&event.payload)) {
        applied = apply_create(candidate, event, *payload);
      }
      break;
    case EventType::DancerAdded:
      if (const auto* payload = std::get_if<DancerPayload>(&event.payload)) {
        applied = apply_add(candidate, *payload);
      }
      break;
    case EventType::DancerRemoved:
      if (const auto* payload = std::get_if<RemoveDancerPayload>(&event.payload)) {
        applied = apply_remove(candidate, *payload);
      }
      break;
    case EventType::DancerMoved:
      if (const auto* payload = std::get_if<DancerPayload>(&event.payload)) {
        applied = apply_move(candidate, *payload);
      }
      break;
    case EventType::FormationDefined:
    case EventType::FormationChanged:
      if (const auto* payload = std::get_if<FormationPayload>(&event.payload)) {
        applied = apply_formation(candidate, event.type, *payload);
      }
      break;
    case EventType::MusicCueTriggered:
      if (const auto* payload = std::get_if<CuePayload>(&event.payload)) {
        if (auto created = require_created(candidate); !created) {
          applied = created.error();
        } else {
          applied = apply_cue(candidate, candidate.music_cues, *payload, event.tick);
        }
      }
      break;
    case EventType::LightingCueTriggered:
      if (const auto* payload = std::get_if<CuePayload>(&event.payload)) {
        if (auto created = require_created(candidate); !created) {
          applied = created.error();
        } else {
          applied = apply_cue(candidate, candidate.lighting_cues, *payload, event.tick);
        }
      }
      break;
    case EventType::NoOp:
      applied = {};
      break;
  }

  if (!applied) {
    return applied.error();
  }

  candidate.last_applied = event.index;
  // Every committed event publishes its term into the hashed state. Existing
  // logs are term 1, so this does not move hashes that were already recorded.
  candidate.term = event.term;
  candidate.applied_events.insert({event.id.value(), canonical_event(event)});
  candidate.applied_commands.insert({event.command_id.value(), event.id.value()});
  state = std::move(candidate);
  return {};
}

// Fold events onto an empty state. Optional filters skip later counts/indexes.
Result<ChoreographyState> replay(const std::vector<Event>& events,
                                 std::optional<MusicalTick> through_tick,
                                 std::optional<LogIndex> through_index) {
  ChoreographyState state{};
  for (const auto& event : events) {
    if (through_tick && through_tick.value() < event.tick) {
      continue;
    }
    if (through_index && through_index.value() < event.index) {
      continue;
    }
    const IndexRule rule =
        (through_tick || through_index) ? IndexRule::Monotonic : IndexRule::Contiguous;
    if (auto applied = apply(state, event, rule); !applied) {
      return applied.error();
    }
  }
  return state;
}

// Sorted, line-oriented snapshot of authoritative fields. Used by state_hash().
std::string canonical_state(const ChoreographyState& state) {
  std::ostringstream out;
  out << "schema=" << kCurrentSchemaVersion << '\n';
  out << "created=" << (state.created ? "true" : "false") << '\n';
  out << "choreography=" << (state.id ? state.id->value() : "") << '\n';
  if (state.stage) {
    out << "stage=" << state.stage->width().mm() << 'x' << state.stage->depth().mm() << '\n';
  } else {
    out << "stage=\n";
  }
  out << "overlap=" << overlap_policy_name(state.overlap) << '\n';
  out << "max_travel_mm=" << state.max_travel_mm << '\n';
  out << "last_applied=" << state.last_applied.value() << '\n';
  out << "term=" << state.term.value() << '\n';
  for (const auto& [id, dancer] : state.dancers) {
    out << "dancer=" << id << ':' << dancer.position.x().mm() << ',' << dancer.position.y().mm()
        << ':' << (dancer.active ? "active" : "inactive") << '\n';
  }
  for (const auto& [id, formation] : state.formations) {
    out << "formation=" << id << ':';
    for (std::size_t i = 0; i < formation.members.size(); ++i) {
      if (i != 0) {
        out << ',';
      }
      out << formation.members[i].value();
    }
    out << '\n';
  }
  for (const auto& [id, cue] : state.music_cues) {
    out << "music=" << id << ':' << cue.tick.ticks() << ':'
        << (cue.depends_on ? cue.depends_on->value() : "") << '\n';
  }
  for (const auto& [id, cue] : state.lighting_cues) {
    out << "light=" << id << ':' << cue.tick.ticks() << ':'
        << (cue.depends_on ? cue.depends_on->value() : "") << '\n';
  }
  return out.str();
}

std::string state_hash(const ChoreographyState& state) {
  const std::string canonical = canonical_state(state);
  std::uint64_t hash = kFnvOffset;
  for (unsigned char byte : canonical) {
    hash ^= byte;
    hash *= kFnvPrime;
  }
  std::ostringstream out;
  out << std::hex << std::nouppercase << std::setw(16) << std::setfill('0') << hash;
  return out.str();
}

// Single-node commit: reuse a prior command id, or propose + apply immediately.
Result<SubmitResult> Engine::submit(Command command) {
  const auto seen = state_.applied_commands.find(command.id.value());
  if (seen != state_.applied_commands.end()) {
    for (const auto& event : events_) {
      if (event.id.value() == seen->second) {
        return SubmitResult{event, true};
      }
    }
    return Error{ErrorCode::DuplicateCommand, "command was accepted but its event is missing"};
  }

  auto proposed = propose(state_, command);
  if (!proposed) {
    return proposed.error();
  }
  if (auto applied = apply_committed(proposed.value()); !applied) {
    return applied.error();
  }
  return SubmitResult{proposed.value(), false};
}

Result<void> Engine::apply_committed(const Event& event) {
  if (auto applied = apply(state_, event); !applied) {
    return applied.error();
  }
  events_.push_back(event);
  return {};
}

Result<ChoreographyState> Engine::replay_through(std::optional<MusicalTick> through_tick,
                                                 std::optional<LogIndex> through_index) const {
  return replay(events_, through_tick, through_index);
}

}  // namespace choreoos::state
