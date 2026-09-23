// Names and the canonical event text format:
//   schema=1;type=DANCER_MOVED;event=...;cmd=...;choreo=...;tick=...;index=...

#include "choreoos/state/model.hpp"

#include <sstream>

namespace choreoos::state {
namespace {

void write_members(std::ostringstream& out, const std::vector<DancerId>& members) {
  for (std::size_t i = 0; i < members.size(); ++i) {
    if (i != 0) {
      out << ',';
    }
    out << members[i].value();
  }
}

}  // namespace

const char* command_type_name(CommandType type) noexcept {
  switch (type) {
    case CommandType::CreateChoreography:
      return "CREATE_CHOREOGRAPHY";
    case CommandType::AddDancer:
      return "ADD_DANCER";
    case CommandType::RemoveDancer:
      return "REMOVE_DANCER";
    case CommandType::MoveDancer:
      return "MOVE_DANCER";
    case CommandType::DefineFormation:
      return "DEFINE_FORMATION";
    case CommandType::ChangeFormation:
      return "CHANGE_FORMATION";
    case CommandType::TriggerMusicCue:
      return "TRIGGER_MUSIC_CUE";
    case CommandType::TriggerLightingCue:
      return "TRIGGER_LIGHTING_CUE";
  }
  return "UNKNOWN";
}

const char* event_type_name(EventType type) noexcept {
  switch (type) {
    case EventType::ChoreographyCreated:
      return "CHOREOGRAPHY_CREATED";
    case EventType::DancerAdded:
      return "DANCER_ADDED";
    case EventType::DancerRemoved:
      return "DANCER_REMOVED";
    case EventType::DancerMoved:
      return "DANCER_MOVED";
    case EventType::FormationDefined:
      return "FORMATION_DEFINED";
    case EventType::FormationChanged:
      return "FORMATION_CHANGED";
    case EventType::MusicCueTriggered:
      return "MUSIC_CUE_TRIGGERED";
    case EventType::LightingCueTriggered:
      return "LIGHTING_CUE_TRIGGERED";
  }
  return "UNKNOWN";
}

const char* overlap_policy_name(OverlapPolicy policy) noexcept {
  return policy == OverlapPolicy::Allowed ? "allowed" : "forbidden";
}

Result<CommandType> parse_command_type(std::string_view name) {
  if (name == "CREATE_CHOREOGRAPHY") {
    return CommandType::CreateChoreography;
  }
  if (name == "ADD_DANCER") {
    return CommandType::AddDancer;
  }
  if (name == "REMOVE_DANCER") {
    return CommandType::RemoveDancer;
  }
  if (name == "MOVE_DANCER") {
    return CommandType::MoveDancer;
  }
  if (name == "DEFINE_FORMATION") {
    return CommandType::DefineFormation;
  }
  if (name == "CHANGE_FORMATION") {
    return CommandType::ChangeFormation;
  }
  if (name == "TRIGGER_MUSIC_CUE") {
    return CommandType::TriggerMusicCue;
  }
  if (name == "TRIGGER_LIGHTING_CUE") {
    return CommandType::TriggerLightingCue;
  }
  return Error{ErrorCode::UnsupportedType, "unknown command type"};
}

Result<EventType> parse_event_type(std::string_view name) {
  if (name == "CHOREOGRAPHY_CREATED") {
    return EventType::ChoreographyCreated;
  }
  if (name == "DANCER_ADDED") {
    return EventType::DancerAdded;
  }
  if (name == "DANCER_REMOVED") {
    return EventType::DancerRemoved;
  }
  if (name == "DANCER_MOVED") {
    return EventType::DancerMoved;
  }
  if (name == "FORMATION_DEFINED") {
    return EventType::FormationDefined;
  }
  if (name == "FORMATION_CHANGED") {
    return EventType::FormationChanged;
  }
  if (name == "MUSIC_CUE_TRIGGERED") {
    return EventType::MusicCueTriggered;
  }
  if (name == "LIGHTING_CUE_TRIGGERED") {
    return EventType::LightingCueTriggered;
  }
  return Error{ErrorCode::UnsupportedType, "unknown event type"};
}

Result<OverlapPolicy> parse_overlap_policy(std::string_view name) {
  if (name == "allowed") {
    return OverlapPolicy::Allowed;
  }
  if (name == "forbidden") {
    return OverlapPolicy::Forbidden;
  }
  return Error{ErrorCode::UnsupportedType, "unknown overlap policy"};
}

EventType event_type_for(CommandType type) noexcept {
  switch (type) {
    case CommandType::CreateChoreography:
      return EventType::ChoreographyCreated;
    case CommandType::AddDancer:
      return EventType::DancerAdded;
    case CommandType::RemoveDancer:
      return EventType::DancerRemoved;
    case CommandType::MoveDancer:
      return EventType::DancerMoved;
    case CommandType::DefineFormation:
      return EventType::FormationDefined;
    case CommandType::ChangeFormation:
      return EventType::FormationChanged;
    case CommandType::TriggerMusicCue:
      return EventType::MusicCueTriggered;
    case CommandType::TriggerLightingCue:
      return EventType::LightingCueTriggered;
  }
  return EventType::ChoreographyCreated;
}

std::string canonical_event(const Event& event) {
  std::ostringstream out;
  out << "schema=" << event.schema_version << ";type=" << event_type_name(event.type)
      << ";event=" << event.id.value() << ";cmd=" << event.command_id.value()
      << ";choreo=" << event.choreography_id.value() << ";tick=" << event.tick.ticks()
      << ";index=" << event.index.value() << ";term=" << event.term.value();

  if (const auto* created = std::get_if<CreateChoreographyPayload>(&event.payload)) {
    out << ";width=" << created->stage.width().mm() << ";depth=" << created->stage.depth().mm()
        << ";overlap=" << overlap_policy_name(created->overlap)
        << ";travel=" << created->max_travel_mm;
  } else if (const auto* dancer = std::get_if<DancerPayload>(&event.payload)) {
    out << ";dancer=" << dancer->dancer_id.value() << ";x=" << dancer->position.x().mm()
        << ";y=" << dancer->position.y().mm();
  } else if (const auto* removed = std::get_if<RemoveDancerPayload>(&event.payload)) {
    out << ";dancer=" << removed->dancer_id.value();
  } else if (const auto* formation = std::get_if<FormationPayload>(&event.payload)) {
    out << ";formation=" << formation->formation_id.value() << ";members=";
    write_members(out, formation->members);
  } else if (const auto* cue = std::get_if<CuePayload>(&event.payload)) {
    out << ";cue=" << cue->cue_id.value() << ";depends=";
    if (cue->depends_on) {
      out << cue->depends_on->value();
    }
  }
  return out.str();
}

}  // namespace choreoos::state
