#include "choreoos/protocol/command_codec.hpp"

#include <map>
#include <sstream>

namespace choreoos::protocol {
namespace {

using choreoos::state::ChoreographyId;
using choreoos::state::Command;
using choreoos::state::CommandId;
using choreoos::state::CommandType;
using choreoos::state::CreateChoreographyPayload;
using choreoos::state::CueId;
using choreoos::state::CuePayload;
using choreoos::state::DancerId;
using choreoos::state::DancerPayload;
using choreoos::state::Error;
using choreoos::state::ErrorCode;
using choreoos::state::FormationId;
using choreoos::state::FormationPayload;
using choreoos::state::MusicalTick;
using choreoos::state::OverlapPolicy;
using choreoos::state::Position;
using choreoos::state::RemoveDancerPayload;
using choreoos::state::Result;
using choreoos::state::StageBounds;
using Fields = std::map<std::string, std::string>;

Result<Fields> fields_from(std::string_view text) {
  Fields fields;
  std::string current{text};
  std::istringstream stream{current};
  std::string token;
  while (std::getline(stream, token, ';')) {
    if (token.empty()) {
      continue;
    }
    const auto eq = token.find('=');
    if (eq == std::string::npos) {
      return Error{ErrorCode::ProtocolError, "command field is missing '='"};
    }
    fields.insert({token.substr(0, eq), token.substr(eq + 1)});
  }
  return fields;
}

Result<std::string> required(const Fields& fields, const char* key) {
  const auto it = fields.find(key);
  if (it == fields.end()) {
    return Error{ErrorCode::ProtocolError, std::string{"command missing "} + key};
  }
  return it->second;
}

Result<std::int64_t> required_i64(const Fields& fields, const char* key) {
  auto raw = required(fields, key);
  if (!raw) {
    return raw.error();
  }
  try {
    return std::stoll(raw.value());
  } catch (...) {
    return Error{ErrorCode::ProtocolError, std::string{"invalid integer "} + key};
  }
}

std::string members_of(const std::vector<choreoos::state::DancerId>& members) {
  std::string text;
  for (const auto& member : members) {
    if (!text.empty()) {
      text.push_back(',');
    }
    text += member.value();
  }
  return text;
}

}  // namespace

std::string canonical_command(const Command& command) {
  std::ostringstream out;
  out << "schema=" << command.schema_version << ";type=" << command_type_name(command.type)
      << ";cmd=" << command.id.value() << ";choreo=" << command.choreography_id.value()
      << ";tick=" << command.tick.ticks();
  if (const auto* created = std::get_if<CreateChoreographyPayload>(&command.payload)) {
    out << ";width=" << created->stage.width().mm() << ";depth=" << created->stage.depth().mm()
        << ";overlap=" << overlap_policy_name(created->overlap)
        << ";travel=" << created->max_travel_mm;
  } else if (const auto* dancer = std::get_if<DancerPayload>(&command.payload)) {
    out << ";dancer=" << dancer->dancer_id.value() << ";x=" << dancer->position.x().mm()
        << ";y=" << dancer->position.y().mm();
  } else if (const auto* removed = std::get_if<RemoveDancerPayload>(&command.payload)) {
    out << ";dancer=" << removed->dancer_id.value();
  } else if (const auto* formation = std::get_if<FormationPayload>(&command.payload)) {
    out << ";formation=" << formation->formation_id.value()
        << ";members=" << members_of(formation->members);
  } else if (const auto* cue = std::get_if<CuePayload>(&command.payload)) {
    out << ";cue=" << cue->cue_id.value()
        << ";depends=" << (cue->depends_on ? cue->depends_on->value() : "");
  }
  return out.str();
}

Result<Command> parse_command(std::string_view text) {
  auto fields = fields_from(text);
  if (!fields) {
    return fields.error();
  }
  auto schema = required_i64(fields.value(), "schema");
  auto type_name = required(fields.value(), "type");
  auto command_id = required(fields.value(), "cmd");
  auto choreography = required(fields.value(), "choreo");
  auto tick = required_i64(fields.value(), "tick");
  if (!schema || !type_name || !command_id || !choreography || !tick) {
    return Error{ErrorCode::ProtocolError, "command envelope is incomplete"};
  }
  auto parsed_type = choreoos::state::parse_command_type(type_name.value());
  auto parsed_id = CommandId::parse(command_id.value());
  auto parsed_show = ChoreographyId::parse(choreography.value());
  auto parsed_tick = MusicalTick::from_ticks(tick.value());
  if (!parsed_type || !parsed_id || !parsed_show || !parsed_tick) {
    return Error{ErrorCode::ProtocolError, "command envelope is invalid"};
  }

  std::optional<choreoos::state::CommandPayload> payload;
  switch (parsed_type.value()) {
    case CommandType::CreateChoreography: {
      auto width = required_i64(fields.value(), "width");
      auto depth = required_i64(fields.value(), "depth");
      auto travel = required_i64(fields.value(), "travel");
      auto overlap_name = required(fields.value(), "overlap");
      if (!width || !depth || !travel || !overlap_name) {
        return Error{ErrorCode::ProtocolError, "create command is incomplete"};
      }
      auto stage = StageBounds::from_mm(static_cast<std::int32_t>(width.value()),
                                        static_cast<std::int32_t>(depth.value()));
      auto overlap = choreoos::state::parse_overlap_policy(overlap_name.value());
      if (!stage || !overlap) {
        return Error{ErrorCode::ProtocolError, "create command stage is invalid"};
      }
      payload = CreateChoreographyPayload{parsed_show.value(), stage.value(), overlap.value(),
                                          static_cast<std::int32_t>(travel.value())};
      break;
    }
    case CommandType::AddDancer:
    case CommandType::MoveDancer: {
      auto dancer = required(fields.value(), "dancer");
      auto x = required_i64(fields.value(), "x");
      auto y = required_i64(fields.value(), "y");
      if (!dancer || !x || !y) {
        return Error{ErrorCode::ProtocolError, "dancer command is incomplete"};
      }
      auto id = DancerId::parse(dancer.value());
      auto position = Position::from_mm(static_cast<std::int32_t>(x.value()),
                                        static_cast<std::int32_t>(y.value()));
      if (!id || !position) {
        return Error{ErrorCode::ProtocolError, "dancer command is invalid"};
      }
      payload = DancerPayload{id.value(), position.value()};
      break;
    }
    case CommandType::RemoveDancer: {
      auto dancer = required(fields.value(), "dancer");
      if (!dancer) {
        return dancer.error();
      }
      auto id = DancerId::parse(dancer.value());
      if (!id) {
        return id.error();
      }
      payload = RemoveDancerPayload{id.value()};
      break;
    }
    case CommandType::DefineFormation:
    case CommandType::ChangeFormation: {
      auto formation = required(fields.value(), "formation");
      auto members_raw = required(fields.value(), "members");
      if (!formation || !members_raw) {
        return Error{ErrorCode::ProtocolError, "formation command is incomplete"};
      }
      auto id = FormationId::parse(formation.value());
      if (!id) {
        return id.error();
      }
      std::vector<DancerId> members;
      std::istringstream stream{members_raw.value()};
      std::string item;
      while (std::getline(stream, item, ',')) {
        auto member = DancerId::parse(item);
        if (!member) {
          return member.error();
        }
        members.push_back(member.value());
      }
      payload = FormationPayload{id.value(), std::move(members)};
      break;
    }
    case CommandType::TriggerMusicCue:
    case CommandType::TriggerLightingCue: {
      auto cue = required(fields.value(), "cue");
      if (!cue) {
        return cue.error();
      }
      auto id = CueId::parse(cue.value());
      if (!id) {
        return id.error();
      }
      std::optional<CueId> depends;
      const auto depends_it = fields.value().find("depends");
      if (depends_it != fields.value().end() && !depends_it->second.empty()) {
        auto parsed = CueId::parse(depends_it->second);
        if (!parsed) {
          return parsed.error();
        }
        depends = parsed.value();
      }
      payload = CuePayload{id.value(), depends};
      break;
    }
  }

  return Command{parsed_id.value(),   parsed_show.value(),
                 parsed_type.value(), static_cast<std::uint16_t>(schema.value()),
                 parsed_tick.value(), std::move(payload.value())};
}

}  // namespace choreoos::protocol
