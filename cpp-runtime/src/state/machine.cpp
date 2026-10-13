// Invariants, propose/apply, replay, and hashing.
// apply() copies state first; a failed event never leaves a partial update.

#include "choreoos/state/machine.hpp"

#include <algorithm>
#include <iomanip>
#include <sstream>

#include "choreoos/protocol/command_codec.hpp"
#include "choreoos/state/store.hpp"

namespace choreoos::state {
namespace {

// FNV-1a 64-bit over canonical_state(). Same bytes => same hash on every node.
constexpr std::uint64_t kFnvOffset = 14695981039346656037ull;
constexpr std::uint64_t kFnvPrime = 1099511628211ull;

constexpr char kBase64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

std::string encode_fingerprint(std::uint64_t value) {
  std::string text(11, 'A');
  for (int i = 10; i >= 0; --i) {
    text[static_cast<std::size_t>(i)] = kBase64[value & 63u];
    value >>= 6;
  }
  return text;
}

Result<std::uint64_t> decode_fingerprint(std::string_view text) {
  if (text.size() != 11) {
    return Error{ErrorCode::StoreError, "receipt fingerprint length is invalid"};
  }
  std::uint64_t value = 0;
  for (std::size_t i = 0; i < text.size(); ++i) {
    const auto* found = std::find(std::begin(kBase64), std::end(kBase64) - 1, text[i]);
    if (found == std::end(kBase64) - 1) {
      return Error{ErrorCode::StoreError, "receipt fingerprint alphabet is invalid"};
    }
    const auto digit = static_cast<std::uint64_t>(found - std::begin(kBase64));
    if (i == 0 && digit > 15) {
      return Error{ErrorCode::StoreError, "receipt fingerprint overflows 64 bits"};
    }
    value = (value << 6) | digit;
  }
  return value;
}

Command normalize_command(Command command) {
  if (auto* formation = std::get_if<FormationPayload>(&command.payload)) {
    std::sort(formation->members.begin(), formation->members.end());
    formation->members.erase(std::unique(formation->members.begin(), formation->members.end()),
                             formation->members.end());
  }
  return command;
}

// FNV-1a is deterministic across platforms. This short fingerprint is not a
// security primitive: an attacker must not be allowed to choose untrusted IDs
// without authentication and stronger collision-resistant identities.
std::uint64_t command_fingerprint(const Command& command) {
  const auto bytes = choreoos::protocol::canonical_command(normalize_command(command));
  std::uint64_t hash = kFnvOffset;
  for (unsigned char byte : bytes) {
    hash ^= byte;
    hash *= kFnvPrime;
  }
  return hash;
}

std::optional<Command> command_from_event(const Event& event) {
  if (event.type == EventType::NoOp) {
    return std::nullopt;
  }
  CommandType type = CommandType::CreateChoreography;
  switch (event.type) {
    case EventType::ChoreographyCreated:
      type = CommandType::CreateChoreography;
      break;
    case EventType::DancerAdded:
      type = CommandType::AddDancer;
      break;
    case EventType::DancerRemoved:
      type = CommandType::RemoveDancer;
      break;
    case EventType::DancerMoved:
      type = CommandType::MoveDancer;
      break;
    case EventType::FormationDefined:
      type = CommandType::DefineFormation;
      break;
    case EventType::FormationChanged:
      type = CommandType::ChangeFormation;
      break;
    case EventType::MusicCueTriggered:
      type = CommandType::TriggerMusicCue;
      break;
    case EventType::LightingCueTriggered:
      type = CommandType::TriggerLightingCue;
      break;
    case EventType::NoOp:
      return std::nullopt;
  }
  return Command{event.command_id, event.choreography_id, type, event.schema_version,
                 event.tick,       event.payload};
}

EventReceipt receipt_from_event(const Event& event) {
  const auto command = command_from_event(event);
  return EventReceipt{event.index.value(), event.term.value(),
                      command ? command_fingerprint(*command) : 0};
}

Error unsupported_schema(std::uint16_t version) {
  return Error{ErrorCode::UnsupportedVersion,
               "unsupported schema version " + std::to_string(version)};
}

// Copy only the mutable domain state, NOT the append-only applied_events and
// applied_commands logs. Those logs grow with every event ever applied, so
// copying them makes every apply or propose O(history).
ChoreographyState domain_copy(const ChoreographyState& state) {
  ChoreographyState copy;
  copy.created = state.created;
  copy.id = state.id;
  copy.stage = state.stage;
  copy.overlap = state.overlap;
  copy.max_travel_mm = state.max_travel_mm;
  copy.last_applied = state.last_applied;
  copy.term = state.term;
  copy.dancers = state.dancers;
  copy.formations = state.formations;
  copy.music_cues = state.music_cues;
  copy.lighting_cues = state.lighting_cues;
  return copy;
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

  // propose() already rejected a reused command id (event id == command id),
  // so the candidate does not need the history logs.
  ChoreographyState candidate = domain_copy(state);
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

  // Full event history is not retained in v3 snapshots. Check compact
  // receipts too, or a replay after restart could reapply an old command
  // under a new index even though its event body was compacted.
  const auto receipt = state.receipts.find(event.command_id.value());
  if (receipt != state.receipts.end()) {
    const auto original = command_from_event(event);
    if (original && receipt->second.index == event.index.value() &&
        receipt->second.term == event.term.value() &&
        receipt->second.fingerprint == command_fingerprint(*original)) {
      return {};
    }
    return Error{ErrorCode::ConflictingEvent,
                 "committed command id was reused with different event content"};
  }
  if (state.applied_commands.count(event.command_id.value()) != 0) {
    return Error{ErrorCode::ConflictingEvent,
                 "legacy committed command id was replayed without an outcome receipt"};
  }

  if (index_rule == IndexRule::Contiguous && event.index != state.last_applied.next()) {
    return Error{ErrorCode::IndexGap, "event index is not contiguous"};
  }
  if (index_rule == IndexRule::Monotonic && !(state.last_applied < event.index)) {
    return Error{ErrorCode::IndexGap, "event index must increase during filtered replay"};
  }

  // The mutation below never reads the history logs (duplicate checks use
  // `state` directly above), so they are updated in place on success. On
  // failure `state` is untouched, preserving the transactional guarantee.
  ChoreographyState candidate = domain_copy(state);
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
  // Success path: move the mutated domain state back, then append to the
  // logs in place (O(log n) inserts, no O(history) copy).
  state.created = candidate.created;
  state.id = std::move(candidate.id);
  state.stage = std::move(candidate.stage);
  state.overlap = candidate.overlap;
  state.max_travel_mm = candidate.max_travel_mm;
  state.dancers = std::move(candidate.dancers);
  state.formations = std::move(candidate.formations);
  state.music_cues = std::move(candidate.music_cues);
  state.lighting_cues = std::move(candidate.lighting_cues);
  state.last_applied = event.index;
  state.term = event.term;
  state.applied_events.insert({event.id.value(), canonical_event(event)});
  state.applied_commands.insert({event.command_id.value(), event.id.value()});
  if (event.type != EventType::NoOp) {
    state.receipts[event.command_id.value()] = receipt_from_event(event);
  }
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
Result<Event> resolve_receipt(const ChoreographyState& state, const Command& command) {
  const auto it = state.receipts.find(command.id.value());
  if (it == state.receipts.end()) {
    return Error{ErrorCode::OutcomeUnavailable,
                 "committed legacy command has no recoverable receipt"};
  }
  if (command_fingerprint(command) != it->second.fingerprint) {
    return Error{ErrorCode::CommandConflict, "operation id reused with different command"};
  }
  const auto normalized = normalize_command(command);
  auto event_id = EventId::parse(command.id.value());
  auto index = LogIndex::parse(it->second.index);
  auto term = Term::parse(it->second.term);
  if (!event_id) return event_id.error();
  if (!index) return index.error();
  if (!term) return term.error();
  return Event{
      event_id.value(),       command.id,   command.choreography_id, event_type_for(command.type),
      command.schema_version, command.tick, index.value(),           term.value(),
      normalized.payload};
}

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

std::string snapshot_payload(const ChoreographyState& state) {
  std::ostringstream out;
  out << "snapshot-state 3\n";
  out << "schema " << kCurrentSchemaVersion << '\n';
  out << "created " << (state.created ? 1 : 0) << '\n';
  out << "choreography " << (state.id ? state.id->value() : "-") << '\n';
  if (state.stage) {
    out << "stage " << state.stage->width().mm() << ' ' << state.stage->depth().mm() << '\n';
  } else {
    out << "stage -\n";
  }
  out << "overlap " << overlap_policy_name(state.overlap) << '\n';
  out << "max_travel " << state.max_travel_mm << '\n';
  out << "last_applied " << state.last_applied.value() << '\n';
  out << "term " << state.term.value() << '\n';
  for (const auto& [id, dancer] : state.dancers) {
    out << "dancer " << id << ' ' << dancer.position.x().mm() << ' ' << dancer.position.y().mm()
        << ' ' << (dancer.active ? 1 : 0) << '\n';
  }
  for (const auto& [id, formation] : state.formations) {
    out << "formation " << id;
    for (const auto& member : formation.members) {
      out << ' ' << member.value();
    }
    out << '\n';
  }
  for (const auto& [id, cue] : state.music_cues) {
    out << "music " << id << ' ' << cue.tick.ticks() << ' '
        << (cue.depends_on ? cue.depends_on->value() : "-") << '\n';
  }
  for (const auto& [id, cue] : state.lighting_cues) {
    out << "light " << id << ' ' << cue.tick.ticks() << ' '
        << (cue.depends_on ? cue.depends_on->value() : "-") << '\n';
  }
  // Store only compact receipts, sorted by committed log index. The index
  // advances implicitly; gaps (e.g. election no-ops) use a cursor marker.
  std::vector<std::pair<std::string, EventReceipt>> ordered(state.receipts.begin(),
                                                            state.receipts.end());
  std::sort(ordered.begin(), ordered.end(),
            [](const auto& a, const auto& b) { return a.second.index < b.second.index; });
  std::uint64_t cursor = 0;
  std::uint64_t active_term = 1;
  for (const auto& [id, receipt] : ordered) {
    if (receipt.index != cursor + 1) {
      out << "at " << receipt.index - 1 << '\n';
    }
    if (receipt.term != active_term) {
      out << "receipt-term " << receipt.term << '\n';
      active_term = receipt.term;
    }
    out << "r " << id << ' ' << encode_fingerprint(receipt.fingerprint) << '\n';
    cursor = receipt.index;
  }
  // Only legacy records without fingerprints need an explicit tombstone.
  for (const auto& [id, event_id] : state.applied_commands) {
    if (state.receipts.find(id) == state.receipts.end() && event_id.rfind("noop-", 0) != 0) {
      out << "legacy " << id << '\n';
    }
  }
  return out.str();
}

Result<ChoreographyState> restore_state(std::string_view payload) {
  const auto bad = [](const std::string& why) {
    return Error{ErrorCode::StoreError, "snapshot state is invalid: " + why};
  };
  ChoreographyState state;
  std::istringstream lines{std::string{payload}};
  std::string line;
  bool header = false;
  std::string format_version;
  std::uint64_t receipt_index = 0;
  std::uint64_t receipt_term = 1;
  while (std::getline(lines, line)) {
    if (line.empty()) {
      continue;
    }
    std::istringstream fields{line};
    std::string key;
    fields >> key;
    if (!header) {
      std::string version;
      fields >> version;
      if (key != "snapshot-state" || (version != "1" && version != "2" && version != "3")) {
        return bad("missing or unsupported header");
      }
      format_version = version;
      header = true;
      continue;
    }
    if (key == "schema") {
      std::uint32_t schema = 0;
      fields >> schema;
      if (schema != kCurrentSchemaVersion) {
        return unsupported_schema(static_cast<std::uint16_t>(schema));
      }
    } else if (key == "created") {
      int value = 0;
      fields >> value;
      state.created = value != 0;
    } else if (key == "choreography") {
      std::string id;
      fields >> id;
      if (id != "-") {
        auto parsed = ChoreographyId::parse(id);
        if (!parsed) {
          return parsed.error();
        }
        state.id = parsed.value();
      }
    } else if (key == "stage") {
      std::string width;
      fields >> width;
      if (width != "-") {
        std::int32_t width_mm = 0;
        std::int32_t depth = 0;
        std::istringstream{width} >> width_mm;
        fields >> depth;
        auto stage = StageBounds::from_mm(width_mm, depth);
        if (!stage) {
          return stage.error();
        }
        state.stage = stage.value();
      }
    } else if (key == "overlap") {
      std::string name;
      fields >> name;
      auto policy = parse_overlap_policy(name);
      if (!policy) {
        return policy.error();
      }
      state.overlap = policy.value();
    } else if (key == "max_travel") {
      fields >> state.max_travel_mm;
    } else if (key == "last_applied") {
      std::uint64_t value = 0;
      fields >> value;
      state.last_applied = LogIndex::parse(value).value();
    } else if (key == "term") {
      std::uint64_t value = 0;
      fields >> value;
      auto term = Term::parse(value);
      if (!term) {
        return term.error();
      }
      state.term = term.value();
    } else if (key == "dancer") {
      std::string id;
      std::int32_t x = 0;
      std::int32_t y = 0;
      int active = 0;
      fields >> id >> x >> y >> active;
      auto position = Position::from_mm(x, y);
      if (!position) {
        return position.error();
      }
      state.dancers.insert_or_assign(id, DancerState{position.value(), active != 0});
    } else if (key == "formation") {
      std::string id;
      fields >> id;
      FormationState formation;
      std::string member;
      while (fields >> member) {
        auto parsed = DancerId::parse(member);
        if (!parsed) {
          return parsed.error();
        }
        formation.members.push_back(parsed.value());
      }
      state.formations[id] = std::move(formation);
    } else if (key == "music" || key == "light") {
      std::string id;
      std::int64_t ticks = 0;
      std::string depends;
      fields >> id >> ticks >> depends;
      auto tick = MusicalTick::from_ticks(ticks);
      if (!tick) {
        return tick.error();
      }
      CueState cue{tick.value(), std::nullopt};
      if (depends != "-" && !depends.empty()) {
        auto parsed = CueId::parse(depends);
        if (!parsed) {
          return parsed.error();
        }
        cue.depends_on = parsed.value();
      }
      (key == "music" ? state.music_cues : state.lighting_cues).insert_or_assign(id, cue);
    } else if (key == "at" && format_version == "3") {
      std::uint64_t cursor = 0;
      fields >> cursor;
      if (fields.fail() || cursor <= receipt_index || cursor >= state.last_applied.value()) {
        return bad("receipt cursor is invalid");
      }
      receipt_index = cursor;
    } else if (key == "receipt-term" && format_version == "3") {
      fields >> receipt_term;
      if (fields.fail() || receipt_term == 0) {
        return bad("receipt term is invalid");
      }
    } else if (key == "r" && format_version == "3") {
      std::string id;
      std::string fingerprint;
      fields >> id >> fingerprint;
      auto parsed_id = CommandId::parse(id);
      auto decoded = decode_fingerprint(fingerprint);
      if (!parsed_id || !decoded || receipt_index >= state.last_applied.value() ||
          state.receipts.count(id) != 0) {
        return bad("compact receipt is invalid or repeated");
      }
      const auto index = ++receipt_index;
      state.receipts.emplace(id, EventReceipt{index, receipt_term, decoded.value()});
      state.applied_commands[id] = id;
    } else if (key == "legacy" && format_version == "3") {
      std::string id;
      fields >> id;
      if (!CommandId::parse(id) || state.applied_commands.count(id) != 0) {
        return bad("legacy command id is invalid or repeated");
      }
      state.applied_commands[id] = id;
    } else if (key == "receipt" && format_version == "2") {
      std::string event_id;
      std::string canonical;
      fields >> event_id >> canonical;
      if (event_id.empty() || canonical.empty()) {
        return bad("receipt is missing an event id or payload");
      }
      auto event = parse_event(canonical);
      if (!event || event.value().id.value() != event_id) {
        return bad("legacy full receipt is malformed");
      }
      state.applied_events[event_id] = canonical;
      if (event.value().type != EventType::NoOp) {
        state.receipts[event.value().command_id.value()] = receipt_from_event(event.value());
      }
    } else if (key == "applied") {
      std::string command;
      std::string event;
      fields >> command >> event;
      state.applied_commands[command] = event.empty() ? command : event;
    } else {
      return bad("unknown line '" + key + "'");
    }
    if (fields.fail() && !fields.eof()) {
      return bad("malformed line '" + key + "'");
    }
  }
  if (!header) {
    return bad("empty payload");
  }
  return state;
}

// Single-node commit: reuse a prior command id, or propose + apply immediately.
Result<SubmitResult> Engine::submit(Command command) {
  const auto seen = state_.applied_commands.find(command.id.value());
  if (seen != state_.applied_commands.end()) {
    auto receipt = resolve_receipt(state_, command);
    if (!receipt) {
      return receipt.error();
    }
    return SubmitResult{receipt.value(), true};
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

void Engine::restore(ChoreographyState state, std::vector<Event> history) {
  state_ = std::move(state);
  events_ = std::move(history);
  // Legacy v1 snapshots can recover receipts for history that still exists.
  for (const auto& event : events_) {
    if (event.type != EventType::NoOp) {
      state_.receipts[event.command_id.value()] = receipt_from_event(event);
    }
  }
}

Result<ChoreographyState> Engine::replay_through(std::optional<MusicalTick> through_tick,
                                                 std::optional<LogIndex> through_index) const {
  return replay(events_, through_tick, through_index);
}

}  // namespace choreoos::state
