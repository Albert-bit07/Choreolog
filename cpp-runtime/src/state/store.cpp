// Parse and write canonical event lines. Not crash-safe; that is Milestone 2.

#include "choreoos/state/store.hpp"

#include <cstdint>
#include <fstream>
#include <map>
#include <sstream>
#include <system_error>

#include "choreoos/storage/metadata.hpp"
#include "choreoos/storage/snapshot.hpp"

namespace choreoos::state {
namespace {

using Fields = std::map<std::string, std::string>;

Result<Fields> parse_fields(std::string_view line) {
  Fields fields;
  std::string current{line};
  std::istringstream stream{current};
  std::string token;
  while (std::getline(stream, token, ';')) {
    if (token.empty()) {
      continue;
    }
    const auto eq = token.find('=');
    if (eq == std::string::npos) {
      return Error{ErrorCode::StoreError, "event record is missing '='"};
    }
    fields.insert({token.substr(0, eq), token.substr(eq + 1)});
  }
  return fields;
}

Result<std::string> require_field(const Fields& fields, const char* key) {
  const auto it = fields.find(key);
  if (it == fields.end()) {
    return Error{ErrorCode::StoreError, std::string{"event record missing field "} + key};
  }
  return it->second;
}

Result<std::int64_t> parse_i64(const Fields& fields, const char* key) {
  auto raw = require_field(fields, key);
  if (!raw) {
    return raw.error();
  }
  try {
    return std::stoll(raw.value());
  } catch (...) {
    return Error{ErrorCode::StoreError, std::string{"invalid integer field "} + key};
  }
}

Result<std::uint64_t> parse_u64(const Fields& fields, const char* key) {
  auto raw = require_field(fields, key);
  if (!raw) {
    return raw.error();
  }
  try {
    return std::stoull(raw.value());
  } catch (...) {
    return Error{ErrorCode::StoreError, std::string{"invalid unsigned field "} + key};
  }
}

std::vector<DancerId> parse_members(std::string_view raw) {
  std::vector<DancerId> members;
  if (raw.empty()) {
    return members;
  }
  std::string text{raw};
  std::istringstream stream{text};
  std::string item;
  while (std::getline(stream, item, ',')) {
    auto id = DancerId::parse(item);
    if (id) {
      members.push_back(id.value());
    }
  }
  return members;
}

}  // namespace

Result<Event> parse_event(std::string_view line) {
  auto fields = parse_fields(line);
  if (!fields) {
    return fields.error();
  }

  auto schema_raw = parse_u64(fields.value(), "schema");
  auto type_raw = require_field(fields.value(), "type");
  auto event_raw = require_field(fields.value(), "event");
  auto cmd_raw = require_field(fields.value(), "cmd");
  auto choreo_raw = require_field(fields.value(), "choreo");
  auto tick_raw = parse_i64(fields.value(), "tick");
  auto index_raw = parse_u64(fields.value(), "index");
  auto term_raw = parse_u64(fields.value(), "term");
  if (!schema_raw || !type_raw || !event_raw || !cmd_raw || !choreo_raw || !tick_raw ||
      !index_raw || !term_raw) {
    return Error{ErrorCode::StoreError, "event record is missing required envelope fields"};
  }

  auto type = parse_event_type(type_raw.value());
  auto event_id = EventId::parse(event_raw.value());
  auto command_id = CommandId::parse(cmd_raw.value());
  auto choreography_id = ChoreographyId::parse(choreo_raw.value());
  auto tick = MusicalTick::from_ticks(tick_raw.value());
  auto index = LogIndex::parse(index_raw.value());
  auto term = Term::parse(term_raw.value());
  if (!type || !event_id || !command_id || !choreography_id || !tick || !index || !term) {
    return Error{ErrorCode::StoreError, "event envelope contains invalid values"};
  }

  EventPayload payload = CreateChoreographyPayload{
      choreography_id.value(), StageBounds::from_mm(1, 1).value(), OverlapPolicy::Forbidden, 0};
  switch (type.value()) {
    case EventType::ChoreographyCreated: {
      auto width = parse_i64(fields.value(), "width");
      auto depth = parse_i64(fields.value(), "depth");
      auto overlap_raw = require_field(fields.value(), "overlap");
      auto travel = parse_i64(fields.value(), "travel");
      if (!width || !depth || !overlap_raw || !travel) {
        return Error{ErrorCode::StoreError, "create event is missing stage fields"};
      }
      auto stage = StageBounds::from_mm(static_cast<std::int32_t>(width.value()),
                                        static_cast<std::int32_t>(depth.value()));
      auto overlap = parse_overlap_policy(overlap_raw.value());
      if (!stage || !overlap) {
        return Error{ErrorCode::StoreError, "create event has invalid stage fields"};
      }
      payload = CreateChoreographyPayload{choreography_id.value(), stage.value(), overlap.value(),
                                          static_cast<std::int32_t>(travel.value())};
      break;
    }
    case EventType::DancerAdded:
    case EventType::DancerMoved: {
      auto dancer_raw = require_field(fields.value(), "dancer");
      auto x = parse_i64(fields.value(), "x");
      auto y = parse_i64(fields.value(), "y");
      if (!dancer_raw || !x || !y) {
        return Error{ErrorCode::StoreError, "dancer event is missing fields"};
      }
      auto dancer = DancerId::parse(dancer_raw.value());
      auto position = Position::from_mm(static_cast<std::int32_t>(x.value()),
                                        static_cast<std::int32_t>(y.value()));
      if (!dancer || !position) {
        return Error{ErrorCode::StoreError, "dancer event has invalid fields"};
      }
      payload = DancerPayload{dancer.value(), position.value()};
      break;
    }
    case EventType::DancerRemoved: {
      auto dancer_raw = require_field(fields.value(), "dancer");
      if (!dancer_raw) {
        return dancer_raw.error();
      }
      auto dancer = DancerId::parse(dancer_raw.value());
      if (!dancer) {
        return dancer.error();
      }
      payload = RemoveDancerPayload{dancer.value()};
      break;
    }
    case EventType::FormationDefined:
    case EventType::FormationChanged: {
      auto formation_raw = require_field(fields.value(), "formation");
      auto members_raw = require_field(fields.value(), "members");
      if (!formation_raw || !members_raw) {
        return Error{ErrorCode::StoreError, "formation event is missing fields"};
      }
      auto formation = FormationId::parse(formation_raw.value());
      if (!formation) {
        return formation.error();
      }
      payload = FormationPayload{formation.value(), parse_members(members_raw.value())};
      break;
    }
    case EventType::MusicCueTriggered:
    case EventType::LightingCueTriggered: {
      auto cue_raw = require_field(fields.value(), "cue");
      if (!cue_raw) {
        return cue_raw.error();
      }
      auto cue = CueId::parse(cue_raw.value());
      if (!cue) {
        return cue.error();
      }
      std::optional<CueId> depends;
      const auto depends_it = fields.value().find("depends");
      if (depends_it != fields.value().end() && !depends_it->second.empty()) {
        auto dep = CueId::parse(depends_it->second);
        if (!dep) {
          return dep.error();
        }
        depends = dep.value();
      }
      payload = CuePayload{cue.value(), depends};
      break;
    }
    case EventType::NoOp:
      break;
  }

  return Event{event_id.value(),
               command_id.value(),
               choreography_id.value(),
               type.value(),
               static_cast<std::uint16_t>(schema_raw.value()),
               tick.value(),
               index.value(),
               term.value(),
               std::move(payload)};
}

std::filesystem::path event_log_path(const std::filesystem::path& directory) {
  return directory / "wal.bin";
}

FileEngine::FileEngine(std::filesystem::path directory, StoreOptions options)
    : directory_(std::move(directory)), options_(options), mutex_(std::make_unique<std::mutex>()) {}

const storage::WalStats& FileEngine::wal_stats() const { return wal_->stats(); }

Result<FileEngine> FileEngine::open(std::filesystem::path directory, StoreOptions options) {
  std::error_code ec;
  std::filesystem::create_directories(directory, ec);
  if (ec) {
    return Error{ErrorCode::StoreError, "unable to create store directory"};
  }
  FileEngine store{std::move(directory), options};
  auto wal = storage::WriteAheadLog::open(event_log_path(store.directory_));
  if (!wal) {
    return wal.error();
  }
  store.wal_ = std::make_unique<storage::WriteAheadLog>(std::move(wal.value()));
  store.recovery_.truncated_tail_bytes = store.wal_->stats().truncated_tail_bytes;

  auto snapshot = storage::load_latest_snapshot(store.directory_);
  if (!snapshot) {
    return snapshot.error();
  }
  auto metadata = storage::load_metadata(store.directory_ / "meta.bin");
  if (!metadata) {
    return metadata.error();
  }

  // Cluster recovery applies only through the durable commit index. Later WAL
  // records stay on disk so a leader can finish replicating them, and a
  // follower can still truncate them if they conflict.
  const std::uint64_t apply_limit =
      options.commit_on_append ? UINT64_MAX : metadata.value().commit_index.value();
  std::uint64_t start_after = 0;
  if (snapshot.value() && snapshot.value()->index.value() <= apply_limit) {
    for (const auto& event : snapshot.value()->events) {
      if (auto applied = store.engine_.apply_committed(event); !applied) {
        return applied.error();
      }
    }
    store.recovery_.used_snapshot = true;
    store.recovery_.snapshot_index = snapshot.value()->index.value();
    store.snapshot_ = snapshot.value();
    start_after = snapshot.value()->index.value();
  }
  for (const auto& event : store.wal_->events()) {
    if (event.index.value() <= start_after) {
      continue;
    }
    if (event.index.value() > apply_limit) {
      break;
    }
    if (auto applied = store.engine_.apply_committed(event); !applied) {
      return applied.error();
    }
    ++store.recovery_.replayed_after_snapshot;
  }

  const auto commit =
      options.commit_on_append ? store.engine_.state().last_applied : metadata.value().commit_index;
  store.commit_index_ = commit;
  if (options.commit_on_append && (metadata.value().commit_index < commit ||
                                   metadata.value().term != store.engine_.state().term)) {
    storage::NodeMetadata updated = metadata.value();
    updated.commit_index = commit;
    updated.term = store.engine_.state().term;
    if (auto stored = storage::store_metadata(store.directory_ / "meta.bin", updated,
                                              options.durability == storage::Durability::Sync);
        !stored) {
      return stored.error();
    }
  }
  store.recovery_.commit_index = commit.value();
  store.recovery_.state_hash = state_hash(store.engine_.state());
  return store;
}

Result<SubmitResult> FileEngine::submit(Command command) {
  std::lock_guard<std::mutex> lock(*mutex_);
  const auto seen = engine_.state().applied_commands.find(command.id.value());
  if (seen != engine_.state().applied_commands.end()) {
    for (const auto& event : engine_.events()) {
      if (event.id.value() == seen->second) {
        return SubmitResult{event, true};
      }
    }
    return Error{ErrorCode::DuplicateCommand, "command was accepted but its event is missing"};
  }

  auto proposed = propose(engine_.state(), command);
  if (!proposed) {
    return proposed.error();
  }
  const bool sync = options_.durability == storage::Durability::Sync;
  if (auto blocked = fail_before_flush_unlocked(); !blocked) {
    return blocked.error();
  }
  if (auto written = wal_->append(proposed.value(), options_.durability); !written) {
    return written.error();
  }
  if (auto after = fail_after_flush_unlocked(); !after) {
    return after.error();
  }
  if (auto applied = engine_.apply_committed(proposed.value()); !applied) {
    return applied.error();
  }
  storage::NodeMetadata metadata{engine_.state().term, engine_.state().last_applied, std::nullopt};
  if (auto stored = storage::store_metadata(directory_ / "meta.bin", metadata, sync); !stored) {
    return stored.error();
  }
  if (options_.snapshot_every > 0 &&
      engine_.state().last_applied.value() % options_.snapshot_every == 0) {
    if (auto saved = storage::save_snapshot(directory_, engine_.events(), engine_.state(), sync);
        !saved) {
      return saved.error();
    }
  }
  recovery_.commit_index = engine_.state().last_applied.value();
  recovery_.state_hash = state_hash(engine_.state());
  commit_index_ = engine_.state().last_applied;
  return SubmitResult{proposed.value(), false};
}

const std::vector<Event>& FileEngine::log_events() const { return wal_->events(); }

Result<void> FileEngine::append_event(const Event& event) {
  std::lock_guard<std::mutex> lock(*mutex_);
  const std::uint64_t expected =
      wal_->events().empty() ? 1 : wal_->events().back().index.next().value();
  if (event.index.value() != expected) {
    return Error{ErrorCode::IndexGap, "appended event is not the next log index"};
  }
  if (auto blocked = fail_before_flush_unlocked(); !blocked) {
    return blocked.error();
  }
  if (auto written = wal_->append(event, options_.durability); !written) {
    return written.error();
  }
  return fail_after_flush_unlocked();
}

Result<void> FileEngine::arm_storage_fault(StorageFault fault) {
  std::lock_guard<std::mutex> lock(*mutex_);
  if (!options_.test_mode) {
    return Error{ErrorCode::StoreError, "storage faults require test mode"};
  }
  fault_ = fault;
  return {};
}

Result<void> FileEngine::fail_before_flush_unlocked() {
  if (fault_ != StorageFault::FailBeforeFlush) {
    return {};
  }
  fault_ = StorageFault::None;
  return Error{ErrorCode::StoreError, "injected failure before flush"};
}

Result<void> FileEngine::fail_after_flush_unlocked() {
  if (fault_ != StorageFault::FailAfterFlush) {
    return {};
  }
  fault_ = StorageFault::None;
  return Error{ErrorCode::StoreError, "injected failure after flush"};
}

Result<void> FileEngine::truncate_after(LogIndex index) {
  std::lock_guard<std::mutex> lock(*mutex_);
  if (index < commit_index_) {
    return Error{ErrorCode::StoreError, "committed log prefix cannot be truncated"};
  }
  return wal_->truncate_after(index);
}

Result<void> FileEngine::commit_through(LogIndex index) {
  std::lock_guard<std::mutex> lock(*mutex_);
  if (index < commit_index_) {
    return {};
  }
  for (const auto& event : wal_->events()) {
    if (event.index <= commit_index_) {
      continue;
    }
    if (index < event.index) {
      break;
    }
    if (auto applied = engine_.apply_committed(event); !applied) {
      return applied.error();
    }
  }
  if (engine_.state().last_applied != index) {
    return Error{ErrorCode::IndexGap, "commit index is not present in the log"};
  }
  auto metadata = storage::load_metadata(directory_ / "meta.bin");
  if (!metadata) {
    return metadata.error();
  }
  storage::NodeMetadata updated = metadata.value();
  updated.commit_index = index;
  // The election term can be ahead of the last applied event. Never roll it back.
  if (updated.term.value() < engine_.state().term.value()) {
    updated.term = engine_.state().term;
  }
  const bool sync = options_.durability == storage::Durability::Sync;
  if (auto stored = storage::store_metadata(directory_ / "meta.bin", updated, sync); !stored) {
    return stored.error();
  }
  commit_index_ = index;
  recovery_.commit_index = index.value();
  recovery_.state_hash = state_hash(engine_.state());
  if (options_.snapshot_every > 0 && index.value() % options_.snapshot_every == 0) {
    if (auto saved = storage::save_snapshot(directory_, engine_.events(), engine_.state(), sync);
        !saved) {
      return saved.error();
    }
    snapshot_ = storage::Snapshot{index, engine_.state().term, state_hash(engine_.state()),
                                  engine_.events()};
  }
  return {};
}

storage::NodeMetadata FileEngine::consensus_metadata() const {
  auto loaded = storage::load_metadata(directory_ / "meta.bin");
  if (!loaded) {
    return storage::NodeMetadata{};
  }
  return loaded.value();
}

Result<void> FileEngine::persist_consensus(Term term, std::optional<NodeId> voted_for) {
  std::lock_guard<std::mutex> lock(*mutex_);
  auto metadata = storage::load_metadata(directory_ / "meta.bin");
  if (!metadata) {
    return metadata.error();
  }
  if (term.value() < metadata.value().term.value()) {
    return Error{ErrorCode::StoreError, "consensus term cannot move backwards"};
  }
  storage::NodeMetadata updated = metadata.value();
  updated.term = term;
  updated.voted_for = std::move(voted_for);
  return storage::store_metadata(directory_ / "meta.bin", updated,
                                 options_.durability == storage::Durability::Sync);
}

Result<void> FileEngine::install_snapshot(const storage::Snapshot& snapshot) {
  std::lock_guard<std::mutex> lock(*mutex_);
  if (snapshot.index <= commit_index_) {
    return {};
  }
  auto replayed = replay(snapshot.events);
  if (!replayed) {
    return replayed.error();
  }
  if (state_hash(replayed.value()) != snapshot.state_hash ||
      replayed.value().last_applied != snapshot.index) {
    return Error{ErrorCode::StoreError, "snapshot checksum does not match its events"};
  }

  bool prefix_matches = false;
  for (const auto& event : wal_->events()) {
    if (event.index == snapshot.index && event.term == snapshot.term) {
      prefix_matches = true;
    }
  }
  std::vector<Event> suffix;
  if (prefix_matches) {
    for (const auto& event : wal_->events()) {
      if (snapshot.index < event.index) {
        suffix.push_back(event);
      }
    }
  }
  if (auto replaced = wal_->rewrite(suffix); !replaced) {
    return replaced.error();
  }

  Engine replacement;
  for (const auto& event : snapshot.events) {
    if (auto applied = replacement.apply_committed(event); !applied) {
      return applied.error();
    }
  }
  engine_ = std::move(replacement);
  commit_index_ = snapshot.index;
  snapshot_ = snapshot;
  recovery_.used_snapshot = true;
  recovery_.snapshot_index = snapshot.index.value();
  recovery_.commit_index = snapshot.index.value();
  recovery_.state_hash = snapshot.state_hash;

  auto metadata = storage::load_metadata(directory_ / "meta.bin");
  if (!metadata) {
    return metadata.error();
  }
  storage::NodeMetadata updated = metadata.value();
  updated.commit_index = snapshot.index;
  if (updated.term.value() < engine_.state().term.value()) {
    updated.term = engine_.state().term;
  }
  const bool sync = options_.durability == storage::Durability::Sync;
  if (auto stored = storage::store_metadata(directory_ / "meta.bin", updated, sync); !stored) {
    return stored.error();
  }
  return storage::save_snapshot(directory_, snapshot.events, engine_.state(), sync);
}

Result<void> FileEngine::discard_compacted_prefix() {
  std::lock_guard<std::mutex> lock(*mutex_);
  if (!snapshot_) {
    return Error{ErrorCode::StoreError, "no snapshot to compact against"};
  }
  std::vector<Event> suffix;
  for (const auto& event : wal_->events()) {
    if (snapshot_->index < event.index) {
      suffix.push_back(event);
    }
  }
  return wal_->rewrite(suffix);
}

Result<void> FileEngine::checkpoint() {
  std::lock_guard<std::mutex> lock(*mutex_);
  const bool sync = options_.durability == storage::Durability::Sync;
  if (auto saved = storage::save_snapshot(directory_, engine_.events(), engine_.state(), sync);
      !saved) {
    return saved.error();
  }
  if (engine_.state().last_applied.value() > 0) {
    snapshot_ = storage::Snapshot{engine_.state().last_applied, engine_.state().term,
                                  state_hash(engine_.state()), engine_.events()};
  }
  return {};
}

}  // namespace choreoos::state
