// Durable directory store: canonical event lines, WAL, snapshots, metadata.

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

// An invalid member is an error, not something to skip: dropping it would make
// replay produce a different formation than the one that was committed.
Result<std::vector<DancerId>> parse_members(std::string_view raw) {
  std::vector<DancerId> members;
  if (raw.empty()) {
    return members;
  }
  std::string text{raw};
  std::istringstream stream{text};
  std::string item;
  while (std::getline(stream, item, ',')) {
    auto id = DancerId::parse(item);
    if (!id) {
      return id.error();
    }
    members.push_back(id.value());
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
      auto members = parse_members(members_raw.value());
      if (!members) {
        return members.error();
      }
      payload = FormationPayload{formation.value(), std::move(members.value())};
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
  // Resolve a committed install before scanning/truncating the old WAL or
  // choosing snapshots using potentially intermediate metadata.
  if (auto recovered = storage::recover_snapshot_transaction(store.directory_); !recovered) {
    return recovered.error();
  }
  auto wal = storage::WriteAheadLog::open(event_log_path(store.directory_));
  if (!wal) {
    return wal.error();
  }
  store.wal_ = std::make_unique<storage::WriteAheadLog>(std::move(wal.value()));
  store.recovery_.truncated_tail_bytes = store.wal_->stats().truncated_tail_bytes;

  auto loaded_snapshot = storage::load_latest_snapshot(store.directory_);
  if (!loaded_snapshot) return loaded_snapshot.error();
  auto snapshot = std::move(loaded_snapshot.value());
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
  if (snapshot && snapshot->index.value() <= apply_limit) {
    auto restored = storage::verify_snapshot(*snapshot);
    if (!restored) {
      return restored.error();
    }
    // Always hold the snapshot in state form so a leader can serve it to a
    // lagging peer, whichever format was on disk.
    storage::Snapshot in_memory = *snapshot;
    if (in_memory.legacy()) {
      in_memory.state_payload = snapshot_payload(restored.value());
      in_memory.events.clear();
    }
    if (snapshot->legacy()) {
      for (const auto& event : snapshot->events) {
        if (auto applied = store.engine_.apply_committed(event); !applied) {
          return applied.error();
        }
      }
    } else {
      // Keep committed history in memory only when the log still holds the
      // whole prefix, so tools that replay to an index keep working.
      std::vector<Event> history;
      const auto& records = store.wal_->events();
      if (!records.empty() && records.front().index.value() == 1) {
        for (const auto& event : records) {
          if (event.index.value() > snapshot->index.value()) {
            break;
          }
          history.push_back(event);
        }
      }
      store.engine_.restore(std::move(restored.value()), std::move(history));
    }
    store.recovery_.used_snapshot = true;
    store.recovery_.snapshot_index = snapshot->index.value();
    store.snapshot_ = std::move(in_memory);
    start_after = snapshot->index.value();
  }
  if (start_after == 0 && !store.wal_->events().empty() &&
      store.wal_->events().front().index.value() > 1 &&
      store.wal_->events().front().index.value() <= apply_limit) {
    return Error{ErrorCode::StoreError,
                 "the log was compacted but no valid snapshot covers its missing prefix"};
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

  // A torn uncommitted tail is recoverable. Missing committed state is not:
  // never advertise a commit boundary that replay could not reconstruct.
  if (store.engine_.state().last_applied < metadata.value().commit_index) {
    return Error{ErrorCode::StoreError, "committed history is missing from snapshot and WAL"};
  }

  const auto commit =
      options.commit_on_append ? store.engine_.state().last_applied : metadata.value().commit_index;
  store.commit_index_ = commit;
  if (options.commit_on_append &&
      (metadata.value().commit_index < commit ||
       metadata.value().term.value() < store.engine_.state().term.value())) {
    storage::NodeMetadata updated = metadata.value();
    updated.commit_index = commit;
    if (updated.term.value() < store.engine_.state().term.value())
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

Result<SubmitResult> FileEngine::submit(const Command& command) {
  std::lock_guard<std::mutex> lock(*mutex_);
  if (auto writable = require_writable_unlocked(); !writable) return writable.error();
  const auto seen = engine_.state().applied_commands.find(command.id.value());
  if (seen != engine_.state().applied_commands.end() ||
      engine_.state().receipts.count(command.id.value()) != 0) {
    auto receipt = resolve_receipt(engine_.state(), command);
    if (!receipt) {
      return receipt.error();
    }
    return SubmitResult{receipt.value(), true};
  }

  auto proposed = propose(engine_.state(), command);
  if (!proposed) {
    return proposed.error();
  }
  if (canonical_event(proposed.value()).size() > storage::kMaxPayloadBytes)
    return Error{ErrorCode::StoreError, "event payload exceeds 1 MiB"};
  const bool sync = options_.durability == storage::Durability::Sync;
  if (auto blocked = fail_before_flush_unlocked(); !blocked) {
    return blocked.error();
  }
  if (auto written = wal_->append(proposed.value(), options_.durability); !written) {
    return storage_failure_unlocked(written.error());
  }
  if (auto after = fail_after_flush_unlocked(); !after) {
    return storage_failure_unlocked(after.error());
  }
  // propose() already validated this event against the unchanged engine.
  // Publish it only after metadata is durable, without copying full history.
  auto metadata = storage::load_metadata(directory_ / "meta.bin");
  if (!metadata) return storage_failure_unlocked(metadata.error());
  auto updated = metadata.value();
  updated.commit_index = proposed.value().index;
  if (updated.term.value() < proposed.value().term.value()) updated.term = proposed.value().term;
  if (auto stored = storage::store_metadata(directory_ / "meta.bin", updated, sync); !stored) {
    return storage_failure_unlocked(stored.error());
  }
  if (auto applied = engine_.apply_committed(proposed.value()); !applied) {
    return storage_failure_unlocked(applied.error());
  }
  recovery_.commit_index = engine_.state().last_applied.value();
  recovery_.state_hash = state_hash(engine_.state());
  commit_index_ = engine_.state().last_applied;
  if (auto snapshotted = maybe_snapshot_unlocked(); !snapshotted) {
    return storage_failure_unlocked(snapshotted.error());
  }
  return SubmitResult{proposed.value(), false};
}

const std::vector<Event>& FileEngine::log_events() const { return wal_->events(); }

const Event* FileEngine::log_entry(std::uint64_t index) const {
  const auto& events = wal_->events();
  if (events.empty() || index < events.front().index.value() ||
      index > events.back().index.value()) {
    return nullptr;
  }
  return &events[static_cast<std::size_t>(index - events.front().index.value())];
}

std::uint64_t FileEngine::last_log_index() const {
  const auto& events = wal_->events();
  return events.empty() ? engine_.state().last_applied.value() : events.back().index.value();
}

std::uint64_t FileEngine::log_first_index() const {
  const auto& events = wal_->events();
  return events.empty() ? engine_.state().last_applied.value() + 1 : events.front().index.value();
}

std::optional<std::uint64_t> FileEngine::log_term_at(std::uint64_t index) const {
  if (index == 0) {
    return 0;
  }
  if (const Event* entry = log_entry(index)) {
    return entry->term.value();
  }
  if (wal_->events().empty() && snapshot_ && snapshot_->index.value() == index) {
    return snapshot_->term.value();
  }
  return std::nullopt;
}

Result<void> FileEngine::append_event(const Event& event) {
  std::lock_guard<std::mutex> lock(*mutex_);
  if (auto writable = require_writable_unlocked(); !writable) return writable.error();
  const std::uint64_t expected = wal_->events().empty()
                                     ? engine_.state().last_applied.value() + 1
                                     : wal_->events().back().index.next().value();
  if (event.index.value() != expected) {
    return Error{ErrorCode::IndexGap, "appended event is not the next log index"};
  }
  if (canonical_event(event).size() > storage::kMaxPayloadBytes)
    return Error{ErrorCode::StoreError, "event payload exceeds 1 MiB"};
  if (auto blocked = fail_before_flush_unlocked(); !blocked) {
    return blocked.error();
  }
  if (auto written = wal_->append(event, options_.durability); !written) {
    return storage_failure_unlocked(written.error());
  }
  return checked_mutation_unlocked(fail_after_flush_unlocked());
}

Result<void> FileEngine::arm_storage_fault(StorageFault fault) {
  std::lock_guard<std::mutex> lock(*mutex_);
  if (!options_.test_mode) {
    return Error{ErrorCode::StoreError, "storage faults require test mode"};
  }
  fault_ = fault;
  return {};
}

Result<void> FileEngine::require_writable_unlocked() const {
  if (recovery_required_) {
    return Error{ErrorCode::StoreError, "storage failure requires reopen before further writes"};
  }
  return {};
}

Error FileEngine::storage_failure_unlocked(Error error) {
  recovery_required_ = true;
  return error;
}

Result<void> FileEngine::checked_mutation_unlocked(const Result<void>& result) {
  if (!result) return storage_failure_unlocked(result.error());
  return {};
}

Result<void> FileEngine::arm_snapshot_install_fault(storage::InstallFault fault) {
  std::lock_guard<std::mutex> lock(*mutex_);
  if (!options_.test_mode) {
    return Error{ErrorCode::StoreError, "snapshot install faults require test mode"};
  }
  install_fault_ = fault;
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
  if (auto writable = require_writable_unlocked(); !writable) return writable.error();
  if (index < commit_index_) {
    return Error{ErrorCode::StoreError, "committed log prefix cannot be truncated"};
  }
  return checked_mutation_unlocked(wal_->truncate_after(index));
}

Result<void> FileEngine::commit_through(LogIndex index) {
  std::lock_guard<std::mutex> lock(*mutex_);
  if (auto writable = require_writable_unlocked(); !writable) return writable.error();
  if (index < commit_index_) {
    return {};
  }
  // Validate the complete batch without exposing state or retry receipts.
  // A failed metadata write may be ambiguous; only recovery decides what won.
  std::vector<Event> batch;
  for (std::uint64_t next = commit_index_.value() + 1; next <= index.value(); ++next) {
    const Event* event = log_entry(next);
    if (event == nullptr) {
      break;
    }
    batch.push_back(*event);
  }
  auto prepared = engine_.prepare_committed(batch);
  if (!prepared) return prepared.error();
  auto& replacement = prepared.value();
  if (replacement.state().last_applied != index) {
    return Error{ErrorCode::IndexGap, "commit index is not present in the log"};
  }
  engine_.reserve_commit(batch.size());
  auto metadata = storage::load_metadata(directory_ / "meta.bin");
  if (!metadata) {
    return storage_failure_unlocked(metadata.error());
  }
  storage::NodeMetadata updated = metadata.value();
  updated.commit_index = index;
  // The election term can be ahead of the last applied event. Never roll it back.
  if (updated.term.value() < replacement.state().term.value()) {
    updated.term = replacement.state().term;
  }
  const bool sync = options_.durability == storage::Durability::Sync;
  if (auto stored = storage::store_metadata(directory_ / "meta.bin", updated, sync); !stored) {
    return storage_failure_unlocked(stored.error());
  }
  engine_.publish_committed(std::move(replacement));
  commit_index_ = index;
  recovery_.commit_index = index.value();
  recovery_.state_hash = state_hash(engine_.state());
  return checked_mutation_unlocked(maybe_snapshot_unlocked());
}

Result<void> FileEngine::maybe_snapshot_unlocked() {
  const std::uint64_t every = options_.snapshot_every;
  if (every == 0) {
    return {};
  }
  // Compare snapshot "periods" rather than testing index % every, so a commit
  // that jumps over a multiple (batched catch-up) still takes the snapshot.
  const std::uint64_t applied = engine_.state().last_applied.value();
  const std::uint64_t last = snapshot_ ? snapshot_->index.value() : 0;
  if (applied / every <= last / every) {
    return {};
  }
  return checked_mutation_unlocked(take_snapshot_unlocked());
}

Result<void> FileEngine::take_snapshot_unlocked() {
  const bool sync = options_.durability == storage::Durability::Sync;
  if (auto saved = storage::save_snapshot(directory_, engine_.state(), sync); !saved) {
    return saved.error();
  }
  snapshot_ = storage::Snapshot{engine_.state().last_applied,
                                engine_.state().term,
                                state_hash(engine_.state()),
                                {},
                                snapshot_payload(engine_.state())};
  // Two snapshots are enough: the newest, and one fallback if it is damaged.
  auto pruned = storage::prune_snapshots(directory_, 2);
  if (!pruned) return pruned.error();
  if (options_.compact_log && pruned.value().remaining >= 2) {
    // Only compact once a fallback exists, and only back to the fallback.
    return compact_unlocked(pruned.value().oldest_index);
  }
  return {};
}

Result<void> FileEngine::compact_unlocked(std::uint64_t anchor) {
  const auto& events = wal_->events();
  if (events.empty() || events.front().index.value() >= anchor) {
    return {};
  }
  if (anchor > events.back().index.value()) {
    return {};
  }
  // Keep the snapshot's own record: it supplies the term for the prev-entry
  // check of the first AppendEntries after the boundary.
  const auto offset = static_cast<std::ptrdiff_t>(anchor - events.front().index.value());
  std::vector<Event> kept(events.begin() + offset, events.end());
  auto rewritten = checked_mutation_unlocked(wal_->rewrite(kept));
  if (rewritten && options_.compact_log)
    return checked_mutation_unlocked(engine_.retain_history_from(anchor));
  return rewritten;
}

Result<storage::NodeMetadata> FileEngine::consensus_metadata() const {
  return storage::load_metadata(directory_ / "meta.bin");
}

Result<void> FileEngine::persist_consensus(Term term, std::optional<NodeId> voted_for) {
  std::lock_guard<std::mutex> lock(*mutex_);
  if (auto writable = require_writable_unlocked(); !writable) return writable.error();
  auto metadata = storage::load_metadata(directory_ / "meta.bin");
  if (!metadata) {
    return storage_failure_unlocked(metadata.error());
  }
  if (term.value() < metadata.value().term.value()) {
    return Error{ErrorCode::StoreError, "consensus term cannot move backwards"};
  }
  storage::NodeMetadata updated = metadata.value();
  updated.term = term;
  updated.voted_for = std::move(voted_for);
  return checked_mutation_unlocked(storage::store_metadata(
      directory_ / "meta.bin", updated, options_.durability == storage::Durability::Sync));
}

Result<void> FileEngine::install_snapshot(const storage::Snapshot& snapshot) {
  std::lock_guard<std::mutex> lock(*mutex_);
  if (auto writable = require_writable_unlocked(); !writable) return writable.error();
  if (snapshot.index <= commit_index_) {
    return {};
  }
  // Validate and build the replacement entirely in memory first. Nothing on
  // disk or in this object changes until the snapshot is known to be good.
  auto restored = storage::verify_snapshot(snapshot);
  if (!restored) {
    return restored.error();
  }
  Engine replacement;
  if (snapshot.legacy()) {
    for (const auto& event : snapshot.events) {
      if (auto applied = replacement.apply_committed(event); !applied) {
        return applied.error();
      }
    }
  } else {
    replacement.restore(std::move(restored.value()));
  }

  bool prefix_matches = false;
  if (const Event* anchor = log_entry(snapshot.index.value())) {
    prefix_matches = anchor->term == snapshot.term;
  }
  // Keep the snapshot's own record (the anchor) and anything after it when the
  // local log agrees with the snapshot; otherwise the whole log is stale.
  std::vector<Event> suffix;
  if (prefix_matches) {
    for (const auto& event : wal_->events()) {
      if (snapshot.index <= event.index) {
        suffix.push_back(event);
      }
    }
  }

  // Encode every replacement before committing the redo decision. Recovery
  // owns completion once that journal is durable, regardless of which files
  // were replaced before the process stopped.
  auto metadata = storage::load_metadata(directory_ / "meta.bin");
  if (!metadata) {
    return storage_failure_unlocked(metadata.error());
  }
  storage::NodeMetadata updated = metadata.value();
  updated.commit_index = snapshot.index;
  if (updated.term.value() < replacement.state().term.value()) {
    updated.term = replacement.state().term;
  }
  auto snapshot_bytes = storage::encode_snapshot(replacement.state());
  if (!snapshot_bytes) return snapshot_bytes.error();
  auto metadata_bytes = storage::encode_metadata(updated);
  if (!metadata_bytes) return metadata_bytes.error();
  auto wal_bytes = storage::encode_wal(suffix);
  if (!wal_bytes) return wal_bytes.error();
  storage::SnapshotTransaction transaction{
      snapshot.index.value(), std::move(snapshot_bytes.value()), std::move(metadata_bytes.value()),
      std::move(wal_bytes.value())};
  const auto fault = install_fault_;
  install_fault_ = storage::InstallFault::None;
  auto committed = storage::commit_snapshot_transaction(directory_, transaction, fault);
  if (!committed) {
    recovery_required_ = true;
    return committed.error();
  }
  auto refreshed = storage::WriteAheadLog::open(event_log_path(directory_));
  if (!refreshed) {
    recovery_required_ = true;
    return refreshed.error();
  }
  *wal_ = std::move(refreshed.value());

  engine_ = std::move(replacement);
  commit_index_ = snapshot.index;
  snapshot_ = storage::Snapshot{
      snapshot.index, snapshot.term, snapshot.state_hash, {}, snapshot_payload(engine_.state())};
  // An older snapshot cannot bridge to a log that now starts at this one, so
  // keep only the installed snapshot.
  (void)storage::prune_snapshots(directory_, 1);
  recovery_.used_snapshot = true;
  recovery_.snapshot_index = snapshot.index.value();
  recovery_.commit_index = snapshot.index.value();
  recovery_.state_hash = snapshot.state_hash;
  return {};
}

Result<void> FileEngine::discard_compacted_prefix() {
  std::lock_guard<std::mutex> lock(*mutex_);
  if (auto writable = require_writable_unlocked(); !writable) return writable.error();
  if (!snapshot_) {
    return Error{ErrorCode::StoreError, "no snapshot to compact against"};
  }
  return compact_unlocked(snapshot_->index.value());
}

Result<void> FileEngine::checkpoint() {
  std::lock_guard<std::mutex> lock(*mutex_);
  if (auto writable = require_writable_unlocked(); !writable) return writable.error();
  if (engine_.state().last_applied.value() == 0) {
    return {};
  }
  return checked_mutation_unlocked(take_snapshot_unlocked());
}

}  // namespace choreoos::state
