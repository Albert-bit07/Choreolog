#include "choreoos/raft/domain_bridge.hpp"

#include <cstddef>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace choreoos::raft {
namespace {

state::Error bridge_error(std::string message) {
  return {state::ErrorCode::StoreError, std::move(message)};
}

}  // namespace

state::Result<Entry> encode_domain_event(const state::Event& event) {
  if (event.index.value() == 0 || event.term.value() == 0 ||
      event.schema_version != state::kCurrentSchemaVersion) {
    return bridge_error("Raft event projection has unsupported envelope");
  }
  const auto encoded = state::canonical_event(event);
  auto decoded = state::parse_event(encoded);
  if (!decoded || decoded.value().index != event.index || decoded.value().term != event.term ||
      state::canonical_event(decoded.value()) != encoded) {
    return bridge_error("Raft event projection is not a canonical round trip");
  }
  return Entry{event.term.value(), encoded};
}

state::Result<state::Event> decode_domain_entry(const Entry& entry, std::uint64_t index) {
  if (index == 0 || entry.term == 0) {
    return bridge_error("Raft entry must have a positive index and term");
  }
  auto decoded = state::parse_event(entry.command);
  if (!decoded) return decoded.error();
  const auto& event = decoded.value();
  if (event.schema_version != state::kCurrentSchemaVersion || event.index.value() != index ||
      event.term.value() != entry.term || state::canonical_event(event) != entry.command) {
    return bridge_error("Raft entry has an inconsistent or noncanonical event envelope");
  }
  return decoded.value();
}

state::Result<state::ChoreographyState> replay_domain_committed(const Durable& durable) {
  if (durable.commit > durable.log.size() || durable.applied > durable.commit) {
    return bridge_error("Raft domain replay has invalid commit/applied indexes");
  }
  std::vector<state::Event> committed;
  committed.reserve(static_cast<std::size_t>(durable.commit));
  std::uint64_t previous_term = 0;
  for (std::uint64_t index = 1; index <= durable.log.size(); ++index) {
    const auto& entry = durable.log[static_cast<std::size_t>(index - 1)];
    if (entry.term < previous_term || entry.term > durable.term) {
      return bridge_error("Raft domain entry terms decrease or exceed the current term");
    }
    previous_term = entry.term;
    auto decoded = decode_domain_entry(entry, index);
    if (!decoded) return decoded.error();
    if (index <= durable.commit) committed.push_back(std::move(decoded.value()));
  }
  return state::replay(committed);
}

state::Result<DomainProjection> audit_domain_store(const state::FileEngine& store) {
  // Propagate read failures; never substitute an initial term or empty vote.
  auto metadata = store.consensus_metadata();
  if (!metadata) return metadata.error();

  const auto& events = store.log_events();
  const auto commit = store.commit_index().value();
  const auto applied = store.engine().state().last_applied.value();
  if (commit != metadata.value().commit_index.value() || applied != commit) {
    return bridge_error("Recovered WAL, metadata commit and applied index disagree");
  }
  if ((!events.empty() && events.front().index.value() != 1) || commit > events.size()) {
    return bridge_error("Raft event bridge requires an uncompacted, complete WAL prefix");
  }

  DomainProjection projection;
  projection.raft.term = metadata.value().term.value();
  const auto& vote = metadata.value().voted_for;
  if (vote) {
    projection.raft.voted_for = vote->value();
  }
  projection.raft.commit = commit;
  projection.raft.applied = applied;
  projection.raft.log.reserve(events.size());

  for (std::size_t offset = 0; offset < events.size(); ++offset) {
    const auto& event = events[offset];
    if (event.index.value() != offset + 1 || event.term.value() > projection.raft.term) {
      return bridge_error("Raft event bridge found a WAL index gap or future-term entry");
    }
    auto encoded = encode_domain_event(event);
    if (!encoded) return encoded.error();
    projection.raft.log.push_back(std::move(encoded.value()));
  }

  auto replayed = replay_domain_committed(projection.raft);
  if (!replayed) return replayed.error();
  projection.applied_state_hash = state::state_hash(replayed.value());
  if (projection.applied_state_hash != state::state_hash(store.engine().state()) ||
      state::snapshot_payload(replayed.value()) !=
          state::snapshot_payload(store.engine().state())) {
    return bridge_error(
        "Recovered choreography state or retry receipts differ from committed replay");
  }
  return projection;
}

state::Result<state::ChoreographyState> replay_recovered_domain(
    const RecoveredDomainProjection& projection) {
  state::ChoreographyState replayed;
  std::uint64_t base = 0;
  std::uint64_t previous_term = 0;
  if (projection.snapshot) {
    const auto& snapshot = *projection.snapshot;
    auto verified = storage::verify_snapshot(snapshot);
    if (!verified) return verified.error();
    base = snapshot.index.value();
    previous_term = snapshot.term.value();
    if (base == 0 || previous_term > projection.term)
      return bridge_error("Recovered projection has an invalid snapshot boundary");
    replayed = std::move(verified.value());
  }
  if (projection.term == 0 || projection.applied != projection.commit || projection.commit < base ||
      projection.suffix.size() > std::numeric_limits<std::uint64_t>::max() - base ||
      projection.commit > base + projection.suffix.size()) {
    return bridge_error("Recovered projection has invalid absolute commit/applied indexes");
  }
  for (std::size_t offset = 0; offset < projection.suffix.size(); ++offset) {
    const auto index = base + static_cast<std::uint64_t>(offset) + 1;
    const auto& entry = projection.suffix[offset];
    if (entry.term < previous_term || entry.term > projection.term)
      return bridge_error("Recovered projection suffix terms decrease or exceed metadata term");
    previous_term = entry.term;
    auto decoded = decode_domain_entry(entry, index);
    if (!decoded) return decoded.error();
    if (index <= projection.commit) {
      if (auto applied = state::apply(replayed, decoded.value()); !applied) return applied.error();
    }
  }
  if (replayed.last_applied.value() != projection.applied ||
      state::state_hash(replayed) != projection.applied_state_hash ||
      state::snapshot_payload(replayed) != projection.applied_state_payload)
    return bridge_error("Recovered projection differs from applied state or retry receipts");
  return replayed;
}

state::Result<RecoveredDomainProjection> audit_recovered_domain_store(
    const state::FileEngine& store) {
  std::error_code error;
  const bool pending_install =
      std::filesystem::exists(store.directory() / "snapshot-install.txn", error);
  if (error || pending_install)
    return bridge_error("Snapshot installation requires disk recovery before projection");
  auto metadata = store.consensus_metadata();
  if (!metadata) return metadata.error();
  RecoveredDomainProjection projection;
  projection.term = metadata.value().term.value();
  const auto& vote = metadata.value().voted_for;
  if (vote) projection.voted_for = vote->value();
  projection.snapshot = store.latest_snapshot();
  projection.commit = metadata.value().commit_index.value();
  projection.applied = store.engine().state().last_applied.value();
  projection.applied_state_hash = state::state_hash(store.engine().state());
  projection.applied_state_payload = state::snapshot_payload(store.engine().state());
  if (projection.commit != store.commit_index().value())
    return bridge_error("Recovered metadata and FileEngine commit indexes disagree");
  const auto base = projection.snapshot ? projection.snapshot->index.value() : 0;
  const auto& events = store.log_events();
  std::uint64_t previous_index = 0;
  std::uint64_t previous_term = 0;
  for (const auto& event : events) {
    const auto index = event.index.value();
    if (index == 0 ||
        (previous_index != 0 && (previous_index == std::numeric_limits<std::uint64_t>::max() ||
                                 index != previous_index + 1)) ||
        event.term.value() < previous_term || event.term.value() > projection.term)
      return bridge_error("Retained WAL has a gap, decreasing terms or future-term entry");
    previous_index = index;
    previous_term = event.term.value();
    auto encoded = encode_domain_event(event);
    if (!encoded) return encoded.error();
    if (index <= base && projection.snapshot &&
        event.term.value() > projection.snapshot->term.value())
      return bridge_error("Retained WAL prefix term exceeds snapshot boundary term");
    if (index == base && projection.snapshot && event.term != projection.snapshot->term)
      return bridge_error("Retained WAL anchor disagrees with snapshot term");
    if (index > base) projection.suffix.push_back(std::move(encoded.value()));
  }
  auto replayed = replay_recovered_domain(projection);
  if (!replayed) return replayed.error();
  return projection;
}

}  // namespace choreoos::raft
