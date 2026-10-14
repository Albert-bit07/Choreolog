#include "choreoos/raft/domain_bridge.hpp"

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "choreoos/storage/metadata.hpp"

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
  for (std::uint64_t index = 1; index <= durable.log.size(); ++index) {
    const auto& entry = durable.log[static_cast<std::size_t>(index - 1)];
    if (entry.term > durable.term) {
      return bridge_error("Raft domain entry term exceeds the current term");
    }
    auto decoded = decode_domain_entry(entry, index);
    if (!decoded) return decoded.error();
    if (index <= durable.commit) committed.push_back(std::move(decoded.value()));
  }
  return state::replay(committed);
}

state::Result<DomainProjection> audit_domain_store(const state::FileEngine& store) {
  // Do not use FileEngine::consensus_metadata() here: its legacy API returns
  // default metadata on a read error, which would mask a corrupted vote/term.
  auto metadata = storage::load_metadata(store.directory() / "meta.bin");
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
  if (metadata.value().voted_for) {
    projection.raft.voted_for = metadata.value().voted_for->value();
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
  if (projection.applied_state_hash != state::state_hash(store.engine().state())) {
    return bridge_error("Recovered choreography state hash differs from committed replay");
  }
  return projection;
}

}  // namespace choreoos::raft
