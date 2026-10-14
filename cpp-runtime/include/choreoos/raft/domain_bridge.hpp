#pragma once

// Strict, read-only correspondence between the experimental Raft log entries
// and the existing typed choreography WAL. No production writes or migration.
#include <cstdint>
#include <string>

#include "choreoos/raft/transition.hpp"
#include "choreoos/state/store.hpp"

namespace choreoos::raft {

struct DomainProjection {
  Durable raft;
  std::string applied_state_hash;
};

// Canonical typed event -> one opaque Raft entry. Never fabricates term/index.
[[nodiscard]] state::Result<Entry> encode_domain_event(const state::Event& event);

// Raft entry -> typed event, validating canonical payload and exact index/term.
[[nodiscard]] state::Result<state::Event> decode_domain_entry(const Entry& entry,
                                                               std::uint64_t index);

// Derive authoritative choreography state by replaying exactly the committed
// prefix. Uncommitted suffixes never affect the returned state.
[[nodiscard]] state::Result<state::ChoreographyState> replay_domain_committed(
    const Durable& durable);

// Compare real FileEngine WAL, durable metadata, commit index and recovered
// applied state to the strict Raft projection. Compacted/missing prefixes
// intentionally fail closed until snapshot-offset mapping is implemented.
[[nodiscard]] state::Result<DomainProjection> audit_domain_store(
    const state::FileEngine& store);

}  // namespace choreoos::raft
