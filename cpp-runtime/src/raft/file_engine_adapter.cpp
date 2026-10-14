#include "choreoos/raft/file_engine_adapter.hpp"

#include <algorithm>
#include <cstddef>
#include <exception>
#include <limits>
#include <utility>

namespace choreoos::raft {
namespace {

state::Error invalid(std::string why) { return {state::ErrorCode::StoreError, std::move(why)}; }

struct Prepared {
  RecoveredDomainProjection expected;
  std::vector<state::Event> events;
};

state::Result<Prepared> prepare(const RecoveredDomainProjection& old, const DomainWritePlan& plan) {
  const auto base = old.snapshot ? old.snapshot->index.value() : 0;
  const auto base_term = old.snapshot ? old.snapshot->term.value() : 0;
  if (plan.base_index != base || plan.base_term != base_term || plan.term < old.term ||
      plan.commit < old.commit || plan.suffix.size() > 100000 ||
      plan.suffix.size() > std::numeric_limits<std::uint64_t>::max() - base ||
      plan.commit > base + plan.suffix.size())
    return invalid("FileEngine write plan has a stale boundary or regressing/invalid indexes");
  if (plan.term == old.term && old.voted_for && plan.voted_for != old.voted_for)
    return invalid("FileEngine write plan cannot change a vote in the same term");
  if (plan.voted_for && !state::NodeId::parse(*plan.voted_for))
    return invalid("FileEngine write plan vote is not a valid node ID");
  const auto committed = old.commit - base;
  if (plan.suffix.size() < committed ||
      !std::equal(old.suffix.begin(), old.suffix.begin() + static_cast<std::ptrdiff_t>(committed),
                  plan.suffix.begin()))
    return invalid("FileEngine write plan cannot replace the committed prefix");

  state::ChoreographyState applied;
  if (old.snapshot) {
    auto verified = storage::verify_snapshot(*old.snapshot);
    if (!verified) return verified.error();
    applied = std::move(verified.value());
  }
  Prepared prepared;
  prepared.expected.snapshot = old.snapshot;
  prepared.expected.term = plan.term;
  prepared.expected.voted_for = plan.voted_for;
  prepared.expected.suffix = plan.suffix;
  prepared.expected.commit = plan.commit;
  prepared.expected.applied = plan.commit;  // Recovered application is replay-derived.
  auto previous_term = base_term;
  prepared.events.reserve(plan.suffix.size());
  for (std::size_t offset = 0; offset < plan.suffix.size(); ++offset) {
    const auto index = base + static_cast<std::uint64_t>(offset) + 1;
    const auto& entry = plan.suffix[offset];
    if (entry.term < previous_term || entry.term > plan.term)
      return invalid("FileEngine write plan terms decrease or exceed metadata term");
    previous_term = entry.term;
    auto event = decode_domain_entry(entry, index);
    if (!event) return event.error();
    if (index <= plan.commit) {
      if (auto result = state::apply(applied, event.value()); !result) return result.error();
    }
    prepared.events.push_back(std::move(event.value()));
  }
  prepared.expected.applied_state_hash = state::state_hash(applied);
  prepared.expected.applied_state_payload = state::snapshot_payload(applied);
  return prepared;
}

}  // namespace

state::Result<FileEngineAdapter> FileEngineAdapter::open(std::filesystem::path directory,
                                                         bool test_mode) {
  FileEngineAdapter adapter(std::move(directory), test_mode);
  auto recovered = adapter.recover();
  if (!recovered) return recovered.error();
  return adapter;
}

state::Result<RecoveredDomainProjection> FileEngineAdapter::current() const {
  if (uncertain()) return invalid("FileEngine adapter requires disk recovery");
  return current_;
}

state::Result<RecoveredDomainProjection> FileEngineAdapter::recover() try {
  uncertain_ = true;
  fault_ = DomainWriteFault::None;
  store_.reset();
  state::StoreOptions options;
  options.commit_on_append = false;
  options.snapshot_every = 0;
  options.durability = storage::Durability::Sync;
  options.test_mode = test_mode_;
  auto opened = state::FileEngine::open(directory_, options);
  if (!opened) return opened.error();
  auto projected = audit_recovered_domain_store(opened.value());
  if (!projected) return projected.error();
  store_ = std::make_unique<state::FileEngine>(std::move(opened.value()));
  current_ = std::move(projected.value());
  uncertain_ = false;
  return current_;
} catch (const std::exception& error) {
  uncertain_ = true;
  return invalid(std::string("FileEngine adapter recovery failed: ") + error.what());
}

state::Result<void> FileEngineAdapter::inject_next_fault_for_test(DomainWriteFault fault) {
  if (!test_mode_ || uncertain()) return invalid("FileEngine adapter fault injection unavailable");
  fault_ = fault;
  return {};
}

state::Result<void> FileEngineAdapter::persist(const DomainWritePlan& plan) try {
  if (uncertain()) return invalid("FileEngine adapter requires disk recovery");
  auto prepared = prepare(current_, plan);
  if (!prepared) return prepared.error();  // Validation has not modified disk.
  const auto fault = std::exchange(fault_, DomainWriteFault::None);
  auto fail = [this](state::Error error) -> state::Result<void> {
    uncertain_ = true;
    return error;
  };
  auto injected = [&fail]() { return fail(invalid("injected FileEngine adapter write failure")); };
  std::optional<state::NodeId> vote;
  if (plan.voted_for) vote = state::NodeId::parse(*plan.voted_for).value();
  auto metadata = store_->persist_consensus(state::Term::parse(plan.term).value(), vote);
  if (!metadata) return fail(metadata.error());
  if (fault == DomainWriteFault::AfterMetadata) return injected();

  std::size_t common = 0;
  while (common < current_.suffix.size() && common < plan.suffix.size() &&
         current_.suffix[common] == plan.suffix[common])
    ++common;
  if (common < current_.suffix.size()) {
    const auto keep = plan.base_index + static_cast<std::uint64_t>(common);
    auto truncated = store_->truncate_after(state::LogIndex::parse(keep).value());
    if (!truncated) return fail(truncated.error());
  }
  if (fault == DomainWriteFault::AfterTruncate) return injected();
  if (fault == DomainWriteFault::AppendBeforeFlush || fault == DomainWriteFault::AppendAfterFlush) {
    if (common < prepared.value().events.size()) {
      auto armed = store_->arm_storage_fault(fault == DomainWriteFault::AppendBeforeFlush
                                                 ? state::StorageFault::FailBeforeFlush
                                                 : state::StorageFault::FailAfterFlush);
      if (!armed) return fail(armed.error());
    }
  }
  for (std::size_t offset = common; offset < prepared.value().events.size(); ++offset) {
    auto appended = store_->append_event(prepared.value().events[offset]);
    if (!appended) return fail(appended.error());
    if (fault == DomainWriteFault::AfterAppend) return injected();
  }
  if (plan.commit > current_.commit) {
    auto committed = store_->commit_through(state::LogIndex::parse(plan.commit).value());
    if (!committed) return fail(committed.error());
  }
  if (fault == DomainWriteFault::AfterCommit) return injected();
  auto observed = audit_recovered_domain_store(*store_);
  if (!observed) return fail(observed.error());
  const auto& expected = prepared.value().expected;
  const auto& actual = observed.value();
  if (actual.term != expected.term || actual.voted_for != expected.voted_for ||
      actual.commit != expected.commit || actual.suffix != expected.suffix ||
      actual.applied_state_payload != expected.applied_state_payload)
    return fail(invalid("FileEngine adapter result differs from validated write plan"));
  current_ = std::move(observed.value());
  return {};
} catch (const std::exception& error) {
  uncertain_ = true;
  return invalid(std::string("FileEngine adapter write failed: ") + error.what());
}

}  // namespace choreoos::raft
