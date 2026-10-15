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
      plan.commit < old.commit || plan.suffix.size() > EntryLog::kMaximumEntries ||
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
  auto view = current_shared();
  if (!view) return view.error();
  return *view.value();
}

state::Result<std::shared_ptr<const RecoveredDomainProjection>> FileEngineAdapter::current_shared()
    const {
  if (uncertain()) return invalid("FileEngine adapter requires disk recovery");
  if (!view_) {
    auto projection = std::make_shared<RecoveredDomainProjection>(current_);
    projection->applied_state_payload = state::snapshot_payload(store_->engine().state());
    projection->applied_state_hash = store_->recovery().state_hash;
    view_ = std::move(projection);
  }
  return view_;
}

state::Result<void> FileEngineAdapter::persist_delta(const DomainWriteDelta& delta) try {
  if (uncertain()) return invalid("FileEngine adapter requires disk recovery");
  const auto base = current_.snapshot ? current_.snapshot->index.value() : 0;
  const auto base_term = current_.snapshot ? current_.snapshot->term.value() : 0;
  if (delta.base_index != base || delta.base_term != base_term || delta.term < current_.term ||
      delta.keep_index < current_.commit || delta.keep_index > store_->last_log_index() ||
      delta.append.size() > EntryLog::kMaximumEntries ||
      delta.append.size() >= std::numeric_limits<std::uint64_t>::max() - delta.keep_index ||
      delta.keep_index - base > EntryLog::kMaximumEntries - delta.append.size() ||
      delta.commit < current_.commit || delta.commit > delta.keep_index + delta.append.size() ||
      (delta.term == current_.term && current_.voted_for && delta.voted_for != current_.voted_for))
    return invalid("incremental FileEngine plan has invalid boundary, capacity, vote or indexes");
  std::optional<state::NodeId> vote;
  if (delta.voted_for) {
    auto parsed = state::NodeId::parse(*delta.voted_for);
    if (!parsed) return parsed.error();
    vote = parsed.value();
  }
  auto previous_term = base_term;
  if (delta.keep_index != base) {
    auto retained_term = store_->log_term_at(delta.keep_index);
    if (!retained_term) return invalid("incremental FileEngine prefix term is missing");
    previous_term = *retained_term;
  }
  std::vector<state::Event> appended;
  appended.reserve(delta.append.size());
  for (std::size_t offset = 0; offset < delta.append.size(); ++offset) {
    const auto& entry = delta.append[offset];
    if (entry.term < previous_term || entry.term > delta.term)
      return invalid("incremental FileEngine suffix terms decrease or exceed metadata term");
    previous_term = entry.term;
    auto event = decode_domain_entry(entry, delta.keep_index + offset + 1);
    if (!event) return event.error();
    appended.push_back(std::move(event.value()));
  }
  // Validate only the newly committed application batch against authoritative
  // state. Existing retained history was verified on recovery/previous writes.
  std::vector<state::Event> committed;
  for (auto index = current_.commit + 1; index <= delta.commit; ++index) {
    const auto* event = index <= delta.keep_index
                            ? store_->log_entry(index)
                            : &appended[static_cast<std::size_t>(index - delta.keep_index - 1)];
    if (!event) return invalid("incremental FileEngine commit has a missing entry");
    committed.push_back(*event);
  }
  if (!committed.empty()) {
    auto validated = store_->engine().prepare_committed(committed);
    if (!validated) return validated.error();
    if (validated.value().state().last_applied.value() != delta.commit)
      return invalid("incremental FileEngine commit did not advance application state");
  }
  auto fail = [this](state::Error error) -> state::Result<void> {
    uncertain_ = true;
    view_.reset();
    return error;
  };
  const auto fault = std::exchange(fault_, DomainWriteFault::None);
  auto injected = [&fail]() {
    return fail(invalid("injected incremental FileEngine write failure"));
  };
  auto metadata = store_->persist_consensus(state::Term::parse(delta.term).value(), vote);
  if (!metadata) return fail(metadata.error());
  if (fault == DomainWriteFault::AfterMetadata) return injected();
  if (delta.keep_index < store_->last_log_index()) {
    auto truncated = store_->truncate_after(state::LogIndex::parse(delta.keep_index).value());
    if (!truncated) return fail(truncated.error());
  }
  if (fault == DomainWriteFault::AfterTruncate) return injected();
  if (!appended.empty() && (fault == DomainWriteFault::AppendBeforeFlush ||
                            fault == DomainWriteFault::AppendAfterFlush)) {
    auto armed = store_->arm_storage_fault(fault == DomainWriteFault::AppendBeforeFlush
                                               ? state::StorageFault::FailBeforeFlush
                                               : state::StorageFault::FailAfterFlush);
    if (!armed) return fail(armed.error());
  }
  for (const auto& event : appended) {
    auto written = store_->append_event(event);
    if (!written) return fail(written.error());
    if (fault == DomainWriteFault::AfterAppend) return injected();
  }
  if (delta.commit > current_.commit) {
    auto applied = store_->commit_through(state::LogIndex::parse(delta.commit).value());
    if (!applied) return fail(applied.error());
  }
  if (fault == DomainWriteFault::AfterCommit) return injected();
  current_.suffix.resize(static_cast<std::size_t>(delta.keep_index - base));
  for (const auto& entry : delta.append) current_.suffix.push_back(entry);
  current_.term = delta.term;
  current_.voted_for = delta.voted_for;
  current_.commit = delta.commit;
  current_.applied = delta.commit;
  current_.applied_state_hash = store_->recovery().state_hash;
  current_.applied_state_payload.clear();  // Materialize only on explicit view requests.
  view_.reset();
  return {};
} catch (const std::exception& error) {
  uncertain_ = true;
  view_.reset();
  return invalid(std::string("incremental FileEngine write failed: ") + error.what());
}

state::Result<void> FileEngineAdapter::checkpoint() try {
  if (uncertain()) return invalid("FileEngine adapter requires disk recovery");
  if (current_.commit == 0) return {};
  uncertain_ = true;
  view_.reset();
  auto saved = store_->checkpoint();
  if (!saved) return saved.error();
  auto compacted = store_->discard_compacted_prefix();
  if (!compacted) return compacted.error();
  auto observed = audit_recovered_domain_store(*store_);
  if (!observed) return observed.error();
  current_ = std::move(observed.value());
  uncertain_ = false;
  return {};
} catch (const std::exception& error) {
  uncertain_ = true;
  return invalid(std::string("FileEngine checkpoint failed: ") + error.what());
}

state::Result<RecoveredDomainProjection> FileEngineAdapter::recover() try {
  uncertain_ = true;
  view_.reset();
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
  view_.reset();
  return {};
} catch (const std::exception& error) {
  uncertain_ = true;
  return invalid(std::string("FileEngine adapter write failed: ") + error.what());
}

}  // namespace choreoos::raft
