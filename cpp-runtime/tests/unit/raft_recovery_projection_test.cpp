#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "choreoos/raft/domain_bridge.hpp"

namespace choreoos::raft {
namespace {

class RaftRecoveryProjectionTest : public testing::Test {
 protected:
  std::filesystem::path path;
  std::vector<state::Event> events;
  storage::Snapshot snapshot;
  state::Command create() const {
    using namespace state;
    const auto show = ChoreographyId::parse("opening").value();
    return {CommandId::parse("create").value(),
            show,
            CommandType::CreateChoreography,
            kCurrentSchemaVersion,
            MusicalTick::from_count(0).value(),
            CreateChoreographyPayload{show, StageBounds::from_mm(20000, 12000).value(),
                                      OverlapPolicy::Forbidden, 5000}};
  }
  state::StoreOptions options() const {
    state::StoreOptions value;
    value.commit_on_append = false;
    value.snapshot_every = 0;
    value.test_mode = true;
    return value;
  }
  void SetUp() override {
    path = std::filesystem::temp_directory_path() /
           (std::string("choreoos-recovery-projection-") +
            testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::remove_all(path);
    state::Engine source;
    ASSERT_TRUE(source.submit(create()));
    for (int i = 1; i <= 2; ++i) {
      using namespace state;
      const auto id = "d" + std::to_string(i);
      ASSERT_TRUE(source.submit(
          {CommandId::parse(id).value(), ChoreographyId::parse("opening").value(),
           CommandType::AddDancer, kCurrentSchemaVersion, MusicalTick::from_count(i).value(),
           DancerPayload{DancerId::parse(id).value(), Position::from_mm(i * 2000, 1000).value()}}));
      if (i == 1)
        snapshot = {source.state().last_applied,
                    source.state().term,
                    state_hash(source.state()),
                    {},
                    snapshot_payload(source.state())};
    }
    events = source.events();
  }
  void TearDown() override { std::filesystem::remove_all(path); }
};

TEST_F(RaftRecoveryProjectionTest, CompactedWalUsesAbsoluteIndexesAndIgnoresUncommittedSuffix) {
  events[2].term = state::Term::parse(3).value();
  {
    auto store = state::FileEngine::open(path, options());
    ASSERT_TRUE(store);
    for (const auto& event : events) ASSERT_TRUE(store.value().append_event(event));
    ASSERT_TRUE(store.value().commit_through(state::LogIndex::parse(2).value()));
    ASSERT_TRUE(store.value().checkpoint());
    ASSERT_TRUE(store.value().discard_compacted_prefix());
    ASSERT_TRUE(store.value().persist_consensus(state::Term::parse(7).value(),
                                                state::NodeId::parse("node-b").value()));
    EXPECT_EQ(store.value().log_first_index(), 2u);
    EXPECT_FALSE(audit_domain_store(store.value()));  // Core still has no offset semantics.
  }
  auto store = state::FileEngine::open(path, options());
  ASSERT_TRUE(store);
  auto projection = audit_recovered_domain_store(store.value());
  ASSERT_TRUE(projection) << projection.error().to_string();
  const auto& p = projection.value();
  ASSERT_TRUE(p.snapshot);
  EXPECT_EQ(p.snapshot->index.value(), 2u);
  EXPECT_EQ(p.term, 7u);
  EXPECT_EQ(p.voted_for, "node-b");
  EXPECT_EQ(p.commit, 2u);
  ASSERT_EQ(p.suffix.size(), 1u);
  EXPECT_EQ(p.suffix.front().command, state::canonical_event(events[2]));
  auto replayed = replay_recovered_domain(p);
  ASSERT_TRUE(replayed);
  EXPECT_EQ(replayed.value().dancers.size(), 1u);
  EXPECT_EQ(replayed.value().receipts.count("d2"), 0u);
  EXPECT_TRUE(state::resolve_receipt(replayed.value(), create()));
  ASSERT_TRUE(store.value().commit_through(state::LogIndex::parse(3).value()));
  projection = audit_recovered_domain_store(store.value());
  ASSERT_TRUE(projection);
  replayed = replay_recovered_domain(projection.value());
  ASSERT_TRUE(replayed);
  EXPECT_EQ(replayed.value().dancers.size(), 2u);
  EXPECT_EQ(replayed.value().receipts.at("d2").index, 3u);
}

TEST_F(RaftRecoveryProjectionTest, InstalledSnapshotWithEmptyWalPreservesReceiptsAfterRestart) {
  {
    auto store = state::FileEngine::open(path, options());
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().install_snapshot(snapshot));
    EXPECT_TRUE(store.value().log_events().empty());
    ASSERT_TRUE(audit_recovered_domain_store(store.value()));
  }
  auto store = state::FileEngine::open(path, options());
  ASSERT_TRUE(store);
  auto projection = audit_recovered_domain_store(store.value());
  ASSERT_TRUE(projection) << projection.error().to_string();
  EXPECT_TRUE(projection.value().suffix.empty());
  auto replayed = replay_recovered_domain(projection.value());
  ASSERT_TRUE(replayed);
  auto retry = state::resolve_receipt(replayed.value(), create());
  ASSERT_TRUE(retry);
  EXPECT_EQ(state::canonical_event(retry.value()), state::canonical_event(events[0]));
}

TEST_F(RaftRecoveryProjectionTest, FullWalAndEmptyStoreAlsoHaveConsistentProjections) {
  auto store = state::FileEngine::open(path, options());
  ASSERT_TRUE(store);
  auto empty = audit_recovered_domain_store(store.value());
  ASSERT_TRUE(empty);
  EXPECT_EQ(empty.value().commit, 0u);
  EXPECT_TRUE(empty.value().suffix.empty());
  for (const auto& event : events) ASSERT_TRUE(store.value().append_event(event));
  ASSERT_TRUE(store.value().commit_through(state::LogIndex::parse(2).value()));
  auto projection = audit_recovered_domain_store(store.value());
  ASSERT_TRUE(projection);
  EXPECT_FALSE(projection.value().snapshot);
  EXPECT_EQ(projection.value().suffix.size(), 3u);
  auto original = audit_domain_store(store.value());
  ASSERT_TRUE(original);
  EXPECT_EQ(original.value().applied_state_hash, projection.value().applied_state_hash);
}

TEST_F(RaftRecoveryProjectionTest, SnapshotAndIndexMutationsFailClosed) {
  auto store = state::FileEngine::open(path, options());
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().install_snapshot(snapshot));
  auto projection = audit_recovered_domain_store(store.value());
  ASSERT_TRUE(projection);
  auto bad = projection.value();
  bad.snapshot->state_hash = "corrupted";
  EXPECT_FALSE(replay_recovered_domain(bad));
  bad = projection.value();
  bad.snapshot->term = state::Term::parse(2).value();
  EXPECT_FALSE(replay_recovered_domain(bad));
  bad = projection.value();
  bad.term = 0;
  EXPECT_FALSE(replay_recovered_domain(bad));
  bad = projection.value();
  bad.commit = 1;
  bad.applied = 1;
  EXPECT_FALSE(replay_recovered_domain(bad));
  bad = projection.value();
  bad.commit = 3;
  bad.applied = 3;
  EXPECT_FALSE(replay_recovered_domain(bad));
  bad = projection.value();
  bad.applied = 1;
  EXPECT_FALSE(replay_recovered_domain(bad));
  bad = projection.value();
  bad.applied_state_hash = "different";
  EXPECT_FALSE(replay_recovered_domain(bad));
}

TEST_F(RaftRecoveryProjectionTest, UncommittedEnvelopeAndTermsAreStillValidated) {
  auto store = state::FileEngine::open(path, options());
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().install_snapshot(snapshot));
  ASSERT_TRUE(store.value().append_event(events[2]));
  auto projection = audit_recovered_domain_store(store.value());
  ASSERT_TRUE(projection);
  auto bad = projection.value();
  {
    auto changed = bad.suffix.front();
    changed.command = state::canonical_event(events[1]);
    bad.suffix.set(0, std::move(changed));
  }  // Index2, expected3.
  EXPECT_FALSE(replay_recovered_domain(bad));
  bad = projection.value();
  {
    auto changed = bad.suffix.front();
    changed.command += ";unknown=field";
    bad.suffix.set(0, std::move(changed));
  }
  EXPECT_FALSE(replay_recovered_domain(bad));
  bad = projection.value();
  {
    auto changed = bad.suffix.front();
    changed.term = 2;
    bad.suffix.set(0, std::move(changed));
  }
  EXPECT_FALSE(replay_recovered_domain(bad));
  auto future = events[2];
  future.term = state::Term::parse(2).value();
  bad.suffix.set(0, encode_domain_event(future).value());
  EXPECT_FALSE(replay_recovered_domain(bad));  // Canonical future-term envelope too.
  // Even a valid envelope must respect the snapshot term ordering.
  auto higher = events[1];
  higher.term = state::Term::parse(2).value();
  auto prefix = state::replay({events[0], higher});
  ASSERT_TRUE(prefix);
  bad = projection.value();
  bad.term = 3;
  bad.snapshot = storage::Snapshot{prefix.value().last_applied,
                                   prefix.value().term,
                                   state::state_hash(prefix.value()),
                                   {},
                                   state::snapshot_payload(prefix.value())};
  bad.applied_state_hash = state::state_hash(prefix.value());
  bad.applied_state_payload = state::snapshot_payload(prefix.value());
  EXPECT_FALSE(replay_recovered_domain(bad));  // Suffix term1 below snapshot term2.
}

TEST_F(RaftRecoveryProjectionTest, ReceiptDriftIsRejectedEvenWhenDomainHashIsUnchanged) {
  auto store = state::FileEngine::open(path, options());
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().install_snapshot(snapshot));
  auto projection = audit_recovered_domain_store(store.value());
  ASSERT_TRUE(projection);
  auto observed = storage::verify_snapshot(snapshot);
  ASSERT_TRUE(observed);
  observed.value().receipts.at("create").fingerprint ^= 1;
  EXPECT_EQ(state::state_hash(observed.value()), projection.value().applied_state_hash);
  projection.value().applied_state_payload = state::snapshot_payload(observed.value());
  EXPECT_FALSE(replay_recovered_domain(projection.value()));
}

TEST_F(RaftRecoveryProjectionTest, ReceiptsRepairedFromFullWalMustStillMatchSnapshotProjection) {
  state::ChoreographyState observed;
  {
    auto store = state::FileEngine::open(path, options());
    ASSERT_TRUE(store);
    for (const auto& event : events) ASSERT_TRUE(store.value().append_event(event));
    ASSERT_TRUE(store.value().commit_through(state::LogIndex::parse(2).value()));
    ASSERT_TRUE(store.value().checkpoint());
    observed = store.value().engine().state();
  }
  const auto original_hash = state::state_hash(observed);
  observed.receipts.at("create").fingerprint ^= 1;
  ASSERT_EQ(state::state_hash(observed), original_hash);
  // A fully checksummed, parseable snapshot can still have receipts that
  // disagree with retained history: the domain hash does not cover receipts.
  ASSERT_TRUE(storage::save_snapshot(path, observed, true));
  auto store = state::FileEngine::open(path, options());
  ASSERT_TRUE(store);
  EXPECT_EQ(state::state_hash(store.value().engine().state()), original_hash);
  // FileEngine rebuilds receipts from complete retained history. Its final
  // state agrees with full replay, but the snapshot base still disagrees.
  EXPECT_TRUE(audit_domain_store(store.value()));
  EXPECT_FALSE(audit_recovered_domain_store(store.value()));
}

TEST_F(RaftRecoveryProjectionTest, EverySnapshotRedoBoundaryRequiresRecoveryBeforeProjection) {
  for (const auto fault :
       {storage::InstallFault::AfterJournal, storage::InstallFault::AfterSnapshot,
        storage::InstallFault::AfterMetadata, storage::InstallFault::AfterWal}) {
    const auto directory = path / std::to_string(static_cast<int>(fault));
    {
      auto store = state::FileEngine::open(directory, options());
      ASSERT_TRUE(store);
      ASSERT_TRUE(store.value().arm_snapshot_install_fault(fault));
      EXPECT_FALSE(store.value().install_snapshot(snapshot));
      EXPECT_FALSE(audit_recovered_domain_store(store.value()));
    }
    auto store = state::FileEngine::open(directory, options());
    ASSERT_TRUE(store);
    auto projection = audit_recovered_domain_store(store.value());
    ASSERT_TRUE(projection) << projection.error().to_string();
    EXPECT_EQ(projection.value().commit, 2u);
    EXPECT_TRUE(replay_recovered_domain(projection.value()));
  }
}

TEST_F(RaftRecoveryProjectionTest, MetadataCorruptionIsNotReplacedWithDefaults) {
  auto store = state::FileEngine::open(path, options());
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().install_snapshot(snapshot));
  std::ofstream corrupt(path / "meta.bin", std::ios::binary | std::ios::trunc);
  corrupt << "corrupt";
  corrupt.close();
  EXPECT_FALSE(audit_recovered_domain_store(store.value()));
}

TEST_F(RaftRecoveryProjectionTest, RetainedAnchorMustAgreeWithTheSnapshotTerm) {
  events[1].term = state::Term::parse(2).value();
  events[2].term = state::Term::parse(2).value();
  {
    auto store = state::FileEngine::open(path, options());
    ASSERT_TRUE(store);
    for (const auto& event : events) ASSERT_TRUE(store.value().append_event(event));
    ASSERT_TRUE(store.value().commit_through(state::LogIndex::parse(2).value()));
    ASSERT_TRUE(store.value().checkpoint());
    ASSERT_TRUE(store.value().discard_compacted_prefix());
    ASSERT_TRUE(store.value().persist_consensus(state::Term::parse(5).value(), std::nullopt));
  }
  {
    auto wal = storage::WriteAheadLog::open(state::event_log_path(path));
    ASSERT_TRUE(wal);
    auto retained = std::vector<state::Event>{events[1], events[2]};
    retained[0].term = state::Term::parse(1).value();
    ASSERT_TRUE(wal.value().rewrite(retained));
  }
  // Recovery uses the valid snapshot and leaves the uncommitted suffix alone;
  // the correspondence checker must additionally notice the wrong anchor.
  auto store = state::FileEngine::open(path, options());
  ASSERT_TRUE(store);
  EXPECT_FALSE(audit_recovered_domain_store(store.value()));
}

}  // namespace
}  // namespace choreoos::raft
