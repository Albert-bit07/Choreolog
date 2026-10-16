#include "choreoos/raft/runtime_host.hpp"

#include <gtest/gtest.h>

#include <filesystem>

#include "choreoos/replication/runtime_replica.hpp"

namespace choreoos::raft {
namespace {

class RuntimeHostTest : public testing::Test {
 protected:
  std::filesystem::path path;
  std::vector<state::Event> events;
  void SetUp() override {
    path = std::filesystem::temp_directory_path() /
           (std::string("choreoos-runtime-host-") +
            testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::remove_all(path);
    const auto show = state::ChoreographyId::parse("opening").value();
    state::Engine engine;
    ASSERT_TRUE(engine.submit(
        {state::CommandId::parse("create").value(), show, state::CommandType::CreateChoreography,
         state::kCurrentSchemaVersion, state::MusicalTick::from_count(0).value(),
         state::CreateChoreographyPayload{show, state::StageBounds::from_mm(20000, 12000).value(),
                                          state::OverlapPolicy::Forbidden, 5000}}));
    for (int i = 1; i <= 2; ++i) {
      const auto id = "d" + std::to_string(i);
      ASSERT_TRUE(
          engine.submit({state::CommandId::parse(id).value(), show, state::CommandType::AddDancer,
                         state::kCurrentSchemaVersion, state::MusicalTick::from_count(i).value(),
                         state::DancerPayload{state::DancerId::parse(id).value(),
                                              state::Position::from_mm(i * 2000, 1000).value()}}));
    }
    events = engine.events();
  }
  void TearDown() override { std::filesystem::remove_all(path); }
  Entry entry(std::size_t offset, std::uint64_t term) const {
    auto event = events[offset];
    event.term = state::Term::parse(term).value();
    return encode_domain_event(event).value();
  }
  storage::Snapshot snapshot(std::uint64_t last_term = 2) const {
    state::Engine engine;
    for (std::size_t i = 0; i < 2; ++i) {
      auto event = events[i];
      event.term = state::Term::parse(i == 0 ? 1 : last_term).value();
      if (!engine.apply_committed(event)) throw std::logic_error("invalid test snapshot");
    }
    return {engine.state().last_applied,
            engine.state().term,
            state::state_hash(engine.state()),
            {},
            state::snapshot_payload(engine.state())};
  }
};

TEST_F(RuntimeHostTest, SingleNodeCommitConfirmsDurableApplicationExactlyOnceAndReopens) {
  {
    auto host = RuntimeHost::open({"a", {}}, path, 0);
    ASSERT_TRUE(host);
    ASSERT_TRUE(host.value().dispatch(ElectionTimeout{}));
    const auto term = host.value().node().durable.term;
    auto committed = host.value().dispatch(ClientPropose{entry(0, term).command});
    ASSERT_TRUE(committed);
    EXPECT_EQ(host.value().node().durable.commit, 1u);
    EXPECT_EQ(host.value().node().durable.applied, 1u);
    EXPECT_EQ(host.value().store().engine().events().size(), 1u);
    EXPECT_EQ(host.value().store().engine().state().receipts.at("create").index, 1u);
    auto again = host.value().dispatch(ApplyCommitted{});
    ASSERT_TRUE(again);
    EXPECT_EQ(host.value().store().engine().events().size(), 1u);
    EXPECT_EQ(host.value().store().engine().state().receipts.size(), 1u);
  }
  auto reopened = RuntimeHost::open({"a", {}}, path, 0);
  ASSERT_TRUE(reopened);
  EXPECT_EQ(reopened.value().node().durable.applied, 1u);
  EXPECT_EQ(reopened.value().node().volatile_state.role, Role::Follower);
}

TEST_F(RuntimeHostTest, MajorityAckCommitsOnlyTheSentEntryAfterActualDiskPersistence) {
  auto host = RuntimeHost::open({"a", {"b", "c"}}, path, 0);
  ASSERT_TRUE(host);
  auto election = host.value().dispatch(ElectionTimeout{});
  ASSERT_TRUE(election);
  ASSERT_EQ(election.value().send.size(), 2u);
  auto metadata = host.value().store().consensus_metadata();
  ASSERT_TRUE(metadata);
  EXPECT_EQ(metadata.value().term.value(), 2u);
  ASSERT_TRUE(metadata.value().voted_for);
  EXPECT_EQ(metadata.value().voted_for->value(), "a");
  auto leader = host.value().dispatch(RecvVoteReply{VoteReply{2, "b", true, 1}});
  ASSERT_TRUE(leader);
  ASSERT_EQ(leader.value().send.size(), 2u);
  auto first = std::get<AppendEntries>(leader.value().send.front().message);
  ASSERT_TRUE(host.value().dispatch(ClientPropose{entry(0, 2).command}));
  EXPECT_EQ(host.value().store().last_log_index(), 1u);
  EXPECT_EQ(host.value().store().commit_index().value(), 0u);
  EXPECT_TRUE(host.value().store().engine().state().receipts.empty());
  ASSERT_TRUE(
      host.value().dispatch(RecvAppendReply{AppendReply{2, "b", true, 999, first.request_id}}));
  auto sending = host.value().dispatch(HeartbeatTimeout{});
  ASSERT_TRUE(sending);
  const auto append = std::get<AppendEntries>(sending.value().send.front().message);
  ASSERT_EQ(append.entries.size(), 1u);
  auto acknowledged =
      host.value().dispatch(RecvAppendReply{AppendReply{2, "b", true, 999, append.request_id}});
  ASSERT_TRUE(acknowledged);
  EXPECT_EQ(host.value().node().durable.commit, 1u);
  EXPECT_EQ(host.value().node().durable.applied, 1u);
  EXPECT_EQ(host.value().store().engine().state().receipts.at("create").index, 1u);
}

TEST_F(RuntimeHostTest, FailedMetadataPublishesNoVoteAndRequiresRealDiskRecovery) {
  {
    auto host = RuntimeHost::open({"a", {"b"}}, path, 0, true);
    ASSERT_TRUE(host);
    ASSERT_TRUE(host.value().inject_next_fault_for_test(DomainWriteFault::AfterMetadata));
    EXPECT_FALSE(host.value().dispatch(ElectionTimeout{}));
    EXPECT_TRUE(host.value().node().halted);
    EXPECT_EQ(host.value().node().durable.term, 1u);
    EXPECT_FALSE(host.value().dispatch(Restart{}));
  }
  auto recovered = RuntimeHost::open({"a", {"b"}}, path, 0);
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered.value().node().durable.term, 2u);
  EXPECT_EQ(recovered.value().node().durable.voted_for, "a");
  EXPECT_EQ(recovered.value().node().volatile_state.role, Role::Follower);
}

TEST_F(RuntimeHostTest, AmbiguousAppendNeverConfirmsCommitAndRecoveryRetainsUncommittedTail) {
  {
    auto host = RuntimeHost::open({"a", {}}, path, 0, true);
    ASSERT_TRUE(host);
    ASSERT_TRUE(host.value().dispatch(ElectionTimeout{}));
    ASSERT_TRUE(host.value().inject_next_fault_for_test(DomainWriteFault::AppendAfterFlush));
    EXPECT_FALSE(host.value().dispatch(ClientPropose{entry(0, 2).command}));
    EXPECT_TRUE(host.value().node().halted);
    EXPECT_EQ(host.value().node().durable.commit, 0u);
  }
  auto recovered = RuntimeHost::open({"a", {}}, path, 0);
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered.value().node().durable.log.size(), 1u);
  EXPECT_EQ(recovered.value().node().durable.commit, 0u);
  EXPECT_TRUE(recovered.value().store().engine().state().receipts.empty());
}

TEST_F(RuntimeHostTest, AutomaticMaintenanceChecksActualApplicationReceiptsAndBoundary) {
  auto host = RuntimeHost::open({"a", {}}, path, 2);
  ASSERT_TRUE(host);
  ASSERT_TRUE(host.value().dispatch(ElectionTimeout{}));
  ASSERT_TRUE(host.value().dispatch(ClientPropose{entry(0, 2).command}));
  ASSERT_TRUE(host.value().dispatch(ClientPropose{entry(1, 2).command}));
  EXPECT_EQ(host.value().node().durable.snapshot_index, 2u);
  EXPECT_TRUE(host.value().node().durable.log.empty());
  ASSERT_TRUE(host.value().store().latest_snapshot());
  auto verified = storage::verify_snapshot(*host.value().store().latest_snapshot());
  ASSERT_TRUE(verified);
  EXPECT_EQ(verified.value().receipts.size(), 2u);
  EXPECT_EQ(verified.value().receipts.at("d1").index, 2u);
}

TEST_F(RuntimeHostTest, SnapshotNeedsVerifiedPayloadAndCompletesTermAndApplicationBeforeReply) {
  auto host = RuntimeHost::open({"a", {"b"}}, path, 0);
  ASSERT_TRUE(host);
  const auto data = snapshot();
  auto observed = host.value().dispatch(RecvSnapshot{{4, "b", 2, 2, 11, false}});
  ASSERT_TRUE(observed);
  EXPECT_TRUE(observed.value().send.empty());
  EXPECT_EQ(host.value().store().consensus_metadata().value().term.value(), 4u);
  auto installed = host.value().dispatch(RecvSnapshot{{4, "b", 2, 2, 12}}, &data);
  ASSERT_TRUE(installed);
  ASSERT_EQ(installed.value().send.size(), 1u);
  const auto response = std::get<SnapshotReply>(installed.value().send.front().message);
  EXPECT_TRUE(response.success);
  EXPECT_EQ(response.request_id, 12u);
  EXPECT_EQ(host.value().node().durable.snapshot_index, 2u);
  EXPECT_EQ(host.value().node().durable.applied, 2u);
  EXPECT_EQ(host.value().store().engine().state().receipts.at("d1").index, 2u);
  EXPECT_EQ(host.value().store().recovery().state_hash, data.state_hash);
}

TEST_F(RuntimeHostTest, SnapshotIntentWithoutApplicationMaterialCannotReleaseSuccess) {
  auto host = RuntimeHost::open({"a", {"b"}}, path, 0);
  ASSERT_TRUE(host);
  EXPECT_FALSE(host.value().dispatch(RecvSnapshot{{4, "b", 2, 2, 12}}));
  EXPECT_TRUE(host.value().node().halted);
  EXPECT_EQ(host.value().store().commit_index().value(), 0u);
}

TEST_F(RuntimeHostTest, SnapshotInstallationRetainsOnlyAnAgreeingUncommittedSuffix) {
  for (bool matching : {false, true}) {
    const auto directory = path / (matching ? "matching" : "conflicting");
    {
      state::StoreOptions options;
      options.commit_on_append = false;
      auto store = state::FileEngine::open(directory, options);
      ASSERT_TRUE(store);
      ASSERT_TRUE(store.value().persist_consensus(state::Term::parse(4).value(), std::nullopt));
      for (std::size_t i = 0; i < events.size(); ++i) {
        auto event = events[i];
        event.term = state::Term::parse(i + 1).value();
        ASSERT_TRUE(store.value().append_event(event));
      }
      ASSERT_TRUE(store.value().commit_through(state::LogIndex::parse(1).value()));
    }
    auto host = RuntimeHost::open({"a", {"b"}}, directory, 0);
    ASSERT_TRUE(host);
    const auto data = snapshot(matching ? 2 : 3);
    auto installed = host.value().dispatch(RecvSnapshot{{5, "b", 2, data.term.value(), 1}}, &data);
    ASSERT_TRUE(installed);
    EXPECT_EQ(host.value().node().durable.log.size(), matching ? 1u : 0u);
    EXPECT_EQ(host.value().store().last_log_index(), matching ? 3u : 2u);
    EXPECT_EQ(host.value().node().durable.commit, 2u);
  }
}

TEST_F(RuntimeHostTest, SnapshotStreamRemainsPinnedWhenAnotherQuorumMakesANewerCheckpoint) {
  {
    state::StoreOptions options;
    options.commit_on_append = false;
    auto store = state::FileEngine::open(path, options);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().persist_consensus(state::Term::parse(4).value(), std::nullopt));
    for (const auto& event : events) ASSERT_TRUE(store.value().append_event(event));
    ASSERT_TRUE(store.value().commit_through(state::LogIndex::parse(3).value()));
    ASSERT_TRUE(store.value().checkpoint());
    ASSERT_TRUE(store.value().discard_compacted_prefix());
  }
  auto host = RuntimeHost::open({"a", {"b", "c"}}, path, 1);
  ASSERT_TRUE(host);
  ASSERT_TRUE(host.value().dispatch(ElectionTimeout{}));
  auto elected = host.value().dispatch(RecvVoteReply{{5, "c", true, 1}});
  ASSERT_TRUE(elected);
  RequestId first_b = 0;
  for (const auto& outbound : elected.value().send)
    if (outbound.to == "b") first_b = std::get<AppendEntries>(outbound.message).request_id;
  ASSERT_NE(first_b, 0u);
  ASSERT_TRUE(host.value().dispatch(RecvAppendReply{{5, "b", false, 0, first_b}}));
  auto requesting = host.value().dispatch(HeartbeatTimeout{});
  ASSERT_TRUE(requesting);
  ASSERT_EQ(requesting.value().snapshots.size(), 1u);
  const auto pinned = requesting.value().snapshots.front();
  EXPECT_EQ(pinned.snapshot->index.value(), 3u);
  RequestId first_c = 0;
  for (const auto& outbound : requesting.value().send)
    if (outbound.to == "c") first_c = std::get<AppendEntries>(outbound.message).request_id;
  ASSERT_NE(first_c, 0u);
  ASSERT_TRUE(host.value().dispatch(RecvAppendReply{{5, "c", true, 3, first_c}}));
  const auto show = state::ChoreographyId::parse("opening").value();
  auto proposed =
      state::propose(host.value().store().engine().state(),
                     {state::CommandId::parse("d3").value(), show, state::CommandType::AddDancer,
                      state::kCurrentSchemaVersion, state::MusicalTick::from_count(3).value(),
                      state::DancerPayload{state::DancerId::parse("d3").value(),
                                           state::Position::from_mm(6000, 1000).value()}});
  ASSERT_TRUE(proposed);
  proposed.value().term = state::Term::parse(5).value();
  auto encoded = encode_domain_event(proposed.value());
  ASSERT_TRUE(encoded);
  auto appended = host.value().dispatch(ClientPropose{encoded.value().command});
  ASSERT_TRUE(appended);
  RequestId next_c = 0;
  for (const auto& outbound : appended.value().send)
    if (outbound.to == "c") next_c = std::get<AppendEntries>(outbound.message).request_id;
  ASSERT_NE(next_c, 0u);
  ASSERT_TRUE(host.value().dispatch(RecvAppendReply{{5, "c", true, 4, next_c}}));
  EXPECT_EQ(host.value().node().durable.snapshot_index, 4u);
  auto retry = host.value().dispatch(HeartbeatTimeout{});
  ASSERT_TRUE(retry);
  ASSERT_EQ(retry.value().snapshots.size(), 1u);
  EXPECT_EQ(retry.value().snapshots.front().snapshot, pinned.snapshot);
  EXPECT_EQ(retry.value().snapshots.front().request.request_id, pinned.request.request_id);
  EXPECT_EQ(retry.value().snapshots.front().snapshot->index.value(), 3u);
  EXPECT_TRUE(storage::verify_snapshot(*pinned.snapshot));
}

TEST_F(RuntimeHostTest, WriterLeaseExcludesBothRuntimeBackendsAndReleasesOnClose) {
  replication::ReplicaConfig config;
  config.id = "a";
  config.leader_id = "a";
  config.directory = path;
  config.elections = true;
  {
    auto core = replication::RuntimeReplica::open(config, true);
    ASSERT_TRUE(core);
    EXPECT_FALSE(replication::RuntimeReplica::open(config, false));
    EXPECT_FALSE(RuntimeHost::open({"a", {}}, path));
  }
  {
    auto legacy = replication::RuntimeReplica::open(config, false);
    ASSERT_TRUE(legacy);
    EXPECT_FALSE(replication::RuntimeReplica::open(config, true));
  }
  EXPECT_TRUE(replication::RuntimeReplica::open(config, true));
}

}  // namespace
}  // namespace choreoos::raft
