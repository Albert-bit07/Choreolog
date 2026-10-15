#include <gtest/gtest.h>

#include <filesystem>
#include <limits>
#include <stdexcept>

#include "choreoos/raft/checkpoint.hpp"
#include "choreoos/raft/file_engine_adapter.hpp"
#include "choreoos/raft/trace.hpp"

namespace choreoos::raft {
namespace {

const Config config{"a", {"b"}};

Node recovered() {
  auto node = initial(config);
  node.durable.term = 4;
  node.durable.snapshot_index = 5;
  node.durable.snapshot_term = 2;
  node.durable.log = {{3, "entry-6"}, {4, "entry-7"}};
  node.durable.commit = 6;
  node.durable.applied = 6;
  return node;
}

std::pair<Node, Effects> finish(Node node, const Effects& effects) {
  if (!effects.persist) throw std::logic_error("expected a persistence request");
  return step(config, std::move(node), PersistCompleted{effects.persist->token});
}

TEST(RaftSnapshotOffsetTest, ElectionsAdvertiseAbsoluteLastIndexAndWaitForPersistence) {
  auto [staged, effects] = step(config, recovered(), ElectionTimeout{});
  ASSERT_TRUE(effects.persist);
  EXPECT_TRUE(effects.send_now.empty());
  EXPECT_EQ(staged.durable.term, 4u);
  auto [candidate, sent] = finish(staged, effects);
  ASSERT_EQ(sent.send_now.size(), 1u);
  const auto request = std::get<RequestVote>(sent.send_now.front().message);
  EXPECT_EQ(request.last_index, 7u);
  EXPECT_EQ(request.last_term, 4u);
  EXPECT_EQ(request.term, 5u);
  auto [leader, heartbeats] =
      step(config, candidate, RecvVoteReply{VoteReply{5, "b", true, request.request_id}});
  ASSERT_EQ(leader.volatile_state.role, Role::Leader);
  ASSERT_EQ(heartbeats.send_now.size(), 1u);
  const auto append = std::get<AppendEntries>(heartbeats.send_now.front().message);
  EXPECT_EQ(append.prev_index, 7u);
  EXPECT_EQ(append.prev_term, 4u);
  EXPECT_TRUE(append.entries.empty());
}

TEST(RaftSnapshotOffsetTest, VotingComparesAbsoluteIndexesIncludingSnapshotOnlyLogs) {
  for (const bool snapshot_only : {false, true}) {
    auto node = recovered();
    if (snapshot_only) {
      node.durable.log.clear();
      node.durable.commit = node.durable.applied = 5;
    }
    const std::uint64_t last = snapshot_only ? 5 : 7;
    const std::uint64_t term = snapshot_only ? 2 : 4;
    for (const bool up_to_date : {false, true}) {
      auto [staged, effects] = step(
          config, node, RecvRequestVote{RequestVote{5, "b", last - (up_to_date ? 0 : 1), term, 1}});
      ASSERT_TRUE(effects.persist);
      EXPECT_TRUE(effects.send_now.empty());
      auto [written, sent] = finish(staged, effects);
      ASSERT_EQ(sent.send_now.size(), 1u);
      EXPECT_EQ(std::get<VoteReply>(sent.send_now.front().message).granted, up_to_date);
      EXPECT_EQ(written.durable.snapshot_index, 5u);
    }
  }
}

TEST(RaftSnapshotOffsetTest, SendingUsesSnapshotTermAndSuffixOffsets) {
  const std::uint64_t terms[]{2, 3, 4};
  for (std::uint64_t next = 6; next <= 8; ++next) {
    auto leader = recovered();
    leader.volatile_state.role = Role::Leader;
    leader.volatile_state.peers.at("b").next = next;
    auto [sending, effects] = step(config, leader, HeartbeatTimeout{});
    ASSERT_EQ(effects.send_now.size(), 1u);
    const auto append = std::get<AppendEntries>(effects.send_now.front().message);
    EXPECT_EQ(append.prev_index, next - 1);
    EXPECT_EQ(append.prev_term, terms[next - 6]);
    EXPECT_EQ(append.entries.size(), next <= 7 ? 1u : 0u);
    if (next <= 7) {
      EXPECT_EQ(append.entries.front(), leader.durable.log[next - 6]);
    }
    EXPECT_EQ(sending.volatile_state.peers.at("b").in_flight->prev, next - 1);
  }
}

TEST(RaftSnapshotOffsetTest, BoundaryAppendRepairsOnlyUncommittedSuffixAndAppliesAbsoluteEntry) {
  auto node = recovered();
  node.durable.commit = node.durable.applied = 5;
  const AppendEntries message{4, "b", 5, 2, {{3, "replacement-6"}, {4, "replacement-7"}}, 6, 9};
  auto [staged, effects] = step(config, node, RecvAppend{message});
  ASSERT_TRUE(effects.persist);
  EXPECT_EQ(staged.durable.log, node.durable.log);
  EXPECT_TRUE(effects.send_now.empty());
  EXPECT_EQ(effects.persist->target.commit, 6u);
  auto [written, sent] = finish(staged, effects);
  ASSERT_EQ(sent.send_now.size(), 1u);
  EXPECT_TRUE(std::get<AppendReply>(sent.send_now.front().message).success);
  EXPECT_EQ(written.durable.snapshot_index, 5u);
  EXPECT_EQ(written.durable.applied, 5u);
  auto [applying, checkpoint] = step(config, written, ApplyCommitted{});
  ASSERT_TRUE(checkpoint.persist);
  EXPECT_TRUE(checkpoint.apply.empty());
  auto [applied, released] = finish(applying, checkpoint);
  ASSERT_EQ(released.apply.size(), 1u);
  EXPECT_EQ(released.apply.front().command, "replacement-6");
  EXPECT_EQ(applied.durable.applied, 6u);
}

TEST(RaftSnapshotOffsetTest, CompactedOrWrongBoundaryRequestsCannotRewriteHiddenHistory) {
  for (const auto prev : {4u, 5u}) {
    auto node = recovered();
    const AppendEntries message{5, "b", prev, 3, {}, 7, 9};
    auto [staged, effects] = step(config, node, RecvAppend{message});
    ASSERT_TRUE(effects.persist);  // Only the higher term is written.
    EXPECT_EQ(effects.persist->target.log, node.durable.log);
    EXPECT_EQ(effects.persist->target.commit, node.durable.commit);
    auto [written, sent] = finish(staged, effects);
    ASSERT_EQ(sent.send_now.size(), 1u);
    EXPECT_FALSE(std::get<AppendReply>(sent.send_now.front().message).success);
    EXPECT_EQ(written.durable.snapshot_index, 5u);
  }
}

TEST(RaftSnapshotOffsetTest, CommittedConflictAndShortHeartbeatPreserveAbsoluteCommit) {
  auto node = recovered();
  auto [same, rejected] =
      step(config, node, RecvAppend{AppendEntries{4, "b", 5, 2, {{3, "conflict-6"}}, 7, 1}});
  EXPECT_FALSE(rejected.persist);
  ASSERT_EQ(rejected.send_now.size(), 1u);
  EXPECT_FALSE(std::get<AppendReply>(rejected.send_now.front().message).success);
  EXPECT_EQ(same.durable, node.durable);
  auto [heartbeat, acknowledged] =
      step(config, node, RecvAppend{AppendEntries{4, "b", 5, 2, {}, 7, 2}});
  EXPECT_FALSE(acknowledged.persist);
  EXPECT_EQ(heartbeat.durable, node.durable);
  EXPECT_TRUE(std::get<AppendReply>(acknowledged.send_now.front().message).success);
}

TEST(RaftSnapshotOffsetTest, QuorumCommitAndApplicationCountAbsoluteIndexes) {
  auto leader = recovered();
  leader.volatile_state.role = Role::Leader;
  leader.volatile_state.peers.at("b").match = 7;
  auto [staged, effects] = step(config, leader, AdvanceCommit{});
  ASSERT_TRUE(effects.persist);
  EXPECT_EQ(effects.persist->target.commit, 7u);
  auto [written, no_send] = finish(staged, effects);
  EXPECT_TRUE(no_send.send_now.empty());
  auto [applying, checkpoint] = step(config, written, ApplyCommitted{});
  auto [applied, released] = finish(applying, checkpoint);
  ASSERT_EQ(released.apply.size(), 1u);
  EXPECT_EQ(released.apply.front().command, "entry-7");
  EXPECT_EQ(applied.durable.applied, 7u);
  leader.durable.log.pop_back();
  leader.durable.commit = leader.durable.applied = 5;
  leader.volatile_state.peers.at("b").match = 6;
  auto [old_term, ignored] = step(config, leader, AdvanceCommit{});
  EXPECT_FALSE(ignored.persist);
  EXPECT_EQ(old_term.durable.commit, 5u);
}

TEST(RaftSnapshotOffsetTest, LaggingPeerRequestsSnapshotInsteadOfFabricatedEntries) {
  auto leader = recovered();
  leader.volatile_state.role = Role::Leader;
  leader.volatile_state.peers.at("b").next = 3;
  auto [same, effects] = step(config, leader, HeartbeatTimeout{});
  EXPECT_TRUE(effects.send_now.empty());
  EXPECT_FALSE(effects.persist);
  ASSERT_EQ(effects.snapshot_needed.size(), 1u);
  EXPECT_EQ(effects.snapshot_needed.front().peer, "b");
  EXPECT_EQ(effects.snapshot_needed.front().index, 5u);
  EXPECT_EQ(effects.snapshot_needed.front().term, 2u);
  EXPECT_EQ(same.durable, leader.durable);
  auto [staged, proposal] = step(config, leader, ClientPropose{"entry-8"});
  ASSERT_TRUE(proposal.persist);
  EXPECT_TRUE(proposal.snapshot_needed.empty());
  auto [written, released] = finish(staged, proposal);
  ASSERT_EQ(released.snapshot_needed.size(), 1u);
  EXPECT_EQ(written.durable.log.size(), 3u);
  auto [halted, failed] = step(config, staged, PersistFailed{proposal.persist->token});
  EXPECT_TRUE(halted.halted);
  EXPECT_TRUE(failed.snapshot_needed.empty());
}

TEST(RaftSnapshotOffsetTest, InvalidRecoveredBoundariesFailStopRatherThanWrappingIndexes) {
  for (int mutation = 0; mutation < 6; ++mutation) {
    auto node = recovered();
    if (mutation == 0) node.durable.snapshot_term = 0;
    if (mutation == 1) node.durable.commit = 4;
    if (mutation == 2) node.durable.applied = 4;
    if (mutation == 3) {
      auto changed = node.durable.log[0];
      changed.term = 1;
      node.durable.log.set(0, std::move(changed));
    }
    if (mutation == 4) {
      auto changed = node.durable.log[1];
      changed.term = 5;
      node.durable.log.set(1, std::move(changed));
    }
    if (mutation == 5) node.durable.snapshot_index = std::numeric_limits<std::uint64_t>::max();
    auto [halted, effects] = step(config, node, Restart{});
    EXPECT_TRUE(halted.halted);
    EXPECT_TRUE(effects.halt);
    EXPECT_FALSE(effects.persist);
    EXPECT_TRUE(effects.send_now.empty());
    EXPECT_TRUE(effects.snapshot_needed.empty());
  }
}

TEST(RaftSnapshotOffsetTest, InvalidAppendBoundsAndTermsCannotChangeDurableIdentity) {
  const auto maximum = std::numeric_limits<std::uint64_t>::max();
  const AppendEntries invalid[]{{8, "b", maximum, 2, {}, 7, 1},
                                {8, "b", maximum - 1, 2, {{3, "overflow"}}, 7, 2},
                                {8, "b", 5, 2, {{1, "decreasing"}}, 7, 3},
                                {8, "b", 5, 2, {{9, "future"}}, 7, 4}};
  for (const auto& message : invalid) {
    auto node = recovered();
    auto [same, effects] = step(config, node, RecvAppend{message});
    EXPECT_EQ(same.durable, node.durable);
    EXPECT_FALSE(effects.persist);
    EXPECT_TRUE(effects.send_now.empty());
  }
  // The largest supported boundary must remain usable without wrapping next.
  auto edge = initial(config);
  edge.durable.term = 2;
  edge.durable.snapshot_index = maximum - 1;
  edge.durable.snapshot_term = 1;
  edge.durable.commit = edge.durable.applied = maximum - 1;
  edge.volatile_state.role = Role::Leader;
  edge.volatile_state.peers.at("b").next = maximum;
  auto [sending, effects] = step(config, edge, HeartbeatTimeout{});
  EXPECT_FALSE(sending.halted);
  ASSERT_EQ(effects.send_now.size(), 1u);
  EXPECT_EQ(std::get<AppendEntries>(effects.send_now.front().message).prev_index, maximum - 1);
  auto [unchanged, refused] = step(config, edge, ClientPropose{"would-overflow"});
  EXPECT_FALSE(refused.persist);
  EXPECT_EQ(unchanged.durable, edge.durable);
}

TEST(RaftSnapshotOffsetTest, LegacyCheckpointReplayAndTraceRefuseUnmappedSnapshots) {
  const auto path = std::filesystem::temp_directory_path() / "choreoos-offset-v1-refusal";
  std::filesystem::remove_all(path);
  auto checkpoint = CheckpointStore::open(path);
  ASSERT_TRUE(checkpoint);
  auto node = recovered();
  EXPECT_FALSE(checkpoint.value().persist(node.durable));
  EXPECT_FALSE(replay_domain_committed(node.durable));
  EXPECT_THROW((void)trace_line(config, node, HeartbeatTimeout{}, 0), std::invalid_argument);
  EXPECT_FALSE(checkpoint.value().uncertain());
  std::filesystem::remove_all(path);
}

TEST(RaftSnapshotOffsetTest, ProductionSnapshotRecoveryCanBackAnOffsetElectionWrite) {
  const auto path = std::filesystem::temp_directory_path() / "choreoos-offset-election";
  std::filesystem::remove_all(path);
  using namespace state;
  const auto show = ChoreographyId::parse("opening").value();
  Engine engine;
  ASSERT_TRUE(
      engine.submit({CommandId::parse("create").value(), show, CommandType::CreateChoreography,
                     kCurrentSchemaVersion, MusicalTick::from_count(0).value(),
                     CreateChoreographyPayload{show, StageBounds::from_mm(20000, 12000).value(),
                                               OverlapPolicy::Forbidden, 5000}}));
  {
    StoreOptions options;
    options.commit_on_append = false;
    auto store = FileEngine::open(path, options);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().install_snapshot({engine.state().last_applied,
                                                engine.state().term,
                                                state_hash(engine.state()),
                                                {},
                                                snapshot_payload(engine.state())}));
  }
  auto adapter = FileEngineAdapter::open(path);
  ASSERT_TRUE(adapter);
  auto projection = adapter.value().current();
  ASSERT_TRUE(projection);
  auto node = initial(config);
  node.durable.term = projection.value().term;
  node.durable.log = projection.value().suffix;
  node.durable.commit = projection.value().commit;
  node.durable.applied = projection.value().applied;
  node.durable.snapshot_index = projection.value().snapshot->index.value();
  node.durable.snapshot_term = projection.value().snapshot->term.value();
  auto [staged, effects] = step(config, node, ElectionTimeout{});
  ASSERT_TRUE(effects.persist);
  const auto& target = effects.persist->target;
  DomainWritePlan plan{target.snapshot_index, target.snapshot_term, target.term,
                       target.voted_for,      target.log,           target.commit};
  ASSERT_TRUE(adapter.value().persist(plan));
  auto [candidate, released] = finish(staged, effects);
  ASSERT_EQ(released.send_now.size(), 1u);
  EXPECT_EQ(std::get<RequestVote>(released.send_now.front().message).last_index, 1u);
  EXPECT_EQ(candidate.durable.applied, 1u);
  auto disk = adapter.value().recover();
  ASSERT_TRUE(disk);
  EXPECT_EQ(disk.value().voted_for, "a");
  EXPECT_EQ(disk.value().applied_state_payload, projection.value().applied_state_payload);
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace choreoos::raft
