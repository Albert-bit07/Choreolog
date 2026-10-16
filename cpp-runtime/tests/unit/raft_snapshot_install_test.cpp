#include <gtest/gtest.h>

#include "choreoos/raft/trace.hpp"

namespace choreoos::raft {
namespace {
const Config config{"a", {"b"}};
Node recovered() {
  auto node = initial(config);
  node.durable.term = 4;
  node.durable.voted_for = "a";
  node.durable.snapshot_index = 5;
  node.durable.snapshot_term = 2;
  node.durable.log = {{3, "entry-6"}, {4, "entry-7"}, {4, "entry-8"}};
  node.durable.commit = node.durable.applied = 6;
  return node;
}

TEST(RaftSnapshotInstallTest, NewBoundaryAndApplicationWaitForMatchingDurableCompletion) {
  const auto original = recovered();
  auto [staged, effects] = step(config, original, RecvSnapshot{{7, "b", 10, 6, 99}});
  ASSERT_TRUE(effects.persist);
  EXPECT_EQ(effects.persist->kind, PersistKind::SnapshotInstall);
  EXPECT_EQ(staged.durable, original.durable);
  EXPECT_TRUE(effects.send_now.empty());
  const auto& target = effects.persist->target;
  EXPECT_EQ(target.term, 7u);
  EXPECT_FALSE(target.voted_for);
  EXPECT_EQ(target.snapshot_index, 10u);
  EXPECT_EQ(target.snapshot_term, 6u);
  EXPECT_EQ(target.commit, 10u);
  EXPECT_EQ(target.applied, 10u);
  EXPECT_TRUE(target.log.empty());
  auto [waiting, stale] = step(config, staged, PersistCompleted{effects.persist->token + 1});
  EXPECT_TRUE(waiting.pending);
  EXPECT_TRUE(stale.send_now.empty());
  auto [installed, reply] = step(config, waiting, PersistCompleted{effects.persist->token});
  EXPECT_EQ(installed.durable, target);
  ASSERT_EQ(reply.send_now.size(), 1u);
  const auto ack = std::get<SnapshotReply>(reply.send_now.front().message);
  EXPECT_TRUE(ack.success);
  EXPECT_EQ(ack.index, 10u);
  EXPECT_EQ(ack.request_id, 99u);
  EXPECT_EQ(ack.term, 7u);
}

TEST(RaftSnapshotInstallTest, IncompleteChunkOnlyPersistsTermBeforeHostAcknowledgement) {
  auto [staged, effects] = step(config, recovered(), RecvSnapshot{{7, "b", 10, 6, 99, false}});
  ASSERT_TRUE(effects.persist);
  EXPECT_EQ(effects.persist->kind, PersistKind::StateUpdate);
  EXPECT_EQ(effects.persist->target.snapshot_index, 5u);
  EXPECT_EQ(effects.persist->target.commit, 6u);
  auto [observed, completion] = step(config, staged, PersistCompleted{effects.persist->token});
  EXPECT_EQ(observed.durable.term, 7u);
  EXPECT_TRUE(completion.send_now.empty());
  EXPECT_TRUE(completion.apply.empty());
}

TEST(RaftSnapshotInstallTest, FailureKeepsOldAcknowledgedStateAndHalts) {
  const auto original = recovered();
  auto [staged, effects] = step(config, original, RecvSnapshot{{7, "b", 10, 6, 99}});
  ASSERT_TRUE(effects.persist);
  auto [failed, error] = step(config, staged, PersistFailed{effects.persist->token});
  EXPECT_TRUE(error.halt);
  EXPECT_TRUE(failed.halted);
  EXPECT_EQ(failed.durable, original.durable);
  EXPECT_TRUE(error.send_now.empty());
}

TEST(RaftSnapshotInstallTest, SnapshotRetainsSuffixOnlyWhenItsAnchorTermAgrees) {
  for (bool matching : {false, true}) {
    auto [staged, effects] =
        step(config, recovered(), RecvSnapshot{{5, "b", 7, matching ? 4u : 3u, 1}});
    ASSERT_TRUE(effects.persist);
    EXPECT_EQ(effects.persist->target.log.size(), matching ? 1u : 0u);
    if (matching) {
      EXPECT_EQ(effects.persist->target.log.front().command, "entry-8");
    }
    EXPECT_EQ(effects.persist->target.applied, 7u);
    EXPECT_EQ(staged.durable.commit, 6u);
  }
}

TEST(RaftSnapshotInstallTest, OldSnapshotsNeverRegressCommittedApplicationOrInventHiddenTerms) {
  for (const auto index : {4u, 5u, 6u}) {
    const auto original = recovered();
    const std::uint64_t term = index == 6 ? 3 : 2;
    auto [unchanged, effects] = step(config, original, RecvSnapshot{{4, "b", index, term, 1}});
    EXPECT_EQ(unchanged.durable, original.durable);
    EXPECT_FALSE(effects.persist);
    ASSERT_EQ(effects.send_now.size(), 1u);
    EXPECT_EQ(std::get<SnapshotReply>(effects.send_now.front().message).success, index >= 5);
  }
  auto [staged, effects] = step(config, recovered(), RecvSnapshot{{5, "b", 6, 4, 1}});
  ASSERT_TRUE(effects.persist);
  EXPECT_EQ(effects.persist->kind, PersistKind::StateUpdate);
  EXPECT_EQ(effects.persist->target.commit, 6u);
  auto [written, reply] = step(config, staged, PersistCompleted{effects.persist->token});
  EXPECT_EQ(written.durable.applied, 6u);
  EXPECT_FALSE(std::get<SnapshotReply>(reply.send_now.front().message).success);
}

TEST(RaftSnapshotInstallTest, UnknownMembersAndTermsOlderThanCommittedTruthCannotInstall) {
  for (const auto& request : std::vector<SnapshotRequest>{
           {99, "unknown", 10, 6, 1}, {9, "b", 10, 2, 1}, {9, "b", 10, 10, 1}, {9, "b", 0, 0, 1}}) {
    const auto original = recovered();
    auto [unchanged, effects] = step(config, original, RecvSnapshot{request});
    EXPECT_EQ(unchanged.durable, original.durable);
    EXPECT_FALSE(effects.persist);
    EXPECT_TRUE(effects.send_now.empty());
  }
}

TEST(RaftSnapshotInstallTest, OnlyExactSnapshotProbeCanAdvancePeerMatchAcrossLocalMaintenance) {
  auto leader = recovered();
  leader.volatile_state.role = Role::Leader;
  leader.volatile_state.peers.at("b").next = 1;
  auto [probing, sent] = step(config, leader, HeartbeatTimeout{});
  ASSERT_EQ(sent.snapshot_needed.size(), 1u);
  const auto first = sent.snapshot_needed.front();
  auto [compacting, checkpoint] = step(config, probing, CompactCommitted{});
  ASSERT_TRUE(checkpoint.persist);
  auto [compacted, done] = step(config, compacting, PersistCompleted{checkpoint.persist->token});
  EXPECT_TRUE(done.send_now.empty());
  auto [retrying, retry] = step(config, compacted, HeartbeatTimeout{});
  ASSERT_EQ(retry.snapshot_needed.size(), 1u);
  EXPECT_EQ(retry.snapshot_needed.front().request_id, first.request_id);
  EXPECT_EQ(retry.snapshot_needed.front().index, 5u);
  auto [wrong, ignored] =
      step(config, retrying, RecvSnapshotReply{{4, "b", true, 6, first.request_id}});
  EXPECT_TRUE(ignored.send_now.empty());
  EXPECT_EQ(wrong.volatile_state.peers.at("b").match, 0u);
  auto [matched, ack] = step(config, wrong, RecvSnapshotReply{{4, "b", true, 5, first.request_id}});
  EXPECT_TRUE(ack.send_now.empty());
  EXPECT_EQ(matched.volatile_state.peers.at("b").match, 5u);
  auto [next, newer] = step(config, matched, HeartbeatTimeout{});
  ASSERT_EQ(newer.snapshot_needed.size(), 1u);
  EXPECT_EQ(newer.snapshot_needed.front().index, 6u);
  EXPECT_NE(newer.snapshot_needed.front().request_id, first.request_id);
  auto [stale, old_ack] =
      step(config, next, RecvSnapshotReply{{4, "b", true, 5, first.request_id}});
  EXPECT_TRUE(old_ack.send_now.empty());
  EXPECT_EQ(stale.volatile_state.peers.at("b").match, 5u);
}

TEST(RaftSnapshotInstallTest, SnapshotActionsRemainOutsideV1TraceEvenWithoutOffsets) {
  const auto empty = initial(config);
  EXPECT_THROW((void)trace_line(config, empty, RecvSnapshot{{4, "b", 2, 2, 1, false}}, 0),
               std::invalid_argument);
  EXPECT_THROW((void)trace_line(config, empty, RecvSnapshotReply{{4, "b", false, 0, 1}}, 0),
               std::invalid_argument);
}

}  // namespace
}  // namespace choreoos::raft
