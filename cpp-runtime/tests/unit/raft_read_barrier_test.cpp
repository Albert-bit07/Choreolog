#include <gtest/gtest.h>

#include "choreoos/raft/trace.hpp"
#include "choreoos/raft/transition.hpp"

namespace choreoos::raft {
namespace {

class RaftReadBarrierTest : public testing::Test {
 protected:
  Config config{"a", {"b", "c"}};
  Node node = initial(config);
  void SetUp() override {
    node.durable.term = 2;
    node.durable.log.push_back({1, "old"});
    node.durable.log.push_back({2, "barrier"});
    node.durable.commit = node.durable.applied = 2;
    node.volatile_state.role = Role::Leader;
    for (auto& [peer, progress] : node.volatile_state.peers) {
      (void)peer;
      progress.next = 3;
    }
  }
  Effects drive(Input input) {
    auto [next, effects] = step(config, std::move(node), input);
    node = std::move(next);
    return effects;
  }
  AppendReply reply(const std::string& peer = "b", bool success = true) const {
    const auto& probe = *node.volatile_state.peers.at(peer).in_flight;
    return {node.durable.term, peer, success, 0, probe.request_id, probe.prev + probe.count};
  }
};

TEST_F(RaftReadBarrierTest, FreshQuorumReturnsCapturedAppliedPrefixWithoutWriting) {
  const auto durable = node.durable;
  auto begin = drive(BeginRead{41});
  EXPECT_FALSE(begin.persist);
  EXPECT_FALSE(begin.read);
  ASSERT_EQ(begin.send_now.size(), 2u);
  auto ready = drive(RecvAppendReply{reply()});
  ASSERT_TRUE(ready.read);
  EXPECT_EQ(ready.read->status, ReadStatus::Ready);
  EXPECT_EQ(ready.read->context, 41u);
  EXPECT_EQ(ready.read->term, 2u);
  EXPECT_EQ(ready.read->index, 2u);
  EXPECT_FALSE(ready.persist);
  EXPECT_EQ(node.durable, durable);
  EXPECT_FALSE(node.volatile_state.read);
}

TEST_F(RaftReadBarrierTest, OldInflightRepliesCannotAuthorizeNewRead) {
  ASSERT_EQ(drive(HeartbeatTimeout{}).send_now.size(), 2u);
  const auto old = reply();
  EXPECT_TRUE(drive(BeginRead{42}).send_now.empty());
  EXPECT_FALSE(drive(RecvAppendReply{old}).read);
  ASSERT_TRUE(node.volatile_state.read);
  EXPECT_EQ(node.volatile_state.read->acknowledgements.size(), 1u);
  auto fresh = drive(HeartbeatTimeout{});
  ASSERT_EQ(fresh.send_now.size(), 2u);  // Fresh b, exact retransmission to c.
  EXPECT_FALSE(drive(RecvAppendReply{old}).read);
  auto ready = drive(RecvAppendReply{reply()});
  ASSERT_TRUE(ready.read);
  EXPECT_EQ(ready.read->status, ReadStatus::Ready);
}

TEST_F(RaftReadBarrierTest, IsolatedLeaderCannotCompleteAndCancellationBoundsState) {
  drive(BeginRead{43});
  for (int i = 0; i < 20; ++i) {
    auto retry = drive(HeartbeatTimeout{});
    EXPECT_FALSE(retry.read);
    EXPECT_EQ(retry.send_now.size(), 2u);
    EXPECT_EQ(node.volatile_state.read->probes.size(), 2u);
  }
  auto busy = drive(BeginRead{44});
  ASSERT_TRUE(busy.read);
  EXPECT_EQ(busy.read->status, ReadStatus::Busy);
  EXPECT_EQ(node.volatile_state.read->context, 43u);
  EXPECT_FALSE(drive(CancelRead{44}).read);
  auto cancel = drive(CancelRead{43});
  ASSERT_TRUE(cancel.read);
  EXPECT_EQ(cancel.read->status, ReadStatus::Cancelled);
  EXPECT_FALSE(node.volatile_state.read);
  EXPECT_FALSE(drive(RecvAppendReply{reply()}).read);
}

TEST_F(RaftReadBarrierTest, PriorRoundRepliesDoNotAuthorizeReusedClientContext) {
  drive(BeginRead{7});
  const auto prior_b = reply();
  const auto prior_c = reply("c");
  ASSERT_TRUE(drive(RecvAppendReply{prior_b}).read);
  drive(BeginRead{7});
  EXPECT_FALSE(drive(RecvAppendReply{prior_c}).read);
  EXPECT_FALSE(drive(RecvAppendReply{prior_b}).read);
  ASSERT_TRUE(drive(RecvAppendReply{reply()}).read);
}

TEST_F(RaftReadBarrierTest, FiveMembersRequireDistinctFreshSuccessfulExactReplies) {
  config.peers = {"b", "c", "d", "e"};
  node.volatile_state.peers.try_emplace("d");
  node.volatile_state.peers.try_emplace("e");
  drive(BeginRead{45});
  auto stale_term = reply();
  --stale_term.term;
  EXPECT_FALSE(drive(RecvAppendReply{stale_term}).read);
  auto wrong = reply();
  ++wrong.matched;
  EXPECT_FALSE(drive(RecvAppendReply{wrong}).read);
  EXPECT_EQ(node.volatile_state.read->acknowledgements.size(), 1u);
  drive(HeartbeatTimeout{});
  const auto valid = reply();
  EXPECT_FALSE(drive(RecvAppendReply{valid}).read);
  EXPECT_FALSE(drive(RecvAppendReply{valid}).read);
  EXPECT_FALSE(drive(RecvAppendReply{reply("c", false)}).read);
  auto stranger = valid;
  stranger.follower = "outsider";
  EXPECT_FALSE(drive(RecvAppendReply{stranger}).read);
  EXPECT_EQ(node.volatile_state.read->acknowledgements.size(), 2u);
  // A successful snapshot is replication progress, not a fresh append round.
  EXPECT_FALSE(drive(RecvSnapshotReply{{2, "d", true, 2, 999}}).read);
  drive(HeartbeatTimeout{});
  auto ready = drive(RecvAppendReply{reply("c")});
  ASSERT_TRUE(ready.read);
  EXPECT_EQ(ready.read->status, ReadStatus::Ready);
}

TEST_F(RaftReadBarrierTest, QuorumWaitsForMatchingApplicationPersistenceCompletion) {
  node.durable.applied = 1;
  drive(BeginRead{46});
  EXPECT_FALSE(drive(RecvAppendReply{reply()}).read);
  auto staged = drive(ApplyCommitted{});
  ASSERT_TRUE(staged.persist);
  EXPECT_FALSE(staged.read);
  EXPECT_FALSE(drive(PersistCompleted{staged.persist->token + 1}).read);
  auto ready = drive(PersistCompleted{staged.persist->token});
  ASSERT_TRUE(ready.read);
  EXPECT_EQ(ready.read->status, ReadStatus::Ready);
  EXPECT_EQ(node.durable.applied, ready.read->index);
}

TEST_F(RaftReadBarrierTest, FailedApplicationNeverReleasesReady) {
  node.durable.applied = 1;
  drive(BeginRead{47});
  drive(RecvAppendReply{reply()});
  auto staged = drive(ApplyCommitted{});
  ASSERT_TRUE(staged.persist);
  auto failed = drive(PersistFailed{staged.persist->token});
  ASSERT_TRUE(failed.read);
  EXPECT_EQ(failed.read->status, ReadStatus::Unavailable);
  EXPECT_TRUE(failed.halt);
  EXPECT_FALSE(node.volatile_state.read);
}

TEST_F(RaftReadBarrierTest, HigherTermReplyCancelsBeforeItsPersistenceCompletes) {
  drive(BeginRead{48});
  auto higher = reply();
  ++higher.term;
  auto down = drive(RecvAppendReply{higher});
  ASSERT_TRUE(down.persist);
  ASSERT_TRUE(down.read);
  EXPECT_EQ(down.read->status, ReadStatus::NotLeader);
  EXPECT_FALSE(node.volatile_state.read);
  EXPECT_FALSE(drive(PersistCompleted{down.persist->token}).read);
}

TEST_F(RaftReadBarrierTest, SameTermAppendAndRestartDiscardAuthority) {
  drive(BeginRead{49});
  auto follower = drive(RecvAppend{{2, "b", 2, 2, {}, 2, 88}});
  ASSERT_TRUE(follower.read);
  EXPECT_EQ(follower.read->status, ReadStatus::NotLeader);
  node.volatile_state.role = Role::Leader;
  drive(BeginRead{50});
  auto restarted = drive(Restart{});
  ASSERT_TRUE(restarted.read);
  EXPECT_EQ(restarted.read->status, ReadStatus::Cancelled);
  EXPECT_EQ(node.volatile_state.role, Role::Follower);
  EXPECT_FALSE(node.volatile_state.read);
}

TEST_F(RaftReadBarrierTest, SingleMemberRequiresCurrentTermCommitIncludingCompactedBoundary) {
  config.peers.clear();
  node = initial(config);
  auto follower = drive(BeginRead{51});
  ASSERT_TRUE(follower.read);
  EXPECT_EQ(follower.read->status, ReadStatus::NotLeader);
  node.durable.term = 2;
  node.volatile_state.role = Role::Leader;
  auto empty = drive(BeginRead{52});
  ASSERT_TRUE(empty.read);
  EXPECT_EQ(empty.read->status, ReadStatus::NotReady);
  node.durable.snapshot_index = node.durable.commit = node.durable.applied = 5;
  node.durable.snapshot_term = 1;
  auto old_term = drive(BeginRead{53});
  ASSERT_TRUE(old_term.read);
  EXPECT_EQ(old_term.read->status, ReadStatus::NotReady);
  node.durable.snapshot_term = 2;
  auto ready = drive(BeginRead{54});
  ASSERT_TRUE(ready.read);
  EXPECT_EQ(ready.read->status, ReadStatus::Ready);
  EXPECT_EQ(ready.read->index, 5u);
}

TEST_F(RaftReadBarrierTest, PendingWriteReturnsBusyAndCancellationStillWorks) {
  drive(BeginRead{55});
  auto staged = drive(ClientPropose{"new"});
  ASSERT_TRUE(staged.persist);
  auto busy = drive(BeginRead{56});
  ASSERT_TRUE(busy.read);
  EXPECT_EQ(busy.read->status, ReadStatus::Busy);
  auto cancel = drive(CancelRead{55});
  ASSERT_TRUE(cancel.read);
  EXPECT_EQ(cancel.read->status, ReadStatus::Cancelled);
  EXPECT_TRUE(node.pending);
  EXPECT_FALSE(drive(PersistCompleted{staged.persist->token}).read);
}

TEST_F(RaftReadBarrierTest, V1TraceRefusesReadActionsAndActiveReadState) {
  EXPECT_THROW((void)trace_line(config, node, BeginRead{57}, 1), std::invalid_argument);
  EXPECT_THROW((void)trace_line(config, node, CancelRead{57}, 1), std::invalid_argument);
  drive(BeginRead{57});
  EXPECT_THROW((void)trace_line(config, node, HeartbeatTimeout{}, 1), std::invalid_argument);
  EXPECT_STREQ(trace_action(BeginRead{57}), "BeginRead");
  EXPECT_STREQ(trace_action(CancelRead{57}), "CancelRead");
}

}  // namespace
}  // namespace choreoos::raft
