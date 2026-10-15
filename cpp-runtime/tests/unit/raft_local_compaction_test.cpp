#include <gtest/gtest.h>

#include <stdexcept>

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
  node.durable.log = {{3, "entry-6"}, {4, "entry-7"}};
  node.durable.commit = node.durable.applied = 6;
  return node;
}

TEST(RaftLocalCompactionTest, AllRolesWaitForMatchingCompletionAndPreserveAbsoluteSuffix) {
  for (auto role : {Role::Follower, Role::Candidate, Role::Leader}) {
    auto original = recovered();
    original.volatile_state.role = role;
    auto [staged, request] = step(config, original, CompactCommitted{});
    ASSERT_TRUE(request.persist);
    EXPECT_EQ(request.persist->kind, PersistKind::LocalCompaction);
    EXPECT_EQ(staged.durable, original.durable);
    EXPECT_TRUE(staged.durable.log.shares_storage_with(original.durable.log));
    EXPECT_TRUE(request.send_now.empty());
    EXPECT_TRUE(request.apply.empty());
    EXPECT_TRUE(request.snapshot_needed.empty());
    const auto& target = request.persist->target;
    EXPECT_EQ(target.snapshot_index, 6u);
    EXPECT_EQ(target.snapshot_term, 3u);
    EXPECT_EQ(target.term, 4u);
    EXPECT_EQ(target.voted_for, "a");
    EXPECT_EQ(target.commit, 6u);
    EXPECT_EQ(target.applied, 6u);
    ASSERT_EQ(target.log.size(), 1u);
    EXPECT_EQ(target.log.front(), original.durable.log.back());
    auto [waiting, stale] = step(config, staged, PersistCompleted{request.persist->token + 1});
    EXPECT_TRUE(waiting.pending);
    EXPECT_EQ(waiting.durable, original.durable);
    EXPECT_TRUE(stale.send_now.empty());
    auto [compacted, completed] = step(config, waiting, PersistCompleted{request.persist->token});
    EXPECT_FALSE(compacted.pending);
    EXPECT_EQ(compacted.durable, target);
    EXPECT_EQ(compacted.volatile_state.role, role);
    EXPECT_TRUE(completed.send_now.empty());
    EXPECT_TRUE(completed.apply.empty());
    EXPECT_FALSE(completed.halt);
    EXPECT_EQ(original.durable.log.size(), 2u);
  }
}

TEST(RaftLocalCompactionTest, DefersUntilFullyAppliedAndDoesNotRewriteExistingBoundary) {
  for (int scenario = 0; scenario < 3; ++scenario) {
    auto node = recovered();
    if (scenario == 0) node.durable.applied = 5;
    if (scenario == 1) node.durable.commit = node.durable.applied = 5;
    if (scenario == 2) node = initial(config);
    auto [unchanged, effects] = step(config, node, CompactCommitted{});
    EXPECT_EQ(unchanged.durable, node.durable);
    EXPECT_EQ(unchanged.next_token, node.next_token);
    EXPECT_FALSE(unchanged.pending);
    EXPECT_FALSE(effects.persist);
    EXPECT_FALSE(effects.halt);
  }
}

TEST(RaftLocalCompactionTest, FailureHaltsWithoutPublishingCompactedState) {
  const auto original = recovered();
  auto [staged, request] = step(config, original, CompactCommitted{});
  ASSERT_TRUE(request.persist);
  auto [waiting, stale] = step(config, staged, PersistFailed{request.persist->token + 1});
  EXPECT_FALSE(waiting.halted);
  EXPECT_FALSE(stale.halt);
  ASSERT_TRUE(waiting.pending);
  auto [failed, effects] = step(config, waiting, PersistFailed{request.persist->token});
  EXPECT_TRUE(failed.halted);
  EXPECT_TRUE(effects.halt);
  EXPECT_FALSE(failed.pending);
  EXPECT_EQ(failed.durable, original.durable);
  EXPECT_TRUE(effects.send_now.empty());
  EXPECT_TRUE(effects.apply.empty());
  auto [still_failed, refused] = step(config, failed, CompactCommitted{});
  EXPECT_TRUE(still_failed.halted);
  EXPECT_FALSE(refused.persist);
}

TEST(RaftLocalCompactionTest, OtherInputsCannotActOnPendingCompaction) {
  auto [staged, request] = step(config, recovered(), CompactCommitted{});
  ASSERT_TRUE(request.persist);
  const std::vector<Input> inputs{HeartbeatTimeout{}, ClientPropose{"next"}, CompactCommitted{},
                                  RecvRequestVote{RequestVote{9, "b", 7, 4, 99}}};
  for (const auto& input : inputs) {
    auto [waiting, effects] = step(config, staged, input);
    ASSERT_TRUE(waiting.pending);
    EXPECT_EQ(waiting.pending->token, request.persist->token);
    EXPECT_EQ(waiting.durable, staged.durable);
    EXPECT_FALSE(effects.persist);
    EXPECT_TRUE(effects.send_now.empty());
    EXPECT_TRUE(effects.apply.empty());
  }
}

TEST(RaftLocalCompactionTest, LaggingPeerNeedsSnapshotWithoutInventedAcknowledgement) {
  auto leader = recovered();
  leader.volatile_state.role = Role::Leader;
  leader.volatile_state.peers.at("b").next = 5;
  leader.volatile_state.peers.at("b").match = 4;
  auto [staged, request] = step(config, leader, CompactCommitted{});
  ASSERT_TRUE(request.persist);
  auto [compacted, completed] = step(config, staged, PersistCompleted{request.persist->token});
  EXPECT_TRUE(completed.snapshot_needed.empty());
  auto [sending, effects] = step(config, compacted, HeartbeatTimeout{});
  EXPECT_TRUE(effects.send_now.empty());
  ASSERT_EQ(effects.snapshot_needed.size(), 1u);
  EXPECT_EQ(effects.snapshot_needed.front().peer, "b");
  EXPECT_EQ(effects.snapshot_needed.front().index, 6u);
  EXPECT_EQ(effects.snapshot_needed.front().term, 3u);
  EXPECT_EQ(sending.volatile_state.peers.at("b").match, 4u);
}

TEST(RaftLocalCompactionTest, OutstandingProbeKeepsExactContextAcrossMaintenance) {
  auto leader = recovered();
  leader.volatile_state.role = Role::Leader;
  leader.volatile_state.peers.at("b").next = 6;
  auto [probing, sent] = step(config, leader, HeartbeatTimeout{});
  ASSERT_EQ(sent.send_now.size(), 1u);
  const auto probe = std::get<AppendEntries>(sent.send_now.front().message);
  auto [staged, request] = step(config, probing, CompactCommitted{});
  ASSERT_TRUE(request.persist);
  auto [compacted, completed] = step(config, staged, PersistCompleted{request.persist->token});
  EXPECT_TRUE(completed.send_now.empty());
  auto [retrying, retry] = step(config, compacted, HeartbeatTimeout{});
  ASSERT_EQ(retry.send_now.size(), 1u);
  const auto repeated = std::get<AppendEntries>(retry.send_now.front().message);
  EXPECT_EQ(repeated.request_id, probe.request_id);
  EXPECT_EQ(repeated.prev_index, probe.prev_index);
  EXPECT_EQ(repeated.prev_term, probe.prev_term);
  EXPECT_EQ(repeated.entries, probe.entries);
  EXPECT_EQ(repeated.leader_commit, probe.leader_commit);
  auto [matched, ack] =
      step(config, retrying, RecvAppendReply{AppendReply{4, "b", true, 999, probe.request_id}});
  EXPECT_TRUE(ack.send_now.empty());
  EXPECT_EQ(matched.volatile_state.peers.at("b").match, 6u);
  auto [next, effects] = step(config, matched, HeartbeatTimeout{});
  ASSERT_EQ(effects.send_now.size(), 1u);
  const auto append = std::get<AppendEntries>(effects.send_now.front().message);
  EXPECT_EQ(append.prev_index, 6u);
  EXPECT_EQ(append.prev_term, 3u);
  ASSERT_EQ(append.entries.size(), 1u);
  EXPECT_EQ(append.entries.front().command, "entry-7");
  EXPECT_FALSE(next.halted);
}

TEST(RaftLocalCompactionTest, FullWindowAcceptsNewProposalOnlyAfterDurableCompaction) {
  auto leader = initial(config);
  leader.volatile_state.role = Role::Leader;
  leader.durable.term = 1;
  for (std::size_t i = 0; i < EntryLog::kMaximumEntries; ++i)
    leader.durable.log.push_back({1, "entry"});
  const auto commit = static_cast<std::uint64_t>(EntryLog::kMaximumEntries - 1);
  leader.durable.commit = leader.durable.applied = commit;
  auto [full, refused] = step(config, leader, ClientPropose{"next"});
  EXPECT_TRUE(refused.capacity_exhausted);
  auto [staged, request] = step(config, full, CompactCommitted{});
  ASSERT_TRUE(request.persist);
  auto [blocked, early] = step(config, staged, ClientPropose{"next"});
  EXPECT_FALSE(early.persist);
  EXPECT_EQ(blocked.durable.log.size(), EntryLog::kMaximumEntries);
  auto [compacted, completed] = step(config, blocked, PersistCompleted{request.persist->token});
  EXPECT_TRUE(completed.apply.empty());
  ASSERT_EQ(compacted.durable.log.size(), 1u);
  auto [proposing, accepted] = step(config, compacted, ClientPropose{"next"});
  ASSERT_TRUE(accepted.persist);
  EXPECT_EQ(accepted.persist->kind, PersistKind::StateUpdate);
  EXPECT_FALSE(accepted.capacity_exhausted);
  EXPECT_EQ(accepted.persist->target.snapshot_index, commit);
  ASSERT_EQ(accepted.persist->target.log.size(), 2u);
  EXPECT_EQ(accepted.persist->target.log.back().command, "next");
  EXPECT_FALSE(proposing.halted);
}

TEST(RaftLocalCompactionTest, SnapshotOnlyLogCanAppendApplyAndCompactAgain) {
  auto leader = recovered();
  leader.volatile_state.role = Role::Leader;
  leader.durable.commit = leader.durable.applied = 7;
  auto [staged, request] = step(config, leader, CompactCommitted{});
  ASSERT_TRUE(request.persist);
  EXPECT_EQ(request.persist->target.snapshot_index, 7u);
  EXPECT_EQ(request.persist->target.snapshot_term, 4u);
  EXPECT_TRUE(request.persist->target.log.empty());
  auto [compacted, completed] = step(config, staged, PersistCompleted{request.persist->token});
  EXPECT_TRUE(completed.apply.empty());
  auto [same, redundant] = step(config, compacted, CompactCommitted{});
  EXPECT_FALSE(redundant.persist);
  EXPECT_EQ(same.next_token, compacted.next_token);
  auto [proposing, append] = step(config, same, ClientPropose{"entry-8"});
  ASSERT_TRUE(append.persist);
  auto [written, sent] = step(config, proposing, PersistCompleted{append.persist->token});
  EXPECT_TRUE(sent.apply.empty());
  written.volatile_state.peers.at("b").match = 8;
  auto [committing, commit] = step(config, written, AdvanceCommit{});
  ASSERT_TRUE(commit.persist);
  auto [committed, no_apply] = step(config, committing, PersistCompleted{commit.persist->token});
  EXPECT_TRUE(no_apply.apply.empty());
  auto [applying, apply] = step(config, committed, ApplyCommitted{});
  ASSERT_TRUE(apply.persist);
  auto [applied, released] = step(config, applying, PersistCompleted{apply.persist->token});
  ASSERT_EQ(released.apply.size(), 1u);
  EXPECT_EQ(released.apply.front().command, "entry-8");
  auto [restaging, second] = step(config, applied, CompactCommitted{});
  ASSERT_TRUE(second.persist);
  EXPECT_EQ(second.persist->kind, PersistKind::LocalCompaction);
  EXPECT_EQ(second.persist->target.snapshot_index, 8u);
  EXPECT_EQ(second.persist->target.commit, 8u);
  EXPECT_EQ(second.persist->target.applied, 8u);
  EXPECT_TRUE(second.persist->target.log.empty());
  EXPECT_FALSE(restaging.halted);
}

TEST(RaftLocalCompactionTest, NewActionIsExplicitlyOutsideV1TraceModelEvenWhenNoOp) {
  EXPECT_STREQ(trace_action(CompactCommitted{}), "CompactCommitted");
  const auto empty = initial(config);
  EXPECT_THROW((void)trace_line(config, empty, CompactCommitted{}, 0), std::invalid_argument);
  auto [staged, request] = step(config, recovered(), CompactCommitted{});
  ASSERT_TRUE(request.persist);
  EXPECT_THROW((void)trace_line(config, staged, CompactCommitted{}, 1), std::invalid_argument);
}

}  // namespace
}  // namespace choreoos::raft
