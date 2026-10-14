#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <utility>

#include "choreoos/raft/transition.hpp"

namespace choreoos::raft {
namespace {

Config cluster(const std::string& self) {
  if (self == "a") return {self, {"b", "c"}};
  if (self == "b") return {self, {"a", "c"}};
  return {self, {"a", "b"}};
}

std::pair<Node, Effects> run(const Config& config, Node node, Input input) {
  return step(config, std::move(node), input);
}

TEST(RaftTransitionTest, ElectionDoesNotEmitVotesBeforeDurableSelfVote) {
  const auto c = cluster("a");
  auto [staged, proposal] = run(c, initial(c), ElectionTimeout{});
  ASSERT_TRUE(proposal.persist);
  EXPECT_TRUE(proposal.send_now.empty());
  EXPECT_TRUE(staged.pending.has_value());
  EXPECT_EQ(staged.durable.term, 0u);
  EXPECT_EQ(staged.volatile_state.role, Role::Candidate);

  auto [leader_candidate, completion] =
      run(c, staged, PersistCompleted{proposal.persist->token});
  EXPECT_FALSE(leader_candidate.pending);
  EXPECT_EQ(leader_candidate.durable.term, 1u);
  ASSERT_TRUE(leader_candidate.durable.voted_for);
  EXPECT_EQ(*leader_candidate.durable.voted_for, "a");
  ASSERT_EQ(completion.send_now.size(), 2u);
  EXPECT_TRUE(std::holds_alternative<RequestVote>(completion.send_now[0].message));
  EXPECT_EQ(leader_candidate.volatile_state.votes.count("a"), 1u);

  auto [leader, election_effects] = run(
      c, leader_candidate, RecvVoteReply{VoteReply{1, "b", true, 100}});
  EXPECT_EQ(leader.volatile_state.role, Role::Leader);
  EXPECT_EQ(leader.volatile_state.leadership_epoch, 1u);
  EXPECT_EQ(election_effects.send_now.size(), 2u);
}

TEST(RaftTransitionTest, VoteGrantAndHigherTermWaitForPersistCompletion) {
  const auto c = cluster("b");
  auto [staged, effects] = run(c, initial(c),
                               RecvRequestVote{RequestVote{2, "a", 0, 0, 41}});
  ASSERT_TRUE(effects.persist);
  EXPECT_TRUE(effects.send_now.empty());
  EXPECT_EQ(staged.durable.term, 0u);
  auto [voter, completed] = run(c, staged, PersistCompleted{effects.persist->token});
  ASSERT_EQ(completed.send_now.size(), 1u);
  const auto* reply = std::get_if<VoteReply>(&completed.send_now[0].message);
  ASSERT_NE(reply, nullptr);
  EXPECT_TRUE(reply->granted);
  EXPECT_EQ(reply->term, 2u);
  EXPECT_EQ(reply->request_id, 41u);
  EXPECT_EQ(voter.durable.voted_for, std::optional<std::string>{"a"});

  auto [same, duplicate] = run(c, voter,
      RecvRequestVote{RequestVote{2, "a", 0, 0, 42}});
  EXPECT_FALSE(duplicate.persist);
  ASSERT_EQ(duplicate.send_now.size(), 1u);
  EXPECT_EQ(std::get<VoteReply>(duplicate.send_now[0].message).granted, true);
  EXPECT_EQ(same.durable, voter.durable);
}

TEST(RaftTransitionTest, FailedDurableWriteIsFailStopAndNeverReleasesReply) {
  const auto c = cluster("b");
  auto [staged, effects] = run(c, initial(c),
                               RecvRequestVote{RequestVote{1, "a", 0, 0, 7}});
  ASSERT_TRUE(effects.persist);
  auto [halted, failed] = run(c, staged, PersistFailed{effects.persist->token});
  EXPECT_TRUE(halted.halted);
  EXPECT_TRUE(failed.halt);
  EXPECT_TRUE(failed.send_now.empty());
  EXPECT_EQ(halted.durable.term, 0u);
  auto [ignored, no_effects] = run(c, halted, ElectionTimeout{});
  EXPECT_FALSE(no_effects.persist);
  EXPECT_TRUE(no_effects.send_now.empty());
  EXPECT_EQ(ignored.durable.term, 0u);

  auto [restarted, recovery] = run(c, halted, Restart{});
  EXPECT_FALSE(restarted.halted);
  EXPECT_FALSE(restarted.pending);
  EXPECT_EQ(restarted.volatile_state.role, Role::Follower);
  EXPECT_TRUE(recovery.send_now.empty());
}

TEST(RaftTransitionTest, ReplicationAndApplicationEffectsFollowDurability) {
  const auto ca = cluster("a");
  const auto cb = cluster("b");
  auto [candidate, elect] = run(ca, initial(ca), ElectionTimeout{});
  ASSERT_TRUE(elect.persist);
  auto [ready, vote_messages] = run(ca, candidate, PersistCompleted{elect.persist->token});
  ASSERT_EQ(vote_messages.send_now.size(), 2u);
  auto [leader, initial_heartbeats] =
      run(ca, ready, RecvVoteReply{VoteReply{1, "b", true, 10}});
  ASSERT_EQ(leader.volatile_state.role, Role::Leader);
  ASSERT_EQ(initial_heartbeats.send_now.size(), 2u);

  // The initial heartbeats might be lost. Timer input replaces their probes.
  auto [new_leader, proposed] = run(ca, leader, ClientPropose{"cmd-1"});
  ASSERT_TRUE(proposed.persist);
  EXPECT_TRUE(proposed.send_now.empty());
  EXPECT_EQ(new_leader.durable.log.size(), 0u);
  auto [written, flush] = run(ca, new_leader, PersistCompleted{proposed.persist->token});
  EXPECT_EQ(written.durable.log.size(), 1u);
  // Existing heartbeats were in flight; retransmission includes the new entry.
  auto [sending, heartbeat] = run(ca, written, HeartbeatTimeout{});
  ASSERT_EQ(heartbeat.send_now.size(), 2u);
  const auto* append = std::get_if<AppendEntries>(&heartbeat.send_now[0].message);
  ASSERT_NE(append, nullptr);
  ASSERT_EQ(append->entries.size(), 1u);
  EXPECT_EQ(append->entries.front().command, "cmd-1");

  auto [follower_staged, append_effect] = run(cb, initial(cb), RecvAppend{*append});
  ASSERT_TRUE(append_effect.persist);
  EXPECT_TRUE(append_effect.send_now.empty());
  EXPECT_TRUE(follower_staged.durable.log.empty());
  auto [follower, ack_effect] =
      run(cb, follower_staged, PersistCompleted{append_effect.persist->token});
  ASSERT_EQ(follower.durable.log.size(), 1u);
  ASSERT_EQ(ack_effect.send_now.size(), 1u);
  const auto* ack = std::get_if<AppendReply>(&ack_effect.send_now[0].message);
  ASSERT_NE(ack, nullptr);
  EXPECT_TRUE(ack->success);
  EXPECT_EQ(ack->request_id, append->request_id);

  auto [acknowledged, ack_result] = run(ca, sending, RecvAppendReply{*ack});
  EXPECT_TRUE(ack_result.send_now.empty());
  EXPECT_EQ(acknowledged.volatile_state.peers.at("b").match, 1u);
  auto [committing, commit_effect] = run(ca, acknowledged, AdvanceCommit{});
  ASSERT_TRUE(commit_effect.persist);
  EXPECT_EQ(committing.durable.commit, 0u);
  EXPECT_TRUE(commit_effect.apply.empty());
  auto [committed, committed_effects] =
      run(ca, committing, PersistCompleted{commit_effect.persist->token});
  EXPECT_EQ(committed.durable.commit, 1u);
  EXPECT_TRUE(committed_effects.apply.empty());
  auto [applying, apply_effect] = run(ca, committed, ApplyCommitted{});
  ASSERT_TRUE(apply_effect.persist);
  EXPECT_TRUE(apply_effect.apply.empty());
  auto [applied, application] =
      run(ca, applying, PersistCompleted{apply_effect.persist->token});
  EXPECT_EQ(applied.durable.applied, 1u);
  ASSERT_EQ(application.apply.size(), 1u);
  EXPECT_EQ(application.apply.front().command, "cmd-1");
}

TEST(RaftTransitionTest, ShortHeartbeatPreservesMatchingFollowerSuffix) {
  const auto config = cluster("b");
  auto follower = initial(config);
  follower.durable.term = 2;
  follower.durable.log = {{1, "already-1"}, {2, "already-2"}};
  auto [after, effects] = run(config, follower,
      RecvAppend{AppendEntries{2, "a", 0, 0, {}, 0, 100}});
  EXPECT_FALSE(effects.persist);
  ASSERT_EQ(effects.send_now.size(), 1u);
  EXPECT_TRUE(std::get<AppendReply>(effects.send_now[0].message).success);
  EXPECT_EQ(after.durable.log.size(), 2u);
}

TEST(RaftTransitionTest, CannotTruncateCommittedPrefix) {
  const auto config = cluster("b");
  auto follower = initial(config);
  follower.durable.term = 2;
  follower.durable.log = {{1, "committed"}};
  follower.durable.commit = 1;
  auto [after, effects] = run(config, follower,
      RecvAppend{AppendEntries{2, "a", 0, 0, {{2, "conflict"}}, 0, 11}});
  EXPECT_FALSE(effects.persist);
  ASSERT_EQ(effects.send_now.size(), 1u);
  EXPECT_FALSE(std::get<AppendReply>(effects.send_now[0].message).success);
  EXPECT_EQ(after.durable.log, follower.durable.log);
}

TEST(RaftTransitionTest, OldProbeReplyCannotInflateMatchOrRegressNext) {
  const auto c = cluster("a");
  auto leader = initial(c);
  leader.durable.term = 4;
  leader.durable.voted_for = "a";
  leader.durable.log = {{4, "cmd"}};
  leader.volatile_state.role = Role::Leader;
  leader.volatile_state.peers.at("b").next = 1;
  auto [first, effects] = run(c, leader, HeartbeatTimeout{});
  const auto& probe1 = std::get<AppendEntries>(effects.send_now[0].message);
  auto [second, effects2] = run(c, first, HeartbeatTimeout{});
  const auto& probe2 = std::get<AppendEntries>(effects2.send_now[0].message);
  ASSERT_NE(probe1.request_id, probe2.request_id);
  auto [unchanged, stale] = run(c, second,
      RecvAppendReply{AppendReply{4, "b", true, 1000, probe1.request_id}});
  EXPECT_TRUE(stale.send_now.empty());
  EXPECT_EQ(unchanged.volatile_state.peers.at("b").match, 0u);
  auto [matched, now] = run(c, unchanged,
      RecvAppendReply{AppendReply{4, "b", true, 1000, probe2.request_id}});
  EXPECT_TRUE(now.send_now.empty());
  EXPECT_EQ(matched.volatile_state.peers.at("b").match, 1u);
  EXPECT_EQ(matched.volatile_state.peers.at("b").next, 2u);
}

TEST(RaftTransitionTest, OldTermEntriesAreNotDirectlyQuorumCommitted) {
  const auto c = cluster("a");
  auto leader = initial(c);
  leader.durable.term = 3;
  leader.durable.log = {{2, "old-term"}};
  leader.volatile_state.role = Role::Leader;
  leader.volatile_state.peers.at("b").match = 1;
  auto [after, effects] = run(c, leader, AdvanceCommit{});
  EXPECT_FALSE(effects.persist);
  EXPECT_EQ(after.durable.commit, 0u);
}

}  // namespace
}  // namespace choreoos::raft
