#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <variant>

#include "choreoos/raft/checkpoint.hpp"
#include "choreoos/raft/durable_node.hpp"

namespace choreoos::raft {
namespace {

Config cfg(const std::string& self) {
  if (self == "a") return {"a", {"b", "c"}};
  return {self, {"a", "c"}};
}

std::filesystem::path fresh(const std::string& name) {
  auto dir = std::filesystem::temp_directory_path() / ("choreoos-raft-durable-" + name);
  std::filesystem::remove_all(dir);
  return dir;
}

TEST(RaftDurableTest, SelfVoteAndGrantedVoteSurviveRealDiskReopen) {
  const auto path = fresh("votes");
  auto opened = DurableNode::open(cfg("a"), path);
  ASSERT_TRUE(opened) << opened.error().to_string();
  auto election = opened.value().dispatch(ElectionTimeout{});
  EXPECT_FALSE(election.error);
  ASSERT_EQ(election.effects.send_now.size(), 2u);
  EXPECT_TRUE(std::holds_alternative<RequestVote>(election.effects.send_now[0].message));
  EXPECT_EQ(opened.value().disk().term, 1u);
  EXPECT_EQ(opened.value().disk().voted_for, std::optional<std::string>{"a"});

  auto reopened = DurableNode::open(cfg("a"), path);
  ASSERT_TRUE(reopened) << reopened.error().to_string();
  EXPECT_EQ(reopened.value().node().durable.term, 1u);
  EXPECT_EQ(reopened.value().node().durable.voted_for, std::optional<std::string>{"a"});
  EXPECT_EQ(reopened.value().node().volatile_state.role, Role::Follower);
  auto blocked = reopened.value().dispatch(RecvRequestVote{RequestVote{1, "b", 0, 0, 9}});
  EXPECT_FALSE(blocked.error);
  ASSERT_EQ(blocked.effects.send_now.size(), 1u);
  EXPECT_FALSE(std::get<VoteReply>(blocked.effects.send_now.front().message).granted);
  std::filesystem::remove_all(path);
}

TEST(RaftDurableTest, ReplicationAndQuorumCommitReachDiskBeforeReopen) {
  const auto a_dir = fresh("cluster-a");
  const auto b_dir = fresh("cluster-b");
  auto a = DurableNode::open(cfg("a"), a_dir);
  auto b = DurableNode::open(cfg("b"), b_dir);
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  auto election = a.value().dispatch(ElectionTimeout{});
  ASSERT_EQ(election.effects.send_now.size(), 2u);
  RequestVote request{};
  for (const auto& out : election.effects.send_now) {
    if (out.to == "b") request = std::get<RequestVote>(out.message);
  }
  auto grant = b.value().dispatch(RecvRequestVote{request});
  ASSERT_EQ(grant.effects.send_now.size(), 1u);
  ASSERT_TRUE(std::get<VoteReply>(grant.effects.send_now.front().message).granted);
  auto leadership = a.value().dispatch(
      RecvVoteReply{std::get<VoteReply>(grant.effects.send_now.front().message)});
  EXPECT_EQ(a.value().node().volatile_state.role, Role::Leader);
  EXPECT_EQ(leadership.effects.send_now.size(), 2u);
  auto append = a.value().dispatch(ClientPropose{"command-one"});
  EXPECT_TRUE(append.effects.send_now.empty());
  EXPECT_EQ(a.value().disk().log.size(), 1u);

  auto sent = a.value().dispatch(HeartbeatTimeout{});
  AppendEntries message{};
  for (const auto& out : sent.effects.send_now) {
    if (out.to == "b") message = std::get<AppendEntries>(out.message);
  }
  ASSERT_EQ(message.entries.size(), 1u);
  auto follower_ack = b.value().dispatch(RecvAppend{message});
  ASSERT_EQ(follower_ack.effects.send_now.size(), 1u);
  EXPECT_TRUE(std::get<AppendReply>(follower_ack.effects.send_now.front().message).success);
  a.value().dispatch(
      RecvAppendReply{std::get<AppendReply>(follower_ack.effects.send_now.front().message)});
  auto committed = a.value().dispatch(AdvanceCommit{});
  EXPECT_FALSE(committed.error);
  EXPECT_EQ(a.value().disk().commit, 1u);

  auto restored_a = DurableNode::open(cfg("a"), a_dir);
  auto restored_b = DurableNode::open(cfg("b"), b_dir);
  ASSERT_TRUE(restored_a);
  ASSERT_TRUE(restored_b);
  EXPECT_EQ(restored_a.value().disk().term, 1u);
  EXPECT_EQ(restored_a.value().disk().commit, 1u);
  ASSERT_EQ(restored_a.value().disk().log.size(), 1u);
  EXPECT_EQ(restored_a.value().disk().log[0].command, "command-one");
  EXPECT_EQ(restored_b.value().disk().log, restored_a.value().disk().log);
  EXPECT_EQ(restored_a.value().disk().applied, 0u);
  std::filesystem::remove_all(a_dir);
  std::filesystem::remove_all(b_dir);
}

TEST(RaftDurableTest, InjectedFailureBeforeReplaceNeverReleasesVote) {
  const auto path = fresh("before");
  auto opened = DurableNode::open(cfg("b"), path, {.test_mode = true});
  ASSERT_TRUE(opened);
  ASSERT_TRUE(opened.value().inject_next_fault_for_test(CheckpointFault::BeforeReplace));
  const auto first = opened.value().dispatch(RecvRequestVote{RequestVote{3, "a", 0, 0, 31}});
  EXPECT_TRUE(first.error);
  EXPECT_TRUE(first.effects.halt);
  EXPECT_TRUE(first.effects.send_now.empty());
  EXPECT_EQ(opened.value().disk().term, 0u);
  auto ignored = opened.value().dispatch(ElectionTimeout{});
  EXPECT_TRUE(ignored.effects.send_now.empty());
  auto restarted = opened.value().dispatch(Restart{});
  EXPECT_FALSE(restarted.error);
  EXPECT_EQ(opened.value().disk().term, 0u);
  auto reopened = DurableNode::open(cfg("b"), path);
  ASSERT_TRUE(reopened);
  EXPECT_EQ(reopened.value().disk().term, 0u);
  std::filesystem::remove_all(path);
}

TEST(RaftDurableTest, FailureAfterReplaceIsAmbiguousButRecoveryUsesDisk) {
  const auto path = fresh("after");
  auto opened = DurableNode::open(cfg("b"), path, {.test_mode = true});
  ASSERT_TRUE(opened);
  ASSERT_TRUE(opened.value().inject_next_fault_for_test(CheckpointFault::AfterReplace));
  auto failed = opened.value().dispatch(RecvRequestVote{RequestVote{3, "a", 0, 0, 35}});
  ASSERT_TRUE(failed.error);
  EXPECT_TRUE(failed.effects.halt);
  EXPECT_TRUE(failed.effects.send_now.empty());
  EXPECT_EQ(opened.value().node().durable.term, 0u);

  // A flushed/renamed file may contain the proposed term/vote even though
  // PersistFailed prevented any externally visible positive reply.
  auto reopened = DurableNode::open(cfg("b"), path);
  ASSERT_TRUE(reopened);
  EXPECT_EQ(reopened.value().disk().term, 3u);
  EXPECT_EQ(reopened.value().disk().voted_for, std::optional<std::string>{"a"});
  auto recovered = opened.value().dispatch(Restart{});
  EXPECT_FALSE(recovered.error);
  EXPECT_EQ(opened.value().node().durable.term, 3u);
  auto vote = opened.value().dispatch(RecvRequestVote{RequestVote{3, "c", 0, 0, 36}});
  ASSERT_EQ(vote.effects.send_now.size(), 1u);
  EXPECT_FALSE(std::get<VoteReply>(vote.effects.send_now.front().message).granted);
  std::filesystem::remove_all(path);
}

TEST(RaftDurableTest, BadCheckpointChecksumCannotSilentlyResetConsensus) {
  const auto path = fresh("corrupt");
  {
    auto opened = DurableNode::open(cfg("a"), path);
    ASSERT_TRUE(opened);
    ASSERT_FALSE(opened.value().dispatch(ElectionTimeout{}).error);
  }
  {
    std::fstream file{path / "raft-core-v1.bin", std::ios::in | std::ios::out | std::ios::binary};
    ASSERT_TRUE(file.is_open());
    file.seekp(23);
    file.put('X');
  }
  auto recovered = DurableNode::open(cfg("a"), path);
  EXPECT_FALSE(recovered);
  std::filesystem::remove_all(path);
}

TEST(RaftDurableTest, CommittedPrefixAndSameTermVoteCannotBeOverwritten) {
  const auto path = fresh("prefix");
  auto checkpoint = CheckpointStore::open(path);
  ASSERT_TRUE(checkpoint);
  Durable stable;
  stable.term = 2;
  stable.voted_for = "a";
  stable.log = {{1, "one"}, {2, "two"}};
  stable.commit = 1;
  ASSERT_TRUE(checkpoint.value().persist(stable));

  Durable conflict = stable;
  conflict.log[0].command = "other";
  EXPECT_FALSE(checkpoint.value().persist(conflict));
  conflict = stable;
  conflict.log.resize(0);
  EXPECT_FALSE(checkpoint.value().persist(conflict));
  conflict = stable;
  conflict.voted_for = "b";
  EXPECT_FALSE(checkpoint.value().persist(conflict));
  conflict = stable;
  conflict.term = 1;
  EXPECT_FALSE(checkpoint.value().persist(conflict));

  // Uncommitted suffix repair is permitted if committed prefix survives.
  Durable repaired = stable;
  repaired.term = 3;
  repaired.voted_for.reset();
  repaired.log[1] = {3, "replacement"};
  ASSERT_TRUE(checkpoint.value().persist(repaired));
  auto reopened = CheckpointStore::open(path);
  ASSERT_TRUE(reopened);
  EXPECT_EQ(reopened.value().current(), repaired);
  std::filesystem::remove_all(path);
}

TEST(RaftDurableTest, DurableApplicationRequiresStateMachineCheckpoint) {
  const auto path = fresh("apply");
  auto opened = DurableNode::open(cfg("a"), path);
  ASSERT_TRUE(opened);
  auto result = opened.value().dispatch(ApplyCommitted{});
  EXPECT_TRUE(result.error);
  EXPECT_TRUE(result.effects.send_now.empty());
  EXPECT_EQ(opened.value().disk().applied, 0u);
  auto checkpoint = CheckpointStore::open(path);
  ASSERT_TRUE(checkpoint);
  Durable invalid;
  invalid.term = 1;
  invalid.log = {{1, "command"}};
  invalid.commit = 1;
  invalid.applied = 1;
  EXPECT_FALSE(checkpoint.value().persist(invalid));
  std::filesystem::remove_all(path);
}

TEST(RaftDurableTest, CannotForgePersistCompletedThroughDriver) {
  const auto path = fresh("forged");
  auto opened = DurableNode::open(cfg("a"), path);
  ASSERT_TRUE(opened);
  auto result = opened.value().dispatch(PersistCompleted{1});
  EXPECT_TRUE(result.error);
  EXPECT_FALSE(result.effects.persist);
  EXPECT_TRUE(result.effects.send_now.empty());
  EXPECT_EQ(opened.value().disk().term, 0u);
  std::filesystem::remove_all(path);
}

TEST(RaftDurableTest, FaultsNeedExplicitTestMode) {
  const auto path = fresh("test-mode");
  auto opened = DurableNode::open(cfg("a"), path);
  ASSERT_TRUE(opened);
  EXPECT_FALSE(opened.value().inject_next_fault_for_test(CheckpointFault::AfterReplace));
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace choreoos::raft
