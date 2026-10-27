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

  // A proposal cannot replace an unanswered empty heartbeat. Acknowledge it
  // first, including the schedule in which its original delivery was lost.
  for (const auto& out : leadership.effects.send_now) {
    if (out.to == "b") {
      const auto& probe = std::get<AppendEntries>(out.message);
      auto ack = a.value().dispatch(
          RecvAppendReply{AppendReply{probe.term, "b", true, probe.prev_index, probe.request_id}});
      EXPECT_FALSE(ack.error);
    }
  }
  auto sent = a.value().dispatch(HeartbeatTimeout{});
  AppendEntries message{};
  for (const auto& out : sent.effects.send_now) {
    if (out.to == "b") message = std::get<AppendEntries>(out.message);
  }
  ASSERT_EQ(message.entries.size(), 1u);
  auto follower_ack = b.value().dispatch(RecvAppend{message});
  ASSERT_EQ(follower_ack.effects.send_now.size(), 1u);
  EXPECT_TRUE(std::get<AppendReply>(follower_ack.effects.send_now.front().message).success);
  auto acknowledged = a.value().dispatch(
      RecvAppendReply{std::get<AppendReply>(follower_ack.effects.send_now.front().message)});
  EXPECT_FALSE(acknowledged.error);
  EXPECT_TRUE(acknowledged.effects.send_now.empty());
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

// A failed higher-term vote must recover from the actual filesystem boundary,
// not from the in-memory staged vote. BeforeReplace leaves the old self-vote;
// AfterReplace may durably contain the new vote without having sent a reply.
TEST(RaftDurableTest, InterruptedHigherTermVoteReopensAtActualDiskBoundary) {
  for (const auto fault : {CheckpointFault::BeforeReplace, CheckpointFault::AfterReplace}) {
    const bool replaced = fault == CheckpointFault::AfterReplace;
    const auto path = fresh(replaced ? "higher-term-after" : "higher-term-before");
    {
      auto opened = DurableNode::open(cfg("a"), path, {.test_mode = true});
      ASSERT_TRUE(opened);
      const auto election = opened.value().dispatch(ElectionTimeout{});
      ASSERT_FALSE(election.error);
      EXPECT_EQ(opened.value().disk().term, 1u);
      EXPECT_EQ(opened.value().disk().voted_for, std::optional<std::string>{"a"});

      ASSERT_TRUE(opened.value().inject_next_fault_for_test(fault));
      const auto failed = opened.value().dispatch(RecvRequestVote{RequestVote{2, "b", 0, 0, 41}});
      ASSERT_TRUE(failed.error);
      EXPECT_TRUE(failed.effects.halt);
      EXPECT_TRUE(failed.effects.send_now.empty());
      // Failed persistence must not publish the staged term/vote in memory.
      EXPECT_EQ(opened.value().node().durable.term, 1u);
      EXPECT_EQ(opened.value().node().durable.voted_for, std::optional<std::string>{"a"});
    }

    auto reopened = DurableNode::open(cfg("a"), path);
    ASSERT_TRUE(reopened) << reopened.error().to_string();
    const auto expected_term = replaced ? 2u : 1u;
    const auto expected_vote = std::optional<std::string>{replaced ? "b" : "a"};
    EXPECT_EQ(reopened.value().disk().term, expected_term);
    EXPECT_EQ(reopened.value().disk().voted_for, expected_vote);
    EXPECT_EQ(reopened.value().node().volatile_state.role, Role::Follower);

    // A competing candidate cannot obtain another vote in the recovered term.
    const auto competing = reopened.value().dispatch(
        RecvRequestVote{RequestVote{expected_term, replaced ? "c" : "b", 0, 0, 42}});
    EXPECT_FALSE(competing.error);
    ASSERT_EQ(competing.effects.send_now.size(), 1u);
    EXPECT_FALSE(std::get<VoteReply>(competing.effects.send_now.front().message).granted);
    EXPECT_EQ(reopened.value().disk().term, expected_term);
    EXPECT_EQ(reopened.value().disk().voted_for, expected_vote);
    std::filesystem::remove_all(path);
  }
}

// A higher-term election failure must not discard a previously committed log
// prefix. Both replacement boundaries are checked through a fresh disk reopen.
TEST(RaftDurableTest, InterruptedHigherTermVotePreservesCommittedPrefixOnDisk) {
  for (const auto fault : {CheckpointFault::BeforeReplace, CheckpointFault::AfterReplace}) {
    const bool replaced = fault == CheckpointFault::AfterReplace;
    const auto path = fresh(replaced ? "prefix-vote-after" : "prefix-vote-before");
    Durable stable;
    stable.term = 1;
    stable.voted_for = "a";
    stable.log = {{1, "committed-command"}};
    stable.commit = 1;
    {
      auto checkpoint = CheckpointStore::open(path);
      ASSERT_TRUE(checkpoint);
      ASSERT_TRUE(checkpoint.value().persist(stable));
    }
    {
      auto opened = DurableNode::open(cfg("a"), path, {.test_mode = true});
      ASSERT_TRUE(opened);
      EXPECT_EQ(opened.value().disk(), stable);
      ASSERT_TRUE(opened.value().inject_next_fault_for_test(fault));
      const auto failed = opened.value().dispatch(RecvRequestVote{RequestVote{2, "b", 1, 1, 91}});
      ASSERT_TRUE(failed.error);
      EXPECT_TRUE(failed.effects.halt);
      EXPECT_TRUE(failed.effects.send_now.empty());
      EXPECT_EQ(opened.value().node().durable, stable);
    }

    auto reopened = DurableNode::open(cfg("a"), path);
    ASSERT_TRUE(reopened) << reopened.error().to_string();
    const auto expected_term = replaced ? 2u : 1u;
    const auto expected_vote = std::optional<std::string>{replaced ? "b" : "a"};
    EXPECT_EQ(reopened.value().disk().term, expected_term);
    EXPECT_EQ(reopened.value().disk().voted_for, expected_vote);
    EXPECT_EQ(reopened.value().disk().log, stable.log);
    EXPECT_EQ(reopened.value().disk().commit, stable.commit);
    EXPECT_EQ(reopened.value().disk().applied, stable.applied);
    EXPECT_EQ(reopened.value().node().volatile_state.role, Role::Follower);

    // A new candidate with an equally fresh log cannot obtain a second vote.
    const auto competing = reopened.value().dispatch(
        RecvRequestVote{RequestVote{expected_term, replaced ? "c" : "b", 1, 1, 92}});
    EXPECT_FALSE(competing.error);
    ASSERT_EQ(competing.effects.send_now.size(), 1u);
    EXPECT_FALSE(std::get<VoteReply>(competing.effects.send_now.front().message).granted);
    EXPECT_EQ(reopened.value().disk().log, stable.log);
    EXPECT_EQ(reopened.value().disk().commit, stable.commit);
    std::filesystem::remove_all(path);
  }
}

// A stale candidate's higher term must be persisted without granting its
// vote. An interrupted replacement can leave either the old term/self-vote or
// the new term/no vote, but neither may lose a committed prefix or leak a reply.
TEST(RaftDurableTest, StaleHigherTermVoteRecoverySeparatesTermFromVote) {
  for (const auto fault : {CheckpointFault::BeforeReplace, CheckpointFault::AfterReplace}) {
    const bool replaced = fault == CheckpointFault::AfterReplace;
    const auto path = fresh(replaced ? "stale-vote-after" : "stale-vote-before");
    Durable stable;
    stable.term = 2;
    stable.voted_for = "a";
    stable.log = {{1, "committed"}, {2, "uncommitted"}};
    stable.commit = 1;
    {
      auto checkpoint = CheckpointStore::open(path);
      ASSERT_TRUE(checkpoint);
      ASSERT_TRUE(checkpoint.value().persist(stable));
    }
    {
      auto opened = DurableNode::open(cfg("a"), path, {.test_mode = true});
      ASSERT_TRUE(opened);
      ASSERT_TRUE(opened.value().inject_next_fault_for_test(fault));
      // Candidate b is missing the newer uncommitted term-2 suffix.
      const auto failed = opened.value().dispatch(RecvRequestVote{RequestVote{3, "b", 1, 1, 101}});
      ASSERT_TRUE(failed.error);
      EXPECT_TRUE(failed.effects.halt);
      EXPECT_TRUE(failed.effects.send_now.empty());
      EXPECT_EQ(opened.value().node().durable, stable);
    }

    auto reopened = DurableNode::open(cfg("a"), path);
    ASSERT_TRUE(reopened) << reopened.error().to_string();
    const auto expected_term = replaced ? 3u : 2u;
    const auto expected_vote =
        replaced ? std::optional<std::string>{} : std::optional<std::string>{"a"};
    EXPECT_EQ(reopened.value().disk().term, expected_term);
    EXPECT_EQ(reopened.value().disk().voted_for, expected_vote);
    EXPECT_EQ(reopened.value().disk().log, stable.log);
    EXPECT_EQ(reopened.value().disk().commit, stable.commit);
    EXPECT_EQ(reopened.value().disk().applied, stable.applied);

    // A candidate still missing the suffix cannot get a vote, even if the
    // higher term survived the interrupted replacement.
    const auto stale = reopened.value().dispatch(RecvRequestVote{RequestVote{3, "b", 1, 1, 102}});
    ASSERT_FALSE(stale.error);
    ASSERT_EQ(stale.effects.send_now.size(), 1u);
    const auto& denied = std::get<VoteReply>(stale.effects.send_now.front().message);
    EXPECT_FALSE(denied.granted);
    EXPECT_EQ(denied.request_id, 102u);
    EXPECT_EQ(reopened.value().disk().term, 3u);
    EXPECT_EQ(reopened.value().disk().voted_for, std::optional<std::string>{});

    // An up-to-date candidate may be granted term 3 only after its vote is
    // durably written, with the original request identity preserved.
    const auto fresh_vote =
        reopened.value().dispatch(RecvRequestVote{RequestVote{3, "c", 2, 2, 103}});
    ASSERT_FALSE(fresh_vote.error);
    ASSERT_EQ(fresh_vote.effects.send_now.size(), 1u);
    EXPECT_EQ(fresh_vote.effects.send_now.front().to, "c");
    const auto& granted = std::get<VoteReply>(fresh_vote.effects.send_now.front().message);
    EXPECT_TRUE(granted.granted);
    EXPECT_EQ(granted.term, 3u);
    EXPECT_EQ(granted.request_id, 103u);
    EXPECT_EQ(reopened.value().disk().term, 3u);
    EXPECT_EQ(reopened.value().disk().voted_for, std::optional<std::string>{"c"});

    auto final_reopen = DurableNode::open(cfg("a"), path);
    ASSERT_TRUE(final_reopen) << final_reopen.error().to_string();
    EXPECT_EQ(final_reopen.value().disk().term, 3u);
    EXPECT_EQ(final_reopen.value().disk().voted_for, std::optional<std::string>{"c"});
    EXPECT_EQ(final_reopen.value().disk().log, stable.log);
    EXPECT_EQ(final_reopen.value().disk().commit, stable.commit);
    const auto competing =
        final_reopen.value().dispatch(RecvRequestVote{RequestVote{3, "b", 2, 2, 104}});
    ASSERT_FALSE(competing.error);
    ASSERT_EQ(competing.effects.send_now.size(), 1u);
    EXPECT_FALSE(std::get<VoteReply>(competing.effects.send_now.front().message).granted);
    std::filesystem::remove_all(path);
  }
}

// After a stale higher-term request durably records the new term without a
// vote, a separate up-to-date request can fail while writing its grant. The
// replacement boundary determines whether that grant survived, but a failed
// dispatch must never send a positive reply.
TEST(RaftDurableTest, InterruptedGrantAfterDurableTermOnlyRecovery) {
  for (const auto fault : {CheckpointFault::BeforeReplace, CheckpointFault::AfterReplace}) {
    const bool replaced = fault == CheckpointFault::AfterReplace;
    const auto path = fresh(replaced ? "post-term-grant-after" : "post-term-grant-before");
    Durable initial;
    initial.term = 2;
    initial.voted_for = "a";
    initial.log = {{1, "committed"}, {2, "uncommitted"}};
    initial.commit = 1;
    {
      auto checkpoint = CheckpointStore::open(path);
      ASSERT_TRUE(checkpoint);
      ASSERT_TRUE(checkpoint.value().persist(initial));
    }
    {
      auto opened = DurableNode::open(cfg("a"), path, {.test_mode = true});
      ASSERT_TRUE(opened);
      const auto stale = opened.value().dispatch(RecvRequestVote{RequestVote{3, "b", 1, 1, 201}});
      ASSERT_FALSE(stale.error);
      ASSERT_EQ(stale.effects.send_now.size(), 1u);
      const auto& denied = std::get<VoteReply>(stale.effects.send_now.front().message);
      EXPECT_FALSE(denied.granted);
      EXPECT_EQ(denied.term, 3u);
      EXPECT_EQ(denied.request_id, 201u);
      EXPECT_EQ(opened.value().disk().term, 3u);
      EXPECT_EQ(opened.value().disk().voted_for, std::optional<std::string>{});
      EXPECT_EQ(opened.value().disk().log, initial.log);
      EXPECT_EQ(opened.value().disk().commit, initial.commit);

      ASSERT_TRUE(opened.value().inject_next_fault_for_test(fault));
      const auto failed = opened.value().dispatch(RecvRequestVote{RequestVote{3, "c", 2, 2, 202}});
      ASSERT_TRUE(failed.error);
      EXPECT_TRUE(failed.effects.halt);
      EXPECT_TRUE(failed.effects.send_now.empty());
      EXPECT_EQ(opened.value().node().durable.term, 3u);
      EXPECT_EQ(opened.value().node().durable.voted_for, std::optional<std::string>{});
    }

    auto reopened = DurableNode::open(cfg("a"), path);
    ASSERT_TRUE(reopened) << reopened.error().to_string();
    EXPECT_EQ(reopened.value().disk().term, 3u);
    EXPECT_EQ(reopened.value().disk().voted_for,
              replaced ? std::optional<std::string>{"c"} : std::optional<std::string>{});
    EXPECT_EQ(reopened.value().disk().log, initial.log);
    EXPECT_EQ(reopened.value().disk().commit, initial.commit);
    EXPECT_EQ(reopened.value().disk().applied, initial.applied);

    // The same-term candidate b can receive a vote only when the failed
    // replacement left the vote slot empty. An already-durable vote for c
    // must exclude another candidate even though c never received a reply.
    const auto competing =
        reopened.value().dispatch(RecvRequestVote{RequestVote{3, "b", 2, 2, 203}});
    ASSERT_FALSE(competing.error);
    ASSERT_EQ(competing.effects.send_now.size(), 1u);
    EXPECT_EQ(competing.effects.send_now.front().to, "b");
    const auto& reply = std::get<VoteReply>(competing.effects.send_now.front().message);
    EXPECT_EQ(reply.term, 3u);
    EXPECT_EQ(reply.request_id, 203u);
    EXPECT_EQ(reply.granted, !replaced);
    const auto final_vote = std::optional<std::string>{replaced ? "c" : "b"};
    EXPECT_EQ(reopened.value().disk().voted_for, final_vote);

    auto final_reopen = DurableNode::open(cfg("a"), path);
    ASSERT_TRUE(final_reopen) << final_reopen.error().to_string();
    EXPECT_EQ(final_reopen.value().disk().term, 3u);
    EXPECT_EQ(final_reopen.value().disk().voted_for, final_vote);
    EXPECT_EQ(final_reopen.value().disk().log, initial.log);
    EXPECT_EQ(final_reopen.value().disk().commit, initial.commit);
    std::filesystem::remove_all(path);
  }
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
  {
    auto changed = conflict.log[0];
    changed.command = "other";
    conflict.log.set(0, std::move(changed));
  }
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
  repaired.log.set(1, {3, "replacement"});
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

TEST(RaftDurableTest, DiskVoteCannotBeLoadedUnderUnrelatedMembership) {
  const auto path = fresh("membership");
  auto node = DurableNode::open(cfg("a"), path);
  ASSERT_TRUE(node);
  ASSERT_FALSE(node.value().dispatch(ElectionTimeout{}).error);
  auto incompatible = DurableNode::open(Config{"d", {"e", "f"}}, path);
  EXPECT_FALSE(incompatible);
  auto compatible = DurableNode::open(cfg("a"), path);
  ASSERT_TRUE(compatible);
  EXPECT_EQ(compatible.value().disk().voted_for, std::optional<std::string>{"a"});
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace choreoos::raft
