#include <gtest/gtest.h>

#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>

#include "choreoos/raft/trace.hpp"

namespace choreoos::raft {
namespace {

TEST(RaftTraceV2Test, CompactionProjectionMatchesGoldenAndV1StillRefusesIt) {
  const Config config{"b", {"a", "c"}};
  auto node = initial(config);
  node.durable.term = 3;
  node.durable.snapshot_index = 5;
  node.durable.snapshot_term = 2;
  node.durable.log = {{3, "committed"}};
  node.durable.commit = node.durable.applied = 6;
  auto [staged, effects] = step(config, node, CompactCommitted{});
  ASSERT_TRUE(effects.persist);
  EXPECT_THROW(static_cast<void>(trace_line(config, staged, CompactCommitted{}, 7)),
               std::invalid_argument);
  std::ifstream file{std::string(CHOREOOS_FIXTURE_DIR) + "/raft_trace_v2_compaction.ndjson"};
  ASSERT_TRUE(file);
  std::string expected{std::istreambuf_iterator<char>{file}, std::istreambuf_iterator<char>{}};
  ASSERT_FALSE(expected.empty());
  expected.pop_back();  // Fixture ends with one newline; trace_line emits none.
  EXPECT_EQ(trace_line_v2(config, staged, CompactCommitted{}, 7), expected);
}

TEST(RaftTraceV2Test, SnapshotInputKeepsReadinessBoundaryAndRequestContext) {
  const Config config{"b", {"a", "c"}};
  const auto node = initial(config);
  const auto line =
      trace_line_v2(config, node, RecvSnapshot{SnapshotRequest{4, "a", 20, 3, 91, false}}, 0);
  EXPECT_NE(line.find(R"("act":"InstallSnapshot")"), std::string::npos);
  EXPECT_NE(
      line.find(
          R"("in":{"term":4,"leader":"a","index":20,"snapshot_term":3,"request_id":91,"ready":false})"),
      std::string::npos);
  const auto reply =
      trace_line_v2(config, node, RecvSnapshotReply{SnapshotReply{4, "a", true, 20, 91}}, 1);
  EXPECT_NE(reply.find(R"("success":true,"index":20,"request_id":91)"), std::string::npos);
}

TEST(RaftTraceV2Test, DurableVoteAndLogAreVisibleDuringAnUnacknowledgedWrite) {
  const Config config{"b", {"a", "c"}};
  auto node = initial(config);
  node.durable.term = 2;
  node.durable.voted_for = "c";
  node.durable.log = {{1, "a\"b\nc\\d"}};
  auto [staged, effects] = step(config, node, RecvRequestVote{RequestVote{3, "a", 1, 1, 17}});
  ASSERT_TRUE(effects.persist);
  const auto line =
      trace_line_v2(config, staged, RecvRequestVote{RequestVote{3, "a", 1, 1, 17}}, 2);
  EXPECT_NE(
      line.find(R"("durable":{"term":2,"votedFor":"c","log":[{"term":1,"val":"a\"b\nc\\d"}])"),
      std::string::npos);
  EXPECT_EQ(line.find('\n'), std::string::npos);
  EXPECT_NE(line.find(R"("request_id":17)"), std::string::npos);
}

TEST(RaftTraceV2Test, MessageInputsPreserveReplicationAndVoteContext) {
  const Config config{"a", {"b", "c"}};
  const auto node = initial(config);
  const auto vote = trace_line_v2(config, node, RecvVoteReply{VoteReply{3, "b", true, 17}}, 0);
  EXPECT_NE(vote.find(R"("in":{"term":3,"voter":"b","granted":true,"request_id":17})"),
            std::string::npos);
  const auto append = trace_line_v2(
      config, node, RecvAppend{AppendEntries{3, "b", 5, 2, {{3, "entry"}}, 5, 18}}, 1);
  EXPECT_NE(
      append.find(
          R"("prev_index":5,"prev_term":2,"entries":[{"term":3,"val":"entry"}],"leader_commit":5,"request_id":18)"),
      std::string::npos);
  const auto ack =
      trace_line_v2(config, node, RecvAppendReply{AppendReply{3, "b", true, 100, 18, 6}}, 2);
  EXPECT_NE(ack.find(R"("success":true,"hint":100,"matched":6,"request_id":18)"),
            std::string::npos);
  EXPECT_NE(
      trace_line_v2(config, node, ClientPropose{"a\"b"}, 3).find(R"("in":{"command":"a\"b"})"),
      std::string::npos);
  EXPECT_NE(trace_line_v2(config, node, PersistFailed{19}, 4).find(R"("in":{"token":19})"),
            std::string::npos);
}

TEST(RaftTraceV2Test, ReadsRemainOutsideTheSnapshotContract) {
  const Config config{"a", {"b", "c"}};
  auto node = initial(config);
  EXPECT_THROW(static_cast<void>(trace_line_v2(config, node, BeginRead{1}, 0)),
               std::invalid_argument);
  EXPECT_THROW(static_cast<void>(trace_line_v2(config, node, CancelRead{1}, 0)),
               std::invalid_argument);
  node.volatile_state.read = ReadBarrier{1, 0, 0, {}, {}};
  EXPECT_THROW(static_cast<void>(trace_line_v2(config, node, Restart{}, 0)), std::invalid_argument);
}

}  // namespace
}  // namespace choreoos::raft
