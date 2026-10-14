#include <gtest/gtest.h>

#include <string>
#include <variant>

#include "choreoos/raft/trace.hpp"

namespace choreoos::raft {
namespace {

TEST(RaftTraceTest, StagedAndDurableTermAreDistinguished) {
  const Config config{"a", {"b", "c"}};
  auto [staged, effects] = step(config, initial(config), ElectionTimeout{});
  ASSERT_TRUE(effects.persist);
  EXPECT_EQ(trace_action(ElectionTimeout{}), std::string("Timeout"));
  EXPECT_EQ(
      trace_line(config, staged, ElectionTimeout{}, 7),
      R"({"v":1,"seq":7,"node":"a","act":"Timeout","post":{"term":1,"role":"Candidate","votedFor":"a","commit":0,"log":[],"durable":{"term":0,"log_len":0,"commit":0,"applied":0},"alive":true}})");

  auto [written, released] = step(config, staged, PersistCompleted{effects.persist->token});
  ASSERT_EQ(released.send_now.size(), 2u);
  EXPECT_EQ(
      trace_line(config, written, PersistCompleted{effects.persist->token}, 8),
      R"({"v":1,"seq":8,"node":"a","act":"PersistCompleted","post":{"term":1,"role":"Candidate","votedFor":"a","commit":0,"log":[],"durable":{"term":1,"log_len":0,"commit":0,"applied":0},"alive":true}})");
}

TEST(RaftTraceTest, EscapesUntrustedLogStringsAsJson) {
  const Config config{"a", {"b", "c"}};
  auto node = initial(config);
  node.durable.term = 3;
  node.durable.log = {{3, "a\"b\nc\\d"}};
  const std::string serialized =
      trace_line(config, node, RecvAppendReply{AppendReply{3, "b", true, 1, 5}}, 1);
  EXPECT_NE(serialized.find(R"("val":"a\"b\nc\\d")"), std::string::npos);
  EXPECT_EQ(serialized.find('\n'), std::string::npos);
  EXPECT_NE(serialized.find(R"("act":"HandleAEResp")"), std::string::npos);
}

TEST(RaftTraceTest, FailStopProjectsOfflineUntilRestart) {
  const Config config{"a", {"b", "c"}};
  auto [staged, effects] = step(config, initial(config), ElectionTimeout{});
  ASSERT_TRUE(effects.persist);
  auto [halted, error] = step(config, staged, PersistFailed{effects.persist->token});
  ASSERT_TRUE(error.halt);
  const auto trace = trace_line(config, halted, PersistFailed{effects.persist->token}, 9);
  EXPECT_NE(trace.find(R"("alive":false)"), std::string::npos);
  EXPECT_EQ(trace.find(R"("term":1)"), std::string::npos);
  EXPECT_NE(trace.find(R"("act":"PersistFailed")"), std::string::npos);
}

}  // namespace
}  // namespace choreoos::raft
