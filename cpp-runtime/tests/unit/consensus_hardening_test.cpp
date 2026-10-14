#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "choreoos/protocol/codec.hpp"
#include "choreoos/replication/replica.hpp"
#include "choreoos/state/store.hpp"

namespace choreoos::replication {
namespace {

std::filesystem::path safety_directory(const char* suffix) {
  auto path = std::filesystem::temp_directory_path() /
              (std::string("choreoos-raft-safety-") + suffix);
  std::filesystem::remove_all(path);
  return path;
}

ReplicaConfig safety_config(const std::filesystem::path& path) {
  ReplicaConfig config{"node-a", "", {"node-b", "node-c"}, path};
  config.elections = true;
  config.election_timeout_min = 1;
  config.election_timeout_max = 1;
  return config;
}

template <class Message>
choreoos::protocol::Frame wire(choreoos::protocol::MessageType type,
                                const Message& message) {
  choreoos::protocol::Frame frame;
  frame.type = type;
  frame.correlation = 7;
  frame.payload = choreoos::protocol::encode(message).value();
  return frame;
}

TEST(ConsensusHardeningTest, MetadataReadErrorCannotBecomeInitialTermAndVote) {
  const auto path = safety_directory("metadata");
  auto store = choreoos::state::FileEngine::open(path);
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().consensus_metadata());
  {
    std::ofstream out(path / "meta.bin", std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out << "invalid-metadata";
  }
  EXPECT_FALSE(store.value().consensus_metadata());
  auto resumed = Replica::open(safety_config(path));
  EXPECT_FALSE(resumed);
  std::filesystem::remove_all(path);
}

TEST(ConsensusHardeningTest, HigherTermWriteFailureHaltsAndPreventsAcknowledgements) {
  const auto path = safety_directory("term-write");
  auto replica = Replica::open(safety_config(path));
  ASSERT_TRUE(replica);
  std::size_t sends = 0;
  replica.value().set_sender([&](const std::string&, const choreoos::protocol::Frame&) {
    ++sends;
  });
  // Force the atomic metadata write to fail without changing the old term.
  ASSERT_TRUE(std::filesystem::create_directory(path / "meta.bin.tmp"));
  const auto request = wire(choreoos::protocol::MessageType::RequestVote,
                            choreoos::protocol::RequestVote{7, "node-b", 0, 0});
  replica.value().handle(request, "node-b");
  EXPECT_TRUE(replica.value().halted());
  EXPECT_FALSE(replica.value().is_leader());
  EXPECT_EQ(replica.value().status().role, "halted");
  EXPECT_EQ(sends, 0u);
  replica.value().tick();
  replica.value().tick();
  EXPECT_EQ(sends, 0u);
  EXPECT_EQ(replica.value().status().term, 1u);
  // A halted replica must be reconstructed from disk, not resumed by a tick.
  replica.value().handle(request, "node-b");
  EXPECT_EQ(sends, 0u);
  std::filesystem::remove_all(path);
}

TEST(ConsensusHardeningTest, ForgedSessionIdentityCannotChangeTermOrCastVote) {
  const auto path = safety_directory("identity");
  auto replica = Replica::open(safety_config(path));
  ASSERT_TRUE(replica);
  const auto request = wire(choreoos::protocol::MessageType::RequestVote,
                            choreoos::protocol::RequestVote{999, "node-c", 0, 0});
  replica.value().handle(request, "node-b");
  EXPECT_EQ(replica.value().status().term, 1u);
  EXPECT_FALSE(replica.value().halted());

  // A member ID not in the fixed configuration never supplies quorum.
  replica.value().tick();
  EXPECT_EQ(replica.value().status().role, "candidate");
  const auto term = replica.value().status().term;
  const auto fake_vote = wire(choreoos::protocol::MessageType::RequestVoteResponse,
                               choreoos::protocol::RequestVoteResponse{term, "stranger", true});
  replica.value().handle(fake_vote);
  EXPECT_EQ(replica.value().status().term, term);
  EXPECT_EQ(replica.value().status().role, "candidate");
  EXPECT_FALSE(replica.value().is_leader());

  // Known members with authenticated matching identities still work.
  const auto real_vote = wire(choreoos::protocol::MessageType::RequestVoteResponse,
                               choreoos::protocol::RequestVoteResponse{term, "node-b", true});
  replica.value().handle(real_vote, "node-b");
  EXPECT_TRUE(replica.value().is_leader());
  std::filesystem::remove_all(path);
}

TEST(ConsensusHardeningTest, ForgedAppendLeaderCannotReplaceDurableTerm) {
  const auto path = safety_directory("append-identity");
  auto replica = Replica::open(safety_config(path));
  ASSERT_TRUE(replica);
  const auto forged = wire(choreoos::protocol::MessageType::AppendEntries,
                           choreoos::protocol::AppendEntries{1000, "node-c", 0, 0, {}, 0});
  replica.value().handle(forged, "node-b");
  EXPECT_EQ(replica.value().status().term, 1u);
  EXPECT_FALSE(replica.value().halted());
  const auto genuine = wire(choreoos::protocol::MessageType::AppendEntries,
                            choreoos::protocol::AppendEntries{2, "node-b", 0, 0, {}, 0});
  replica.value().handle(genuine, "node-b");
  EXPECT_EQ(replica.value().status().term, 2u);
  EXPECT_FALSE(replica.value().halted());
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace choreoos::replication
