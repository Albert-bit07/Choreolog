#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <thread>

#include "choreoos/network/node_server.hpp"
#include "choreoos/protocol/client.hpp"
#include "choreoos/protocol/codec.hpp"
#include "choreoos/protocol/command_codec.hpp"
#include "choreoos/runtime/config.hpp"

namespace choreoos::network {
namespace {

using choreoos::state::ChoreographyId;
using choreoos::state::Command;
using choreoos::state::CommandId;
using choreoos::state::CommandType;
using choreoos::state::CreateChoreographyPayload;
using choreoos::state::DancerId;
using choreoos::state::DancerPayload;
using choreoos::state::kCurrentSchemaVersion;
using choreoos::state::MusicalTick;
using choreoos::state::OverlapPolicy;
using choreoos::state::Position;
using choreoos::state::StageBounds;

std::filesystem::path fresh(const char* name) {
  auto dir = std::filesystem::temp_directory_path() / name;
  std::filesystem::remove_all(dir);
  return dir;
}

choreoos::runtime::NodeConfig node_config(const char* id, const char* leader,
                                          std::filesystem::path data, std::uint16_t port,
                                          std::vector<choreoos::runtime::PeerEndpoint> peers) {
  choreoos::runtime::NodeConfig config;
  config.id = id;
  config.leader_id = leader;
  config.data = std::move(data);
  config.host = "127.0.0.1";
  config.port = port;
  config.peers = std::move(peers);
  return config;
}

choreoos::protocol::Frame command_frame(const Command& command) {
  choreoos::protocol::ClientCommand body;
  body.canonical = choreoos::protocol::canonical_command(command);
  choreoos::protocol::Frame frame;
  frame.type = choreoos::protocol::MessageType::ClientCommand;
  frame.correlation = 1;
  frame.payload = choreoos::protocol::encode(body).value();
  return frame;
}

choreoos::state::Result<choreoos::protocol::StatusResponse> status_of(std::uint16_t port) {
  choreoos::protocol::Frame frame;
  frame.type = choreoos::protocol::MessageType::StatusQuery;
  frame.payload = choreoos::protocol::encode_status_query().value();
  auto reply =
      choreoos::protocol::transact("127.0.0.1", port, frame, std::chrono::milliseconds(1000));
  if (!reply) {
    return reply.error();
  }
  return choreoos::protocol::decode_status_response(reply.value().payload);
}

TEST(ClusterTest, ThreeTcpNodesCommitAndCatchUp) {
  const auto root = fresh("choreoos-tcp-cluster");
  const auto leader_dir = root / "node-1";
  const auto a_dir = root / "node-2";
  const auto b_dir = root / "node-3";
  auto leader = NodeServer::open(
      node_config("node-1", "node-1", leader_dir, 19121,
                  {{"node-2", "127.0.0.1", 19122}, {"node-3", "127.0.0.1", 19123}}));
  auto follower_a = NodeServer::open(
      node_config("node-2", "node-1", a_dir, 19122,
                  {{"node-1", "127.0.0.1", 19121}, {"node-3", "127.0.0.1", 19123}}));
  auto follower_b = NodeServer::open(
      node_config("node-3", "node-1", b_dir, 19123,
                  {{"node-1", "127.0.0.1", 19121}, {"node-2", "127.0.0.1", 19122}}));
  ASSERT_TRUE(leader);
  ASSERT_TRUE(follower_a);
  ASSERT_TRUE(follower_b);
  std::thread leader_thread([&] { leader.value()->run(); });
  std::thread follower_a_thread([&] { follower_a.value()->run(); });
  std::thread follower_b_thread([&] { follower_b.value()->run(); });

  Command create{CommandId::parse("c-create").value(),
                 ChoreographyId::parse("opening").value(),
                 CommandType::CreateChoreography,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(0).value(),
                 CreateChoreographyPayload{ChoreographyId::parse("opening").value(),
                                           StageBounds::from_mm(20000, 12000).value(),
                                           OverlapPolicy::Forbidden, 5000}};
  bool committed = false;
  for (int attempt = 0; attempt < 20 && !committed; ++attempt) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    auto reply = choreoos::protocol::transact("127.0.0.1", 19121, command_frame(create),
                                              std::chrono::milliseconds(2500));
    if (!reply) {
      continue;
    }
    auto decoded = choreoos::protocol::decode_client_response(reply.value().payload);
    committed = decoded && decoded.value().ok && decoded.value().index == 1;
  }
  EXPECT_TRUE(committed);

  std::string hash;
  for (int attempt = 0; attempt < 30 && hash.empty(); ++attempt) {
    auto first = status_of(19121);
    auto second = status_of(19122);
    auto third = status_of(19123);
    if (first && second && third && first.value().commit_index == 1 &&
        second.value().state_hash == first.value().state_hash &&
        third.value().state_hash == first.value().state_hash) {
      hash = first.value().state_hash;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  EXPECT_FALSE(hash.empty());

  follower_b.value()->stop();
  follower_b_thread.join();
  follower_b.value().reset();

  Command add{
      CommandId::parse("c-alice").value(),
      ChoreographyId::parse("opening").value(),
      CommandType::AddDancer,
      kCurrentSchemaVersion,
      MusicalTick::from_count(1).value(),
      DancerPayload{DancerId::parse("alice").value(), Position::from_mm(1000, 1000).value()}};
  auto added = choreoos::protocol::transact("127.0.0.1", 19121, command_frame(add),
                                            std::chrono::milliseconds(2500));
  ASSERT_TRUE(added);
  auto added_body = choreoos::protocol::decode_client_response(added.value().payload);
  ASSERT_TRUE(added_body);
  EXPECT_TRUE(added_body.value().ok);

  follower_b = NodeServer::open(
      node_config("node-3", "node-1", b_dir, 19123,
                  {{"node-1", "127.0.0.1", 19121}, {"node-2", "127.0.0.1", 19122}}));
  ASSERT_TRUE(follower_b);
  follower_b_thread = std::thread([&] { follower_b.value()->run(); });

  bool caught_up = false;
  for (int attempt = 0; attempt < 40 && !caught_up; ++attempt) {
    auto first = status_of(19121);
    auto third = status_of(19123);
    caught_up = first && third && third.value().commit_index == first.value().commit_index &&
                third.value().state_hash == first.value().state_hash &&
                first.value().commit_index >= 2;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  EXPECT_TRUE(caught_up);

  leader.value()->stop();
  follower_a.value()->stop();
  follower_b.value()->stop();
  leader_thread.join();
  follower_a_thread.join();
  follower_b_thread.join();
}

}  // namespace
}  // namespace choreoos::network
