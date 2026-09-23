#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <iostream>
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
                                          std::vector<choreoos::runtime::PeerEndpoint> peers,
                                          bool elections = false, std::uint64_t seed = 1) {
  choreoos::runtime::NodeConfig config;
  config.id = id;
  config.leader_id = leader;
  config.data = std::move(data);
  config.host = "127.0.0.1";
  config.port = port;
  config.peers = std::move(peers);
  config.elections = elections;
  config.rng_seed = seed;
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

TEST(ClusterTest, LeaderFailoverElectsAndConverges) {
  const auto root = fresh("choreoos-tcp-failover");
  const std::uint16_t ports[3] = {19141, 19142, 19143};
  const char* ids[3] = {"node-1", "node-2", "node-3"};
  auto opened = [&](int index) {
    std::vector<choreoos::runtime::PeerEndpoint> peers;
    for (int i = 0; i < 3; ++i) {
      if (i != index) {
        peers.push_back({ids[i], "127.0.0.1", ports[i]});
      }
    }
    return NodeServer::open(node_config(ids[index], "node-1", root / ids[index], ports[index],
                                        peers, true, static_cast<std::uint64_t>(11 + index)));
  };
  auto node_a = opened(0);
  auto node_b = opened(1);
  auto node_c = opened(2);
  ASSERT_TRUE(node_a);
  ASSERT_TRUE(node_b);
  ASSERT_TRUE(node_c);
  std::thread thread_a([&] { node_a.value()->run(); });
  std::thread thread_b([&] { node_b.value()->run(); });
  std::thread thread_c([&] { node_c.value()->run(); });

  auto find_leader = [&](int attempts) -> int {
    for (int attempt = 0; attempt < attempts; ++attempt) {
      for (int i = 0; i < 3; ++i) {
        auto status = status_of(ports[i]);
        if (status && status.value().role == "leader") {
          return i;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return -1;
  };

  const auto started = std::chrono::steady_clock::now();
  const int first = find_leader(160);
  const auto elected = std::chrono::steady_clock::now();
  ASSERT_GE(first, 0);

  Command create{CommandId::parse("c-create").value(),
                 ChoreographyId::parse("opening").value(),
                 CommandType::CreateChoreography,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(0).value(),
                 CreateChoreographyPayload{ChoreographyId::parse("opening").value(),
                                           StageBounds::from_mm(20000, 12000).value(),
                                           OverlapPolicy::Forbidden, 5000}};
  bool committed = false;
  for (int attempt = 0; attempt < 40 && !committed; ++attempt) {
    auto reply = choreoos::protocol::transact("127.0.0.1", ports[first], command_frame(create),
                                              std::chrono::milliseconds(1500));
    if (reply) {
      auto decoded = choreoos::protocol::decode_client_response(reply.value().payload);
      committed = decoded && decoded.value().ok;
    }
    if (!committed) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }
  EXPECT_TRUE(committed);

  NodeServer* servers[3] = {node_a.value().get(), node_b.value().get(), node_c.value().get()};
  std::thread* threads[3] = {&thread_a, &thread_b, &thread_c};
  const auto killed = std::chrono::steady_clock::now();
  servers[first]->stop();
  threads[first]->join();
  // Release the port and the log before a new process reopens the same directory.
  if (first == 0) {
    node_a.value().reset();
  } else if (first == 1) {
    node_b.value().reset();
  } else {
    node_c.value().reset();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  int second = -1;
  for (int attempt = 0; attempt < 160 && second < 0; ++attempt) {
    for (int i = 0; i < 3; ++i) {
      if (i == first) {
        continue;
      }
      auto status = status_of(ports[i]);
      if (status && status.value().role == "leader") {
        second = i;
      }
    }
    if (second < 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }
  const auto reelected = std::chrono::steady_clock::now();
  ASSERT_GE(second, 0);
  ASSERT_NE(second, first);

  Command add{
      CommandId::parse("c-alice").value(),
      ChoreographyId::parse("opening").value(),
      CommandType::AddDancer,
      kCurrentSchemaVersion,
      MusicalTick::from_count(1).value(),
      DancerPayload{DancerId::parse("alice").value(), Position::from_mm(1000, 1000).value()}};
  bool added = false;
  for (int attempt = 0; attempt < 40 && !added; ++attempt) {
    auto reply = choreoos::protocol::transact("127.0.0.1", ports[second], command_frame(add),
                                              std::chrono::milliseconds(1500));
    if (reply) {
      auto decoded = choreoos::protocol::decode_client_response(reply.value().payload);
      added = decoded && decoded.value().ok;
    }
    if (!added) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }
  const auto resumed = std::chrono::steady_clock::now();
  EXPECT_TRUE(added);

  auto revived = opened(first);
  ASSERT_TRUE(revived);
  std::thread revived_thread([&] { revived.value()->run(); });
  bool converged = false;
  std::string hash;
  for (int attempt = 0; attempt < 80 && !converged; ++attempt) {
    auto left = status_of(ports[second]);
    auto right = status_of(ports[first]);
    auto other = status_of(ports[3 - first - second]);
    converged = left && right && other && left.value().commit_index >= 2 &&
                left.value().state_hash == right.value().state_hash &&
                left.value().state_hash == other.value().state_hash;
    if (converged) {
      hash = left.value().state_hash;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  EXPECT_TRUE(converged);
  const auto elapsed = [](auto from, auto to) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(to - from).count();
  };
  std::cout << "election_ms=" << elapsed(started, elected)
            << " failover_ms=" << elapsed(killed, reelected)
            << " resumed_commit_ms=" << elapsed(reelected, resumed) << " hash=" << hash << '\n';

  if (first != 0) {
    node_a.value()->stop();
  }
  if (first != 1) {
    node_b.value()->stop();
  }
  if (first != 2) {
    node_c.value()->stop();
  }
  revived.value()->stop();
  if (first != 0) {
    thread_a.join();
  }
  if (first != 1) {
    thread_b.join();
  }
  if (first != 2) {
    thread_c.join();
  }
  revived_thread.join();
}

}  // namespace
}  // namespace choreoos::network
