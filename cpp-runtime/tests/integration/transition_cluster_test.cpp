#include <gtest/gtest.h>

#include <array>
#include <boost/asio.hpp>
#include <chrono>
#include <filesystem>
#include <thread>

#include "choreoos/network/node_server.hpp"
#include "choreoos/protocol/client.hpp"
#include "choreoos/protocol/codec.hpp"
#include "choreoos/protocol/command_codec.hpp"

namespace choreoos::network {
namespace {
using namespace std::chrono_literals;

class LiveTransitionNode {
 public:
  explicit LiveTransitionNode(std::unique_ptr<NodeServer> server)
      : server_(std::move(server)), thread_([this] { server_->run(); }) {}
  ~LiveTransitionNode() {
    server_->stop();
    thread_.join();
  }

 private:
  std::unique_ptr<NodeServer> server_;
  std::thread thread_;
};

class TransitionClusterTest : public testing::Test {
 protected:
  std::filesystem::path root;
  std::array<runtime::NodeConfig, 3> configs;
  std::array<std::unique_ptr<LiveTransitionNode>, 3> live;
  void SetUp() override {
    root = std::filesystem::temp_directory_path() /
           (std::string("choreoos-transition-tcp-") +
            testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::remove_all(root);
    std::array<std::uint16_t, 3> ports{};
    {
      boost::asio::io_context io;
      std::vector<boost::asio::ip::tcp::acceptor> held;
      for (auto& port : ports) {
        held.emplace_back(
            io, boost::asio::ip::tcp::endpoint{boost::asio::ip::make_address("127.0.0.1"), 0});
        port = held.back().local_endpoint().port();
      }
    }
    for (std::size_t i = 0; i < configs.size(); ++i) {
      auto& config = configs[i];
      config.id = "node-" + std::to_string(i + 1);
      config.leader_id = "node-1";
      config.data = root / config.id;
      config.port = ports[i];
      config.elections = true;
      config.consensus_engine = runtime::ConsensusEngine::Transition;
      config.snapshot_every = 3;
      config.election_timeout_min = 20;
      config.election_timeout_max = 40;
      config.rng_seed = 11 + i * 7;
      for (std::size_t j = 0; j < configs.size(); ++j)
        if (i != j)
          config.peers.push_back({"node-" + std::to_string(j + 1), "127.0.0.1", ports[j]});
    }
    for (std::size_t i = 0; i < configs.size(); ++i) start(i);
  }
  void start(std::size_t index) {
    auto opened = NodeServer::open(configs[index]);
    ASSERT_TRUE(opened) << opened.error().to_string();
    live[index] = std::make_unique<LiveTransitionNode>(std::move(opened.value()));
  }
  void TearDown() override {
    for (auto& node : live) node.reset();
    std::filesystem::remove_all(root);
  }
  state::Result<protocol::StatusResponse> status(std::size_t index) {
    protocol::Frame frame{protocol::MessageType::StatusQuery, 1,
                          protocol::encode_status_query().value()};
    auto reply = protocol::transact("127.0.0.1", configs[index].port, frame, 1s);
    if (!reply) return reply.error();
    return protocol::decode_status_response(reply.value().payload);
  }
  int leader() {
    const auto until = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < until) {
      for (std::size_t i = 0; i < live.size(); ++i) {
        if (!live[i]) continue;
        auto current = status(i);
        if (current && current.value().role == "leader") return static_cast<int>(i);
      }
      std::this_thread::sleep_for(20ms);
    }
    return -1;
  }
  state::Result<protocol::ClientResponse> submit(std::size_t index, const state::Command& command) {
    protocol::Frame frame{
        protocol::MessageType::ClientCommand, 2,
        protocol::encode(protocol::ClientCommand{protocol::canonical_command(command)}).value()};
    auto reply = protocol::transact("127.0.0.1", configs[index].port, frame, 5s);
    if (!reply) return reply.error();
    return protocol::decode_client_response(reply.value().payload);
  }
};

TEST_F(TransitionClusterTest, ReopenedLaggingNodeInstallsSnapshotAndPreservesRetryReceipts) {
  const int elected = leader();
  ASSERT_GE(elected, 0);
  const auto primary = static_cast<std::size_t>(elected);
  const auto lagging = (primary + 1) % live.size();
  const auto show = state::ChoreographyId::parse("opening").value();
  const state::Command create{
      state::CommandId::parse("create").value(),
      show,
      state::CommandType::CreateChoreography,
      state::kCurrentSchemaVersion,
      state::MusicalTick::from_count(0).value(),
      state::CreateChoreographyPayload{show, state::StageBounds::from_mm(100000, 20000).value(),
                                       state::OverlapPolicy::Forbidden, 5000}};
  auto first = submit(primary, create);
  ASSERT_TRUE(first);
  ASSERT_TRUE(first.value().ok) << first.value().error_message;
  live[lagging].reset();
  std::uint64_t last = first.value().index;
  for (int i = 1; i <= 8; ++i) {
    const auto id = "d" + std::to_string(i);
    auto added =
        submit(primary, {state::CommandId::parse(id).value(), show, state::CommandType::AddDancer,
                         state::kCurrentSchemaVersion, state::MusicalTick::from_count(i).value(),
                         state::DancerPayload{state::DancerId::parse(id).value(),
                                              state::Position::from_mm(i * 2000, 1000).value()}});
    ASSERT_TRUE(added);
    ASSERT_TRUE(added.value().ok) << added.value().error_message;
    last = added.value().index;
  }
  start(lagging);
  bool converged = false;
  const auto until = std::chrono::steady_clock::now() + 10s;
  while (std::chrono::steady_clock::now() < until) {
    auto a = status(primary);
    auto b = status(lagging);
    if (a && b && a.value().commit_index >= last &&
        a.value().commit_index == b.value().commit_index &&
        a.value().state_hash == b.value().state_hash) {
      converged = true;
      break;
    }
    std::this_thread::sleep_for(25ms);
  }
  ASSERT_TRUE(converged);
  protocol::ManagementQuery query;
  query.operation = "recovery";
  protocol::Frame frame{protocol::MessageType::ManagementQuery, 3, protocol::encode(query).value()};
  auto stats = protocol::transact("127.0.0.1", configs[lagging].port, frame, 2s);
  ASSERT_TRUE(stats);
  auto decoded = protocol::decode_management_reply(stats.value().payload);
  ASSERT_TRUE(decoded);
  EXPECT_GE(decoded.value().snapshot_index, 6u);
  EXPECT_NE(decoded.value().recovery_report.find("consensus_engine=transition"), std::string::npos);
  auto retry = submit(primary, create);
  ASSERT_TRUE(retry);
  EXPECT_TRUE(retry.value().ok);
  EXPECT_TRUE(retry.value().duplicate);
  EXPECT_EQ(retry.value().index, first.value().index);
}

TEST(TransitionRuntimeConfigTest, InvalidProgrammaticSelectionDoesNotTouchTheStore) {
  const auto path =
      std::filesystem::temp_directory_path() / "choreoos-transition-invalid-programmatic";
  std::filesystem::remove_all(path);
  runtime::NodeConfig config;
  config.id = config.leader_id = "a";
  config.data = path;
  config.consensus_engine = runtime::ConsensusEngine::Transition;
  EXPECT_FALSE(NodeServer::open(config));
  EXPECT_FALSE(std::filesystem::exists(path));
  config.elections = true;
  config.host = "0.0.0.0";
  EXPECT_FALSE(NodeServer::open(config));
  EXPECT_FALSE(std::filesystem::exists(path));
}

}  // namespace
}  // namespace choreoos::network
