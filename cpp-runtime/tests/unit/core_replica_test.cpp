#include "choreoos/replication/core_replica.hpp"

#include <gtest/gtest.h>

#include <array>
#include <deque>
#include <filesystem>
#include <limits>

#include "choreoos/protocol/codec.hpp"

namespace choreoos::replication {
namespace {

class CoreReplicaTest : public testing::Test {
 protected:
  struct Delivery {
    int from;
    int to;
    protocol::Frame frame;
  };
  std::filesystem::path path;
  std::array<std::unique_ptr<CoreReplica>, 3> nodes;
  std::deque<Delivery> messages;
  std::array<bool, 3> isolated{};
  bool drop_final_ack = false;
  bool dropped_final_ack = false;
  std::uint64_t final_correlation = 0;
  state::Command create() const {
    const auto show = state::ChoreographyId::parse("opening").value();
    return {
        state::CommandId::parse("create").value(),
        show,
        state::CommandType::CreateChoreography,
        state::kCurrentSchemaVersion,
        state::MusicalTick::from_count(0).value(),
        state::CreateChoreographyPayload{show, state::StageBounds::from_mm(100000, 20000).value(),
                                         state::OverlapPolicy::Forbidden, 5000}};
  }
  state::Command dancer(int index) const {
    const auto id = "d" + std::to_string(index);
    return {state::CommandId::parse(id).value(),
            state::ChoreographyId::parse("opening").value(),
            state::CommandType::AddDancer,
            state::kCurrentSchemaVersion,
            state::MusicalTick::from_count(index).value(),
            state::DancerPayload{state::DancerId::parse(id).value(),
                                 state::Position::from_mm(index * 2000, 1000).value()}};
  }
  ReplicaConfig config(int index) const {
    const std::array<std::string, 3> ids{"a", "b", "c"};
    ReplicaConfig config;
    config.id = ids[static_cast<std::size_t>(index)];
    config.leader_id = "a";
    config.directory = path / config.id;
    config.elections = true;
    config.snapshot_every = 3;
    config.max_snapshot_chunk_bytes = 64;
    config.election_timeout_min = 2;
    config.election_timeout_max = 4;
    for (int i = 0; i < 3; ++i)
      if (i != index) config.peers.push_back(ids[static_cast<std::size_t>(i)]);
    return config;
  }
  void open(int index) {
    auto opened = CoreReplica::open(config(index));
    ASSERT_TRUE(opened) << opened.error().to_string();
    nodes[static_cast<std::size_t>(index)] =
        std::make_unique<CoreReplica>(std::move(opened.value()));
    nodes[static_cast<std::size_t>(index)]->set_sender(
        [this, index](const std::string& peer, const protocol::Frame& frame) {
          const int target = peer == "a" ? 0 : peer == "b" ? 1 : 2;
          messages.push_back({index, target, frame});
        });
  }
  void deliver() {
    int steps = 0;
    while (!messages.empty() && ++steps < 20000) {
      auto message = std::move(messages.front());
      messages.pop_front();
      if (isolated[static_cast<std::size_t>(message.from)] ||
          isolated[static_cast<std::size_t>(message.to)] ||
          !nodes[static_cast<std::size_t>(message.to)])
        continue;
      if (message.frame.type == protocol::MessageType::InstallSnapshot) {
        auto chunk = protocol::decode_install_snapshot(message.frame.payload);
        if (chunk && !chunk.value().more) final_correlation = message.frame.correlation;
      }
      if (drop_final_ack && !dropped_final_ack && message.from == 2 &&
          message.frame.type == protocol::MessageType::InstallSnapshotResponse &&
          message.frame.correlation == final_correlation) {
        dropped_final_ack = true;
        continue;
      }
      const auto sender = config(message.from).id;
      nodes[static_cast<std::size_t>(message.to)]->handle(message.frame, sender);
    }
    ASSERT_LT(steps, 20000) << "unbounded synchronous heartbeat/message pump";
  }
  void campaign(int index) {
    for (int i = 0; i < 5; ++i) {
      nodes[static_cast<std::size_t>(index)]->heartbeat();
      deliver();
      if (nodes[static_cast<std::size_t>(index)]->status().role == "leader") break;
    }
    ASSERT_EQ(nodes[static_cast<std::size_t>(index)]->status().role, "leader");
    deliver();
  }
  void SetUp() override {
    path = std::filesystem::temp_directory_path() /
           (std::string("choreoos-core-replica-") +
            testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::remove_all(path);
    for (int i = 0; i < 3; ++i) open(i);
    campaign(0);
  }
  void TearDown() override {
    for (auto& node : nodes) node.reset();
    std::filesystem::remove_all(path);
  }
  void converge() {
    for (int i = 0; i < 10; ++i) {
      for (auto& node : nodes)
        if (node) node->heartbeat();
      deliver();
    }
  }
};

TEST_F(CoreReplicaTest, MajorityCommitAndDurableRetryReceiptSurviveLeaderReopen) {
  isolated[1] = isolated[2] = true;
  auto first = nodes[0]->enqueue(create());
  ASSERT_TRUE(first);
  EXPECT_FALSE(first.value().committed);
  EXPECT_EQ(nodes[0]->store().engine().state().receipts.count("create"), 0u);
  auto duplicate = nodes[0]->enqueue(create());
  ASSERT_TRUE(duplicate);
  EXPECT_TRUE(duplicate.value().duplicate);
  EXPECT_EQ(duplicate.value().event.index, first.value().event.index);
  auto changed = create();
  std::get<state::CreateChoreographyPayload>(changed.payload).max_travel_mm = 6000;
  auto conflict = nodes[0]->enqueue(changed);
  ASSERT_FALSE(conflict);
  EXPECT_EQ(conflict.error().code(), state::ErrorCode::CommandConflict);
  isolated[1] = isolated[2] = false;
  converge();
  EXPECT_EQ(nodes[0]->store().engine().state().receipts.at("create").index,
            first.value().event.index.value());
  messages.clear();
  nodes[0].reset();
  open(0);
  EXPECT_EQ(nodes[0]->status().role, "follower");
  campaign(0);
  auto recovered = nodes[0]->enqueue(create());
  ASSERT_TRUE(recovered);
  EXPECT_TRUE(recovered.value().duplicate);
  EXPECT_TRUE(recovered.value().committed);
  EXPECT_EQ(recovered.value().event.index, first.value().event.index);
}

TEST_F(CoreReplicaTest, ChunkedSnapshotCatchupRetriesLostFinalAckThenReplicatesSuffix) {
  isolated[2] = true;
  ASSERT_TRUE(nodes[0]->enqueue(create()));
  deliver();
  for (int i = 1; i <= 8; ++i) {
    ASSERT_TRUE(nodes[0]->enqueue(dancer(i)));
    deliver();
  }
  ASSERT_TRUE(nodes[0]->store().latest_snapshot());
  EXPECT_GE(nodes[0]->store().latest_snapshot()->index.value(), 6u);
  isolated[2] = false;
  drop_final_ack = true;
  converge();
  EXPECT_TRUE(dropped_final_ack);
  EXPECT_GT(nodes[0]->metrics().snapshot_chunks_sent, 1u);
  EXPECT_EQ(nodes[2]->status().commit_index, nodes[0]->status().commit_index);
  EXPECT_EQ(nodes[2]->status().state_hash, nodes[0]->status().state_hash);
  EXPECT_EQ(nodes[2]->store().engine().state().receipts.size(), 9u);
  EXPECT_EQ(nodes[2]->store().engine().state().dancers.size(), 8u);
}

TEST_F(CoreReplicaTest, TransportIdentityMustAgreeBeforeAnyTermOrDiskChange) {
  const auto before = nodes[0]->status();
  protocol::AppendEntries forged;
  forged.term = before.term + 100;
  forged.leader_id = "b";
  protocol::Frame frame{protocol::MessageType::AppendEntries, 123,
                        protocol::encode(forged).value()};
  nodes[0]->handle(frame, "c");
  nodes[0]->handle(frame, "unknown");
  nodes[0]->handle(frame, "");
  EXPECT_EQ(nodes[0]->status().term, before.term);
  EXPECT_EQ(nodes[0]->status().role, "leader");
  nodes[0]->handle(frame, "b");
  EXPECT_EQ(nodes[0]->status().term, forged.term);
  EXPECT_EQ(nodes[0]->status().role, "follower");
  EXPECT_EQ(nodes[0]->store().consensus_metadata().value().term.value(), forged.term);
}

TEST_F(CoreReplicaTest, FailoverRepairsUncommittedCommandWithoutCreatingItsReceipt) {
  ASSERT_TRUE(nodes[0]->enqueue(create()));
  converge();
  isolated[0] = true;
  auto abandoned = nodes[0]->enqueue(dancer(1));
  ASSERT_TRUE(abandoned);
  EXPECT_FALSE(abandoned.value().committed);
  deliver();
  campaign(1);
  ASSERT_TRUE(nodes[1]->enqueue(dancer(2)));
  deliver();
  isolated[0] = false;
  converge();
  EXPECT_EQ(nodes[0]->status().role, "follower");
  EXPECT_EQ(nodes[0]->status().state_hash, nodes[1]->status().state_hash);
  EXPECT_EQ(nodes[0]->store().engine().state().receipts.count("d1"), 0u);
  EXPECT_EQ(nodes[0]->store().engine().state().receipts.count("d2"), 1u);
  auto retried = nodes[1]->enqueue(dancer(1));
  ASSERT_TRUE(retried);
  EXPECT_FALSE(retried.value().duplicate);
  EXPECT_GT(retried.value().event.index.value(), abandoned.value().event.index.value());
  converge();
  EXPECT_EQ(nodes[0]->store().engine().state().receipts.count("d1"), 1u);
}

TEST(CoreReplicaStandaloneTest, ExhaustedAbsoluteIndexCannotExposeAUsableLeader) {
  const auto path = std::filesystem::temp_directory_path() / "choreoos-core-index-exhausted";
  std::filesystem::remove_all(path);
  {
    state::ChoreographyState state;
    state.last_applied =
        state::LogIndex::parse(std::numeric_limits<std::uint64_t>::max() - 1).value();
    const storage::Snapshot snapshot{state.last_applied,
                                     state.term,
                                     state::state_hash(state),
                                     {},
                                     state::snapshot_payload(state)};
    auto store = state::FileEngine::open(path);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().install_snapshot(snapshot));
  }
  {
    ReplicaConfig config;
    config.id = config.leader_id = "a";
    config.directory = path;
    config.elections = true;
    config.election_timeout_min = config.election_timeout_max = 1;
    config.snapshot_every = 0;
    auto replica = CoreReplica::open(config);
    ASSERT_TRUE(replica);
    replica.value().heartbeat();
    EXPECT_EQ(replica.value().status().role, "halted");
    EXPECT_EQ(replica.value().store().last_log_index(),
              std::numeric_limits<std::uint64_t>::max() - 1);
    EXPECT_TRUE(replica.value().store().engine().state().receipts.empty());
  }
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace choreoos::replication
