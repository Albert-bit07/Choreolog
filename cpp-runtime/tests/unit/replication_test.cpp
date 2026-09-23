#include <gtest/gtest.h>

#include <filesystem>

#include "choreoos/replication/replica.hpp"
#include "choreoos/replication/simulator.hpp"

namespace choreoos::replication {
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
using choreoos::state::state_hash;

std::filesystem::path fresh(const char* name) {
  auto dir = std::filesystem::temp_directory_path() / name;
  std::filesystem::remove_all(dir);
  return dir;
}

Command create_command(const char* id = "c-create") {
  return Command{CommandId::parse(id).value(),
                 ChoreographyId::parse("opening").value(),
                 CommandType::CreateChoreography,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(0).value(),
                 CreateChoreographyPayload{ChoreographyId::parse("opening").value(),
                                           StageBounds::from_mm(20000, 12000).value(),
                                           OverlapPolicy::Forbidden, 5000}};
}

Command add_command(const char* id, std::int32_t x) {
  return Command{CommandId::parse(id).value(),
                 ChoreographyId::parse("opening").value(),
                 CommandType::AddDancer,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(1).value(),
                 DancerPayload{DancerId::parse(id).value(), Position::from_mm(x, 1000).value()}};
}

ReplicaConfig config_for(const char* id, const char* leader, std::filesystem::path dir,
                         std::vector<std::string> peers) {
  return ReplicaConfig{id, leader, std::move(peers), std::move(dir)};
}

void pump(SimulatedNetwork& net, Replica& leader, int rounds) {
  for (int i = 0; i < rounds; ++i) {
    leader.heartbeat();
    net.advance();
  }
}

TEST(ReplicationTest, MajorityCommitConvergesThreeNodes) {
  SimulatedNetwork net{7};
  auto leader =
      Replica::open(config_for("node-1", "node-1", fresh("choreoos-rep-1"), {"node-2", "node-3"}));
  auto follower_a =
      Replica::open(config_for("node-2", "node-1", fresh("choreoos-rep-2"), {"node-1", "node-3"}));
  auto follower_b =
      Replica::open(config_for("node-3", "node-1", fresh("choreoos-rep-3"), {"node-1", "node-2"}));
  ASSERT_TRUE(leader);
  ASSERT_TRUE(follower_a);
  ASSERT_TRUE(follower_b);
  net.attach(leader.value());
  net.attach(follower_a.value());
  net.attach(follower_b.value());

  auto created = leader.value().enqueue(create_command());
  ASSERT_TRUE(created);
  pump(net, leader.value(), 20);
  EXPECT_TRUE(created.value().event.index <= leader.value().store().commit_index());
  EXPECT_EQ(state_hash(leader.value().store().engine().state()),
            state_hash(follower_a.value().store().engine().state()));
  EXPECT_EQ(state_hash(leader.value().store().engine().state()),
            state_hash(follower_b.value().store().engine().state()));
  EXPECT_EQ(leader.value().status().role, "leader");
  EXPECT_EQ(follower_a.value().status().role, "follower");
}

TEST(ReplicationTest, OneFollowerLossStillCommitsThenCatchesUp) {
  SimulatedNetwork net{11};
  auto leader =
      Replica::open(config_for("node-1", "node-1", fresh("choreoos-loss-1"), {"node-2", "node-3"}));
  auto follower_a =
      Replica::open(config_for("node-2", "node-1", fresh("choreoos-loss-2"), {"node-1", "node-3"}));
  auto follower_b =
      Replica::open(config_for("node-3", "node-1", fresh("choreoos-loss-3"), {"node-1", "node-2"}));
  ASSERT_TRUE(leader);
  ASSERT_TRUE(follower_a);
  ASSERT_TRUE(follower_b);
  net.attach(leader.value());
  net.attach(follower_a.value());
  net.attach(follower_b.value());
  net.isolate("node-3");

  ASSERT_TRUE(leader.value().enqueue(create_command()));
  pump(net, leader.value(), 20);
  EXPECT_EQ(leader.value().store().commit_index().value(), 1u);
  EXPECT_EQ(follower_a.value().store().commit_index().value(), 1u);
  EXPECT_EQ(follower_b.value().store().commit_index().value(), 0u);

  net.heal();
  pump(net, leader.value(), 30);
  EXPECT_EQ(state_hash(leader.value().store().engine().state()),
            state_hash(follower_b.value().store().engine().state()));
}

TEST(ReplicationTest, BothFollowersDownLeavesCommandUncommitted) {
  SimulatedNetwork net{3};
  auto leader =
      Replica::open(config_for("node-1", "node-1", fresh("choreoos-down-1"), {"node-2", "node-3"}));
  auto follower_a =
      Replica::open(config_for("node-2", "node-1", fresh("choreoos-down-2"), {"node-1", "node-3"}));
  auto follower_b =
      Replica::open(config_for("node-3", "node-1", fresh("choreoos-down-3"), {"node-1", "node-2"}));
  ASSERT_TRUE(leader);
  ASSERT_TRUE(follower_a);
  ASSERT_TRUE(follower_b);
  net.attach(leader.value());
  net.attach(follower_a.value());
  net.attach(follower_b.value());
  net.isolate("node-2");
  net.isolate("node-3");
  ASSERT_TRUE(leader.value().enqueue(create_command()));
  pump(net, leader.value(), 10);
  EXPECT_EQ(leader.value().store().commit_index().value(), 0u);
  EXPECT_EQ(leader.value().store().log_events().size(), 1u);
  EXPECT_FALSE(net.schedule().empty());
}

TEST(ReplicationTest, FollowerRejectsClientAndDuplicateAppendIsIdempotent) {
  SimulatedNetwork net{1};
  auto leader = Replica::open(config_for("node-1", "node-1", fresh("choreoos-dup-1"), {"node-2"}));
  auto follower =
      Replica::open(config_for("node-2", "node-1", fresh("choreoos-dup-2"), {"node-1"}));
  ASSERT_TRUE(leader);
  ASSERT_TRUE(follower);
  net.attach(leader.value());
  net.attach(follower.value());
  EXPECT_FALSE(follower.value().enqueue(create_command()));
  EXPECT_EQ(follower.value().enqueue(create_command()).error().code(),
            choreoos::state::ErrorCode::NotLeader);

  ASSERT_TRUE(leader.value().enqueue(create_command()));
  pump(net, leader.value(), 15);
  const auto size = follower.value().store().log_events().size();
  leader.value().heartbeat();
  net.advance();
  net.advance();
  EXPECT_EQ(follower.value().store().log_events().size(), size);

  auto again = leader.value().enqueue(create_command());
  ASSERT_TRUE(again);
  EXPECT_TRUE(again.value().duplicate);
  EXPECT_EQ(leader.value().store().log_events().size(), 1u);
}

TEST(ReplicationTest, ConflictingUncommittedSuffixIsReplaced) {
  auto dir = fresh("choreoos-conflict");
  {
    choreoos::state::StoreOptions options;
    options.commit_on_append = false;
    auto store = choreoos::state::FileEngine::open(dir, options);
    ASSERT_TRUE(store);
    auto proposed =
        choreoos::state::propose(store.value().engine().state(), create_command("other"));
    ASSERT_TRUE(proposed);
    ASSERT_TRUE(store.value().append_event(proposed.value()));
  }
  auto leader =
      Replica::open(config_for("node-1", "node-1", fresh("choreoos-conflict-leader"), {"node-2"}));
  auto follower = Replica::open(config_for("node-2", "node-1", dir, {"node-1"}));
  ASSERT_TRUE(leader);
  ASSERT_TRUE(follower);
  EXPECT_EQ(follower.value().store().log_events().size(), 1u);
  SimulatedNetwork net{5};
  net.attach(leader.value());
  net.attach(follower.value());
  ASSERT_TRUE(leader.value().enqueue(create_command("c-create")));
  pump(net, leader.value(), 25);
  EXPECT_EQ(follower.value().store().commit_index().value(), 1u);
  EXPECT_EQ(follower.value().store().log_events().front().command_id.value(), "c-create");
  EXPECT_EQ(state_hash(leader.value().store().engine().state()),
            state_hash(follower.value().store().engine().state()));
}

}  // namespace
}  // namespace choreoos::replication
