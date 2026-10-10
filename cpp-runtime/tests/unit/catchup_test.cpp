// Catch-up through the real wire limit. The simulator drops any frame over
// protocol::kMaxFramePayload and counts it, so these tests fail if a leader
// ever builds a message a TCP peer could not receive.

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "choreoos/replication/replica.hpp"
#include "choreoos/replication/simulator.hpp"
#include "choreoos/state/store.hpp"

namespace choreoos::replication {
namespace {

using choreoos::state::ChoreographyId;
using choreoos::state::Command;
using choreoos::state::CommandId;
using choreoos::state::CommandType;
using choreoos::state::CreateChoreographyPayload;
using choreoos::state::DancerId;
using choreoos::state::DancerPayload;
using choreoos::state::FileEngine;
using choreoos::state::kCurrentSchemaVersion;
using choreoos::state::MusicalTick;
using choreoos::state::OverlapPolicy;
using choreoos::state::Position;
using choreoos::state::StageBounds;
using choreoos::state::StoreOptions;

constexpr int kDancers = 20;

std::filesystem::path fresh(const std::string& name) {
  auto dir = std::filesystem::temp_directory_path() / name;
  std::filesystem::remove_all(dir);
  return dir;
}

Command create_command() {
  return Command{CommandId::parse("c-create").value(),
                 ChoreographyId::parse("opening").value(),
                 CommandType::CreateChoreography,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(0).value(),
                 CreateChoreographyPayload{ChoreographyId::parse("opening").value(),
                                           StageBounds::from_mm(100000, 100000).value(),
                                           OverlapPolicy::Allowed, 5000}};
}

Command add_command(int i) {
  const std::string id = "w" + std::to_string(i);
  return Command{CommandId::parse("add-" + id).value(),
                 ChoreographyId::parse("opening").value(),
                 CommandType::AddDancer,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(1).value(),
                 DancerPayload{DancerId::parse(id).value(), Position::from_mm(1000, i * 1000).value()}};
}

Command move_command(int i) {
  const std::string id = "w" + std::to_string(i % kDancers);
  return Command{CommandId::parse("mv-" + std::to_string(i)).value(),
                 ChoreographyId::parse("opening").value(),
                 CommandType::MoveDancer,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(1).value(),
                 DancerPayload{DancerId::parse(id).value(),
                               Position::from_mm(((i / kDancers) % 2 == 0) ? 1000 : 2000,
                                                 (i % kDancers) * 1000)
                                   .value()}};
}

// Build a committed single-node store of `moves` move events plus setup.
void prebuild_leader_log(const std::filesystem::path& dir, int moves) {
  StoreOptions options;
  options.durability = choreoos::storage::Durability::Buffered;
  options.snapshot_every = 0;
  auto store = FileEngine::open(dir, options);
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().submit(create_command()));
  for (int i = 0; i < kDancers; ++i) {
    ASSERT_TRUE(store.value().submit(add_command(i)));
  }
  for (int i = 0; i < moves; ++i) {
    auto result = store.value().submit(move_command(i));
    ASSERT_TRUE(result) << result.error().to_string();
  }
}

bool converge(SimulatedNetwork& net, Replica& leader, Replica& a, Replica& b, int max_rounds) {
  for (int round = 0; round < max_rounds; ++round) {
    leader.heartbeat();
    net.advance();
    net.advance();
    const auto target = leader.status().commit_index;
    if (a.status().commit_index == target && b.status().commit_index == target) {
      return true;
    }
  }
  return false;
}

TEST(CatchUpTest, BacklogLargerThanOneFrameStillConverges) {
  // ~12,000 events at ~117 bytes each is more than one 1 MiB frame holds. The
  // old leader sent the whole backlog in one message and the frame was dropped
  // forever.
  const auto leader_dir = fresh("choreoos-catchup-leader");
  prebuild_leader_log(leader_dir, 12000);

  SimulatedNetwork net{11};
  auto leader = Replica::open(ReplicaConfig{"node-1", "node-1", {"node-2", "node-3"}, leader_dir});
  auto a = Replica::open(ReplicaConfig{"node-2", "node-1", {"node-1", "node-3"}, fresh("choreoos-catchup-a")});
  auto b = Replica::open(ReplicaConfig{"node-3", "node-1", {"node-1", "node-2"}, fresh("choreoos-catchup-b")});
  ASSERT_TRUE(leader);
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  net.attach(leader.value());
  net.attach(a.value());
  net.attach(b.value());

  ASSERT_GT(leader.value().status().commit_index, 12000u);
  ASSERT_TRUE(converge(net, leader.value(), a.value(), b.value(), 400));

  EXPECT_EQ(net.oversize_frames(), 0u);
  EXPECT_EQ(leader.value().metrics().messages_dropped_oversize, 0u);
  EXPECT_EQ(a.value().status().state_hash, leader.value().status().state_hash);
  EXPECT_EQ(b.value().status().state_hash, leader.value().status().state_hash);
}

TEST(CatchUpTest, SmallBatchCapSplitsCatchUpAndStillConverges) {
  const auto leader_dir = fresh("choreoos-catchup-small-leader");
  prebuild_leader_log(leader_dir, 300);

  ReplicaConfig leader_config{"node-1", "node-1", {"node-2", "node-3"}, leader_dir};
  leader_config.max_append_entries = 7;
  SimulatedNetwork net{5};
  auto leader = Replica::open(std::move(leader_config));
  auto a = Replica::open(ReplicaConfig{"node-2", "node-1", {"node-1", "node-3"}, fresh("choreoos-catchup-small-a")});
  auto b = Replica::open(ReplicaConfig{"node-3", "node-1", {"node-1", "node-2"}, fresh("choreoos-catchup-small-b")});
  ASSERT_TRUE(leader);
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  net.attach(leader.value());
  net.attach(a.value());
  net.attach(b.value());

  ASSERT_TRUE(converge(net, leader.value(), a.value(), b.value(), 300));
  EXPECT_EQ(a.value().status().state_hash, leader.value().status().state_hash);
  EXPECT_EQ(b.value().status().last_log_index, leader.value().status().last_log_index);
}

}  // namespace
}  // namespace choreoos::replication
