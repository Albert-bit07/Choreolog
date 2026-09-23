#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <optional>

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

ReplicaConfig electing(const char* id, std::filesystem::path dir, std::vector<std::string> peers,
                       std::uint64_t seed) {
  ReplicaConfig config{id, "", std::move(peers), std::move(dir), true, seed, 3, 6, 100};
  return config;
}

int leaders_now(Replica& a, Replica& b, Replica& c) {
  int count = 0;
  for (Replica* node : {&a, &b, &c}) {
    if (node->status().role == "leader") {
      ++count;
    }
  }
  return count;
}

Replica* current_leader(Replica& a, Replica& b, Replica& c) {
  for (Replica* node : {&a, &b, &c}) {
    if (node->is_leader()) {
      return node;
    }
  }
  return nullptr;
}

void campaign(SimulatedNetwork& net, Replica& a, Replica& b, Replica& c, int rounds,
              std::map<std::uint64_t, std::string>* terms = nullptr) {
  for (int i = 0; i < rounds; ++i) {
    a.tick();
    b.tick();
    c.tick();
    net.advance();
    if (terms != nullptr) {
      for (Replica* node : {&a, &b, &c}) {
        if (!node->is_leader()) {
          continue;
        }
        const auto term = node->status().term;
        const auto seen = terms->find(term);
        if (seen == terms->end()) {
          terms->insert({term, node->id()});
        } else {
          EXPECT_EQ(seen->second, node->id());
        }
      }
    }
  }
}

TEST(ElectionTest, OneLeaderPerTermThenCommit) {
  SimulatedNetwork net{9};
  auto a = Replica::open(electing("node-1", fresh("choreoos-el-1"), {"node-2", "node-3"}, 11));
  auto b = Replica::open(electing("node-2", fresh("choreoos-el-2"), {"node-1", "node-3"}, 22));
  auto c = Replica::open(electing("node-3", fresh("choreoos-el-3"), {"node-1", "node-2"}, 33));
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  ASSERT_TRUE(c);
  net.attach(a.value());
  net.attach(b.value());
  net.attach(c.value());
  std::map<std::uint64_t, std::string> terms;
  campaign(net, a.value(), b.value(), c.value(), 80, &terms);
  EXPECT_EQ(leaders_now(a.value(), b.value(), c.value()), 1);
  auto* leader = current_leader(a.value(), b.value(), c.value());
  ASSERT_NE(leader, nullptr);
  ASSERT_TRUE(leader->enqueue(create_command()));
  campaign(net, a.value(), b.value(), c.value(), 40, &terms);
  EXPECT_GT(leader->store().commit_index().value(), 0u);
  EXPECT_EQ(state_hash(a.value().store().engine().state()),
            state_hash(b.value().store().engine().state()));
  EXPECT_EQ(state_hash(a.value().store().engine().state()),
            state_hash(c.value().store().engine().state()));
  EXPECT_GE(a.value().metrics().elections_started + b.value().metrics().elections_started +
                c.value().metrics().elections_started,
            1u);
}

TEST(ElectionTest, MinorityCannotCommit) {
  SimulatedNetwork net{4};
  auto a = Replica::open(electing("node-1", fresh("choreoos-min-1"), {"node-2", "node-3"}, 3));
  auto b = Replica::open(electing("node-2", fresh("choreoos-min-2"), {"node-1", "node-3"}, 5));
  auto c = Replica::open(electing("node-3", fresh("choreoos-min-3"), {"node-1", "node-2"}, 7));
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  ASSERT_TRUE(c);
  net.attach(a.value());
  net.attach(b.value());
  net.attach(c.value());
  net.isolate("node-3");
  campaign(net, a.value(), b.value(), c.value(), 80);
  EXPECT_EQ(c.value().store().commit_index().value(), 0u);
  EXPECT_EQ(c.value().status().role == "leader", false);
  auto* leader = current_leader(a.value(), b.value(), c.value());
  ASSERT_NE(leader, nullptr);
  ASSERT_NE(leader->id(), "node-3");
  ASSERT_TRUE(leader->enqueue(create_command()));
  campaign(net, a.value(), b.value(), c.value(), 40);
  EXPECT_GT(leader->store().commit_index().value(), 0u);
  EXPECT_EQ(c.value().store().commit_index().value(), 0u);
  net.heal();
  campaign(net, a.value(), b.value(), c.value(), 80);
  EXPECT_EQ(state_hash(a.value().store().engine().state()),
            state_hash(c.value().store().engine().state()));
}

TEST(ElectionTest, SplitVoteRetriesWithNewTimeouts) {
  SimulatedNetwork net{1};
  auto a = Replica::open(electing("node-1", fresh("choreoos-split-1"), {"node-2", "node-3"}, 1));
  auto b = Replica::open(electing("node-2", fresh("choreoos-split-2"), {"node-1", "node-3"}, 1));
  auto c = Replica::open(electing("node-3", fresh("choreoos-split-3"), {"node-1", "node-2"}, 1));
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  ASSERT_TRUE(c);
  a.value().set_election_bounds(4, 4);
  b.value().set_election_bounds(4, 4);
  c.value().set_election_bounds(4, 4);
  net.attach(a.value());
  net.attach(b.value());
  net.attach(c.value());
  campaign(net, a.value(), b.value(), c.value(), 20);
  EXPECT_EQ(leaders_now(a.value(), b.value(), c.value()), 0);
  EXPECT_GE(a.value().metrics().elections_started, 2u);
  a.value().set_election_bounds(2, 7);
  b.value().set_election_bounds(2, 7);
  c.value().set_election_bounds(2, 7);
  campaign(net, a.value(), b.value(), c.value(), 80);
  EXPECT_EQ(leaders_now(a.value(), b.value(), c.value()), 1);
}

TEST(ElectionTest, OldLeaderStepsDownAndCommittedPrefixSurvives) {
  SimulatedNetwork net{8};
  auto a = Replica::open(electing("node-1", fresh("choreoos-old-1"), {"node-2", "node-3"}, 13));
  auto b = Replica::open(electing("node-2", fresh("choreoos-old-2"), {"node-1", "node-3"}, 17));
  auto c = Replica::open(electing("node-3", fresh("choreoos-old-3"), {"node-1", "node-2"}, 19));
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  ASSERT_TRUE(c);
  net.attach(a.value());
  net.attach(b.value());
  net.attach(c.value());
  campaign(net, a.value(), b.value(), c.value(), 80);
  auto* first = current_leader(a.value(), b.value(), c.value());
  ASSERT_NE(first, nullptr);
  const std::string old_id = first->id();
  ASSERT_TRUE(first->enqueue(create_command()));
  campaign(net, a.value(), b.value(), c.value(), 30);
  ASSERT_GT(first->store().commit_index().value(), 0u);
  std::string committed;
  for (const auto& event : first->store().log_events()) {
    if (event.command_id.value() == "c-create") {
      committed = choreoos::state::canonical_event(event);
    }
  }
  ASSERT_FALSE(committed.empty());
  net.isolate(old_id);
  campaign(net, a.value(), b.value(), c.value(), 80);
  Replica* next = nullptr;
  for (Replica* node : {&a.value(), &b.value(), &c.value()}) {
    if (node->is_leader() && node->id() != old_id) {
      next = node;
    }
  }
  ASSERT_NE(next, nullptr);
  ASSERT_TRUE(next->enqueue(add_command("alice", 1000)));
  campaign(net, a.value(), b.value(), c.value(), 40);
  net.heal();
  campaign(net, a.value(), b.value(), c.value(), 80);
  EXPECT_NE(current_leader(a.value(), b.value(), c.value()), nullptr);
  Replica* old = old_id == "node-1" ? &a.value() : old_id == "node-2" ? &b.value() : &c.value();
  EXPECT_EQ(old->status().role, "follower");
  EXPECT_EQ(state_hash(a.value().store().engine().state()),
            state_hash(b.value().store().engine().state()));
  EXPECT_EQ(state_hash(a.value().store().engine().state()),
            state_hash(c.value().store().engine().state()));
  bool preserved = false;
  for (const auto& event : next->store().log_events()) {
    if (choreoos::state::canonical_event(event) == committed) {
      preserved = true;
    }
  }
  EXPECT_TRUE(preserved);
}

TEST(ElectionTest, RestartComesBackAsFollower) {
  SimulatedNetwork net{6};
  auto dir = fresh("choreoos-restart-1");
  auto config = electing("node-1", dir, {"node-2", "node-3"}, 41);
  auto a = Replica::open(config);
  auto b = Replica::open(electing("node-2", fresh("choreoos-restart-2"), {"node-1", "node-3"}, 43));
  auto c = Replica::open(electing("node-3", fresh("choreoos-restart-3"), {"node-1", "node-2"}, 47));
  ASSERT_TRUE(a);
  ASSERT_TRUE(b);
  ASSERT_TRUE(c);
  net.attach(a.value());
  net.attach(b.value());
  net.attach(c.value());
  campaign(net, a.value(), b.value(), c.value(), 80);
  auto* leader = current_leader(a.value(), b.value(), c.value());
  ASSERT_NE(leader, nullptr);
  ASSERT_TRUE(leader->enqueue(create_command()));
  campaign(net, a.value(), b.value(), c.value(), 30);
  const auto term = a.value().status().term;
  const auto hash = state_hash(b.value().store().engine().state());
  {
    Replica retired = std::move(a.value());
    (void)retired;
  }
  auto restarted = Replica::open(config);
  ASSERT_TRUE(restarted);
  EXPECT_EQ(restarted.value().status().role, "follower");
  EXPECT_GE(restarted.value().status().term, term);
  SimulatedNetwork rejoined{6};
  rejoined.attach(restarted.value());
  rejoined.attach(b.value());
  rejoined.attach(c.value());
  campaign(rejoined, restarted.value(), b.value(), c.value(), 50);
  EXPECT_EQ(state_hash(restarted.value().store().engine().state()), hash);
}

TEST(ReplicationTest, SnapshotInstallCatchesUpCompactedFollower) {
  SimulatedNetwork net{2};
  // node-3 is a peer so the leader tracks it, but it is not attached yet.
  // node-2 supplies the second copy required to commit.
  ReplicaConfig leader_config{
      "node-1", "node-1", {"node-2", "node-3"}, fresh("choreoos-snap-1"), false, 1, 3, 6, 1};
  auto leader = Replica::open(leader_config);
  auto helper = Replica::open(ReplicaConfig{
      "node-2", "node-1", {"node-1", "node-3"}, fresh("choreoos-snap-2"), false, 1, 3, 6, 1});
  ASSERT_TRUE(leader);
  ASSERT_TRUE(helper);
  net.attach(leader.value());
  net.attach(helper.value());
  ASSERT_TRUE(leader.value().enqueue(create_command()));
  pump(net, leader.value(), 20);
  ASSERT_TRUE(leader.value().enqueue(add_command("alice", 1000)));
  pump(net, leader.value(), 20);
  ASSERT_GE(leader.value().store().commit_index().value(), 2u);
  ASSERT_TRUE(leader.value().store().latest_snapshot());
  ASSERT_TRUE(leader.value().store().discard_compacted_prefix());
  EXPECT_TRUE(leader.value().store().log_events().empty());

  auto behind = Replica::open(ReplicaConfig{
      "node-3", "node-1", {"node-1", "node-2"}, fresh("choreoos-snap-3"), false, 1, 3, 6, 1});
  ASSERT_TRUE(behind);
  SimulatedNetwork catchup{2};
  catchup.attach(leader.value());
  catchup.attach(behind.value());
  pump(catchup, leader.value(), 30);
  EXPECT_EQ(state_hash(leader.value().store().engine().state()),
            state_hash(behind.value().store().engine().state()));
  EXPECT_EQ(behind.value().store().commit_index().value(),
            leader.value().store().commit_index().value());
}

}  // namespace
}  // namespace choreoos::replication
