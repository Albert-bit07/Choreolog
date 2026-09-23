// Seeded property checks for Milestone 5.
// A passing run is evidence for these seeds, not a proof for every schedule.
// On failure the assertion prints the seed note and, after shrink, the reduced
// prefix that still contains the rejected command.

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "choreoos/protocol/frame.hpp"
#include "choreoos/replication/replica.hpp"
#include "choreoos/replication/simulator.hpp"
#include "choreoos/state/machine.hpp"
#include "choreoos/state/store.hpp"
#include "workload.hpp"

namespace choreoos {
namespace {

using choreoos::replication::Replica;
using choreoos::replication::ReplicaConfig;
using choreoos::replication::SimulatedNetwork;
using choreoos::state::FileEngine;
using choreoos::state::OverlapPolicy;
using choreoos::state::replay;
using choreoos::state::state_hash;
using choreoos::testing::generate_workload;
using choreoos::testing::shrink_failing_prefix;
using choreoos::testing::WorkloadStep;

std::filesystem::path fresh_dir(const std::string& name) {
  auto dir = std::filesystem::temp_directory_path() / name;
  std::filesystem::remove_all(dir);
  return dir;
}

void expect_invariants(const choreoos::state::ChoreographyState& state, const std::string& note) {
  if (!state.created || !state.stage) {
    return;
  }
  std::vector<choreoos::state::Position> marks;
  for (const auto& [id, dancer] : state.dancers) {
    if (!dancer.active) {
      continue;
    }
    EXPECT_TRUE(state.stage->contains(dancer.position)) << note << " dancer=" << id;
    if (state.overlap == OverlapPolicy::Forbidden) {
      for (const auto& earlier : marks) {
        EXPECT_NE(earlier, dancer.position) << note << " dancer=" << id;
      }
      marks.push_back(dancer.position);
    }
  }
}

ReplicaConfig fixed_leader(const char* id, std::filesystem::path dir,
                           std::vector<std::string> peers) {
  return ReplicaConfig{id, "node-1", std::move(peers), std::move(dir)};
}

void pump(SimulatedNetwork& net, Replica& leader, int rounds) {
  for (int i = 0; i < rounds; ++i) {
    leader.heartbeat();
    net.advance();
  }
}

TEST(PropertyTest, ShrinkKeepsTheRejectedCommand) {
  const auto workload = generate_workload(4, 2);
  const auto reduced =
      shrink_failing_prefix(workload.steps, [](const std::vector<WorkloadStep>& steps) {
        for (const auto& step : steps) {
          if (!step.expect_ok) {
            return true;
          }
        }
        return false;
      });
  ASSERT_FALSE(reduced.empty());
  EXPECT_FALSE(reduced.back().expect_ok);
  EXPECT_LT(reduced.size(), workload.steps.size());
  EXPECT_NE(workload.note.find("seed=4"), std::string::npos);
}

TEST(PropertyTest, SeededWorkloadsReplayAndReopen) {
  for (std::uint64_t seed = 1; seed <= 8; ++seed) {
    const auto workload = generate_workload(seed, 2);
    const auto dir = fresh_dir("choreoos-prop-" + std::to_string(seed));
    std::string hash;
    std::vector<choreoos::state::Event> committed;
    {
      auto store = FileEngine::open(dir);
      ASSERT_TRUE(store) << workload.note;
      for (const auto& step : workload.steps) {
        auto submitted = store.value().submit(step.command);
        EXPECT_EQ(static_cast<bool>(submitted), step.expect_ok) << workload.note;
        if (!submitted) {
          continue;
        }
        EXPECT_EQ(submitted.value().duplicate, step.expect_duplicate) << workload.note;
        expect_invariants(store.value().engine().state(), workload.note);
      }
      committed = store.value().engine().events();
      hash = state_hash(store.value().engine().state());
      std::uint64_t index = 1;
      for (const auto& event : committed) {
        EXPECT_EQ(event.index.value(), index) << workload.note;
        ++index;
      }
      auto played = replay(committed);
      ASSERT_TRUE(played) << workload.note;
      EXPECT_EQ(state_hash(played.value()), hash) << workload.note;
    }
    auto reopened = FileEngine::open(dir);
    ASSERT_TRUE(reopened) << workload.note;
    EXPECT_EQ(state_hash(reopened.value().engine().state()), hash) << workload.note;
    EXPECT_EQ(reopened.value().engine().events().size(), committed.size()) << workload.note;
  }
}

TEST(PropertyTest, SeededDropHealsToOneCommittedLog) {
  for (std::uint64_t seed : {2u, 5u, 11u}) {
    const auto workload = generate_workload(seed, 1);
    SimulatedNetwork net{seed};
    net.enable_faults();
    const auto lagged = (seed % 2 == 0) ? "node-2" : "node-3";
    auto leader = Replica::open(fixed_leader(
        "node-1", fresh_dir("choreoos-prop-c1-" + std::to_string(seed)), {"node-2", "node-3"}));
    auto follower_a = Replica::open(fixed_leader(
        "node-2", fresh_dir("choreoos-prop-c2-" + std::to_string(seed)), {"node-1", "node-3"}));
    auto follower_b = Replica::open(fixed_leader(
        "node-3", fresh_dir("choreoos-prop-c3-" + std::to_string(seed)), {"node-1", "node-2"}));
    ASSERT_TRUE(leader) << workload.note;
    ASSERT_TRUE(follower_a) << workload.note;
    ASSERT_TRUE(follower_b) << workload.note;
    net.drop_link(choreoos::protocol::MessageType::AppendEntries, "node-1", lagged);
    net.attach(leader.value());
    net.attach(follower_a.value());
    net.attach(follower_b.value());

    for (const auto& step : workload.steps) {
      auto queued = leader.value().enqueue(step.command);
      EXPECT_EQ(static_cast<bool>(queued), step.expect_ok) << workload.note;
    }
    pump(net, leader.value(), 25);
    net.heal();
    pump(net, leader.value(), 40);

    const auto& lead_store = leader.value().store();
    const auto hash = state_hash(lead_store.engine().state());
    EXPECT_EQ(state_hash(follower_a.value().store().engine().state()), hash) << workload.note;
    EXPECT_EQ(state_hash(follower_b.value().store().engine().state()), hash) << workload.note;
    EXPECT_EQ(follower_a.value().store().commit_index().value(), lead_store.commit_index().value())
        << workload.note;
    EXPECT_EQ(follower_b.value().store().commit_index().value(), lead_store.commit_index().value())
        << workload.note;
    const auto committed = lead_store.commit_index().value();
    ASSERT_GE(committed, 1u) << workload.note << "\n" << net.schedule();
    for (std::uint64_t index = 0; index < committed; ++index) {
      EXPECT_EQ(follower_a.value().store().log_events().at(index).command_id.value(),
                lead_store.log_events().at(index).command_id.value())
          << workload.note;
      EXPECT_EQ(follower_b.value().store().log_events().at(index).command_id.value(),
                lead_store.log_events().at(index).command_id.value())
          << workload.note;
    }
  }
}

}  // namespace
}  // namespace choreoos
