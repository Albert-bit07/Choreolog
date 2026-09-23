// Fault-injection tests.
// Storage faults are one-shot and refused unless the store was opened in test
// mode. Link drops and partitions do nothing until the simulator enables
// faults, and a heal must let the lagged follower converge.

#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <vector>

#include "choreoos/protocol/frame.hpp"
#include "choreoos/replication/replica.hpp"
#include "choreoos/replication/simulator.hpp"
#include "choreoos/state/machine.hpp"
#include "choreoos/state/store.hpp"

namespace choreoos {
namespace {

using choreoos::protocol::MessageType;
using choreoos::replication::Replica;
using choreoos::replication::ReplicaConfig;
using choreoos::replication::SimulatedNetwork;
using choreoos::state::ChoreographyId;
using choreoos::state::Command;
using choreoos::state::CommandId;
using choreoos::state::CommandType;
using choreoos::state::CreateChoreographyPayload;
using choreoos::state::ErrorCode;
using choreoos::state::FileEngine;
using choreoos::state::kCurrentSchemaVersion;
using choreoos::state::MusicalTick;
using choreoos::state::OverlapPolicy;
using choreoos::state::propose;
using choreoos::state::StageBounds;
using choreoos::state::state_hash;
using choreoos::state::StorageFault;
using choreoos::state::StoreOptions;

std::filesystem::path fresh_dir(const char* name) {
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
                                           StageBounds::from_mm(20000, 12000).value(),
                                           OverlapPolicy::Forbidden, 5000}};
}

StoreOptions testing_store(bool commit_on_append) {
  StoreOptions options;
  options.commit_on_append = commit_on_append;
  options.test_mode = true;
  return options;
}

ReplicaConfig config_for(const char* id, std::filesystem::path dir,
                         std::vector<std::string> peers) {
  return ReplicaConfig{id, "node-1", std::move(peers), std::move(dir)};
}

void pump(SimulatedNetwork& net, Replica& leader, int rounds) {
  for (int i = 0; i < rounds; ++i) {
    leader.heartbeat();
    net.advance();
  }
}

TEST(StorageFaultTest, RequiresTestMode) {
  auto store = FileEngine::open(fresh_dir("choreoos-fault-gated"));
  ASSERT_TRUE(store);
  auto armed = store.value().arm_storage_fault(StorageFault::FailBeforeFlush);
  EXPECT_FALSE(armed);
  EXPECT_EQ(armed.error().code(), ErrorCode::StoreError);
  EXPECT_EQ(armed.error().message(), "storage faults require test mode");

  // The refused arm must not stick, so a normal submit still commits.
  ASSERT_TRUE(store.value().submit(create_command()));
  EXPECT_EQ(store.value().log_events().size(), 1u);
  EXPECT_EQ(store.value().commit_index().value(), 1u);
}

TEST(StorageFaultTest, FailBeforeFlushWritesNothing) {
  const auto dir = fresh_dir("choreoos-fault-before");
  {
    auto store = FileEngine::open(dir, testing_store(true));
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().arm_storage_fault(StorageFault::FailBeforeFlush));
    auto failed = store.value().submit(create_command());
    EXPECT_FALSE(failed);
    EXPECT_EQ(failed.error().message(), "injected failure before flush");
    EXPECT_TRUE(store.value().log_events().empty());
    EXPECT_EQ(store.value().commit_index().value(), 0u);

    // The fault is one-shot: the retry is a normal append.
    ASSERT_TRUE(store.value().submit(create_command()));
    EXPECT_EQ(store.value().log_events().size(), 1u);
  }

  const auto untouched = fresh_dir("choreoos-fault-before-reopen");
  {
    auto store = FileEngine::open(untouched, testing_store(true));
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().arm_storage_fault(StorageFault::FailBeforeFlush));
    EXPECT_FALSE(store.value().submit(create_command()));
  }
  auto reopened = FileEngine::open(untouched, testing_store(true));
  ASSERT_TRUE(reopened);
  EXPECT_TRUE(reopened.value().log_events().empty());
  EXPECT_EQ(reopened.value().commit_index().value(), 0u);
}

TEST(StorageFaultTest, FailAfterFlushReplaysOnReopen) {
  const auto dir = fresh_dir("choreoos-fault-after");
  std::string expected;
  {
    auto clean = FileEngine::open(fresh_dir("choreoos-fault-after-clean"));
    ASSERT_TRUE(clean);
    ASSERT_TRUE(clean.value().submit(create_command()));
    expected = state_hash(clean.value().engine().state());
  }
  {
    auto store = FileEngine::open(dir, testing_store(true));
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().arm_storage_fault(StorageFault::FailAfterFlush));
    // The caller sees a failure, but the record was already flushed.
    auto failed = store.value().submit(create_command());
    EXPECT_FALSE(failed);
    EXPECT_EQ(failed.error().message(), "injected failure after flush");
    EXPECT_EQ(store.value().log_events().size(), 1u);
    EXPECT_EQ(store.value().commit_index().value(), 0u);
  }

  // Single-node recovery applies every valid record, so the flushed entry commits.
  auto reopened = FileEngine::open(dir, testing_store(true));
  ASSERT_TRUE(reopened);
  EXPECT_EQ(reopened.value().commit_index().value(), 1u);
  EXPECT_EQ(state_hash(reopened.value().engine().state()), expected);
}

TEST(StorageFaultTest, FailAfterFlushStaysUncommittedOnClusterLog) {
  const auto dir = fresh_dir("choreoos-fault-cluster");
  {
    auto store = FileEngine::open(dir, testing_store(false));
    ASSERT_TRUE(store);
    auto proposed = propose(store.value().engine().state(), create_command());
    ASSERT_TRUE(proposed);
    ASSERT_TRUE(store.value().arm_storage_fault(StorageFault::FailAfterFlush));
    auto appended = store.value().append_event(proposed.value());
    EXPECT_FALSE(appended);
    EXPECT_EQ(appended.error().message(), "injected failure after flush");
    EXPECT_EQ(store.value().log_events().size(), 1u);
    EXPECT_EQ(store.value().commit_index().value(), 0u);
  }

  // Cluster recovery stops at the durable commit index, which never moved.
  auto reopened = FileEngine::open(dir, testing_store(false));
  ASSERT_TRUE(reopened);
  EXPECT_EQ(reopened.value().log_events().size(), 1u);
  EXPECT_EQ(reopened.value().commit_index().value(), 0u);
  EXPECT_EQ(reopened.value().engine().state().last_applied.value(), 0u);
}

TEST(FaultTest, DropLinkDoesNothingUntilFaultsAreEnabled) {
  SimulatedNetwork net{31};
  auto leader =
      Replica::open(config_for("node-1", fresh_dir("choreoos-drop-off-1"), {"node-2", "node-3"}));
  auto follower =
      Replica::open(config_for("node-2", fresh_dir("choreoos-drop-off-2"), {"node-1", "node-3"}));
  ASSERT_TRUE(leader);
  ASSERT_TRUE(follower);
  EXPECT_FALSE(net.faults_enabled());
  net.drop_link(MessageType::AppendEntries, "node-1", "node-2");
  net.attach(leader.value());
  net.attach(follower.value());
  ASSERT_TRUE(leader.value().enqueue(create_command()));
  pump(net, leader.value(), 20);
  EXPECT_EQ(follower.value().store().commit_index().value(), 1u);
  EXPECT_EQ(net.schedule().find("drop"), std::string::npos);
}

TEST(FaultTest, DroppedFollowerCatchesUpAfterHeal) {
  SimulatedNetwork net{37};
  net.enable_faults();
  auto leader =
      Replica::open(config_for("node-1", fresh_dir("choreoos-drop-on-1"), {"node-2", "node-3"}));
  auto dropped =
      Replica::open(config_for("node-2", fresh_dir("choreoos-drop-on-2"), {"node-1", "node-3"}));
  auto other =
      Replica::open(config_for("node-3", fresh_dir("choreoos-drop-on-3"), {"node-1", "node-2"}));
  ASSERT_TRUE(leader);
  ASSERT_TRUE(dropped);
  ASSERT_TRUE(other);
  net.drop_link(MessageType::AppendEntries, "node-1", "node-2");
  net.attach(leader.value());
  net.attach(dropped.value());
  net.attach(other.value());

  ASSERT_TRUE(leader.value().enqueue(create_command()));
  pump(net, leader.value(), 20);
  // node-3 still forms a majority with the leader, so the entry commits.
  EXPECT_EQ(leader.value().store().commit_index().value(), 1u);
  EXPECT_EQ(other.value().store().commit_index().value(), 1u);
  EXPECT_EQ(dropped.value().store().commit_index().value(), 0u);
  EXPECT_NE(net.schedule().find("drop"), std::string::npos);

  net.heal();
  pump(net, leader.value(), 30);
  EXPECT_EQ(state_hash(leader.value().store().engine().state()),
            state_hash(dropped.value().store().engine().state()));
  EXPECT_EQ(dropped.value().store().commit_index().value(), 1u);
}

TEST(FaultTest, PartitionOfBothFollowersCannotCommitUntilHeal) {
  SimulatedNetwork net{41};
  net.enable_faults();
  auto leader =
      Replica::open(config_for("node-1", fresh_dir("choreoos-part-1"), {"node-2", "node-3"}));
  auto follower_a =
      Replica::open(config_for("node-2", fresh_dir("choreoos-part-2"), {"node-1", "node-3"}));
  auto follower_b =
      Replica::open(config_for("node-3", fresh_dir("choreoos-part-3"), {"node-1", "node-2"}));
  ASSERT_TRUE(leader);
  ASSERT_TRUE(follower_a);
  ASSERT_TRUE(follower_b);
  net.attach(leader.value());
  net.attach(follower_a.value());
  net.attach(follower_b.value());
  // Both followers are on the far side, so the leader has no majority.
  net.partition({"node-2", "node-3"});

  ASSERT_TRUE(leader.value().enqueue(create_command()));
  pump(net, leader.value(), 15);
  EXPECT_EQ(leader.value().store().commit_index().value(), 0u);
  EXPECT_EQ(leader.value().store().log_events().size(), 1u);
  EXPECT_NE(net.schedule().find("partition"), std::string::npos);

  net.heal();
  pump(net, leader.value(), 30);
  EXPECT_EQ(leader.value().store().commit_index().value(), 1u);
  EXPECT_EQ(state_hash(leader.value().store().engine().state()),
            state_hash(follower_a.value().store().engine().state()));
  EXPECT_EQ(state_hash(leader.value().store().engine().state()),
            state_hash(follower_b.value().store().engine().state()));
}

}  // namespace
}  // namespace choreoos
