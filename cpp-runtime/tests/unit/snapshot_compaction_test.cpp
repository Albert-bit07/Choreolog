// State snapshots, log compaction and chunked snapshot transfer.

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "choreoos/replication/replica.hpp"
#include "choreoos/replication/simulator.hpp"
#include "choreoos/state/machine.hpp"
#include "choreoos/state/store.hpp"
#include "choreoos/storage/snapshot.hpp"

namespace choreoos::replication {
namespace {

using namespace choreoos::state;

constexpr int kDancers = 20;

std::filesystem::path fresh(const std::string& name) {
  auto dir = std::filesystem::temp_directory_path() / name;
  std::filesystem::remove_all(dir);
  return dir;
}

ChoreographyId show() { return ChoreographyId::parse("opening").value(); }

Command create_command() {
  return Command{CommandId::parse("c-create").value(),
                 show(),
                 CommandType::CreateChoreography,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(0).value(),
                 CreateChoreographyPayload{show(), StageBounds::from_mm(100000, 100000).value(),
                                           OverlapPolicy::Allowed, 5000}};
}

Command add_command(const std::string& id, int x, int y) {
  return Command{CommandId::parse("add-" + id).value(),
                 show(),
                 CommandType::AddDancer,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(1).value(),
                 DancerPayload{DancerId::parse(id).value(), Position::from_mm(x, y).value()}};
}

Command move_command(int i) {
  const std::string id = "w" + std::to_string(i % kDancers);
  return Command{CommandId::parse("mv-" + std::to_string(i)).value(),
                 show(),
                 CommandType::MoveDancer,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(1).value(),
                 DancerPayload{DancerId::parse(id).value(),
                               Position::from_mm(((i / kDancers) % 2 == 0) ? 1000 : 2000,
                                                 (i % kDancers) * 1000)
                                   .value()}};
}

Command add_w(int i) { return add_command("w" + std::to_string(i), 1000, i * 1000); }

// Exercises every state table, including ids that contain ':' and '.'.
ChoreographyState build_rich_state() {
  Engine engine;
  auto must = [&](Command command) {
    auto result = engine.submit(std::move(command));
    EXPECT_TRUE(result) << result.error().to_string();
  };
  must(create_command());
  must(add_command("lead:1", 1000, 1000));
  must(add_command("back.row", 5000, 1000));
  must(add_command("gone", 9000, 1000));
  must(Command{CommandId::parse("rm-gone").value(), show(), CommandType::RemoveDancer,
               kCurrentSchemaVersion, MusicalTick::from_count(2).value(),
               RemoveDancerPayload{DancerId::parse("gone").value()}});
  must(Command{
      CommandId::parse("form-a").value(), show(), CommandType::DefineFormation,
      kCurrentSchemaVersion, MusicalTick::from_count(3).value(),
      FormationPayload{FormationId::parse("front:line").value(),
                       {DancerId::parse("lead:1").value(), DancerId::parse("back.row").value()}}});
  must(Command{CommandId::parse("cue-1").value(), show(), CommandType::TriggerMusicCue,
               kCurrentSchemaVersion, MusicalTick::from_count(4).value(),
               CuePayload{CueId::parse("intro").value(), std::nullopt}});
  must(Command{CommandId::parse("cue-2").value(), show(), CommandType::TriggerLightingCue,
               kCurrentSchemaVersion, MusicalTick::from_count(5).value(),
               CuePayload{CueId::parse("fade").value(), std::nullopt}});
  return engine.state();
}

TEST(SnapshotStateTest, PayloadRoundTripsEveryTable) {
  const ChoreographyState original = build_rich_state();
  const std::string payload = snapshot_payload(original);
  auto restored = restore_state(payload);
  ASSERT_TRUE(restored) << restored.error().to_string();
  EXPECT_EQ(state_hash(restored.value()), state_hash(original));
  EXPECT_EQ(canonical_state(restored.value()), canonical_state(original));
  EXPECT_EQ(restored.value().applied_commands, original.applied_commands);
  // And it keeps working: the restored state accepts the next event and refuses
  // a command it has already accepted.
  Engine engine;
  engine.restore(restored.value());
  EXPECT_FALSE(engine.submit(add_command("lead:1", 2000, 2000)));  // reused command id
  EXPECT_TRUE(engine.submit(add_command("fresh", 2000, 2000)));
}

TEST(SnapshotStateTest, RejectsDamagedPayloads) {
  const std::string payload = snapshot_payload(build_rich_state());
  EXPECT_FALSE(restore_state(""));
  EXPECT_FALSE(restore_state("not a snapshot\n"));
  EXPECT_FALSE(restore_state(payload + "mystery line\n"));
  std::string bad_overlap = payload;
  bad_overlap.replace(bad_overlap.find("overlap "), 14, "overlap bogus ");
  EXPECT_FALSE(restore_state(bad_overlap));
}

TEST(SnapshotStateTest, EventLineWithInvalidFormationMemberIsRejectedNotDropped) {
  Engine engine;
  ASSERT_TRUE(engine.submit(create_command()));
  ASSERT_TRUE(engine.submit(add_command("lead", 1000, 1000)));
  ASSERT_TRUE(engine.submit(add_command("back", 5000, 1000)));
  ASSERT_TRUE(engine.submit(Command{
      CommandId::parse("form-a").value(), show(), CommandType::DefineFormation,
      kCurrentSchemaVersion, MusicalTick::from_count(3).value(),
      FormationPayload{FormationId::parse("front").value(),
                       {DancerId::parse("lead").value(), DancerId::parse("back").value()}}}));
  const std::string line = canonical_event(engine.events().back());
  ASSERT_TRUE(parse_event(line));
  // Silently skipping the bad member would replay a different formation than
  // the one that was committed.
  std::string damaged = line;
  const auto at = damaged.find("lead");
  ASSERT_NE(at, std::string::npos);
  damaged.replace(at, 4, "l ad");
  EXPECT_FALSE(parse_event(damaged));
}

TEST(SnapshotStateTest, SizeFollowsStateNotHistory) {
  Engine engine;
  ASSERT_TRUE(engine.submit(create_command()));
  for (int i = 0; i < kDancers; ++i) {
    ASSERT_TRUE(engine.submit(add_w(i)));
  }
  for (int i = 0; i < 2000; ++i) {
    ASSERT_TRUE(engine.submit(move_command(i)));
  }
  std::size_t history_bytes = 0;
  for (const auto& event : engine.events()) {
    history_bytes += canonical_event(event).size() + 1;
  }
  const std::size_t snapshot_bytes = snapshot_payload(engine.state()).size();
  // Receipts must remain materially smaller than the event history, even
  // though one compact fingerprint is retained for each accepted command.
  EXPECT_GT(snapshot_bytes, 0u);
  EXPECT_LT(snapshot_bytes * 5, history_bytes);
  auto restored = restore_state(snapshot_payload(engine.state()));
  ASSERT_TRUE(restored);
  EXPECT_EQ(restored.value().receipts.size(), engine.state().receipts.size());
  EXPECT_EQ(state_hash(restored.value()), state_hash(engine.state()));
}

TEST(SnapshotStateTest, LegacyReceiptFormatsRemainReadable) {
  Engine engine;
  auto original = engine.submit(create_command());
  ASSERT_TRUE(original);
  const std::string modern = snapshot_payload(engine.state());
  const auto receipt_start = modern.find("\nr ");
  ASSERT_NE(receipt_start, std::string::npos);
  const auto receipt_end = modern.find('\n', receipt_start + 1);
  ASSERT_NE(receipt_end, std::string::npos);

  // The old v2 payload stored the complete canonical event per command.
  std::string old_v2 = modern;
  old_v2.replace(receipt_start + 1, receipt_end - receipt_start - 1,
                 "receipt " + original.value().event.id.value() + " " +
                     canonical_event(original.value().event));
  old_v2.replace(0, std::string("snapshot-state 3").size(), "snapshot-state 2");
  old_v2 += "applied " + original.value().event.command_id.value() + "\n";
  auto recovered = restore_state(old_v2);
  ASSERT_TRUE(recovered);
  Engine replica;
  replica.restore(recovered.value());
  auto duplicate = replica.submit(create_command());
  ASSERT_TRUE(duplicate);
  EXPECT_TRUE(duplicate.value().duplicate);
  EXPECT_EQ(canonical_event(duplicate.value().event), canonical_event(original.value().event));

  // A legacy v1 payload has dedup IDs but no event outcome after compaction.
  std::string old_v1 = modern;
  old_v1.replace(receipt_start + 1, receipt_end - receipt_start - 1,
                 "applied " + original.value().event.command_id.value());
  old_v1.replace(0, std::string("snapshot-state 3").size(), "snapshot-state 1");
  auto legacy = restore_state(old_v1);
  ASSERT_TRUE(legacy);
  Engine legacy_engine;
  legacy_engine.restore(legacy.value());
  auto missing = legacy_engine.submit(create_command());
  ASSERT_FALSE(missing);
  EXPECT_EQ(missing.error().code(), ErrorCode::OutcomeUnavailable);
}

TEST(SnapshotStateTest, CompactReceiptPayloadRejectsMalformedFields) {
  Engine engine;
  ASSERT_TRUE(engine.submit(create_command()));
  const std::string valid = snapshot_payload(engine.state());
  const auto start = valid.find("\nr ");
  ASSERT_NE(start, std::string::npos);
  const auto end = valid.find('\n', start + 1);
  ASSERT_NE(end, std::string::npos);
  std::string bad = valid;
  bad.replace(start + 1, end - start - 1, "r c-create invalid");
  EXPECT_FALSE(restore_state(bad));
  bad = valid;
  bad.replace(start + 1, end - start - 1, "r c-create zAAAAAAAAAA");
  EXPECT_FALSE(restore_state(bad));
  bad = valid;
  bad.insert(end + 1, "at 999999\n");
  EXPECT_FALSE(restore_state(bad));
}

TEST(SnapshotStateTest, LoadsNewestValidAndFallsBackWhenNewestIsDamaged) {
  const auto dir = fresh("choreoos-snapfile");
  StoreOptions options;
  options.snapshot_every = 5;
  {
    auto store = FileEngine::open(dir, options);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().submit(create_command()));
    for (int i = 0; i < 12; ++i) {
      ASSERT_TRUE(store.value().submit(add_w(i)));
    }
  }
  // Snapshots at 5 and 10 exist; pruning keeps two. Damage the newest.
  std::vector<std::filesystem::path> files;
  for (const auto& entry : std::filesystem::directory_iterator(dir / "snapshots")) {
    files.push_back(entry.path());
  }
  ASSERT_EQ(files.size(), 2u);
  std::sort(files.begin(), files.end());
  {
    std::ofstream damaged{files.back(), std::ios::binary | std::ios::trunc};
    damaged << "damaged";
  }
  auto latest = storage::load_latest_snapshot(dir);
  ASSERT_TRUE(latest);
  ASSERT_TRUE(latest.value());
  EXPECT_EQ(latest.value()->index.value(), 5u);
  // The store still opens: recovery uses the older snapshot plus the log.
  auto reopened = FileEngine::open(dir, options);
  ASSERT_TRUE(reopened) << reopened.error().to_string();
  EXPECT_EQ(reopened.value().engine().state().last_applied.value(), 13u);
}

TEST(CompactionTest, KeepsAnchorAndRecoversAcrossRestart) {
  const auto dir = fresh("choreoos-compact-store");
  StoreOptions options;
  options.commit_on_append = false;
  options.snapshot_every = 10;
  options.compact_log = true;
  std::string expected_hash;
  {
    auto store = FileEngine::open(dir, options);
    ASSERT_TRUE(store);
    Engine source;
    ASSERT_TRUE(source.submit(create_command()));
    for (int i = 0; i < kDancers; ++i) {
      ASSERT_TRUE(source.submit(add_w(i)));
    }
    for (int i = 0; i < 49; ++i) {
      ASSERT_TRUE(source.submit(move_command(i)));
    }
    for (const auto& event : source.events()) {
      ASSERT_TRUE(store.value().append_event(event));
    }
    // Commit in steps so snapshots land at 20, 30, ... 70.
    for (std::uint64_t step = 20; step <= 70; step += 10) {
      ASSERT_TRUE(store.value().commit_through(LogIndex::parse(step).value()));
    }
    expected_hash = state_hash(source.state());
    // Snapshots 60 and 70 are retained, so the log is cut back to 60: a lag
    // margin for followers, and the fallback snapshot still joins up.
    EXPECT_EQ(store.value().log_first_index(), 60u);
    EXPECT_EQ(store.value().log_events().size(), 11u);
    EXPECT_EQ(store.value().last_log_index(), 70u);
    EXPECT_EQ(store.value().log_entry(30), nullptr);
    EXPECT_TRUE(store.value().log_term_at(60).has_value());
    EXPECT_FALSE(store.value().log_term_at(59).has_value());
  }
  auto reopened = FileEngine::open(dir, options);
  ASSERT_TRUE(reopened) << reopened.error().to_string();
  EXPECT_TRUE(reopened.value().recovery().used_snapshot);
  EXPECT_EQ(state_hash(reopened.value().engine().state()), expected_hash);
  EXPECT_EQ(reopened.value().commit_index().value(), 70u);
}

TEST(CompactionTest, DamagedNewestSnapshotFallsBackToTheOlderOneAndTheLog) {
  const auto dir = fresh("choreoos-compact-fallback");
  StoreOptions options;
  options.commit_on_append = false;
  options.snapshot_every = 10;
  options.compact_log = true;
  std::string expected_hash;
  {
    auto store = FileEngine::open(dir, options);
    ASSERT_TRUE(store);
    Engine source;
    ASSERT_TRUE(source.submit(create_command()));
    for (int i = 0; i < kDancers; ++i) {
      ASSERT_TRUE(source.submit(add_w(i)));
    }
    for (int i = 0; i < 49; ++i) {
      ASSERT_TRUE(source.submit(move_command(i)));
    }
    for (const auto& event : source.events()) {
      ASSERT_TRUE(store.value().append_event(event));
    }
    for (std::uint64_t step = 20; step <= 70; step += 10) {
      ASSERT_TRUE(store.value().commit_through(LogIndex::parse(step).value()));
    }
    expected_hash = state_hash(source.state());
  }
  {
    std::ofstream damaged{dir / "snapshots" / "0000000000000070.snap",
                          std::ios::binary | std::ios::trunc};
    damaged << "damaged";
  }
  auto reopened = FileEngine::open(dir, options);
  ASSERT_TRUE(reopened) << reopened.error().to_string();
  EXPECT_EQ(reopened.value().recovery().snapshot_index, 60u);
  EXPECT_EQ(reopened.value().recovery().replayed_after_snapshot, 10u);
  EXPECT_EQ(state_hash(reopened.value().engine().state()), expected_hash);
  EXPECT_EQ(reopened.value().commit_index().value(), 70u);
}

TEST(CompactionTest, RefusesToOpenWhenTheCompactedPrefixHasNoSnapshot) {
  const auto dir = fresh("choreoos-compact-nosnap");
  StoreOptions options;
  options.commit_on_append = false;
  options.snapshot_every = 10;
  options.compact_log = true;
  {
    auto store = FileEngine::open(dir, options);
    ASSERT_TRUE(store);
    Engine source;
    ASSERT_TRUE(source.submit(create_command()));
    for (int i = 0; i < 24; ++i) {
      ASSERT_TRUE(source.submit(add_w(i % kDancers)) || true);
    }
    for (const auto& event : source.events()) {
      ASSERT_TRUE(store.value().append_event(event));
    }
    ASSERT_TRUE(store.value().commit_through(LogIndex::parse(10).value()));
    ASSERT_TRUE(store.value().commit_through(source.state().last_applied));
    ASSERT_GT(store.value().log_first_index(), 1u);
  }
  std::filesystem::remove_all(dir / "snapshots");
  auto reopened = FileEngine::open(dir, options);
  // Losing the snapshot of a compacted log is data loss. It must be reported,
  // not papered over with a partial state.
  EXPECT_FALSE(reopened);
}

// --- replication -----------------------------------------------------------

struct Cluster {
  SimulatedNetwork net;
  std::vector<std::unique_ptr<Result<Replica>>> holders;
  explicit Cluster(std::uint64_t seed) : net(seed) {}

  Replica& open(ReplicaConfig config) {
    holders.push_back(std::make_unique<Result<Replica>>(Replica::open(std::move(config))));
    EXPECT_TRUE(*holders.back());
    Replica& replica = holders.back()->value();
    net.attach(replica);
    return replica;
  }
};

ReplicaConfig node_config(const std::string& id, const std::string& name,
                          std::vector<std::string> peers) {
  ReplicaConfig config{id, "node-1", std::move(peers), fresh(name)};
  config.snapshot_every = 10;
  return config;
}

void drive(SimulatedNetwork& net, Replica& leader, int rounds) {
  for (int i = 0; i < rounds; ++i) {
    leader.heartbeat();
    net.advance();
    net.advance();
  }
}

bool converged(Replica& leader, Replica& other) {
  return other.status().commit_index == leader.status().commit_index &&
         other.status().state_hash == leader.status().state_hash;
}

TEST(SnapshotTransferTest, CaughtUpFollowerNeverNeedsASnapshotWhileLeaderCompacts) {
  Cluster cluster{3};
  Replica& leader = cluster.open(node_config("node-1", "choreoos-xfer-l1", {"node-2", "node-3"}));
  Replica& a = cluster.open(node_config("node-2", "choreoos-xfer-a1", {"node-1", "node-3"}));
  Replica& b = cluster.open(node_config("node-3", "choreoos-xfer-b1", {"node-1", "node-2"}));
  ASSERT_TRUE(leader.enqueue(create_command()));
  for (int i = 0; i < kDancers; ++i) {
    ASSERT_TRUE(leader.enqueue(add_w(i)));
  }
  for (int i = 0; i < 150; ++i) {
    ASSERT_TRUE(leader.enqueue(move_command(i)));
    drive(cluster.net, leader, 1);
  }
  drive(cluster.net, leader, 10);
  ASSERT_TRUE(converged(leader, a));
  ASSERT_TRUE(converged(leader, b));
  // The leader compacted many times, yet nobody fell behind the boundary.
  EXPECT_GT(leader.store().log_first_index(), 100u);
  EXPECT_EQ(leader.metrics().snapshot_chunks_sent, 0u);
}

TEST(SnapshotTransferTest, FreshFollowerCatchesUpFromACompactedLeaderInChunks) {
  Cluster cluster{4};
  auto leader_config = node_config("node-1", "choreoos-xfer-l2", {"node-2", "node-3"});
  leader_config.max_snapshot_chunk_bytes = 200;  // force many chunks
  Replica& leader = cluster.open(std::move(leader_config));
  Replica& helper = cluster.open(node_config("node-2", "choreoos-xfer-a2", {"node-1", "node-3"}));
  ASSERT_TRUE(leader.enqueue(create_command()));
  for (int i = 0; i < kDancers; ++i) {
    ASSERT_TRUE(leader.enqueue(add_w(i)));
    drive(cluster.net, leader, 1);
  }
  for (int i = 0; i < 120; ++i) {
    ASSERT_TRUE(leader.enqueue(move_command(i)));
    drive(cluster.net, leader, 1);
  }
  drive(cluster.net, leader, 10);
  ASSERT_GT(leader.store().log_first_index(), 100u);  // the early log is gone
  ASSERT_TRUE(leader.store().log_entry(5) == nullptr);

  Replica& fresh_node =
      cluster.open(node_config("node-3", "choreoos-xfer-b2", {"node-1", "node-2"}));
  drive(cluster.net, leader, 400);
  EXPECT_TRUE(converged(leader, fresh_node));
  EXPECT_TRUE(converged(leader, helper));
  EXPECT_GT(leader.metrics().snapshot_chunks_sent, 1u);  // really was chunked
  EXPECT_TRUE(fresh_node.store().recovery().used_snapshot);
  EXPECT_EQ(cluster.net.oversize_frames(), 0u);
}

TEST(SnapshotTransferTest, ChunkedTransferSurvivesLossAndDuplication) {
  Cluster cluster{5};
  auto leader_config = node_config("node-1", "choreoos-xfer-l3", {"node-2", "node-3"});
  leader_config.max_snapshot_chunk_bytes = 150;
  Replica& leader = cluster.open(std::move(leader_config));
  Replica& helper = cluster.open(node_config("node-2", "choreoos-xfer-a3", {"node-1", "node-3"}));
  ASSERT_TRUE(leader.enqueue(create_command()));
  for (int i = 0; i < kDancers; ++i) {
    ASSERT_TRUE(leader.enqueue(add_w(i)));
    drive(cluster.net, leader, 1);
  }
  for (int i = 0; i < 100; ++i) {
    ASSERT_TRUE(leader.enqueue(move_command(i)));
    drive(cluster.net, leader, 1);
  }
  drive(cluster.net, leader, 10);
  Replica& fresh_node =
      cluster.open(node_config("node-3", "choreoos-xfer-b3", {"node-1", "node-2"}));
  cluster.net.enable_faults();
  cluster.net.set_drop_percent(25);
  cluster.net.set_duplicate_percent(25);
  cluster.net.set_reorder_ticks(2);
  drive(cluster.net, leader, 900);
  cluster.net.set_drop_percent(0);
  cluster.net.set_duplicate_percent(0);
  cluster.net.set_reorder_ticks(0);
  drive(cluster.net, leader, 60);
  EXPECT_TRUE(converged(leader, fresh_node));
  EXPECT_TRUE(converged(leader, helper));
}

TEST(SnapshotTransferTest, CompactedClusterRestartReturnsSameOutcomeWithoutNewEvents) {
  Cluster cluster{6};
  Replica* leader = &cluster.open(node_config("node-1", "choreoos-xfer-l4", {"node-2", "node-3"}));
  Replica* a = &cluster.open(node_config("node-2", "choreoos-xfer-a4", {"node-1", "node-3"}));
  Replica* b = &cluster.open(node_config("node-3", "choreoos-xfer-b4", {"node-1", "node-2"}));
  ASSERT_TRUE(leader->enqueue(create_command()));
  for (int i = 0; i < kDancers; ++i) {
    ASSERT_TRUE(leader->enqueue(add_w(i)));
    drive(cluster.net, *leader, 1);
  }
  std::optional<Event> original_event;
  for (int i = 0; i < 60; ++i) {
    auto proposed = leader->enqueue(move_command(i));
    ASSERT_TRUE(proposed);
    if (i == 0) {
      original_event = proposed.value().event;
    }
    drive(cluster.net, *leader, 1);
  }
  drive(cluster.net, *leader, 10);
  ASSERT_TRUE(converged(*leader, *a));
  const std::string hash = leader->status().state_hash;
  const std::uint64_t commit = leader->status().commit_index;
  ASSERT_GT(leader->store().log_first_index(), 1u);
  (void)b;

  // Restart the whole cluster from disk. The first cluster still holds the
  // directories open, so destroy its replicas first. open() reuses a directory
  // as-is, so nothing is wiped here.
  cluster.holders.clear();
  Cluster restarted{7};
  auto open_existing = [&](const std::string& id, const std::string& dir,
                           std::vector<std::string> peers) {
    ReplicaConfig config{id, "node-1", std::move(peers),
                         std::filesystem::temp_directory_path() / dir};
    config.snapshot_every = 10;
    return &restarted.open(std::move(config));
  };
  Replica* leader2 = open_existing("node-1", "choreoos-xfer-l4", {"node-2", "node-3"});
  Replica* a2 = open_existing("node-2", "choreoos-xfer-a4", {"node-1", "node-3"});
  open_existing("node-3", "choreoos-xfer-b4", {"node-1", "node-2"});
  drive(restarted.net, *leader2, 20);
  EXPECT_EQ(leader2->status().state_hash, hash);
  EXPECT_EQ(leader2->status().commit_index, commit);
  EXPECT_TRUE(converged(*leader2, *a2));

  // A compacted retry returns the exact original outcome without mutation.
  const auto before_retry = leader2->status();
  auto duplicate = leader2->enqueue(move_command(0));
  ASSERT_TRUE(duplicate);
  EXPECT_TRUE(duplicate.value().duplicate);
  EXPECT_TRUE(duplicate.value().committed);
  ASSERT_TRUE(original_event);
  EXPECT_EQ(duplicate.value().event.index, original_event->index);
  EXPECT_EQ(duplicate.value().event.term, original_event->term);
  EXPECT_EQ(canonical_event(duplicate.value().event), canonical_event(*original_event));
  EXPECT_LT(duplicate.value().event.index.value(), leader2->store().log_first_index());
  EXPECT_EQ(leader2->status().state_hash, before_retry.state_hash);
  EXPECT_EQ(leader2->status().commit_index, before_retry.commit_index);
  EXPECT_EQ(leader2->status().last_log_index, before_retry.last_log_index);
  // New work still commits everywhere.
  ASSERT_TRUE(leader2->enqueue(move_command(1000)));
  drive(restarted.net, *leader2, 20);
  EXPECT_EQ(a2->status().commit_index, commit + 1);
  EXPECT_TRUE(converged(*leader2, *a2));
}

}  // namespace
}  // namespace choreoos::replication
