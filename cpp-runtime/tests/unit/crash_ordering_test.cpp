// Crash-ordering tests for snapshot install, WAL rewrite and WAL accounting.
// Each "crash point" test builds the on-disk state a crash would leave between
// two steps of install_snapshot and checks that open() reconciles it.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <vector>

#include "choreoos/state/store.hpp"
#include "choreoos/storage/metadata.hpp"
#include "choreoos/storage/snapshot.hpp"

namespace choreoos::state {
namespace {

CommandId cmd(const std::string& value) { return CommandId::parse(value).value(); }
ChoreographyId show_id() { return ChoreographyId::parse("opening").value(); }

Command create_command() {
  return Command{cmd("c-create"),
                 show_id(),
                 CommandType::CreateChoreography,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(0).value(),
                 CreateChoreographyPayload{show_id(), StageBounds::from_mm(20000, 12000).value(),
                                           OverlapPolicy::Forbidden, 5000}};
}

Command add_command(int i) {
  const std::string id = "d" + std::to_string(i);
  return Command{cmd(id),
                 show_id(),
                 CommandType::AddDancer,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(1).value(),
                 DancerPayload{DancerId::parse(id).value(),
                               Position::from_mm(1000 + (i % 15) * 1000, 1000 + (i / 15) * 1000)
                                   .value()}};
}

std::filesystem::path fresh_dir(const std::string& name) {
  auto dir = std::filesystem::temp_directory_path() / name;
  std::filesystem::remove_all(dir);
  return dir;
}

// A single-node store with `commands` committed events and a snapshot every 10.
struct Leader {
  std::filesystem::path dir;
  std::vector<Event> events;
};

Leader build_leader(const std::string& name, int adds) {
  Leader leader;
  leader.dir = fresh_dir(name);
  StoreOptions options;
  options.snapshot_every = 10;
  auto store = FileEngine::open(leader.dir, options);
  EXPECT_TRUE(store);
  EXPECT_TRUE(store.value().submit(create_command()));
  for (int i = 0; i < adds; ++i) {
    EXPECT_TRUE(store.value().submit(add_command(i)));
  }
  leader.events = store.value().engine().events();
  return leader;
}

std::string hash_through(const std::vector<Event>& events, std::size_t count) {
  std::vector<Event> prefix(events.begin(), events.begin() + static_cast<std::ptrdiff_t>(count));
  auto replayed = replay(prefix);
  EXPECT_TRUE(replayed);
  return state_hash(replayed.value());
}

StoreOptions follower_options() {
  StoreOptions options;
  options.commit_on_append = false;
  options.snapshot_every = 0;
  return options;
}

// A follower holding log entries 1..5, committed through 3.
void build_follower(const std::filesystem::path& dir, const std::vector<Event>& events) {
  auto store = FileEngine::open(dir, follower_options());
  ASSERT_TRUE(store);
  for (std::size_t i = 0; i < 5; ++i) {
    ASSERT_TRUE(store.value().append_event(events[i]));
  }
  ASSERT_TRUE(store.value().commit_through(LogIndex::parse(3).value()));
}

TEST(CrashOrderingTest, SnapshotFileAloneIsIgnoredUntilMetadataCoversIt) {
  // Crash after step 1 (snapshot saved) and before step 2 (metadata).
  const auto leader = build_leader("choreoos-order-leader-1", 11);  // events 1..12, snapshot at 10
  const auto follower = fresh_dir("choreoos-order-follower-1");
  build_follower(follower, leader.events);
  std::filesystem::create_directories(follower / "snapshots");
  std::filesystem::copy_file(leader.dir / "snapshots" / "0000000000000010.snap",
                             follower / "snapshots" / "0000000000000010.snap");

  auto reopened = FileEngine::open(follower, follower_options());
  ASSERT_TRUE(reopened) << reopened.error().to_string();
  EXPECT_EQ(reopened.value().commit_index().value(), 3u);
  EXPECT_FALSE(reopened.value().recovery().used_snapshot);
  EXPECT_EQ(state_hash(reopened.value().engine().state()), hash_through(leader.events, 3));
  EXPECT_EQ(reopened.value().last_log_index(), 5u);
}

TEST(CrashOrderingTest, MetadataCoveringSnapshotRestoresFromIt) {
  // Crash after step 2 (metadata) and before step 3 (WAL rewrite). The WAL
  // still holds its old entries, all of which are skipped or kept as suffix.
  const auto leader = build_leader("choreoos-order-leader-2", 11);
  const auto follower = fresh_dir("choreoos-order-follower-2");
  build_follower(follower, leader.events);
  std::filesystem::create_directories(follower / "snapshots");
  std::filesystem::copy_file(leader.dir / "snapshots" / "0000000000000010.snap",
                             follower / "snapshots" / "0000000000000010.snap");
  storage::NodeMetadata metadata;
  metadata.term = leader.events.back().term;
  metadata.commit_index = LogIndex::parse(10).value();
  ASSERT_TRUE(storage::store_metadata(follower / "meta.bin", metadata, true));

  auto reopened = FileEngine::open(follower, follower_options());
  ASSERT_TRUE(reopened) << reopened.error().to_string();
  EXPECT_TRUE(reopened.value().recovery().used_snapshot);
  EXPECT_EQ(reopened.value().commit_index().value(), 10u);
  EXPECT_EQ(state_hash(reopened.value().engine().state()), hash_through(leader.events, 10));
}

TEST(CrashOrderingTest, CompletedInstallSurvivesReopenAndLeavesNoTempFiles) {
  const auto leader = build_leader("choreoos-order-leader-3", 11);
  const auto follower = fresh_dir("choreoos-order-follower-3");
  build_follower(follower, leader.events);

  auto source = FileEngine::open(leader.dir, StoreOptions{});
  ASSERT_TRUE(source);
  ASSERT_TRUE(source.value().latest_snapshot());
  const storage::Snapshot snapshot = *source.value().latest_snapshot();
  EXPECT_EQ(snapshot.index.value(), 10u);

  {
    auto store = FileEngine::open(follower, follower_options());
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().install_snapshot(snapshot));
    EXPECT_EQ(store.value().commit_index().value(), 10u);
    EXPECT_EQ(state_hash(store.value().engine().state()), hash_through(leader.events, 10));
  }
  for (const auto& entry : std::filesystem::directory_iterator(follower)) {
    EXPECT_NE(entry.path().extension(), ".tmp") << entry.path();
  }
  auto reopened = FileEngine::open(follower, follower_options());
  ASSERT_TRUE(reopened) << reopened.error().to_string();
  EXPECT_EQ(reopened.value().commit_index().value(), 10u);
  EXPECT_EQ(state_hash(reopened.value().engine().state()), hash_through(leader.events, 10));
}

TEST(CrashOrderingTest, WalRewriteReplacesAtomicallyEvenWithStaleTempFile) {
  const auto dir = fresh_dir("choreoos-order-rewrite");
  std::string full_hash;
  {
    StoreOptions options;
    options.snapshot_every = 0;
    auto store = FileEngine::open(dir, options);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().submit(create_command()));
    for (int i = 0; i < 5; ++i) {
      ASSERT_TRUE(store.value().submit(add_command(i)));
    }
    ASSERT_TRUE(store.value().checkpoint());  // snapshot at index 6
    for (int i = 5; i < 7; ++i) {
      ASSERT_TRUE(store.value().submit(add_command(i)));
    }
    full_hash = state_hash(store.value().engine().state());
    // A leftover from an earlier crashed rewrite must not matter.
    std::ofstream stale{dir / "wal.bin.tmp", std::ios::binary};
    stale << "garbage from a crashed rewrite";
    stale.close();
    ASSERT_TRUE(store.value().discard_compacted_prefix());
  }
  EXPECT_FALSE(std::filesystem::exists(dir / "wal.bin.tmp"));
  StoreOptions reopen_options;
  reopen_options.snapshot_every = 0;
  auto reopened = FileEngine::open(dir, reopen_options);
  ASSERT_TRUE(reopened) << reopened.error().to_string();
  EXPECT_EQ(state_hash(reopened.value().engine().state()), full_hash);
  // The snapshot covers 1..6. Records below it are gone; index 6 itself stays
  // as the anchor, followed by 7 and 8.
  ASSERT_EQ(reopened.value().log_events().size(), 3u);
  EXPECT_EQ(reopened.value().log_events().front().index.value(), 6u);
}

TEST(CrashOrderingTest, WalFlushTimeIsPartOfAppendTimeNotACumulativeTotal) {
  const auto dir = fresh_dir("choreoos-order-stats");
  auto store = FileEngine::open(dir, StoreOptions{});
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().submit(create_command()));
  for (int i = 0; i < 40; ++i) {
    ASSERT_TRUE(store.value().submit(add_command(i)));
  }
  const auto& stats = store.value().wal_stats();
  EXPECT_GT(stats.flush_ns, 0u);
  // Every append here is synced, so flush time equals append time. The old
  // code added the running append total each call, which made it far larger.
  EXPECT_LE(stats.flush_ns, stats.append_ns);
}

}  // namespace
}  // namespace choreoos::state
