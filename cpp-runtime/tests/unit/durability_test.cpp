// Crash, checksum, snapshot, and concurrency tests for the durable log.
// A torn tail is discarded. A bad checksum in a complete record is fatal.
// Snapshot recovery must hash-match a full replay of the same log.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <thread>
#include <vector>

#include "choreoos/state/store.hpp"
#include "choreoos/storage/metadata.hpp"
#include "choreoos/storage/wal.hpp"

namespace choreoos::state {
namespace {

CommandId cmd(const char* value) { return CommandId::parse(value).value(); }
ChoreographyId show_id() { return ChoreographyId::parse("opening").value(); }
DancerId dancer(const char* value) { return DancerId::parse(value).value(); }
MusicalTick count(std::int64_t value) { return MusicalTick::from_count(value).value(); }
Position at(std::int32_t x, std::int32_t y) { return Position::from_mm(x, y).value(); }

Command create_command() {
  return Command{cmd("c-create"),
                 show_id(),
                 CommandType::CreateChoreography,
                 kCurrentSchemaVersion,
                 count(0),
                 CreateChoreographyPayload{show_id(), StageBounds::from_mm(20000, 12000).value(),
                                           OverlapPolicy::Forbidden, 5000}};
}

Command add_command(const std::string& id, std::int32_t x) {
  return Command{cmd(id.c_str()),       show_id(), CommandType::AddDancer,
                 kCurrentSchemaVersion, count(1),  DancerPayload{dancer(id.c_str()), at(x, 1000)}};
}

std::filesystem::path fresh_dir(const char* name) {
  auto dir = std::filesystem::temp_directory_path() / name;
  std::filesystem::remove_all(dir);
  return dir;
}

TEST(DurabilityTest, ReopenPreservesHashAndMagic) {
  const auto dir = fresh_dir("choreoos-wal-reopen");
  std::string hash;
  {
    auto store = FileEngine::open(dir);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().submit(create_command()));
    hash = state_hash(store.value().engine().state());
  }
  {
    std::ifstream in{dir / "wal.bin", std::ios::binary};
    std::string magic(8, '\0');
    in.read(magic.data(), 8);
    EXPECT_EQ(magic, "CHOSWAL1");
  }

  auto reopened = FileEngine::open(dir);
  ASSERT_TRUE(reopened);
  EXPECT_EQ(state_hash(reopened.value().engine().state()), hash);
  EXPECT_EQ(reopened.value().recovery().commit_index, 1u);
  std::filesystem::remove_all(dir);
}

TEST(DurabilityTest, TornTailOfEveryPartialLengthIsDiscarded) {
  const auto dir = fresh_dir("choreoos-wal-torn");
  {
    auto store = FileEngine::open(dir);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().submit(create_command()));
  }
  const auto stable = std::filesystem::file_size(dir / "wal.bin");
  std::string original;
  {
    std::ifstream input{dir / "wal.bin", std::ios::binary};
    original.assign(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
  }

  for (std::size_t extra = 1; extra < 48; ++extra) {
    {
      std::ofstream out{dir / "wal.bin", std::ios::binary | std::ios::trunc};
      out << original << std::string(extra, 'X');
    }
    auto store = FileEngine::open(dir);
    ASSERT_TRUE(store) << extra;
    EXPECT_EQ(store.value().engine().events().size(), 1u);
    EXPECT_EQ(store.value().recovery().truncated_tail_bytes, extra);
    EXPECT_EQ(std::filesystem::file_size(dir / "wal.bin"), stable);
  }
  std::filesystem::remove_all(dir);
}

TEST(DurabilityTest, ChecksumMismatchAndOversizedRecordAreRejected) {
  const auto dir = fresh_dir("choreoos-wal-corrupt");
  {
    auto store = FileEngine::open(dir);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().submit(create_command()));
    ASSERT_TRUE(store.value().submit(add_command("alice", 1000)));
  }
  auto bytes = std::filesystem::file_size(dir / "wal.bin");
  {
    std::fstream file{dir / "wal.bin", std::ios::binary | std::ios::in | std::ios::out};
    file.seekp(40);
    file.put('Z');
  }
  auto corrupt = FileEngine::open(dir);
  EXPECT_FALSE(corrupt);
  static_cast<void>(bytes);

  {
    std::ofstream out{dir / "wal.bin", std::ios::binary | std::ios::app};
    std::string prefix(32, '\0');
    prefix[0] = 'L';
    prefix[1] = 'O';
    prefix[2] = 'G';
    prefix[3] = '1';
    prefix[4] = 1;
    prefix[8] = static_cast<char>(0xFF);
    prefix[9] = static_cast<char>(0xFF);
    prefix[10] = static_cast<char>(0xFF);
    prefix[11] = static_cast<char>(0x7F);
    out << prefix;
  }
  // The oversized length sits after a now-corrupt first record, so the checksum
  // error is reported first. Rebuild a log that contains only the oversized header.
  {
    auto fresh = FileEngine::open(fresh_dir("choreoos-wal-oversize"));
    ASSERT_TRUE(fresh);
  }
  const auto over_dir = fresh_dir("choreoos-wal-oversize");
  {
    auto created = FileEngine::open(over_dir);
    ASSERT_TRUE(created);
  }
  {
    std::ofstream out{over_dir / "wal.bin", std::ios::binary | std::ios::app};
    std::string prefix(32, '\0');
    prefix[0] = 'L';
    prefix[1] = 'O';
    prefix[2] = 'G';
    prefix[3] = '1';
    prefix[4] = 1;
    prefix[8] = static_cast<char>(0x01);
    prefix[9] = static_cast<char>(0x00);
    prefix[10] = static_cast<char>(0x10);
    prefix[11] = static_cast<char>(0x00);
    out << prefix;
  }
  EXPECT_FALSE(FileEngine::open(over_dir));
  std::filesystem::remove_all(dir);
  std::filesystem::remove_all(over_dir);
}

TEST(DurabilityTest, SnapshotRecoveryMatchesFullReplay) {
  const auto dir = fresh_dir("choreoos-snapshot");
  StoreOptions options;
  options.snapshot_every = 2;
  std::string hash;
  {
    auto store = FileEngine::open(dir, options);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().submit(create_command()));
    ASSERT_TRUE(store.value().submit(add_command("alice", 1000)));
    ASSERT_TRUE(store.value().submit(add_command("bob", 2000)));
    ASSERT_TRUE(store.value().submit(add_command("cara", 3000)));
    ASSERT_TRUE(store.value().submit(add_command("dee", 4000)));
    hash = state_hash(store.value().engine().state());
  }
  auto recovered = FileEngine::open(dir, options);
  ASSERT_TRUE(recovered);
  EXPECT_TRUE(recovered.value().recovery().used_snapshot);
  EXPECT_EQ(recovered.value().recovery().state_hash, hash);
  EXPECT_GT(recovered.value().recovery().replayed_after_snapshot, 0u);

  auto wal = storage::WriteAheadLog::open(dir / "wal.bin");
  ASSERT_TRUE(wal);
  auto full = replay(wal.value().events());
  ASSERT_TRUE(full);
  EXPECT_EQ(state_hash(full.value()), hash);
  std::filesystem::remove_all(dir);
}

TEST(DurabilityTest, SuffixTruncationDropsUncommittedTail) {
  const auto dir = fresh_dir("choreoos-truncate");
  {
    auto store = FileEngine::open(dir);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().submit(create_command()));
    ASSERT_TRUE(store.value().submit(add_command("alice", 1000)));
    ASSERT_TRUE(store.value().submit(add_command("bob", 2000)));
  }
  auto wal = storage::WriteAheadLog::open(dir / "wal.bin");
  ASSERT_TRUE(wal);
  ASSERT_TRUE(wal.value().truncate_after(LogIndex::parse(2).value()));
  auto reopened = storage::WriteAheadLog::open(dir / "wal.bin");
  ASSERT_TRUE(reopened);
  EXPECT_EQ(reopened.value().events().size(), 2u);
  std::filesystem::remove_all(dir);
}

TEST(DurabilityTest, ConcurrentSubmitsKeepContiguousIndexes) {
  const auto dir = fresh_dir("choreoos-concurrent");
  auto store = FileEngine::open(dir);
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().submit(create_command()));
  constexpr int kThreads = 8;
  std::vector<std::thread> threads;
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&store, i] {
      const auto result =
          store.value().submit(add_command("dancer" + std::to_string(i), 1000 + i * 1000));
      EXPECT_TRUE(result);
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(store.value().engine().state().dancers.size(), static_cast<std::size_t>(kThreads));
  EXPECT_EQ(store.value().engine().state().last_applied.value(),
            static_cast<std::uint64_t>(kThreads + 1));
  std::filesystem::remove_all(dir);
}

// The bytes in fixtures/golden-v1 are a format-version-1 sample. Recovery must
// keep producing this hash after framing or checksum changes.
TEST(DurabilityTest, GoldenV1FixtureRecoversKnownHash) {
  const auto dir = fresh_dir("choreoos-golden-v1");
  std::filesystem::copy(std::filesystem::path{CHOREOOS_FIXTURE_DIR} / "golden-v1", dir,
                        std::filesystem::copy_options::recursive);
  auto store = FileEngine::open(dir);
  ASSERT_TRUE(store);
  EXPECT_EQ(store.value().engine().events().size(), 2u);
  EXPECT_TRUE(store.value().recovery().used_snapshot);
  EXPECT_EQ(store.value().recovery().snapshot_index, 2u);
  EXPECT_EQ(store.value().recovery().commit_index, 2u);
  EXPECT_EQ(state_hash(store.value().engine().state()), "dd22fb677cc125bf");
  std::filesystem::remove_all(dir);
}

TEST(DurabilityTest, MetadataRoundTripsTermVoteAndCommit) {
  const auto dir = fresh_dir("choreoos-meta");
  std::filesystem::create_directories(dir);
  storage::NodeMetadata metadata;
  metadata.term = Term::parse(3).value();
  metadata.commit_index = LogIndex::parse(7).value();
  metadata.voted_for = NodeId::parse("node-a").value();
  ASSERT_TRUE(storage::store_metadata(dir / "meta.bin", metadata, true));
  EXPECT_FALSE(std::filesystem::exists(dir / "meta.bin.tmp"));
  auto loaded = storage::load_metadata(dir / "meta.bin");
  ASSERT_TRUE(loaded);
  EXPECT_EQ(loaded.value().term.value(), 3u);
  EXPECT_EQ(loaded.value().commit_index.value(), 7u);
  ASSERT_TRUE(loaded.value().voted_for);
  EXPECT_EQ(loaded.value().voted_for->value(), "node-a");
  std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace choreoos::state
