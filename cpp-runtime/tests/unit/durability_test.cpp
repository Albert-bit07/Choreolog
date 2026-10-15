// Crash, checksum, snapshot, and concurrency tests for the durable log.
// A torn tail is discarded. A bad checksum in a complete record is fatal.
// Snapshot recovery must hash-match a full replay of the same log.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <thread>
#include <vector>

#include "choreoos/state/store.hpp"
#include "choreoos/storage/file_util.hpp"
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

TEST(DurabilityTest, MissingCommittedTailFailsRecoveryInBothModes) {
  for (bool cluster : {false, true}) {
    const auto dir =
        fresh_dir(cluster ? "choreoos-committed-tail-cluster" : "choreoos-committed-tail-cli");
    {
      auto store = FileEngine::open(dir);
      ASSERT_TRUE(store);
      ASSERT_TRUE(store.value().submit(create_command()));
      ASSERT_TRUE(store.value().submit(add_command("alice", 1000)));
    }
    std::filesystem::resize_file(dir / "wal.bin", std::filesystem::file_size(dir / "wal.bin") - 1);
    StoreOptions options;
    options.commit_on_append = !cluster;
    EXPECT_FALSE(FileEngine::open(dir, options));
    std::filesystem::remove_all(dir);
  }
}

TEST(DurabilityTest, UncertainAppendBlocksEveryMutationUntilReopen) {
  const auto dir = fresh_dir("choreoos-uncertain-reuse");
  StoreOptions options;
  options.test_mode = true;
  {
    auto store = FileEngine::open(dir, options);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().submit(create_command()));
    ASSERT_TRUE(store.value().arm_storage_fault(StorageFault::FailAfterFlush));
    EXPECT_FALSE(store.value().submit(add_command("alice", 1000)));
    EXPECT_FALSE(store.value().submit(add_command("bob", 2000)));
    EXPECT_FALSE(store.value().persist_consensus(Term::parse(2).value(), std::nullopt));
    EXPECT_FALSE(store.value().truncate_after(LogIndex::parse(1).value()));
    EXPECT_FALSE(store.value().commit_through(LogIndex::parse(2).value()));
    EXPECT_FALSE(store.value().checkpoint());
    ASSERT_EQ(store.value().log_events().size(), 2u);
  }
  auto reopened = FileEngine::open(dir, options);
  ASSERT_TRUE(reopened);
  EXPECT_EQ(reopened.value().engine().state().last_applied.value(), 2u);
  EXPECT_TRUE(reopened.value().submit(add_command("bob", 2000)));
  std::filesystem::remove_all(dir);
}

TEST(DurabilityTest, UnreadableMetadataReturnsErrorAndBlocksReuse) {
  const auto dir = fresh_dir("choreoos-metadata-directory");
  auto store = FileEngine::open(dir);
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().submit(create_command()));
  std::filesystem::remove(dir / "meta.bin");
  std::filesystem::create_directory(dir / "meta.bin");
  EXPECT_NO_THROW(
      { EXPECT_FALSE(store.value().persist_consensus(Term::parse(2).value(), std::nullopt)); });
  std::filesystem::remove(dir / "meta.bin");
  EXPECT_FALSE(store.value().submit(add_command("alice", 1000)));
  std::filesystem::remove_all(dir);
}

TEST(DurabilityTest, CompactingClusterBoundsLiveHistoryAndPreservesOldRetryReceipts) {
  const auto dir = fresh_dir("choreoos-bounded-live-history");
  StoreOptions options;
  options.commit_on_append = false;
  options.compact_log = true;
  options.snapshot_every = 10;
  auto store = FileEngine::open(dir, options);
  ASSERT_TRUE(store);
  auto first = propose(ChoreographyState{}, create_command());
  ASSERT_TRUE(first);
  ASSERT_TRUE(store.value().append_event(first.value()));
  ASSERT_TRUE(store.value().commit_through(first.value().index));
  for (std::uint64_t i = 2; i <= 100; ++i) {
    auto event = first.value();
    event.id = EventId::parse("noop-" + std::to_string(i)).value();
    event.command_id = CommandId::parse("noop-" + std::to_string(i)).value();
    event.index = LogIndex::parse(i).value();
    event.type = EventType::NoOp;
    ASSERT_TRUE(store.value().append_event(event));
    ASSERT_TRUE(store.value().commit_through(event.index));
  }
  EXPECT_LE(store.value().engine().events().size(), 11u);
  EXPECT_LE(store.value().engine().state().applied_events.size(), 11u);
  EXPECT_LE(store.value().engine().state().applied_commands.size(), 11u);
  auto retry = store.value().submit(create_command());
  ASSERT_TRUE(retry);
  EXPECT_TRUE(retry.value().duplicate);
  EXPECT_EQ(retry.value().event.index.value(), 1u);
  EXPECT_EQ(store.value().last_log_index(), 100u);
  EXPECT_FALSE(store.value().engine().replay_through(std::nullopt, std::nullopt));
}

TEST(DurabilityTest, RecoveryAndSnapshotInstallBoundAliasesWithoutDroppingReceipts) {
  const auto dir = fresh_dir("choreoos-recovered-history-aliases");
  StoreOptions options;
  options.compact_log = true;
  options.snapshot_every = 10;
  std::optional<storage::Snapshot> snapshot;
  {
    auto store = FileEngine::open(dir, options);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().submit(create_command()));
    for (int i = 0; i < 40; ++i)
      ASSERT_TRUE(store.value().submit(add_command("d" + std::to_string(i), 1000 + i * 100)));
    snapshot = store.value().latest_snapshot();
    EXPECT_EQ(store.value().engine().state().receipts.size(), 41u);
  }
  auto recovered = FileEngine::open(dir, options);
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered.value().engine().state().receipts.size(), 41u);
  EXPECT_LE(recovered.value().engine().state().applied_commands.size(),
            recovered.value().log_events().size());
  auto retry = recovered.value().submit(create_command());
  ASSERT_TRUE(retry);
  EXPECT_TRUE(retry.value().duplicate);
  const auto target = fresh_dir("choreoos-installed-history-aliases");
  auto follower = FileEngine::open(target, options);
  ASSERT_TRUE(follower);
  ASSERT_TRUE(snapshot);
  ASSERT_TRUE(follower.value().install_snapshot(*snapshot));
  EXPECT_EQ(follower.value().engine().state().receipts.size(), 40u);
  EXPECT_TRUE(follower.value().engine().state().applied_commands.empty());
  auto installed_retry = follower.value().submit(create_command());
  ASSERT_TRUE(installed_retry);
  EXPECT_TRUE(installed_retry.value().duplicate);
}

TEST(DurabilityTest, SnapshotCannotHideAGapInRetainedWalIndexes) {
  const auto dir = fresh_dir("choreoos-retained-prefix-gap");
  StoreOptions options;
  options.snapshot_every = 2;
  options.compact_log = true;
  {
    auto store = FileEngine::open(dir, options);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().submit(create_command()));
    for (int i = 0; i < 6; ++i)
      ASSERT_TRUE(store.value().submit(add_command("d" + std::to_string(i), 1000 + i * 100)));
  }
  auto bytes = storage::read_file(dir / "wal.bin");
  ASSERT_TRUE(bytes);
  bool removed = false;
  for (std::size_t offset = 16; offset < bytes.value().size();) {
    const auto size = 32u + static_cast<std::size_t>(storage::read_u32(bytes.value(), offset + 8));
    if (storage::read_u64(bytes.value(), offset + 20) == 5) {
      bytes.value().erase(bytes.value().begin() + static_cast<std::ptrdiff_t>(offset),
                          bytes.value().begin() + static_cast<std::ptrdiff_t>(offset + size));
      removed = true;
      break;
    }
    offset += size;
  }
  ASSERT_TRUE(removed);
  {
    std::ofstream out(dir / "wal.bin", std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.value().data()),
              static_cast<std::streamsize>(bytes.value().size()));
  }
  // Every remaining frame still has its original valid CRC. Snapshot6 can
  // replay7, but indexes4,6,7 violate the retained vector lookup invariant.
  EXPECT_FALSE(FileEngine::open(dir, options));
}

TEST(DurabilityTest, CommittedSnapshotFinishesDiscardingAnObsoleteWalPrefix) {
  const auto dir = fresh_dir("choreoos-retained-prefix-before-snapshot");
  StoreOptions options;
  options.snapshot_every = 1;
  {
    auto store = FileEngine::open(dir, options);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().submit(create_command()));
    ASSERT_TRUE(store.value().submit(add_command("alice", 1000)));
  }
  std::filesystem::resize_file(dir / "wal.bin", std::filesystem::file_size(dir / "wal.bin") - 1);
  options.commit_on_append = false;
  auto recovered = FileEngine::open(dir, options);
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered.value().commit_index().value(), 2u);
  EXPECT_EQ(recovered.value().engine().state().last_applied.value(), 2u);
  EXPECT_EQ(recovered.value().last_log_index(), 2u);
  EXPECT_TRUE(recovered.value().log_events().empty());
  EXPECT_EQ(recovered.value().log_term_at(2), 1u);
}

TEST(DurabilityTest, UncommittedWalCannotStartAfterAMissingPrefix) {
  const auto dir = fresh_dir("choreoos-uncommitted-missing-prefix");
  std::filesystem::create_directories(dir);
  auto event = propose(ChoreographyState{}, create_command());
  ASSERT_TRUE(event);
  event.value().index = LogIndex::parse(2).value();
  auto bytes = storage::encode_wal({event.value()});
  ASSERT_TRUE(bytes);
  {
    std::ofstream out(dir / "wal.bin", std::ios::binary | std::ios::trunc);
    out.write(bytes.value().data(), static_cast<std::streamsize>(bytes.value().size()));
  }
  StoreOptions options;
  options.commit_on_append = false;
  EXPECT_FALSE(FileEngine::open(dir, options));
}

TEST(DurabilityTest, SnapshotBoundaryTermRemainsAvailableAfterAppendingItsSuffix) {
  const auto dir = fresh_dir("choreoos-snapshot-boundary-suffix");
  Engine engine;
  ASSERT_TRUE(engine.submit(create_command()));
  const auto state = engine.state();
  storage::Snapshot snapshot{
      state.last_applied, state.term, state_hash(state), {}, snapshot_payload(state)};
  StoreOptions options;
  options.commit_on_append = false;
  options.compact_log = true;
  {
    auto store = FileEngine::open(dir, options);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().install_snapshot(snapshot));
    auto next = engine.events().front();
    next.id = EventId::parse("noop-2").value();
    next.command_id = CommandId::parse("noop-2").value();
    next.type = EventType::NoOp;
    next.index = LogIndex::parse(2).value();
    ASSERT_TRUE(store.value().append_event(next));
    EXPECT_EQ(store.value().log_term_at(1), 1u);
  }
  auto recovered = FileEngine::open(dir, options);
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered.value().log_first_index(), 2u);
  EXPECT_EQ(recovered.value().log_term_at(1), 1u);
}

TEST(DurabilityTest, MetadataBeyondWalAndMissingSnapshotSuffixFailClosed) {
  const auto dir = fresh_dir("choreoos-missing-committed-coverage");
  StoreOptions options;
  options.snapshot_every = 1;
  {
    auto store = FileEngine::open(dir, options);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().submit(create_command()));
    ASSERT_TRUE(store.value().submit(add_command("alice", 1000)));
  }
  // Destroy the newest checkpoint and the committed WAL tail. The older
  // snapshot covers only index1, so falling back cannot recover commit2.
  std::filesystem::remove(dir / "snapshots" / "0000000000000002.snap");
  std::filesystem::resize_file(dir / "wal.bin", std::filesystem::file_size(dir / "wal.bin") - 1);
  options.commit_on_append = false;
  EXPECT_FALSE(FileEngine::open(dir, options));
  std::filesystem::remove_all(dir);
  {
    auto store = FileEngine::open(dir);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().submit(create_command()));
  }
  auto metadata = storage::load_metadata(dir / "meta.bin");
  ASSERT_TRUE(metadata);
  metadata.value().commit_index = LogIndex::parse(10).value();
  ASSERT_TRUE(storage::store_metadata(dir / "meta.bin", metadata.value(), true));
  EXPECT_FALSE(FileEngine::open(dir, options));
}

TEST(DurabilityTest, TornUncommittedTailCanBeDiscardedWithoutLosingCommit) {
  const auto dir = fresh_dir("choreoos-safe-uncommitted-tail");
  StoreOptions options;
  options.commit_on_append = false;
  {
    auto store = FileEngine::open(dir, options);
    ASSERT_TRUE(store);
    auto first = propose(ChoreographyState{}, create_command());
    ASSERT_TRUE(first);
    ASSERT_TRUE(store.value().append_event(first.value()));
    ASSERT_TRUE(store.value().commit_through(first.value().index));
    auto later = propose(store.value().engine().state(), add_command("alice", 1000));
    ASSERT_TRUE(later);
    ASSERT_TRUE(store.value().append_event(later.value()));
  }
  std::filesystem::resize_file(dir / "wal.bin", std::filesystem::file_size(dir / "wal.bin") - 1);
  auto recovered = FileEngine::open(dir, options);
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered.value().commit_index().value(), 1u);
  EXPECT_EQ(recovered.value().last_log_index(), 1u);
  EXPECT_EQ(recovered.value().engine().state().last_applied.value(), 1u);
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
