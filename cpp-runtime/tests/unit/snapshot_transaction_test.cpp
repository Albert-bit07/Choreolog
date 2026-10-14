#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "choreoos/state/store.hpp"
#include "choreoos/storage/crc32.hpp"
#include "choreoos/storage/file_util.hpp"

namespace choreoos::state {
namespace {

class SnapshotTransactionTest : public testing::Test {
 protected:
  std::filesystem::path path;
  std::vector<Event> events;
  storage::Snapshot snapshot;

  StoreOptions options() const {
    StoreOptions value;
    value.commit_on_append = false;
    value.snapshot_every = 0;
    value.test_mode = true;
    return value;
  }

  void SetUp() override {
    path = std::filesystem::temp_directory_path() /
           (std::string("choreoos-txn-") +
            testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::remove_all(path);
    const auto show = ChoreographyId::parse("opening").value();
    Engine source;
    ASSERT_TRUE(
        source.submit({CommandId::parse("create").value(), show, CommandType::CreateChoreography,
                       kCurrentSchemaVersion, MusicalTick::from_count(0).value(),
                       CreateChoreographyPayload{show, StageBounds::from_mm(20000, 12000).value(),
                                                 OverlapPolicy::Forbidden, 5000}}));
    for (int i = 1; i <= 3; ++i) {
      const auto id = "d" + std::to_string(i);
      ASSERT_TRUE(source.submit(
          {CommandId::parse(id).value(), show, CommandType::AddDancer, kCurrentSchemaVersion,
           MusicalTick::from_count(1).value(),
           DancerPayload{DancerId::parse(id).value(), Position::from_mm(i * 2000, 1000).value()}}));
      if (i == 2) {
        snapshot = {source.state().last_applied,
                    source.state().term,
                    state_hash(source.state()),
                    {},
                    snapshot_payload(source.state())};
      }
    }
    events = source.events();
    auto store = FileEngine::open(path, options());
    ASSERT_TRUE(store);
    for (const auto& event : events) ASSERT_TRUE(store.value().append_event(event));
    ASSERT_TRUE(store.value().commit_through(LogIndex::parse(1).value()));
    ASSERT_TRUE(
        store.value().persist_consensus(Term::parse(9).value(), NodeId::parse("node-b").value()));
  }

  void TearDown() override { std::filesystem::remove_all(path); }

  void expect_recovered() {
    auto store = FileEngine::open(path, options());
    ASSERT_TRUE(store) << store.error().to_string();
    EXPECT_EQ(store.value().commit_index().value(), 3u);
    EXPECT_EQ(state_hash(store.value().engine().state()), snapshot.state_hash);
    EXPECT_EQ(store.value().log_first_index(), 3u);
    EXPECT_EQ(store.value().last_log_index(), 4u);
    EXPECT_EQ(store.value().engine().state().receipts.count("d3"), 0u);
    auto metadata = store.value().consensus_metadata();
    ASSERT_TRUE(metadata);
    EXPECT_EQ(metadata.value().term.value(), 9u);
    ASSERT_TRUE(metadata.value().voted_for);
    EXPECT_EQ(metadata.value().voted_for->value(), "node-b");
    EXPECT_FALSE(std::filesystem::exists(path / "snapshot-install.txn"));
    ASSERT_TRUE(store.value().commit_through(LogIndex::parse(4).value()));
    auto expected = replay(events);
    ASSERT_TRUE(expected);
    EXPECT_EQ(state_hash(store.value().engine().state()), state_hash(expected.value()));
    auto reopened = FileEngine::open(path, options());
    ASSERT_TRUE(reopened);
    EXPECT_EQ(reopened.value().commit_index().value(), 4u);
    EXPECT_EQ(state_hash(reopened.value().engine().state()), state_hash(expected.value()));
  }
};

TEST_F(SnapshotTransactionTest, EveryDurableBoundaryRollsForwardAndBlocksOldWriter) {
  const std::vector<storage::InstallFault> faults{
      storage::InstallFault::AfterJournal, storage::InstallFault::AfterSnapshot,
      storage::InstallFault::AfterMetadata, storage::InstallFault::AfterWal};
  // Keep each simulated crash independent: setup supplies old committed index 1.
  for (const auto fault : faults) {
    const auto case_path = path / std::to_string(static_cast<int>(fault));
    auto store = FileEngine::open(case_path, options());
    ASSERT_TRUE(store);
    for (const auto& event : events) ASSERT_TRUE(store.value().append_event(event));
    ASSERT_TRUE(store.value().commit_through(LogIndex::parse(1).value()));
    ASSERT_TRUE(
        store.value().persist_consensus(Term::parse(9).value(), NodeId::parse("node-b").value()));
    ASSERT_TRUE(store.value().arm_snapshot_install_fault(fault));
    EXPECT_FALSE(store.value().install_snapshot(snapshot));
    EXPECT_EQ(store.value().commit_index().value(), 1u);
    EXPECT_TRUE(std::filesystem::exists(case_path / "snapshot-install.txn"));
    auto next = events.back();
    next.index = LogIndex::parse(5).value();
    EXPECT_FALSE(store.value().append_event(next));
    EXPECT_FALSE(store.value().commit_through(LogIndex::parse(2).value()));
    EXPECT_FALSE(store.value().truncate_after(LogIndex::parse(1).value()));
    EXPECT_FALSE(store.value().persist_consensus(Term::parse(10).value(), std::nullopt));
    EXPECT_FALSE(store.value().checkpoint());
    EXPECT_FALSE(store.value().discard_compacted_prefix());
    EXPECT_FALSE(store.value().install_snapshot(snapshot));
    const auto root = path;
    path = case_path;
    expect_recovered();
    path = root;
  }
}

TEST_F(SnapshotTransactionTest, JournalWriteFailureKeepsAllOldFiles) {
  const auto old_wal = storage::read_file(path / "wal.bin").value();
  const auto old_meta = storage::read_file(path / "meta.bin").value();
  ASSERT_TRUE(std::filesystem::create_directory(path / "snapshot-install.txn.tmp"));
  auto store = FileEngine::open(path, options());
  ASSERT_TRUE(store);
  EXPECT_FALSE(store.value().install_snapshot(snapshot));
  EXPECT_EQ(storage::read_file(path / "wal.bin").value(), old_wal);
  EXPECT_EQ(storage::read_file(path / "meta.bin").value(), old_meta);
  EXPECT_FALSE(std::filesystem::exists(path / "snapshots"));
  auto reopened = FileEngine::open(path, options());
  ASSERT_TRUE(reopened);
  EXPECT_EQ(reopened.value().commit_index().value(), 1u);
}

TEST_F(SnapshotTransactionTest, InterruptedRecoveryCanBeRetriedWithoutLosingTheDecision) {
  auto store = FileEngine::open(path, options());
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().arm_snapshot_install_fault(storage::InstallFault::AfterJournal));
  EXPECT_FALSE(store.value().install_snapshot(snapshot));
  ASSERT_TRUE(std::filesystem::create_directory(path / "meta.bin.tmp"));
  EXPECT_FALSE(FileEngine::open(path, options()));
  EXPECT_TRUE(std::filesystem::exists(path / "snapshot-install.txn"));
  std::filesystem::remove_all(path / "meta.bin.tmp");
  expect_recovered();
}

TEST_F(SnapshotTransactionTest, RecoveryResolvesTheJournalBeforeScanningOldWal) {
  auto store = FileEngine::open(path, options());
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().arm_snapshot_install_fault(storage::InstallFault::AfterJournal));
  EXPECT_FALSE(store.value().install_snapshot(snapshot));
  {
    std::ofstream out(path / "wal.bin", std::ios::binary | std::ios::trunc);
    out << "old WAL unavailable";
  }
  expect_recovered();
}

TEST_F(SnapshotTransactionTest, CorruptJournalFailsClosedBeforeReplacingFiles) {
  auto store = FileEngine::open(path, options());
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().arm_snapshot_install_fault(storage::InstallFault::AfterJournal));
  EXPECT_FALSE(store.value().install_snapshot(snapshot));
  const auto old_wal = storage::read_file(path / "wal.bin").value();
  const auto old_meta = storage::read_file(path / "meta.bin").value();
  {
    std::fstream file(path / "snapshot-install.txn",
                      std::ios::binary | std::ios::in | std::ios::out);
    ASSERT_TRUE(file);
    file.seekp(16);
    file.put('X');
  }
  EXPECT_FALSE(FileEngine::open(path, options()));
  EXPECT_EQ(storage::read_file(path / "wal.bin").value(), old_wal);
  EXPECT_EQ(storage::read_file(path / "meta.bin").value(), old_meta);
  EXPECT_TRUE(std::filesystem::exists(path / "snapshot-install.txn"));
}

TEST_F(SnapshotTransactionTest, ValidChecksumDoesNotPermitAnInvalidLength) {
  auto store = FileEngine::open(path, options());
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().arm_snapshot_install_fault(storage::InstallFault::AfterJournal));
  EXPECT_FALSE(store.value().install_snapshot(snapshot));
  auto bytes = storage::read_file(path / "snapshot-install.txn").value();
  std::string data(bytes.begin(), bytes.end());
  storage::write_u32(data, 20, UINT32_MAX);
  storage::write_u32(data, data.size() - 4, storage::crc32(data.substr(0, data.size() - 4)));
  ASSERT_TRUE(storage::write_atomic(path / "snapshot-install.txn", data, true));
  EXPECT_FALSE(FileEngine::open(path, options()));
  EXPECT_TRUE(std::filesystem::exists(path / "snapshot-install.txn"));
}

TEST_F(SnapshotTransactionTest, FaultInjectionRequiresTestMode) {
  auto value = options();
  value.test_mode = false;
  auto store = FileEngine::open(path, value);
  ASSERT_TRUE(store);
  EXPECT_FALSE(store.value().arm_snapshot_install_fault(storage::InstallFault::AfterJournal));
  ASSERT_TRUE(store.value().install_snapshot(snapshot));
  expect_recovered();
}

TEST_F(SnapshotTransactionTest, ConflictingAnchorDropsTheUncommittedLocalSuffix) {
  Engine source;
  ASSERT_TRUE(source.apply_committed(events[0]));
  ASSERT_TRUE(source.apply_committed(events[1]));
  auto replacement = events[2];
  replacement.term = Term::parse(2).value();
  ASSERT_TRUE(source.apply_committed(replacement));
  storage::Snapshot newer{source.state().last_applied,
                          source.state().term,
                          state_hash(source.state()),
                          {},
                          snapshot_payload(source.state())};
  auto store = FileEngine::open(path, options());
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().arm_snapshot_install_fault(storage::InstallFault::AfterMetadata));
  EXPECT_FALSE(store.value().install_snapshot(newer));
  auto reopened = FileEngine::open(path, options());
  ASSERT_TRUE(reopened);
  EXPECT_EQ(reopened.value().commit_index().value(), 3u);
  EXPECT_TRUE(reopened.value().log_events().empty());
  EXPECT_EQ(reopened.value().last_log_index(), 3u);
  EXPECT_EQ(state_hash(reopened.value().engine().state()), newer.state_hash);
}

TEST_F(SnapshotTransactionTest, MismatchedSnapshotTermCannotCommitAJournal) {
  auto store = FileEngine::open(path, options());
  ASSERT_TRUE(store);
  auto invalid = snapshot;
  invalid.term = Term::parse(2).value();
  EXPECT_FALSE(store.value().install_snapshot(invalid));
  EXPECT_EQ(store.value().commit_index().value(), 1u);
  EXPECT_FALSE(std::filesystem::exists(path / "snapshot-install.txn"));
}

TEST_F(SnapshotTransactionTest, OversizedJournalIsRejectedBeforeLoadingItsImages) {
  const auto old_wal = storage::read_file(path / "wal.bin").value();
  {
    std::ofstream file(path / "snapshot-install.txn", std::ios::binary);
    ASSERT_TRUE(file);
  }
  std::filesystem::resize_file(path / "snapshot-install.txn", 128u * 1024u * 1024u + 1u);
  EXPECT_FALSE(FileEngine::open(path, options()));
  EXPECT_EQ(storage::read_file(path / "wal.bin").value(), old_wal);
}

}  // namespace
}  // namespace choreoos::state
