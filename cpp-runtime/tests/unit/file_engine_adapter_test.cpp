#include "choreoos/raft/file_engine_adapter.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

namespace choreoos::raft {
namespace {

class FileEngineAdapterTest : public testing::Test {
 protected:
  std::filesystem::path path;
  std::vector<state::Event> events;
  state::Command create() const {
    using namespace state;
    const auto show = ChoreographyId::parse("opening").value();
    return {CommandId::parse("create").value(),
            show,
            CommandType::CreateChoreography,
            kCurrentSchemaVersion,
            MusicalTick::from_count(0).value(),
            CreateChoreographyPayload{show, StageBounds::from_mm(20000, 12000).value(),
                                      OverlapPolicy::Forbidden, 5000}};
  }
  void SetUp() override {
    path = std::filesystem::temp_directory_path() /
           (std::string("choreoos-writable-adapter-") +
            testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::remove_all(path);
    state::Engine engine;
    ASSERT_TRUE(engine.submit(create()));
    for (int i = 1; i <= 2; ++i) {
      using namespace state;
      const auto id = "d" + std::to_string(i);
      ASSERT_TRUE(engine.submit(
          {CommandId::parse(id).value(), ChoreographyId::parse("opening").value(),
           CommandType::AddDancer, kCurrentSchemaVersion, MusicalTick::from_count(i).value(),
           DancerPayload{DancerId::parse(id).value(), Position::from_mm(i * 2000, 1000).value()}}));
    }
    events = engine.events();
  }
  void TearDown() override { std::filesystem::remove_all(path); }
  DomainWritePlan plan(std::uint64_t term = 1, std::uint64_t commit = 3) const {
    DomainWritePlan value;
    value.term = term;
    value.commit = commit;
    for (const auto& event : events) value.suffix.push_back(encode_domain_event(event).value());
    return value;
  }
};

Durable projected_durable(const RecoveredDomainProjection& projection) {
  Durable durable;
  durable.term = projection.term;
  durable.voted_for = projection.voted_for;
  durable.log = projection.suffix;
  durable.commit = projection.commit;
  durable.applied = projection.applied;
  if (projection.snapshot) {
    durable.snapshot_index = projection.snapshot->index.value();
    durable.snapshot_term = projection.snapshot->term.value();
  }
  return durable;
}

TEST_F(FileEngineAdapterTest, CoreCompactionCheckpointsBeforeCompletionAndPreservesReceipts) {
  const Config config{"a", {"b"}};
  auto adapter = FileEngineAdapter::open(path);
  ASSERT_TRUE(adapter);
  auto original_plan = plan(4, 2);
  original_plan.voted_for = "a";
  ASSERT_TRUE(adapter.value().persist(original_plan));
  auto before = adapter.value().current_shared();
  ASSERT_TRUE(before);
  auto node = initial(config);
  node.durable = projected_durable(*before.value());
  auto [staged, request] = step(config, node, CompactCommitted{});
  ASSERT_TRUE(request.persist);
  ASSERT_EQ(request.persist->kind, PersistKind::LocalCompaction);
  EXPECT_EQ(staged.durable, node.durable);
  ASSERT_TRUE(adapter.value().checkpoint());
  auto disk = adapter.value().current_shared();
  ASSERT_TRUE(disk);
  EXPECT_EQ(projected_durable(*disk.value()), request.persist->target);
  EXPECT_EQ(disk.value()->applied_state_payload, before.value()->applied_state_payload);
  EXPECT_EQ(disk.value()->applied_state_hash, before.value()->applied_state_hash);
  EXPECT_FALSE(before.value()->snapshot);
  EXPECT_EQ(before.value()->suffix.size(), 3u);
  // Disk completion alone cannot publish the staged core boundary.
  EXPECT_EQ(staged.durable.snapshot_index, 0u);
  auto [compacted, completed] = step(config, staged, PersistCompleted{request.persist->token});
  EXPECT_TRUE(completed.apply.empty());
  EXPECT_TRUE(completed.send_now.empty());
  EXPECT_EQ(compacted.durable, request.persist->target);
  auto reopened = adapter.value().recover();
  ASSERT_TRUE(reopened);
  EXPECT_EQ(projected_durable(reopened.value()), compacted.durable);
  auto domain = replay_recovered_domain(reopened.value());
  ASSERT_TRUE(domain);
  EXPECT_TRUE(state::resolve_receipt(domain.value(), create()));
  EXPECT_EQ(domain.value().receipts.at("d1").index, 2u);
  EXPECT_EQ(domain.value().receipts.count("d2"), 0u);
  EXPECT_EQ(reopened.value().suffix.size(), 1u);
  EXPECT_FALSE(adapter.value().persist(original_plan));  // Pre-maintenance boundary is stale.
  EXPECT_FALSE(adapter.value().uncertain());
}

TEST_F(FileEngineAdapterTest, FailedCoreCompactionMustRecoverActualOldOrNewDiskBoundary) {
  const Config config{"a", {"b"}};
  for (bool disk_completed : {false, true}) {
    auto adapter = FileEngineAdapter::open(path / (disk_completed ? "written" : "not-written"));
    ASSERT_TRUE(adapter);
    ASSERT_TRUE(adapter.value().persist(plan(4, 2)));
    auto before = adapter.value().current();
    ASSERT_TRUE(before);
    auto node = initial(config);
    node.durable = projected_durable(before.value());
    auto [staged, request] = step(config, node, CompactCommitted{});
    ASSERT_TRUE(request.persist);
    if (disk_completed) {
      ASSERT_TRUE(adapter.value().checkpoint());
    }
    // Includes a completed disk checkpoint whose completion was lost.
    auto [failed, effects] = step(config, staged, PersistFailed{request.persist->token});
    ASSERT_TRUE(effects.halt);
    EXPECT_EQ(failed.durable, node.durable);
    auto disk = adapter.value().recover();
    ASSERT_TRUE(disk);
    EXPECT_EQ(disk.value().applied_state_payload, before.value().applied_state_payload);
    failed.durable = projected_durable(disk.value());  // Real recovery must precede Restart.
    auto [restarted, released] = step(config, failed, Restart{});
    EXPECT_FALSE(restarted.halted);
    EXPECT_FALSE(restarted.pending);
    EXPECT_TRUE(released.send_now.empty());
    EXPECT_TRUE(released.apply.empty());
    EXPECT_EQ(restarted.durable, disk_completed ? request.persist->target : node.durable);
    EXPECT_EQ(restarted.durable.commit, 2u);
    EXPECT_EQ(restarted.durable.applied, 2u);
    EXPECT_EQ(restarted.durable.log.size(), disk_completed ? 1u : 3u);
  }
}

TEST_F(FileEngineAdapterTest, IncrementalWritesAndCheckpointPreserveReceiptsAndUncommittedSuffix) {
  auto adapter = FileEngineAdapter::open(path);
  ASSERT_TRUE(adapter);
  DomainWriteDelta delta;
  delta.term = 2;
  delta.voted_for = "node-a";
  delta.commit = 2;
  for (const auto& event : events) delta.append.push_back(encode_domain_event(event).value());
  ASSERT_TRUE(adapter.value().persist_delta(delta));
  auto before = adapter.value().current_shared();
  ASSERT_TRUE(before);
  auto same = adapter.value().current_shared();
  ASSERT_TRUE(same);
  EXPECT_EQ(before.value(), same.value());
  EXPECT_EQ(before.value()->commit, 2u);
  ASSERT_TRUE(adapter.value().checkpoint());
  auto compacted = adapter.value().current_shared();
  ASSERT_TRUE(compacted);
  ASSERT_TRUE(compacted.value()->snapshot);
  EXPECT_EQ(compacted.value()->snapshot->index.value(), 2u);
  EXPECT_EQ(compacted.value()->suffix.size(), 1u);
  // A previously returned immutable view keeps its original boundary.
  EXPECT_FALSE(before.value()->snapshot);
  EXPECT_EQ(before.value()->suffix.size(), 3u);
  EXPECT_FALSE(adapter.value().persist_delta(delta));  // Stale boundary.
  EXPECT_FALSE(adapter.value().uncertain());
  DomainWriteDelta metadata;
  metadata.base_index = 2;
  metadata.base_term = 1;
  metadata.term = 3;
  metadata.voted_for = "node-b";
  metadata.keep_index = 3;
  metadata.commit = 2;
  ASSERT_TRUE(adapter.value().persist_delta(metadata));
  auto updated = adapter.value().current_shared();
  ASSERT_TRUE(updated);
  EXPECT_TRUE(updated.value()->suffix.shares_storage_with(compacted.value()->suffix));
  metadata.commit = 3;
  ASSERT_TRUE(adapter.value().persist_delta(metadata));
  auto recovered = adapter.value().recover();
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered.value().commit, 3u);
  EXPECT_EQ(recovered.value().term, 3u);
  auto replayed = replay_recovered_domain(recovered.value());
  ASSERT_TRUE(replayed);
  EXPECT_TRUE(state::resolve_receipt(replayed.value(), create()));
  EXPECT_EQ(replayed.value().receipts.at("d2").index, 3u);
}

TEST_F(FileEngineAdapterTest, InvalidIncrementalCommitCannotModifyOrPoisonStorage) {
  auto adapter = FileEngineAdapter::open(path);
  ASSERT_TRUE(adapter);
  DomainWriteDelta delta;
  delta.commit = 2;
  auto invalid = events[1];
  invalid.payload = state::DancerPayload{state::DancerId::parse("bad").value(),
                                         state::Position::from_mm(999999, 999999).value()};
  delta.append = {encode_domain_event(events[0]).value(), encode_domain_event(invalid).value()};
  EXPECT_FALSE(adapter.value().persist_delta(delta));
  EXPECT_FALSE(adapter.value().uncertain());
  auto original = adapter.value().current();
  ASSERT_TRUE(original);
  EXPECT_EQ(original.value().commit, 0u);
  EXPECT_TRUE(original.value().suffix.empty());
}

TEST_F(FileEngineAdapterTest, IncrementalMutationFailuresRequireRecoveryAtEveryPhase) {
  for (auto fault : {DomainWriteFault::AfterMetadata, DomainWriteFault::AfterTruncate,
                     DomainWriteFault::AfterAppend, DomainWriteFault::AfterCommit,
                     DomainWriteFault::AppendBeforeFlush, DomainWriteFault::AppendAfterFlush}) {
    auto adapter = FileEngineAdapter::open(path / std::to_string(static_cast<int>(fault)), true);
    ASSERT_TRUE(adapter);
    ASSERT_TRUE(adapter.value().persist(plan(1, 1)));
    DomainWriteDelta delta;
    delta.term = 2;
    delta.keep_index = 1;
    delta.commit = 3;
    for (std::size_t i = 1; i < events.size(); ++i) {
      auto event = events[i];
      event.term = state::Term::parse(2).value();
      delta.append.push_back(encode_domain_event(event).value());
    }
    ASSERT_TRUE(adapter.value().inject_next_fault_for_test(fault));
    EXPECT_FALSE(adapter.value().persist_delta(delta));
    EXPECT_TRUE(adapter.value().uncertain());
    EXPECT_FALSE(adapter.value().current_shared());
    EXPECT_FALSE(adapter.value().persist_delta(delta));
    auto recovered = adapter.value().recover();
    ASSERT_TRUE(recovered);
    if (recovered.value().commit < 3) {
      ASSERT_TRUE(adapter.value().persist_delta(delta));
    }
    auto final = adapter.value().current();
    ASSERT_TRUE(final);
    EXPECT_EQ(final.value().commit, 3u);
    EXPECT_TRUE(replay_recovered_domain(final.value()));
  }
}

TEST_F(FileEngineAdapterTest, CommittedStateAndReceiptsRecoverFromProductionFiles) {
  {
    auto adapter = FileEngineAdapter::open(path);
    ASSERT_TRUE(adapter);
    auto target = plan(7, 2);
    target.voted_for = "node-b";
    ASSERT_TRUE(adapter.value().persist(target));
    auto observed = adapter.value().current();
    ASSERT_TRUE(observed);
    EXPECT_EQ(observed.value().term, 7u);
    EXPECT_EQ(observed.value().voted_for, "node-b");
    EXPECT_EQ(observed.value().commit, 2u);
    EXPECT_EQ(observed.value().applied, 2u);
    EXPECT_EQ(observed.value().suffix.size(), 3u);
    EXPECT_TRUE(std::filesystem::exists(path / "wal.bin"));
    EXPECT_TRUE(std::filesystem::exists(path / "meta.bin"));
    EXPECT_FALSE(std::filesystem::exists(path / "raft-core-v1.bin"));
  }
  auto adapter = FileEngineAdapter::open(path);
  ASSERT_TRUE(adapter);
  auto observed = adapter.value().current();
  ASSERT_TRUE(observed);
  auto replayed = replay_recovered_domain(observed.value());
  ASSERT_TRUE(replayed);
  EXPECT_EQ(replayed.value().receipts.count("d2"), 0u);
  EXPECT_TRUE(state::resolve_receipt(replayed.value(), create()));
  auto target = plan(7);
  target.voted_for = "node-b";
  ASSERT_TRUE(adapter.value().persist(target));
  auto recovered = adapter.value().recover();
  ASSERT_TRUE(recovered);
  replayed = replay_recovered_domain(recovered.value());
  ASSERT_TRUE(replayed);
  EXPECT_EQ(replayed.value().receipts.at("d2").index, 3u);
}

TEST_F(FileEngineAdapterTest, InvalidPlansCannotWriteOrPoisonTheAdapter) {
  auto adapter = FileEngineAdapter::open(path);
  ASSERT_TRUE(adapter);
  auto original = plan(3, 1);
  original.voted_for = "node-a";
  ASSERT_TRUE(adapter.value().persist(original));
  auto bad = original;
  bad.term = 2;
  EXPECT_FALSE(adapter.value().persist(bad));
  bad = original;
  bad.voted_for = "node-b";
  EXPECT_FALSE(adapter.value().persist(bad));
  bad.voted_for.reset();
  EXPECT_FALSE(adapter.value().persist(bad));
  bad = original;
  bad.commit = 0;
  EXPECT_FALSE(adapter.value().persist(bad));
  bad = original;
  bad.commit = 4;
  EXPECT_FALSE(adapter.value().persist(bad));
  bad = original;
  bad.base_index = 1;
  EXPECT_FALSE(adapter.value().persist(bad));
  bad = original;
  bad.base_term = 1;
  EXPECT_FALSE(adapter.value().persist(bad));
  bad = original;
  bad.voted_for = "invalid node";
  bad.term = 4;
  EXPECT_FALSE(adapter.value().persist(bad));
  bad = original;
  bad.term = 9;
  {
    auto changed = bad.suffix[0];
    changed.command += ";unknown=field";
    bad.suffix.set(0, std::move(changed));
  }
  EXPECT_FALSE(adapter.value().persist(bad));
  bad = original;
  auto bad_entry = bad.suffix.back();
  bad_entry.command += ";unknown=field";
  bad.suffix.set(bad.suffix.size() - 1, std::move(bad_entry));  // Uncommitted data is also checked.
  EXPECT_FALSE(adapter.value().persist(bad));
  bad = original;
  auto rewritten = events[0];
  rewritten.term = state::Term::parse(2).value();
  bad.suffix.set(0, encode_domain_event(rewritten).value());
  bad.term = 9;
  EXPECT_FALSE(adapter.value().persist(bad));
  EXPECT_FALSE(adapter.value().uncertain());
  auto recovered = adapter.value().recover();
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered.value().term, original.term);
  EXPECT_EQ(recovered.value().voted_for, original.voted_for);
  EXPECT_EQ(recovered.value().suffix, original.suffix);
  EXPECT_EQ(recovered.value().commit, original.commit);
}

TEST_F(FileEngineAdapterTest, DomainValidationHappensBeforeEvenTermMetadataIsWritten) {
  auto adapter = FileEngineAdapter::open(path);
  ASSERT_TRUE(adapter);
  auto target = plan(9);
  auto invalid = events[2];
  // A second Create at index3 has a valid canonical envelope but cannot apply.
  invalid.type = events[0].type;
  invalid.payload = events[0].payload;
  target.suffix.set(2, encode_domain_event(invalid).value());
  EXPECT_FALSE(adapter.value().persist(target));
  EXPECT_FALSE(adapter.value().uncertain());
  auto recovered = adapter.value().recover();
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered.value().term, 1u);
  EXPECT_EQ(recovered.value().commit, 0u);
  EXPECT_TRUE(recovered.value().suffix.empty());
}

TEST_F(FileEngineAdapterTest, UncommittedConflictCanBeReplacedAndShortenedWithoutLosingCommit) {
  auto adapter = FileEngineAdapter::open(path);
  ASSERT_TRUE(adapter);
  ASSERT_TRUE(adapter.value().persist(plan(1, 1)));
  auto target = plan(2);
  for (std::size_t i = 1; i < events.size(); ++i) {
    auto replacement = events[i];
    replacement.term = state::Term::parse(2).value();
    target.suffix.set(i, encode_domain_event(replacement).value());
  }
  target.commit = 1;
  ASSERT_TRUE(adapter.value().persist(target));
  auto recovered = adapter.value().recover();
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered.value().suffix, target.suffix);
  EXPECT_EQ(recovered.value().commit, 1u);
  target.suffix.resize(1);
  ASSERT_TRUE(adapter.value().persist(target));
  recovered = adapter.value().recover();
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered.value().suffix.size(), 1u);
  EXPECT_EQ(recovered.value().commit, 1u);
}

TEST_F(FileEngineAdapterTest, CompactedSnapshotBoundaryAndRetryReceiptsSurviveCommit) {
  {
    state::StoreOptions options;
    options.commit_on_append = false;
    options.snapshot_every = 0;
    auto store = state::FileEngine::open(path, options);
    ASSERT_TRUE(store);
    for (const auto& event : events) ASSERT_TRUE(store.value().append_event(event));
    ASSERT_TRUE(store.value().commit_through(state::LogIndex::parse(2).value()));
    ASSERT_TRUE(store.value().checkpoint());
    ASSERT_TRUE(store.value().discard_compacted_prefix());
  }
  auto adapter = FileEngineAdapter::open(path);
  ASSERT_TRUE(adapter);
  DomainWritePlan target;
  target.base_index = 2;
  target.base_term = 1;
  target.term = 5;
  target.voted_for = "node-c";
  target.suffix = {encode_domain_event(events[2]).value()};
  target.commit = 3;
  ASSERT_TRUE(adapter.value().persist(target));
  auto observed = adapter.value().recover();
  ASSERT_TRUE(observed);
  ASSERT_TRUE(observed.value().snapshot);
  EXPECT_EQ(observed.value().snapshot->index.value(), 2u);
  EXPECT_EQ(observed.value().applied, 3u);
  auto replayed = replay_recovered_domain(observed.value());
  ASSERT_TRUE(replayed);
  EXPECT_TRUE(state::resolve_receipt(replayed.value(), create()));
  EXPECT_EQ(replayed.value().receipts.at("d2").index, 3u);
}

TEST_F(FileEngineAdapterTest, EveryWriteBoundaryFailsClosedAndRecoversActualDiskOutcome) {
  for (const auto fault :
       {DomainWriteFault::AfterMetadata, DomainWriteFault::AfterTruncate,
        DomainWriteFault::AfterAppend, DomainWriteFault::AfterCommit,
        DomainWriteFault::AppendBeforeFlush, DomainWriteFault::AppendAfterFlush}) {
    const auto directory = path / std::to_string(static_cast<int>(fault));
    auto adapter = FileEngineAdapter::open(directory, true);
    ASSERT_TRUE(adapter);
    ASSERT_TRUE(adapter.value().persist(plan(1, 1)));
    auto target = plan(2);
    target.voted_for = "node-b";
    for (std::size_t i = 1; i < events.size(); ++i) {
      auto event = events[i];
      event.term = state::Term::parse(2).value();
      target.suffix.set(i, encode_domain_event(event).value());
    }
    ASSERT_TRUE(adapter.value().inject_next_fault_for_test(fault));
    EXPECT_FALSE(adapter.value().persist(target));
    EXPECT_TRUE(adapter.value().uncertain());
    EXPECT_FALSE(adapter.value().current());
    EXPECT_FALSE(adapter.value().persist(target));
    auto recovered = adapter.value().recover();
    ASSERT_TRUE(recovered) << recovered.error().to_string();
    EXPECT_EQ(recovered.value().term, 2u);
    EXPECT_EQ(recovered.value().voted_for, "node-b");
    EXPECT_EQ(recovered.value().commit, fault == DomainWriteFault::AfterCommit ? 3u : 1u);
    const auto size =
        fault == DomainWriteFault::AfterMetadata || fault == DomainWriteFault::AfterCommit ? 3u
        : fault == DomainWriteFault::AfterAppend || fault == DomainWriteFault::AppendAfterFlush
            ? 2u
            : 1u;
    EXPECT_EQ(recovered.value().suffix.size(), size);
    EXPECT_TRUE(replay_recovered_domain(recovered.value()));
    ASSERT_TRUE(adapter.value().persist(target));
    recovered = adapter.value().recover();
    ASSERT_TRUE(recovered);
    EXPECT_EQ(recovered.value().suffix, target.suffix);
    EXPECT_EQ(recovered.value().commit, 3u);
  }
}

// A recover() on the same adapter resets its FileEngine internally. This test
// also destroys the writer and acquires a fresh production directory lease,
// exercising the on-disk metadata/WAL boundary without trusting cached state.
TEST_F(FileEngineAdapterTest, FreshOpenAfterInjectedWriteFailureObservesDiskNotStagedVote) {
  for (const auto fault :
       {DomainWriteFault::AfterMetadata, DomainWriteFault::AfterTruncate,
        DomainWriteFault::AfterAppend, DomainWriteFault::AfterCommit,
        DomainWriteFault::AppendBeforeFlush, DomainWriteFault::AppendAfterFlush}) {
    SCOPED_TRACE(static_cast<int>(fault));
    const auto directory = path / ("fresh-reopen-" + std::to_string(static_cast<int>(fault)));
    auto target = plan(2);
    target.voted_for = "node-b";
    for (std::size_t i = 1; i < events.size(); ++i) {
      auto event = events[i];
      event.term = state::Term::parse(2).value();
      target.suffix.set(i, encode_domain_event(event).value());
    }

    {
      auto writer = FileEngineAdapter::open(directory, true);
      ASSERT_TRUE(writer);
      ASSERT_TRUE(writer.value().persist(plan(1, 1)));
      ASSERT_TRUE(writer.value().inject_next_fault_for_test(fault));
      EXPECT_FALSE(writer.value().persist(target));
      EXPECT_TRUE(writer.value().uncertain());
      EXPECT_FALSE(writer.value().current_raft());
      EXPECT_FALSE(writer.value().persist(target));
    }  // Release the writer and its directory lease before opening a new adapter.

    {
      auto reopened = FileEngineAdapter::open(directory);
      ASSERT_TRUE(reopened) << reopened.error().to_string();
      auto disk = reopened.value().current_raft();
      ASSERT_TRUE(disk);
      EXPECT_EQ(disk.value().term, 2u);
      EXPECT_EQ(disk.value().voted_for, target.voted_for);
      const auto expected_commit = fault == DomainWriteFault::AfterCommit ? 3u : 1u;
      const auto expected_suffix_size =
          fault == DomainWriteFault::AfterMetadata || fault == DomainWriteFault::AfterCommit ? 3u
          : fault == DomainWriteFault::AfterAppend || fault == DomainWriteFault::AppendAfterFlush
              ? 2u
              : 1u;
      EXPECT_EQ(disk.value().commit, expected_commit);
      EXPECT_EQ(disk.value().applied, expected_commit);
      EXPECT_EQ(disk.value().log.size(), expected_suffix_size);
      auto recovered = reopened.value().current();
      ASSERT_TRUE(recovered);
      EXPECT_TRUE(replay_recovered_domain(recovered.value()));
      // A failed callback must not be confused with an absent disk write:
      // after metadata succeeds, the higher term and vote are already durable.
      ASSERT_TRUE(reopened.value().persist(target));
    }

    auto final_open = FileEngineAdapter::open(directory);
    ASSERT_TRUE(final_open);
    auto final_disk = final_open.value().current_raft();
    ASSERT_TRUE(final_disk);
    EXPECT_EQ(final_disk.value().term, target.term);
    EXPECT_EQ(final_disk.value().voted_for, target.voted_for);
    EXPECT_EQ(final_disk.value().commit, target.commit);
    EXPECT_EQ(final_disk.value().log, target.suffix);
  }
}

TEST_F(FileEngineAdapterTest, RealMetadataErrorBlocksCurrentAndRecoveryUntilDiskIsRepaired) {
  auto adapter = FileEngineAdapter::open(path);
  ASSERT_TRUE(adapter);
  ASSERT_TRUE(adapter.value().persist(plan(1, 1)));
  std::filesystem::rename(path / "meta.bin", path / "saved-meta.bin");
  std::filesystem::create_directory(path / "meta.bin");
  EXPECT_FALSE(adapter.value().persist(plan(2)));
  EXPECT_TRUE(adapter.value().uncertain());
  EXPECT_FALSE(adapter.value().current());
  EXPECT_FALSE(adapter.value().recover());
  EXPECT_TRUE(adapter.value().uncertain());
  std::filesystem::remove(path / "meta.bin");
  std::filesystem::rename(path / "saved-meta.bin", path / "meta.bin");
  auto recovered = adapter.value().recover();
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered.value().commit, 1u);
  EXPECT_EQ(recovered.value().term, 1u);
}

TEST_F(FileEngineAdapterTest, FaultInjectionIsExplicitlyTestOnly) {
  auto adapter = FileEngineAdapter::open(path);
  ASSERT_TRUE(adapter);
  EXPECT_FALSE(adapter.value().inject_next_fault_for_test(DomainWriteFault::AfterCommit));
  ASSERT_TRUE(adapter.value().persist(plan()));
}

TEST_F(FileEngineAdapterTest, CompactedAppendFailureRecoversAtItsAbsoluteSnapshotBoundary) {
  auto base = state::replay({events[0], events[1]});
  ASSERT_TRUE(base);
  storage::Snapshot snapshot{base.value().last_applied,
                             base.value().term,
                             state::state_hash(base.value()),
                             {},
                             state::snapshot_payload(base.value())};
  {
    state::StoreOptions options;
    options.commit_on_append = false;
    auto store = state::FileEngine::open(path, options);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().install_snapshot(snapshot));
  }
  auto adapter = FileEngineAdapter::open(path, true);
  ASSERT_TRUE(adapter);
  auto event = events[2];
  event.term = state::Term::parse(2).value();
  DomainWritePlan target;
  target.base_index = 2;
  target.base_term = 1;
  target.term = 2;
  target.commit = 3;
  target.suffix = {encode_domain_event(event).value()};
  ASSERT_TRUE(adapter.value().inject_next_fault_for_test(DomainWriteFault::AppendAfterFlush));
  EXPECT_FALSE(adapter.value().persist(target));
  EXPECT_FALSE(adapter.value().current());
  auto recovered = adapter.value().recover();
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered.value().snapshot->index.value(), 2u);
  EXPECT_EQ(recovered.value().commit, 2u);
  EXPECT_EQ(recovered.value().suffix, target.suffix);
  ASSERT_TRUE(adapter.value().persist(target));
  recovered = adapter.value().recover();
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered.value().commit, 3u);
}

TEST_F(FileEngineAdapterTest, MetadataOnlyTermAndVoteChangesDoNotInventApplicationProgress) {
  auto adapter = FileEngineAdapter::open(path);
  ASSERT_TRUE(adapter);
  DomainWritePlan target;
  target.term = 4;
  target.voted_for = "node-a";
  ASSERT_TRUE(adapter.value().persist(target));
  auto recovered = adapter.value().recover();
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered.value().term, 4u);
  EXPECT_EQ(recovered.value().voted_for, "node-a");
  EXPECT_EQ(recovered.value().applied, 0u);
  EXPECT_TRUE(recovered.value().suffix.empty());
  target.term = 5;
  target.voted_for.reset();
  ASSERT_TRUE(adapter.value().persist(target));
  recovered = adapter.value().recover();
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered.value().term, 5u);
  EXPECT_FALSE(recovered.value().voted_for);
  EXPECT_EQ(recovered.value().commit, 0u);
}

TEST_F(FileEngineAdapterTest, NoOpAdvancesCommitWithoutInventingACommandReceipt) {
  auto adapter = FileEngineAdapter::open(path);
  ASSERT_TRUE(adapter);
  auto noop = events[0];
  noop.type = state::EventType::NoOp;
  noop.id = state::EventId::parse("noop-1").value();
  noop.command_id = state::CommandId::parse("noop-1").value();
  noop.term = state::Term::parse(2).value();
  auto encoded = encode_domain_event(noop);
  ASSERT_TRUE(encoded);
  DomainWritePlan target;
  target.term = 2;
  target.commit = 1;
  target.suffix = {encoded.value()};
  ASSERT_TRUE(adapter.value().persist(target));
  auto recovered = adapter.value().recover();
  ASSERT_TRUE(recovered);
  auto replayed = replay_recovered_domain(recovered.value());
  ASSERT_TRUE(replayed);
  EXPECT_EQ(replayed.value().last_applied.value(), 1u);
  EXPECT_FALSE(replayed.value().created);
  EXPECT_TRUE(replayed.value().receipts.empty());
}

TEST_F(FileEngineAdapterTest, FailedMetadataAckCannotAllowConflictingVoteAfterFreshOpen) {
  // Both write APIs persist the higher-term vote before the injected failure
  // reports an unsuccessful callback. A fresh owner must trust the disk vote,
  // not the caller's stale belief that the whole write was rolled back.
  for (const bool incremental : {false, true}) {
    SCOPED_TRACE(incremental ? "delta" : "plan");
    const auto directory = path / (incremental ? "delta-vote" : "plan-vote");
    DomainWritePlan initial_vote;
    initial_vote.term = 4;
    initial_vote.voted_for = "node-a";
    DomainWritePlan higher_vote;
    higher_vote.term = 5;
    higher_vote.voted_for = "node-b";

    {
      auto writer = FileEngineAdapter::open(directory, true);
      ASSERT_TRUE(writer);
      ASSERT_TRUE(writer.value().persist(initial_vote));
      ASSERT_TRUE(writer.value().inject_next_fault_for_test(DomainWriteFault::AfterMetadata));
      if (incremental) {
        DomainWriteDelta delta;
        delta.term = higher_vote.term;
        delta.voted_for = higher_vote.voted_for;
        EXPECT_FALSE(writer.value().persist_delta(delta));
      } else {
        EXPECT_FALSE(writer.value().persist(higher_vote));
      }
      EXPECT_TRUE(writer.value().uncertain());
      EXPECT_FALSE(writer.value().current_raft());
    }

    {
      auto reopened = FileEngineAdapter::open(directory);
      ASSERT_TRUE(reopened) << reopened.error().to_string();
      auto durable = reopened.value().current_raft();
      ASSERT_TRUE(durable);
      EXPECT_EQ(durable.value().term, 5u);
      EXPECT_EQ(durable.value().voted_for, higher_vote.voted_for);
      EXPECT_EQ(durable.value().commit, 0u);
      EXPECT_EQ(durable.value().applied, 0u);
      EXPECT_TRUE(durable.value().log.empty());

      auto conflicting = higher_vote;
      conflicting.voted_for = "node-c";
      EXPECT_FALSE(reopened.value().persist(conflicting));
      DomainWriteDelta conflicting_delta;
      conflicting_delta.term = 5;
      conflicting_delta.voted_for = "node-c";
      EXPECT_FALSE(reopened.value().persist_delta(conflicting_delta));
      auto cleared = higher_vote;
      cleared.voted_for.reset();
      EXPECT_FALSE(reopened.value().persist(cleared));
      EXPECT_FALSE(reopened.value().uncertain());
      auto after_rejections = reopened.value().current_raft();
      ASSERT_TRUE(after_rejections);
      EXPECT_EQ(after_rejections.value(), durable.value());
    }

    {
      auto reopened = FileEngineAdapter::open(directory);
      ASSERT_TRUE(reopened);
      auto persisted = reopened.value().current_raft();
      ASSERT_TRUE(persisted);
      EXPECT_EQ(persisted.value().term, 5u);
      EXPECT_EQ(persisted.value().voted_for, higher_vote.voted_for);
      auto next_term = higher_vote;
      next_term.term = 6;
      next_term.voted_for = "node-c";
      ASSERT_TRUE(reopened.value().persist(next_term));
    }

    auto final_open = FileEngineAdapter::open(directory);
    ASSERT_TRUE(final_open);
    auto final_vote = final_open.value().current_raft();
    ASSERT_TRUE(final_vote);
    EXPECT_EQ(final_vote.value().term, 6u);
    EXPECT_EQ(final_vote.value().voted_for, std::optional<std::string>{"node-c"});
    EXPECT_EQ(final_vote.value().commit, 0u);
    EXPECT_TRUE(final_vote.value().log.empty());
  }
}

}  // namespace
}  // namespace choreoos::raft
