#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>

#include "choreoos/raft/domain_bridge.hpp"

namespace choreoos::raft {
namespace {

std::filesystem::path fresh(const char* name) {
  const auto path = std::filesystem::temp_directory_path() / name;
  std::filesystem::remove_all(path);
  return path;
}

state::Command create_show() {
  using namespace choreoos::state;
  auto id = ChoreographyId::parse("opening").value();
  return Command{CommandId::parse("cmd-create").value(),
                 id,
                 CommandType::CreateChoreography,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(0).value(),
                 CreateChoreographyPayload{id, StageBounds::from_mm(20000, 12000).value(),
                                           OverlapPolicy::Forbidden, 5000}};
}

state::Command add_dancer() {
  using namespace choreoos::state;
  return Command{
      CommandId::parse("cmd-alice").value(),
      ChoreographyId::parse("opening").value(),
      CommandType::AddDancer,
      kCurrentSchemaVersion,
      MusicalTick::from_count(1).value(),
      DancerPayload{DancerId::parse("alice").value(), Position::from_mm(2000, 1500).value()}};
}

TEST(RaftDomainBridgeTest, ProductionWalRoundTripsAndReplaysAfterDiskRestart) {
  const auto path = fresh("choreoos-raft-domain-restart");
  state::StoreOptions options;
  options.snapshot_every = 0;
  {
    auto store = state::FileEngine::open(path, options);
    ASSERT_TRUE(store);
    ASSERT_TRUE(store.value().submit(create_show()));
    ASSERT_TRUE(store.value().submit(add_dancer()));
    auto projected = audit_domain_store(store.value());
    ASSERT_TRUE(projected) << projected.error().to_string();
    EXPECT_EQ(projected.value().raft.commit, 2u);
    EXPECT_EQ(projected.value().raft.applied, 2u);
    EXPECT_EQ(projected.value().raft.log.size(), 2u);
    EXPECT_EQ(projected.value().applied_state_hash,
              state::state_hash(store.value().engine().state()));
    for (std::size_t i = 0; i < projected.value().raft.log.size(); ++i) {
      auto decoded = decode_domain_entry(projected.value().raft.log[i], i + 1);
      ASSERT_TRUE(decoded);
      EXPECT_EQ(state::canonical_event(decoded.value()),
                state::canonical_event(store.value().log_events()[i]));
    }
  }
  auto restored = state::FileEngine::open(path, options);
  ASSERT_TRUE(restored);
  auto projected = audit_domain_store(restored.value());
  ASSERT_TRUE(projected) << projected.error().to_string();
  EXPECT_EQ(projected.value().raft.log.size(), 2u);
  EXPECT_EQ(projected.value().applied_state_hash,
            state::state_hash(restored.value().engine().state()));
  std::filesystem::remove_all(path);
}

TEST(RaftDomainBridgeTest, UncommittedWalSuffixDoesNotAffectCommittedReplay) {
  const auto source_path = fresh("choreoos-raft-domain-source");
  const auto follower_path = fresh("choreoos-raft-domain-follower");
  auto source = state::FileEngine::open(source_path);
  ASSERT_TRUE(source);
  ASSERT_TRUE(source.value().submit(create_show()));
  ASSERT_TRUE(source.value().submit(add_dancer()));
  state::StoreOptions follower_options;
  follower_options.commit_on_append = false;
  follower_options.snapshot_every = 0;
  auto follower = state::FileEngine::open(follower_path, follower_options);
  ASSERT_TRUE(follower);
  ASSERT_TRUE(follower.value().append_event(source.value().log_events()[0]));
  ASSERT_TRUE(follower.value().append_event(source.value().log_events()[1]));
  ASSERT_TRUE(follower.value().commit_through(state::LogIndex::parse(1).value()));
  auto projected = audit_domain_store(follower.value());
  ASSERT_TRUE(projected) << projected.error().to_string();
  EXPECT_EQ(projected.value().raft.commit, 1u);
  EXPECT_EQ(projected.value().raft.applied, 1u);
  EXPECT_EQ(projected.value().raft.log.size(), 2u);
  auto committed = replay_domain_committed(projected.value().raft);
  ASSERT_TRUE(committed);
  EXPECT_TRUE(committed.value().created);
  EXPECT_TRUE(committed.value().dancers.empty());

  auto reopened = state::FileEngine::open(follower_path, follower_options);
  ASSERT_TRUE(reopened);
  auto recovered = audit_domain_store(reopened.value());
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered.value().raft.commit, 1u);
  EXPECT_EQ(recovered.value().raft.log.size(), 2u);
  EXPECT_EQ(projected.value().applied_state_hash, recovered.value().applied_state_hash);
  std::filesystem::remove_all(source_path);
  std::filesystem::remove_all(follower_path);
}

TEST(RaftDomainBridgeTest, NonCanonicalOrInconsistentEntryIsRejected) {
  const auto path = fresh("choreoos-raft-domain-invalid");
  auto store = state::FileEngine::open(path);
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().submit(create_show()));
  auto entry = encode_domain_event(store.value().log_events()[0]);
  ASSERT_TRUE(entry);
  EXPECT_FALSE(decode_domain_entry(entry.value(), 2));
  auto wrong_term = entry.value();
  ++wrong_term.term;
  EXPECT_FALSE(decode_domain_entry(wrong_term, 1));
  auto extra_field = entry.value();
  extra_field.command += ";ignored=anything";
  EXPECT_FALSE(decode_domain_entry(extra_field, 1));
  auto noncanonical = entry.value();
  noncanonical.command = "junk";
  EXPECT_FALSE(decode_domain_entry(noncanonical, 1));
  auto projected = audit_domain_store(store.value());
  ASSERT_TRUE(projected);
  auto bad = projected.value().raft;
  bad.commit = 2;
  EXPECT_FALSE(replay_domain_committed(bad));
  bad = projected.value().raft;
  bad.log[0].term = 10;
  EXPECT_FALSE(replay_domain_committed(bad));
  std::filesystem::remove_all(path);
}

TEST(RaftDomainBridgeTest, MetadataCorruptionNeverFallsBackToDefaultVote) {
  const auto path = fresh("choreoos-raft-domain-meta");
  auto store = state::FileEngine::open(path);
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().submit(create_show()));
  ASSERT_TRUE(audit_domain_store(store.value()));
  {
    std::fstream file{path / "meta.bin", std::ios::in | std::ios::out | std::ios::binary};
    ASSERT_TRUE(file);
    file.seekp(18);
    file.put('Z');
  }
  auto audit = audit_domain_store(store.value());
  EXPECT_FALSE(audit);
  std::filesystem::remove_all(path);
}

TEST(RaftDomainBridgeTest, RefusesCompactedLogsRatherThanGuessingSnapshotOffsets) {
  const auto path = fresh("choreoos-raft-domain-compaction");
  state::StoreOptions options;
  options.snapshot_every = 2;
  auto store = state::FileEngine::open(path, options);
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().submit(create_show()));
  ASSERT_TRUE(store.value().submit(add_dancer()));
  ASSERT_TRUE(store.value().checkpoint());
  ASSERT_TRUE(store.value().discard_compacted_prefix());
  auto audit = audit_domain_store(store.value());
  EXPECT_FALSE(audit);
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace choreoos::raft
