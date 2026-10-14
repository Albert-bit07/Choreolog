#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "choreoos/protocol/codec.hpp"
#include "choreoos/replication/replica.hpp"
#include "choreoos/replication/simulator.hpp"
#include "choreoos/state/store.hpp"

namespace choreoos::replication {
namespace {

std::filesystem::path safety_directory(const char* suffix) {
  auto path =
      std::filesystem::temp_directory_path() / (std::string("choreoos-raft-safety-") + suffix);
  std::filesystem::remove_all(path);
  return path;
}

ReplicaConfig safety_config(const std::filesystem::path& path) {
  ReplicaConfig config{"node-a", "", {"node-b", "node-c"}, path};
  config.elections = true;
  config.election_timeout_min = 1;
  config.election_timeout_max = 1;
  return config;
}

template <class Message>
choreoos::protocol::Frame wire(choreoos::protocol::MessageType type, const Message& message) {
  choreoos::protocol::Frame frame;
  frame.type = type;
  frame.correlation = 7;
  frame.payload = choreoos::protocol::encode(message).value();
  return frame;
}

choreoos::state::Command safety_command() {
  using namespace choreoos::state;
  const auto show = ChoreographyId::parse("opening").value();
  return {CommandId::parse("create").value(),
          show,
          CommandType::CreateChoreography,
          kCurrentSchemaVersion,
          MusicalTick::from_count(0).value(),
          CreateChoreographyPayload{show, StageBounds::from_mm(20000, 12000).value(),
                                    OverlapPolicy::Forbidden, 5000}};
}

choreoos::state::Event safety_noop(std::uint64_t index) {
  using namespace choreoos::state;
  auto event = propose(ChoreographyState{}, safety_command()).value();
  event.type = EventType::NoOp;
  event.index = LogIndex::parse(index).value();
  event.id = EventId::parse("event-" + std::to_string(index)).value();
  event.command_id = CommandId::parse("command-" + std::to_string(index)).value();
  return event;
}

TEST(ConsensusHardeningTest, FailedCommitPublishesNeitherStateNorReceipts) {
  using namespace choreoos::state;
  const auto path = safety_directory("commit-publication");
  StoreOptions options;
  options.commit_on_append = false;
  auto store = FileEngine::open(path, options);
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().append_event(propose(ChoreographyState{}, safety_command()).value()));
  const auto before = state_hash(store.value().engine().state());
  ASSERT_TRUE(std::filesystem::create_directory(path / "meta.bin.tmp"));
  EXPECT_FALSE(store.value().commit_through(LogIndex::parse(1).value()));
  EXPECT_EQ(store.value().commit_index().value(), 0u);
  EXPECT_EQ(state_hash(store.value().engine().state()), before);
  EXPECT_TRUE(store.value().engine().state().receipts.empty());
  auto reopened = FileEngine::open(path, options);
  ASSERT_TRUE(reopened);
  EXPECT_EQ(state_hash(reopened.value().engine().state()), before);
  std::filesystem::remove_all(path);
}

TEST(ConsensusHardeningTest, InvalidCommitBatchDoesNotPublishItsValidPrefix) {
  using namespace choreoos::state;
  const auto path = safety_directory("commit-gap");
  auto replica = Replica::open(safety_config(path));
  ASSERT_TRUE(replica);
  ASSERT_TRUE(replica.value().store().append_event(safety_noop(1)));
  EXPECT_FALSE(replica.value().store().commit_through(LogIndex::parse(2).value()));
  EXPECT_EQ(replica.value().store().engine().state().last_applied.value(), 0u);
  EXPECT_TRUE(replica.value().store().engine().events().empty());
  std::filesystem::remove_all(path);
}

TEST(ConsensusHardeningTest, FailedSubmitDoesNotExposeCommittedState) {
  using namespace choreoos::state;
  const auto path = safety_directory("submit-publication");
  auto store = FileEngine::open(path);
  ASSERT_TRUE(store);
  ASSERT_TRUE(std::filesystem::create_directory(path / "meta.bin.tmp"));
  EXPECT_FALSE(store.value().submit(safety_command()));
  EXPECT_FALSE(store.value().engine().state().created);
  EXPECT_TRUE(store.value().engine().state().receipts.empty());
  EXPECT_EQ(store.value().commit_index().value(), 0u);
  std::filesystem::remove_all(path);
}

TEST(ConsensusHardeningTest, SubmitAndRecoveryPreserveAHigherTermAndVote) {
  using namespace choreoos::state;
  const auto path = safety_directory("preserve-term");
  auto store = FileEngine::open(path);
  ASSERT_TRUE(store);
  ASSERT_TRUE(
      store.value().persist_consensus(Term::parse(7).value(), NodeId::parse("node-b").value()));
  ASSERT_TRUE(store.value().submit(safety_command()));
  auto reopened = FileEngine::open(path);
  ASSERT_TRUE(reopened);
  const auto metadata = reopened.value().consensus_metadata();
  ASSERT_TRUE(metadata);
  EXPECT_EQ(metadata.value().term.value(), 7u);
  ASSERT_TRUE(metadata.value().voted_for);
  EXPECT_EQ(metadata.value().voted_for->value(), "node-b");
  EXPECT_EQ(metadata.value().commit_index.value(), 1u);
  std::filesystem::remove_all(path);
}

TEST(ConsensusHardeningTest, HeartbeatCannotCommitAnUnmatchedLocalSuffix) {
  const auto path = safety_directory("unmatched-suffix");
  auto replica = Replica::open(safety_config(path));
  ASSERT_TRUE(replica);
  ASSERT_TRUE(replica.value().store().append_event(safety_noop(1)));
  ASSERT_TRUE(replica.value().store().append_event(safety_noop(2)));
  const auto heartbeat = wire(choreoos::protocol::MessageType::AppendEntries,
                              choreoos::protocol::AppendEntries{1, "node-b", 1, 1, {}, 2});
  replica.value().handle(heartbeat, "node-b");
  EXPECT_EQ(replica.value().store().commit_index().value(), 1u);
  EXPECT_EQ(replica.value().store().engine().state().last_applied.value(), 1u);
  std::filesystem::remove_all(path);
}

TEST(ConsensusHardeningTest, InvalidAppendEnvelopesCannotMutateTheWal) {
  const auto path = safety_directory("append-envelope");
  auto replica = Replica::open(safety_config(path));
  ASSERT_TRUE(replica);
  auto future = safety_noop(1);
  future.term = choreoos::state::Term::parse(2).value();
  const std::vector<choreoos::protocol::AppendEntries> invalid{
      {1, "node-b", 0, 0, {safety_noop(2)}, 0},
      {1, "node-b", 0, 0, {future}, 0},
      {2, "node-b", 0, 0, {future, safety_noop(2)}, 0},
      {1, "node-b", 0, 1, {}, 0}};
  for (const auto& request : invalid) {
    replica.value().handle(wire(choreoos::protocol::MessageType::AppendEntries, request), "node-b");
    EXPECT_EQ(replica.value().store().last_log_index(), 0u);
    EXPECT_EQ(replica.value().status().term, 1u);
    EXPECT_FALSE(replica.value().halted());
  }
  std::filesystem::remove_all(path);
}

TEST(ConsensusHardeningTest, InvalidSnapshotIsRejectedButStorageFailuresHalt) {
  using namespace choreoos::state;
  Engine engine;
  ASSERT_TRUE(engine.apply_committed(safety_noop(1)));
  const auto payload = snapshot_payload(engine.state());
  for (const auto* blocked : {"meta.bin.tmp", "wal.bin.tmp"}) {
    const auto path = safety_directory(blocked);
    auto replica = Replica::open(safety_config(path));
    ASSERT_TRUE(replica);
    std::size_t sends = 0;
    replica.value().set_sender(
        [&](const std::string&, const choreoos::protocol::Frame&) { ++sends; });
    auto request = choreoos::protocol::InstallSnapshot{
        1, "node-b", 1, 1, state_hash(engine.state()), "invalid-state", 0, false};
    replica.value().handle(wire(choreoos::protocol::MessageType::InstallSnapshot, request),
                           "node-b");
    EXPECT_FALSE(replica.value().halted());
    EXPECT_EQ(sends, 1u);
    ASSERT_TRUE(std::filesystem::create_directory(path / blocked));
    request.payload = payload;
    replica.value().handle(wire(choreoos::protocol::MessageType::InstallSnapshot, request),
                           "node-b");
    EXPECT_TRUE(replica.value().halted()) << blocked;
    EXPECT_EQ(sends, 1u);  // No reply after a potentially durable partial install.
    EXPECT_EQ(replica.value().store().commit_index().value(), 0u);
    std::filesystem::remove_all(path / blocked);
    auto reopened = Replica::open(safety_config(path));
    ASSERT_TRUE(reopened) << blocked;
    const auto expected = std::string(blocked) == "wal.bin.tmp" ? 1u : 0u;
    EXPECT_EQ(reopened.value().store().commit_index().value(), expected);
    std::filesystem::remove_all(path);
  }
}

TEST(ConsensusHardeningTest, AppendRepliesMustMatchTheOutstandingRequestAndRange) {
  using namespace choreoos::protocol;
  const auto path = safety_directory("append-probes");
  ReplicaConfig config{"node-a", "node-a", {"node-b", "node-c"}, path};
  auto replica = Replica::open(config);
  ASSERT_TRUE(replica);
  std::vector<Frame> sent;
  replica.value().set_sender([&](const std::string& peer, const Frame& frame) {
    if (peer == "node-b") sent.push_back(frame);
  });
  ASSERT_TRUE(replica.value().store().append_event(safety_noop(1)));
  replica.value().heartbeat();
  ASSERT_EQ(sent.size(), 1u);
  const auto first = sent.back();
  replica.value().heartbeat();
  ASSERT_EQ(sent.size(), 2u);
  EXPECT_EQ(sent.back().correlation, first.correlation);
  EXPECT_EQ(sent.back().payload, first.payload);
  ASSERT_TRUE(replica.value().store().append_event(safety_noop(2)));

  auto reply =
      wire(MessageType::AppendEntriesResponse, AppendEntriesResponse{1, "node-b", true, 2, 2});
  reply.correlation = first.correlation;
  replica.value().handle(reply, "node-b");
  EXPECT_EQ(replica.value().store().commit_index().value(), 0u);
  reply.payload = encode(AppendEntriesResponse{1, "node-b", true, 1, 2}).value();
  replica.value().handle(reply, "node-b");
  EXPECT_EQ(replica.value().store().commit_index().value(), 1u);
  const auto second = sent.back();
  EXPECT_NE(second.correlation, first.correlation);

  // A duplicate old success cannot acknowledge entries in the next request.
  reply.payload = encode(AppendEntriesResponse{1, "node-b", true, 2, 2}).value();
  replica.value().handle(reply, "node-b");
  EXPECT_EQ(replica.value().store().commit_index().value(), 1u);
  const auto before = sent.size();
  reply.payload = encode(AppendEntriesResponse{1, "node-b", false, 0, 0}).value();
  replica.value().handle(reply, "node-b");
  EXPECT_EQ(sent.size(), before);  // Nor may an old failure rewind progress.
  reply.correlation = second.correlation;
  reply.payload = encode(AppendEntriesResponse{1, "node-b", true, 2, 2}).value();
  replica.value().handle(reply, "node-b");
  EXPECT_EQ(replica.value().store().commit_index().value(), 2u);
  std::filesystem::remove_all(path);
}

TEST(ConsensusHardeningTest, SnapshotRepliesMustMatchTheOutstandingChunk) {
  using namespace choreoos::protocol;
  using namespace choreoos::state;
  const auto path = safety_directory("snapshot-probes");
  ReplicaConfig config{"node-a", "node-a", {"node-b", "node-c"}, path};
  config.max_snapshot_chunk_bytes = 64;
  auto replica = Replica::open(config);
  ASSERT_TRUE(replica);
  ASSERT_TRUE(replica.value().store().append_event(safety_noop(1)));
  ASSERT_TRUE(replica.value().store().append_event(safety_noop(2)));
  ASSERT_TRUE(replica.value().store().commit_through(LogIndex::parse(2).value()));
  ASSERT_TRUE(replica.value().store().checkpoint());
  ASSERT_TRUE(replica.value().store().discard_compacted_prefix());
  std::vector<Frame> sent;
  replica.value().set_sender([&](const std::string& peer, const Frame& frame) {
    if (peer == "node-b") sent.push_back(frame);
  });
  replica.value().heartbeat();
  ASSERT_EQ(sent.size(), 1u);
  const auto first = sent.back();
  ASSERT_EQ(first.type, MessageType::InstallSnapshot);
  const auto chunk = decode_install_snapshot(first.payload);
  ASSERT_TRUE(chunk);
  ASSERT_TRUE(chunk.value().more);
  auto reply = wire(MessageType::InstallSnapshotResponse,
                    InstallSnapshotResponse{1, "node-b", true, UINT64_MAX});
  reply.correlation = first.correlation;
  replica.value().handle(reply, "node-b");
  EXPECT_EQ(sent.size(), 1u);  // A final-offset claim cannot skip unsent chunks.
  reply.payload =
      encode(InstallSnapshotResponse{1, "node-b", true, chunk.value().payload.size()}).value();
  replica.value().handle(reply, "node-b");
  ASSERT_EQ(sent.size(), 2u);
  const auto second = sent.back();
  replica.value().handle(reply, "node-b");  // Duplicate prior chunk response.
  EXPECT_EQ(sent.size(), 2u);
  replica.value().heartbeat();
  ASSERT_EQ(sent.size(), 3u);
  EXPECT_EQ(sent.back().correlation, second.correlation);
  EXPECT_EQ(sent.back().payload, second.payload);
  // Correct acknowledgements still drain the transfer into ordinary appends.
  for (int count = 0; count < 100 && sent.back().type == MessageType::InstallSnapshot; ++count) {
    const auto frame = sent.back();
    const auto next = decode_install_snapshot(frame.payload);
    ASSERT_TRUE(next);
    reply.correlation = frame.correlation;
    reply.payload =
        encode(InstallSnapshotResponse{1, "node-b", true,
                                       next.value().offset + next.value().payload.size()})
            .value();
    replica.value().handle(reply, "node-b");
  }
  EXPECT_EQ(sent.back().type, MessageType::AppendEntries);
  std::filesystem::remove_all(path);
}

TEST(ConsensusHardeningTest, RepliesDelayedBeyondAHeartbeatStillConverge) {
  const auto path = safety_directory("delayed-replies");
  auto leader = Replica::open({"node-a", "node-a", {"node-b", "node-c"}, path / "a"});
  auto b = Replica::open({"node-b", "node-a", {"node-a", "node-c"}, path / "b"});
  auto c = Replica::open({"node-c", "node-a", {"node-a", "node-b"}, path / "c"});
  ASSERT_TRUE(leader);
  ASSERT_TRUE(b);
  ASSERT_TRUE(c);
  SimulatedNetwork net{23};
  net.attach(leader.value());
  net.attach(b.value());
  net.attach(c.value());
  net.enable_faults();
  net.delay(choreoos::protocol::MessageType::AppendEntriesResponse, "node-b", "node-a", 5);
  net.delay(choreoos::protocol::MessageType::AppendEntriesResponse, "node-c", "node-a", 5);
  ASSERT_TRUE(leader.value().enqueue(safety_command()));
  for (int round = 0; round < 40; ++round) {
    leader.value().heartbeat();
    net.advance();
  }
  EXPECT_EQ(leader.value().store().commit_index().value(), 1u);
  EXPECT_EQ(b.value().status().state_hash, leader.value().status().state_hash);
  EXPECT_EQ(c.value().status().state_hash, leader.value().status().state_hash);
  std::filesystem::remove_all(path);
}

TEST(ConsensusHardeningTest, MetadataReadErrorCannotBecomeInitialTermAndVote) {
  const auto path = safety_directory("metadata");
  auto store = choreoos::state::FileEngine::open(path);
  ASSERT_TRUE(store);
  ASSERT_TRUE(store.value().consensus_metadata());
  {
    std::ofstream out(path / "meta.bin", std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out << "invalid-metadata";
  }
  EXPECT_FALSE(store.value().consensus_metadata());
  auto resumed = Replica::open(safety_config(path));
  EXPECT_FALSE(resumed);
  std::filesystem::remove_all(path);
}

TEST(ConsensusHardeningTest, HigherTermWriteFailureHaltsAndPreventsAcknowledgements) {
  const auto path = safety_directory("term-write");
  auto replica = Replica::open(safety_config(path));
  ASSERT_TRUE(replica);
  std::size_t sends = 0;
  replica.value().set_sender(
      [&](const std::string&, const choreoos::protocol::Frame&) { ++sends; });
  // Force the atomic metadata write to fail without changing the old term.
  ASSERT_TRUE(std::filesystem::create_directory(path / "meta.bin.tmp"));
  const auto request = wire(choreoos::protocol::MessageType::RequestVote,
                            choreoos::protocol::RequestVote{7, "node-b", 0, 0});
  replica.value().handle(request, "node-b");
  EXPECT_TRUE(replica.value().halted());
  EXPECT_FALSE(replica.value().is_leader());
  EXPECT_EQ(replica.value().status().role, "halted");
  EXPECT_EQ(sends, 0u);
  replica.value().tick();
  replica.value().tick();
  EXPECT_EQ(sends, 0u);
  EXPECT_EQ(replica.value().status().term, 1u);
  // A halted replica must be reconstructed from disk, not resumed by a tick.
  replica.value().handle(request, "node-b");
  EXPECT_EQ(sends, 0u);
  std::filesystem::remove_all(path);
}

TEST(ConsensusHardeningTest, ForgedSessionIdentityCannotChangeTermOrCastVote) {
  const auto path = safety_directory("identity");
  auto replica = Replica::open(safety_config(path));
  ASSERT_TRUE(replica);
  const auto request = wire(choreoos::protocol::MessageType::RequestVote,
                            choreoos::protocol::RequestVote{999, "node-c", 0, 0});
  replica.value().handle(request, "node-b");
  EXPECT_EQ(replica.value().status().term, 1u);
  EXPECT_FALSE(replica.value().halted());

  // A member ID not in the fixed configuration never supplies quorum.
  replica.value().tick();
  EXPECT_EQ(replica.value().status().role, "candidate");
  const auto term = replica.value().status().term;
  const auto fake_vote = wire(choreoos::protocol::MessageType::RequestVoteResponse,
                              choreoos::protocol::RequestVoteResponse{term, "stranger", true});
  replica.value().handle(fake_vote);
  EXPECT_EQ(replica.value().status().term, term);
  EXPECT_EQ(replica.value().status().role, "candidate");
  EXPECT_FALSE(replica.value().is_leader());

  // Known members with authenticated matching identities still work.
  const auto real_vote = wire(choreoos::protocol::MessageType::RequestVoteResponse,
                              choreoos::protocol::RequestVoteResponse{term, "node-b", true});
  replica.value().handle(real_vote, "node-b");
  EXPECT_TRUE(replica.value().is_leader());
  std::filesystem::remove_all(path);
}

TEST(ConsensusHardeningTest, ForgedAppendLeaderCannotReplaceDurableTerm) {
  const auto path = safety_directory("append-identity");
  auto replica = Replica::open(safety_config(path));
  ASSERT_TRUE(replica);
  const auto forged = wire(choreoos::protocol::MessageType::AppendEntries,
                           choreoos::protocol::AppendEntries{1000, "node-c", 0, 0, {}, 0});
  replica.value().handle(forged, "node-b");
  EXPECT_EQ(replica.value().status().term, 1u);
  EXPECT_FALSE(replica.value().halted());
  const auto genuine = wire(choreoos::protocol::MessageType::AppendEntries,
                            choreoos::protocol::AppendEntries{2, "node-b", 0, 0, {}, 0});
  replica.value().handle(genuine, "node-b");
  EXPECT_EQ(replica.value().status().term, 2u);
  EXPECT_FALSE(replica.value().halted());
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace choreoos::replication
