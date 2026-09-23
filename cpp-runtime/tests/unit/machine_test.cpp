// State-machine properties:
// rejected commands emit no events, duplicates are idempotent, two engines
// that apply the same log produce the same hash, and file replay matches memory.

#include "choreoos/state/machine.hpp"

#include <gtest/gtest.h>

#include <filesystem>

#include "choreoos/state/store.hpp"

namespace choreoos::state {
namespace {

CommandId cmd(const char* value) { return CommandId::parse(value).value(); }
ChoreographyId show_id() { return ChoreographyId::parse("opening").value(); }
DancerId dancer(const char* value) { return DancerId::parse(value).value(); }
FormationId formation(const char* value) { return FormationId::parse(value).value(); }
CueId cue(const char* value) { return CueId::parse(value).value(); }
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

Command add_command(const char* command_id, const char* dancer_id, std::int32_t x, std::int32_t y,
                    std::int64_t at_count) {
  return Command{cmd(command_id),        show_id(),
                 CommandType::AddDancer, kCurrentSchemaVersion,
                 count(at_count),        DancerPayload{dancer(dancer_id), at(x, y)}};
}

Command move_command(const char* command_id, const char* dancer_id, std::int32_t x, std::int32_t y,
                     std::int64_t at_count) {
  return Command{cmd(command_id),         show_id(),
                 CommandType::MoveDancer, kCurrentSchemaVersion,
                 count(at_count),         DancerPayload{dancer(dancer_id), at(x, y)}};
}

TEST(MachineTest, CreateAddAndMoveAreAccepted) {
  Engine engine;
  ASSERT_TRUE(engine.submit(create_command()));
  ASSERT_TRUE(engine.submit(add_command("c-alice", "alice", 2000, 3000, 1)));
  ASSERT_TRUE(engine.submit(add_command("c-bob", "bob", 5000, 3000, 1)));
  ASSERT_TRUE(engine.submit(move_command("c-move", "alice", 4000, 6000, 9)));

  EXPECT_EQ(engine.state().dancers.at("alice").position, at(4000, 6000));
  EXPECT_EQ(engine.state().last_applied.value(), 4u);
}

TEST(MachineTest, RejectsOutOfBoundsOccupiedAndTravelViolations) {
  Engine engine;
  ASSERT_TRUE(engine.submit(create_command()));
  ASSERT_TRUE(engine.submit(add_command("c-alice", "alice", 2000, 3000, 1)));
  ASSERT_TRUE(engine.submit(add_command("c-bob", "bob", 5000, 3000, 1)));

  EXPECT_EQ(engine.submit(add_command("c-out", "cara", 20001, 0, 1)).error().code(),
            ErrorCode::OutOfBounds);
  EXPECT_EQ(engine.submit(add_command("c-overlap", "cara", 2000, 3000, 1)).error().code(),
            ErrorCode::PositionOccupied);
  EXPECT_EQ(engine.submit(move_command("c-far", "alice", 20000, 12000, 9)).error().code(),
            ErrorCode::TravelTooFar);
  EXPECT_TRUE(engine.events().size() == 3);
}

TEST(MachineTest, RejectsUnknownAndRemovedDancers) {
  Engine engine;
  ASSERT_TRUE(engine.submit(create_command()));
  EXPECT_EQ(engine.submit(move_command("c-missing", "alice", 1000, 1000, 1)).error().code(),
            ErrorCode::DancerNotFound);

  ASSERT_TRUE(engine.submit(add_command("c-alice", "alice", 2000, 3000, 1)));
  ASSERT_TRUE(engine.submit(Command{cmd("c-remove"), show_id(), CommandType::RemoveDancer,
                                    kCurrentSchemaVersion, count(2),
                                    RemoveDancerPayload{dancer("alice")}}));
  EXPECT_EQ(engine.submit(move_command("c-late", "alice", 2500, 3000, 3)).error().code(),
            ErrorCode::DancerInactive);
}

TEST(MachineTest, InvalidCommandsEmitNoEvents) {
  Engine engine;
  ASSERT_TRUE(engine.submit(create_command()));
  const auto before = engine.events().size();
  EXPECT_FALSE(engine.submit(add_command("c-bad", "alice", 20001, 0, 1)));
  EXPECT_EQ(engine.events().size(), before);
}

TEST(MachineTest, DuplicateCommandReturnsPriorEvent) {
  Engine engine;
  ASSERT_TRUE(engine.submit(create_command()));
  const auto first = engine.submit(add_command("c-alice", "alice", 2000, 3000, 1));
  const auto second = engine.submit(add_command("c-alice", "alice", 2000, 3000, 1));
  ASSERT_TRUE(first);
  ASSERT_TRUE(second);
  EXPECT_TRUE(second.value().duplicate);
  EXPECT_EQ(first.value().event.id, second.value().event.id);
  EXPECT_EQ(engine.state().dancers.size(), 1u);
}

TEST(MachineTest, DuplicateEventApplyIsIdempotent) {
  Engine engine;
  ASSERT_TRUE(engine.submit(create_command()));
  ASSERT_TRUE(engine.submit(add_command("c-alice", "alice", 2000, 3000, 1)));
  ChoreographyState copy = engine.state();
  ASSERT_TRUE(apply(copy, engine.events().back()));
  EXPECT_EQ(copy.dancers.size(), 1u);
  EXPECT_EQ(state_hash(copy), state_hash(engine.state()));
}

TEST(MachineTest, FormationsAndCueDependencies) {
  Engine engine;
  ASSERT_TRUE(engine.submit(create_command()));
  ASSERT_TRUE(engine.submit(add_command("c-alice", "alice", 2000, 3000, 1)));
  ASSERT_TRUE(engine.submit(add_command("c-bob", "bob", 5000, 3000, 1)));

  const auto defined = engine.submit(
      Command{cmd("c-form"), show_id(), CommandType::DefineFormation, kCurrentSchemaVersion,
              count(1), FormationPayload{formation("front"), {dancer("bob"), dancer("alice")}}});
  ASSERT_TRUE(defined);
  EXPECT_EQ(engine.state().formations.at("front").members.front(), dancer("alice"));

  EXPECT_EQ(
      engine
          .submit(Command{cmd("c-light"), show_id(), CommandType::TriggerLightingCue,
                          kCurrentSchemaVersion, count(1), CuePayload{cue("go"), cue("intro")}})
          .error()
          .code(),
      ErrorCode::CueDependencyUnmet);

  ASSERT_TRUE(engine.submit(Command{cmd("c-music"), show_id(), CommandType::TriggerMusicCue,
                                    kCurrentSchemaVersion, count(1),
                                    CuePayload{cue("intro"), std::nullopt}}));
  ASSERT_TRUE(
      engine.submit(Command{cmd("c-light2"), show_id(), CommandType::TriggerLightingCue,
                            kCurrentSchemaVersion, count(2), CuePayload{cue("go"), cue("intro")}}));
}

TEST(MachineTest, TwoEnginesReplayToTheSameHash) {
  Engine leader;
  ASSERT_TRUE(leader.submit(create_command()));
  ASSERT_TRUE(leader.submit(add_command("c-alice", "alice", 2000, 3000, 1)));
  ASSERT_TRUE(leader.submit(move_command("c-move", "alice", 4000, 4000, 9)));

  Engine follower;
  for (const auto& event : leader.events()) {
    ASSERT_TRUE(follower.apply_committed(event));
  }

  EXPECT_EQ(canonical_state(leader.state()), canonical_state(follower.state()));
  EXPECT_EQ(state_hash(leader.state()), state_hash(follower.state()));

  const auto replayed = replay(leader.events());
  ASSERT_TRUE(replayed);
  EXPECT_EQ(state_hash(replayed.value()), state_hash(leader.state()));
}

TEST(MachineTest, ReplayCanSeekByCount) {
  Engine engine;
  ASSERT_TRUE(engine.submit(create_command()));
  ASSERT_TRUE(engine.submit(add_command("c-alice", "alice", 2000, 3000, 1)));
  ASSERT_TRUE(engine.submit(move_command("c-move", "alice", 4000, 4000, 9)));

  const auto early = engine.replay_through(count(1), std::nullopt);
  ASSERT_TRUE(early);
  EXPECT_EQ(early.value().dancers.at("alice").position, at(2000, 3000));
}

TEST(MachineTest, FileStoreRoundTripPreservesHash) {
  const auto dir = std::filesystem::temp_directory_path() / "choreoos-m1-store";
  std::filesystem::remove_all(dir);

  auto first = FileEngine::open(dir);
  ASSERT_TRUE(first);
  ASSERT_TRUE(first.value().submit(create_command()));
  ASSERT_TRUE(first.value().submit(add_command("c-alice", "alice", 2000, 3000, 1)));
  const std::string hash = state_hash(first.value().engine().state());

  auto second = FileEngine::open(dir);
  ASSERT_TRUE(second);
  EXPECT_EQ(state_hash(second.value().engine().state()), hash);
  std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace choreoos::state
