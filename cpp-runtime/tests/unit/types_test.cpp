// Value-type checks: IDs, tick conversion, inclusive stage bounds, integer travel.

#include "choreoos/state/types.hpp"

#include <gtest/gtest.h>

#include <limits>

namespace choreoos::state {
namespace {

TEST(IdTest, RejectsEmptyAndIllegalCharacters) {
  const auto empty = DancerId::parse("");
  ASSERT_FALSE(empty);
  EXPECT_EQ(empty.error().code(), ErrorCode::EmptyId);

  const auto bad = DancerId::parse("alice bob");
  ASSERT_FALSE(bad);
  EXPECT_EQ(bad.error().code(), ErrorCode::InvalidId);
}

TEST(IdTest, AcceptsStableComparableValues) {
  const auto alice = DancerId::parse("alice");
  const auto bob = DancerId::parse("bob");
  ASSERT_TRUE(alice);
  ASSERT_TRUE(bob);
  EXPECT_EQ(alice.value(), DancerId::parse("alice").value());
  EXPECT_LT(alice.value(), bob.value());
}

TEST(MusicalTickTest, ConvertsWholeCountsAndRejectsOverflow) {
  const auto tick = MusicalTick::from_count(9);
  ASSERT_TRUE(tick);
  EXPECT_EQ(tick.value().ticks(), 4320);
  EXPECT_EQ(tick.value().count_floor(), 9);

  EXPECT_FALSE(MusicalTick::from_count(-1));
  EXPECT_FALSE(MusicalTick::from_ticks(-1));
  EXPECT_FALSE(MusicalTick::from_count((std::numeric_limits<std::int64_t>::max)()));
}

TEST(GeometryTest, StageBoundariesAreInclusive) {
  const auto stage = StageBounds::from_mm(20000, 12000);
  ASSERT_TRUE(stage);

  const auto origin = Position::from_mm(0, 0);
  const auto corner = Position::from_mm(20000, 12000);
  const auto outside = Position::from_mm(20001, 0);
  ASSERT_TRUE(origin);
  ASSERT_TRUE(corner);
  ASSERT_TRUE(outside);
  EXPECT_TRUE(stage.value().contains(origin.value()));
  EXPECT_TRUE(stage.value().contains(corner.value()));
  EXPECT_FALSE(stage.value().contains(outside.value()));
}

TEST(GeometryTest, RejectsInvalidStageAndCoordinates) {
  EXPECT_FALSE(StageBounds::from_mm(0, 12000));
  EXPECT_FALSE(StageBounds::from_mm(20000, 0));
  EXPECT_FALSE(StageCoordinate::from_mm(-1));
  EXPECT_FALSE(StageCoordinate::from_mm(kMaxStageMm + 1));
}

TEST(GeometryTest, TravelLimitUsesIntegerDistance) {
  const auto from = Position::from_mm(0, 0);
  const auto exact = Position::from_mm(3000, 4000);
  const auto too_far = Position::from_mm(3001, 4000);
  ASSERT_TRUE(from);
  ASSERT_TRUE(exact);
  ASSERT_TRUE(too_far);
  EXPECT_TRUE(travel_within_limit(from.value(), exact.value(), 5000));
  EXPECT_FALSE(travel_within_limit(from.value(), too_far.value(), 5000));
}

TEST(TermTest, RejectsZero) {
  EXPECT_FALSE(Term::parse(0));
  EXPECT_TRUE(Term::parse(1));
}

}  // namespace
}  // namespace choreoos::state
