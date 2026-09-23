// Construction and validation for IDs, time, and geometry.
// Invalid values become Error results; they are never stored as "bad objects."

#include "choreoos/state/types.hpp"

#include <cctype>  // std::isalnum
#include <limits>  // numeric_limits for overflow checks

namespace choreoos::state {
namespace {

// True if this character may appear in an ID.
// Cast to unsigned char first: std::isalnum is undefined for negative char.
bool is_allowed_id_char(char ch) {
  return std::isalnum(static_cast<unsigned char>(ch)) != 0 || ch == '_' || ch == '.' || ch == ':' ||
         ch == '-';
}

}  // namespace

// Shared parse for every StrongId tag. Same rules for dancers, commands, etc.
template <typename Tag>
Result<StrongId<Tag>> StrongId<Tag>::parse(std::string_view raw) {
  if (raw.empty()) {
    return Error{ErrorCode::EmptyId, "id must not be empty"};
  }
  if (raw.size() > kMaxIdLength) {
    return Error{ErrorCode::InvalidId, "id exceeds 64 characters"};
  }
  for (char ch : raw) {
    if (!is_allowed_id_char(ch)) {
      return Error{ErrorCode::InvalidId, "id contains an unsupported character"};
    }
  }
  // Private constructor is accessible here because this is a StrongId method.
  return StrongId<Tag>{std::string{raw}};
}

// Force the compiler to emit parse() for each ID type we actually use.
template class StrongId<NodeIdTag>;
template class StrongId<ChoreographyIdTag>;
template class StrongId<DancerIdTag>;
template class StrongId<CommandIdTag>;
template class StrongId<EventIdTag>;
template class StrongId<FormationIdTag>;
template class StrongId<CueIdTag>;

// Any uint64 is a legal log index, including 0 (none).
Result<LogIndex> LogIndex::parse(std::uint64_t value) { return LogIndex{value}; }

// Terms start at 1. 0 would look like "not set" and is rejected.
Result<Term> Term::parse(std::uint64_t value) {
  if (value == 0) {
    return Error{ErrorCode::InvalidTerm, "term must be greater than zero"};
  }
  return Term{value};
}

// Negative time is meaningless for this model.
Result<MusicalTick> MusicalTick::from_ticks(std::int64_t ticks) {
  if (ticks < 0) {
    return Error{ErrorCode::InvalidTick, "musical ticks must be non-negative"};
  }
  return MusicalTick{ticks};
}

// Convert a whole count to ticks. Guard count * 480 so it cannot overflow int64.
Result<MusicalTick> MusicalTick::from_count(std::int64_t count) {
  if (count < 0) {
    return Error{ErrorCode::InvalidTick, "count must be non-negative"};
  }
  // Parentheses around max avoid the Windows min/max macros.
  if (count > (std::numeric_limits<std::int64_t>::max)() / kTicksPerCount) {
    return Error{ErrorCode::InvalidTick, "count is too large to convert to ticks"};
  }
  return MusicalTick{count * kTicksPerCount};
}

// A coordinate is a non-negative millimeter value within the 1km cap.
Result<StageCoordinate> StageCoordinate::from_mm(std::int32_t mm) {
  if (mm < 0) {
    return Error{ErrorCode::InvalidCoordinate, "coordinate must be non-negative"};
  }
  if (mm > kMaxStageMm) {
    return Error{ErrorCode::InvalidCoordinate, "coordinate exceeds the 1km implementation limit"};
  }
  return StageCoordinate{mm};
}

// Both coordinates are already valid, so just store them.
Result<Position> Position::create(StageCoordinate x, StageCoordinate y) { return Position{x, y}; }

// Validate x, then y, then combine. Fail on the first bad axis.
Result<Position> Position::from_mm(std::int32_t x_mm, std::int32_t y_mm) {
  auto x = StageCoordinate::from_mm(x_mm);
  if (!x) {
    return x.error();
  }
  auto y = StageCoordinate::from_mm(y_mm);
  if (!y) {
    return y.error();
  }
  return Position::create(x.value(), y.value());
}

// A stage must have positive size. 0x12m is not a usable floor.
Result<StageBounds> StageBounds::create(StageCoordinate width, StageCoordinate depth) {
  if (width.mm() <= 0 || depth.mm() <= 0) {
    return Error{ErrorCode::InvalidStage, "stage width and depth must be greater than zero"};
  }
  return StageBounds{width, depth};
}

// Parse raw millimeters, then apply the positive-size rule.
Result<StageBounds> StageBounds::from_mm(std::int32_t width_mm, std::int32_t depth_mm) {
  auto width = StageCoordinate::from_mm(width_mm);
  if (!width) {
    return width.error();
  }
  auto depth = StageCoordinate::from_mm(depth_mm);
  if (!depth) {
    return depth.error();
  }
  return StageBounds::create(width.value(), depth.value());
}

// Inclusive: the far corner (width, depth) is still on stage.
// x and y are already >= 0 because StageCoordinate rejected negatives.
bool StageBounds::contains(const Position& position) const noexcept {
  return position.x().mm() <= width_.mm() && position.y().mm() <= depth_.mm();
}

// Integer distance squared. Promote to int64 before subtract so 1km - 0 is safe.
std::int64_t squared_distance_mm(const Position& from, const Position& to) noexcept {
  const std::int64_t dx = static_cast<std::int64_t>(to.x().mm()) - from.x().mm();
  const std::int64_t dy = static_cast<std::int64_t>(to.y().mm()) - from.y().mm();
  return dx * dx + dy * dy;
}

// Stay in integer space: compare squares instead of calling sqrt.
bool travel_within_limit(const Position& from, const Position& to,
                         std::int32_t max_travel_mm) noexcept {
  const std::int64_t limit = static_cast<std::int64_t>(max_travel_mm);
  return squared_distance_mm(from, to) <= limit * limit;
}

}  // namespace choreoos::state
