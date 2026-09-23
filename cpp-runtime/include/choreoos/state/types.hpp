#pragma once
// Include this header only once per translation unit.

// Deterministic domain values.
// Authoritative state uses integers only: IDs, log indexes, musical ticks, and
// millimeter coordinates. Floats are forbidden here so two machines cannot
// diverge from rounding.

#include <cstdint>      // Fixed-width integers: int32_t, int64_t, uint64_t.
#include <string>       // std::string storage for validated IDs.
#include <string_view>  // Non-owning text passed into parse().

#include "choreoos/state/error.hpp"  // Result<T> and Error used by constructors.

namespace choreoos::state {

// 480 ticks = 1 count. Count 9 becomes tick 4320. Later fractional timing
// can still use this same tick unit without changing the event store.
inline constexpr std::int64_t kTicksPerCount = 480;

// Hard cap on one stage axis: 1,000,000 mm = 1 km. Prevents overflow later
// when we multiply coordinates for squared distance.
inline constexpr std::int32_t kMaxStageMm = 1'000'000;

// IDs longer than 64 characters are rejected. Keeps log lines and hashes small.
inline constexpr std::uint32_t kMaxIdLength = 64;

// Current command/event schema. apply() rejects any other version.
inline constexpr std::uint16_t kCurrentSchemaVersion = 1;

// Tagged string ID. The Tag type is empty; it only makes NodeId and DancerId
// different C++ types so you cannot pass one where the other is required.
// Construction goes through parse() so empty or illegal IDs never exist.
template <typename Tag>
class StrongId {
 public:
  // No default ID. You must parse text or you do not have an ID.
  StrongId() = delete;

  // Validate raw text and return StrongId or Error.
  // Allowed characters: letters, digits, '_', '.', ':', '-'.
  [[nodiscard]] static Result<StrongId> parse(std::string_view raw);

  // Owned string used in maps, logs, and hashes.
  [[nodiscard]] const std::string& value() const noexcept { return value_; }

  // Non-owning view of the same text. Useful for printing without a copy.
  [[nodiscard]] std::string_view view() const noexcept { return value_; }

  // Equality is exact string equality: "alice" != "Alice".
  friend bool operator==(const StrongId& lhs, const StrongId& rhs) noexcept {
    return lhs.value_ == rhs.value_;
  }

  // Inequality is the negation of equality.
  friend bool operator!=(const StrongId& lhs, const StrongId& rhs) noexcept {
    return !(lhs == rhs);
  }

  // Ordering lets IDs sit in std::map so canonical_state() is sorted.
  friend bool operator<(const StrongId& lhs, const StrongId& rhs) noexcept {
    return lhs.value_ < rhs.value_;
  }

 private:
  // Only parse() may construct. explicit blocks StrongId{"alice"} accidents.
  explicit StrongId(std::string value) : value_(std::move(value)) {}

  // The validated ID text.
  std::string value_;
};

// Empty tag types. Each one creates a distinct StrongId specialization.
struct NodeIdTag {};          // Identifies a cluster node. Unused until networking.
struct ChoreographyIdTag {};  // Identifies one piece / show.
struct DancerIdTag {};        // Identifies one dancer.
struct CommandIdTag {};       // Idempotency key for a client request.
struct EventIdTag {};         // Unique ID of a committed fact.
struct FormationIdTag {};     // Named group of dancers.
struct CueIdTag {};           // Music or lighting cue.

// Readable aliases. DancerId is StrongId<DancerIdTag>, not a raw string.
using NodeId = StrongId<NodeIdTag>;
using ChoreographyId = StrongId<ChoreographyIdTag>;
using DancerId = StrongId<DancerIdTag>;
using CommandId = StrongId<CommandIdTag>;
using EventId = StrongId<EventIdTag>;
using FormationId = StrongId<FormationIdTag>;
using CueId = StrongId<CueIdTag>;

// Position in the append-only event log.
// 0 means nothing has been applied. The first real event is index 1.
class LogIndex {
 public:
  // Wrap a number from disk or a test. Currently every uint64 is accepted.
  [[nodiscard]] static Result<LogIndex> parse(std::uint64_t value);

  // The "no events yet" index. Empty Engine starts here.
  [[nodiscard]] static LogIndex none() noexcept { return LogIndex{0}; }

  // Raw number for printing, hashing, and file storage.
  [[nodiscard]] std::uint64_t value() const noexcept { return value_; }

  // Next index that must be committed. After 3, the next event must be 4.
  [[nodiscard]] LogIndex next() const noexcept { return LogIndex{value_ + 1}; }

  // True only for 0. Used when asking "has anything been applied?"
  [[nodiscard]] bool is_none() const noexcept { return value_ == 0; }

  // Equal indexes mean the same log position.
  friend bool operator==(LogIndex lhs, LogIndex rhs) noexcept { return lhs.value_ == rhs.value_; }

  // Not the same log position.
  friend bool operator!=(LogIndex lhs, LogIndex rhs) noexcept { return !(lhs == rhs); }

  // True if lhs happened before rhs in the log.
  friend bool operator<(LogIndex lhs, LogIndex rhs) noexcept { return lhs.value_ < rhs.value_; }

  // True if lhs is the same position as rhs or earlier.
  friend bool operator<=(LogIndex lhs, LogIndex rhs) noexcept { return lhs.value_ <= rhs.value_; }

 private:
  // Only parse(), none(), and next() construct an index.
  explicit LogIndex(std::uint64_t value) : value_(value) {}

  // 0 = none. First committed event uses 1.
  std::uint64_t value_ = 0;
};

// Raft-like term. Milestone 1 always uses term 1.
// Events already carry a term so later leader election does not change the
// event envelope.
class Term {
 public:
  // Reject 0. Consensus terms start at 1.
  [[nodiscard]] static Result<Term> parse(std::uint64_t value);

  // Term used by the single-node engine today.
  [[nodiscard]] static Term initial() noexcept { return Term{1}; }

  // Raw term number for the event envelope and hash.
  [[nodiscard]] std::uint64_t value() const noexcept { return value_; }

  // Next election term. Terms only move forward; overflow is an error.
  [[nodiscard]] Result<Term> next() const {
    if (value_ == ~std::uint64_t{0}) {
      return Error{ErrorCode::StoreError, "term overflow"};
    }
    return Term{value_ + 1};
  }

  // Same term.
  friend bool operator==(Term lhs, Term rhs) noexcept { return lhs.value_ == rhs.value_; }

  // Different term.
  friend bool operator!=(Term lhs, Term rhs) noexcept { return !(lhs == rhs); }

 private:
  // Only parse() and initial() construct a term.
  explicit Term(std::uint64_t value) : value_(value) {}

  // Default stored value if a Term is created via initial().
  std::uint64_t value_ = 1;
};

// Authoritative musical time. CLI input is a whole count; storage is ticks.
class MusicalTick {
 public:
  // Create from an already-converted tick count. Rejects negatives.
  [[nodiscard]] static Result<MusicalTick> from_ticks(std::int64_t ticks);

  // Create from a whole count: ticks = count * 480. Rejects overflow.
  [[nodiscard]] static Result<MusicalTick> from_count(std::int64_t count);

  // Exact tick value stored on events and used for seek.
  [[nodiscard]] std::int64_t ticks() const noexcept { return ticks_; }

  // Convert back to a whole count by integer division. Tick 4320 => count 9.
  [[nodiscard]] std::int64_t count_floor() const noexcept { return ticks_ / kTicksPerCount; }

  // Same instant in the music.
  friend bool operator==(MusicalTick lhs, MusicalTick rhs) noexcept {
    return lhs.ticks_ == rhs.ticks_;
  }

  // Different instant.
  friend bool operator!=(MusicalTick lhs, MusicalTick rhs) noexcept { return !(lhs == rhs); }

  // lhs happens before rhs.
  friend bool operator<(MusicalTick lhs, MusicalTick rhs) noexcept {
    return lhs.ticks_ < rhs.ticks_;
  }

  // lhs happens at or before rhs. Used when seeking "through count C".
  friend bool operator<=(MusicalTick lhs, MusicalTick rhs) noexcept {
    return lhs.ticks_ <= rhs.ticks_;
  }

 private:
  // Only from_ticks() and from_count() construct a tick.
  explicit MusicalTick(std::int64_t ticks) : ticks_(ticks) {}

  // 0 is a valid time (the start of the piece).
  std::int64_t ticks_ = 0;
};

// One stage axis in millimeters. Negative values are rejected.
class StageCoordinate {
 public:
  // Accept 0 through kMaxStageMm inclusive. Reject negatives and huge values.
  [[nodiscard]] static Result<StageCoordinate> from_mm(std::int32_t mm);

  // Millimeters on this axis.
  [[nodiscard]] std::int32_t mm() const noexcept { return mm_; }

  // Same point on this axis.
  friend bool operator==(StageCoordinate lhs, StageCoordinate rhs) noexcept {
    return lhs.mm_ == rhs.mm_;
  }

  // Different point on this axis.
  friend bool operator!=(StageCoordinate lhs, StageCoordinate rhs) noexcept {
    return !(lhs == rhs);
  }

 private:
  // Only from_mm() constructs a coordinate.
  explicit StageCoordinate(std::int32_t mm) : mm_(mm) {}

  // Validated millimeter value.
  std::int32_t mm_ = 0;
};

// A dancer mark: (x, y) in millimeters.
class Position {
 public:
  // Combine two already-valid coordinates.
  [[nodiscard]] static Result<Position> create(StageCoordinate x, StageCoordinate y);

  // Parse two raw millimeter values. Fails if either coordinate is invalid.
  [[nodiscard]] static Result<Position> from_mm(std::int32_t x_mm, std::int32_t y_mm);

  // Left/right stage position.
  [[nodiscard]] StageCoordinate x() const noexcept { return x_; }

  // Up/down stage position.
  [[nodiscard]] StageCoordinate y() const noexcept { return y_; }

  // Same mark. Used to detect two dancers on one spot.
  friend bool operator==(const Position& lhs, const Position& rhs) noexcept {
    return lhs.x_ == rhs.x_ && lhs.y_ == rhs.y_;
  }

  // Different mark.
  friend bool operator!=(const Position& lhs, const Position& rhs) noexcept {
    return !(lhs == rhs);
  }

 private:
  // Only create() / from_mm() construct a position.
  Position(StageCoordinate x, StageCoordinate y) : x_(x), y_(y) {}

  // Horizontal coordinate.
  StageCoordinate x_;

  // Vertical coordinate.
  StageCoordinate y_;
};

// Inclusive rectangle from (0,0) to (width, depth).
// A dancer at exactly the far corner is still on stage.
class StageBounds {
 public:
  // Width and depth must both be greater than 0.
  [[nodiscard]] static Result<StageBounds> create(StageCoordinate width, StageCoordinate depth);

  // Parse raw millimeters, then create().
  [[nodiscard]] static Result<StageBounds> from_mm(std::int32_t width_mm, std::int32_t depth_mm);

  // Stage width in millimeters.
  [[nodiscard]] StageCoordinate width() const noexcept { return width_; }

  // Stage depth in millimeters.
  [[nodiscard]] StageCoordinate depth() const noexcept { return depth_; }

  // True if position.x <= width and position.y <= depth.
  // Origin (0,0) is always inside a valid stage.
  [[nodiscard]] bool contains(const Position& position) const noexcept;

 private:
  // Only create() / from_mm() construct bounds.
  StageBounds(StageCoordinate width, StageCoordinate depth) : width_(width), depth_(depth) {}

  // Maximum x a dancer may occupy.
  StageCoordinate width_;

  // Maximum y a dancer may occupy.
  StageCoordinate depth_;
};

// OverlapPolicy::Forbidden: two active dancers may not share a mark.
// OverlapPolicy::Allowed: stacked positions are legal.
enum class OverlapPolicy { Forbidden, Allowed };

// (dx*dx + dy*dy) in millimeters squared. Avoids floating-point sqrt.
[[nodiscard]] std::int64_t squared_distance_mm(const Position& from, const Position& to) noexcept;

// True if the move is at most max_travel_mm. Compared as
// squared_distance <= max_travel_mm * max_travel_mm.
[[nodiscard]] bool travel_within_limit(const Position& from, const Position& to,
                                       std::int32_t max_travel_mm) noexcept;

}  // namespace choreoos::state
