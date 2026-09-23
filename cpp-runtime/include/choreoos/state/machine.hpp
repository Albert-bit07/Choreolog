#pragma once

// Deterministic choreography state machine.
//
// Flow:
//   command -> propose() -> Event  (validate only; state unchanged)
//   Event   -> apply()   -> state  (copy, mutate candidate, swap on success)
//   events  -> replay()  -> state  (same apply function)
//
// Given the same events, every Engine must produce the same canonical_state()
// and state_hash(). This file must stay free of clocks, I/O, and randomness.

#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "choreoos/state/model.hpp"

namespace choreoos::state {

struct DancerState {
  Position position;   // current mark
  bool active = true;  // false after remove; they cannot move again
};

struct FormationState {
  std::vector<DancerId> members;  // sorted dancer ids
};

struct CueState {
  MusicalTick tick;                 // when the cue fired
  std::optional<CueId> depends_on;  // earlier cue, if any
};

// Derived from the committed event log. Maps stay ordered so hashing is stable.
struct ChoreographyState {
  bool created = false;                                 // false until first create event
  std::optional<ChoreographyId> id;                     // empty before create
  std::optional<StageBounds> stage;                     // empty before create
  OverlapPolicy overlap = OverlapPolicy::Forbidden;     // default: one dancer per mark
  std::int32_t max_travel_mm = 0;                       // set by create
  LogIndex last_applied = LogIndex::none();             // 0 until event 1
  Term term = Term::initial();                          // 1 in Milestone 1
  std::map<std::string, DancerState> dancers;           // key = dancer id
  std::map<std::string, FormationState> formations;     // key = formation id
  std::map<std::string, CueState> music_cues;           // key = cue id
  std::map<std::string, CueState> lighting_cues;        // key = cue id
  std::map<std::string, std::string> applied_events;    // event id -> canonical text
  std::map<std::string, std::string> applied_commands;  // command id -> event id
};

struct SubmitResult {
  Event event;             // the accepted (or prior) event
  bool duplicate = false;  // true if this command id was already committed
};

// Contiguous: normal commit, indexes must be last+1.
// Monotonic: filtered replay by count, later-tick events may be skipped.
enum class IndexRule { Contiguous, Monotonic };

// Validate a command against committed state and build one event. No I/O.
[[nodiscard]] Result<Event> propose(const ChoreographyState& state, const Command& command);
// Apply one committed event. On failure the input state is unchanged.
[[nodiscard]] Result<void> apply(ChoreographyState& state, const Event& event,
                                 IndexRule index_rule = IndexRule::Contiguous);
[[nodiscard]] Result<ChoreographyState> replay(
    const std::vector<Event>& events, std::optional<MusicalTick> through_tick = std::nullopt,
    std::optional<LogIndex> through_index = std::nullopt);
[[nodiscard]] std::string canonical_state(const ChoreographyState& state);
[[nodiscard]] std::string state_hash(const ChoreographyState& state);

// In-memory single-node engine: submit assigns the next index and applies.
class Engine {
 public:
  Result<SubmitResult> submit(Command command);
  Result<void> apply_committed(const Event& event);

  [[nodiscard]] const ChoreographyState& state() const noexcept { return state_; }
  [[nodiscard]] const std::vector<Event>& events() const noexcept { return events_; }
  [[nodiscard]] Result<ChoreographyState> replay_through(
      std::optional<MusicalTick> through_tick, std::optional<LogIndex> through_index) const;

 private:
  ChoreographyState state_{};
  std::vector<Event> events_{};
};

}  // namespace choreoos::state
