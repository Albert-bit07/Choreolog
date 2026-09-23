#pragma once

// Structured errors for the state machine.
// Callers check Result<T> instead of using exceptions for domain failures, so
// rejected commands can return a stable ErrorCode without mutating state.

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <variant>

namespace choreoos::state {

// Stable codes used by tests, CLI output, and later protocol responses.
enum class ErrorCode : std::uint16_t {
  EmptyId = 1,           // parse("")
  InvalidId,             // illegal character or too long
  InvalidStage,          // width or depth is 0
  InvalidCoordinate,     // negative or > 1km
  InvalidTick,           // negative time or count overflow
  InvalidTerm,           // term 0
  InvalidTravel,         // negative max-travel setting
  OutOfBounds,           // dancer mark outside the stage
  ChoreographyNotFound,  // command before create, or wrong show id
  ChoreographyExists,    // second create
  DancerExists,          // add the same dancer twice
  DancerNotFound,        // move/remove unknown dancer
  DancerInactive,        // move after remove
  PositionOccupied,      // two active dancers on one mark
  TravelTooFar,          // move longer than max_travel_mm
  FormationExists,       // define an existing formation
  FormationNotFound,     // change a missing formation
  InvalidFormation,      // empty or inactive members
  CueExists,             // reuse a cue id
  CueNotFound,           // reserved for lookups
  CueDependencyUnmet,    // depends_on missing or later in time
  DuplicateCommand,      // command id already accepted
  DuplicateEvent,        // reserved; same event is a no-op
  ConflictingEvent,      // same event id, different payload
  IndexGap,              // log index skipped or went backward
  UnsupportedVersion,    // schema_version != 1
  UnsupportedType,       // unknown command/event name
  StoreError,            // file read/write/parse failure
  NotLeader,             // command arrived at a follower
  Unavailable,           // majority did not acknowledge in time
  ProtocolError,         // malformed or incompatible network frame
};

class Error {
 public:
  // code is machine-readable; message is for humans.
  Error(ErrorCode code, std::string message);

  [[nodiscard]] ErrorCode code() const noexcept;              // e.g. OutOfBounds
  [[nodiscard]] const std::string& message() const noexcept;  // why it failed
  [[nodiscard]] std::string to_string() const;                // "OutOfBounds: ..."

 private:
  ErrorCode code_;       // stable enum
  std::string message_;  // detail text
};

// Either a value or an Error. The [[nodiscard]] attribute makes it harder to
// ignore a failed command or apply.
template <typename T>
class [[nodiscard]] Result {
 public:
  Result(T value) : storage_(std::in_place_type<T>, std::move(value)) {}
  Result(Error error) : storage_(std::in_place_type<Error>, std::move(error)) {}

  [[nodiscard]] bool has_value() const noexcept { return std::holds_alternative<T>(storage_); }
  [[nodiscard]] explicit operator bool() const noexcept { return has_value(); }

  [[nodiscard]] const T& value() const { return std::get<T>(storage_); }
  [[nodiscard]] T& value() { return std::get<T>(storage_); }
  [[nodiscard]] const Error& error() const { return std::get<Error>(storage_); }

 private:
  std::variant<T, Error> storage_;
};

template <>
class [[nodiscard]] Result<void> {
 public:
  Result() = default;
  Result(Error error) : error_(std::move(error)) {}

  [[nodiscard]] bool has_value() const noexcept { return !error_.has_value(); }
  [[nodiscard]] explicit operator bool() const noexcept { return has_value(); }
  [[nodiscard]] const Error& error() const { return *error_; }

 private:
  std::optional<Error> error_{};
};

[[nodiscard]] const char* error_code_name(ErrorCode code) noexcept;

}  // namespace choreoos::state
