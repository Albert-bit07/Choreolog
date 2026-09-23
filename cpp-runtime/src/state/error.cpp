// Maps ErrorCode to a stable name string for CLI, tests, and logs.

#include "choreoos/state/error.hpp"

#include <sstream>

namespace choreoos::state {

Error::Error(ErrorCode code, std::string message) : code_(code), message_(std::move(message)) {}

ErrorCode Error::code() const noexcept { return code_; }

const std::string& Error::message() const noexcept { return message_; }

std::string Error::to_string() const {
  std::ostringstream out;
  out << error_code_name(code_) << ": " << message_;
  return out.str();
}

const char* error_code_name(ErrorCode code) noexcept {
  switch (code) {
    case ErrorCode::EmptyId:
      return "EmptyId";
    case ErrorCode::InvalidId:
      return "InvalidId";
    case ErrorCode::InvalidStage:
      return "InvalidStage";
    case ErrorCode::InvalidCoordinate:
      return "InvalidCoordinate";
    case ErrorCode::InvalidTick:
      return "InvalidTick";
    case ErrorCode::InvalidTerm:
      return "InvalidTerm";
    case ErrorCode::InvalidTravel:
      return "InvalidTravel";
    case ErrorCode::OutOfBounds:
      return "OutOfBounds";
    case ErrorCode::ChoreographyNotFound:
      return "ChoreographyNotFound";
    case ErrorCode::ChoreographyExists:
      return "ChoreographyExists";
    case ErrorCode::DancerExists:
      return "DancerExists";
    case ErrorCode::DancerNotFound:
      return "DancerNotFound";
    case ErrorCode::DancerInactive:
      return "DancerInactive";
    case ErrorCode::PositionOccupied:
      return "PositionOccupied";
    case ErrorCode::TravelTooFar:
      return "TravelTooFar";
    case ErrorCode::FormationExists:
      return "FormationExists";
    case ErrorCode::FormationNotFound:
      return "FormationNotFound";
    case ErrorCode::InvalidFormation:
      return "InvalidFormation";
    case ErrorCode::CueExists:
      return "CueExists";
    case ErrorCode::CueNotFound:
      return "CueNotFound";
    case ErrorCode::CueDependencyUnmet:
      return "CueDependencyUnmet";
    case ErrorCode::DuplicateCommand:
      return "DuplicateCommand";
    case ErrorCode::DuplicateEvent:
      return "DuplicateEvent";
    case ErrorCode::ConflictingEvent:
      return "ConflictingEvent";
    case ErrorCode::IndexGap:
      return "IndexGap";
    case ErrorCode::UnsupportedVersion:
      return "UnsupportedVersion";
    case ErrorCode::UnsupportedType:
      return "UnsupportedType";
    case ErrorCode::StoreError:
      return "StoreError";
    case ErrorCode::NotLeader:
      return "NotLeader";
    case ErrorCode::Unavailable:
      return "Unavailable";
    case ErrorCode::ProtocolError:
      return "ProtocolError";
  }
  return "Unknown";
}

}  // namespace choreoos::state
