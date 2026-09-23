#pragma once

// In-memory copies of the protobuf messages in proto/internal/choreoos.proto.
// Replication code uses these structs. The codec turns them into protobuf bytes.

#include <cstdint>
#include <string>
#include <vector>

#include "choreoos/state/model.hpp"

namespace choreoos::protocol {

struct Handshake {
  std::string node_id;
  std::uint32_t major = 1;
  std::uint32_t minor = 0;
};

struct AppendEntries {
  std::uint64_t term = 1;
  std::string leader_id;
  std::uint64_t prev_log_index = 0;
  std::uint64_t prev_log_term = 0;
  std::vector<choreoos::state::Event> entries;
  std::uint64_t leader_commit = 0;
};

struct AppendEntriesResponse {
  std::uint64_t term = 1;
  std::string follower_id;
  bool success = false;
  std::uint64_t match_index = 0;
  std::uint64_t hint_index = 0;
};

struct RequestVote {
  std::uint64_t term = 1;
  std::string candidate_id;
  std::uint64_t last_log_index = 0;
  std::uint64_t last_log_term = 0;
};

struct RequestVoteResponse {
  std::uint64_t term = 1;
  std::string voter_id;
  bool vote_granted = false;
};

struct InstallSnapshot {
  std::uint64_t term = 1;
  std::string leader_id;
  std::uint64_t last_included_index = 0;
  std::uint64_t last_included_term = 0;
  std::string state_hash;
  std::string payload;
};

struct InstallSnapshotResponse {
  std::uint64_t term = 1;
  std::string follower_id;
  bool success = false;
};

struct ProtocolError {
  std::string code;
  std::string detail;
};

struct ClientCommand {
  std::string canonical;
};

struct ClientResponse {
  bool ok = false;
  bool duplicate = false;
  std::string error_code;
  std::string error_message;
  std::string leader_id;
  std::uint64_t index = 0;
  std::string event_canonical;
  std::string state_hash;
};

struct StatusResponse {
  std::string node_id;
  std::string leader_id;
  std::string role;
  std::uint64_t term = 1;
  std::uint64_t commit_index = 0;
  std::uint64_t last_log_index = 0;
  std::string state_hash;
  std::string canonical_state;
};

}  // namespace choreoos::protocol
