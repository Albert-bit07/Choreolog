#include "choreoos/raft/trace.hpp"

#include <sstream>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <variant>

namespace choreoos::raft {
namespace {

void quoted(std::ostringstream& out, std::string_view text) {
  constexpr char hex[] = "0123456789abcdef";
  out << '"';
  for (char byte : text) {
    const auto c = static_cast<unsigned char>(byte);
    switch (c) {
      case '"':
        out << "\\\"";
        break;
      case '\\':
        out << "\\\\";
        break;
      case '\b':
        out << "\\b";
        break;
      case '\f':
        out << "\\f";
        break;
      case '\n':
        out << "\\n";
        break;
      case '\r':
        out << "\\r";
        break;
      case '\t':
        out << "\\t";
        break;
      default:
        if (c < 0x20) {
          out << "\\u00" << hex[c >> 4] << hex[c & 0x0f];
        } else {
          out << static_cast<char>(c);
        }
    }
  }
  out << '"';
}

const char* role_name(Role role) noexcept {
  switch (role) {
    case Role::Follower:
      return "Follower";
    case Role::Candidate:
      return "Candidate";
    case Role::Leader:
      return "Leader";
  }
  return "Follower";
}

}  // namespace

const char* trace_action(const Input& input) {
  return std::visit(
      [](const auto& event) noexcept -> const char* {
        using T = std::decay_t<decltype(event)>;
        if constexpr (std::is_same_v<T, ElectionTimeout>)
          return "Timeout";
        else if constexpr (std::is_same_v<T, HeartbeatTimeout>)
          return "SendAppend";
        else if constexpr (std::is_same_v<T, ClientPropose>)
          return "ClientPropose";
        else if constexpr (std::is_same_v<T, RecvRequestVote>)
          return "HandleRVReq";
        else if constexpr (std::is_same_v<T, RecvVoteReply>)
          return "HandleRVResp";
        else if constexpr (std::is_same_v<T, RecvAppend>)
          return "HandleAEReq";
        else if constexpr (std::is_same_v<T, RecvAppendReply>)
          return "HandleAEResp";
        else if constexpr (std::is_same_v<T, AdvanceCommit>)
          return "AdvanceCommit";
        else if constexpr (std::is_same_v<T, ApplyCommitted>)
          return "Apply";
        else if constexpr (std::is_same_v<T, CompactCommitted>)
          return "CompactCommitted";
        else if constexpr (std::is_same_v<T, RecvSnapshot>)
          return "InstallSnapshot";
        else if constexpr (std::is_same_v<T, RecvSnapshotReply>)
          return "SnapshotReply";
        else if constexpr (std::is_same_v<T, PersistCompleted>)
          return "PersistCompleted";
        else if constexpr (std::is_same_v<T, PersistFailed>)
          return "PersistFailed";
        else
          return "Restart";
      },
      input);
}

std::string trace_line(const Config& config, const Node& state, const Input& input,
                       std::uint64_t sequence) {
  // mem is staged while disk is the last acknowledged durability boundary.
  const auto& mem = state.pending ? state.pending->target : state.durable;
  const auto& disk = state.durable;
  if (std::holds_alternative<CompactCommitted>(input) ||
      std::holds_alternative<RecvSnapshot>(input) ||
      std::holds_alternative<RecvSnapshotReply>(input) || mem.snapshot_index != 0 ||
      mem.snapshot_term != 0 || disk.snapshot_index != 0 || disk.snapshot_term != 0)
    throw std::invalid_argument(
        "v1 Raft trace projection does not support snapshots or local compaction");
  std::ostringstream out;
  out << "{\"v\":1,\"seq\":" << sequence << ",\"node\":";
  quoted(out, config.self);
  out << ",\"act\":";
  quoted(out, trace_action(input));
  out << ",\"post\":{\"term\":" << mem.term << ",\"role\":";
  quoted(out, role_name(state.volatile_state.role));
  out << ",\"votedFor\":";
  if (mem.voted_for)
    quoted(out, *mem.voted_for);
  else
    out << "null";
  out << ",\"commit\":" << mem.commit << ",\"log\":[";
  for (std::size_t i = 0; i < mem.log.size(); ++i) {
    if (i) out << ',';
    out << "{\"term\":" << mem.log[i].term << ",\"val\":";
    quoted(out, mem.log[i].command);
    out << '}';
  }
  out << "],\"durable\":{\"term\":" << disk.term << ",\"log_len\":" << disk.log.size()
      << ",\"commit\":" << disk.commit << ",\"applied\":" << disk.applied
      << "},\"alive\":" << (state.halted ? "false" : "true") << "}}";
  return out.str();
}

}  // namespace choreoos::raft
