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

void write_log(std::ostringstream& out, const EntryLog& log) {
  out << '[';
  for (std::size_t i = 0; i < log.size(); ++i) {
    if (i) out << ',';
    out << "{\"term\":" << log[i].term << ",\"val\":";
    quoted(out, log[i].command);
    out << '}';
  }
  out << ']';
}

void write_durable(std::ostringstream& out, const Durable& state) {
  out << "{\"term\":" << state.term << ",\"votedFor\":";
  if (state.voted_for)
    quoted(out, *state.voted_for);
  else
    out << "null";
  out << ",\"log\":";
  write_log(out, state.log);
  out << ",\"commit\":" << state.commit << ",\"applied\":" << state.applied
      << ",\"snapshot_index\":" << state.snapshot_index
      << ",\"snapshot_term\":" << state.snapshot_term << '}';
}

template <typename Message>
void write_message(std::ostringstream& out, const Message& message) {
  out << "\"term\":" << message.term;
  if constexpr (std::is_same_v<Message, RequestVote>) {
    out << ",\"candidate\":";
    quoted(out, message.candidate);
    out << ",\"last_index\":" << message.last_index << ",\"last_term\":" << message.last_term;
  } else if constexpr (std::is_same_v<Message, VoteReply>) {
    out << ",\"voter\":";
    quoted(out, message.voter);
    out << ",\"granted\":" << (message.granted ? "true" : "false");
  } else if constexpr (std::is_same_v<Message, AppendEntries>) {
    out << ",\"leader\":";
    quoted(out, message.leader);
    out << ",\"prev_index\":" << message.prev_index << ",\"prev_term\":" << message.prev_term
        << ",\"entries\":";
    write_log(out, EntryLog{message.entries});
    out << ",\"leader_commit\":" << message.leader_commit;
  } else if constexpr (std::is_same_v<Message, SnapshotRequest>) {
    out << ",\"leader\":";
    quoted(out, message.leader);
    out << ",\"index\":" << message.index << ",\"snapshot_term\":" << message.snapshot_term;
  } else {
    out << ",\"follower\":";
    quoted(out, message.follower);
    out << ",\"success\":" << (message.success ? "true" : "false");
    if constexpr (std::is_same_v<Message, AppendReply>)
      out << ",\"hint\":" << message.hint << ",\"matched\":" << message.matched;
    else
      out << ",\"index\":" << message.index;
  }
  out << ",\"request_id\":" << message.request_id;
  if constexpr (std::is_same_v<Message, SnapshotRequest>)
    out << ",\"ready\":" << (message.ready ? "true" : "false");
}

void write_input(std::ostringstream& out, const Input& input) {
  out << '{';
  std::visit(
      [&out](const auto& event) {
        using T = std::decay_t<decltype(event)>;
        if constexpr (std::is_same_v<T, ClientPropose>) {
          out << "\"command\":";
          quoted(out, event.command);
        } else if constexpr (std::is_same_v<T, PersistCompleted> ||
                             std::is_same_v<T, PersistFailed>) {
          out << "\"token\":" << event.token;
        } else if constexpr (std::is_same_v<T, RecvRequestVote> ||
                             std::is_same_v<T, RecvVoteReply> || std::is_same_v<T, RecvAppend> ||
                             std::is_same_v<T, RecvAppendReply> ||
                             std::is_same_v<T, RecvSnapshot> ||
                             std::is_same_v<T, RecvSnapshotReply>) {
          write_message(out, event.message);
        }
      },
      input);
  out << '}';
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
        else if constexpr (std::is_same_v<T, BeginRead>)
          return "BeginRead";
        else if constexpr (std::is_same_v<T, CancelRead>)
          return "CancelRead";
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
  if (std::holds_alternative<BeginRead>(input) || std::holds_alternative<CancelRead>(input) ||
      state.volatile_state.read || std::holds_alternative<CompactCommitted>(input) ||
      std::holds_alternative<RecvSnapshot>(input) ||
      std::holds_alternative<RecvSnapshotReply>(input) || mem.snapshot_index != 0 ||
      mem.snapshot_term != 0 || disk.snapshot_index != 0 || disk.snapshot_term != 0)
    throw std::invalid_argument(
        "v1 Raft trace projection does not support reads, snapshots or local compaction");
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

std::string trace_line_v2(const Config& config, const Node& state, const Input& input,
                          std::uint64_t sequence) {
  if (std::holds_alternative<BeginRead>(input) || std::holds_alternative<CancelRead>(input) ||
      state.volatile_state.read)
    throw std::invalid_argument("v2 snapshot trace contract does not support read rounds");
  std::ostringstream out;
  out << "{\"v\":2,\"seq\":" << sequence << ",\"node\":";
  quoted(out, config.self);
  out << ",\"act\":";
  quoted(out, trace_action(input));
  out << ",\"in\":";
  write_input(out, input);
  out << ",\"post\":";
  std::ostringstream projection;
  write_durable(projection, state.pending ? state.pending->target : state.durable);
  auto staged = projection.str();
  staged.pop_back();
  out << staged << ",\"role\":";
  quoted(out, role_name(state.volatile_state.role));
  out << ",\"durable\":";
  write_durable(out, state.durable);
  out << ",\"alive\":" << (state.halted ? "false" : "true") << "}}";
  return out.str();
}

}  // namespace choreoos::raft
