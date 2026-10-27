// Bounded, reproducible C++ transition scenarios. This is NOT a driver for
// the production Replica, nor a mechanically generated TLA+ behavior.
#include <algorithm>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "choreoos/raft/trace.hpp"
#include "choreoos/raft/transition.hpp"

namespace {
using namespace choreoos::raft;

void require(bool condition, const char* why) {
  if (!condition) {
    throw std::runtime_error(why);
  }
}

PersistToken persist_token(const Effects& effects) {
  if (!effects.persist) throw std::runtime_error("missing persistence intent");
  return effects.persist->token;
}

Config config_for(const std::string& id) {
  if (id == "a") return {"a", {"b", "c"}};
  if (id == "b") return {"b", {"a", "c"}};
  return {"c", {"a", "b"}};
}

std::string projection(const Config& cfg, const Node& node, bool v2) {
  const auto document =
      v2 ? trace_line_v2(cfg, node, Restart{}, 0) : trace_line(cfg, node, Restart{}, 0);
  const auto start = document.find("\"post\":");
  require(start != std::string::npos, "missing post projection");
  return document.substr(start + 7, document.size() - start - 8);
}

const char* message_kind(const WireMessage& message) {
  if (std::holds_alternative<RequestVote>(message)) return "RVReq";
  if (std::holds_alternative<VoteReply>(message)) return "RVResp";
  if (std::holds_alternative<AppendEntries>(message)) return "AEReq";
  if (std::holds_alternative<AppendReply>(message)) return "AEResp";
  return "SnapshotReply";
}

bool positive_response(const WireMessage& message) {
  if (const auto* vote = std::get_if<VoteReply>(&message)) return vote->granted;
  if (const auto* append = std::get_if<AppendReply>(&message)) return append->success;
  if (const auto* snapshot = std::get_if<SnapshotReply>(&message)) return snapshot->success;
  return false;
}

struct Harness {
  std::string scenario;
  std::map<std::string, Node> nodes;
  std::uint64_t seq = 0;
  bool v2 = false;
  explicit Harness(std::string name, bool snapshots = false)
      : scenario(std::move(name)), v2(snapshots) {
    for (const auto* id : {"a", "b", "c"}) nodes.emplace(id, initial(config_for(id)));
  }

  Effects advance(const std::string& id, const Input& input) {
    const auto cfg = config_for(id);
    const Node before = nodes.at(id);
    auto [after, effects] = step(cfg, before, input);
    std::string trace =
        v2 ? trace_line_v2(cfg, after, input, seq++) : trace_line(cfg, after, input, seq++);
    require(trace.size() > 1 && trace.back() == '}', "invalid trace object");
    trace.pop_back();
    trace += ",\"scenario\":\"" + scenario + "\",\"pre\":";
    trace += projection(cfg, before, v2);
    trace += ",\"pending_before\":";
    trace += before.pending ? std::to_string(before.pending->token) : "null";
    trace += ",\"pending_after\":";
    trace += after.pending ? std::to_string(after.pending->token) : "null";
    trace += ",\"input_token\":";
    if (const auto* completed = std::get_if<PersistCompleted>(&input)) {
      trace += std::to_string(completed->token);
    } else if (const auto* failed = std::get_if<PersistFailed>(&input)) {
      trace += std::to_string(failed->token);
    } else {
      trace += "null";
    }
    trace += ",\"effects\":{\"persist\":";
    trace += effects.persist ? std::to_string(persist_token(effects)) : "null";
    trace += ",\"send\":[";
    for (std::size_t i = 0; i < effects.send_now.size(); ++i) {
      if (i != 0) trace += ',';
      const auto& message = effects.send_now[i];
      trace += "{\"kind\":\"";
      trace += message_kind(message.message);
      trace += "\",\"positive\":";
      trace += positive_response(message.message) ? "true" : "false";
      if (v2) {
        // Preserve the wire destination and request identity in diagnostic v2.
        // Model-to-core comparisons must not accept two indistinguishable
        // RVReq kinds sent to the same peer or with a different term.
        std::ostringstream destination;
        quoted(destination, message.to);
        trace += ",\"to\":" + destination.str();
        trace += ",\"term\":" +
                 std::to_string(std::visit([](const auto& wire) { return wire.term; },
                                           message.message));
        trace += ",\"request_id\":" +
                 std::to_string(std::visit([](const auto& wire) { return wire.request_id; },
                                           message.message));
        if (const auto* vote = std::get_if<RequestVote>(&message.message)) {
          std::ostringstream candidate;
          quoted(candidate, vote->candidate);
          trace += ",\"candidate\":" + candidate.str();
          trace += ",\"last_index\":" + std::to_string(vote->last_index);
          trace += ",\"last_term\":" + std::to_string(vote->last_term);
        }
        if (const auto* snapshot = std::get_if<SnapshotReply>(&message.message)) {
          trace += ",\"index\":" + std::to_string(snapshot->index);
          trace += ",\"request_id\":" + std::to_string(snapshot->request_id);
        }
      }
      trace += "}";
    }
    if (v2) {
      trace += "],\"persist_kind\":";
      if (!effects.persist)
        trace += "null";
      else if (effects.persist->kind == PersistKind::LocalCompaction)
        trace += "\"LocalCompaction\"";
      else if (effects.persist->kind == PersistKind::SnapshotInstall)
        trace += "\"SnapshotInstall\"";
      else
        trace += "\"StateUpdate\"";
      trace += ",\"apply\":";
    } else {
      trace += "],\"apply\":";
    }
    trace += std::to_string(effects.apply.size());
    trace += ",\"halt\":";
    trace += effects.halt ? "true" : "false";
    trace += "}}";
    std::cout << trace << '\n';
    nodes.at(id) = std::move(after);
    return effects;
  }
};

void election_replication_commit() {
  Harness h{"election-commit"};
  auto start = h.advance("a", ElectionTimeout{});
  require(start.persist.has_value() && start.send_now.empty(), "early RequestVote");
  auto timeout = h.advance("a", HeartbeatTimeout{});
  require(timeout.send_now.empty(), "pending election progressed before fsync");
  auto voted = h.advance("a", PersistCompleted{persist_token(start)});
  require(voted.send_now.size() == 2, "durable self vote did not release requests");
  RequestVote vote_request{};
  bool found_request = false;
  for (const auto& output : voted.send_now) {
    if (output.to == "b") {
      vote_request = std::get<RequestVote>(output.message);
      found_request = true;
    }
  }
  require(found_request, "missing vote request");
  auto grant = h.advance("b", RecvRequestVote{vote_request});
  require(grant.persist.has_value() && grant.send_now.empty(), "premature vote grant");
  auto reply = h.advance("b", PersistCompleted{persist_token(grant)});
  require(reply.send_now.size() == 1, "no durable vote reply");
  auto lead = h.advance("a", RecvVoteReply{std::get<VoteReply>(reply.send_now[0].message)});
  require(h.nodes["a"].volatile_state.role == Role::Leader, "no leader");
  require(lead.send_now.size() == 2, "no initial heartbeats");
  auto proposal = h.advance("a", ClientPropose{"v"});
  require(proposal.persist.has_value() && proposal.send_now.empty(), "append sent before fsync");
  auto flushed = h.advance("a", PersistCompleted{persist_token(proposal)});
  require(flushed.apply.empty(), "application emitted before commit");
  for (const auto& output : lead.send_now) {
    if (output.to == "b") {
      const auto& initial = std::get<AppendEntries>(output.message);
      h.advance("a", RecvAppendReply{AppendReply{initial.term, "b", true, initial.prev_index,
                                                 initial.request_id}});
    }
  }
  auto heartbeats = h.advance("a", HeartbeatTimeout{});
  AppendEntries append{};
  bool found_append = false;
  for (const auto& output : heartbeats.send_now) {
    if (output.to == "b") {
      append = std::get<AppendEntries>(output.message);
      found_append = true;
    }
  }
  require(found_append && append.entries.size() == 1, "missing replicated value");
  auto replicated = h.advance("b", RecvAppend{append});
  require(replicated.persist.has_value() && replicated.send_now.empty(), "premature append ack");
  auto persisted = h.advance("b", PersistCompleted{persist_token(replicated)});
  require(persisted.send_now.size() == 1, "no append acknowledgement");
  auto ack = h.advance("a", RecvAppendReply{std::get<AppendReply>(persisted.send_now[0].message)});
  require(ack.send_now.empty(), "unexpected immediate effect");
  auto commit = h.advance("a", AdvanceCommit{});
  require(commit.persist.has_value(), "missing quorum commit");
  auto committed = h.advance("a", PersistCompleted{persist_token(commit)});
  require(committed.apply.empty(), "applied before durable apply checkpoint");
  auto app = h.advance("a", ApplyCommitted{});
  require(app.persist.has_value() && app.apply.empty(), "premature application");
  auto applied = h.advance("a", PersistCompleted{persist_token(app)});
  require(applied.apply.size() == 1 && applied.apply[0].command == "v", "missing applied value");
}

void failure_recovery() {
  Harness h{"failed-persist"};
  auto grant = h.advance("b", RecvRequestVote{RequestVote{2, "a", 0, 0, 42}});
  require(grant.persist.has_value() && grant.send_now.empty(), "premature vote grant");
  const auto token = persist_token(grant);
  auto wrong = h.advance("b", PersistCompleted{token + 1});
  require(wrong.send_now.empty() && h.nodes["b"].pending.has_value(), "stale completion used");
  auto failed = h.advance("b", PersistFailed{token});
  require(failed.halt && failed.send_now.empty(), "write failure did not halt");
  auto blocked = h.advance("b", ElectionTimeout{});
  require(blocked.send_now.empty() && !blocked.persist, "halted node made progress");
  auto restarted = h.advance("b", Restart{});
  require(!restarted.halt && !h.nodes["b"].halted, "node did not restart");
  require(h.nodes["b"].durable.term == 0, "used unpersisted term");
}

void log_conflict() {
  Harness h{"conflict-protection"};
  h.nodes["b"].durable.term = 2;
  h.nodes["b"].durable.log = {{1, "committed"}, {2, "uncommitted"}};
  h.nodes["b"].durable.commit = 1;
  auto shorter = h.advance("b", RecvAppend{AppendEntries{2, "a", 0, 0, {}, 0, 100}});
  require(
      shorter.send_now.size() == 1 && std::get<AppendReply>(shorter.send_now[0].message).success,
      "short heartbeat refused");
  require(h.nodes["b"].durable.log.size() == 2, "short heartbeat truncated suffix");

  auto conflict =
      h.advance("b", RecvAppend{AppendEntries{2, "a", 0, 0, {{2, "conflict"}}, 0, 101}});
  require(
      conflict.send_now.size() == 1 && !std::get<AppendReply>(conflict.send_now[0].message).success,
      "committed prefix overwritten");
  require(h.nodes["b"].durable.log.front().command == "committed", "committed prefix lost");

  auto stale = h.advance("b", RecvRequestVote{RequestVote{1, "a", 0, 0, 102}});
  require(stale.send_now.size() == 1 && !std::get<VoteReply>(stale.send_now[0].message).granted,
          "stale term received vote");
}

void uncommitted_repair() {
  Harness h{"log-repair"};
  h.nodes["b"].durable.term = 2;
  h.nodes["b"].durable.log = {{1, "stale-1"}, {1, "stale-2"}};
  auto staged =
      h.advance("b", RecvAppend{AppendEntries{2, "a", 0, 0, {{2, "replacement"}}, 0, 201}});
  require(staged.persist.has_value() && staged.send_now.empty(),
          "follower sent acknowledgement before durable suffix repair");
  require(h.nodes["b"].durable.log.size() == 2, "unacknowledged truncate changed durable log");
  auto completed = h.advance("b", PersistCompleted{persist_token(staged)});
  require(h.nodes["b"].durable.log.size() == 1 &&
              h.nodes["b"].durable.log.front().command == "replacement",
          "conflicting uncommitted suffix was not repaired");
  require(completed.send_now.size() == 1 &&
              std::get<AppendReply>(completed.send_now[0].message).success,
          "durable repair did not produce append acknowledgement");
}

void stale_ack() {
  Harness h{"stale-ack"};
  auto& a = h.nodes["a"];
  a.durable.term = 3;
  a.durable.log = {{3, "v"}};
  a.volatile_state.role = Role::Leader;
  a.volatile_state.peers.at("b").next = 1;
  const auto first = h.advance("a", HeartbeatTimeout{});
  require(first.send_now.size() == 2, "missing probes");
  const auto first_msg = std::get<AppendEntries>(first.send_now[0].message);
  const auto retry = h.advance("a", HeartbeatTimeout{});
  require(retry.send_now.size() == 2, "missing retried probes");
  require(std::get<AppendEntries>(retry.send_now[0].message).request_id == first_msg.request_id,
          "retry replaced an outstanding request");
  h.advance("a", RecvAppendReply{AppendReply{3, "b", true, 999, first_msg.request_id}});
  const auto second = h.advance("a", HeartbeatTimeout{});
  const auto second_msg = std::get<AppendEntries>(second.send_now[0].message);
  require(first_msg.request_id != second_msg.request_id, "completed probe request ID reused");
  auto stale = h.advance("a", RecvAppendReply{AppendReply{3, "b", false, 0, first_msg.request_id}});
  require(stale.send_now.empty() && h.nodes["a"].volatile_state.peers.at("b").match == 1 &&
              h.nodes["a"].volatile_state.peers.at("b").next == 2,
          "obsolete response changed progress");
}

void snapshot_contract() {
  auto base = [](Harness& h) {
    auto& disk = h.nodes.at("b").durable;
    disk.term = 4;
    disk.snapshot_index = 5;
    disk.snapshot_term = 2;
    disk.log = {{3, "committed"}, {4, "anchor"}, {4, "tail"}};
    disk.commit = disk.applied = 6;
  };
  {
    Harness h{"compact", true};
    base(h);
    auto staged = h.advance("b", CompactCommitted{});
    require(staged.persist && staged.persist->kind == PersistKind::LocalCompaction,
            "missing compaction intent");
    h.advance("b", PersistCompleted{persist_token(staged) + 1});
    h.advance("b", PersistCompleted{persist_token(staged)});
    h.advance("b", Restart{});
  }
  for (const bool matching : {true, false}) {
    Harness h{matching ? "install-matching" : "install-conflict", true};
    base(h);
    const auto snapshot_term = matching ? 4u : 3u;
    const SnapshotRequest request{5, "a", 7, snapshot_term, 91};
    auto term = h.advance("b", RecvSnapshot{SnapshotRequest{5, "a", 7, snapshot_term, 90, false}});
    require(term.persist && term.send_now.empty(), "incomplete snapshot acknowledged");
    h.advance("b", PersistCompleted{persist_token(term)});
    auto install = h.advance("b", RecvSnapshot{request});
    require(install.persist && install.persist->kind == PersistKind::SnapshotInstall,
            "missing install intent");
    h.advance("b", PersistCompleted{persist_token(install) + 1});
    h.advance("b", PersistCompleted{persist_token(install)});
    h.advance("b", RecvSnapshot{request});  // Lost final ack: retry is idempotent.
    h.advance("b", Restart{});
  }
  for (const bool compact : {true, false}) {
    Harness h{compact ? "compact-failed" : "install-failed", true};
    base(h);
    const Input input = compact ? Input{CompactCommitted{}}
                                : Input{RecvSnapshot{SnapshotRequest{5, "a", 7, 4, 91}}};
    auto staged = h.advance("b", input);
    require(staged.persist.has_value(), "failure scenario did not stage persistence");
    h.advance("b", PersistFailed{persist_token(staged)});
    h.advance("b", HeartbeatTimeout{});
    h.advance("b", Restart{});
  }
}

// Schedule input uses actual emitted messages and current write tokens. It
// cannot manufacture replies or deliver effects before their durability gate.
void replay_inputs() {
  Harness h{"replay-inputs", true};
  struct Queued {
    std::string from;
    Outbound output;
  };
  std::vector<Queued> network;
  auto advance = [&](const std::string& actor, const Input& input) {
    require(h.nodes.contains(actor), "unknown replay actor");
    auto effects = h.advance(actor, input);
    for (auto& output : effects.send_now) network.push_back({actor, std::move(output)});
  };
  auto delivered = [](const WireMessage& message) -> Input {
    return std::visit(
        [](const auto& value) -> Input {
          using T = std::decay_t<decltype(value)>;
          if constexpr (std::is_same_v<T, RequestVote>) return RecvRequestVote{value};
          if constexpr (std::is_same_v<T, VoteReply>) return RecvVoteReply{value};
          if constexpr (std::is_same_v<T, AppendEntries>) return RecvAppend{value};
          if constexpr (std::is_same_v<T, AppendReply>) return RecvAppendReply{value};
          if constexpr (std::is_same_v<T, SnapshotReply>) return RecvSnapshotReply{value};
        },
        message);
  };
  std::string line;
  std::size_t count = 0;
  while (std::getline(std::cin, line)) {
    require(++count <= 10000 && line.size() <= 65536, "replay schedule limit exceeded");
    std::istringstream fields(line);
    std::string operation, actor;
    require(static_cast<bool>(fields >> operation >> actor), "incomplete replay command");
    require(h.nodes.contains(actor), "unknown replay actor");
    // Validate the entire line before invoking a transition.
    std::string destination, kind, value, extra;
    if (operation == "deliver" || operation == "drop" || operation == "duplicate") {
      require(static_cast<bool>(fields >> destination >> kind), "missing delivery context");
      require(h.nodes.contains(destination), "unknown replay destination");
    } else if (operation == "propose") {
      require(static_cast<bool>(fields >> std::quoted(value)), "missing proposal value");
    }
    require(!(fields >> extra), "unexpected replay fields");
    if (operation == "timeout") {
      advance(actor, ElectionTimeout{});
    } else if (operation == "heartbeat") {
      advance(actor, HeartbeatTimeout{});
    } else if (operation == "propose") {
      advance(actor, ClientPropose{value});
    } else if (operation == "advance") {
      advance(actor, AdvanceCommit{});
    } else if (operation == "apply") {
      advance(actor, ApplyCommitted{});
    } else if (operation == "compact") {
      advance(actor, CompactCommitted{});
    } else if (operation == "restart") {
      advance(actor, Restart{});
    } else if (operation == "complete" || operation == "fail") {
      const auto& pending = h.nodes.at(actor).pending;
      if (!pending) throw std::runtime_error("no pending replay write");
      const auto token = pending->token;
      if (operation == "complete")
        advance(actor, PersistCompleted{token});
      else
        advance(actor, PersistFailed{token});
    } else if (operation == "deliver" || operation == "drop" || operation == "duplicate") {
      const auto found = std::find_if(network.begin(), network.end(), [&](const auto& queued) {
        return queued.from == actor && queued.output.to == destination &&
               kind == message_kind(queued.output.message);
      });
      require(found != network.end(), "replay message is not available");
      const auto message = found->output.message;
      if (operation == "duplicate") {
        const auto copy = *found;
        network.push_back(copy);
      } else {
        network.erase(found);
        if (operation == "deliver") advance(destination, delivered(message));
      }
    } else {
      throw std::runtime_error("unknown replay operation");
    }
  }
  require(count != 0, "empty replay schedule");
  require(!std::cin.bad(), "cannot read replay schedule");
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc == 2 && std::string(argv[1]) == "--snapshot-contract") {
      snapshot_contract();
      return 0;
    }
    if (argc == 2 && std::string(argv[1]) == "--replay-inputs") {
      replay_inputs();
      return 0;
    }
    require(argc == 1, "unknown scenario runner arguments");
    election_replication_commit();
    failure_recovery();
    log_conflict();
    uncommitted_repair();
    stale_ack();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "raft conformance fixture failed: " << error.what() << '\n';
    return 1;
  }
}
