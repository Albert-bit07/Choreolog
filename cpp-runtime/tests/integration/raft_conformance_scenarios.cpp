// Bounded, reproducible C++ transition scenarios. This is NOT a driver for
// the production Replica, nor a mechanically generated TLA+ behavior.
#include <cstdint>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
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

Config config_for(const std::string& id) {
  if (id == "a") return {"a", {"b", "c"}};
  if (id == "b") return {"b", {"a", "c"}};
  return {"c", {"a", "b"}};
}

std::string projection(const Config& cfg, const Node& node) {
  const auto document = trace_line(cfg, node, Restart{}, 0);
  const auto start = document.find("\"post\":");
  require(start != std::string::npos, "missing post projection");
  return document.substr(start + 7, document.size() - start - 8);
}

const char* message_kind(const WireMessage& message) {
  if (std::holds_alternative<RequestVote>(message)) return "RVReq";
  if (std::holds_alternative<VoteReply>(message)) return "RVResp";
  if (std::holds_alternative<AppendEntries>(message)) return "AEReq";
  return "AEResp";
}

bool positive_response(const WireMessage& message) {
  if (const auto* vote = std::get_if<VoteReply>(&message)) return vote->granted;
  if (const auto* append = std::get_if<AppendReply>(&message)) return append->success;
  return false;
}

struct Harness {
  std::string scenario;
  std::map<std::string, Node> nodes;
  std::uint64_t seq = 0;
  explicit Harness(std::string name) : scenario(std::move(name)) {
    for (const auto* id : {"a", "b", "c"}) nodes.emplace(id, initial(config_for(id)));
  }

  Effects advance(const std::string& id, const Input& input) {
    const auto cfg = config_for(id);
    const Node before = nodes.at(id);
    auto [after, effects] = step(cfg, before, input);
    std::string trace = trace_line(cfg, after, input, seq++);
    require(trace.size() > 1 && trace.back() == '}', "invalid trace object");
    trace.pop_back();
    trace += ",\"scenario\":\"" + scenario + "\",\"pre\":";
    trace += projection(cfg, before);
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
    trace += effects.persist ? std::to_string(effects.persist->token) : "null";
    trace += ",\"send\":[";
    for (std::size_t i = 0; i < effects.send_now.size(); ++i) {
      if (i != 0) trace += ',';
      const auto& message = effects.send_now[i];
      trace += "{\"kind\":\"";
      trace += message_kind(message.message);
      trace += "\",\"positive\":";
      trace += positive_response(message.message) ? "true" : "false";
      trace += "}";
    }
    trace += "],\"apply\":" + std::to_string(effects.apply.size());
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
  auto voted = h.advance("a", PersistCompleted{start.persist->token});
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
  auto reply = h.advance("b", PersistCompleted{grant.persist->token});
  require(reply.send_now.size() == 1, "no durable vote reply");
  auto lead = h.advance("a", RecvVoteReply{std::get<VoteReply>(reply.send_now[0].message)});
  require(h.nodes["a"].volatile_state.role == Role::Leader, "no leader");
  require(lead.send_now.size() == 2, "no initial heartbeats");
  auto proposal = h.advance("a", ClientPropose{"v"});
  require(proposal.persist.has_value() && proposal.send_now.empty(), "append sent before fsync");
  auto flushed = h.advance("a", PersistCompleted{proposal.persist->token});
  require(flushed.apply.empty(), "application emitted before commit");
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
  auto persisted = h.advance("b", PersistCompleted{replicated.persist->token});
  require(persisted.send_now.size() == 1, "no append acknowledgement");
  auto ack = h.advance("a", RecvAppendReply{std::get<AppendReply>(persisted.send_now[0].message)});
  require(ack.send_now.empty(), "unexpected immediate effect");
  auto commit = h.advance("a", AdvanceCommit{});
  require(commit.persist.has_value(), "missing quorum commit");
  auto committed = h.advance("a", PersistCompleted{commit.persist->token});
  require(committed.apply.empty(), "applied before durable apply checkpoint");
  auto app = h.advance("a", ApplyCommitted{});
  require(app.persist.has_value() && app.apply.empty(), "premature application");
  auto applied = h.advance("a", PersistCompleted{app.persist->token});
  require(applied.apply.size() == 1 && applied.apply[0].command == "v", "missing applied value");
}

void failure_recovery() {
  Harness h{"failed-persist"};
  auto grant = h.advance("b", RecvRequestVote{RequestVote{2, "a", 0, 0, 42}});
  require(grant.persist.has_value() && grant.send_now.empty(), "premature vote grant");
  const auto token = grant.persist->token;
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
  auto completed = h.advance("b", PersistCompleted{staged.persist->token});
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
  const auto second = h.advance("a", HeartbeatTimeout{});
  require(second.send_now.size() == 2, "missing replacement probes");
  const auto second_msg = std::get<AppendEntries>(second.send_now[0].message);
  require(first_msg.request_id != second_msg.request_id, "probe request ID reused");
  auto stale = h.advance("a", RecvAppendReply{AppendReply{3, first_msg.leader == "a" ? "b" : "b",
                                                          true, 999, first_msg.request_id}});
  require(stale.send_now.empty() && h.nodes["a"].volatile_state.peers.at("b").match == 0,
          "stale response inflated match");
  h.advance("a", RecvAppendReply{AppendReply{3, "b", true, 999, second_msg.request_id}});
  require(h.nodes["a"].volatile_state.peers.at("b").match == 1, "valid reply not counted");
}

}  // namespace

int main() {
  try {
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
