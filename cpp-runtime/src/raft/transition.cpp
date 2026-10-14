#include "choreoos/raft/transition.hpp"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <utility>

namespace choreoos::raft {
namespace {

std::size_t quorum(const Config& config) { return (config.peers.size() + 1) / 2 + 1; }

// The transport must authenticate peer identity; the core additionally refuses
// protocol input attributed to a non-member of this fixed cluster.
bool known_peer(const Node& state, const std::string& id) {
  return state.volatile_state.peers.count(id) != 0;
}

std::uint64_t last_term(const Durable& state) {
  return state.log.empty() ? 0 : state.log.back().term;
}

bool up_to_date(const Durable& self, std::uint64_t term, std::uint64_t index) {
  const auto ours = last_term(self);
  return term > ours || (term == ours && index >= self.log.size());
}

void begin_write(Node& state, Effects& effects, Durable target, std::vector<Outbound> after = {},
                 std::vector<Entry> apply_after = {}, bool self_vote = false) {
  const auto token = state.next_token++;
  effects.persist = PersistRequest{token, target};
  state.pending =
      PendingWrite{token, std::move(target), std::move(after), std::move(apply_after), self_vote};
}

void send_append(const Config& config, Node& state, const Durable& log, const std::string& peer,
                 std::vector<Outbound>& out) {
  auto it = state.volatile_state.peers.find(peer);
  if (it == state.volatile_state.peers.end() || it->second.in_flight) {
    return;
  }
  auto& progress = it->second;
  const auto next = std::max<std::uint64_t>(1, progress.next);
  if (next > log.log.size() + 1) {
    return;
  }
  const auto prev = next - 1;
  std::vector<Entry> entries;
  // One-entry batches make the request's acknowledged index unambiguous.
  if (next <= log.log.size()) {
    entries.push_back(log.log[static_cast<std::size_t>(next - 1)]);
  }
  const auto id = state.next_request_id++;
  progress.in_flight = Probe{id, log.term, prev, entries.size()};
  AppendEntries message{log.term,
                        config.self,
                        prev,
                        prev == 0 ? 0 : log.log[static_cast<std::size_t>(prev - 1)].term,
                        std::move(entries),
                        log.commit,
                        id};
  out.push_back(Outbound{peer, std::move(message)});
}

void become_leader(const Config& config, Node& state, Effects& effects) {
  state.volatile_state.role = Role::Leader;
  ++state.volatile_state.leadership_epoch;
  for (auto& [peer, progress] : state.volatile_state.peers) {
    progress = PeerProgress{state.durable.log.size() + 1, 0, std::nullopt};
    send_append(config, state, state.durable, peer, effects.send_now);
  }
}

void step_down(Node& state) {
  state.volatile_state.role = Role::Follower;
  state.volatile_state.votes.clear();
  for (auto& [peer, progress] : state.volatile_state.peers) {
    (void)peer;
    progress.in_flight.reset();
  }
}

std::uint64_t highest_committable(const Config& config, const Node& state) {
  const auto& d = state.durable;
  for (auto n = static_cast<std::uint64_t>(d.log.size()); n > d.commit; --n) {
    if (d.log[static_cast<std::size_t>(n - 1)].term != d.term) {
      continue;  // Raft Figure 8: do not commit an old-term entry by counting.
    }
    std::size_t votes = 1;  // Only the durable leader log counts itself.
    for (const auto& [peer, progress] : state.volatile_state.peers) {
      (void)peer;
      if (progress.match >= n) {
        ++votes;
      }
    }
    if (votes >= quorum(config)) {
      return n;
    }
  }
  return d.commit;
}

}  // namespace

Node initial(const Config& config) {
  Node state;
  for (const auto& peer : config.peers) {
    state.volatile_state.peers.try_emplace(peer);
  }
  return state;
}

std::pair<Node, Effects> step(const Config& config, Node state, const Input& input) {
  Effects effects;

  if (const auto* completed = std::get_if<PersistCompleted>(&input)) {
    if (state.pending && state.pending->token == completed->token && !state.halted) {
      PendingWrite pending = std::move(*state.pending);
      state.pending.reset();
      state.durable = std::move(pending.target);
      effects.send_now = std::move(pending.after);
      effects.apply = std::move(pending.apply_after);
      if (pending.self_vote) {
        state.volatile_state.votes.insert(config.self);
        if (state.volatile_state.votes.size() >= quorum(config)) {
          become_leader(config, state, effects);
        }
      }
    }
    return {std::move(state), std::move(effects)};
  }
  if (const auto* failed = std::get_if<PersistFailed>(&input)) {
    if (state.pending && state.pending->token == failed->token && !state.halted) {
      state.pending.reset();
      step_down(state);
      state.halted = true;
      effects.halt = true;
    }
    return {std::move(state), std::move(effects)};
  }
  if (std::holds_alternative<Restart>(input)) {
    // The host must rehydrate durable from disk before constructing this Node.
    // Staged changes, in-flight probes and queued effects never survive.
    state.pending.reset();
    state.halted = false;
    state.volatile_state = initial(config).volatile_state;
    return {std::move(state), std::move(effects)};
  }
  if (state.halted || state.pending) {
    // One durable operation in flight per node. Never act on staged state.
    return {std::move(state), std::move(effects)};
  }

  if (std::holds_alternative<ElectionTimeout>(input)) {
    if (state.volatile_state.role != Role::Leader &&
        state.durable.term != std::numeric_limits<std::uint64_t>::max()) {
      Durable target = state.durable;
      ++target.term;
      target.voted_for = config.self;
      step_down(state);
      state.volatile_state.role = Role::Candidate;
      std::vector<Outbound> after;
      for (const auto& peer : config.peers) {
        after.push_back(Outbound{peer, RequestVote{target.term, config.self,
                                                   static_cast<std::uint64_t>(target.log.size()),
                                                   last_term(target), state.next_request_id++}});
      }
      begin_write(state, effects, std::move(target), std::move(after), {}, true);
    }
    return {std::move(state), std::move(effects)};
  }

  if (const auto* request = std::get_if<RecvRequestVote>(&input)) {
    const auto& m = request->message;
    if (!known_peer(state, m.candidate)) {
      return {std::move(state), std::move(effects)};
    }
    if (m.term < state.durable.term) {
      effects.send_now.push_back(
          Outbound{m.candidate, VoteReply{state.durable.term, config.self, false, m.request_id}});
      return {std::move(state), std::move(effects)};
    }
    Durable target = state.durable;
    if (m.term > target.term) {
      target.term = m.term;
      target.voted_for.reset();
      step_down(state);
    }
    const bool grant = (!target.voted_for || target.voted_for == m.candidate) &&
                       up_to_date(target, m.last_term, m.last_index);
    if (grant) {
      target.voted_for = m.candidate;
    }
    Outbound reply{m.candidate, VoteReply{target.term, config.self, grant, m.request_id}};
    if (target != state.durable) {
      begin_write(state, effects, std::move(target), {std::move(reply)});
    } else {
      effects.send_now.push_back(std::move(reply));
    }
    return {std::move(state), std::move(effects)};
  }

  if (const auto* response = std::get_if<RecvVoteReply>(&input)) {
    const auto& m = response->message;
    if (!known_peer(state, m.voter)) {
      return {std::move(state), std::move(effects)};
    }
    if (m.term > state.durable.term) {
      Durable target = state.durable;
      target.term = m.term;
      target.voted_for.reset();
      step_down(state);
      begin_write(state, effects, std::move(target));
    } else if (m.term == state.durable.term && m.granted &&
               state.volatile_state.role == Role::Candidate &&
               state.volatile_state.peers.count(m.voter) != 0) {
      state.volatile_state.votes.insert(m.voter);
      if (state.volatile_state.votes.size() >= quorum(config)) {
        become_leader(config, state, effects);
      }
    }
    return {std::move(state), std::move(effects)};
  }

  if (const auto* proposal = std::get_if<ClientPropose>(&input)) {
    if (state.volatile_state.role == Role::Leader && !proposal->command.empty()) {
      Durable target = state.durable;
      target.log.push_back(Entry{target.term, proposal->command});
      std::vector<Outbound> after;
      for (const auto& peer : config.peers) {
        send_append(config, state, target, peer, after);
      }
      begin_write(state, effects, std::move(target), std::move(after));
    }
    return {std::move(state), std::move(effects)};
  }

  if (std::holds_alternative<HeartbeatTimeout>(input)) {
    if (state.volatile_state.role == Role::Leader) {
      for (auto& [peer, progress] : state.volatile_state.peers) {
        // Supersede a lost probe. Late replies to the old ID are ignored.
        progress.in_flight.reset();
        send_append(config, state, state.durable, peer, effects.send_now);
      }
    }
    return {std::move(state), std::move(effects)};
  }

  if (const auto* incoming = std::get_if<RecvAppend>(&input)) {
    const auto& m = incoming->message;
    if (!known_peer(state, m.leader)) {
      return {std::move(state), std::move(effects)};
    }
    if (m.term < state.durable.term) {
      effects.send_now.push_back(Outbound{
          m.leader,
          AppendReply{state.durable.term, config.self, false,
                      static_cast<std::uint64_t>(state.durable.log.size()), m.request_id}});
      return {std::move(state), std::move(effects)};
    }
    Durable target = state.durable;
    if (m.term > target.term) {
      target.term = m.term;
      target.voted_for.reset();
    }
    step_down(state);
    const bool prev_ok =
        m.prev_index <= target.log.size() &&
        (m.prev_index == 0 ||
         target.log[static_cast<std::size_t>(m.prev_index - 1)].term == m.prev_term);
    bool success = prev_ok;
    std::uint64_t hint = static_cast<std::uint64_t>(target.log.size());
    if (prev_ok) {
      for (std::size_t k = 0; k < m.entries.size(); ++k) {
        const auto index = m.prev_index + static_cast<std::uint64_t>(k);
        if (index < target.log.size() &&
            target.log[static_cast<std::size_t>(index)] != m.entries[k]) {
          if (index < target.commit) {
            success = false;
            hint = target.commit;
            break;
          }
          target.log.resize(static_cast<std::size_t>(index));
        }
        if (index == target.log.size()) {
          target.log.push_back(m.entries[k]);
        }
      }
      if (success) {
        const auto covered = m.prev_index + static_cast<std::uint64_t>(m.entries.size());
        target.commit = std::max(target.commit, std::min(m.leader_commit, covered));
      }
    }
    if (!success && m.prev_index > 0 && !prev_ok && m.prev_index <= target.log.size()) {
      hint = m.prev_index - 1;
    }
    if (!success) {
      // Do not persist an incomplete/conflicting append, only a higher term.
      target.log = state.durable.log;
      target.commit = state.durable.commit;
    }
    Outbound reply{m.leader, AppendReply{target.term, config.self, success, hint, m.request_id}};
    if (target != state.durable) {
      begin_write(state, effects, std::move(target), {std::move(reply)});
    } else {
      effects.send_now.push_back(std::move(reply));
    }
    return {std::move(state), std::move(effects)};
  }

  if (const auto* incoming = std::get_if<RecvAppendReply>(&input)) {
    const auto& m = incoming->message;
    if (!known_peer(state, m.follower)) {
      return {std::move(state), std::move(effects)};
    }
    if (m.term > state.durable.term) {
      Durable target = state.durable;
      target.term = m.term;
      target.voted_for.reset();
      step_down(state);
      begin_write(state, effects, std::move(target));
    } else if (state.volatile_state.role == Role::Leader && m.term == state.durable.term) {
      auto it = state.volatile_state.peers.find(m.follower);
      if (it != state.volatile_state.peers.end() && it->second.in_flight &&
          it->second.in_flight->request_id == m.request_id &&
          it->second.in_flight->term == m.term) {
        auto& progress = it->second;
        const auto probe = *progress.in_flight;
        progress.in_flight.reset();
        if (m.success) {
          // Match derives from the SENT request, never the follower's extra suffix.
          progress.match = std::max(progress.match, probe.prev + probe.count);
          progress.next = std::max(progress.next, progress.match + 1);
        } else {
          // Only the current probe may change next; never regress below match.
          const auto previous = progress.next;
          const auto hint_next =
              m.hint == std::numeric_limits<std::uint64_t>::max() ? previous : m.hint + 1;
          if (previous > 1) {
            progress.next = std::max(progress.match + 1, std::min(previous - 1, hint_next));
          }
        }
      }
    }
    return {std::move(state), std::move(effects)};
  }

  if (std::holds_alternative<AdvanceCommit>(input)) {
    if (state.volatile_state.role == Role::Leader) {
      const auto n = highest_committable(config, state);
      if (n > state.durable.commit) {
        Durable target = state.durable;
        target.commit = n;
        begin_write(state, effects, std::move(target));
      }
    }
    return {std::move(state), std::move(effects)};
  }

  if (std::holds_alternative<ApplyCommitted>(input) &&
      state.durable.applied < state.durable.commit &&
      state.durable.applied < state.durable.log.size()) {
    Durable target = state.durable;
    ++target.applied;
    std::vector<Entry> after{target.log[static_cast<std::size_t>(target.applied - 1)]};
    begin_write(state, effects, std::move(target), {}, std::move(after));
  }
  return {std::move(state), std::move(effects)};
}

}  // namespace choreoos::raft
