#include "choreoos/replication/replica.hpp"

#include "choreoos/protocol/codec.hpp"
#include "choreoos/protocol/command_codec.hpp"
#include "choreoos/state/machine.hpp"

namespace choreoos::replication {
namespace {

using choreoos::state::Error;
using choreoos::state::ErrorCode;
using choreoos::state::LogIndex;

}  // namespace

choreoos::state::Result<Replica> Replica::open(ReplicaConfig config) {
  choreoos::state::StoreOptions options;
  options.commit_on_append = false;
  auto store = choreoos::state::FileEngine::open(config.directory, options);
  if (!store) {
    return store.error();
  }
  auto speculative = choreoos::state::replay(store.value().log_events());
  if (!speculative) {
    return speculative.error();
  }
  Replica replica{std::move(config), std::move(store.value()), std::move(speculative.value())};
  const std::uint64_t next = replica.last_index() + 1;
  for (auto& peer : replica.peers_) {
    peer.next_index = next;
  }
  return replica;
}

Replica::Replica(ReplicaConfig config, choreoos::state::FileEngine store,
                 choreoos::state::ChoreographyState speculative)
    : config_(std::move(config)), store_(std::move(store)), speculative_(std::move(speculative)) {
  peers_.reserve(config_.peers.size());
  for (const auto& id : config_.peers) {
    peers_.push_back(Peer{id, 1, 0});
  }
}

void Replica::set_sender(Sender sender) { sender_ = std::move(sender); }

void Replica::set_commit_hook(std::function<void()> hook) { commit_hook_ = std::move(hook); }

choreoos::state::Result<EnqueueResult> Replica::enqueue(const choreoos::state::Command& command) {
  if (!is_leader()) {
    return Error{ErrorCode::NotLeader, "leader is " + config_.leader_id};
  }
  for (const auto& event : store_.log_events()) {
    if (event.command_id.value() == command.id.value()) {
      return EnqueueResult{event, true, event.index <= store_.commit_index(), config_.leader_id};
    }
  }
  auto proposed = choreoos::state::propose(speculative_, command);
  if (!proposed) {
    return proposed.error();
  }
  if (auto written = store_.append_event(proposed.value()); !written) {
    return written.error();
  }
  if (auto applied = choreoos::state::apply(speculative_, proposed.value()); !applied) {
    return applied.error();
  }
  replicate_all();
  advance_commit();
  return EnqueueResult{proposed.value(), false, proposed.value().index <= store_.commit_index(),
                       config_.leader_id};
}

void Replica::handle(const choreoos::protocol::Frame& frame) {
  using choreoos::protocol::MessageType;
  switch (frame.type) {
    case MessageType::AppendEntries: {
      auto message = choreoos::protocol::decode_append_entries(frame.payload);
      if (message) {
        on_append(message.value(), frame.correlation);
      }
      break;
    }
    case MessageType::AppendEntriesResponse: {
      auto message = choreoos::protocol::decode_append_response(frame.payload);
      if (message) {
        on_append_response(message.value());
      }
      break;
    }
    case MessageType::RequestVote: {
      // Elections start in the next milestone. A fixed leader never grants a vote.
      auto request = choreoos::protocol::decode_request_vote(frame.payload);
      choreoos::protocol::RequestVoteResponse response;
      response.term = store_.engine().state().term.value();
      response.voter_id = config_.id;
      response.vote_granted = false;
      if (request) {
        if (auto payload = choreoos::protocol::encode(response)) {
          send(request.value().candidate_id, MessageType::RequestVoteResponse, frame.correlation,
               payload.value());
        }
      }
      break;
    }
    case MessageType::InstallSnapshot: {
      // Log compaction is later. Catch-up copies missing log entries.
      auto request = choreoos::protocol::decode_install_snapshot(frame.payload);
      if (!request) {
        break;
      }
      choreoos::protocol::InstallSnapshotResponse response;
      response.term = store_.engine().state().term.value();
      response.follower_id = config_.id;
      response.success = false;
      if (auto payload = choreoos::protocol::encode(response)) {
        send(request.value().leader_id, MessageType::InstallSnapshotResponse, frame.correlation,
             payload.value());
      }
      break;
    }
    default:
      break;
  }
}

void Replica::heartbeat() {
  if (is_leader()) {
    replicate_all();
  }
}

NodeStatus Replica::status() const {
  NodeStatus status;
  status.node_id = config_.id;
  status.leader_id = config_.leader_id;
  status.role = is_leader() ? "leader" : "follower";
  status.term = store_.engine().state().term.value();
  status.commit_index = store_.commit_index().value();
  status.last_log_index = last_index();
  status.state_hash = choreoos::state::state_hash(store_.engine().state());
  status.canonical_state = choreoos::state::canonical_state(store_.engine().state());
  return status;
}

void Replica::replicate(Peer& peer) {
  choreoos::protocol::AppendEntries message;
  message.term = store_.engine().state().term.value();
  message.leader_id = config_.id;
  message.leader_commit = store_.commit_index().value();
  message.prev_log_index = peer.next_index == 0 ? 0 : peer.next_index - 1;
  if (message.prev_log_index > 0) {
    if (const auto* previous = find_index(message.prev_log_index)) {
      message.prev_log_term = previous->term.value();
    } else {
      message.prev_log_index = 0;
      message.prev_log_term = 0;
    }
  }
  for (const auto& event : store_.log_events()) {
    if (event.index.value() >= peer.next_index) {
      message.entries.push_back(event);
    }
  }
  if (auto payload = choreoos::protocol::encode(message)) {
    send(peer.id, choreoos::protocol::MessageType::AppendEntries, next_correlation_++,
         payload.value());
  }
}

void Replica::replicate_all() {
  for (auto& peer : peers_) {
    replicate(peer);
  }
}

void Replica::on_append(const choreoos::protocol::AppendEntries& message,
                        std::uint64_t correlation) {
  choreoos::protocol::AppendEntriesResponse response;
  response.term = store_.engine().state().term.value();
  response.follower_id = config_.id;
  response.hint_index = last_index();
  if (message.leader_id != config_.leader_id || is_leader()) {
    response.success = false;
    if (auto payload = choreoos::protocol::encode(response)) {
      send(message.leader_id, choreoos::protocol::MessageType::AppendEntriesResponse, correlation,
           payload.value());
    }
    return;
  }
  bool previous_matches = message.prev_log_index == 0;
  if (message.prev_log_index > last_index()) {
    response.hint_index = last_index();
  } else if (message.prev_log_index > 0) {
    const auto* previous = find_index(message.prev_log_index);
    if (previous != nullptr && previous->term.value() == message.prev_log_term) {
      previous_matches = true;
    } else {
      response.hint_index = message.prev_log_index - 1;
    }
  }
  if (previous_matches) {
    response.success = true;
    for (const auto& entry : message.entries) {
      const auto* existing = find_index(entry.index.value());
      if (existing != nullptr &&
          choreoos::state::canonical_event(*existing) == choreoos::state::canonical_event(entry)) {
        continue;
      }
      if (existing != nullptr && existing->index <= store_.commit_index()) {
        response.success = false;
        response.hint_index = store_.commit_index().value();
        break;
      }
      if (existing != nullptr) {
        auto truncated = store_.truncate_after(LogIndex::parse(entry.index.value() - 1).value());
        if (!truncated) {
          response.success = false;
          break;
        }
      } else if (entry.index.value() != last_index() + 1) {
        response.success = false;
        response.hint_index = last_index();
        break;
      }
      if (auto written = store_.append_event(entry); !written) {
        response.success = false;
        break;
      }
    }
    if (response.success) {
      rebuild_speculative();
      const std::uint64_t covered =
          message.leader_commit < last_index() ? message.leader_commit : last_index();
      if (covered > store_.commit_index().value()) {
        auto committed = store_.commit_through(LogIndex::parse(covered).value());
        if (!committed) {
          response.success = false;
        } else if (commit_hook_) {
          commit_hook_();
        }
      }
      response.match_index = last_index();
      response.hint_index = last_index();
    }
  }
  if (auto payload = choreoos::protocol::encode(response)) {
    send(message.leader_id, choreoos::protocol::MessageType::AppendEntriesResponse, correlation,
         payload.value());
  }
}

void Replica::on_append_response(const choreoos::protocol::AppendEntriesResponse& message) {
  if (!is_leader()) {
    return;
  }
  Peer* peer = nullptr;
  for (auto& candidate : peers_) {
    if (candidate.id == message.follower_id) {
      peer = &candidate;
    }
  }
  if (peer == nullptr) {
    return;
  }
  if (!message.success) {
    const std::uint64_t before = peer->next_index;
    const std::uint64_t hinted = message.hint_index + 1;
    if (hinted < peer->next_index) {
      peer->next_index = hinted == 0 ? 1 : hinted;
    } else if (peer->next_index > 1) {
      --peer->next_index;
    }
    if (peer->next_index < before) {
      replicate(*peer);
    }
    return;
  }
  if (message.match_index > peer->match_index) {
    peer->match_index = message.match_index;
  }
  peer->next_index = peer->match_index + 1;
  advance_commit();
}

void Replica::advance_commit() {
  if (!is_leader()) {
    return;
  }
  const std::uint64_t last = last_index();
  for (std::uint64_t index = last; index > store_.commit_index().value(); --index) {
    const auto* event = find_index(index);
    if (event == nullptr || event->term != store_.engine().state().term) {
      continue;
    }
    int copies = 1;
    for (const auto& peer : peers_) {
      if (peer.match_index >= index) {
        ++copies;
      }
    }
    if (copies >= majority()) {
      auto committed = store_.commit_through(LogIndex::parse(index).value());
      if (committed) {
        if (commit_hook_) {
          commit_hook_();
        }
        replicate_all();
      }
      return;
    }
  }
}

void Replica::rebuild_speculative() {
  auto replayed = choreoos::state::replay(store_.log_events());
  if (replayed) {
    speculative_ = std::move(replayed.value());
  }
}

void Replica::send(const std::string& peer, choreoos::protocol::MessageType type,
                   std::uint64_t correlation, std::string payload) {
  if (!sender_) {
    return;
  }
  choreoos::protocol::Frame frame;
  frame.type = type;
  frame.correlation = correlation;
  frame.payload = std::move(payload);
  sender_(peer, frame);
}

const choreoos::state::Event* Replica::find_index(std::uint64_t index) const {
  for (const auto& event : store_.log_events()) {
    if (event.index.value() == index) {
      return &event;
    }
  }
  return nullptr;
}

std::uint64_t Replica::last_index() const {
  const auto& events = store_.log_events();
  return events.empty() ? 0 : events.back().index.value();
}

int Replica::majority() const { return static_cast<int>(peers_.size() + 1) / 2 + 1; }

}  // namespace choreoos::replication
