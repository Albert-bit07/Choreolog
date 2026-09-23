#include "choreoos/replication/replica.hpp"

#include <algorithm>
#include <sstream>

#include "choreoos/protocol/codec.hpp"
#include "choreoos/protocol/command_codec.hpp"
#include "choreoos/state/machine.hpp"
#include "choreoos/storage/metadata.hpp"

namespace choreoos::replication {
namespace {

using choreoos::state::Error;
using choreoos::state::ErrorCode;
using choreoos::state::LogIndex;

}  // namespace

choreoos::state::Result<Replica> Replica::open(ReplicaConfig config) {
  choreoos::state::StoreOptions options;
  options.commit_on_append = false;
  options.snapshot_every = config.snapshot_every;
  auto store = choreoos::state::FileEngine::open(config.directory, options);
  if (!store) {
    return store.error();
  }
  auto speculative = choreoos::state::replay(store.value().log_events());
  if (!speculative) {
    return speculative.error();
  }
  Replica replica{std::move(config), std::move(store.value()), std::move(speculative.value())};
  const auto metadata = replica.store_.consensus_metadata();
  replica.current_term_ = metadata.term;
  if (metadata.voted_for) {
    replica.voted_for_ = metadata.voted_for->value();
  }
  if (!replica.config_.elections) {
    replica.role_ = replica.config_.id == replica.config_.leader_id ? Role::Leader : Role::Follower;
    if (replica.store_.engine().state().term.value() > replica.current_term_.value()) {
      replica.current_term_ = replica.store_.engine().state().term;
    }
  } else {
    // A restarted node is a follower until it hears a leader or wins a vote.
    // The config leader id is not a term of office.
    replica.role_ = Role::Follower;
    replica.config_.leader_id.clear();
  }
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
    peers_.push_back(Peer{id, 1, 0, 0});
  }
  rng_ = config_.rng_seed == 0 ? 1 : config_.rng_seed;
  for (unsigned char ch : config_.id) {
    rng_ = rng_ * 131u + ch;
  }
  if (rng_ == 0) {
    rng_ = 1;
  }
  if (config_.election_timeout_max < config_.election_timeout_min) {
    std::swap(config_.election_timeout_min, config_.election_timeout_max);
  }
  election_timeout_ = draw_timeout();
  role_ = (!config_.elections && config_.id == config_.leader_id) ? Role::Leader : Role::Follower;
}

void Replica::set_sender(Sender sender) { sender_ = std::move(sender); }

void Replica::set_commit_hook(std::function<void()> hook) { commit_hook_ = std::move(hook); }

void Replica::set_election_bounds(int min_ticks, int max_ticks) {
  config_.election_timeout_min = min_ticks;
  config_.election_timeout_max = max_ticks;
  if (config_.election_timeout_max < config_.election_timeout_min) {
    std::swap(config_.election_timeout_min, config_.election_timeout_max);
  }
  election_timeout_ = draw_timeout();
  election_elapsed_ = 0;
}

bool Replica::is_leader() const noexcept {
  if (config_.elections) {
    return role_ == Role::Leader;
  }
  return config_.id == config_.leader_id;
}

choreoos::state::Result<EnqueueResult> Replica::enqueue(const choreoos::state::Command& command) {
  if (!is_leader()) {
    return Error{ErrorCode::NotLeader, "leader is " + config_.leader_id};
  }
  for (const auto& event : store_.log_events()) {
    if (event.command_id.value() == command.id.value()) {
      return EnqueueResult{event, true, event.index <= store_.commit_index(), config_.leader_id};
    }
  }
  // Propose against speculative state, which includes uncommitted entries, but
  // stamp the event with the election term rather than a stale applied term.
  auto proposed = choreoos::state::propose(speculative_, command);
  if (!proposed) {
    return proposed.error();
  }
  if (config_.elections && proposed.value().term.value() != current_term_.value()) {
    auto restamped = proposed.value();
    auto term = choreoos::state::Term::parse(current_term_.value());
    if (!term) {
      return term.error();
    }
    restamped.term = term.value();
    if (auto applied = choreoos::state::apply(speculative_, restamped); !applied) {
      // propose() already applied the original term to a copy only. Rebuild
      // and apply the restamped event so the speculative term matches the log.
      rebuild_speculative();
      if (auto retry = choreoos::state::apply(speculative_, restamped); !retry) {
        return retry.error();
      }
    }
    proposed = std::move(restamped);
  }
  if (auto written = store_.append_event(proposed.value()); !written) {
    return written.error();
  }
  if (!config_.elections) {
    if (auto applied = choreoos::state::apply(speculative_, proposed.value()); !applied) {
      return applied.error();
    }
  } else if (speculative_.last_applied != proposed.value().index) {
    if (auto applied = choreoos::state::apply(speculative_, proposed.value()); !applied) {
      return applied.error();
    }
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
      auto request = choreoos::protocol::decode_request_vote(frame.payload);
      if (request) {
        on_vote(request.value(), frame.correlation);
      }
      break;
    }
    case MessageType::RequestVoteResponse: {
      auto response = choreoos::protocol::decode_vote_response(frame.payload);
      if (response) {
        on_vote_response(response.value());
      }
      break;
    }
    case MessageType::InstallSnapshot: {
      auto request = choreoos::protocol::decode_install_snapshot(frame.payload);
      if (request) {
        on_install(request.value(), frame.correlation);
      }
      break;
    }
    case MessageType::InstallSnapshotResponse: {
      auto response = choreoos::protocol::decode_snapshot_response(frame.payload);
      if (response) {
        on_install_response(response.value());
      }
      break;
    }
    default:
      break;
  }
}

void Replica::tick() {
  if (!config_.elections) {
    if (is_leader()) {
      replicate_all();
    }
    return;
  }
  if (role_ == Role::Leader) {
    replicate_all();
    return;
  }
  ++election_elapsed_;
  if (election_elapsed_ >= election_timeout_) {
    start_election();
  }
}

void Replica::heartbeat() { tick(); }

NodeStatus Replica::status() const {
  NodeStatus status;
  status.node_id = config_.id;
  status.leader_id = config_.leader_id;
  status.role = role_name();
  status.term = current_term_.value();
  status.commit_index = store_.commit_index().value();
  status.last_log_index = last_index();
  status.state_hash = choreoos::state::state_hash(store_.engine().state());
  status.canonical_state = choreoos::state::canonical_state(store_.engine().state());
  return status;
}

void Replica::replicate(Peer& peer) {
  if (const auto& snapshot = store_.latest_snapshot()) {
    const bool behind_snapshot = peer.match_index < snapshot->index.value();
    const bool prev_missing = peer.next_index <= 1 ||
                              find_index(peer.next_index == 0 ? 0 : peer.next_index - 1) == nullptr;
    if (behind_snapshot && prev_missing && snapshot->index.value() > 0) {
      choreoos::protocol::InstallSnapshot message;
      message.term = current_term_.value();
      message.leader_id = config_.id;
      message.last_included_index = snapshot->index.value();
      message.last_included_term = snapshot->term.value();
      message.state_hash = snapshot->state_hash;
      for (const auto& event : snapshot->events) {
        if (!message.payload.empty()) {
          message.payload.push_back('\n');
        }
        message.payload += choreoos::state::canonical_event(event);
      }
      peer.snapshot_index = snapshot->index.value();
      if (auto payload = choreoos::protocol::encode(message)) {
        send(peer.id, choreoos::protocol::MessageType::InstallSnapshot, next_correlation_++,
             payload.value());
      }
      return;
    }
  }

  choreoos::protocol::AppendEntries message;
  message.term = current_term_.value();
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
  response.term = current_term_.value();
  response.follower_id = config_.id;
  response.hint_index = last_index();
  if (config_.elections) {
    if (message.term < current_term_.value()) {
      if (auto payload = choreoos::protocol::encode(response)) {
        send(message.leader_id, choreoos::protocol::MessageType::AppendEntriesResponse, correlation,
             payload.value());
      }
      return;
    }
    if (message.term > current_term_.value()) {
      step_down(message.term);
    }
    if (role_ != Role::Follower) {
      role_ = Role::Follower;
      ++metrics_.role_changes;
      ++metrics_.step_downs;
    }
    config_.leader_id = message.leader_id;
    election_elapsed_ = 0;
    response.term = current_term_.value();
  } else if (message.leader_id != config_.leader_id || is_leader()) {
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
  response.term = current_term_.value();
  if (auto payload = choreoos::protocol::encode(response)) {
    send(message.leader_id, choreoos::protocol::MessageType::AppendEntriesResponse, correlation,
         payload.value());
  }
}

void Replica::on_append_response(const choreoos::protocol::AppendEntriesResponse& message) {
  if (config_.elections && message.term > current_term_.value()) {
    step_down(message.term);
    return;
  }
  if (config_.elections && message.term < current_term_.value()) {
    return;
  }
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

void Replica::on_vote(const choreoos::protocol::RequestVote& message, std::uint64_t correlation) {
  choreoos::protocol::RequestVoteResponse response;
  response.voter_id = config_.id;
  response.vote_granted = false;
  if (!config_.elections) {
    response.term = current_term_.value();
    if (auto payload = choreoos::protocol::encode(response)) {
      send(message.candidate_id, choreoos::protocol::MessageType::RequestVoteResponse, correlation,
           payload.value());
    }
    return;
  }
  if (message.term > current_term_.value()) {
    step_down(message.term);
  }
  response.term = current_term_.value();
  const bool current = message.term == current_term_.value();
  const bool vote_free = !voted_for_ || *voted_for_ == message.candidate_id;
  const bool up_to_date = log_is_up_to_date(message.last_log_term, message.last_log_index);
  if (current && vote_free && up_to_date) {
    voted_for_ = message.candidate_id;
    if (persist_vote()) {
      response.vote_granted = true;
      election_elapsed_ = 0;
      ++metrics_.votes_granted;
    }
  }
  if (auto payload = choreoos::protocol::encode(response)) {
    send(message.candidate_id, choreoos::protocol::MessageType::RequestVoteResponse, correlation,
         payload.value());
  }
}

void Replica::on_vote_response(const choreoos::protocol::RequestVoteResponse& message) {
  if (!config_.elections) {
    return;
  }
  if (message.term > current_term_.value()) {
    step_down(message.term);
    return;
  }
  if (role_ != Role::Candidate || message.term != current_term_.value() || !message.vote_granted) {
    return;
  }
  if (std::find(votes_from_.begin(), votes_from_.end(), message.voter_id) != votes_from_.end()) {
    return;
  }
  votes_from_.push_back(message.voter_id);
  if (static_cast<int>(votes_from_.size()) >= majority()) {
    become_leader();
  }
}

void Replica::on_install(const choreoos::protocol::InstallSnapshot& message,
                         std::uint64_t correlation) {
  choreoos::protocol::InstallSnapshotResponse response;
  response.follower_id = config_.id;
  response.success = false;
  response.term = current_term_.value();
  if (config_.elections && message.term < current_term_.value()) {
    if (auto payload = choreoos::protocol::encode(response)) {
      send(message.leader_id, choreoos::protocol::MessageType::InstallSnapshotResponse, correlation,
           payload.value());
    }
    return;
  }
  if (config_.elections && message.term > current_term_.value()) {
    step_down(message.term);
  }
  if (config_.elections) {
    config_.leader_id = message.leader_id;
    if (role_ != Role::Follower) {
      role_ = Role::Follower;
      ++metrics_.role_changes;
    }
    election_elapsed_ = 0;
  } else if (message.leader_id != config_.leader_id || is_leader()) {
    if (auto payload = choreoos::protocol::encode(response)) {
      send(message.leader_id, choreoos::protocol::MessageType::InstallSnapshotResponse, correlation,
           payload.value());
    }
    return;
  }

  auto index = LogIndex::parse(message.last_included_index);
  auto term = choreoos::state::Term::parse(message.last_included_term);
  if (index && term && message.state_hash.size() == 16) {
    choreoos::storage::Snapshot snapshot;
    snapshot.index = index.value();
    snapshot.term = term.value();
    snapshot.state_hash = message.state_hash;
    std::istringstream lines{message.payload};
    std::string line;
    bool parsed = true;
    while (std::getline(lines, line)) {
      if (line.empty()) {
        continue;
      }
      auto event = choreoos::state::parse_event(line);
      if (!event) {
        parsed = false;
        break;
      }
      snapshot.events.push_back(event.value());
    }
    if (parsed && !snapshot.events.empty() && snapshot.events.back().index == snapshot.index &&
        snapshot.events.back().term == snapshot.term) {
      if (auto installed = store_.install_snapshot(snapshot); installed) {
        rebuild_speculative();
        response.success = true;
        if (commit_hook_) {
          commit_hook_();
        }
      }
    }
  }
  response.term = current_term_.value();
  if (auto payload = choreoos::protocol::encode(response)) {
    send(message.leader_id, choreoos::protocol::MessageType::InstallSnapshotResponse, correlation,
         payload.value());
  }
}

void Replica::on_install_response(const choreoos::protocol::InstallSnapshotResponse& message) {
  if (config_.elections && message.term > current_term_.value()) {
    step_down(message.term);
    return;
  }
  if (!is_leader() || !message.success) {
    return;
  }
  for (auto& peer : peers_) {
    if (peer.id != message.follower_id) {
      continue;
    }
    if (peer.snapshot_index > peer.match_index) {
      peer.match_index = peer.snapshot_index;
    }
    peer.next_index = peer.match_index + 1;
    advance_commit();
    replicate(peer);
    return;
  }
}

void Replica::advance_commit() {
  if (!is_leader()) {
    return;
  }
  const std::uint64_t last = last_index();
  for (std::uint64_t index = last; index > store_.commit_index().value(); --index) {
    const auto* event = find_index(index);
    // Only a current-term entry may move the commit index. Everything beneath
    // it commits with it. A stale leader's old-term suffix stays uncommitted.
    if (event == nullptr || event->term.value() != current_term_.value()) {
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

void Replica::start_election() {
  auto next = current_term_.next();
  if (!next) {
    return;
  }
  auto self = choreoos::state::NodeId::parse(config_.id);
  if (!self) {
    return;
  }
  if (auto stored = store_.persist_consensus(next.value(), self.value()); !stored) {
    return;
  }
  const Role previous = role_;
  role_ = Role::Candidate;
  current_term_ = next.value();
  voted_for_ = config_.id;
  votes_from_.clear();
  votes_from_.push_back(config_.id);
  config_.leader_id.clear();
  ++metrics_.elections_started;
  if (previous != Role::Candidate) {
    ++metrics_.role_changes;
  }
  election_elapsed_ = 0;
  election_timeout_ = draw_timeout();
  if (static_cast<int>(votes_from_.size()) >= majority()) {
    become_leader();
    return;
  }
  choreoos::protocol::RequestVote request;
  request.term = current_term_.value();
  request.candidate_id = config_.id;
  request.last_log_index = last_index();
  request.last_log_term = last_term();
  if (auto payload = choreoos::protocol::encode(request)) {
    for (const auto& peer : peers_) {
      send(peer.id, choreoos::protocol::MessageType::RequestVote, next_correlation_++,
           payload.value());
    }
  }
}

void Replica::become_leader() {
  if (role_ == Role::Leader) {
    return;
  }
  role_ = Role::Leader;
  config_.leader_id = config_.id;
  ++metrics_.role_changes;
  const std::uint64_t next_index = last_index() + 1;
  for (auto& peer : peers_) {
    peer.next_index = next_index;
    peer.match_index = 0;
    peer.snapshot_index = 0;
  }

  const std::string serial =
      std::to_string(current_term_.value()) + "-" + std::to_string(next_index);
  auto event_id = choreoos::state::EventId::parse("noop-" + config_.id + "-" + serial);
  auto command_id = choreoos::state::CommandId::parse("noop-" + serial);
  auto choreography =
      speculative_.id ? choreoos::state::Result<choreoos::state::ChoreographyId>{*speculative_.id}
                      : choreoos::state::ChoreographyId::parse("system");
  auto index = LogIndex::parse(next_index);
  auto term = choreoos::state::Term::parse(current_term_.value());
  auto tick = choreoos::state::MusicalTick::from_ticks(0);
  if (!event_id || !command_id || !choreography || !index || !term || !tick) {
    role_ = Role::Candidate;
    return;
  }
  choreoos::state::Event noop{
      event_id.value(),
      command_id.value(),
      choreography.value(),
      choreoos::state::EventType::NoOp,
      choreoos::state::kCurrentSchemaVersion,
      tick.value(),
      index.value(),
      term.value(),
      choreoos::state::CreateChoreographyPayload{
          choreography.value(), choreoos::state::StageBounds::from_mm(1, 1).value(),
          choreoos::state::OverlapPolicy::Forbidden, 0}};
  if (auto written = store_.append_event(noop); !written) {
    role_ = Role::Candidate;
    return;
  }
  if (auto applied = choreoos::state::apply(speculative_, noop); !applied) {
    role_ = Role::Candidate;
    return;
  }
  replicate_all();
  advance_commit();
}

bool Replica::step_down(std::uint64_t term) {
  if (term < current_term_.value()) {
    return false;
  }
  if (term == current_term_.value()) {
    if (role_ == Role::Leader) {
      role_ = Role::Follower;
      ++metrics_.role_changes;
      ++metrics_.step_downs;
      config_.leader_id.clear();
    }
    return true;
  }
  auto parsed = choreoos::state::Term::parse(term);
  if (!parsed) {
    return false;
  }
  if (auto stored = store_.persist_consensus(parsed.value(), std::nullopt); !stored) {
    return false;
  }
  current_term_ = parsed.value();
  voted_for_.reset();
  votes_from_.clear();
  if (role_ != Role::Follower) {
    ++metrics_.role_changes;
    ++metrics_.step_downs;
  }
  role_ = Role::Follower;
  config_.leader_id.clear();
  return true;
}

choreoos::state::Result<void> Replica::persist_vote() {
  auto term = choreoos::state::Term::parse(current_term_.value());
  if (!term) {
    return term.error();
  }
  std::optional<choreoos::state::NodeId> vote;
  if (voted_for_) {
    auto parsed = choreoos::state::NodeId::parse(*voted_for_);
    if (!parsed) {
      return parsed.error();
    }
    vote = parsed.value();
  }
  return store_.persist_consensus(term.value(), vote);
}

bool Replica::log_is_up_to_date(std::uint64_t last_log_term, std::uint64_t last_log_index) const {
  const std::uint64_t ours = last_term();
  if (last_log_term != ours) {
    return last_log_term > ours;
  }
  return last_log_index >= last_index();
}

int Replica::draw_timeout() {
  const int span = config_.election_timeout_max - config_.election_timeout_min + 1;
  if (span <= 1) {
    return config_.election_timeout_min;
  }
  return config_.election_timeout_min +
         static_cast<int>(next_random() % static_cast<std::uint64_t>(span));
}

std::uint64_t Replica::next_random() {
  rng_ ^= rng_ << 13;
  rng_ ^= rng_ >> 7;
  rng_ ^= rng_ << 17;
  return rng_;
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

std::uint64_t Replica::last_term() const {
  const auto* event = find_index(last_index());
  return event == nullptr ? 0 : event->term.value();
}

int Replica::majority() const { return static_cast<int>(peers_.size() + 1) / 2 + 1; }

const char* Replica::role_name() const noexcept {
  if (!config_.elections) {
    return is_leader() ? "leader" : "follower";
  }
  switch (role_) {
    case Role::Leader:
      return "leader";
    case Role::Candidate:
      return "candidate";
    case Role::Follower:
      return "follower";
  }
  return "follower";
}

}  // namespace choreoos::replication
