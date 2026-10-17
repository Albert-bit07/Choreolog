#include "choreoos/replication/core_replica.hpp"

#include <algorithm>
#include <iterator>
#include <limits>

#include "choreoos/protocol/codec.hpp"
#include "choreoos/protocol/command_codec.hpp"

namespace choreoos::replication {
namespace {
constexpr std::size_t kMaximumSnapshotBytes = 64u << 20;
state::Error invalid(std::string message) {
  return {state::ErrorCode::StoreError, std::move(message)};
}
void append_effects(raft::HostEffects& target, raft::HostEffects source) {
  target.send.insert(target.send.end(), std::make_move_iterator(source.send.begin()),
                     std::make_move_iterator(source.send.end()));
  target.snapshots.insert(target.snapshots.end(), std::make_move_iterator(source.snapshots.begin()),
                          std::make_move_iterator(source.snapshots.end()));
  target.capacity_exhausted = target.capacity_exhausted || source.capacity_exhausted;
}
}  // namespace

state::Result<CoreReplica> CoreReplica::open(ReplicaConfig config) {
  if (!config.elections || config.election_timeout_min < 1 ||
      config.election_timeout_max < config.election_timeout_min ||
      config.election_timeout_max > 1000 || config.max_snapshot_chunk_bytes == 0 ||
      config.max_snapshot_chunk_bytes > (256u << 10))
    return invalid("transition backend requires elections and valid timeout/chunk bounds");
  auto host =
      raft::RuntimeHost::open({config.id, config.peers}, config.directory, config.snapshot_every);
  if (!host) return host.error();
  CoreReplica replica(std::move(config), std::move(host.value()));
  auto valid = replica.ensure_speculative();
  if (!valid) return valid.error();
  return replica;
}

CoreReplica::CoreReplica(ReplicaConfig config, raft::RuntimeHost host)
    : config_(std::move(config)), host_(std::move(host)) {
  rng_ = config_.rng_seed == 0 ? 1 : config_.rng_seed;
  for (char byte : config_.id) rng_ = rng_ * 131u + static_cast<unsigned char>(byte);
  if (rng_ == 0) rng_ = 1;
  timeout_ = draw_timeout();
  synchronize_log(raft::Durable{});
}

bool CoreReplica::known_peer(const std::string& peer) const {
  return std::find(config_.peers.begin(), config_.peers.end(), peer) != config_.peers.end();
}
bool CoreReplica::is_leader() const {
  return !host_.node().halted && !failure_ &&
         host_.node().volatile_state.role == raft::Role::Leader;
}
int CoreReplica::draw_timeout() {
  rng_ ^= rng_ << 13;
  rng_ ^= rng_ >> 7;
  rng_ ^= rng_ << 17;
  const auto span = config_.election_timeout_max - config_.election_timeout_min + 1;
  return config_.election_timeout_min + static_cast<int>(rng_ % static_cast<std::uint64_t>(span));
}

void CoreReplica::synchronize_log(const raft::Durable& before) {
  const auto& after = host_.node().durable;
  const auto anchor = store().log_first_index();
  const bool boundary_changed = before.snapshot_index != after.snapshot_index;
  const bool repaired =
      !boundary_changed && before.log.common_prefix_size(after.log) < before.log.size();
  if (boundary_changed || repaired || !index_initialized_) {
    command_index_.clear();
    index_initialized_ = true;
    for (const auto& event : store().log_events())
      if (event.type != state::EventType::NoOp)
        command_index_[event.command_id.value()] = event.index.value();
  } else {
    const auto begin = before.snapshot_index + before.log.size() + 1;
    for (auto index = begin; index <= store().last_log_index(); ++index)
      if (const auto* event = store().log_entry(index);
          event && event->type != state::EventType::NoOp)
        command_index_[event->command_id.value()] = index;
  }
  if (repaired) speculative_dirty_ = true;
  if (boundary_changed) {
    auto retained = state::retain_state_history_from(speculative_, anchor);
    if (!retained) speculative_dirty_ = true;
    // A remote snapshot may replace the domain rather than only trim history.
    if (after.commit > before.commit) speculative_dirty_ = true;
  }
}

state::Result<void> CoreReplica::ensure_speculative() {
  if (!speculative_dirty_) return {};
  auto state = store().engine().state();
  for (auto index = store().commit_index().value() + 1; index <= store().last_log_index();
       ++index) {
    const auto* event = store().log_entry(index);
    if (!event) return invalid("transition backend has a missing uncommitted event");
    auto applied = state::apply(state, *event);
    if (!applied) return applied.error();
  }
  speculative_ = std::move(state);
  speculative_dirty_ = false;
  return {};
}

state::Result<state::Event> CoreReplica::noop() {
  auto ready = ensure_speculative();
  if (!ready) return ready.error();
  const auto& durable = host_.node().durable;
  const auto last = durable.snapshot_index + durable.log.size();
  if (last >= std::numeric_limits<std::uint64_t>::max() - 1)
    return invalid("log index space exhausted before the current-term barrier");
  const auto index = last + 1;
  const auto serial = std::to_string(durable.term) + "-" + std::to_string(index);
  const auto show =
      speculative_.id ? *speculative_.id : state::ChoreographyId::parse("system").value();
  return state::Event{
      state::EventId::parse("noop-" + serial).value(),
      state::CommandId::parse("noop-" + serial).value(),
      show,
      state::EventType::NoOp,
      state::kCurrentSchemaVersion,
      state::MusicalTick::from_ticks(0).value(),
      state::LogIndex::parse(index).value(),
      state::Term::parse(durable.term).value(),
      state::CreateChoreographyPayload{show, state::StageBounds::from_mm(1, 1).value(),
                                       state::OverlapPolicy::Forbidden, 0}};
}

state::Result<raft::HostEffects> CoreReplica::drive(const raft::Input& input,
                                                    const storage::Snapshot* installing) {
  const auto before = host_.node().durable;
  const auto role = host_.node().volatile_state.role;
  auto output = host_.dispatch(input, installing);
  if (!output) {
    failure_ = output.error();
    return output.error();
  }
  const auto next_role = host_.node().volatile_state.role;
  if (role != next_role) {
    ++metrics_.role_changes;
    if (next_role == raft::Role::Follower) ++metrics_.step_downs;
  }
  if (host_.node().durable.term != before.term || next_role != raft::Role::Leader) {
    if (host_.node().durable.term != before.term || role == raft::Role::Leader) {
      outgoing_.clear();
      incoming_.reset();
      leader_id_.clear();
    }
  }
  synchronize_log(before);
  if (next_role == raft::Role::Leader && role != raft::Role::Leader) {
    leader_id_ = config_.id;
    speculative_dirty_ = true;
    auto event = noop();
    if (!event) {
      failure_ = event.error();
      return event.error();
    }
    auto encoded = raft::encode_domain_event(event.value());
    if (!encoded) {
      failure_ = encoded.error();
      return encoded.error();
    }
    auto appended = host_.dispatch(raft::ClientPropose{encoded.value().command});
    if (!appended) {
      failure_ = appended.error();
      return appended.error();
    }
    const auto* stored_barrier = store().log_entry(event.value().index.value());
    if (appended.value().capacity_exhausted || !stored_barrier ||
        state::canonical_event(*stored_barrier) != encoded.value().command) {
      failure_ =
          invalid("no capacity for the current-term barrier; node requires operational recovery");
      return *failure_;
    }
    append_effects(output.value(), std::move(appended.value()));
    synchronize_log(before);
    speculative_dirty_ = true;
  }
  return output;
}

void CoreReplica::send(const std::string& peer, const protocol::Frame& frame) {
  if (host_.node().halted || failure_) return;
  if (frame.payload.size() > protocol::kMaxFramePayload) {
    ++metrics_.messages_dropped_oversize;
    failure_ = invalid("transition backend produced an oversized replication frame");
    return;
  }
  if (sender_) sender_(peer, frame);
}

void CoreReplica::emit(const raft::Outbound& outbound, std::uint64_t snapshot_ack_bytes) {
  protocol::Frame frame;
  state::Result<std::string> encoded = invalid("unsupported core outbound message");
  if (const auto* request = std::get_if<raft::RequestVote>(&outbound.message)) {
    frame.type = protocol::MessageType::RequestVote;
    frame.correlation = request->request_id;
    encoded = protocol::encode(protocol::RequestVote{request->term, request->candidate,
                                                     request->last_index, request->last_term});
  } else if (const auto* reply = std::get_if<raft::VoteReply>(&outbound.message)) {
    frame.type = protocol::MessageType::RequestVoteResponse;
    frame.correlation = reply->request_id;
    encoded =
        protocol::encode(protocol::RequestVoteResponse{reply->term, reply->voter, reply->granted});
    if (reply->granted) {
      elapsed_ = 0;
      ++metrics_.votes_granted;
    }
  } else if (const auto* append = std::get_if<raft::AppendEntries>(&outbound.message)) {
    frame.type = protocol::MessageType::AppendEntries;
    frame.correlation = append->request_id;
    protocol::AppendEntries message{
        append->term, append->leader,       append->prev_index, append->prev_term,
        {},           append->leader_commit};
    for (std::size_t offset = 0; offset < append->entries.size(); ++offset) {
      auto event =
          raft::decode_domain_entry(append->entries[offset], append->prev_index + offset + 1);
      if (!event) {
        failure_ = event.error();
        return;
      }
      message.entries.push_back(std::move(event.value()));
    }
    encoded = protocol::encode(message);
  } else if (const auto* ack = std::get_if<raft::AppendReply>(&outbound.message)) {
    frame.type = protocol::MessageType::AppendEntriesResponse;
    frame.correlation = ack->request_id;
    encoded = protocol::encode(protocol::AppendEntriesResponse{
        ack->term, ack->follower, ack->success, ack->matched, ack->hint});
  } else if (const auto* snapshot = std::get_if<raft::SnapshotReply>(&outbound.message)) {
    frame.type = protocol::MessageType::InstallSnapshotResponse;
    frame.correlation = snapshot->request_id;
    encoded = protocol::encode(
        protocol::InstallSnapshotResponse{snapshot->term, snapshot->follower, snapshot->success,
                                          snapshot->success ? snapshot_ack_bytes : 0});
  }
  if (!encoded) {
    failure_ = encoded.error();
    return;
  }
  frame.payload = std::move(encoded.value());
  send(outbound.to, frame);
}

void CoreReplica::release(raft::HostEffects output, std::uint64_t snapshot_ack_bytes) {
  for (const auto& outbound : output.send) emit(outbound, snapshot_ack_bytes);
  for (auto& pinned : output.snapshots) {
    const auto peer = pinned.request.peer;
    auto found = outgoing_.find(peer);
    if (found == outgoing_.end() ||
        found->second.pinned.request.request_id != pinned.request.request_id) {
      outgoing_.insert_or_assign(peer, Transfer{std::move(pinned), {}, 0, false});
      send_chunk(peer, 0);
    } else {
      send(peer, found->second.frame);  // Exact per-chunk correlation retry.
    }
  }
  if (!host_.node().halted && !failure_ && commit_hook_) commit_hook_();
}

void CoreReplica::heartbeat() {
  if (host_.node().halted || failure_) return;
  raft::Input input = raft::HeartbeatTimeout{};
  if (!is_leader() && ++elapsed_ >= timeout_) {
    elapsed_ = 0;
    timeout_ = draw_timeout();
    ++metrics_.elections_started;
    input = raft::ElectionTimeout{};
  }
  auto output = drive(input);
  if (output) release(std::move(output.value()));
}

state::Result<EnqueueResult> CoreReplica::enqueue(const state::Command& command) {
  if (host_.node().halted || failure_)
    return invalid("transition backend halted; reopen from disk");
  if (!is_leader()) return state::Error{state::ErrorCode::NotLeader, "leader is " + leader_id_};
  const auto& committed = store().engine().state();
  if (committed.receipts.count(command.id.value()) ||
      committed.applied_commands.count(command.id.value())) {
    auto receipt = state::resolve_receipt(committed, command);
    if (!receipt) return receipt.error();
    return EnqueueResult{receipt.value(), true, true, leader_id_};
  }
  const auto found = command_index_.find(command.id.value());
  if (found != command_index_.end()) {
    const auto* event = store().log_entry(found->second);
    if (!event) return invalid("transition command index disagrees with retained log");
    auto normalized = command;
    if (auto* formation = std::get_if<state::FormationPayload>(&normalized.payload)) {
      std::sort(formation->members.begin(), formation->members.end());
      formation->members.erase(std::unique(formation->members.begin(), formation->members.end()),
                               formation->members.end());
    }
    const state::Command original{event->command_id, event->choreography_id,
                                  command.type,      event->schema_version,
                                  event->tick,       event->payload};
    if (event->type != state::event_type_for(command.type) ||
        protocol::canonical_command(original) != protocol::canonical_command(normalized))
      return state::Error{state::ErrorCode::CommandConflict,
                          "command ID reused with a different request"};
    return EnqueueResult{*event, true, event->index <= store().commit_index(), leader_id_};
  }
  if (host_.node().durable.log.size() >= raft::EntryLog::kMaximumEntries - 1)
    return state::Error{state::ErrorCode::Unavailable,
                        "retained log needs quorum progress and maintenance"};
  auto ready = ensure_speculative();
  if (!ready) return ready.error();
  auto proposed = state::propose(speculative_, command);
  if (!proposed) return proposed.error();
  proposed.value().term = state::Term::parse(host_.node().durable.term).value();
  protocol::AppendEntries sized{
      host_.node().durable.term, config_.id,
      store().last_log_index(),  store().log_term_at(store().last_log_index()).value_or(0),
      {proposed.value()},        store().commit_index().value()};
  auto wire = protocol::encode(sized);
  if (!wire || wire.value().size() > protocol::kMaxFramePayload)
    return state::Error{state::ErrorCode::ProtocolError,
                        "command exceeds replication frame capacity"};
  auto encoded = raft::encode_domain_event(proposed.value());
  if (!encoded) return encoded.error();
  auto output = drive(raft::ClientPropose{encoded.value().command});
  if (!output) return output.error();
  if (output.value().capacity_exhausted)
    return state::Error{state::ErrorCode::Unavailable,
                        "retained log window is full; retry after quorum progress"};
  const auto* stored = store().log_entry(proposed.value().index.value());
  if (!stored || state::canonical_event(*stored) != encoded.value().command) {
    failure_ = invalid("core proposal did not durably append the requested event");
    return *failure_;
  }
  auto applied = state::apply(speculative_, proposed.value());
  if (!applied) {
    failure_ = applied.error();
    return applied.error();
  }
  release(std::move(output.value()));
  return EnqueueResult{proposed.value(), false, proposed.value().index <= store().commit_index(),
                       leader_id_};
}

void CoreReplica::handle(const protocol::Frame& frame, const std::string& transport_peer) {
  if (host_.node().halted || failure_ || !known_peer(transport_peer)) return;
  std::optional<raft::Input> input;
  const auto before_commit = host_.node().durable.commit;
  const auto before_match = host_.node().volatile_state.peers.at(transport_peer).match;
  const auto before_next = host_.node().volatile_state.peers.at(transport_peer).next;
  std::string leader;
  if (frame.type == protocol::MessageType::RequestVote) {
    auto m = protocol::decode_request_vote(frame.payload);
    if (!m || m.value().candidate_id != transport_peer) return;
    input = raft::RecvRequestVote{{m.value().term, m.value().candidate_id, m.value().last_log_index,
                                   m.value().last_log_term, frame.correlation}};
  } else if (frame.type == protocol::MessageType::RequestVoteResponse) {
    auto m = protocol::decode_vote_response(frame.payload);
    if (!m || m.value().voter_id != transport_peer) return;
    input = raft::RecvVoteReply{
        {m.value().term, m.value().voter_id, m.value().vote_granted, frame.correlation}};
  } else if (frame.type == protocol::MessageType::AppendEntries) {
    auto m = protocol::decode_append_entries(frame.payload);
    if (!m || m.value().leader_id != transport_peer) return;
    raft::AppendEntries append{m.value().term,
                               m.value().leader_id,
                               m.value().prev_log_index,
                               m.value().prev_log_term,
                               {},
                               m.value().leader_commit,
                               frame.correlation};
    auto index = append.prev_index;
    auto term = append.prev_term;
    if ((index == 0) != (term == 0) || term > append.term) return;
    for (const auto& event : m.value().entries) {
      if (index == std::numeric_limits<std::uint64_t>::max()) return;
      ++index;
      if (event.index.value() != index || event.term.value() < term ||
          event.term.value() > append.term)
        return;
      auto encoded = raft::encode_domain_event(event);
      if (!encoded) return;
      term = event.term.value();
      append.entries.push_back(std::move(encoded.value()));
    }
    if (append.term >= host_.node().durable.term) leader = append.leader;
    input = raft::RecvAppend{std::move(append)};
  } else if (frame.type == protocol::MessageType::AppendEntriesResponse) {
    auto m = protocol::decode_append_response(frame.payload);
    if (!m || m.value().follower_id != transport_peer) return;
    if (m.value().term == host_.node().durable.term && m.value().success) {
      const auto& probe = host_.node().volatile_state.peers.at(transport_peer).in_flight;
      if (!probe || probe->request_id != frame.correlation ||
          m.value().match_index != probe->prev + probe->count)
        return;
    }
    input = raft::RecvAppendReply{{m.value().term, m.value().follower_id, m.value().success,
                                   m.value().hint_index, frame.correlation, m.value().match_index}};
  } else if (frame.type == protocol::MessageType::InstallSnapshot) {
    auto m = protocol::decode_install_snapshot(frame.payload);
    if (m && m.value().leader_id == transport_peer) on_snapshot(m.value(), frame.correlation);
    return;
  } else if (frame.type == protocol::MessageType::InstallSnapshotResponse) {
    auto m = protocol::decode_snapshot_response(frame.payload);
    if (m && m.value().follower_id == transport_peer)
      on_snapshot_reply(m.value(), frame.correlation);
    return;
  }
  if (!input) return;
  auto output = drive(*input);
  if (!output) return;
  if (!leader.empty() && host_.node().volatile_state.role == raft::Role::Follower) {
    leader_id_ = std::move(leader);
    elapsed_ = 0;
  }
  if (frame.type == protocol::MessageType::AppendEntries) {
    speculative_dirty_ = true;
  }
  if (frame.type == protocol::MessageType::AppendEntriesResponse && is_leader()) {
    const auto& progress = host_.node().volatile_state.peers.at(transport_peer);
    const auto last = store().last_log_index();
    const bool advanced = progress.match > before_match || progress.next < before_next;
    if ((advanced && progress.next <= last) || host_.node().durable.commit > before_commit) {
      auto pumped = drive(raft::HeartbeatTimeout{});
      if (!pumped) return;
      append_effects(output.value(), std::move(pumped.value()));
    }
  }
  release(std::move(output.value()));
}

void CoreReplica::send_chunk(const std::string& peer, std::uint64_t offset) {
  auto& transfer = outgoing_.at(peer);
  const auto& snapshot = *transfer.pinned.snapshot;
  if (offset > snapshot.state_payload.size() || snapshot_correlation_ == UINT64_MAX) {
    failure_ = invalid("invalid snapshot offset or exhausted correlation IDs");
    return;
  }
  const auto length = std::min(config_.max_snapshot_chunk_bytes,
                               snapshot.state_payload.size() - static_cast<std::size_t>(offset));
  transfer.end = offset + length;
  transfer.final = transfer.end == snapshot.state_payload.size();
  protocol::InstallSnapshot chunk{
      host_.node().durable.term,
      config_.id,
      snapshot.index.value(),
      snapshot.term.value(),
      snapshot.state_hash,
      snapshot.state_payload.substr(static_cast<std::size_t>(offset), length),
      offset,
      !transfer.final};
  auto encoded = protocol::encode(chunk);
  if (!encoded) {
    failure_ = encoded.error();
    return;
  }
  transfer.frame = {protocol::MessageType::InstallSnapshot, snapshot_correlation_++,
                    std::move(encoded.value())};
  ++metrics_.snapshot_chunks_sent;
  send(peer, transfer.frame);
}

void CoreReplica::reply_chunk(const std::string& peer, std::uint64_t correlation, bool success,
                              std::uint64_t next_offset) {
  auto encoded = protocol::encode(protocol::InstallSnapshotResponse{
      host_.node().durable.term, config_.id, success, next_offset});
  if (!encoded) {
    failure_ = encoded.error();
    return;
  }
  send(peer,
       {protocol::MessageType::InstallSnapshotResponse, correlation, std::move(encoded.value())});
}

void CoreReplica::on_snapshot(const protocol::InstallSnapshot& message, std::uint64_t correlation) {
  if (message.state_hash.size() != 16 || message.last_included_index == 0 ||
      message.last_included_index == UINT64_MAX || message.last_included_term == 0 ||
      message.last_included_term > message.term || message.offset > kMaximumSnapshotBytes ||
      message.payload.size() > kMaximumSnapshotBytes - static_cast<std::size_t>(message.offset) ||
      (message.more && message.payload.empty()))
    return;
  const auto committed_term = store().log_term_at(host_.node().durable.commit).value_or(0);
  if (message.last_included_index > host_.node().durable.commit &&
      message.last_included_term < committed_term)
    return;
  const raft::SnapshotRequest header{message.term,
                                     message.leader_id,
                                     message.last_included_index,
                                     message.last_included_term,
                                     correlation,
                                     false};
  auto observed = drive(raft::RecvSnapshot{header});
  if (!observed) return;
  release(std::move(observed.value()));
  if (message.term != host_.node().durable.term ||
      host_.node().volatile_state.role != raft::Role::Follower)
    return;
  leader_id_ = message.leader_id;
  elapsed_ = 0;
  const auto& installed = store().latest_snapshot();
  if (message.last_included_index <= host_.node().durable.commit) {
    if (installed && installed->index.value() == message.last_included_index &&
        installed->state_hash != message.state_hash) {
      reply_chunk(message.leader_id, correlation, false, 0);
      return;
    }
    auto ready = header;
    ready.ready = true;
    auto reply = drive(raft::RecvSnapshot{ready});
    if (reply) release(std::move(reply.value()), message.offset + message.payload.size());
    return;
  }
  const bool same = incoming_ && incoming_->leader == message.leader_id &&
                    incoming_->term == message.term &&
                    incoming_->index == message.last_included_index &&
                    incoming_->snapshot_term == message.last_included_term &&
                    incoming_->hash == message.state_hash;
  if (message.offset == 0 && !same)
    incoming_ =
        Incoming{message.leader_id,          message.term,       message.last_included_index,
                 message.last_included_term, message.state_hash, {}};
  if (!incoming_ || (!same && message.offset != 0) || message.offset > incoming_->bytes.size()) {
    incoming_.reset();
    reply_chunk(message.leader_id, correlation, false, 0);
    return;
  }
  auto& bytes = incoming_->bytes;
  if (message.offset < bytes.size()) {
    const auto offset = static_cast<std::size_t>(message.offset);
    if (message.payload.size() <= bytes.size() - offset &&
        bytes.compare(offset, message.payload.size(), message.payload) == 0)
      reply_chunk(message.leader_id, correlation, true, bytes.size());
    else {
      incoming_.reset();
      reply_chunk(message.leader_id, correlation, false, 0);
    }
    return;
  }
  bytes += message.payload;
  if (message.more) {
    reply_chunk(message.leader_id, correlation, true, bytes.size());
    return;
  }
  storage::Snapshot snapshot{state::LogIndex::parse(message.last_included_index).value(),
                             state::Term::parse(message.last_included_term).value(),
                             message.state_hash,
                             {},
                             std::move(bytes)};
  incoming_.reset();
  if (!storage::verify_snapshot(snapshot)) {
    reply_chunk(message.leader_id, correlation, false, 0);
    return;
  }
  auto ready = header;
  ready.ready = true;
  auto output = drive(raft::RecvSnapshot{ready}, &snapshot);
  if (output) release(std::move(output.value()), snapshot.state_payload.size());
}

void CoreReplica::on_snapshot_reply(const protocol::InstallSnapshotResponse& message,
                                    std::uint64_t correlation) {
  if (message.term > host_.node().durable.term) {
    auto output = drive(raft::RecvSnapshotReply{{message.term, message.follower_id, false, 0, 0}});
    if (output) release(std::move(output.value()));
    return;
  }
  auto found = outgoing_.find(message.follower_id);
  if (!is_leader() || message.term != host_.node().durable.term || found == outgoing_.end() ||
      found->second.frame.correlation != correlation)
    return;
  auto& transfer = found->second;
  if (!message.success) {
    send_chunk(message.follower_id, 0);
    return;
  }
  if (message.next_offset != transfer.end) return;
  if (!transfer.final) {
    send_chunk(message.follower_id, transfer.end);
    return;
  }
  const auto request = transfer.pinned.request;
  outgoing_.erase(found);
  auto output = drive(raft::RecvSnapshotReply{
      {message.term, message.follower_id, true, request.index, request.request_id}});
  if (output && is_leader() && store().last_log_index() > request.index) {
    auto pumped = drive(raft::HeartbeatTimeout{});
    if (!pumped) return;
    append_effects(output.value(), std::move(pumped.value()));
  }
  if (output) release(std::move(output.value()));
}

NodeStatus CoreReplica::status() const {
  const auto& node = host_.node();
  const char* role = node.volatile_state.role == raft::Role::Leader      ? "leader"
                     : node.volatile_state.role == raft::Role::Candidate ? "candidate"
                                                                         : "follower";
  if (node.halted || failure_) role = "halted";
  return {config_.id,
          leader_id_,
          role,
          node.durable.term,
          node.durable.commit,
          node.durable.snapshot_index + node.durable.log.size(),
          committed_state_hash(),
          {}};
}

}  // namespace choreoos::replication
