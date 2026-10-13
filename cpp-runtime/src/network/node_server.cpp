#include "choreoos/network/node_server.hpp"

#include <algorithm>
#include <array>
#include <boost/asio.hpp>
#include <chrono>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <utility>

#include "choreoos/protocol/codec.hpp"
#include "choreoos/protocol/command_codec.hpp"
#include "choreoos/protocol/frame.hpp"
#include "choreoos/replication/replica.hpp"
#include "choreoos/state/error.hpp"

namespace choreoos::network {
namespace {

using boost::asio::ip::tcp;
constexpr std::size_t kMaxQueuedFrames = 64;

choreoos::protocol::ClientResponse response_from(const choreoos::state::Error& error,
                                                 const std::string& leader) {
  choreoos::protocol::ClientResponse response;
  response.ok = false;
  response.error_code = choreoos::state::error_code_name(error.code());
  response.error_message = error.message();
  response.leader_id = leader;
  return response;
}

choreoos::protocol::ClientResponse response_from(const choreoos::replication::EnqueueResult& result,
                                                 const std::string& hash) {
  choreoos::protocol::ClientResponse response;
  response.ok = true;
  response.duplicate = result.duplicate;
  response.leader_id = result.leader_id;
  response.index = result.event.index.value();
  response.event_canonical = choreoos::state::canonical_event(result.event);
  response.state_hash = hash;
  return response;
}

}  // namespace

struct NodeServer::Impl {
  struct Session;

  Impl(choreoos::runtime::NodeConfig node_config, choreoos::replication::Replica opened_replica)
      : config(std::move(node_config)),
        replica(std::move(opened_replica)),
        strand(boost::asio::make_strand(io)),
        acceptor(io),
        heartbeat(io),
        reconnect(io) {}

  void start() {
    tcp::endpoint endpoint{boost::asio::ip::make_address(config.host), config.port};
    acceptor.open(endpoint.protocol());
    acceptor.set_option(tcp::acceptor::reuse_address(true));
    acceptor.bind(endpoint);
    acceptor.listen();
    bound_port = acceptor.local_endpoint().port();
    replica.set_sender([this](const std::string& peer, const choreoos::protocol::Frame& frame) {
      send_to(peer, frame);
    });
    replica.set_commit_hook([this] { complete_waiters(); });
    accept_next();
    dial_missing();
    schedule_heartbeat();
    schedule_reconnect();
  }

  void send_to(const std::string& peer, const choreoos::protocol::Frame& frame);
  void accept_next();
  void dial_missing();
  void schedule_heartbeat();
  [[nodiscard]] std::string leader_contact() const;
  void schedule_reconnect();
  void complete_waiters();
  void on_client(const std::shared_ptr<Session>& session, const choreoos::protocol::Frame& frame);
  void on_management(const std::shared_ptr<Session>& session,
                     const choreoos::protocol::Frame& frame);
  void dispatch(const std::shared_ptr<Session>& session, const choreoos::protocol::Frame& frame);
  [[nodiscard]] bool is_configured_peer(const std::string& id) const {
    return std::any_of(config.peers.begin(), config.peers.end(),
                       [&](const auto& peer) { return peer.id == id; });
  }

  choreoos::runtime::NodeConfig config;
  choreoos::replication::Replica replica;
  boost::asio::io_context io;
  boost::asio::strand<boost::asio::io_context::executor_type> strand;
  tcp::acceptor acceptor;
  boost::asio::steady_timer heartbeat;
  boost::asio::steady_timer reconnect;
  std::uint16_t bound_port = 0;
  TransportMetrics transport_metrics;
  std::vector<std::shared_ptr<Session>> sessions;
  std::set<std::string> dialing;  // peers with a connect attempt in flight
  std::set<std::string> blocked_peers;
  std::map<std::string, std::uint32_t> delayed_peers;
  bool paused = false;

  struct Pending {
    std::string command_id;
    std::optional<choreoos::state::Event> event;
    bool duplicate = false;
    std::shared_ptr<Session> session;
    std::uint64_t correlation = 0;
    std::shared_ptr<boost::asio::steady_timer> timer;
  };
  std::vector<Pending> pending;
};

struct NodeServer::Impl::Session : std::enable_shared_from_this<Session> {
  Session(tcp::socket accepted, Impl* owner, std::string remote_id)
      : socket(std::move(accepted)), node(owner), peer_id(std::move(remote_id)) {}

  void start() { read_header(); }

  void assign_peer(std::string id) { peer_id = std::move(id); }

  void send(std::string bytes) {
    if (outgoing.size() >= kMaxQueuedFrames) {
      ++node->transport_metrics.dropped;
      return;
    }
    outgoing.push_back(std::move(bytes));
    if (!writing) {
      write_next();
    }
  }

  void close() {
    boost::system::error_code ignored;
    socket.close(ignored);
  }

  tcp::socket socket;
  Impl* node;
  std::string peer_id;
  std::array<std::uint8_t, choreoos::protocol::kFrameHeaderBytes> header{};
  std::vector<std::uint8_t> body;
  std::deque<std::string> outgoing;
  bool writing = false;

  void read_header() {
    auto self = shared_from_this();
    boost::asio::async_read(
        socket, boost::asio::buffer(header),
        boost::asio::bind_executor(node->strand, [self](const boost::system::error_code& error,
                                                        std::size_t) {
          if (error) {
            self->retire();
            return;
          }
          auto peeked = choreoos::protocol::peek_header(self->header.data(), self->header.size());
          if (!peeked) {
            ++self->node->transport_metrics.malformed;
            self->retire();
            return;
          }
          self->body.assign(peeked.value().payload_length + 4, 0);
          self->read_body();
        }));
  }

  void read_body() {
    auto self = shared_from_this();
    boost::asio::async_read(
        socket, boost::asio::buffer(body),
        boost::asio::bind_executor(
            node->strand, [self](const boost::system::error_code& error, std::size_t) {
              if (error) {
                self->retire();
                return;
              }
              std::vector<std::uint8_t> bytes(self->header.begin(), self->header.end());
              bytes.insert(bytes.end(), self->body.begin(), self->body.end());
              auto frame = choreoos::protocol::decode_frame(bytes.data(), bytes.size());
              if (!frame) {
                ++self->node->transport_metrics.malformed;
                self->retire();
                return;
              }
              ++self->node->transport_metrics.frames_received;
              self->node->dispatch(self, frame.value());
              self->read_header();
            }));
  }

  void write_next() {
    if (outgoing.empty()) {
      writing = false;
      return;
    }
    writing = true;
    auto self = shared_from_this();
    boost::asio::async_write(
        socket, boost::asio::buffer(outgoing.front()),
        boost::asio::bind_executor(node->strand,
                                   [self](const boost::system::error_code& error, std::size_t) {
                                     if (error) {
                                       self->retire();
                                       return;
                                     }
                                     ++self->node->transport_metrics.frames_sent;
                                     self->outgoing.pop_front();
                                     self->write_next();
                                   }));
  }

  void retire() {
    auto& live = node->sessions;
    live.erase(std::remove(live.begin(), live.end(), shared_from_this()), live.end());
    close();
  }
};

void NodeServer::Impl::send_to(const std::string& peer, const choreoos::protocol::Frame& frame) {
  if (paused || blocked_peers.count(peer) != 0) {
    ++transport_metrics.dropped;
    return;
  }
  auto encoded = choreoos::protocol::encode_frame(frame);
  if (!encoded) {
    ++transport_metrics.dropped;
    return;
  }
  auto send_encoded = [this, peer, bytes = encoded.value()] {
    if (paused || blocked_peers.count(peer) != 0) {
      ++transport_metrics.dropped;
      return;
    }
    for (const auto& session : sessions) {
      if (session->peer_id == peer) {
        session->send(bytes);
        return;
      }
    }
    ++transport_metrics.dropped;
  };
  const auto delay = delayed_peers.find(peer);
  if (delay != delayed_peers.end() && delay->second != 0) {
    auto timer = std::make_shared<boost::asio::steady_timer>(io);
    timer->expires_after(std::chrono::milliseconds(delay->second));
    timer->async_wait(boost::asio::bind_executor(
        strand, [timer, send_encoded](const boost::system::error_code& error) {
          if (!error) {
            send_encoded();
          }
        }));
    return;
  }
  send_encoded();
}

void NodeServer::Impl::accept_next() {
  acceptor.async_accept(boost::asio::bind_executor(
      strand, [this](const boost::system::error_code& error, tcp::socket socket) {
        if (!error) {
          auto session = std::make_shared<Session>(std::move(socket), this, "");
          sessions.push_back(session);
          session->start();
        }
        if (acceptor.is_open()) {
          accept_next();
        }
      }));
}

void NodeServer::Impl::dial_missing() {
  for (const auto& peer : config.peers) {
    bool connected = false;
    for (const auto& session : sessions) {
      if (session->peer_id == peer.id) {
        connected = true;
      }
    }
    // One attempt per peer at a time. async_connect has no timeout of its own,
    // so without this an unreachable host collected a new pending socket every
    // reconnect tick until the OS gave up on each.
    if (connected || dialing.count(peer.id) != 0 || blocked_peers.count(peer.id) != 0) {
      continue;
    }
    dialing.insert(peer.id);
    auto socket = std::make_shared<tcp::socket>(io);
    tcp::endpoint endpoint{boost::asio::ip::make_address(peer.host), peer.port};
    socket->async_connect(
        endpoint, boost::asio::bind_executor(
                      strand, [this, socket, peer](const boost::system::error_code& error) {
                        dialing.erase(peer.id);
                        if (error) {
                          return;
                        }
                        ++transport_metrics.reconnects;
                        auto session = std::make_shared<Session>(std::move(*socket), this, peer.id);
                        sessions.push_back(session);
                        session->start();
                        choreoos::protocol::Handshake hello;
                        hello.node_id = config.id;
                        hello.major = choreoos::protocol::kProtocolMajor;
                        hello.minor = choreoos::protocol::kProtocolMinor;
                        if (auto payload = choreoos::protocol::encode(hello)) {
                          choreoos::protocol::Frame frame;
                          frame.type = choreoos::protocol::MessageType::Handshake;
                          frame.payload = payload.value();
                          if (auto bytes = choreoos::protocol::encode_frame(frame)) {
                            session->send(bytes.value());
                          }
                        }
                      }));
  }
}

std::string NodeServer::Impl::leader_contact() const {
  const std::string id = replica.leader_id();
  if (id.empty()) {
    return {};
  }
  if (id == config.id) {
    return id + "@" + config.host + ":" + std::to_string(config.port);
  }
  for (const auto& peer : config.peers) {
    if (peer.id == id) {
      return peer.id + "@" + peer.host + ":" + std::to_string(peer.port);
    }
  }
  return id;
}

void NodeServer::Impl::schedule_heartbeat() {
  heartbeat.expires_after(std::chrono::milliseconds(config.heartbeat_ms));
  heartbeat.async_wait(
      boost::asio::bind_executor(strand, [this](const boost::system::error_code& error) {
        if (error) {
          return;
        }
        if (!paused) {
          replica.heartbeat();
        }
        schedule_heartbeat();
      }));
}

void NodeServer::Impl::schedule_reconnect() {
  reconnect.expires_after(std::chrono::milliseconds(200));
  reconnect.async_wait(
      boost::asio::bind_executor(strand, [this](const boost::system::error_code& error) {
        if (error) {
          return;
        }
        dial_missing();
        schedule_reconnect();
      }));
}

void NodeServer::Impl::complete_waiters() {
  std::vector<Pending> still;
  for (auto& waiter : pending) {
    const auto& receipts = replica.store().engine().state().receipts;
    const auto receipt = receipts.find(waiter.command_id);
    if (!waiter.event || receipt == receipts.end() ||
        receipt->second.index != waiter.event->index.value() ||
        receipt->second.term != waiter.event->term.value()) {
      still.push_back(std::move(waiter));
      continue;
    }
    const auto* event = &*waiter.event;
    choreoos::replication::EnqueueResult result{*event, waiter.duplicate, true,
                                                replica.leader_id()};
    auto response = response_from(result, replica.committed_state_hash());
    if (auto payload = choreoos::protocol::encode(response)) {
      choreoos::protocol::Frame frame;
      frame.type = choreoos::protocol::MessageType::ClientResponse;
      frame.correlation = waiter.correlation;
      frame.payload = payload.value();
      if (auto bytes = choreoos::protocol::encode_frame(frame)) {
        waiter.session->send(bytes.value());
      }
    }
    if (waiter.timer) {
      waiter.timer->cancel();
    }
  }
  pending = std::move(still);
}

void NodeServer::Impl::on_client(const std::shared_ptr<Session>& session,
                                 const choreoos::protocol::Frame& frame) {
  auto reply = [session, correlation = frame.correlation](
                   const choreoos::protocol::ClientResponse& response) {
    if (auto payload = choreoos::protocol::encode(response)) {
      choreoos::protocol::Frame out;
      out.type = choreoos::protocol::MessageType::ClientResponse;
      out.correlation = correlation;
      out.payload = payload.value();
      if (auto bytes = choreoos::protocol::encode_frame(out)) {
        session->send(bytes.value());
      }
    }
  };
  if (frame.type == choreoos::protocol::MessageType::StatusQuery) {
    const auto status = replica.status();
    choreoos::protocol::StatusResponse response;
    response.node_id = status.node_id;
    response.leader_id = status.leader_id;
    response.role = status.role;
    response.term = status.term;
    response.commit_index = status.commit_index;
    response.last_log_index = status.last_log_index;
    response.state_hash = status.state_hash;
    response.canonical_state = status.canonical_state;
    if (auto payload = choreoos::protocol::encode(response)) {
      choreoos::protocol::Frame out;
      out.type = choreoos::protocol::MessageType::StatusResponse;
      out.correlation = frame.correlation;
      out.payload = payload.value();
      if (auto bytes = choreoos::protocol::encode_frame(out)) {
        session->send(bytes.value());
      }
    }
    return;
  }
  auto command_message = choreoos::protocol::decode_client_command(frame.payload);
  if (!command_message) {
    reply(response_from(command_message.error(), config.leader_id));
    return;
  }
  auto command = choreoos::protocol::parse_command(command_message.value().canonical);
  if (!command) {
    reply(response_from(command.error(), config.leader_id));
    return;
  }
  auto queued = replica.enqueue(command.value());
  if (!queued) {
    auto response = response_from(queued.error(), replica.leader_id());
    if (queued.error().code() == choreoos::state::ErrorCode::NotLeader) {
      const std::string contact = leader_contact();
      if (!contact.empty()) {
        response.error_message = "leader is " + contact;
      }
    }
    reply(response);
    return;
  }
  if (queued.value().committed) {
    reply(response_from(queued.value(), replica.committed_state_hash()));
    return;
  }
  Pending waiter;
  waiter.command_id = command.value().id.value();
  waiter.event = queued.value().event;
  waiter.duplicate = queued.value().duplicate;
  waiter.session = session;
  waiter.correlation = frame.correlation;
  waiter.timer = std::make_shared<boost::asio::steady_timer>(io);
  waiter.timer->expires_after(std::chrono::seconds(2));
  const std::string command_id = waiter.command_id;
  waiter.timer->async_wait(boost::asio::bind_executor(
      strand, [this, command_id](const boost::system::error_code& error) {
        if (error) {
          return;
        }
        std::vector<Pending> still;
        for (auto& item : pending) {
          if (item.command_id != command_id) {
            still.push_back(std::move(item));
            continue;
          }
          choreoos::protocol::ClientResponse response;
          response.ok = false;
          response.error_code = "Unavailable";
          response.error_message = "majority did not acknowledge the command";
          response.leader_id = config.leader_id;
          if (auto payload = choreoos::protocol::encode(response)) {
            choreoos::protocol::Frame out;
            out.type = choreoos::protocol::MessageType::ClientResponse;
            out.correlation = item.correlation;
            out.payload = payload.value();
            if (auto bytes = choreoos::protocol::encode_frame(out)) {
              item.session->send(bytes.value());
            }
          }
        }
        pending = std::move(still);
      }));
  pending.push_back(std::move(waiter));
}

void NodeServer::Impl::on_management(const std::shared_ptr<Session>& session,
                                     const choreoos::protocol::Frame& frame) {
  choreoos::protocol::ManagementReply reply;
  auto request = choreoos::protocol::decode_management_query(frame.payload);
  boost::system::error_code endpoint_error;
  const auto endpoint = session->socket.remote_endpoint(endpoint_error);
  const bool local = !endpoint_error && endpoint.address().is_loopback();
  if (!local) {
    reply.error_code = "PermissionDenied";
    reply.error_message = "management requests require a loopback client";
  } else if (!request) {
    reply.error_code = "ProtocolError";
    reply.error_message = request.error().message();
  } else if (request.value().operation == "events") {
    const auto& log = replica.store().log_events();
    const auto committed = replica.store().commit_index().value();
    const auto after = request.value().after_index;
    const auto first = replica.store().log_first_index();
    const auto limit = request.value().page_size == 0 ? 64u : request.value().page_size;
    reply.next_after_index = after;
    std::size_t page_bytes = 0;
    constexpr std::size_t kMaxEventPageBytes = choreoos::protocol::kMaxFramePayload - 4096;
    if (first > 1 && after < first - 1) {
      reply.error_code = "Compacted";
      reply.error_message = "requested event range has been compacted; bootstrap from snapshot";
    } else {
      // Log records are contiguous. Seek directly to the first index after
      // the page cursor instead of rescanning the full retained WAL per page.
      std::size_t offset = 0;
      if (!log.empty() && after >= log.front().index.value()) {
        const auto distance = after - log.front().index.value() + 1;
        offset = distance >= log.size() ? log.size() : static_cast<std::size_t>(distance);
      }
      for (std::size_t i = offset; i < log.size(); ++i) {
        const auto& event = log[i];
        const auto index = event.index.value();
        if (index > committed) {
          break;
        }
        const auto canonical = choreoos::state::canonical_event(event);
        if (canonical.size() > kMaxEventPageBytes) {
          reply.error_code = "EventTooLarge";
          reply.error_message = "committed event exceeds the management page limit";
          reply.events.clear();
          break;
        }
        if (reply.events.size() == limit || canonical.size() > kMaxEventPageBytes - page_bytes) {
          reply.has_more = true;
          break;
        }
        page_bytes += canonical.size();
        reply.events.push_back(canonical);
        reply.next_after_index = index;
      }
    }
  } else if (request.value().operation == "metrics") {
    const auto& consensus = replica.metrics();
    reply.role_changes = consensus.role_changes;
    reply.elections_started = consensus.elections_started;
    reply.votes_granted = consensus.votes_granted;
    reply.step_downs = consensus.step_downs;
    reply.messages_dropped_oversize = consensus.messages_dropped_oversize;
    reply.snapshot_chunks_sent = consensus.snapshot_chunks_sent;
    reply.transport_frames_sent = transport_metrics.frames_sent;
    reply.transport_frames_received = transport_metrics.frames_received;
    reply.transport_dropped = transport_metrics.dropped;
  } else if (request.value().operation == "recovery") {
    const auto& report = replica.store().recovery();
    reply.snapshot_index = report.snapshot_index;
    const auto& snapshot = replica.store().latest_snapshot();
    if (snapshot) {
      reply.snapshot_index = snapshot->index.value();
      reply.snapshot_state_hash = snapshot->state_hash;
    }
    std::ostringstream description;
    description << "used_snapshot=" << report.used_snapshot
                << " replayed_after_snapshot=" << report.replayed_after_snapshot
                << " truncated_tail_bytes=" << report.truncated_tail_bytes
                << " commit_index=" << replica.store().commit_index().value();
    reply.recovery_report = description.str();
  } else if (request.value().operation == "drop_peer" ||
             request.value().operation == "delay_peer" || request.value().operation == "heal" ||
             request.value().operation == "pause" || request.value().operation == "resume") {
    if (!config.test_mode) {
      reply.error_code = "PermissionDenied";
      reply.error_message = "fault injection requires test_mode and a loopback client";
    } else if (request.value().operation == "drop_peer" ||
               request.value().operation == "delay_peer") {
      const auto& peer = request.value().peer_id;
      if (!is_configured_peer(peer)) {
        reply.error_code = "InvalidPeer";
        reply.error_message = "fault target must be a configured peer";
      } else if (request.value().operation == "drop_peer") {
        blocked_peers.insert(peer);
        for (const auto& connection : sessions) {
          if (connection->peer_id == peer) {
            connection->close();
          }
        }
      } else {
        delayed_peers[peer] = request.value().delay_ms;
      }
    } else if (request.value().operation == "heal") {
      blocked_peers.clear();
      delayed_peers.clear();
      paused = false;
    } else if (request.value().operation == "pause") {
      paused = true;
    } else {
      paused = false;
    }
  } else {
    reply.error_code = "UnsupportedOperation";
    reply.error_message = "unrecognized management operation";
  }
  if (auto payload = choreoos::protocol::encode(reply)) {
    choreoos::protocol::Frame result;
    result.type = choreoos::protocol::MessageType::ManagementReply;
    result.correlation = frame.correlation;
    result.payload = payload.value();
    if (auto bytes = choreoos::protocol::encode_frame(result)) {
      session->send(bytes.value());
    }
  }
}

void NodeServer::Impl::dispatch(const std::shared_ptr<Session>& session,
                                const choreoos::protocol::Frame& frame) {
  if (frame.type == choreoos::protocol::MessageType::Handshake) {
    auto hello = choreoos::protocol::decode_handshake(frame.payload);
    if (!hello) {
      ++transport_metrics.malformed;
      return;
    }
    // Only configured peers may claim a peer identity. This is a gate against
    // strangers, not authentication: a process that knows a peer id can still
    // claim it, so real deployments need a shared secret or mTLS on top.
    if (!is_configured_peer(hello.value().node_id)) {
      ++transport_metrics.rejected;
      return;
    }
    session->assign_peer(hello.value().node_id);
    return;
  }
  if (frame.type == choreoos::protocol::MessageType::ManagementQuery) {
    on_management(session, frame);
    return;
  }
  if (frame.type == choreoos::protocol::MessageType::ClientCommand ||
      frame.type == choreoos::protocol::MessageType::StatusQuery) {
    on_client(session, frame);
    return;
  }
  // Everything else is replication traffic (AppendEntries, votes, snapshots).
  // It must not come from an unidentified connection: a forged high term would
  // be persisted and could depose the leader or block elections.
  if (!is_configured_peer(session->peer_id)) {
    ++transport_metrics.rejected;
    return;
  }
  if (paused || blocked_peers.count(session->peer_id) != 0) {
    ++transport_metrics.dropped;
    return;
  }
  replica.handle(frame);
}

choreoos::state::Result<std::unique_ptr<NodeServer>> NodeServer::open(
    const choreoos::runtime::NodeConfig& config) {
  try {
    choreoos::replication::ReplicaConfig replica_config;
    replica_config.id = config.id;
    replica_config.leader_id = config.leader_id;
    replica_config.directory = config.data;
    replica_config.elections = config.elections;
    replica_config.rng_seed = config.rng_seed == 0 ? 1 : config.rng_seed;
    replica_config.election_timeout_min = static_cast<int>(config.election_timeout_min);
    replica_config.election_timeout_max = static_cast<int>(config.election_timeout_max);
    for (const auto& peer : config.peers) {
      replica_config.peers.push_back(peer.id);
    }
    auto replica = choreoos::replication::Replica::open(replica_config);
    if (!replica) {
      return replica.error();
    }
    auto impl = std::make_unique<Impl>(config, std::move(replica.value()));
    impl->start();
    return std::unique_ptr<NodeServer>(new NodeServer(std::move(impl)));
  } catch (const std::exception& error) {
    return choreoos::state::Error{choreoos::state::ErrorCode::StoreError, error.what()};
  }
}

NodeServer::NodeServer(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

NodeServer::~NodeServer() {
  if (impl_) {
    impl_->io.stop();
  }
}

void NodeServer::run() { impl_->io.run(); }

void NodeServer::stop() { impl_->io.stop(); }

std::uint16_t NodeServer::port() const { return impl_->bound_port; }

TransportMetrics NodeServer::metrics() const { return impl_->transport_metrics; }

}  // namespace choreoos::network
