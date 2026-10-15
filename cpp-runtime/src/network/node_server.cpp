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
#include <tuple>
#include <utility>

#include "choreoos/network/outbound_queue.hpp"
#include "choreoos/network/peer_tls.hpp"
#include "choreoos/network/pending_requests.hpp"
#include "choreoos/protocol/codec.hpp"
#include "choreoos/protocol/command_codec.hpp"
#include "choreoos/protocol/frame.hpp"
#include "choreoos/replication/replica.hpp"
#include "choreoos/state/error.hpp"

namespace choreoos::network {
namespace {

using boost::asio::ip::tcp;
constexpr std::size_t kMaxSessions = 128;
constexpr std::size_t kMaxClientSessions = 96;
constexpr std::size_t kMaxSessionWaiters = 64;

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

  Impl(choreoos::runtime::NodeConfig node_config, choreoos::replication::Replica opened_replica,
       std::unique_ptr<boost::asio::ssl::context> context)
      : config(std::move(node_config)),
        replica(std::move(opened_replica)),
        tls_context(std::move(context)),
        strand(boost::asio::make_strand(io)),
        acceptor(io),
        peer_acceptor(io),
        heartbeat(io),
        reconnect(io) {}

  void start() {
    tcp::endpoint endpoint{boost::asio::ip::make_address(config.host), config.port};
    acceptor.open(endpoint.protocol());
    acceptor.set_option(tcp::acceptor::reuse_address(true));
    acceptor.bind(endpoint);
    acceptor.listen();
    bound_port = acceptor.local_endpoint().port();
    if (config.peer_tls) {
      const auto& tls = *config.peer_tls;
      tcp::endpoint peer_endpoint{boost::asio::ip::make_address(tls.host), tls.port};
      peer_acceptor.open(peer_endpoint.protocol());
      peer_acceptor.set_option(tcp::acceptor::reuse_address(true));
      peer_acceptor.bind(peer_endpoint);
      peer_acceptor.listen();
      accept_peer();
    }
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
  void accept_peer();
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
  std::unique_ptr<boost::asio::ssl::context> tls_context;
  boost::asio::io_context io;
  boost::asio::strand<boost::asio::io_context::executor_type> strand;
  tcp::acceptor acceptor;
  tcp::acceptor peer_acceptor;
  boost::asio::steady_timer heartbeat;
  boost::asio::steady_timer reconnect;
  std::uint16_t bound_port = 0;
  TransportMetrics transport_metrics;
  std::shared_ptr<QueueBudget> queue_budget = std::make_shared<QueueBudget>();
  std::vector<std::shared_ptr<Session>> sessions;
  std::set<std::string> dialing;  // peers with a connect attempt in flight
  std::set<std::string> blocked_peers;
  std::map<std::string, std::uint32_t> delayed_peers;
  using DelayedKey = std::tuple<std::string, std::uint16_t, std::uint64_t>;
  std::map<DelayedKey, std::shared_ptr<boost::asio::steady_timer>> delayed_sends;
  bool paused = false;
  std::string cached_state_hash;
  std::string cached_state;

  struct Pending {
    std::string command_id;
    std::optional<choreoos::state::Event> event;
    bool duplicate = false;
    std::shared_ptr<Session> session;
    std::uint64_t correlation = 0;
    std::shared_ptr<boost::asio::steady_timer> timer;
  };
  PendingRequests<Pending> pending;
};

struct NodeServer::Impl::Session : std::enable_shared_from_this<Session> {
  Session(tcp::socket accepted, Impl* owner, std::string remote_id, bool encrypted = false)
      : socket(std::move(accepted)),
        node(owner),
        peer_id(encrypted ? "" : remote_id),
        expected_peer(std::move(remote_id)),
        deadline(node->io),
        read_deadline(node->io),
        write_deadline(node->io),
        outgoing(node->queue_budget) {
    if (encrypted) {
      tls = std::make_unique<boost::asio::ssl::stream<tcp::socket>>(std::move(socket),
                                                                    *node->tls_context);
      dial_pending = !expected_peer.empty();
    }
  }

  void send_hello() {
    choreoos::protocol::Frame frame;
    frame.type = choreoos::protocol::MessageType::Handshake;
    auto payload = choreoos::protocol::encode(choreoos::protocol::Handshake{
        node->config.id, choreoos::protocol::kProtocolMajor, choreoos::protocol::kProtocolMinor});
    if (!payload) {
      retire();
      return;
    }
    frame.payload = std::move(payload.value());
    auto bytes = choreoos::protocol::encode_frame(frame);
    if (!bytes) {
      retire();
      return;
    }
    send(std::move(bytes.value()));
  }

  void start(bool outbound = false) {
    if (!tls) {
      ready = true;
      read_header();
      if (outbound) send_hello();
      return;
    }
    auto self = shared_from_this();
    std::weak_ptr<Session> weak = self;
    tls->set_verify_callback([weak](bool preverified, boost::asio::ssl::verify_context& context) {
      const auto live = weak.lock();
      return live && verify_peer_certificate(preverified, context, live->node->config,
                                             live->expected_peer, live->verified_peer);
    });
    deadline.expires_after(std::chrono::seconds(5));
    deadline.async_wait(
        boost::asio::bind_executor(node->strand, [self](const boost::system::error_code& error) {
          if (!error) {
            ++self->node->transport_metrics.rejected;
            self->retire();
          }
        }));
    tls->async_handshake(
        outbound ? boost::asio::ssl::stream_base::client : boost::asio::ssl::stream_base::server,
        boost::asio::bind_executor(node->strand, [self](const boost::system::error_code& error) {
          if (self->closed) return;
          if (error || self->verified_peer.empty()) {
            ++self->node->transport_metrics.rejected;
            self->retire();
            return;
          }
          self->deadline.cancel();
          self->peer_id = self->verified_peer;
          self->ready = true;
          if (self->dial_pending) {
            self->node->dialing.erase(self->expected_peer);
            self->dial_pending = false;
          }
          self->read_header();
          self->send_hello();
        }));
  }

  tcp::socket& lowest_layer() { return tls ? tls->next_layer() : socket; }

  template <class Buffer, class Handler>
  void read(const Buffer& buffer, Handler handler) {
    if (tls)
      boost::asio::async_read(*tls, buffer, std::move(handler));
    else
      boost::asio::async_read(socket, buffer, std::move(handler));
  }

  template <class Buffer, class Handler>
  void write(const Buffer& buffer, Handler handler) {
    if (tls)
      boost::asio::async_write(*tls, buffer, std::move(handler));
    else
      boost::asio::async_write(socket, buffer, std::move(handler));
  }

  void assign_peer(std::string id) { peer_id = std::move(id); }

  void send(std::string bytes) { send(std::make_shared<const std::string>(std::move(bytes))); }

  void send(OutboundQueue::Buffer buffer) {
    std::optional<OutboundQueue::RetryKey> key;
    auto parsed_header = choreoos::protocol::peek_header(
        reinterpret_cast<const std::uint8_t*>(buffer->data()), buffer->size());
    if (parsed_header &&
        (parsed_header.value().type == choreoos::protocol::MessageType::AppendEntries ||
         parsed_header.value().type == choreoos::protocol::MessageType::InstallSnapshot))
      key = {static_cast<std::uint16_t>(parsed_header.value().type),
             parsed_header.value().correlation};
    if (closed) return;
    if (!outgoing.push(std::move(buffer), !peer_id.empty() || tls != nullptr, key)) {
      ++node->transport_metrics.dropped;
      retire();
      return;
    }
    if (ready && !writing) write_next();
  }

  void send_frame(choreoos::protocol::Frame frame) {
    auto bytes = choreoos::protocol::encode_frame(frame);
    if (!bytes) {
      frame.type = choreoos::protocol::MessageType::ProtocolError;
      auto error = choreoos::protocol::encode(choreoos::protocol::ProtocolError{
          "ResponseTooLarge", "response exceeds frame limit; use a paginated query"});
      if (!error) {
        retire();
        return;
      }
      frame.payload = std::move(error.value());
      bytes = choreoos::protocol::encode_frame(frame);
    }
    if (bytes)
      send(std::move(bytes.value()));
    else
      retire();
  }

  void arm_deadline(boost::asio::steady_timer& timer) {
    timer.expires_after(std::chrono::seconds(10));
    auto self = shared_from_this();
    timer.async_wait(
        boost::asio::bind_executor(node->strand, [self](const boost::system::error_code& error) {
          if (!error) self->retire();
        }));
  }

  void close() {
    boost::system::error_code error;
    deadline.cancel();
    read_deadline.cancel();
    write_deadline.cancel();
    const auto result = lowest_layer().close(error);
    if (error || result) {
      ++node->transport_metrics.dropped;
    }
  }

  tcp::socket socket;
  Impl* node;
  std::string peer_id;
  std::string expected_peer;
  std::string verified_peer;
  std::unique_ptr<boost::asio::ssl::stream<tcp::socket>> tls;
  boost::asio::steady_timer deadline;
  boost::asio::steady_timer read_deadline;
  boost::asio::steady_timer write_deadline;
  std::size_t waiters = 0;
  bool client_admitted = false;
  std::array<std::uint8_t, choreoos::protocol::kFrameHeaderBytes> header{};
  std::vector<std::uint8_t> body;
  OutboundQueue outgoing;
  bool writing = false;
  bool ready = false;
  bool closed = false;
  bool dial_pending = false;

  void read_header() {
    if (closed) return;
    auto self = shared_from_this();
    arm_deadline(read_deadline);
    read(boost::asio::buffer(header),
         boost::asio::bind_executor(node->strand, [self](const boost::system::error_code& error,
                                                         std::size_t) {
           if (error) {
             self->retire();
             return;
           }
           self->read_deadline.cancel();
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
    arm_deadline(read_deadline);
    read(boost::asio::buffer(body),
         boost::asio::bind_executor(
             node->strand, [self](const boost::system::error_code& error, std::size_t) {
               if (error) {
                 self->retire();
                 return;
               }
               self->read_deadline.cancel();
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
    auto bytes = outgoing.front();
    arm_deadline(write_deadline);
    write(boost::asio::buffer(*bytes),
          boost::asio::bind_executor(
              node->strand, [self, bytes](const boost::system::error_code& error, std::size_t) {
                self->write_deadline.cancel();
                if (self->closed) return;
                if (error) {
                  self->retire();
                  return;
                }
                ++self->node->transport_metrics.frames_sent;
                self->outgoing.pop();
                self->write_next();
              }));
  }

  void retire() {
    if (closed) return;
    closed = true;
    outgoing.clear();
    auto cancelled = node->pending.remove_if(
        [this](const Pending& waiter) { return waiter.session.get() == this; });
    for (auto& waiter : cancelled) waiter.timer->cancel();
    waiters = 0;
    if (dial_pending) {
      node->dialing.erase(expected_peer);
      dial_pending = false;
    }
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
  auto buffer = std::make_shared<const std::string>(std::move(encoded.value()));
  auto send_encoded = [this, peer, bytes = buffer] {
    if (paused || blocked_peers.count(peer) != 0) {
      ++transport_metrics.dropped;
      return;
    }
    for (const auto& session : sessions) {
      if (session->ready && !session->closed && session->peer_id == peer) {
        session->send(bytes);
        return;
      }
    }
    ++transport_metrics.dropped;
  };
  const auto delay = delayed_peers.find(peer);
  if (delay != delayed_peers.end() && delay->second != 0) {
    const DelayedKey key{peer, static_cast<std::uint16_t>(frame.type), frame.correlation};
    if (delayed_sends.count(key) != 0) return;
    auto reserved = std::make_shared<OutboundQueue>(queue_budget);
    if (delayed_sends.size() >= kMaxSessions || !reserved->push(buffer, true)) {
      ++transport_metrics.dropped;
      return;
    }
    auto timer = std::make_shared<boost::asio::steady_timer>(io);
    delayed_sends.emplace(key, timer);
    timer->expires_after(std::chrono::milliseconds(delay->second));
    timer->async_wait(boost::asio::bind_executor(
        strand, [this, key, timer, reserved, send_encoded](const boost::system::error_code& error) {
          delayed_sends.erase(key);
          reserved->clear();
          if (!error) send_encoded();
        }));
    return;
  }
  send_encoded();
}

void NodeServer::Impl::accept_next() {
  acceptor.async_accept(boost::asio::bind_executor(
      strand, [this](const boost::system::error_code& error, tcp::socket socket) {
        if (!error) {
          const auto client_plane = std::count_if(
              sessions.begin(), sessions.end(),
              [](const auto& item) { return !item->tls && item->expected_peer.empty(); });
          if (sessions.size() >= kMaxSessions ||
              (config.peer_tls && static_cast<std::size_t>(client_plane) >= kMaxClientSessions)) {
            ++transport_metrics.rejected;
            socket.close();
            accept_next();
            return;
          }
          auto session = std::make_shared<Session>(std::move(socket), this, "");
          sessions.push_back(session);
          session->start();
        }
        if (acceptor.is_open()) {
          accept_next();
        }
      }));
}

void NodeServer::Impl::accept_peer() {
  peer_acceptor.async_accept(boost::asio::bind_executor(
      strand, [this](const boost::system::error_code& error, tcp::socket socket) {
        if (!error) {
          if (sessions.size() >= kMaxSessions) {
            ++transport_metrics.rejected;
            socket.close();
          } else {
            auto session = std::make_shared<Session>(std::move(socket), this, "", true);
            sessions.push_back(session);
            session->start();
          }
        }
        if (peer_acceptor.is_open()) accept_peer();
      }));
}

void NodeServer::Impl::dial_missing() {
  for (const auto& peer : config.peer_tls ? config.peer_tls->peers : config.peers) {
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
    if (sessions.size() + dialing.size() >= kMaxSessions) break;
    dialing.insert(peer.id);
    auto socket = std::make_shared<tcp::socket>(io);
    tcp::endpoint endpoint{boost::asio::ip::make_address(peer.host), peer.port};
    auto connect_deadline = std::make_shared<boost::asio::steady_timer>(io);
    connect_deadline->expires_after(std::chrono::seconds(5));
    connect_deadline->async_wait(boost::asio::bind_executor(
        strand, [this, socket, connect_deadline](const boost::system::error_code& error) {
          if (!error) {
            boost::system::error_code ignored;
            const auto cancelled = socket->cancel(ignored);
            if (ignored || cancelled) ++transport_metrics.dropped;
          }
        }));
    socket->async_connect(
        endpoint, boost::asio::bind_executor(strand, [this, socket, peer, connect_deadline](
                                                         const boost::system::error_code& error) {
          connect_deadline->cancel();
          if (error) {
            dialing.erase(peer.id);
            return;
          }
          if (sessions.size() >= kMaxSessions) {
            dialing.erase(peer.id);
            ++transport_metrics.rejected;
            boost::system::error_code close_error;
            const auto result = socket->close(close_error);
            if (close_error || result) {
              ++transport_metrics.dropped;
            }
            return;
          }
          ++transport_metrics.reconnects;
          auto session = std::make_shared<Session>(std::move(*socket), this, peer.id,
                                                   config.peer_tls.has_value());
          sessions.push_back(session);
          if (!config.peer_tls) dialing.erase(peer.id);
          session->start(true);
        }));
  }
}

std::string NodeServer::Impl::leader_contact() const {
  std::string id = replica.leader_id();
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
  auto ready = pending.take_through(replica.store().commit_index().value());
  for (auto& waiter : ready) {
    if (waiter.session->closed) {
      waiter.timer->cancel();
      continue;
    }
    --waiter.session->waiters;
    waiter.timer->cancel();
    const auto& receipts = replica.store().engine().state().receipts;
    const auto receipt = receipts.find(waiter.command_id);
    if (!waiter.event || receipt == receipts.end() ||
        receipt->second.index != waiter.event->index.value() ||
        receipt->second.term != waiter.event->term.value()) {
      waiter.session->retire();
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
      waiter.session->send_frame(std::move(frame));
    }
    if (waiter.timer) {
      waiter.timer->cancel();
    }
  }
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
      session->send_frame(std::move(out));
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
      session->send_frame(std::move(out));
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
  if (pending.full() || session->waiters >= kMaxSessionWaiters) {
    choreoos::protocol::ClientResponse overloaded;
    overloaded.error_code = "Unavailable";
    overloaded.error_message = "pending request capacity reached; retry the same operation ID";
    reply(overloaded);
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
  auto timer = waiter.timer;
  ++session->waiters;
  const auto index = waiter.event->index.value();
  const auto id = pending.add(index, std::move(waiter));
  timer->async_wait(
      boost::asio::bind_executor(strand, [this, id](const boost::system::error_code& error) {
        if (error) return;
        auto removed = pending.take(id);
        if (!removed) return;
        auto& item = *removed;
        --item.session->waiters;
        item.timer->cancel();
        choreoos::protocol::ClientResponse response;
        response.error_code = "Unavailable";
        response.error_message = "majority did not acknowledge the command; outcome unknown";
        response.leader_id = replica.leader_id();
        if (auto payload = choreoos::protocol::encode(response)) {
          choreoos::protocol::Frame out;
          out.type = choreoos::protocol::MessageType::ClientResponse;
          out.correlation = item.correlation;
          out.payload = std::move(payload.value());
          item.session->send_frame(std::move(out));
        }
      }));
}

void NodeServer::Impl::on_management(const std::shared_ptr<Session>& session,
                                     const choreoos::protocol::Frame& frame) {
  choreoos::protocol::ManagementReply reply;
  auto request = choreoos::protocol::decode_management_query(frame.payload);
  boost::system::error_code endpoint_error;
  const auto endpoint = session->lowest_layer().remote_endpoint(endpoint_error);
  const bool local = !endpoint_error && endpoint.address().is_loopback();
  if (!local) {
    reply.error_code = "PermissionDenied";
    reply.error_message = "management requests require a loopback client";
  } else if (!request) {
    reply.error_code = "ProtocolError";
    reply.error_message = request.error().message();
  } else if (request.value().operation == "state") {
    const auto& hash = replica.committed_state_hash();
    const auto& query = request.value();
    if ((query.after_index != 0 && query.expected_state_hash.empty()) ||
        (!query.expected_state_hash.empty() && query.expected_state_hash != hash)) {
      reply.error_code = "StateChanged";
      reply.error_message = "state changed or continuation token missing; restart pagination";
    } else {
      if (cached_state_hash != hash) {
        cached_state = choreoos::state::canonical_state(replica.store().engine().state());
        cached_state_hash = hash;
      }
      reply.snapshot_index = replica.store().commit_index().value();
      reply.snapshot_state_hash = hash;
      reply.state_size = cached_state.size();
      const auto offset = query.after_index;
      if (offset > cached_state.size()) {
        reply.error_code = "InvalidCursor";
        reply.error_message = "state byte cursor exceeds state size";
      } else {
        // State page_size is KiB; event page_size remains an event count.
        const auto kib = query.page_size == 0 ? 64u : std::min(query.page_size, 64u);
        reply.state_chunk = cached_state.substr(static_cast<std::size_t>(offset),
                                                static_cast<std::size_t>(kib) * 1024u);
        reply.next_after_index = offset + reply.state_chunk.size();
        reply.has_more = reply.next_after_index < cached_state.size();
      }
    }
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
    session->send_frame(std::move(result));
  }
}

void NodeServer::Impl::dispatch(const std::shared_ptr<Session>& session,
                                const choreoos::protocol::Frame& frame) {
  const bool client_frame = frame.type == choreoos::protocol::MessageType::ManagementQuery ||
                            frame.type == choreoos::protocol::MessageType::ClientCommand ||
                            frame.type == choreoos::protocol::MessageType::StatusQuery;
  if ((config.peer_tls && !session->tls && !client_frame) || (session->tls && client_frame)) {
    ++transport_metrics.rejected;
    session->retire();
    return;
  }
  if (client_frame && !session->client_admitted) {
    const auto clients = std::count_if(sessions.begin(), sessions.end(),
                                       [](const auto& item) { return item->client_admitted; });
    if (static_cast<std::size_t>(clients) >= kMaxClientSessions || !session->peer_id.empty()) {
      ++transport_metrics.rejected;
      session->retire();
      return;
    }
    session->client_admitted = true;
  }
  if (frame.type == choreoos::protocol::MessageType::Handshake) {
    if (session->client_admitted) {
      session->retire();
      return;
    }
    auto hello = choreoos::protocol::decode_handshake(frame.payload);
    if (!hello) {
      ++transport_metrics.malformed;
      return;
    }
    // TLS sessions already have a certificate-bound identity. Legacy mode has
    // only a configured-name gate and is not authenticated transport.
    if (!is_configured_peer(hello.value().node_id) ||
        (!session->peer_id.empty() && session->peer_id != hello.value().node_id)) {
      ++transport_metrics.rejected;
      session->retire();
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
  replica.handle(frame, session->peer_id);
}

choreoos::state::Result<std::unique_ptr<NodeServer>> NodeServer::open(
    const choreoos::runtime::NodeConfig& config) {
  try {
    if (auto valid = choreoos::runtime::validate_peer_tls_config(config); !valid)
      return valid.error();
    auto context = make_peer_tls_context(config);
    if (!context) return context.error();
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
    auto impl =
        std::make_unique<Impl>(config, std::move(replica.value()), std::move(context.value()));
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
