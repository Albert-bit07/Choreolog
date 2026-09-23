#include "choreoos/network/node_server.hpp"

#include <algorithm>
#include <array>
#include <boost/asio.hpp>
#include <chrono>
#include <deque>
#include <memory>
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

  Impl(choreoos::runtime::NodeConfig config, choreoos::replication::Replica replica)
      : config(std::move(config)),
        replica(std::move(replica)),
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
  void schedule_reconnect();
  void complete_waiters();
  void on_client(const std::shared_ptr<Session>& session, const choreoos::protocol::Frame& frame);
  void dispatch(const std::shared_ptr<Session>& session, const choreoos::protocol::Frame& frame);

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

  struct Pending {
    std::string command_id;
    bool duplicate = false;
    std::shared_ptr<Session> session;
    std::uint64_t correlation = 0;
    std::shared_ptr<boost::asio::steady_timer> timer;
  };
  std::vector<Pending> pending;
};

struct NodeServer::Impl::Session : std::enable_shared_from_this<Session> {
  Session(tcp::socket socket, Impl* node, std::string peer_id)
      : socket(std::move(socket)), node(node), peer_id(std::move(peer_id)) {}

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
  auto encoded = choreoos::protocol::encode_frame(frame);
  if (!encoded) {
    return;
  }
  for (const auto& session : sessions) {
    if (session->peer_id == peer) {
      session->send(encoded.value());
      return;
    }
  }
  ++transport_metrics.dropped;
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
    if (connected) {
      continue;
    }
    auto socket = std::make_shared<tcp::socket>(io);
    tcp::endpoint endpoint{boost::asio::ip::make_address(peer.host), peer.port};
    socket->async_connect(
        endpoint, boost::asio::bind_executor(
                      strand, [this, socket, peer](const boost::system::error_code& error) {
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

void NodeServer::Impl::schedule_heartbeat() {
  heartbeat.expires_after(std::chrono::milliseconds(50));
  heartbeat.async_wait(
      boost::asio::bind_executor(strand, [this](const boost::system::error_code& error) {
        if (error) {
          return;
        }
        replica.heartbeat();
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
    const choreoos::state::Event* event = nullptr;
    for (const auto& candidate : replica.store().log_events()) {
      if (candidate.command_id.value() == waiter.command_id) {
        event = &candidate;
      }
    }
    if (event == nullptr || replica.store().commit_index() < event->index) {
      still.push_back(std::move(waiter));
      continue;
    }
    choreoos::replication::EnqueueResult result{*event, waiter.duplicate, true,
                                                replica.status().leader_id};
    auto response = response_from(result, replica.status().state_hash);
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
    reply(response_from(queued.error(), config.leader_id));
    return;
  }
  if (queued.value().committed) {
    reply(response_from(queued.value(), replica.status().state_hash));
    return;
  }
  Pending waiter;
  waiter.command_id = command.value().id.value();
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

void NodeServer::Impl::dispatch(const std::shared_ptr<Session>& session,
                                const choreoos::protocol::Frame& frame) {
  if (frame.type == choreoos::protocol::MessageType::Handshake) {
    auto hello = choreoos::protocol::decode_handshake(frame.payload);
    if (!hello) {
      ++transport_metrics.malformed;
      return;
    }
    session->assign_peer(hello.value().node_id);
    return;
  }
  if (frame.type == choreoos::protocol::MessageType::ClientCommand ||
      frame.type == choreoos::protocol::MessageType::StatusQuery) {
    on_client(session, frame);
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
