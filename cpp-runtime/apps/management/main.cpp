#include <grpcpp/grpcpp.h>

#include <charconv>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "choreoos/protocol/client.hpp"
#include "choreoos/protocol/codec.hpp"
#include "choreoos/protocol/command_codec.hpp"
#include "choreoos/protocol/frame.hpp"
#include "choreoos/state/store.hpp"
#include "choreoos_management.grpc.pb.h"

namespace {

using grpc::Status;
using grpc::StatusCode;
using namespace choreoos::management::v1;
using choreoos::protocol::Frame;
using choreoos::protocol::MessageType;

class ManagementService final : public ChoreoManagement::Service {
 public:
  explicit ManagementService(std::uint16_t port) : port_(port) {}

  Status SubmitCommand(grpc::ServerContext* context, const SubmitCommandRequest* request,
                       SubmitCommandResponse* response) override {
    auto command = choreoos::protocol::parse_command(request->canonical_command());
    if (!command || request->operation_id().empty() ||
        command.value().id.value() != request->operation_id()) {
      return {StatusCode::INVALID_ARGUMENT, "operation_id must match a valid canonical command ID"};
    }
    choreoos::protocol::ClientCommand wire;
    wire.canonical = choreoos::protocol::canonical_command(command.value());
    Frame frame;
    frame.type = MessageType::ClientCommand;
    auto encoded = choreoos::protocol::encode(wire);
    if (!encoded) {
      return {StatusCode::INVALID_ARGUMENT, encoded.error().message()};
    }
    frame.payload = encoded.value();
    auto reply = transact(frame);
    if (!reply) {
      // Lost transports and timeouts do not prove that the command failed.
      context->AddTrailingMetadata("choreoos-outcome", "unknown");
      return {StatusCode::UNAVAILABLE, reply.error().message()};
    }
    if (reply.value().type != MessageType::ClientResponse) {
      return {StatusCode::INTERNAL, "node returned unexpected command frame"};
    }
    auto body = choreoos::protocol::decode_client_response(reply.value().payload);
    if (!body) {
      return {StatusCode::INTERNAL, body.error().message()};
    }
    if (!body.value().ok) {
      // Typed error identity is stable across changes to human-readable
      // messages; clients must not parse response text to find a new leader.
      context->AddTrailingMetadata("choreoos-error-code", body.value().error_code);
      if (body.value().error_code == "NotLeader") {
        return {StatusCode::FAILED_PRECONDITION, body.value().error_message};
      }
      if (body.value().error_code == "Unavailable") {
        context->AddTrailingMetadata("choreoos-outcome", "unknown");
        return {StatusCode::UNAVAILABLE, body.value().error_message};
      }
      if (body.value().error_code == "CommandConflict") {
        return {StatusCode::ALREADY_EXISTS, body.value().error_message};
      }
      if (body.value().error_code == "OutcomeUnavailable") {
        return {StatusCode::FAILED_PRECONDITION, body.value().error_message};
      }
      return {StatusCode::INVALID_ARGUMENT,
              body.value().error_code + ": " + body.value().error_message};
    }
    response->set_committed(true);
    response->set_duplicate(body.value().duplicate);
    response->set_committed_index(body.value().index);
    response->set_canonical_event(body.value().event_canonical);
    response->set_state_hash(body.value().state_hash);
    response->set_leader_id(body.value().leader_id);
    return Status::OK;
  }

  Status GetState(grpc::ServerContext*, const GetStateRequest* request,
                  GetStateResponse* response) override {
    auto status = fetch_status();
    if (!status) {
      return {StatusCode::UNAVAILABLE, status.error().message()};
    }
    const bool leader = status.value().role == "leader";
    if (request->require_leader() && !leader) {
      return {StatusCode::FAILED_PRECONDITION,
              "not leader; last known leader: " + status.value().leader_id};
    }
    auto state = choreoos::protocol::fetch_state("127.0.0.1", port_, status.value().state_hash,
                                                 std::chrono::milliseconds(2500));
    if (!state)
      return {state.error().code() == choreoos::state::ErrorCode::ProtocolError
                  ? StatusCode::RESOURCE_EXHAUSTED
                  : StatusCode::UNAVAILABLE,
              state.error().message()};
    response->set_canonical_state(std::move(state.value()));
    response->set_state_hash(status.value().state_hash);
    response->set_applied_index(status.value().commit_index);
    response->set_term(status.value().term);
    // A leader read is authoritative for the local applied state, but not
    // guaranteed linearizable without a quorum/read barrier.
    response->set_authoritative(leader);
    return Status::OK;
  }

  Status GetNodeStatus(grpc::ServerContext*, const GetNodeStatusRequest*,
                       NodeStatus* response) override {
    auto status = fetch_status();
    if (!status) {
      return {StatusCode::UNAVAILABLE, status.error().message()};
    }
    response->set_node_id(status.value().node_id);
    response->set_leader_id(status.value().leader_id);
    response->set_role(status.value().role);
    response->set_term(status.value().term);
    response->set_commit_index(status.value().commit_index);
    response->set_last_log_index(status.value().last_log_index);
    response->set_state_hash(status.value().state_hash);
    response->set_ready(true);
    return Status::OK;
  }

  Status ListEvents(grpc::ServerContext*, const ListEventsRequest* request,
                    ListEventsResponse* response) override {
    if (request->page_size() > 128) {
      return {StatusCode::INVALID_ARGUMENT, "page_size must be at most 128"};
    }
    choreoos::protocol::ManagementQuery query;
    query.operation = "events";
    query.after_index = request->after_index();
    query.page_size = request->page_size();
    auto body = fetch_management(query);
    if (!body) {
      return {StatusCode::UNAVAILABLE, body.error().message()};
    }
    if (!body.value().error_code.empty()) {
      const auto code = body.value().error_code == "Compacted" ? StatusCode::OUT_OF_RANGE
                                                               : StatusCode::FAILED_PRECONDITION;
      return {code, body.value().error_message};
    }
    for (const auto& canonical : body.value().events) {
      auto parsed = choreoos::state::parse_event(canonical);
      if (!parsed) {
        return {StatusCode::INTERNAL, "node returned malformed committed event"};
      }
      auto* item = response->add_events();
      item->set_index(parsed.value().index.value());
      item->set_canonical_event(canonical);
    }
    response->set_next_after_index(body.value().next_after_index);
    response->set_has_more(body.value().has_more);
    return Status::OK;
  }

  Status GetMetrics(grpc::ServerContext*, const GetMetricsRequest*,
                    MetricsResponse* response) override {
    choreoos::protocol::ManagementQuery query;
    query.operation = "metrics";
    auto body = fetch_management(query);
    if (!body) {
      return {StatusCode::UNAVAILABLE, body.error().message()};
    }
    if (!body.value().error_code.empty()) {
      return {StatusCode::FAILED_PRECONDITION, body.value().error_message};
    }
    const auto& metrics = body.value();
    response->set_role_changes(metrics.role_changes);
    response->set_elections_started(metrics.elections_started);
    response->set_votes_granted(metrics.votes_granted);
    response->set_step_downs(metrics.step_downs);
    response->set_messages_dropped_oversize(metrics.messages_dropped_oversize);
    response->set_snapshot_chunks_sent(metrics.snapshot_chunks_sent);
    response->set_transport_frames_sent(metrics.transport_frames_sent);
    response->set_transport_frames_received(metrics.transport_frames_received);
    response->set_transport_dropped(metrics.transport_dropped);
    return Status::OK;
  }

  Status GetRecoveryStatus(grpc::ServerContext*, const GetRecoveryStatusRequest*,
                           RecoveryStatus* response) override {
    choreoos::protocol::ManagementQuery query;
    query.operation = "recovery";
    auto body = fetch_management(query);
    if (!body) {
      return {StatusCode::UNAVAILABLE, body.error().message()};
    }
    auto status = fetch_status();
    if (!status) {
      return {StatusCode::UNAVAILABLE, status.error().message()};
    }
    response->set_commit_index(status.value().commit_index);
    response->set_snapshot_index(body.value().snapshot_index);
    response->set_snapshot_state_hash(body.value().snapshot_state_hash);
    response->set_recovering(false);
    response->set_recovery_report(body.value().recovery_report);
    return Status::OK;
  }

  Status InjectTestFault(grpc::ServerContext*, const InjectTestFaultRequest* request,
                         InjectTestFaultResponse* response) override {
    choreoos::protocol::ManagementQuery query;
    query.peer_id = request->peer_id();
    query.delay_ms = request->delay_ms();
    switch (request->fault()) {
      case InjectTestFaultRequest::DROP_PEER:
        query.operation = "drop_peer";
        break;
      case InjectTestFaultRequest::DELAY_PEER:
        query.operation = "delay_peer";
        break;
      case InjectTestFaultRequest::HEAL_PEERS:
        query.operation = "heal";
        break;
      case InjectTestFaultRequest::PAUSE_NODE:
        query.operation = "pause";
        break;
      default:
        return {StatusCode::INVALID_ARGUMENT, "unknown test fault"};
    }
    auto body = fetch_management(query);
    if (!body) {
      return {StatusCode::UNAVAILABLE, body.error().message()};
    }
    if (!body.value().error_code.empty()) {
      const auto code = body.value().error_code == "PermissionDenied"
                            ? StatusCode::PERMISSION_DENIED
                            : StatusCode::INVALID_ARGUMENT;
      return {code, body.value().error_message};
    }
    response->set_applied(true);
    return Status::OK;
  }

 private:
  choreoos::state::Result<Frame> transact(const Frame& frame) const {
    return choreoos::protocol::transact("127.0.0.1", port_, frame, std::chrono::milliseconds(2500));
  }

  choreoos::state::Result<choreoos::protocol::StatusResponse> fetch_status() const {
    Frame request;
    request.type = MessageType::StatusQuery;
    auto payload = choreoos::protocol::encode_status_query();
    if (!payload) {
      return payload.error();
    }
    request.payload = payload.value();
    auto reply = transact(request);
    if (!reply) {
      return reply.error();
    }
    if (reply.value().type != MessageType::StatusResponse) {
      return choreoos::state::Error{choreoos::state::ErrorCode::ProtocolError,
                                    "unexpected node status response"};
    }
    return choreoos::protocol::decode_status_response(reply.value().payload);
  }

  choreoos::state::Result<choreoos::protocol::ManagementReply> fetch_management(
      const choreoos::protocol::ManagementQuery& query) const {
    Frame request;
    request.type = MessageType::ManagementQuery;
    auto payload = choreoos::protocol::encode(query);
    if (!payload) {
      return payload.error();
    }
    request.payload = payload.value();
    auto reply = transact(request);
    if (!reply) {
      return reply.error();
    }
    if (reply.value().type != MessageType::ManagementReply) {
      return choreoos::state::Error{choreoos::state::ErrorCode::ProtocolError,
                                    "unexpected management response"};
    }
    return choreoos::protocol::decode_management_reply(reply.value().payload);
  }

  std::uint16_t port_;
};

}  // namespace

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "usage: choreoos-management <node-port> <management-port>\n";
    return 2;
  }
  try {
    const auto parse_port = [](std::string_view text) -> std::optional<std::uint16_t> {
      unsigned int number = 0;
      const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), number);
      if (text.empty() || error != std::errc{} || end != text.data() + text.size()) {
        return std::nullopt;
      }
      if (number == 0 || number > 65535) {
        return std::nullopt;
      }
      return static_cast<std::uint16_t>(number);
    };
    const auto node_port = parse_port(argv[1]);
    const auto management_port = parse_port(argv[2]);
    if (!node_port || !management_port || node_port == management_port) {
      std::cerr << "ports must be distinct whole numbers between 1 and 65535\n";
      return 2;
    }
    ManagementService service{*node_port};
    grpc::ServerBuilder builder;
    const std::string address = "127.0.0.1:" + std::to_string(*management_port);
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    if (!server) {
      std::cerr << "could not bind " << address << '\n';
      return 1;
    }
    std::cout << "management API listening on " << address << '\n';
    server->Wait();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "invalid management arguments: " << error.what() << '\n';
    return 2;
  }
}
