#pragma once

// Protobuf encode/decode for each message type. Bytes are the frame payload.

#include <string>
#include <string_view>

#include "choreoos/protocol/messages.hpp"
#include "choreoos/state/error.hpp"

namespace choreoos::protocol {

[[nodiscard]] choreoos::state::Result<std::string> encode(const Handshake& message);
[[nodiscard]] choreoos::state::Result<std::string> encode(const AppendEntries& message);
[[nodiscard]] choreoos::state::Result<std::string> encode(const AppendEntriesResponse& message);
[[nodiscard]] choreoos::state::Result<std::string> encode(const RequestVote& message);
[[nodiscard]] choreoos::state::Result<std::string> encode(const RequestVoteResponse& message);
[[nodiscard]] choreoos::state::Result<std::string> encode(const InstallSnapshot& message);
[[nodiscard]] choreoos::state::Result<std::string> encode(const InstallSnapshotResponse& message);
[[nodiscard]] choreoos::state::Result<std::string> encode(const ProtocolError& message);
[[nodiscard]] choreoos::state::Result<std::string> encode(const ClientCommand& message);
[[nodiscard]] choreoos::state::Result<std::string> encode(const ClientResponse& message);
[[nodiscard]] choreoos::state::Result<std::string> encode_status_query();
[[nodiscard]] choreoos::state::Result<std::string> encode(const StatusResponse& message);

[[nodiscard]] choreoos::state::Result<Handshake> decode_handshake(std::string_view bytes);
[[nodiscard]] choreoos::state::Result<AppendEntries> decode_append_entries(std::string_view bytes);
[[nodiscard]] choreoos::state::Result<AppendEntriesResponse> decode_append_response(
    std::string_view bytes);
[[nodiscard]] choreoos::state::Result<RequestVote> decode_request_vote(std::string_view bytes);
[[nodiscard]] choreoos::state::Result<RequestVoteResponse> decode_vote_response(
    std::string_view bytes);
[[nodiscard]] choreoos::state::Result<InstallSnapshot> decode_install_snapshot(
    std::string_view bytes);
[[nodiscard]] choreoos::state::Result<InstallSnapshotResponse> decode_snapshot_response(
    std::string_view bytes);
[[nodiscard]] choreoos::state::Result<ProtocolError> decode_protocol_error(std::string_view bytes);
[[nodiscard]] choreoos::state::Result<ClientCommand> decode_client_command(std::string_view bytes);
[[nodiscard]] choreoos::state::Result<ClientResponse> decode_client_response(
    std::string_view bytes);
[[nodiscard]] choreoos::state::Result<StatusResponse> decode_status_response(
    std::string_view bytes);

}  // namespace choreoos::protocol
