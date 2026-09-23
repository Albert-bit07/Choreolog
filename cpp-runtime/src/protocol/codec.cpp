#include "choreoos/protocol/codec.hpp"

#include <cstddef>
#include <cstdint>
#include <string>

#include "choreoos/state/store.hpp"

namespace choreoos::protocol {
namespace {

using choreoos::state::Error;
using choreoos::state::ErrorCode;
using choreoos::state::Result;

// Protobuf wire types. Proto3 omits zeros and empty strings.
constexpr int kVarint = 0;
constexpr int kLength = 2;

void write_varint(std::string& out, std::uint64_t value) {
  while (value >= 0x80) {
    out.push_back(static_cast<char>((value & 0x7f) | 0x80));
    value >>= 7;
  }
  out.push_back(static_cast<char>(value));
}

void write_key(std::string& out, int field, int type) {
  write_varint(out, (static_cast<std::uint64_t>(field) << 3) | static_cast<std::uint64_t>(type));
}

void write_uint(std::string& out, int field, std::uint64_t value) {
  if (value == 0) {
    return;
  }
  write_key(out, field, kVarint);
  write_varint(out, value);
}

void write_bool(std::string& out, int field, bool value) {
  if (!value) {
    return;
  }
  write_key(out, field, kVarint);
  out.push_back(1);
}

void write_bytes(std::string& out, int field, std::string_view value) {
  if (value.empty()) {
    return;
  }
  write_key(out, field, kLength);
  write_varint(out, value.size());
  out.append(value);
}

struct Reader {
  std::string_view bytes;
  std::size_t offset = 0;

  bool done() const { return offset >= bytes.size(); }

  Result<std::uint64_t> varint() {
    std::uint64_t value = 0;
    int shift = 0;
    while (offset < bytes.size() && shift < 70) {
      const auto byte = static_cast<std::uint8_t>(bytes[offset++]);
      value |= static_cast<std::uint64_t>(byte & 0x7f) << shift;
      if ((byte & 0x80) == 0) {
        return value;
      }
      shift += 7;
    }
    return Error{ErrorCode::ProtocolError, "protobuf varint is truncated"};
  }

  Result<std::string> skip_or_read(int wire, bool read) {
    if (wire == kVarint) {
      auto value = varint();
      if (!value) {
        return value.error();
      }
      return std::string{};
    }
    if (wire == kLength) {
      auto length = varint();
      if (!length) {
        return length.error();
      }
      if (offset + length.value() > bytes.size()) {
        return Error{ErrorCode::ProtocolError, "protobuf length exceeds payload"};
      }
      std::string slice(bytes.substr(offset, static_cast<std::size_t>(length.value())));
      offset += static_cast<std::size_t>(length.value());
      return read ? slice : std::string{};
    }
    if (wire == 1) {
      if (offset + 8 > bytes.size()) {
        return Error{ErrorCode::ProtocolError, "protobuf fixed64 is truncated"};
      }
      offset += 8;
      return std::string{};
    }
    if (wire == 5) {
      if (offset + 4 > bytes.size()) {
        return Error{ErrorCode::ProtocolError, "protobuf fixed32 is truncated"};
      }
      offset += 4;
      return std::string{};
    }
    return Error{ErrorCode::ProtocolError, "protobuf wire type is unsupported"};
  }
};

}  // namespace

Result<std::string> encode(const Handshake& message) {
  std::string out;
  write_bytes(out, 1, message.node_id);
  write_uint(out, 2, message.major);
  write_uint(out, 3, message.minor);
  return out;
}

Result<std::string> encode(const AppendEntries& message) {
  std::string out;
  write_uint(out, 1, message.term);
  write_bytes(out, 2, message.leader_id);
  write_uint(out, 3, message.prev_log_index);
  write_uint(out, 4, message.prev_log_term);
  for (const auto& event : message.entries) {
    std::string record;
    write_bytes(record, 1, choreoos::state::canonical_event(event));
    write_bytes(out, 5, record);
  }
  write_uint(out, 6, message.leader_commit);
  return out;
}

Result<std::string> encode(const AppendEntriesResponse& message) {
  std::string out;
  write_uint(out, 1, message.term);
  write_bytes(out, 2, message.follower_id);
  write_bool(out, 3, message.success);
  write_uint(out, 4, message.match_index);
  write_uint(out, 5, message.hint_index);
  return out;
}

Result<std::string> encode(const RequestVote& message) {
  std::string out;
  write_uint(out, 1, message.term);
  write_bytes(out, 2, message.candidate_id);
  write_uint(out, 3, message.last_log_index);
  write_uint(out, 4, message.last_log_term);
  return out;
}

Result<std::string> encode(const RequestVoteResponse& message) {
  std::string out;
  write_uint(out, 1, message.term);
  write_bytes(out, 2, message.voter_id);
  write_bool(out, 3, message.vote_granted);
  return out;
}

Result<std::string> encode(const InstallSnapshot& message) {
  std::string out;
  write_uint(out, 1, message.term);
  write_bytes(out, 2, message.leader_id);
  write_uint(out, 3, message.last_included_index);
  write_uint(out, 4, message.last_included_term);
  write_bytes(out, 5, message.state_hash);
  write_bytes(out, 6, message.payload);
  return out;
}

Result<std::string> encode(const InstallSnapshotResponse& message) {
  std::string out;
  write_uint(out, 1, message.term);
  write_bytes(out, 2, message.follower_id);
  write_bool(out, 3, message.success);
  return out;
}

Result<std::string> encode(const ProtocolError& message) {
  std::string out;
  write_bytes(out, 1, message.code);
  write_bytes(out, 2, message.detail);
  return out;
}

Result<std::string> encode(const ClientCommand& message) {
  std::string out;
  write_bytes(out, 1, message.canonical);
  return out;
}

Result<std::string> encode(const ClientResponse& message) {
  std::string out;
  write_bool(out, 1, message.ok);
  write_bool(out, 2, message.duplicate);
  write_bytes(out, 3, message.error_code);
  write_bytes(out, 4, message.error_message);
  write_bytes(out, 5, message.leader_id);
  write_uint(out, 6, message.index);
  write_bytes(out, 7, message.event_canonical);
  write_bytes(out, 8, message.state_hash);
  return out;
}

Result<std::string> encode_status_query() { return std::string{}; }

Result<std::string> encode(const StatusResponse& message) {
  std::string out;
  write_bytes(out, 1, message.node_id);
  write_bytes(out, 2, message.leader_id);
  write_bytes(out, 3, message.role);
  write_uint(out, 4, message.term);
  write_uint(out, 5, message.commit_index);
  write_uint(out, 6, message.last_log_index);
  write_bytes(out, 7, message.state_hash);
  write_bytes(out, 8, message.canonical_state);
  return out;
}

Result<Handshake> decode_handshake(std::string_view bytes) {
  Reader reader{bytes, 0};
  Handshake message;
  while (!reader.done()) {
    auto key = reader.varint();
    if (!key) {
      return key.error();
    }
    const int field = static_cast<int>(key.value() >> 3);
    const int wire = static_cast<int>(key.value() & 7);
    if (field == 1 && wire == kLength) {
      auto value = reader.skip_or_read(wire, true);
      if (!value) {
        return value.error();
      }
      message.node_id = value.value();
    } else if ((field == 2 || field == 3) && wire == kVarint) {
      auto value = reader.varint();
      if (!value) {
        return value.error();
      }
      if (field == 2) {
        message.major = static_cast<std::uint32_t>(value.value());
      } else {
        message.minor = static_cast<std::uint32_t>(value.value());
      }
    } else if (auto skipped = reader.skip_or_read(wire, false); !skipped) {
      return skipped.error();
    }
  }
  return message;
}

Result<AppendEntries> decode_append_entries(std::string_view bytes) {
  Reader reader{bytes, 0};
  AppendEntries message;
  while (!reader.done()) {
    auto key = reader.varint();
    if (!key) {
      return key.error();
    }
    const int field = static_cast<int>(key.value() >> 3);
    const int wire = static_cast<int>(key.value() & 7);
    if ((field == 1 || field == 3 || field == 4 || field == 6) && wire == kVarint) {
      auto value = reader.varint();
      if (!value) {
        return value.error();
      }
      if (field == 1) {
        message.term = value.value();
      } else if (field == 3) {
        message.prev_log_index = value.value();
      } else if (field == 4) {
        message.prev_log_term = value.value();
      } else {
        message.leader_commit = value.value();
      }
    } else if (field == 2 && wire == kLength) {
      auto value = reader.skip_or_read(wire, true);
      if (!value) {
        return value.error();
      }
      message.leader_id = value.value();
    } else if (field == 5 && wire == kLength) {
      auto record = reader.skip_or_read(wire, true);
      if (!record) {
        return record.error();
      }
      Reader inner{record.value(), 0};
      std::string canonical;
      while (!inner.done()) {
        auto inner_key = inner.varint();
        if (!inner_key) {
          return inner_key.error();
        }
        const int inner_field = static_cast<int>(inner_key.value() >> 3);
        const int inner_wire = static_cast<int>(inner_key.value() & 7);
        if (inner_field == 1 && inner_wire == kLength) {
          auto value = inner.skip_or_read(inner_wire, true);
          if (!value) {
            return value.error();
          }
          canonical = value.value();
        } else if (auto skipped = inner.skip_or_read(inner_wire, false); !skipped) {
          return skipped.error();
        }
      }
      auto event = choreoos::state::parse_event(canonical);
      if (!event) {
        return event.error();
      }
      message.entries.push_back(event.value());
    } else if (auto skipped = reader.skip_or_read(wire, false); !skipped) {
      return skipped.error();
    }
  }
  return message;
}

Result<AppendEntriesResponse> decode_append_response(std::string_view bytes) {
  Reader reader{bytes, 0};
  AppendEntriesResponse message;
  while (!reader.done()) {
    auto key = reader.varint();
    if (!key) {
      return key.error();
    }
    const int field = static_cast<int>(key.value() >> 3);
    const int wire = static_cast<int>(key.value() & 7);
    if (wire == kVarint && (field == 1 || field == 3 || field == 4 || field == 5)) {
      auto value = reader.varint();
      if (!value) {
        return value.error();
      }
      if (field == 1) {
        message.term = value.value();
      } else if (field == 3) {
        message.success = value.value() != 0;
      } else if (field == 4) {
        message.match_index = value.value();
      } else {
        message.hint_index = value.value();
      }
    } else if (field == 2 && wire == kLength) {
      auto value = reader.skip_or_read(wire, true);
      if (!value) {
        return value.error();
      }
      message.follower_id = value.value();
    } else if (auto skipped = reader.skip_or_read(wire, false); !skipped) {
      return skipped.error();
    }
  }
  return message;
}

Result<RequestVote> decode_request_vote(std::string_view bytes) {
  Reader reader{bytes, 0};
  RequestVote message;
  while (!reader.done()) {
    auto key = reader.varint();
    if (!key) {
      return key.error();
    }
    const int field = static_cast<int>(key.value() >> 3);
    const int wire = static_cast<int>(key.value() & 7);
    if (wire == kVarint && field != 2) {
      auto value = reader.varint();
      if (!value) {
        return value.error();
      }
      if (field == 1) {
        message.term = value.value();
      } else if (field == 3) {
        message.last_log_index = value.value();
      } else if (field == 4) {
        message.last_log_term = value.value();
      }
    } else if (field == 2 && wire == kLength) {
      auto value = reader.skip_or_read(wire, true);
      if (!value) {
        return value.error();
      }
      message.candidate_id = value.value();
    } else if (auto skipped = reader.skip_or_read(wire, false); !skipped) {
      return skipped.error();
    }
  }
  return message;
}

Result<RequestVoteResponse> decode_vote_response(std::string_view bytes) {
  Reader reader{bytes, 0};
  RequestVoteResponse message;
  while (!reader.done()) {
    auto key = reader.varint();
    if (!key) {
      return key.error();
    }
    const int field = static_cast<int>(key.value() >> 3);
    const int wire = static_cast<int>(key.value() & 7);
    if (wire == kVarint && field != 2) {
      auto value = reader.varint();
      if (!value) {
        return value.error();
      }
      if (field == 1) {
        message.term = value.value();
      } else if (field == 3) {
        message.vote_granted = value.value() != 0;
      }
    } else if (field == 2 && wire == kLength) {
      auto value = reader.skip_or_read(wire, true);
      if (!value) {
        return value.error();
      }
      message.voter_id = value.value();
    } else if (auto skipped = reader.skip_or_read(wire, false); !skipped) {
      return skipped.error();
    }
  }
  return message;
}

Result<InstallSnapshot> decode_install_snapshot(std::string_view bytes) {
  Reader reader{bytes, 0};
  InstallSnapshot message;
  while (!reader.done()) {
    auto key = reader.varint();
    if (!key) {
      return key.error();
    }
    const int field = static_cast<int>(key.value() >> 3);
    const int wire = static_cast<int>(key.value() & 7);
    if (wire == kVarint && (field == 1 || field == 3 || field == 4)) {
      auto value = reader.varint();
      if (!value) {
        return value.error();
      }
      if (field == 1) {
        message.term = value.value();
      } else if (field == 3) {
        message.last_included_index = value.value();
      } else {
        message.last_included_term = value.value();
      }
    } else if (wire == kLength && (field == 2 || field == 5 || field == 6)) {
      auto value = reader.skip_or_read(wire, true);
      if (!value) {
        return value.error();
      }
      if (field == 2) {
        message.leader_id = value.value();
      } else if (field == 5) {
        message.state_hash = value.value();
      } else {
        message.payload = value.value();
      }
    } else if (auto skipped = reader.skip_or_read(wire, false); !skipped) {
      return skipped.error();
    }
  }
  return message;
}

Result<InstallSnapshotResponse> decode_snapshot_response(std::string_view bytes) {
  Reader reader{bytes, 0};
  InstallSnapshotResponse message;
  while (!reader.done()) {
    auto key = reader.varint();
    if (!key) {
      return key.error();
    }
    const int field = static_cast<int>(key.value() >> 3);
    const int wire = static_cast<int>(key.value() & 7);
    if (wire == kVarint && field != 2) {
      auto value = reader.varint();
      if (!value) {
        return value.error();
      }
      if (field == 1) {
        message.term = value.value();
      } else if (field == 3) {
        message.success = value.value() != 0;
      }
    } else if (field == 2 && wire == kLength) {
      auto value = reader.skip_or_read(wire, true);
      if (!value) {
        return value.error();
      }
      message.follower_id = value.value();
    } else if (auto skipped = reader.skip_or_read(wire, false); !skipped) {
      return skipped.error();
    }
  }
  return message;
}

Result<ProtocolError> decode_protocol_error(std::string_view bytes) {
  Reader reader{bytes, 0};
  ProtocolError message;
  while (!reader.done()) {
    auto key = reader.varint();
    if (!key) {
      return key.error();
    }
    const int field = static_cast<int>(key.value() >> 3);
    const int wire = static_cast<int>(key.value() & 7);
    if (wire == kLength && (field == 1 || field == 2)) {
      auto value = reader.skip_or_read(wire, true);
      if (!value) {
        return value.error();
      }
      if (field == 1) {
        message.code = value.value();
      } else {
        message.detail = value.value();
      }
    } else if (auto skipped = reader.skip_or_read(wire, false); !skipped) {
      return skipped.error();
    }
  }
  return message;
}

Result<ClientCommand> decode_client_command(std::string_view bytes) {
  Reader reader{bytes, 0};
  ClientCommand message;
  while (!reader.done()) {
    auto key = reader.varint();
    if (!key) {
      return key.error();
    }
    const int field = static_cast<int>(key.value() >> 3);
    const int wire = static_cast<int>(key.value() & 7);
    if (field == 1 && wire == kLength) {
      auto value = reader.skip_or_read(wire, true);
      if (!value) {
        return value.error();
      }
      message.canonical = value.value();
    } else if (auto skipped = reader.skip_or_read(wire, false); !skipped) {
      return skipped.error();
    }
  }
  return message;
}

Result<ClientResponse> decode_client_response(std::string_view bytes) {
  Reader reader{bytes, 0};
  ClientResponse message;
  while (!reader.done()) {
    auto key = reader.varint();
    if (!key) {
      return key.error();
    }
    const int field = static_cast<int>(key.value() >> 3);
    const int wire = static_cast<int>(key.value() & 7);
    if (wire == kVarint && (field == 1 || field == 2 || field == 6)) {
      auto value = reader.varint();
      if (!value) {
        return value.error();
      }
      if (field == 1) {
        message.ok = value.value() != 0;
      } else if (field == 2) {
        message.duplicate = value.value() != 0;
      } else {
        message.index = value.value();
      }
    } else if (wire == kLength && field >= 3 && field <= 8 && field != 6) {
      auto value = reader.skip_or_read(wire, true);
      if (!value) {
        return value.error();
      }
      if (field == 3) {
        message.error_code = value.value();
      } else if (field == 4) {
        message.error_message = value.value();
      } else if (field == 5) {
        message.leader_id = value.value();
      } else if (field == 7) {
        message.event_canonical = value.value();
      } else {
        message.state_hash = value.value();
      }
    } else if (auto skipped = reader.skip_or_read(wire, false); !skipped) {
      return skipped.error();
    }
  }
  return message;
}

Result<StatusResponse> decode_status_response(std::string_view bytes) {
  Reader reader{bytes, 0};
  StatusResponse message;
  while (!reader.done()) {
    auto key = reader.varint();
    if (!key) {
      return key.error();
    }
    const int field = static_cast<int>(key.value() >> 3);
    const int wire = static_cast<int>(key.value() & 7);
    if (wire == kVarint && (field == 4 || field == 5 || field == 6)) {
      auto value = reader.varint();
      if (!value) {
        return value.error();
      }
      if (field == 4) {
        message.term = value.value();
      } else if (field == 5) {
        message.commit_index = value.value();
      } else {
        message.last_log_index = value.value();
      }
    } else if (wire == kLength &&
               (field == 1 || field == 2 || field == 3 || field == 7 || field == 8)) {
      auto value = reader.skip_or_read(wire, true);
      if (!value) {
        return value.error();
      }
      if (field == 1) {
        message.node_id = value.value();
      } else if (field == 2) {
        message.leader_id = value.value();
      } else if (field == 3) {
        message.role = value.value();
      } else if (field == 7) {
        message.state_hash = value.value();
      } else {
        message.canonical_state = value.value();
      }
    } else if (auto skipped = reader.skip_or_read(wire, false); !skipped) {
      return skipped.error();
    }
  }
  return message;
}

}  // namespace choreoos::protocol
