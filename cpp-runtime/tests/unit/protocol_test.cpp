#include <gtest/gtest.h>

#include <string>

#include "choreoos/protocol/codec.hpp"
#include "choreoos/protocol/command_codec.hpp"
#include "choreoos/protocol/frame.hpp"

namespace choreoos::protocol {
namespace {

using choreoos::state::ChoreographyId;
using choreoos::state::Command;
using choreoos::state::CommandId;
using choreoos::state::CommandType;
using choreoos::state::CreateChoreographyPayload;
using choreoos::state::kCurrentSchemaVersion;
using choreoos::state::MusicalTick;
using choreoos::state::OverlapPolicy;
using choreoos::state::StageBounds;

TEST(ProtocolTest, FrameRoundTripAndChecksum) {
  Frame frame;
  frame.type = MessageType::Handshake;
  frame.correlation = 42;
  frame.payload = "hello";
  auto encoded = encode_frame(frame);
  ASSERT_TRUE(encoded);
  auto decoded = decode_frame(reinterpret_cast<const std::uint8_t*>(encoded.value().data()),
                              encoded.value().size());
  ASSERT_TRUE(decoded);
  EXPECT_EQ(decoded.value().type, MessageType::Handshake);
  EXPECT_EQ(decoded.value().correlation, 42u);
  EXPECT_EQ(decoded.value().payload, "hello");

  std::string broken = encoded.value();
  broken.back() = static_cast<char>(broken.back() ^ 0x1);
  EXPECT_FALSE(decode_frame(reinterpret_cast<const std::uint8_t*>(broken.data()), broken.size()));
  EXPECT_FALSE(decode_frame(reinterpret_cast<const std::uint8_t*>(encoded.value().data()), 4));
}

TEST(ProtocolTest, OversizedLengthIsRejectedBeforeUse) {
  std::string bytes(kFrameHeaderBytes, '\0');
  bytes[0] = 'C';
  bytes[1] = 'H';
  bytes[2] = 'O';
  bytes[3] = 'S';
  bytes[4] = 1;
  bytes[16] = static_cast<char>(0xff);
  bytes[17] = static_cast<char>(0xff);
  bytes[18] = static_cast<char>(0xff);
  bytes[19] = static_cast<char>(0x7f);
  auto header = peek_header(reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size());
  EXPECT_FALSE(header);
}

TEST(ProtocolTest, HandshakeBytesStayCompatible) {
  Handshake hello;
  hello.node_id = "node-1";
  hello.major = 1;
  hello.minor = 0;
  auto bytes = encode(hello);
  ASSERT_TRUE(bytes);
  const std::string expected("\x0a\x06node-1\x10\x01", 10);
  EXPECT_EQ(bytes.value(), expected);
  auto decoded = decode_handshake(bytes.value());
  ASSERT_TRUE(decoded);
  EXPECT_EQ(decoded.value().node_id, "node-1");
}

TEST(ProtocolTest, MessagesRoundTrip) {
  AppendEntries append;
  append.term = 1;
  append.leader_id = "node-1";
  append.prev_log_index = 0;
  append.leader_commit = 0;
  auto encoded_append = encode(append);
  ASSERT_TRUE(encoded_append);
  auto decoded_append = decode_append_entries(encoded_append.value());
  ASSERT_TRUE(decoded_append);
  EXPECT_EQ(decoded_append.value().leader_id, "node-1");

  RequestVoteResponse vote;
  vote.voter_id = "node-2";
  vote.vote_granted = false;
  auto encoded_vote = encode(vote);
  ASSERT_TRUE(encoded_vote);
  auto decoded_vote = decode_vote_response(encoded_vote.value());
  ASSERT_TRUE(decoded_vote);
  EXPECT_FALSE(decoded_vote.value().vote_granted);

  InstallSnapshot snapshot;
  snapshot.leader_id = "node-1";
  snapshot.last_included_index = 4;
  snapshot.state_hash = "0123456789abcdef";
  snapshot.payload = "events";
  auto encoded_snapshot = encode(snapshot);
  ASSERT_TRUE(encoded_snapshot);
  auto decoded_snapshot = decode_install_snapshot(encoded_snapshot.value());
  ASSERT_TRUE(decoded_snapshot);
  EXPECT_EQ(decoded_snapshot.value().last_included_index, 4u);
  EXPECT_EQ(decoded_snapshot.value().payload, "events");
}

TEST(ProtocolTest, CommandTextRoundTrip) {
  Command command{CommandId::parse("c-create").value(),
                  ChoreographyId::parse("opening").value(),
                  CommandType::CreateChoreography,
                  kCurrentSchemaVersion,
                  MusicalTick::from_count(0).value(),
                  CreateChoreographyPayload{ChoreographyId::parse("opening").value(),
                                            StageBounds::from_mm(20000, 12000).value(),
                                            OverlapPolicy::Forbidden, 5000}};
  auto parsed = parse_command(canonical_command(command));
  ASSERT_TRUE(parsed);
  EXPECT_EQ(parsed.value().id.value(), "c-create");
  EXPECT_EQ(std::get<CreateChoreographyPayload>(parsed.value().payload).max_travel_mm, 5000);
}

}  // namespace
}  // namespace choreoos::protocol
