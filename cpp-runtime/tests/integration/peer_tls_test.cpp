#include <gtest/gtest.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>

#include <array>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include "choreoos/network/node_server.hpp"
#include "choreoos/protocol/client.hpp"
#include "choreoos/protocol/codec.hpp"
#include "choreoos/protocol/command_codec.hpp"

namespace choreoos::network {
namespace {

using boost::asio::ip::tcp;
using Key = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;
using Certificate = std::unique_ptr<X509, decltype(&X509_free)>;
using namespace std::chrono_literals;

void require(bool condition) {
  if (!condition) throw std::runtime_error("test PKI construction failed");
}

void extension(X509* certificate, X509* issuer, int nid, const std::string& value) {
  X509V3_CTX context;
  X509V3_set_ctx(&context, issuer, certificate, nullptr, nullptr, 0);
  std::unique_ptr<X509_EXTENSION, decltype(&X509_EXTENSION_free)> ext(
      X509V3_EXT_conf_nid(nullptr, &context, nid, value.c_str()), X509_EXTENSION_free);
  require(ext && X509_add_ext(certificate, ext.get(), -1) == 1);
}

struct Identity {
  Key key{EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "prime256v1"), EVP_PKEY_free};
  Certificate certificate{X509_new(), X509_free};
};

std::unique_ptr<Identity> issue(const std::string& id, Identity* authority = nullptr,
                                bool expired = false, const std::string& san = "auto",
                                bool client_only = false) {
  static long serial = 0;
  auto result = std::make_unique<Identity>();
  require(result->key && result->certificate);
  auto* certificate = result->certificate.get();
  require(X509_set_version(certificate, 2) == 1);
  require(ASN1_INTEGER_set(X509_get_serialNumber(certificate), ++serial) == 1);
  require(X509_gmtime_adj(X509_getm_notBefore(certificate), -3600) != nullptr);
  require(X509_gmtime_adj(X509_getm_notAfter(certificate), expired ? -60 : 86400) != nullptr);
  require(X509_set_pubkey(certificate, result->key.get()) == 1);
  auto* subject = X509_get_subject_name(certificate);
  require(X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC,
                                     reinterpret_cast<const unsigned char*>(id.c_str()), -1, -1,
                                     0) == 1);
  auto* signer = authority ? authority : result.get();
  require(X509_set_issuer_name(certificate, X509_get_subject_name(signer->certificate.get())) == 1);
  extension(certificate, signer->certificate.get(), NID_basic_constraints,
            authority ? "critical,CA:FALSE" : "critical,CA:TRUE");
  extension(certificate, signer->certificate.get(), NID_key_usage,
            authority ? "critical,digitalSignature" : "critical,keyCertSign,cRLSign");
  if (authority) {
    extension(certificate, signer->certificate.get(), NID_ext_key_usage,
              client_only ? "clientAuth" : "serverAuth,clientAuth");
    if (!san.empty())
      extension(certificate, signer->certificate.get(), NID_subject_alt_name,
                san == "auto" ? "URI:urn:choreoos:node:" + id : san);
  }
  require(X509_sign(certificate, signer->key.get(), EVP_sha256()) > 0);
  return result;
}

void save_certificate(const std::filesystem::path& path, X509* certificate) {
  std::unique_ptr<BIO, decltype(&BIO_free)> file(BIO_new_file(path.string().c_str(), "wb"),
                                                 BIO_free);
  require(file && PEM_write_bio_X509(file.get(), certificate) == 1);
}

void save_identity(const std::filesystem::path& root, const std::string& name, Identity& identity) {
  save_certificate(root / (name + ".pem"), identity.certificate.get());
  const auto path = root / (name + ".key");
  std::unique_ptr<BIO, decltype(&BIO_free)> file(BIO_new_file(path.string().c_str(), "wb"),
                                                 BIO_free);
  require(file && PEM_write_bio_PrivateKey(file.get(), identity.key.get(), nullptr, nullptr, 0,
                                           nullptr, nullptr) == 1);
  std::filesystem::permissions(
      path, std::filesystem::perms::owner_read | std::filesystem::perms::owner_write);
}

std::array<std::uint16_t, 6> ports() {
  boost::asio::io_context io;
  std::vector<tcp::acceptor> held;
  std::array<std::uint16_t, 6> result{};
  for (auto& port : result) {
    held.emplace_back(io, tcp::endpoint{boost::asio::ip::make_address("127.0.0.1"), 0});
    port = held.back().local_endpoint().port();
  }
  return result;
}

class RunningNode {
 public:
  explicit RunningNode(std::unique_ptr<NodeServer> value)
      : node(std::move(value)), runner([this] { node->run(); }) {}
  ~RunningNode() { stop(); }
  void stop() {
    if (!runner.joinable()) return;
    node->stop();
    runner.join();
  }
  std::unique_ptr<NodeServer> node;

 private:
  std::thread runner;
};

// Every operation is deadline-bound; TLS 1.3 may reject a client certificate
// after the client's handshake completes, so require an actual RPC response.
class Probe : public std::enable_shared_from_this<Probe> {
 public:
  Probe(const std::filesystem::path& root, const std::string& identity, std::uint16_t port,
        std::string hello_id, std::string claimed_id, int maximum_version = TLS1_3_VERSION,
        bool status_request = false)
      : context(boost::asio::ssl::context::tls_client),
        timer(io),
        endpoint(tcp::v4(), port),
        hello(std::move(hello_id)),
        claimed(std::move(claimed_id)),
        status(status_request) {
    endpoint.address(boost::asio::ip::make_address("127.0.0.1"));
    context.load_verify_file((root / "ca.pem").string());
    context.set_verify_mode(boost::asio::ssl::verify_peer);
    require(SSL_CTX_set_max_proto_version(context.native_handle(), maximum_version) == 1);
    if (!identity.empty()) {
      context.use_certificate_chain_file((root / (identity + ".pem")).string());
      context.use_private_key_file((root / (identity + ".key")).string(),
                                   boost::asio::ssl::context::pem);
    }
    stream = std::make_unique<boost::asio::ssl::stream<tcp::socket>>(io, context);
  }

  bool run() {
    auto self = shared_from_this();
    timer.expires_after(2s);
    timer.async_wait([self](const boost::system::error_code& error) {
      if (!error) self->finish(false);
    });
    stream->next_layer().async_connect(endpoint, [self](const boost::system::error_code& error) {
      if (error) {
        self->finish(false);
        return;
      }
      self->stream->async_handshake(boost::asio::ssl::stream_base::client,
                                    [self](const boost::system::error_code& failure) {
                                      if (failure)
                                        self->finish(false);
                                      else
                                        self->read_frame(true);
                                    });
    });
    io.run();
    return succeeded;
  }

 private:
  void finish(bool success) {
    if (done) return;
    done = true;
    succeeded = success;
    timer.cancel();
    boost::system::error_code ignored;
    stream->next_layer().close(ignored);
  }

  void read_frame(bool greeting) {
    auto self = shared_from_this();
    boost::asio::async_read(
        *stream, boost::asio::buffer(header),
        [self, greeting](const boost::system::error_code& error, std::size_t) {
          if (error) {
            self->finish(false);
            return;
          }
          auto parsed = protocol::peek_header(self->header.data(), self->header.size());
          if (!parsed) {
            self->finish(false);
            return;
          }
          self->body.resize(parsed.value().payload_length + 4);
          boost::asio::async_read(
              *self->stream, boost::asio::buffer(self->body),
              [self, greeting](const boost::system::error_code& failure, std::size_t) {
                if (failure) {
                  self->finish(false);
                  return;
                }
                std::vector<std::uint8_t> bytes(self->header.begin(), self->header.end());
                bytes.insert(bytes.end(), self->body.begin(), self->body.end());
                auto frame = protocol::decode_frame(bytes.data(), bytes.size());
                if (!frame) {
                  self->finish(false);
                  return;
                }
                if (greeting) {
                  self->send_request();
                  return;
                }
                if (self->status) {
                  auto reply = protocol::decode_status_response(frame.value().payload);
                  self->finish(frame.value().type == protocol::MessageType::StatusResponse &&
                               reply);
                  return;
                }
                auto reply = protocol::decode_append_response(frame.value().payload);
                self->finish(frame.value().type == protocol::MessageType::AppendEntriesResponse &&
                             reply && reply.value().success);
              });
        });
  }

  void send_request() {
    protocol::Frame frame;
    frame.type = protocol::MessageType::Handshake;
    frame.payload = protocol::encode(protocol::Handshake{hello, 1, 0}).value();
    outgoing = protocol::encode_frame(frame).value();
    frame.type = status ? protocol::MessageType::StatusQuery : protocol::MessageType::AppendEntries;
    frame.payload =
        status ? protocol::encode_status_query().value()
               : protocol::encode(protocol::AppendEntries{7, claimed, 0, 0, {}, 0}).value();
    outgoing += protocol::encode_frame(frame).value();
    auto self = shared_from_this();
    boost::asio::async_write(*stream, boost::asio::buffer(outgoing),
                             [self](const boost::system::error_code& error, std::size_t) {
                               if (error)
                                 self->finish(false);
                               else
                                 self->read_frame(false);
                             });
  }

  boost::asio::io_context io;
  boost::asio::ssl::context context;
  std::unique_ptr<boost::asio::ssl::stream<tcp::socket>> stream;
  boost::asio::steady_timer timer;
  tcp::endpoint endpoint;
  std::string hello;
  std::string claimed;
  bool status;
  std::array<std::uint8_t, protocol::kFrameHeaderBytes> header{};
  std::vector<std::uint8_t> body;
  std::string outgoing;
  bool done = false;
  bool succeeded = false;
};

class PeerTlsTest : public testing::Test {
 protected:
  std::filesystem::path root;
  std::array<std::uint16_t, 6> endpoints{};
  std::unique_ptr<Identity> ca;
  std::unique_ptr<Identity> other_ca;

  virtual runtime::ConsensusEngine backend() const { return runtime::ConsensusEngine::Legacy; }
  void check_identity_binding();
  void check_certificate_rejections();
  void check_plaintext_rejection();
  void check_failover_recovery();

  void SetUp() override {
    root = std::filesystem::temp_directory_path() /
           (std::string("choreoos-peer-tls-") +
            testing::UnitTest::GetInstance()->current_test_info()->test_suite_name() + "-" +
            testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);
    endpoints = ports();
    ca = issue("test-root");
    other_ca = issue("other-root");
    save_certificate(root / "ca.pem", ca->certificate.get());
    for (const auto* name : {"node-a", "node-b", "node-c"}) {
      auto identity = issue(name, ca.get());
      save_identity(root, name, *identity);
    }
  }

  void TearDown() override { std::filesystem::remove_all(root); }

  runtime::NodeConfig config(std::size_t index) {
    const std::array<std::string, 3> ids{"node-a", "node-b", "node-c"};
    runtime::NodeConfig result;
    result.id = ids[index];
    result.leader_id = "node-a";
    result.consensus_engine = backend();
    result.elections = backend() == runtime::ConsensusEngine::Transition;
    result.data = root / ids[index];
    result.port = endpoints[index];
    result.heartbeat_ms = 50;
    result.peer_tls.emplace();
    auto& tls = *result.peer_tls;
    tls.port = endpoints[index + 3];
    tls.ca_file = root / "ca.pem";
    tls.certificate_file = root / (ids[index] + ".pem");
    tls.private_key_file = root / (ids[index] + ".key");
    for (std::size_t i = 0; i < ids.size(); ++i) {
      if (i == index) continue;
      result.peers.push_back({ids[i], "127.0.0.1", endpoints[i]});
      tls.peers.push_back({ids[i], "127.0.0.1", endpoints[i + 3]});
    }
    return result;
  }

  state::Result<protocol::StatusResponse> status_of(std::uint16_t port) {
    protocol::Frame query;
    query.type = protocol::MessageType::StatusQuery;
    query.payload = protocol::encode_status_query().value();
    auto result = protocol::transact("127.0.0.1", port, query, 1s);
    if (!result) return result.error();
    return protocol::decode_status_response(result.value().payload);
  }
};

class TransitionPeerTlsTest : public PeerTlsTest {
 protected:
  runtime::ConsensusEngine backend() const override { return runtime::ConsensusEngine::Transition; }
};

void PeerTlsTest::check_identity_binding() {
  auto value = config(0);
  value.elections = true;
  value.heartbeat_ms = 10000;
  auto opened = NodeServer::open(value);
  ASSERT_TRUE(opened) << opened.error().to_string();
  RunningNode live(std::move(opened.value()));
  EXPECT_FALSE(std::make_shared<Probe>(root, "node-b", endpoints[3], "node-c", "node-c")->run());
  EXPECT_FALSE(std::make_shared<Probe>(root, "node-b", endpoints[3], "node-b", "node-c")->run());
  auto before = status_of(endpoints[0]);
  ASSERT_TRUE(before);
  EXPECT_EQ(before.value().term, 1u);
  EXPECT_TRUE(std::make_shared<Probe>(root, "node-b", endpoints[3], "node-b", "node-b")->run());
  auto after = status_of(endpoints[0]);
  ASSERT_TRUE(after);
  EXPECT_EQ(after.value().term, 7u);
}

TEST_F(PeerTlsTest, TrustedCertificateCanSendPeerRpcButCannotChangeItsClaimedIdentity) {
  check_identity_binding();
}

TEST_F(TransitionPeerTlsTest, TrustedCertificateCanSendPeerRpcButCannotChangeItsClaimedIdentity) {
  check_identity_binding();
}

void PeerTlsTest::check_certificate_rejections() {
  auto untrusted = issue("node-b", other_ca.get());
  save_identity(root, "untrusted", *untrusted);
  auto expired = issue("node-b", ca.get(), true);
  save_identity(root, "expired", *expired);
  auto unknown = issue("stranger", ca.get());
  save_identity(root, "unknown", *unknown);
  auto client_only = issue("node-b", ca.get(), false, "auto", true);
  save_identity(root, "client-only", *client_only);
  auto cn_only = issue("node-b", ca.get(), false, "");
  save_identity(root, "cn-only", *cn_only);
  auto ambiguous =
      issue("node-b", ca.get(), false, "URI:urn:choreoos:node:node-b,URI:urn:choreoos:node:node-c");
  save_identity(root, "ambiguous", *ambiguous);
  auto value = config(0);
  value.elections = true;
  value.heartbeat_ms = 10000;
  auto opened = NodeServer::open(value);
  ASSERT_TRUE(opened);
  RunningNode live(std::move(opened.value()));
  for (const auto* name :
       {"", "untrusted", "expired", "unknown", "client-only", "cn-only", "ambiguous"}) {
    EXPECT_FALSE(std::make_shared<Probe>(root, name, endpoints[3], "node-b", "node-b")->run())
        << name;
  }
  EXPECT_FALSE(
      std::make_shared<Probe>(root, "node-b", endpoints[3], "node-b", "node-b", TLS1_2_VERSION)
          ->run());
  EXPECT_FALSE(std::make_shared<Probe>(root, "node-b", endpoints[3], "node-b", "node-b",
                                       TLS1_3_VERSION, true)
                   ->run());
  auto before = status_of(endpoints[0]);
  ASSERT_TRUE(before);
  EXPECT_EQ(before.value().term, 1u);
  EXPECT_TRUE(std::make_shared<Probe>(root, "node-b", endpoints[3], "node-b", "node-b")->run());
}

TEST_F(PeerTlsTest, UntrustedExpiredUnknownAndAmbiguousCertificatesAreRejected) {
  check_certificate_rejections();
}

TEST_F(TransitionPeerTlsTest, UntrustedExpiredUnknownAndAmbiguousCertificatesAreRejected) {
  check_certificate_rejections();
}

void PeerTlsTest::check_plaintext_rejection() {
  auto value = config(0);
  value.elections = true;
  value.heartbeat_ms = 10000;
  auto opened = NodeServer::open(value);
  ASSERT_TRUE(opened);
  RunningNode live(std::move(opened.value()));
  protocol::Frame hello;
  hello.type = protocol::MessageType::Handshake;
  hello.payload = protocol::encode(protocol::Handshake{"node-b", 1, 0}).value();
  auto bytes = protocol::encode_frame(hello).value();
  hello.type = protocol::MessageType::AppendEntries;
  hello.payload = protocol::encode(protocol::AppendEntries{999, "node-b", 0, 0, {}, 0}).value();
  bytes += protocol::encode_frame(hello).value();
  boost::asio::io_context io;
  tcp::socket socket(io);
  socket.connect({boost::asio::ip::make_address("127.0.0.1"), endpoints[0]});
  boost::system::error_code error;
  boost::asio::write(socket, boost::asio::buffer(bytes), error);
  socket.non_blocking(true);
  bool got_reply = false;
  std::array<char, 1> received{};
  const auto until = std::chrono::steady_clock::now() + 1s;
  while (std::chrono::steady_clock::now() < until) {
    const auto count = socket.read_some(boost::asio::buffer(received), error);
    if (count != 0) {
      got_reply = true;
      break;
    }
    if (error != boost::asio::error::would_block && error != boost::asio::error::try_again) break;
    std::this_thread::sleep_for(5ms);
  }
  EXPECT_FALSE(got_reply);
  auto status = status_of(endpoints[0]);
  ASSERT_TRUE(status);
  EXPECT_EQ(status.value().term, 1u);
}

TEST_F(PeerTlsTest, PlaintextHandshakeCannotBypassCertificateAuthentication) {
  check_plaintext_rejection();
}

TEST_F(TransitionPeerTlsTest, PlaintextHandshakeCannotBypassCertificateAuthentication) {
  check_plaintext_rejection();
}

TEST_F(PeerTlsTest, InvalidLocalIdentityAndProgrammaticConfigFailBeforeOpeningTheStore) {
  auto value = config(0);
  value.host = "0.0.0.0";
  EXPECT_FALSE(NodeServer::open(value));
  value = config(0);
  value.peer_tls->certificate_file = root / "node-b.pem";
  value.peer_tls->private_key_file = root / "node-b.key";
  EXPECT_FALSE(NodeServer::open(value));
  value = config(0);
  value.peer_tls->private_key_file = root / "node-b.key";
  EXPECT_FALSE(NodeServer::open(value));
  value = config(0);
  value.peer_tls->peers.pop_back();
  EXPECT_FALSE(NodeServer::open(value));
  auto expired = issue("node-a", ca.get(), true);
  save_identity(root, "expired-a", *expired);
  value = config(0);
  value.peer_tls->certificate_file = root / "expired-a.pem";
  value.peer_tls->private_key_file = root / "expired-a.key";
  EXPECT_FALSE(NodeServer::open(value));
  auto client_only = issue("node-a", ca.get(), false, "auto", true);
  save_identity(root, "client-only-a", *client_only);
  value.peer_tls->certificate_file = root / "client-only-a.pem";
  value.peer_tls->private_key_file = root / "client-only-a.key";
  EXPECT_FALSE(NodeServer::open(value));
  EXPECT_FALSE(std::filesystem::exists(value.data));
}

void PeerTlsTest::check_failover_recovery() {
  std::array<runtime::NodeConfig, 3> configs{config(0), config(1), config(2)};
  std::array<std::unique_ptr<RunningNode>, 3> live;
  for (std::size_t i = 0; i < live.size(); ++i) {
    configs[i].elections = true;
    configs[i].election_timeout_min = 10;
    configs[i].election_timeout_max = 20;
    auto opened = NodeServer::open(configs[i]);
    ASSERT_TRUE(opened) << opened.error().to_string();
    live[i] = std::make_unique<RunningNode>(std::move(opened.value()));
  }
  auto find_leader = [&]() -> int {
    for (int attempt = 0; attempt < 300; ++attempt) {
      for (std::size_t i = 0; i < live.size(); ++i) {
        if (!live[i]) continue;
        auto state = status_of(endpoints[i]);
        if (state && state.value().role == "leader") return static_cast<int>(i);
      }
      std::this_thread::sleep_for(25ms);
    }
    return -1;
  };
  const int first_leader = find_leader();
  ASSERT_GE(first_leader, 0);
  using namespace choreoos::state;
  const auto id = ChoreographyId::parse("opening").value();
  Command create{CommandId::parse("elected-create").value(),
                 id,
                 CommandType::CreateChoreography,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(0).value(),
                 CreateChoreographyPayload{id, StageBounds::from_mm(20000, 12000).value(),
                                           OverlapPolicy::Forbidden, 5000}};
  auto submit = [&](int index, const Command& command) -> Result<protocol::ClientResponse> {
    protocol::Frame frame;
    frame.type = protocol::MessageType::ClientCommand;
    frame.payload =
        protocol::encode(protocol::ClientCommand{protocol::canonical_command(command)}).value();
    auto response =
        protocol::transact("127.0.0.1", endpoints[static_cast<std::size_t>(index)], frame, 5s);
    if (!response) return response.error();
    return protocol::decode_client_response(response.value().payload);
  };
  auto first = submit(first_leader, create);
  ASSERT_TRUE(first);
  ASSERT_TRUE(first.value().ok) << first.value().error_message;
  const auto old = static_cast<std::size_t>(first_leader);
  live[old].reset();
  const int replacement = find_leader();
  ASSERT_GE(replacement, 0);
  ASSERT_NE(replacement, first_leader);
  Command add{
      CommandId::parse("elected-add").value(),
      id,
      CommandType::AddDancer,
      kCurrentSchemaVersion,
      MusicalTick::from_count(1).value(),
      DancerPayload{DancerId::parse("alice").value(), Position::from_mm(1000, 1000).value()}};
  auto second = submit(replacement, add);
  ASSERT_TRUE(second);
  ASSERT_TRUE(second.value().ok) << second.value().error_message;
  auto reopened = NodeServer::open(configs[old]);
  ASSERT_TRUE(reopened);
  live[old] = std::make_unique<RunningNode>(std::move(reopened.value()));
  bool converged = false;
  for (int attempt = 0; attempt < 200; ++attempt) {
    auto a = status_of(endpoints[0]);
    auto b = status_of(endpoints[1]);
    auto c = status_of(endpoints[2]);
    if (a && b && c && a.value().commit_index >= second.value().index &&
        a.value().commit_index == b.value().commit_index &&
        a.value().commit_index == c.value().commit_index &&
        a.value().state_hash == b.value().state_hash &&
        a.value().state_hash == c.value().state_hash) {
      converged = true;
      break;
    }
    std::this_thread::sleep_for(25ms);
  }
  EXPECT_TRUE(converged);
}

TEST_F(PeerTlsTest, LeaderFailureAndDiskRecoveryConvergeOverAuthenticatedChannels) {
  check_failover_recovery();
}

TEST_F(TransitionPeerTlsTest, LeaderFailureAndDiskRecoveryConvergeOverAuthenticatedChannels) {
  check_failover_recovery();
}

TEST_F(PeerTlsTest, OutboundCertificateMustMatchTheDialedMemberEvenWhenItIsAnotherKnownPeer) {
  auto a_config = config(0);
  // B's route reaches a server presenting C's otherwise trusted certificate.
  a_config.peer_tls->peers[0].port = endpoints[5];
  a_config.peer_tls->peers[1].port = endpoints[4];
  auto c_config = config(2);
  for (auto& peer : c_config.peer_tls->peers) peer.port = endpoints[4];
  auto c_opened = NodeServer::open(c_config);
  ASSERT_TRUE(c_opened);
  RunningNode c(std::move(c_opened.value()));
  auto a_opened = NodeServer::open(a_config);
  ASSERT_TRUE(a_opened);
  RunningNode a(std::move(a_opened.value()));
  using namespace choreoos::state;
  const auto id = ChoreographyId::parse("opening").value();
  Command command{CommandId::parse("misroute").value(),
                  id,
                  CommandType::CreateChoreography,
                  kCurrentSchemaVersion,
                  MusicalTick::from_count(0).value(),
                  CreateChoreographyPayload{id, StageBounds::from_mm(20000, 12000).value(),
                                            OverlapPolicy::Forbidden, 5000}};
  protocol::Frame request;
  request.type = protocol::MessageType::ClientCommand;
  request.payload =
      protocol::encode(protocol::ClientCommand{protocol::canonical_command(command)}).value();
  auto response = protocol::transact("127.0.0.1", endpoints[0], request, 5s);
  ASSERT_TRUE(response);
  auto result = protocol::decode_client_response(response.value().payload);
  ASSERT_TRUE(result);
  EXPECT_FALSE(result.value().ok);
  auto state = status_of(endpoints[2]);
  ASSERT_TRUE(state);
  EXPECT_EQ(state.value().last_log_index, 0u);
  a.stop();
  EXPECT_GE(a.node->metrics().rejected, 1u);
}

TEST_F(PeerTlsTest, ThreeNodesCommitAndReconnectAfterCertificateRotationWithOverlappingTrust) {
  // Both roots are trusted before rotating one peer to a new CA/key.
  save_certificate(root / "other-ca.pem", other_ca->certificate.get());
  {
    std::ifstream in(root / "other-ca.pem", std::ios::binary);
    std::ofstream out(root / "ca.pem", std::ios::binary | std::ios::app);
    out << in.rdbuf();
  }
  std::array<runtime::NodeConfig, 3> configs{config(0), config(1), config(2)};
  std::array<std::unique_ptr<RunningNode>, 3> live;
  for (std::size_t i = 0; i < live.size(); ++i) {
    auto opened = NodeServer::open(configs[i]);
    ASSERT_TRUE(opened) << opened.error().to_string();
    live[i] = std::make_unique<RunningNode>(std::move(opened.value()));
  }
  using namespace choreoos::state;
  const auto id = ChoreographyId::parse("opening").value();
  Command create{CommandId::parse("tls-create").value(),
                 id,
                 CommandType::CreateChoreography,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(0).value(),
                 CreateChoreographyPayload{id, StageBounds::from_mm(20000, 12000).value(),
                                           OverlapPolicy::Forbidden, 5000}};
  auto submit = [&](const Command& command) {
    protocol::Frame frame;
    frame.type = protocol::MessageType::ClientCommand;
    frame.payload =
        protocol::encode(protocol::ClientCommand{protocol::canonical_command(command)}).value();
    return protocol::transact("127.0.0.1", endpoints[0], frame, 5s);
  };
  auto first = submit(create);
  ASSERT_TRUE(first) << first.error().to_string();
  auto committed = protocol::decode_client_response(first.value().payload);
  ASSERT_TRUE(committed);
  ASSERT_TRUE(committed.value().ok) << committed.value().error_message;
  live[1].reset();
  auto rotated = issue("node-b", other_ca.get());
  save_identity(root, "node-b", *rotated);
  auto reopened = NodeServer::open(configs[1]);
  ASSERT_TRUE(reopened);
  live[1] = std::make_unique<RunningNode>(std::move(reopened.value()));
  Command add{
      CommandId::parse("tls-add").value(),
      id,
      CommandType::AddDancer,
      kCurrentSchemaVersion,
      MusicalTick::from_count(1).value(),
      DancerPayload{DancerId::parse("alice").value(), Position::from_mm(1000, 1000).value()}};
  auto second = submit(add);
  ASSERT_TRUE(second);
  auto result = protocol::decode_client_response(second.value().payload);
  ASSERT_TRUE(result);
  ASSERT_TRUE(result.value().ok) << result.value().error_message;
  bool converged = false;
  for (int attempt = 0; attempt < 100; ++attempt) {
    auto a = status_of(endpoints[0]);
    auto b = status_of(endpoints[1]);
    auto c = status_of(endpoints[2]);
    if (a && b && c && a.value().commit_index == 2 && b.value().commit_index == 2 &&
        c.value().commit_index == 2 && a.value().state_hash == b.value().state_hash &&
        a.value().state_hash == c.value().state_hash) {
      converged = true;
      break;
    }
    std::this_thread::sleep_for(25ms);
  }
  EXPECT_TRUE(converged);
}

}  // namespace
}  // namespace choreoos::network
