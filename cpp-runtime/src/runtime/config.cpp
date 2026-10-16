#include "choreoos/runtime/config.hpp"

#include <boost/asio/ip/address.hpp>
#include <cctype>
#include <charconv>
#include <fstream>
#include <set>
#include <sstream>

#include "choreoos/state/types.hpp"

namespace choreoos::runtime {
namespace {

using choreoos::state::Error;
using choreoos::state::ErrorCode;
using choreoos::state::Result;

std::string trim(std::string text) {
  while (!text.empty() && std::isspace(static_cast<unsigned char>(text.front()))) {
    text.erase(text.begin());
  }
  while (!text.empty() && std::isspace(static_cast<unsigned char>(text.back()))) {
    text.pop_back();
  }
  return text;
}

Result<PeerEndpoint> parse_peer(const std::string& text) {
  const auto at = text.find('@');
  const auto colon = text.rfind(':');
  if (at == std::string::npos || colon == std::string::npos || colon < at) {
    return Error{ErrorCode::StoreError, "peer must look like id@host:port"};
  }
  PeerEndpoint peer;
  peer.id = text.substr(0, at);
  peer.host = text.substr(at + 1, colon - at - 1);
  if (peer.id.empty() || peer.host.empty()) {
    return Error{ErrorCode::StoreError, "peer id and host must not be empty"};
  }
  const std::string port_text = text.substr(colon + 1);
  std::uint32_t port = 0;
  const auto [end, error] =
      std::from_chars(port_text.data(), port_text.data() + port_text.size(), port);
  if (port_text.empty() || error != std::errc{} || end != port_text.data() + port_text.size() ||
      port == 0 || port > 65535) {
    return Error{ErrorCode::StoreError, "peer port must be a whole number from 1 to 65535"};
  }
  peer.port = static_cast<std::uint16_t>(port);
  return peer;
}

// Parse a whole decimal number within [min, max]. std::stoi and std::stoull
// throw on bad input, which would terminate the node on a config typo.
Result<std::uint64_t> parse_number(const std::string& key, const std::string& text,
                                   std::uint64_t min, std::uint64_t max) {
  std::uint64_t value = 0;
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto [stop, error] = std::from_chars(begin, end, value);
  if (text.empty() || error != std::errc{} || stop != end || value < min || value > max) {
    return Error{ErrorCode::StoreError, "config " + key + " must be a whole number from " +
                                            std::to_string(min) + " to " + std::to_string(max)};
  }
  return value;
}

}  // namespace

Result<NodeConfig> load_node_config(const std::filesystem::path& path) {
  std::ifstream input{path};
  if (!input) {
    return Error{ErrorCode::StoreError, "unable to open node config " + path.string()};
  }
  NodeConfig config;
  std::string line;
  while (std::getline(input, line)) {
    line = trim(line);
    if (line.empty() || line[0] == '#') {
      continue;
    }
    const auto colon = line.find(':');
    if (colon == std::string::npos) {
      return Error{ErrorCode::StoreError, "config line is missing ':'"};
    }
    const std::string key = trim(line.substr(0, colon));
    const std::string value = trim(line.substr(colon + 1));
    if (key == "id") {
      config.id = value;
    } else if (key == "leader") {
      config.leader_id = value;
    } else if (key == "data") {
      config.data = value;
    } else if (key == "listen") {
      const auto split = value.rfind(':');
      if (split == std::string::npos) {
        return Error{ErrorCode::StoreError, "listen must look like host:port"};
      }
      config.host = value.substr(0, split);
      auto port = parse_number("listen port", value.substr(split + 1), 0, 65535);
      if (!port) {
        return port.error();
      }
      config.port = static_cast<std::uint16_t>(port.value());
    } else if (key == "peer" || key == "peers") {
      std::stringstream stream{value};
      std::string item;
      while (std::getline(stream, item, ',')) {
        item = trim(item);
        if (item.empty()) {
          continue;
        }
        auto peer = parse_peer(item);
        if (!peer) {
          return peer.error();
        }
        config.peers.push_back(peer.value());
      }
    } else if (key.starts_with("peer_tls_")) {
      if (!config.peer_tls) config.peer_tls.emplace();
      auto& tls = *config.peer_tls;
      if (key == "peer_tls_listen") {
        auto endpoint = parse_peer("listener@" + value);
        if (!endpoint) return endpoint.error();
        tls.host = endpoint.value().host;
        tls.port = endpoint.value().port;
      } else if (key == "peer_tls_ca") {
        tls.ca_file = value;
      } else if (key == "peer_tls_certificate") {
        tls.certificate_file = value;
      } else if (key == "peer_tls_key") {
        tls.private_key_file = value;
      } else if (key == "peer_tls_peer") {
        auto endpoint = parse_peer(value);
        if (!endpoint) return endpoint.error();
        tls.peers.push_back(endpoint.value());
      } else {
        return Error{ErrorCode::StoreError, "unknown peer TLS configuration key: " + key};
      }
    } else if (key == "consensus_engine") {
      if (value == "legacy")
        config.consensus_engine = ConsensusEngine::Legacy;
      else if (value == "transition")
        config.consensus_engine = ConsensusEngine::Transition;
      else
        return Error{ErrorCode::StoreError, "consensus_engine must be legacy or transition"};
    } else if (key == "elections") {
      config.elections = value == "true" || value == "1";
    } else if (key == "test_mode") {
      if (value != "true" && value != "false" && value != "1" && value != "0") {
        return Error{ErrorCode::StoreError, "test_mode must be true or false"};
      }
      config.test_mode = value == "true" || value == "1";
    } else if (key == "rng_seed") {
      auto seed = parse_number(key, value, 0, UINT64_MAX);
      if (!seed) {
        return seed.error();
      }
      config.rng_seed = seed.value();
    } else if (key == "snapshot_every") {
      auto interval = parse_number(key, value, 0, 100000);
      if (!interval) return interval.error();
      config.snapshot_every = interval.value();
    } else if (key == "heartbeat_ms") {
      auto number = parse_number(key, value, 10, 10000);
      if (!number) {
        return number.error();
      }
      config.heartbeat_ms = static_cast<std::uint32_t>(number.value());
    } else if (key == "election_timeout_min") {
      auto number = parse_number(key, value, 2, 1000);
      if (!number) {
        return number.error();
      }
      config.election_timeout_min = static_cast<std::uint32_t>(number.value());
    } else if (key == "election_timeout_max") {
      auto number = parse_number(key, value, 2, 1000);
      if (!number) {
        return number.error();
      }
      config.election_timeout_max = static_cast<std::uint32_t>(number.value());
    }
  }
  if (config.election_timeout_max < config.election_timeout_min) {
    return Error{ErrorCode::StoreError,
                 "config election_timeout_max must not be below election_timeout_min"};
  }
  if (config.id.empty() || config.leader_id.empty() || config.data.empty()) {
    return Error{ErrorCode::StoreError, "config requires id, leader, and data"};
  }
  if (auto valid = validate_consensus_config(config); !valid) return valid.error();
  if (auto valid = validate_peer_tls_config(config); !valid) return valid.error();
  return config;
}

Result<void> validate_consensus_config(const NodeConfig& config) {
  if (config.consensus_engine == ConsensusEngine::Legacy) return {};
  if (config.consensus_engine != ConsensusEngine::Transition || !config.elections ||
      config.election_timeout_min < 2 ||
      config.election_timeout_max < config.election_timeout_min ||
      config.election_timeout_max > 1000 || config.heartbeat_ms < 10 || config.heartbeat_ms > 10000)
    return Error{ErrorCode::StoreError,
                 "transition consensus requires elections and valid timer bounds"};
  if (!config.peer_tls) {
    boost::system::error_code error;
    const auto listener = boost::asio::ip::make_address(config.host, error);
    if (error || !listener.is_loopback())
      return Error{ErrorCode::StoreError,
                   "transition plaintext listener must be loopback; use peer TLS for remote peers"};
    for (const auto& peer : config.peers) {
      const auto address = boost::asio::ip::make_address(peer.host, error);
      if (error || !address.is_loopback())
        return Error{ErrorCode::StoreError, "transition remote peers require peer TLS"};
    }
  }
  return {};
}

Result<void> validate_peer_tls_config(const NodeConfig& config) {
  if (!config.peer_tls) return {};
  const auto& tls = *config.peer_tls;
  auto invalid = [](const std::string& why) -> Result<void> {
    return Error{ErrorCode::StoreError, "peer TLS: " + why};
  };
  if (tls.port == 0 || tls.ca_file.empty() || tls.certificate_file.empty() ||
      tls.private_key_file.empty())
    return invalid("listener, CA, certificate and key are required");
  boost::system::error_code error;
  const auto client_address = boost::asio::ip::make_address(config.host, error);
  if (error || !client_address.is_loopback())
    return invalid("plaintext client listener must be loopback");
  (void)boost::asio::ip::make_address(tls.host, error);
  if (error) return invalid("listener host must be a numeric IP address");
  if (tls.port == config.port) return invalid("peer and client ports must differ");
  if (!choreoos::state::NodeId::parse(config.id)) return invalid("invalid local node ID");
  std::set<std::string> members;
  for (const auto& peer : config.peers) {
    if (peer.id == config.id || !choreoos::state::NodeId::parse(peer.id) ||
        !members.insert(peer.id).second)
      return invalid("duplicate or invalid configured peer ID");
  }
  std::set<std::string> secured;
  for (const auto& peer : tls.peers) {
    (void)boost::asio::ip::make_address(peer.host, error);
    if (error || peer.port == 0 || !members.contains(peer.id) || !secured.insert(peer.id).second)
      return invalid("invalid, unknown or duplicate TLS peer endpoint");
  }
  if (secured != members) return invalid("every configured peer needs exactly one TLS endpoint");
  return {};
}

}  // namespace choreoos::runtime
