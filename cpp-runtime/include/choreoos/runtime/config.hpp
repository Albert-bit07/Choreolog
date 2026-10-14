#pragma once

// Flat node configuration. Files use "key: value" lines. A peer is
// "id@host:port". This is the subset of configs/nodeN.yaml that the node reads.

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "choreoos/state/error.hpp"

namespace choreoos::runtime {

struct PeerEndpoint {
  std::string id;
  std::string host;
  std::uint16_t port = 0;
};

struct PeerTlsConfig {
  std::string host = "127.0.0.1";
  std::uint16_t port = 0;
  std::filesystem::path ca_file;
  std::filesystem::path certificate_file;
  std::filesystem::path private_key_file;
  std::vector<PeerEndpoint> peers;
};

struct NodeConfig {
  std::string id;
  std::string leader_id;
  std::filesystem::path data;
  std::string host = "127.0.0.1";
  std::uint16_t port = 0;
  std::vector<PeerEndpoint> peers;
  // False keeps the configured leader. True runs Raft-like elections.
  bool elections = false;
  // Explicitly permits loopback-only management fault injection.
  bool test_mode = false;
  std::uint64_t rng_seed = 1;
  // One replica tick every heartbeat_ms. A leader heartbeats on every tick and
  // a follower starts an election after a random number of silent ticks drawn
  // from [election_timeout_min, election_timeout_max]. Raise heartbeat_ms on
  // slow disks or networks: a blocking fsync longer than the election timeout
  // (heartbeat_ms * election_timeout_min) can trigger a spurious election.
  std::uint32_t heartbeat_ms = 50;
  std::uint32_t election_timeout_min = 3;
  std::uint32_t election_timeout_max = 6;
  std::optional<PeerTlsConfig> peer_tls = std::nullopt;
};

// Called both by the file loader and NodeServer::open for programmatic configs.
[[nodiscard]] choreoos::state::Result<void> validate_peer_tls_config(const NodeConfig& config);

[[nodiscard]] choreoos::state::Result<NodeConfig> load_node_config(
    const std::filesystem::path& path);

}  // namespace choreoos::runtime
