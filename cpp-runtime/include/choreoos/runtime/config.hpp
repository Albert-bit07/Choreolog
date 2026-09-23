#pragma once

// Flat node configuration. Files use "key: value" lines. A peer is
// "id@host:port". This is the subset of configs/nodeN.yaml that the node reads.

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "choreoos/state/error.hpp"

namespace choreoos::runtime {

struct PeerEndpoint {
  std::string id;
  std::string host;
  std::uint16_t port = 0;
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
  std::uint64_t rng_seed = 1;
};

[[nodiscard]] choreoos::state::Result<NodeConfig> load_node_config(
    const std::filesystem::path& path);

}  // namespace choreoos::runtime
