#include "choreoos/runtime/config.hpp"

#include <cctype>
#include <fstream>
#include <sstream>

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
  try {
    peer.port = static_cast<std::uint16_t>(std::stoi(text.substr(colon + 1)));
  } catch (...) {
    return Error{ErrorCode::StoreError, "peer port is invalid"};
  }
  return peer;
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
      config.port = static_cast<std::uint16_t>(std::stoi(value.substr(split + 1)));
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
    } else if (key == "elections") {
      config.elections = value == "true" || value == "1";
    } else if (key == "rng_seed") {
      config.rng_seed = std::stoull(value);
    }
  }
  if (config.id.empty() || config.leader_id.empty() || config.data.empty()) {
    return Error{ErrorCode::StoreError, "config requires id, leader, and data"};
  }
  return config;
}

}  // namespace choreoos::runtime
