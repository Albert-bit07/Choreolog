// One cluster process, with configured fixed-leader or election behavior.
// consensus_engine selects the legacy or transition-core runtime backend.

#include <filesystem>
#include <fstream>
#include <iostream>

#include "choreoos/core/version.hpp"
#include "choreoos/network/node_server.hpp"
#include "choreoos/runtime/config.hpp"

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: choreoos-node CONFIG\n";
    return 2;
  }
  auto config = choreoos::runtime::load_node_config(argv[1]);
  if (!config) {
    std::cerr << config.error().to_string() << '\n';
    return 1;
  }
  auto server = choreoos::network::NodeServer::open(config.value());
  if (!server) {
    std::cerr << server.error().to_string() << '\n';
    return 1;
  }
  const auto engine =
      config.value().consensus_engine == choreoos::runtime::ConsensusEngine::Transition
          ? "transition"
          : "legacy";
  const auto ready = config.value().data / "ready";
  std::filesystem::create_directories(config.value().data);
  {
    std::ofstream mark{ready};
    mark << "choreoos-node " << choreoos::core::version() << " id=" << config.value().id
         << " port=" << server.value()->port() << " consensus_engine=" << engine << '\n';
  }
  std::cout << "ready id=" << config.value().id << " port=" << server.value()->port()
            << " consensus_engine=" << engine << '\n';
  server.value()->run();
  std::filesystem::remove(ready);
  return 0;
}
