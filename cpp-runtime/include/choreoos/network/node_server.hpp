#pragma once

// One asynchronous TCP endpoint. A single io_context thread owns the replica,
// so network callbacks do not mutate replication state concurrently.

#include <cstdint>
#include <memory>

#include "choreoos/runtime/config.hpp"

namespace choreoos::network {

struct TransportMetrics {
  std::uint64_t frames_sent = 0;
  std::uint64_t frames_received = 0;
  std::uint64_t malformed = 0;
  std::uint64_t dropped = 0;
  std::uint64_t reconnects = 0;
};

class NodeServer {
 public:
  static choreoos::state::Result<std::unique_ptr<NodeServer>> open(
      const choreoos::runtime::NodeConfig& config);
  ~NodeServer();

  void run();
  void stop();
  [[nodiscard]] std::uint16_t port() const;
  [[nodiscard]] TransportMetrics metrics() const;

 private:
  struct Impl;
  explicit NodeServer(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace choreoos::network
