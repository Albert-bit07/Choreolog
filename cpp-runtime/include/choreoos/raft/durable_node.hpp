#pragma once

// Single-threaded effect executor around the experimental pure Raft core.
// Production Replica still owns its separate persistence/network path.
#include <filesystem>
#include <optional>

#include "choreoos/raft/checkpoint.hpp"
#include "choreoos/raft/transition.hpp"
#include "choreoos/state/error.hpp"

namespace choreoos::raft {

struct DriveResult {
  Effects effects;
  std::optional<state::Error> error;
};

class DurableNode {
 public:
  [[nodiscard]] static state::Result<DurableNode> open(
      Config config, std::filesystem::path directory, CheckpointOptions options = {});

  // PersistEffects are executed synchronously. Effects returned to a caller
  // are safe to send only after the required checkpoint has completed.
  [[nodiscard]] DriveResult dispatch(const Input& input);
  [[nodiscard]] const Node& node() const noexcept { return node_; }
  [[nodiscard]] const Durable& disk() const noexcept { return checkpoint_.current(); }

  [[nodiscard]] state::Result<void> inject_next_fault_for_test(CheckpointFault fault) {
    return checkpoint_.inject_next_fault_for_test(fault);
  }

 private:
  DurableNode(Config config, CheckpointStore checkpoint, Node node)
      : config_(std::move(config)), checkpoint_(std::move(checkpoint)), node_(std::move(node)) {}

  Config config_;
  CheckpointStore checkpoint_;
  Node node_;
};

}  // namespace choreoos::raft
