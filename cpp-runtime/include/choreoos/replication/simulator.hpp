#pragma once

// In-process network for replication tests. Logical time is an integer.
// Delay, drop, duplicate, reorder, and partition are seeded so a failing
// schedule can be saved and repeated.

#include <cstdint>
#include <string>
#include <vector>

#include "choreoos/protocol/frame.hpp"
#include "choreoos/replication/replica.hpp"

namespace choreoos::replication {

class SimulatedNetwork {
 public:
  explicit SimulatedNetwork(std::uint64_t seed);

  void set_drop_percent(int percent);
  void set_duplicate_percent(int percent);
  void set_reorder_ticks(int ticks);
  void isolate(const std::string& id);
  void heal();
  void attach(Replica& replica);

  // Called by a replica sender. `from` is that replica's id.
  void send(const std::string& from, const std::string& to, const choreoos::protocol::Frame& frame);

  // Move logical time forward one tick and deliver every message that is due.
  std::size_t advance();
  [[nodiscard]] std::uint64_t now() const noexcept { return now_; }
  [[nodiscard]] const std::string& schedule() const noexcept { return schedule_; }

 private:
  struct Packet {
    std::string from;
    std::string to;
    choreoos::protocol::Frame frame;
    std::uint64_t deliver_at = 0;
  };

  [[nodiscard]] Replica* find(const std::string& id);
  [[nodiscard]] bool isolated(const std::string& id) const;

  std::uint64_t seed_;
  std::uint64_t rng_;
  int drop_percent_ = 0;
  int duplicate_percent_ = 0;
  int reorder_ticks_ = 0;
  std::uint64_t now_ = 0;
  std::vector<std::string> isolated_;
  std::vector<Replica*> replicas_;
  std::vector<Packet> packets_;
  std::string schedule_;
};

}  // namespace choreoos::replication
