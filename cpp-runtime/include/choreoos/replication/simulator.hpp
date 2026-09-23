#pragma once

// In-process network for replication tests. Logical time is an integer.
// Delay, drop, duplicate, reorder, and partition are seeded so a failing
// schedule can be saved and repeated.

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
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

  // The controls below do nothing until this is called. Production nodes never
  // construct a SimulatedNetwork, and these rules stay out of the TCP path.
  void enable_faults();
  [[nodiscard]] bool faults_enabled() const noexcept { return faults_enabled_; }

  // Empty from/to matches every endpoint. type nullopt matches every message.
  void delay(std::optional<choreoos::protocol::MessageType> type, std::string from, std::string to,
             int ticks);
  void drop_link(std::optional<choreoos::protocol::MessageType> type, std::string from,
                 std::string to);
  void duplicate_link(std::optional<choreoos::protocol::MessageType> type, std::string from,
                      std::string to);
  void disconnect(const std::string& left, const std::string& right);
  void partition(std::vector<std::string> side);
  void pause(const std::string& id);
  void resume(const std::string& id);
  [[nodiscard]] bool paused(const std::string& id) const;
  // Remove the node from delivery. The test restarts it by opening the same directory.
  void terminate(const std::string& id);

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

  struct LinkRule {
    std::optional<choreoos::protocol::MessageType> type;
    std::string from;
    std::string to;
    int extra_delay = 0;
    bool drop = false;
    bool duplicate = false;
  };

  [[nodiscard]] Replica* find(const std::string& id);
  [[nodiscard]] bool isolated(const std::string& id) const;
  [[nodiscard]] bool blocked(const std::string& from, const std::string& to) const;
  [[nodiscard]] bool matches(const LinkRule& rule, const std::string& from, const std::string& to,
                             choreoos::protocol::MessageType type) const;

  std::uint64_t seed_;
  std::uint64_t rng_;
  int drop_percent_ = 0;
  int duplicate_percent_ = 0;
  int reorder_ticks_ = 0;
  std::uint64_t now_ = 0;
  bool faults_enabled_ = false;
  std::vector<std::string> isolated_;
  std::vector<std::string> paused_;
  std::vector<std::string> side_;
  std::vector<std::pair<std::string, std::string>> cuts_;
  std::vector<LinkRule> rules_;
  std::vector<Replica*> replicas_;
  std::vector<Packet> packets_;
  std::string schedule_;
};

}  // namespace choreoos::replication
