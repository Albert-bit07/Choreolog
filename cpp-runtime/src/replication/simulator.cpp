#include "choreoos/replication/simulator.hpp"

#include <algorithm>
#include <sstream>
#include <utility>

namespace choreoos::replication {

SimulatedNetwork::SimulatedNetwork(std::uint64_t seed) : seed_(seed), rng_(seed == 0 ? 1 : seed) {}

void SimulatedNetwork::set_drop_percent(int percent) { drop_percent_ = percent; }

void SimulatedNetwork::set_duplicate_percent(int percent) { duplicate_percent_ = percent; }

void SimulatedNetwork::set_reorder_ticks(int ticks) { reorder_ticks_ = ticks; }

void SimulatedNetwork::isolate(const std::string& id) { isolated_.push_back(id); }

void SimulatedNetwork::heal() {
  isolated_.clear();
  paused_.clear();
  side_.clear();
  cuts_.clear();
  rules_.clear();
  schedule_ += std::to_string(now_) + " heal\n";
}

void SimulatedNetwork::enable_faults() { faults_enabled_ = true; }

void SimulatedNetwork::delay(std::optional<choreoos::protocol::MessageType> type, std::string from,
                             std::string to, int ticks) {
  if (!faults_enabled_ || ticks <= 0) {
    return;
  }
  rules_.push_back(LinkRule{type, std::move(from), std::move(to), ticks, false, false});
}

void SimulatedNetwork::drop_link(std::optional<choreoos::protocol::MessageType> type,
                                 std::string from, std::string to) {
  if (!faults_enabled_) {
    return;
  }
  rules_.push_back(LinkRule{type, std::move(from), std::move(to), 0, true, false});
}

void SimulatedNetwork::duplicate_link(std::optional<choreoos::protocol::MessageType> type,
                                      std::string from, std::string to) {
  if (!faults_enabled_) {
    return;
  }
  rules_.push_back(LinkRule{type, std::move(from), std::move(to), 0, false, true});
}

void SimulatedNetwork::disconnect(const std::string& left, const std::string& right) {
  if (!faults_enabled_) {
    return;
  }
  cuts_.push_back({left, right});
  schedule_ += std::to_string(now_) + " disconnect " + left + " " + right + "\n";
}

void SimulatedNetwork::partition(std::vector<std::string> side) {
  if (!faults_enabled_) {
    return;
  }
  side_ = std::move(side);
  schedule_ += std::to_string(now_) + " partition\n";
}

void SimulatedNetwork::pause(const std::string& id) {
  if (!faults_enabled_ || paused(id)) {
    return;
  }
  paused_.push_back(id);
  schedule_ += std::to_string(now_) + " pause " + id + "\n";
}

void SimulatedNetwork::resume(const std::string& id) {
  if (!faults_enabled_) {
    return;
  }
  paused_.erase(std::remove(paused_.begin(), paused_.end(), id), paused_.end());
  schedule_ += std::to_string(now_) + " resume " + id + "\n";
}

bool SimulatedNetwork::paused(const std::string& id) const {
  return std::find(paused_.begin(), paused_.end(), id) != paused_.end();
}

void SimulatedNetwork::terminate(const std::string& id) {
  if (!faults_enabled_) {
    return;
  }
  replicas_.erase(std::remove_if(replicas_.begin(), replicas_.end(),
                                 [&](const Replica* replica) { return replica->id() == id; }),
                  replicas_.end());
  schedule_ += std::to_string(now_) + " terminate " + id + "\n";
}

void SimulatedNetwork::attach(Replica& replica) {
  replicas_.push_back(&replica);
  const std::string from = replica.id();
  replica.set_sender([this, from](const std::string& to, const choreoos::protocol::Frame& frame) {
    send(from, to, frame);
  });
}

void SimulatedNetwork::send(const std::string& from, const std::string& to,
                            const choreoos::protocol::Frame& frame) {
  const auto roll = [this] {
    rng_ ^= rng_ << 13;
    rng_ ^= rng_ >> 7;
    rng_ ^= rng_ << 17;
    return rng_;
  };
  if (isolated(from) || isolated(to) || blocked(from, to)) {
    schedule_ += std::to_string(now_) + " drop " + from + " " + to + " isolated\n";
    return;
  }
  int extra_delay = 0;
  bool forced_drop = false;
  bool forced_duplicate = false;
  for (const auto& rule : rules_) {
    if (!matches(rule, from, to, frame.type)) {
      continue;
    }
    extra_delay += rule.extra_delay;
    forced_drop = forced_drop || rule.drop;
    forced_duplicate = forced_duplicate || rule.duplicate;
  }
  if (forced_drop || (drop_percent_ > 0 && static_cast<int>(roll() % 100) < drop_percent_)) {
    schedule_ += std::to_string(now_) + " drop " + from + " " + to + " random\n";
    return;
  }
  const int copies = (forced_duplicate || (duplicate_percent_ > 0 &&
                                           static_cast<int>(roll() % 100) < duplicate_percent_))
                         ? 2
                         : 1;
  for (int copy = 0; copy < copies; ++copy) {
    std::uint64_t extra = static_cast<std::uint64_t>(extra_delay);
    if (reorder_ticks_ > 0) {
      extra += roll() % static_cast<std::uint64_t>(reorder_ticks_ + 1);
    }
    packets_.push_back(Packet{from, to, frame, now_ + 1 + extra});
    schedule_ += std::to_string(now_) + " queue " + from + " " + to + " at " +
                 std::to_string(now_ + 1 + extra) + "\n";
  }
}

std::size_t SimulatedNetwork::advance() {
  ++now_;
  std::vector<Packet> due;
  std::vector<Packet> waiting;
  for (auto& packet : packets_) {
    if (packet.deliver_at <= now_) {
      due.push_back(std::move(packet));
    } else {
      waiting.push_back(std::move(packet));
    }
  }
  packets_ = std::move(waiting);
  for (const auto& packet : due) {
    if (paused(packet.to)) {
      Packet held = packet;
      held.deliver_at = now_ + 1;
      packets_.push_back(std::move(held));
      schedule_ += std::to_string(now_) + " hold " + packet.from + " " + packet.to + " paused\n";
      continue;
    }
    schedule_ += std::to_string(now_) + " deliver " + packet.from + " " + packet.to + "\n";
    if (Replica* replica = find(packet.to)) {
      replica->handle(packet.frame);
    }
  }
  return due.size();
}

Replica* SimulatedNetwork::find(const std::string& id) {
  for (Replica* replica : replicas_) {
    if (replica->id() == id) {
      return replica;
    }
  }
  return nullptr;
}

bool SimulatedNetwork::isolated(const std::string& id) const {
  for (const auto& isolated : isolated_) {
    if (isolated == id) {
      return true;
    }
  }
  return false;
}

bool SimulatedNetwork::blocked(const std::string& from, const std::string& to) const {
  for (const auto& cut : cuts_) {
    if ((cut.first == from && cut.second == to) || (cut.first == to && cut.second == from)) {
      return true;
    }
  }
  if (side_.empty()) {
    return false;
  }
  const bool from_inside = std::find(side_.begin(), side_.end(), from) != side_.end();
  const bool to_inside = std::find(side_.begin(), side_.end(), to) != side_.end();
  return from_inside != to_inside;
}

bool SimulatedNetwork::matches(const LinkRule& rule, const std::string& from, const std::string& to,
                               choreoos::protocol::MessageType type) const {
  if (rule.type && *rule.type != type) {
    return false;
  }
  if (!rule.from.empty() && rule.from != from) {
    return false;
  }
  return rule.to.empty() || rule.to == to;
}

}  // namespace choreoos::replication
