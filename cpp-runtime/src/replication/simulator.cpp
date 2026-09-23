#include "choreoos/replication/simulator.hpp"

#include <sstream>
#include <utility>

namespace choreoos::replication {

SimulatedNetwork::SimulatedNetwork(std::uint64_t seed) : seed_(seed), rng_(seed == 0 ? 1 : seed) {}

void SimulatedNetwork::set_drop_percent(int percent) { drop_percent_ = percent; }

void SimulatedNetwork::set_duplicate_percent(int percent) { duplicate_percent_ = percent; }

void SimulatedNetwork::set_reorder_ticks(int ticks) { reorder_ticks_ = ticks; }

void SimulatedNetwork::isolate(const std::string& id) { isolated_.push_back(id); }

void SimulatedNetwork::heal() { isolated_.clear(); }

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
  if (isolated(from) || isolated(to)) {
    schedule_ += std::to_string(now_) + " drop " + from + " " + to + " isolated\n";
    return;
  }
  if (drop_percent_ > 0 && static_cast<int>(roll() % 100) < drop_percent_) {
    schedule_ += std::to_string(now_) + " drop " + from + " " + to + " random\n";
    return;
  }
  const int copies =
      (duplicate_percent_ > 0 && static_cast<int>(roll() % 100) < duplicate_percent_) ? 2 : 1;
  for (int copy = 0; copy < copies; ++copy) {
    std::uint64_t extra = 0;
    if (reorder_ticks_ > 0) {
      extra = roll() % static_cast<std::uint64_t>(reorder_ticks_ + 1);
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

}  // namespace choreoos::replication
