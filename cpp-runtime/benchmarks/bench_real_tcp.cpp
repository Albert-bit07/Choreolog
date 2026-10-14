// Isolated audit harness: real TCP, 3 independently running ChoreoOS nodes.
// Client latency starts before TCP connect and ends after a committed response.
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

#include "choreoos/protocol/client.hpp"
#include "choreoos/protocol/codec.hpp"
#include "choreoos/protocol/command_codec.hpp"
#include "choreoos/state/model.hpp"

using namespace choreoos::state;
using namespace choreoos::protocol;

Frame frame_for(const Command& command) {
  Frame f;
  f.type = MessageType::ClientCommand;
  f.correlation = 1;
  f.payload = encode(ClientCommand{canonical_command(command)}).value();
  return f;
}
bool commit(std::uint16_t port, const Command& c) {
  auto response = transact("127.0.0.1", port, frame_for(c), std::chrono::milliseconds(4000));
  if (!response) {
    std::cerr << "TRANSPORT FAIL: " << response.error().message() << "\n";
    return false;
  }
  auto result = decode_client_response(response.value().payload);
  if (!result || !result.value().ok || result.value().duplicate) {
    std::cerr << "COMMIT FAIL id=" << c.id.value() << "\n";
    return false;
  }
  return true;
}
Command move_command(int n) {
  const auto choreography = ChoreographyId::parse("opening").value();
  return {CommandId::parse("bench-move-" + std::to_string(n)).value(),
          choreography, CommandType::MoveDancer, kCurrentSchemaVersion,
          MusicalTick::from_count(n + 2).value(),
          DancerPayload{DancerId::parse("alice").value(),
                        Position::from_mm(1000, 1000).value()}};
}
double percentile(const std::vector<double>& values, double p) {
  const double pos = (values.size() - 1) * p;
  const auto lo = static_cast<std::size_t>(std::floor(pos));
  const auto hi = static_cast<std::size_t>(std::ceil(pos));
  return values[lo] + (values[hi] - values[lo]) * (pos - lo);
}
int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "usage: choreoos-bench-real-tcp <port> <samples>\n";
    return 2;
  }
  const auto port = static_cast<std::uint16_t>(std::stoi(argv[1]));
  const int samples = std::stoi(argv[2]);
  const auto choreography = ChoreographyId::parse("opening").value();
  Command create{CommandId::parse("bench-create").value(), choreography,
                 CommandType::CreateChoreography, kCurrentSchemaVersion,
                 MusicalTick::from_count(0).value(),
                 CreateChoreographyPayload{choreography,
                    StageBounds::from_mm(20000,12000).value(), OverlapPolicy::Forbidden,5000}};
  bool ready = false;
  for (int retry = 0; retry < 20; ++retry) {
    if (commit(port, create)) { ready = true; break; }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  if (!ready) return 3;
  Command add{CommandId::parse("bench-add").value(), choreography,
              CommandType::AddDancer, kCurrentSchemaVersion,
              MusicalTick::from_count(1).value(),
              DancerPayload{DancerId::parse("alice").value(),
                            Position::from_mm(1000,1000).value()}};
  if (!commit(port, add)) return 4;
  constexpr int warmup = 20;
  std::vector<double> times;
  times.reserve(static_cast<std::size_t>(samples));
  const auto total_start = std::chrono::steady_clock::now();
  for (int i = 0; i < samples + warmup; ++i) {
    const auto cmd = move_command(i);
    const auto started = std::chrono::steady_clock::now();
    if (!commit(port, cmd)) return 5;
    const auto ended = std::chrono::steady_clock::now();
    if (i >= warmup) {
      const double ms = std::chrono::duration<double, std::milli>(ended - started).count();
      times.push_back(ms);
    }
  }
  const auto total_end = std::chrono::steady_clock::now();
  std::sort(times.begin(), times.end());
  const double mean = std::accumulate(times.begin(), times.end(), 0.0) / times.size();
  const double seconds = std::chrono::duration<double>(total_end - total_start).count();
  std::cout << "{\"samples\":" << samples << ",\"warmup\":" << warmup
            << ",\"p50_ms\":" << percentile(times, 0.5)
            << ",\"p95_ms\":" << percentile(times, 0.95)
            << ",\"p99_ms\":" << percentile(times, 0.99)
            << ",\"mean_ms\":" << mean
            << ",\"min_ms\":" << times.front()
            << ",\"max_ms\":" << times.back()
            << ",\"measured_ops_per_sec\":" << (samples / seconds)
            << "}\n";
}
