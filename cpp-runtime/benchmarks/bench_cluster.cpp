// Step 31: cluster throughput and commit latency.
//
// All networking runs through SimulatedNetwork, so these measure the
// replication protocol (not TCP): enqueue on the leader, tick every replica,
// advance logical time until the entry commits.
//
//   BM_OneNodeThroughput     - fixed-leader single replica, commands/second.
//   BM_ThreeNodeThroughput   - three electing replicas, commands/second.
//   BM_CommandToCommitLatency - ticks from enqueue to commit on the leader.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "bench_helpers.hpp"
#include "choreoos/replication/replica.hpp"
#include "choreoos/replication/simulator.hpp"

namespace choreoos::bench {
namespace {

namespace fs = std::filesystem;
using choreoos::replication::Replica;
using choreoos::replication::ReplicaConfig;
using choreoos::replication::SimulatedNetwork;

fs::path fresh_dir(const std::string& name) {
  auto dir = fs::temp_directory_path() / name;
  fs::remove_all(dir);
  return dir;
}

ReplicaConfig fixed_leader(const std::string& id, const fs::path& dir) {
  ReplicaConfig config;
  config.id = id;
  config.leader_id = id;
  config.directory = dir;
  config.elections = false;
  return config;
}

ReplicaConfig electing(const std::string& id, const fs::path& dir, std::vector<std::string> peers,
                       std::uint64_t seed) {
  ReplicaConfig config;
  config.id = id;
  config.peers = std::move(peers);
  config.directory = dir;
  config.elections = true;
  config.rng_seed = seed;
  config.election_timeout_min = 3;
  config.election_timeout_max = 6;
  return config;
}

void tick_all(SimulatedNetwork& net, Replica& a, Replica& b, Replica& c) {
  a.tick();
  b.tick();
  c.tick();
  net.advance();
}

Replica* current_leader(Replica& a, Replica& b, Replica& c) {
  for (Replica* node : {&a, &b, &c}) {
    if (node->is_leader()) {
      return node;
    }
  }
  return nullptr;
}

// Runs ticks until `is_done()` or `max_ticks` elapses. Returns ticks used.
template <typename Done>
int pump_until(SimulatedNetwork& net, Replica& a, Replica& b, Replica& c, Done is_done,
               int max_ticks = 500) {
  int ticks = 0;
  while (!is_done() && ticks < max_ticks) {
    tick_all(net, a, b, c);
    ++ticks;
  }
  return ticks;
}

template <typename Done>
int pump_until_one(SimulatedNetwork& net, Replica& node, Done is_done, int max_ticks = 500) {
  int ticks = 0;
  while (!is_done() && ticks < max_ticks) {
    node.tick();
    net.advance();
    ++ticks;
  }
  return ticks;
}

void BM_OneNodeThroughput(benchmark::State& state) {
  const auto commands_per_iter = static_cast<std::uint64_t>(state.range(0));
  const fs::path dir = fresh_dir("choreoos-bench-1node");
  auto opened = Replica::open(fixed_leader("node-1", dir));
  if (!opened) {
    state.SkipWithError("could not open replica");
    return;
  }
  Replica& node = opened.value();
  SimulatedNetwork net{1};
  net.attach(node);
  // Prime with the create command.
  auto primed = node.enqueue(make_create(0));
  if (!primed) {
    state.SkipWithError("create rejected");
    return;
  }
  node.tick();
  net.advance();
  std::uint64_t seq = 0;
  const auto per_iter = static_cast<std::int64_t>(commands_per_iter);
  for (auto _ : state) {
    for (std::uint64_t i = 0; i < commands_per_iter; ++i) {
      auto enqueued = node.enqueue(make_add_dancer(seq++));
      if (!enqueued) {
        state.SkipWithError("enqueue failed");
        return;
      }
      const auto want = node.store().commit_index();
      pump_until_one(net, node, [&] { return node.store().commit_index().value() > want.value(); });
    }
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(per_iter * state.iterations());
  fs::remove_all(dir);
}
BENCHMARK(BM_OneNodeThroughput)->Arg(10)->Arg(50);

void BM_ThreeNodeThroughput(benchmark::State& state) {
  const auto commands_per_iter = static_cast<std::uint64_t>(state.range(0));
  auto dir_a = fresh_dir("choreoos-bench-3a");
  auto dir_b = fresh_dir("choreoos-bench-3b");
  auto dir_c = fresh_dir("choreoos-bench-3c");
  auto ra = Replica::open(electing("node-1", dir_a, {"node-2", "node-3"}, 11));
  auto rb = Replica::open(electing("node-2", dir_b, {"node-1", "node-3"}, 22));
  auto rc = Replica::open(electing("node-3", dir_c, {"node-1", "node-2"}, 33));
  if (!ra || !rb || !rc) {
    state.SkipWithError("could not open replicas");
    return;
  }
  SimulatedNetwork net{9};
  net.attach(ra.value());
  net.attach(rb.value());
  net.attach(rc.value());
  // Elect a leader before timing.
  pump_until(
      net, ra.value(), rb.value(), rc.value(),
      [&] { return current_leader(ra.value(), rb.value(), rc.value()) != nullptr; }, 200);
  Replica* leader = current_leader(ra.value(), rb.value(), rc.value());
  if (leader == nullptr) {
    state.SkipWithError("no leader elected");
    return;
  }
  auto primed = leader->enqueue(make_create(0));
  if (!primed) {
    state.SkipWithError("create rejected");
    return;
  }
  std::uint64_t seq = 0;
  const auto per_iter = static_cast<std::int64_t>(commands_per_iter);
  for (auto _ : state) {
    leader = current_leader(ra.value(), rb.value(), rc.value());
    if (leader == nullptr) {
      state.SkipWithError("lost leader");
      return;
    }
    for (std::uint64_t i = 0; i < commands_per_iter; ++i) {
      auto enqueued = leader->enqueue(make_add_dancer(seq++));
      if (!enqueued) {
        state.SkipWithError("enqueue failed");
        return;
      }
      const auto want = leader->store().commit_index();
      pump_until(net, ra.value(), rb.value(), rc.value(),
                 [&] { return leader->store().commit_index().value() > want.value(); });
    }
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(per_iter * state.iterations());
  fs::remove_all(dir_a);
  fs::remove_all(dir_b);
  fs::remove_all(dir_c);
}
BENCHMARK(BM_ThreeNodeThroughput)->Arg(10)->Arg(50);

void BM_CommandToCommitLatency(benchmark::State& state) {
  auto dir_a = fresh_dir("choreoos-bench-lat-a");
  auto dir_b = fresh_dir("choreoos-bench-lat-b");
  auto dir_c = fresh_dir("choreoos-bench-lat-c");
  auto ra = Replica::open(electing("node-1", dir_a, {"node-2", "node-3"}, 11));
  auto rb = Replica::open(electing("node-2", dir_b, {"node-1", "node-3"}, 22));
  auto rc = Replica::open(electing("node-3", dir_c, {"node-1", "node-2"}, 33));
  if (!ra || !rb || !rc) {
    state.SkipWithError("could not open replicas");
    return;
  }
  SimulatedNetwork net{9};
  net.attach(ra.value());
  net.attach(rb.value());
  net.attach(rc.value());
  pump_until(
      net, ra.value(), rb.value(), rc.value(),
      [&] { return current_leader(ra.value(), rb.value(), rc.value()) != nullptr; }, 200);
  Replica* leader = current_leader(ra.value(), rb.value(), rc.value());
  if (leader == nullptr) {
    state.SkipWithError("no leader elected");
    return;
  }
  if (!leader->enqueue(make_create(0))) {
    state.SkipWithError("create rejected");
    return;
  }
  std::uint64_t seq = 0;
  for (auto _ : state) {
    state.PauseTiming();
    leader = current_leader(ra.value(), rb.value(), rc.value());
    if (leader == nullptr) {
      state.SkipWithError("lost leader");
      return;
    }
    auto enqueued = leader->enqueue(make_add_dancer(seq++));
    if (!enqueued) {
      state.SkipWithError("enqueue failed");
      return;
    }
    const auto want = leader->store().commit_index();
    state.ResumeTiming();
    const int ticks = pump_until(net, ra.value(), rb.value(), rc.value(), [&] {
      return leader->store().commit_index().value() > want.value();
    });
    state.PauseTiming();
    // Latency is logical ticks; report it as a custom counter.
    state.counters["ticks_to_commit"] =
        benchmark::Counter(static_cast<double>(ticks), benchmark::Counter::kAvgThreads);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations());
  fs::remove_all(dir_a);
  fs::remove_all(dir_b);
  fs::remove_all(dir_c);
}
BENCHMARK(BM_CommandToCommitLatency);

}  // namespace
}  // namespace choreoos::bench

BENCHMARK_MAIN();
