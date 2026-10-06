// Step 31: catch-up and failover recovery.
//
//   BM_FailoverRecovery - kill the leader mid-stream; measure logical ticks
//                         until a new leader is elected and commits resume.
//   BM_FollowerCatchUp   - isolate a follower, commit M commands on the
//                         majority, heal; measure ticks until its state hash
//                         converges with the leader's.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "bench_helpers.hpp"
#include "choreoos/replication/replica.hpp"
#include "choreoos/replication/simulator.hpp"
#include "choreoos/state/machine.hpp"

namespace choreoos::bench {
namespace {

namespace fs = std::filesystem;
using choreoos::replication::Replica;
using choreoos::replication::ReplicaConfig;
using choreoos::replication::SimulatedNetwork;
using choreoos::state::state_hash;

fs::path fresh_dir(const std::string& name) {
  auto dir = fs::temp_directory_path() / name;
  fs::remove_all(dir);
  return dir;
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

struct Cluster {
  SimulatedNetwork net{9};
  std::optional<Replica> a;
  std::optional<Replica> b;
  std::optional<Replica> c;
  fs::path dir_a;
  fs::path dir_b;
  fs::path dir_c;

  // Returns a heap-allocated cluster. The cluster is never moved after the
  // replicas are attached to the network, because SimulatedNetwork stores
  // raw Replica* pointers that a move would dangle.
  static std::unique_ptr<Cluster> open() {
    auto cluster = std::make_unique<Cluster>();
    cluster->dir_a = fresh_dir("choreoos-bench-rec-a");
    cluster->dir_b = fresh_dir("choreoos-bench-rec-b");
    cluster->dir_c = fresh_dir("choreoos-bench-rec-c");
    auto ra = Replica::open(electing("node-1", cluster->dir_a, {"node-2", "node-3"}, 11));
    auto rb = Replica::open(electing("node-2", cluster->dir_b, {"node-1", "node-3"}, 22));
    auto rc = Replica::open(electing("node-3", cluster->dir_c, {"node-1", "node-2"}, 33));
    if (!ra || !rb || !rc) {
      return nullptr;
    }
    cluster->a = std::move(ra.value());
    cluster->b = std::move(rb.value());
    cluster->c = std::move(rc.value());
    cluster->net.attach(*cluster->a);
    cluster->net.attach(*cluster->b);
    cluster->net.attach(*cluster->c);
    return cluster;
  }

  void tick_all() {
    a->tick();
    b->tick();
    c->tick();
    net.advance();
  }

  // Tick every live node; the terminated leader is a dead process.
  void tick_except(const std::string& dead_id) {
    if (a->id() != dead_id) {
      a->tick();
    }
    if (b->id() != dead_id) {
      b->tick();
    }
    if (c->id() != dead_id) {
      c->tick();
    }
    net.advance();
  }

  Replica* leader() {
    for (Replica* node : {&*a, &*b, &*c}) {
      if (node->is_leader()) {
        return node;
      }
    }
    return nullptr;
  }

  void cleanup() {
    fs::remove_all(dir_a);
    fs::remove_all(dir_b);
    fs::remove_all(dir_c);
  }
};

void BM_FailoverRecovery(benchmark::State& state) {
  for (auto _ : state) {
    state.PauseTiming();
    auto cluster = Cluster::open();
    if (!cluster) {
      state.SkipWithError("could not open cluster");
      return;
    }
    // Elect and commit a few commands so there is real state.
    int warmup = 0;
    while (cluster->leader() == nullptr && warmup < 200) {
      cluster->tick_all();
      ++warmup;
    }
    Replica* first = cluster->leader();
    if (first == nullptr) {
      state.SkipWithError("no leader elected");
      return;
    }
    if (!first->enqueue(make_create(0))) {
      state.SkipWithError("create rejected");
      return;
    }
    for (std::uint64_t i = 0; i < 5; ++i) {
      Replica* lead = cluster->leader();
      if (lead == nullptr || !lead->enqueue(make_add_dancer(i))) {
        state.SkipWithError("warmup enqueue failed");
        return;
      }
      const auto want = lead->store().commit_index();
      int waited = 0;
      while (lead->store().commit_index().value() <= want.value() && waited < 200) {
        cluster->tick_all();
        ++waited;
      }
    }
    // Kill the leader: drop it from the network and stop ticking it.
    const std::string dead_id = cluster->leader()->id();
    cluster->net.terminate(dead_id);
    state.ResumeTiming();
    // Measure ticks until a new leader commits again.
    int ticks = 0;
    bool recovered = false;
    while (ticks < 500 && !recovered) {
      cluster->tick_except(dead_id);
      ++ticks;
      Replica* lead = cluster->leader();
      if (lead != nullptr && lead->id() != dead_id) {
        // Move dancer bench-add-0 from x=1000 to x=4500: free lane, 3500mm
        // under the 5000mm travel limit. One commit proves the new leader
        // is functional.
        auto enqueued = lead->enqueue(make_move_dancer(99991, 0));
        if (enqueued) {
          const auto want = lead->store().commit_index();
          int waited = 0;
          while (lead->store().commit_index().value() <= want.value() && waited < 200) {
            cluster->tick_except(dead_id);
            ++waited;
            ++ticks;
          }
          recovered = lead->store().commit_index().value() > want.value();
        }
      }
    }
    state.PauseTiming();
    if (!recovered) {
      state.SkipWithError("cluster did not recover");
      return;
    }
    state.counters["ticks_to_recover"] =
        benchmark::Counter(static_cast<double>(ticks), benchmark::Counter::kAvgThreads);
    state.ResumeTiming();
    cluster->cleanup();
  }
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_FailoverRecovery);

void BM_FollowerCatchUp(benchmark::State& state) {
  const auto lag_commands = static_cast<std::uint64_t>(state.range(0));
  for (auto _ : state) {
    state.PauseTiming();
    auto cluster = Cluster::open();
    if (!cluster) {
      state.SkipWithError("could not open cluster");
      return;
    }
    int warmup = 0;
    while (cluster->leader() == nullptr && warmup < 200) {
      cluster->tick_all();
      ++warmup;
    }
    Replica* lead = cluster->leader();
    if (lead == nullptr || !lead->enqueue(make_create(0))) {
      state.SkipWithError("setup failed");
      return;
    }
    // Isolate node-3, then commit lag_commands on the majority.
    cluster->net.isolate("node-3");
    std::uint64_t seq = 0;
    for (std::uint64_t i = 0; i < lag_commands; ++i) {
      lead = cluster->leader();
      if (lead == nullptr || !lead->enqueue(make_add_dancer(seq++))) {
        state.SkipWithError("lag enqueue failed");
        return;
      }
      const auto want = lead->store().commit_index();
      int waited = 0;
      while (lead->store().commit_index().value() <= want.value() && waited < 200) {
        cluster->tick_all();
        ++waited;
      }
    }
    cluster->net.heal();
    state.ResumeTiming();
    int ticks = 0;
    // After healing, the isolated follower may have a higher term from
    // campaigning alone, so the leadership can change. Wait for all three
    // nodes to converge on the same state hash instead of pinning the
    // pre-heal leader.
    while (ticks < 2000) {
      cluster->tick_all();
      ++ticks;
      const std::string ha = state_hash(cluster->a->store().engine().state());
      const std::string hb = state_hash(cluster->b->store().engine().state());
      const std::string hc = state_hash(cluster->c->store().engine().state());
      if (ha == hb && hb == hc) {
        break;
      }
    }
    state.PauseTiming();
    state.counters["ticks_to_catch_up"] =
        benchmark::Counter(static_cast<double>(ticks), benchmark::Counter::kAvgThreads);
    state.ResumeTiming();
    cluster->cleanup();
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(lag_commands) * state.iterations());
}
BENCHMARK(BM_FollowerCatchUp)->Arg(20)->Arg(100);

}  // namespace
}  // namespace choreoos::bench

BENCHMARK_MAIN();
