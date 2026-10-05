// Step 31: full replay.
//
// Builds a committed event vector of N events with a scratch engine, then
// measures replay() rebuilding authoritative state from scratch.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <vector>

#include "bench_helpers.hpp"
#include "choreoos/state/machine.hpp"

namespace choreoos::bench {
namespace {

using choreoos::state::Engine;
using choreoos::state::Event;
using choreoos::state::replay;

void BM_FullReplay(benchmark::State& state) {
  const auto adds = static_cast<std::uint64_t>(state.range(0));
  Engine scratch;
  for (const auto& command : make_workload(adds)) {
    auto result = scratch.submit(command);
    if (!result) {
      state.SkipWithError("workload command rejected during setup");
      return;
    }
  }
  const std::vector<Event> events = scratch.events();
  const auto total = static_cast<std::int64_t>(events.size());
  for (auto _ : state) {
    auto rebuilt = replay(events);
    benchmark::DoNotOptimize(rebuilt);
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(total * state.iterations());
}
BENCHMARK(BM_FullReplay)->Arg(100)->Arg(1000)->Arg(10000);

}  // namespace
}  // namespace choreoos::bench

BENCHMARK_MAIN();
