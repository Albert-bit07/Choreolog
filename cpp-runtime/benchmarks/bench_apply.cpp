// Step 31: single-node apply throughput.
//
// Measures the deterministic state machine with no I/O: propose() validates a
// command and apply() folds the event into state. Two harnesses:
//   BM_SubmitThroughput  - Engine::submit, the full validate+index+apply path.
//   BM_ApplyThroughput   - propose() once up front, then apply() only.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <vector>

#include "bench_helpers.hpp"
#include "choreoos/state/machine.hpp"

namespace choreoos::bench {
namespace {

using choreoos::state::apply;
using choreoos::state::ChoreographyState;
using choreoos::state::Engine;
using choreoos::state::Event;

void BM_SubmitThroughput(benchmark::State& state) {
  const auto adds = static_cast<std::uint64_t>(state.range(0));
  const std::vector<Command> commands = make_workload(adds);
  const auto total = static_cast<std::int64_t>(commands.size());
  for (auto _ : state) {
    Engine engine;
    for (const auto& command : commands) {
      auto result = engine.submit(command);
      benchmark::DoNotOptimize(result);
    }
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(total * state.iterations());
}
BENCHMARK(BM_SubmitThroughput)->Arg(100)->Arg(1000)->Arg(10000);

void BM_ApplyThroughput(benchmark::State& state) {
  const auto adds = static_cast<std::uint64_t>(state.range(0));
  const std::vector<Command> commands = make_workload(adds);
  // Submit through a scratch engine so events carry contiguous log indexes.
  // The timed region then measures apply() only.
  Engine scratch;
  for (const auto& command : commands) {
    auto result = scratch.submit(command);
    if (!result) {
      state.SkipWithError("workload command rejected during setup");
      return;
    }
  }
  const std::vector<Event> events = scratch.events();
  const auto total = static_cast<std::int64_t>(events.size());
  for (auto _ : state) {
    ChoreographyState fresh_state;
    for (const auto& event : events) {
      auto applied = apply(fresh_state, event);
      benchmark::DoNotOptimize(applied);
    }
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(total * state.iterations());
}
BENCHMARK(BM_ApplyThroughput)->Arg(100)->Arg(1000)->Arg(10000);

}  // namespace
}  // namespace choreoos::bench

BENCHMARK_MAIN();
