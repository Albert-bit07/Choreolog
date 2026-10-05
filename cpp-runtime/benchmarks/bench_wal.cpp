// Step 31: log append and flush.
//
// FileEngine::submit() appends a checksummed record and flushes before
// returning (Sync durability, the default). This harness measures that
// durable per-command cost directly: one submit per iteration against a
// single engine opened once outside the timed region.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <filesystem>
#include <string>

#include "bench_helpers.hpp"
#include "choreoos/state/store.hpp"

namespace choreoos::bench {
namespace {

namespace fs = std::filesystem;
using choreoos::state::FileEngine;
using choreoos::state::StoreOptions;

fs::path fresh_dir(const std::string& name) {
  auto dir = fs::temp_directory_path() / name;
  fs::remove_all(dir);
  return dir;
}

void BM_WalAppendFlush(benchmark::State& state) {
  const fs::path dir = fresh_dir("choreoos-bench-wal");
  auto opened = FileEngine::open(dir, StoreOptions{});
  if (!opened) {
    state.SkipWithError("could not open FileEngine");
    return;
  }
  FileEngine engine = std::move(opened.value());
  // Prime with the create so every timed submit is a valid add-dancer.
  auto primed = engine.submit(make_create(0));
  if (!primed) {
    state.SkipWithError("create command rejected during setup");
    return;
  }
  std::uint64_t seq = 0;
  for (auto _ : state) {
    auto result = engine.submit(make_add_dancer(seq++));
    benchmark::DoNotOptimize(result);
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations());
  fs::remove_all(dir);
}
BENCHMARK(BM_WalAppendFlush);

}  // namespace
}  // namespace choreoos::bench

BENCHMARK_MAIN();
