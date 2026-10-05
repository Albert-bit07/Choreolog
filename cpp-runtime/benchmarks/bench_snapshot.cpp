// Step 31: snapshot creation and restore.
//
//   BM_SnapshotCreate  - FileEngine::checkpoint() over N committed events.
//   BM_SnapshotRestore - FileEngine::open() recovering from a snapshot plus
//                        a WAL tail, the node-restart path.
// Setup (filling the log) runs under PauseTiming so the timed region
// measures only the snapshot operation itself.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <filesystem>
#include <optional>
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

// Opens an engine and commits `adds` events. Returns nullopt on failure.
std::optional<FileEngine> fill_engine(const fs::path& dir, std::uint64_t adds) {
  auto opened = FileEngine::open(dir, StoreOptions{});
  if (!opened) {
    return std::nullopt;
  }
  FileEngine engine = std::move(opened.value());
  for (const auto& command : make_workload(adds)) {
    if (!engine.submit(command)) {
      return std::nullopt;
    }
  }
  return std::make_optional(std::move(engine));
}

void BM_SnapshotCreate(benchmark::State& state) {
  const auto adds = static_cast<std::uint64_t>(state.range(0));
  for (auto _ : state) {
    state.PauseTiming();
    const fs::path dir = fresh_dir("choreoos-bench-snap");
    auto engine = fill_engine(dir, adds);
    if (!engine) {
      state.SkipWithError("setup failed");
      return;
    }
    state.ResumeTiming();
    auto result = engine->checkpoint();
    benchmark::DoNotOptimize(result);
    benchmark::ClobberMemory();
    state.PauseTiming();
    fs::remove_all(dir);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(adds) * state.iterations());
}
BENCHMARK(BM_SnapshotCreate)->Arg(500)->Arg(2000);

void BM_SnapshotRestore(benchmark::State& state) {
  const auto adds = static_cast<std::uint64_t>(state.range(0));
  for (auto _ : state) {
    state.PauseTiming();
    const fs::path dir = fresh_dir("choreoos-bench-restore");
    {
      auto engine = fill_engine(dir, adds);
      if (!engine) {
        state.SkipWithError("setup failed");
        return;
      }
      if (!engine->checkpoint()) {
        state.SkipWithError("checkpoint failed");
        return;
      }
      // A few post-snapshot appends so open() also replays a WAL tail.
      for (std::uint64_t i = 0; i < 10; ++i) {
        if (!engine->submit(make_add_dancer(adds + i))) {
          state.SkipWithError("tail append failed");
          return;
        }
      }
    }
    state.ResumeTiming();
    auto reopened = FileEngine::open(dir, StoreOptions{});
    benchmark::DoNotOptimize(reopened);
    benchmark::ClobberMemory();
    state.PauseTiming();
    fs::remove_all(dir);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(adds) * state.iterations());
}
BENCHMARK(BM_SnapshotRestore)->Arg(500)->Arg(2000);

}  // namespace
}  // namespace choreoos::bench

BENCHMARK_MAIN();
