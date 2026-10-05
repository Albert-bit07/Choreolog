// Step 31: serialization and hashing.
//
//   BM_AppendEntriesCodec - encode/decode of a replication batch carrying K
//                           events, the hot path of leader-to-follower traffic.
//   BM_ClientCommandCodec - encode/decode of a single client command.
//   BM_CanonicalState     - canonical_state() over a state with N dancers.
//   BM_StateHash          - state_hash() over a state with N dancers.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <string>
#include <vector>

#include "bench_helpers.hpp"
#include "choreoos/protocol/codec.hpp"
#include "choreoos/protocol/command_codec.hpp"
#include "choreoos/protocol/messages.hpp"
#include "choreoos/state/machine.hpp"

namespace choreoos::bench {
namespace {

using choreoos::protocol::AppendEntries;
using choreoos::protocol::ClientCommand;
using choreoos::state::canonical_state;
using choreoos::state::Engine;
using choreoos::state::Event;
using choreoos::state::state_hash;

std::vector<Event> build_events(std::uint64_t adds) {
  Engine scratch;
  for (const auto& command : make_workload(adds)) {
    auto result = scratch.submit(command);
    if (!result) {
      break;
    }
  }
  return scratch.events();
}

void BM_AppendEntriesCodec(benchmark::State& state) {
  const auto adds = static_cast<std::uint64_t>(state.range(0));
  const std::vector<Event> events = build_events(adds);
  AppendEntries message;
  message.term = 7;
  message.leader_id = "node-1";
  message.prev_log_index = 0;
  message.prev_log_term = 0;
  message.entries = events;
  message.leader_commit = events.size();
  const auto bytes = static_cast<std::int64_t>(events.size());
  for (auto _ : state) {
    auto encoded = choreoos::protocol::encode(message);
    benchmark::DoNotOptimize(encoded);
    if (encoded) {
      auto decoded = choreoos::protocol::decode_append_entries(encoded.value());
      benchmark::DoNotOptimize(decoded);
    }
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(bytes * state.iterations());
  state.SetBytesProcessed(static_cast<std::int64_t>(sizeof(Event)) * bytes * state.iterations());
}
BENCHMARK(BM_AppendEntriesCodec)->Arg(10)->Arg(100)->Arg(1000);

void BM_ClientCommandCodec(benchmark::State& state) {
  const Command command = make_add_dancer(0);
  const ClientCommand message{choreoos::protocol::canonical_command(command)};
  for (auto _ : state) {
    auto encoded = choreoos::protocol::encode(message);
    benchmark::DoNotOptimize(encoded);
    if (encoded) {
      auto decoded = choreoos::protocol::decode_client_command(encoded.value());
      benchmark::DoNotOptimize(decoded);
    }
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ClientCommandCodec);

void BM_CanonicalState(benchmark::State& state) {
  const auto adds = static_cast<std::uint64_t>(state.range(0));
  Engine scratch;
  for (const auto& command : make_workload(adds)) {
    auto result = scratch.submit(command);
    if (!result) {
      state.SkipWithError("workload command rejected during setup");
      return;
    }
  }
  const auto& built = scratch.state();
  for (auto _ : state) {
    std::string text = canonical_state(built);
    benchmark::DoNotOptimize(text);
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_CanonicalState)->Arg(100)->Arg(1000)->Arg(10000);

void BM_StateHash(benchmark::State& state) {
  const auto adds = static_cast<std::uint64_t>(state.range(0));
  Engine scratch;
  for (const auto& command : make_workload(adds)) {
    auto result = scratch.submit(command);
    if (!result) {
      state.SkipWithError("workload command rejected during setup");
      return;
    }
  }
  const auto& built = scratch.state();
  for (auto _ : state) {
    std::string hash = state_hash(built);
    benchmark::DoNotOptimize(hash);
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_StateHash)->Arg(100)->Arg(1000)->Arg(10000);

}  // namespace
}  // namespace choreoos::bench

BENCHMARK_MAIN();
