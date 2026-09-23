// Replays a store directory from wal.bin plus the newest valid snapshot.
// Prints the recovery report and the deterministic state hash.
// Running this twice on the same directory must print the same hash.

#include <iostream>
#include <string>

#include "choreoos/core/version.hpp"
#include "choreoos/state/store.hpp"

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: choreoos-replay STORE_DIR\n";
    return 2;
  }

  auto store = choreoos::state::FileEngine::open(argv[1]);
  if (!store) {
    std::cerr << store.error().to_string() << '\n';
    return 1;
  }

  const auto& report = store.value().recovery();
  const auto& state = store.value().engine().state();
  std::cout << "ChoreoOS replay " << choreoos::core::version() << '\n';
  std::cout << "events " << store.value().engine().events().size() << '\n';
  std::cout << "snapshot " << (report.used_snapshot ? "yes" : "no") << '\n';
  std::cout << "snapshot_index " << report.snapshot_index << '\n';
  std::cout << "replayed_after_snapshot " << report.replayed_after_snapshot << '\n';
  std::cout << "truncated_tail_bytes " << report.truncated_tail_bytes << '\n';
  std::cout << "commit_index " << report.commit_index << '\n';
  std::cout << "hash " << choreoos::state::state_hash(state) << '\n';
  std::cout << choreoos::state::canonical_state(state);
  return 0;
}
