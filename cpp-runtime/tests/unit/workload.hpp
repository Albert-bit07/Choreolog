// Seeded command streams for property tests.
// The generator is a pure function of the seed: the same seed always builds
// the same accepts, rejects, and retries. shrink_failing_prefix drops a
// trailing suffix while a scenario still fails, so a counterexample stays small.

#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "choreoos/state/model.hpp"

namespace choreoos::testing {

struct WorkloadStep {
  choreoos::state::Command command;
  bool expect_ok = false;
  bool expect_duplicate = false;
};

struct Workload {
  std::uint64_t seed = 0;
  std::vector<WorkloadStep> steps;
  // seed, step count, and each command id. Printed when a property fails.
  std::string note;
};

// create, a dancer, a retry of that command, a duplicate dancer, an
// out-of-bounds add, a second dancer, a legal move, an illegal move, then
// extra_dancers more legal adds. Positions stay inside a 20000x12000 stage.
[[nodiscard]] Workload generate_workload(std::uint64_t seed, int extra_dancers);

// Returns the shortest suffix-trimmed prefix that still fails. Empty when
// the full sequence does not fail.
[[nodiscard]] std::vector<WorkloadStep> shrink_failing_prefix(
    std::vector<WorkloadStep> steps,
    const std::function<bool(const std::vector<WorkloadStep>&)>& still_fails);

}  // namespace choreoos::testing
