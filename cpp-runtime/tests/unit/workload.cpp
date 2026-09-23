#include "workload.hpp"

#include <sstream>
#include <utility>

namespace choreoos::testing {
namespace {

using choreoos::state::ChoreographyId;
using choreoos::state::Command;
using choreoos::state::CommandId;
using choreoos::state::CommandType;
using choreoos::state::CreateChoreographyPayload;
using choreoos::state::DancerId;
using choreoos::state::DancerPayload;
using choreoos::state::kCurrentSchemaVersion;
using choreoos::state::MusicalTick;
using choreoos::state::OverlapPolicy;
using choreoos::state::Position;
using choreoos::state::StageBounds;

ChoreographyId show_id() { return ChoreographyId::parse("opening").value(); }

Command create_command() {
  return Command{CommandId::parse("c-create").value(),
                 show_id(),
                 CommandType::CreateChoreography,
                 kCurrentSchemaVersion,
                 MusicalTick::from_count(0).value(),
                 CreateChoreographyPayload{show_id(), StageBounds::from_mm(20000, 12000).value(),
                                           OverlapPolicy::Forbidden, 5000}};
}

Command add_command(const std::string& command_id, const std::string& dancer_id, std::int32_t x) {
  return Command{
      CommandId::parse(command_id).value(),
      show_id(),
      CommandType::AddDancer,
      kCurrentSchemaVersion,
      MusicalTick::from_count(1).value(),
      DancerPayload{DancerId::parse(dancer_id).value(), Position::from_mm(x, 1000).value()}};
}

Command move_command(const std::string& command_id, const std::string& dancer_id, std::int32_t x) {
  return Command{
      CommandId::parse(command_id).value(),
      show_id(),
      CommandType::MoveDancer,
      kCurrentSchemaVersion,
      MusicalTick::from_count(2).value(),
      DancerPayload{DancerId::parse(dancer_id).value(), Position::from_mm(x, 1000).value()}};
}

std::string note_for(std::uint64_t seed, const std::vector<WorkloadStep>& steps) {
  std::ostringstream out;
  out << "seed=" << seed << " steps=" << steps.size();
  for (const auto& step : steps) {
    out << " " << step.command.id.value() << (step.expect_ok ? ":ok" : ":reject");
    if (step.expect_duplicate) {
      out << ":retry";
    }
  }
  return out.str();
}

}  // namespace

Workload generate_workload(std::uint64_t seed, int extra_dancers) {
  if (extra_dancers < 0) {
    extra_dancers = 0;
  }
  // Five lanes so a seed only slides Alice, and she never lands on Bob at 8000.
  const std::int32_t alice_x = 1000 + static_cast<std::int32_t>(seed % 5) * 500;
  Workload workload;
  workload.seed = seed;
  workload.steps.push_back(WorkloadStep{create_command(), true, false});
  workload.steps.push_back(WorkloadStep{add_command("c-alice", "alice", alice_x), true, false});
  // Same command id. The store must return the original event, not a second one.
  workload.steps.push_back(WorkloadStep{add_command("c-alice", "alice", alice_x), true, true});
  workload.steps.push_back(
      WorkloadStep{add_command("c-alice-again", "alice", alice_x + 1500), false, false});
  workload.steps.push_back(WorkloadStep{add_command("c-out", "cara", 20001), false, false});
  workload.steps.push_back(WorkloadStep{add_command("c-bob", "bob", 8000), true, false});
  workload.steps.push_back(
      WorkloadStep{move_command("c-move-ok", "alice", alice_x + 1000), true, false});
  // 6000 mm past the mark she just reached. max_travel_mm on create is 5000.
  workload.steps.push_back(
      WorkloadStep{move_command("c-move-far", "alice", alice_x + 7000), false, false});
  for (int i = 0; i < extra_dancers; ++i) {
    const auto name = "d" + std::to_string(i);
    const std::int32_t x = 11000 + i * 1500;
    workload.steps.push_back(WorkloadStep{add_command("c-" + name, name, x), true, false});
  }
  workload.note = note_for(seed, workload.steps);
  return workload;
}

std::vector<WorkloadStep> shrink_failing_prefix(
    std::vector<WorkloadStep> steps,
    const std::function<bool(const std::vector<WorkloadStep>&)>& still_fails) {
  if (!still_fails(steps)) {
    return {};
  }
  while (steps.size() > 1) {
    auto shorter = steps;
    shorter.pop_back();
    if (!still_fails(shorter)) {
      break;
    }
    steps = std::move(shorter);
  }
  return steps;
}

}  // namespace choreoos::testing
