# GoogleTest binaries for value types and the state machine.

find_package(GTest CONFIG REQUIRED)

add_executable(
  choreoos-unit-tests
  unit/version_test.cpp
  unit/types_test.cpp
  unit/machine_test.cpp
  unit/durability_test.cpp
  unit/protocol_test.cpp
  unit/replication_test.cpp
  unit/consensus_hardening_test.cpp
  unit/raft_transition_test.cpp
  unit/raft_read_barrier_test.cpp
  unit/raft_snapshot_offset_test.cpp
  unit/raft_local_compaction_test.cpp
  unit/runtime_host_test.cpp
  unit/raft_snapshot_install_test.cpp
  unit/core_replica_test.cpp
  unit/raft_domain_bridge_test.cpp
  unit/raft_recovery_projection_test.cpp
  unit/file_engine_adapter_test.cpp
  unit/raft_durable_test.cpp
  unit/raft_trace_test.cpp
  unit/raft_trace_v2_test.cpp
  unit/catchup_test.cpp
  unit/network_capacity_test.cpp
  unit/config_test.cpp
  unit/crash_ordering_test.cpp
  unit/snapshot_transaction_test.cpp
  unit/snapshot_compaction_test.cpp
  unit/faults_test.cpp
  unit/workload.cpp
  unit/property_test.cpp
  integration/cluster_test.cpp
  integration/transition_cluster_test.cpp
  integration/peer_tls_test.cpp
)
target_link_libraries(
  choreoos-unit-tests
  PRIVATE
    ChoreoOS::Core
    GTest::gtest_main
)
choreoos_set_project_warnings(choreoos-unit-tests)
choreoos_enable_sanitizers(choreoos-unit-tests)
target_compile_definitions(
  choreoos-unit-tests
  PRIVATE
    CHOREOOS_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/fixtures"
)

include(GoogleTest)
gtest_discover_tests(choreoos-unit-tests)

# PR #11: compile the real transition core into a deterministic event producer.
# The checker is independent Python code, deliberately run through CTest on
# both Windows and Linux (and in sanitizer builds).
find_package(Python3 COMPONENTS Interpreter REQUIRED)

add_executable(choreoos-raft-conformance integration/raft_conformance_scenarios.cpp)
target_link_libraries(choreoos-raft-conformance PRIVATE ChoreoOS::Core)
choreoos_set_project_warnings(choreoos-raft-conformance)
choreoos_enable_sanitizers(choreoos-raft-conformance)

add_test(
  NAME raft-transition-trace-contract
  COMMAND "${Python3_EXECUTABLE}"
          "${PROJECT_SOURCE_DIR}/scripts/check_raft_conformance.py"
          --runner "$<TARGET_FILE:choreoos-raft-conformance>"
          --schema "${PROJECT_SOURCE_DIR}/formal/raft/trace-schema.v1.json"
          --self-test
)
set_tests_properties(raft-transition-trace-contract PROPERTIES TIMEOUT 90)

# Actual process termination exercises OS lease release and durable retry
# recovery through the shipped executables, on both Windows and Linux.
add_test(
  NAME transition-runtime-process-recovery
  COMMAND "${Python3_EXECUTABLE}"
          "${PROJECT_SOURCE_DIR}/scripts/check_runtime_recovery.py"
          --node "$<TARGET_FILE:choreoos-node>"
          --cli "$<TARGET_FILE:choreoos-cli>"
          --replay "$<TARGET_FILE:choreoos-replay>"
)
set_tests_properties(transition-runtime-process-recovery PROPERTIES TIMEOUT 120)

# Snapshot-aware diagnostics supplement v1; neither contract claims TLC replay.
add_test(
  NAME raft-snapshot-trace-contract
  COMMAND "${Python3_EXECUTABLE}"
          "${PROJECT_SOURCE_DIR}/scripts/check_raft_conformance.py"
          --runner "$<TARGET_FILE:choreoos-raft-conformance>"
          --schema "${PROJECT_SOURCE_DIR}/formal/raft/trace-schema.v2.json"
          --snapshot-contract --self-test
)
set_tests_properties(raft-snapshot-trace-contract PROPERTIES TIMEOUT 90)
