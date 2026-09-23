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
  unit/faults_test.cpp
  unit/workload.cpp
  unit/property_test.cpp
  integration/cluster_test.cpp
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
