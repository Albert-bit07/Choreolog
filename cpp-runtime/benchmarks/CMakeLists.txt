# Step 31 benchmark harnesses (Milestone 6).
#
# Each bench_*.cpp is a standalone Google Benchmark binary so suites can run
# independently and results stay comparable across runs.

find_package(benchmark CONFIG REQUIRED)

function(choreoos_add_benchmark name source)
  add_executable(${name} ${source})
  target_link_libraries(${name} PRIVATE ChoreoOS::Core benchmark::benchmark)
  choreoos_set_project_warnings(${name})
  choreoos_enable_sanitizers(${name})
  target_include_directories(${name} PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
endfunction()

choreoos_add_benchmark(choreoos-bench-apply bench_apply.cpp)
choreoos_add_benchmark(choreoos-bench-codec bench_codec.cpp)
choreoos_add_benchmark(choreoos-bench-wal bench_wal.cpp)
choreoos_add_benchmark(choreoos-bench-replay bench_replay.cpp)
choreoos_add_benchmark(choreoos-bench-snapshot bench_snapshot.cpp)
choreoos_add_benchmark(choreoos-bench-cluster bench_cluster.cpp)
choreoos_add_benchmark(choreoos-bench-recovery bench_recovery.cpp)
