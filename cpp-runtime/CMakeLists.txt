# Core library: state machine, durable log, and fixed-leader replication.
# Node, CLI, and replay binaries only link this library; they do not own rules.

find_package(boost_asio CONFIG REQUIRED)

add_library(choreoos_core STATIC)
add_library(ChoreoOS::Core ALIAS choreoos_core)

target_sources(
  choreoos_core
  PRIVATE
    src/core/version.cpp
    src/state/error.cpp
    src/state/types.cpp
    src/state/model.cpp
    src/state/machine.cpp
    src/state/store.cpp
    src/storage/crc32.cpp
    src/storage/file_util.cpp
    src/storage/wal.cpp
    src/storage/metadata.cpp
    src/storage/snapshot.cpp
    src/protocol/frame.cpp
    src/protocol/codec.cpp
    src/protocol/command_codec.cpp
    src/protocol/client.cpp
    src/replication/replica.cpp
    src/replication/simulator.cpp
    src/runtime/config.cpp
    src/network/node_server.cpp
)

target_link_libraries(choreoos_core PUBLIC Boost::asio)
if(WIN32)
  # MSVC pulls Winsock in by itself. MinGW, which GitHub's Ninja build uses,
  # needs these on the link line or WSAStartup/setsockopt/AcceptEx stay unresolved.
  target_link_libraries(choreoos_core PUBLIC ws2_32 mswsock)
  target_compile_definitions(choreoos_core PUBLIC _WIN32_WINNT=0x0A00)
endif()

target_include_directories(choreoos_core PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_compile_features(choreoos_core PUBLIC cxx_std_20)
choreoos_set_project_warnings(choreoos_core)
choreoos_enable_sanitizers(choreoos_core)

add_executable(choreoos-node apps/node/main.cpp)
target_link_libraries(choreoos-node PRIVATE ChoreoOS::Core)
choreoos_set_project_warnings(choreoos-node)
choreoos_enable_sanitizers(choreoos-node)

add_executable(choreoos-cli apps/cli/main.cpp)
target_link_libraries(choreoos-cli PRIVATE ChoreoOS::Core)
choreoos_set_project_warnings(choreoos-cli)
choreoos_enable_sanitizers(choreoos-cli)

add_executable(choreoos-replay apps/replay/main.cpp)
target_link_libraries(choreoos-replay PRIVATE ChoreoOS::Core)
choreoos_set_project_warnings(choreoos-replay)
choreoos_enable_sanitizers(choreoos-replay)

if(CHOREOOS_BUILD_TESTS)
  add_subdirectory(tests)
endif()
