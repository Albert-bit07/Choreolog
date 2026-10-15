# Core library: state machine, durable log, and fixed-leader replication.
# Node, CLI, and replay binaries only link this library; they do not own rules.

find_package(boost_asio CONFIG REQUIRED)
find_package(OpenSSL 3 REQUIRED)

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
    src/storage/transaction.cpp
    src/protocol/frame.cpp
    src/protocol/codec.cpp
    src/protocol/command_codec.cpp
    src/protocol/client.cpp
    src/raft/transition.cpp
    src/raft/domain_bridge.cpp
    src/raft/file_engine_adapter.cpp
    src/raft/checkpoint.cpp
    src/raft/durable_node.cpp
    src/raft/trace.cpp
    src/replication/replica.cpp
    src/replication/simulator.cpp
    src/runtime/config.cpp
    src/network/node_server.cpp
    src/network/peer_tls.cpp
)

target_link_libraries(choreoos_core PUBLIC Boost::asio OpenSSL::SSL OpenSSL::Crypto)
if(WIN32)
  # MSVC auto-links Winsock. MinGW does not, so keep these on every WIN32
  # link line. CI now compiles with cl, but a local MinGW build still needs them.
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


# The management gateway is a separate loopback-only gRPC process. It uses
# the node's existing framed TCP endpoint and does not own authoritative state.
# Enable with -DCHOREOOS_BUILD_MANAGEMENT_API=ON and the vcpkg feature
# -DVCPKG_MANIFEST_FEATURES=management-grpc.
option(CHOREOOS_BUILD_MANAGEMENT_API "Build the gRPC management gateway" OFF)
if(CHOREOOS_BUILD_MANAGEMENT_API)
  find_package(Protobuf MODULE REQUIRED)
  find_package(gRPC CONFIG REQUIRED)
  set(management_proto "${PROJECT_SOURCE_DIR}/proto/management/choreoos_management.proto")
  set(management_generated "${CMAKE_CURRENT_BINARY_DIR}/generated")
  file(MAKE_DIRECTORY "${management_generated}")
  set(management_sources
      "${management_generated}/choreoos_management.pb.cc"
      "${management_generated}/choreoos_management.grpc.pb.cc")
  set(management_headers
      "${management_generated}/choreoos_management.pb.h"
      "${management_generated}/choreoos_management.grpc.pb.h")
  add_custom_command(
    OUTPUT ${management_sources} ${management_headers}
    COMMAND protobuf::protoc
    ARGS "--proto_path=${PROJECT_SOURCE_DIR}/proto/management"
         "--cpp_out=${management_generated}"
         "--grpc_out=${management_generated}"
         "--plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
         "${management_proto}"
    DEPENDS "${management_proto}"
    VERBATIM
  )
  add_executable(choreoos-management apps/management/main.cpp ${management_sources}
                                     ${management_headers})
  target_include_directories(choreoos-management PRIVATE "${management_generated}")
  target_link_libraries(choreoos-management PRIVATE ChoreoOS::Core gRPC::grpc++
                                                    protobuf::libprotobuf)
  target_compile_features(choreoos-management PRIVATE cxx_std_20)
  choreoos_set_project_warnings(choreoos-management)
endif()

if(CHOREOOS_BUILD_TESTS)
  add_subdirectory(tests)
endif()

option(CHOREOOS_BUILD_BENCHMARKS "Build ChoreoOS benchmark harnesses" ON)
if(CHOREOOS_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()
