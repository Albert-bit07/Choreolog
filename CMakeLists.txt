cmake_minimum_required(VERSION 3.28)

project(
  ChoreoOS
  VERSION 0.1.0
  DESCRIPTION "Distributed runtime for deterministic live-performance systems"
  LANGUAGES CXX
)

option(CHOREOOS_BUILD_TESTS "Build ChoreoOS tests" ON)
option(CHOREOOS_WARNINGS_AS_ERRORS "Treat ChoreoOS compiler warnings as errors" ON)
option(CHOREOOS_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(CompilerWarnings)
include(Sanitizers)

if(CHOREOOS_BUILD_TESTS)
  include(CTest)
  enable_testing()
endif()

add_subdirectory(cpp-runtime)
