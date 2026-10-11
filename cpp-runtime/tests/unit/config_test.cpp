#include "choreoos/runtime/config.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

namespace choreoos::runtime {
namespace {

choreoos::state::Result<NodeConfig> load(const std::string& body) {
  const auto path = std::filesystem::temp_directory_path() / "choreoos-config-test.yaml";
  {
    std::ofstream out{path, std::ios::trunc};
    out << "id: node-1\nleader: node-1\ndata: /tmp/node-1\n" << body;
  }
  return load_node_config(path);
}

TEST(ConfigTest, TimingDefaultsMatchTheOriginalHardcodedValues) {
  auto config = load("listen: 127.0.0.1:7101\n");
  ASSERT_TRUE(config) << config.error().to_string();
  EXPECT_EQ(config.value().heartbeat_ms, 50u);
  EXPECT_EQ(config.value().election_timeout_min, 3u);
  EXPECT_EQ(config.value().election_timeout_max, 6u);
}

TEST(ConfigTest, TimingKeysAreRead) {
  auto config = load("heartbeat_ms: 200\nelection_timeout_min: 8\nelection_timeout_max: 16\n");
  ASSERT_TRUE(config) << config.error().to_string();
  EXPECT_EQ(config.value().heartbeat_ms, 200u);
  EXPECT_EQ(config.value().election_timeout_min, 8u);
  EXPECT_EQ(config.value().election_timeout_max, 16u);
}

TEST(ConfigTest, BadNumbersAreErrorsNotCrashes) {
  // These used to reach std::stoi / std::stoull, which throw and would
  // terminate the node on a typo.
  EXPECT_FALSE(load("listen: 127.0.0.1:abc\n"));
  EXPECT_FALSE(load("listen: 127.0.0.1:70000\n"));
  EXPECT_FALSE(load("rng_seed: lots\n"));
  EXPECT_FALSE(load("heartbeat_ms: fast\n"));
  EXPECT_FALSE(load("heartbeat_ms: 5\n"));          // below the 10 ms floor
  EXPECT_FALSE(load("heartbeat_ms: 12x\n"));        // trailing junk
  EXPECT_FALSE(load("election_timeout_min: 1\n"));  // would elect on every tick
}

TEST(ConfigTest, ElectionWindowMustNotBeInverted) {
  EXPECT_FALSE(load("election_timeout_min: 10\nelection_timeout_max: 4\n"));
  EXPECT_TRUE(load("election_timeout_min: 4\nelection_timeout_max: 4\n"));
}

}  // namespace
}  // namespace choreoos::runtime
