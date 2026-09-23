// Smoke test that the core library links and reports a version.

#include "choreoos/core/version.hpp"

#include <gtest/gtest.h>

namespace choreoos::core {
namespace {

TEST(VersionTest, ReportsProjectVersion) { EXPECT_EQ(version(), "0.1.0"); }

}  // namespace
}  // namespace choreoos::core
