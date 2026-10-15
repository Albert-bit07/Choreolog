#include <gtest/gtest.h>

#include "choreoos/network/outbound_queue.hpp"
#include "choreoos/network/pending_requests.hpp"

namespace choreoos::network {
namespace {

TEST(NetworkCapacityTest, DuplicateWaitersKeepIndependentDeadlinesAndCommitFanout) {
  PendingRequests<std::string> requests;
  const auto first = requests.add(42, "same-command-early");
  const auto later = requests.add(42, "same-command-later");
  requests.add(43, "other-command");
  EXPECT_EQ(requests.take(first), "same-command-early");
  EXPECT_FALSE(requests.take(first));
  ASSERT_EQ(requests.size(), 2u);
  const auto committed = requests.take_through(42);
  ASSERT_EQ(committed.size(), 1u);
  EXPECT_EQ(committed.front(), "same-command-later");
  EXPECT_FALSE(requests.take(later));
  EXPECT_EQ(requests.size(), 1u);
}

TEST(NetworkCapacityTest, ByteBudgetsReservePeerCapacityAndReleaseOnRetirement) {
  auto budget = std::make_shared<QueueBudget>();
  const auto frame = std::make_shared<const std::string>(1u << 20, 'x');
  std::vector<std::unique_ptr<OutboundQueue>> clients;
  for (int i = 0; i < 12; ++i) {
    auto queue = std::make_unique<OutboundQueue>(budget);
    ASSERT_TRUE(queue->push(frame, false));
    ASSERT_TRUE(queue->push(frame, false));
    EXPECT_FALSE(queue->push(frame, false));
    clients.push_back(std::move(queue));
  }
  OutboundQueue another(budget);
  EXPECT_FALSE(another.push(frame, false));
  EXPECT_TRUE(another.push(frame, true));
  EXPECT_TRUE(another.push(frame, true));
  EXPECT_EQ(budget->client_bytes, QueueBudget::kClientBytes);
  clients.clear();
  EXPECT_EQ(budget->client_bytes, 0u);
  EXPECT_EQ(budget->bytes, 2u << 20);
  another.clear();
  EXPECT_EQ(budget->bytes, 0u);
}

TEST(NetworkCapacityTest, RetiredActiveWritesCannotReuseTheirGlobalReservationsEarly) {
  auto budget = std::make_shared<QueueBudget>();
  auto bytes = std::make_shared<const std::string>(1u << 20, 'x');
  std::vector<OutboundQueue::Buffer> active;
  for (std::size_t i = 0; i < 32; ++i) {
    OutboundQueue retired(budget);
    ASSERT_TRUE(retired.push(bytes, true));
    active.push_back(retired.front());
    retired.clear();
  }
  EXPECT_EQ(budget->bytes, QueueBudget::kGlobalBytes);
  OutboundQueue next(budget);
  EXPECT_FALSE(next.push(bytes, true));
  active.clear();  // Completion releases the final async-write references.
  EXPECT_EQ(budget->bytes, 0u);
  EXPECT_TRUE(next.push(bytes, true));
}

TEST(NetworkCapacityTest, CorrelatedRetriesDoNotGrowQueueAndActiveBuffersOutliveIt) {
  auto budget = std::make_shared<QueueBudget>();
  OutboundQueue queue(budget);
  auto frame = std::make_shared<const std::string>(1024, 'x');
  for (int retry = 0; retry < 1000; ++retry)
    ASSERT_TRUE(queue.push(frame, true, OutboundQueue::RetryKey{2, 123}));
  EXPECT_EQ(queue.bytes(), 1024u);
  auto active = queue.front();
  queue.clear();
  EXPECT_EQ(budget->bytes, 1024u);  // Cancelled write still owns its bytes.
  EXPECT_EQ(active->size(), 1024u);
  active.reset();
  EXPECT_EQ(budget->bytes, 0u);
}

}  // namespace
}  // namespace choreoos::network
