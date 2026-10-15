#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <utility>

namespace choreoos::network {

struct QueueBudget {
  static constexpr std::size_t kGlobalBytes = 32u << 20;
  static constexpr std::size_t kClientBytes = 24u << 20;
  std::size_t bytes = 0;
  std::size_t client_bytes = 0;
};

// Strand-owned accounting includes the frame currently being written. A
// shared frame captured by its write handler remains alive after retirement.
class OutboundQueue {
 public:
  using Buffer = std::shared_ptr<const std::string>;
  using RetryKey = std::pair<std::uint16_t, std::uint64_t>;
  explicit OutboundQueue(std::shared_ptr<QueueBudget> budget) : budget_(std::move(budget)) {}
  ~OutboundQueue() { clear(); }
  OutboundQueue(const OutboundQueue&) = delete;
  OutboundQueue& operator=(const OutboundQueue&) = delete;
  bool push(Buffer bytes, bool peer, std::optional<RetryKey> retry = {}) {
    if (retry) {
      for (const auto& frame : frames_)
        if (frame.retry == retry) return true;
    }
    const auto size = bytes->size();
    if (frames_.size() >= 64 || size > kSessionBytes - bytes_ ||
        size > QueueBudget::kGlobalBytes - budget_->bytes ||
        (!peer && size > QueueBudget::kClientBytes - budget_->client_bytes))
      return false;
    frames_.push_back({std::move(bytes), peer, retry});
    bytes_ += size;
    budget_->bytes += size;
    if (!peer) budget_->client_bytes += size;
    return true;
  }
  [[nodiscard]] bool empty() const { return frames_.empty(); }
  [[nodiscard]] std::size_t bytes() const { return bytes_; }
  [[nodiscard]] Buffer front() const { return frames_.front().bytes; }
  void pop() {
    const auto& frame = frames_.front();
    const auto size = frame.bytes->size();
    bytes_ -= size;
    budget_->bytes -= size;
    if (!frame.peer) budget_->client_bytes -= size;
    frames_.pop_front();
  }
  void clear() {
    while (!empty()) pop();
  }

 private:
  static constexpr std::size_t kSessionBytes = 2u << 20;
  struct Frame {
    Buffer bytes;
    bool peer;
    std::optional<RetryKey> retry;
  };
  std::shared_ptr<QueueBudget> budget_;
  std::size_t bytes_ = 0;
  std::deque<Frame> frames_;
};

}  // namespace choreoos::network
