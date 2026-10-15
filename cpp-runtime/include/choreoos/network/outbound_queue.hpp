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
    auto reservation = std::make_shared<Reservation>(std::move(bytes), budget_, peer);
    // Aliasing ownership carries the reservation into an active async write.
    // Retirement may clear the queue before that write's cancellation runs.
    Buffer accounted(reservation, reservation->buffer.get());
    frames_.push_back({std::move(accounted), retry});
    bytes_ += size;
    return true;
  }
  [[nodiscard]] bool empty() const { return frames_.empty(); }
  [[nodiscard]] std::size_t bytes() const { return bytes_; }
  [[nodiscard]] Buffer front() const { return frames_.front().bytes; }
  void pop() {
    const auto& frame = frames_.front();
    const auto size = frame.bytes->size();
    bytes_ -= size;
    frames_.pop_front();
  }
  void clear() {
    while (!empty()) pop();
  }

 private:
  static constexpr std::size_t kSessionBytes = 2u << 20;
  struct Frame {
    Buffer bytes;
    std::optional<RetryKey> retry;
  };
  struct Reservation {
    Reservation(Buffer bytes, std::shared_ptr<QueueBudget> owner, bool is_peer)
        : buffer(std::move(bytes)), budget(std::move(owner)), peer(is_peer) {
      budget->bytes += buffer->size();
      if (!peer) budget->client_bytes += buffer->size();
    }
    ~Reservation() {
      budget->bytes -= buffer->size();
      if (!peer) budget->client_bytes -= buffer->size();
    }
    Reservation(const Reservation&) = delete;
    Reservation& operator=(const Reservation&) = delete;
    Buffer buffer;
    std::shared_ptr<QueueBudget> budget;
    bool peer;
  };
  std::shared_ptr<QueueBudget> budget_;
  std::size_t bytes_ = 0;
  std::deque<Frame> frames_;
};

}  // namespace choreoos::network
