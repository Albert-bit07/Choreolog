#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <utility>
#include <vector>

namespace choreoos::network {

// Strand-owned waiters. Each request owns its deadline; the commit index
// selects only newly eligible requests rather than scanning every live waiter.
template <class T>
class PendingRequests {
 public:
  static constexpr std::size_t kCapacity = 1024;
  [[nodiscard]] bool full() const { return items_.size() >= kCapacity; }
  [[nodiscard]] std::size_t size() const { return items_.size(); }
  std::uint64_t add(std::uint64_t index, T item) {
    const auto id = next_++;
    by_index_[index].insert(id);
    items_.emplace(id, Item{index, std::move(item)});
    return id;
  }
  std::optional<T> take(std::uint64_t id) {
    auto it = items_.find(id);
    if (it == items_.end()) return std::nullopt;
    auto group = by_index_.find(it->second.index);
    group->second.erase(id);
    if (group->second.empty()) by_index_.erase(group);
    T value = std::move(it->second.value);
    items_.erase(it);
    return value;
  }
  std::vector<T> take_through(std::uint64_t commit) {
    std::vector<T> ready;
    while (!by_index_.empty() && by_index_.begin()->first <= commit) {
      auto item = take(*by_index_.begin()->second.begin());
      if (item) ready.push_back(std::move(*item));
    }
    return ready;
  }
  template <class Predicate>
  std::vector<T> remove_if(Predicate predicate) {
    std::vector<std::uint64_t> ids;
    for (const auto& [id, item] : items_)
      if (predicate(item.value)) ids.push_back(id);
    std::vector<T> removed;
    removed.reserve(ids.size());
    for (auto id : ids) {
      auto item = take(id);
      if (item) removed.push_back(std::move(*item));
    }
    return removed;
  }

 private:
  struct Item {
    std::uint64_t index;
    T value;
  };
  std::uint64_t next_ = 1;
  std::map<std::uint64_t, Item> items_;
  std::map<std::uint64_t, std::set<std::uint64_t>> by_index_;
};

}  // namespace choreoos::network
