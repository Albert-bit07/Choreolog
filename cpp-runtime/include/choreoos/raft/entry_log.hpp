#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace choreoos::raft {

struct Entry {
  std::uint64_t term = 0;
  std::string command;
  bool operator==(const Entry&) const = default;
};

// Immutable shared chunks make Node/projection copies and metadata transitions
// independent of payload history. Mutation copies a pointer directory (at most
// 391 pointers in the supported window) and at most one 256-entry chunk.
// No mutable references escape, so cached term validation cannot become stale.
class EntryLog {
 public:
  static constexpr std::size_t kMaximumEntries = 100000;
  EntryLog() : data_(std::make_shared<Data>()) {}
  EntryLog(std::initializer_list<Entry> entries) : EntryLog() {
    for (const auto& entry : entries) push_back(entry);
  }
  EntryLog(const std::vector<Entry>& entries) : EntryLog() {
    for (const auto& entry : entries) push_back(entry);
  }
  [[nodiscard]] std::size_t size() const { return data_->size; }
  [[nodiscard]] bool empty() const { return size() == 0; }
  [[nodiscard]] const Entry& operator[](std::size_t index) const {
    return (*data_->chunks[index / kChunkSize])[index % kChunkSize];
  }
  [[nodiscard]] const Entry& front() const { return (*this)[0]; }
  [[nodiscard]] const Entry& back() const { return (*this)[size() - 1]; }
  [[nodiscard]] bool valid_terms(std::uint64_t before, std::uint64_t maximum) const {
    return data_->valid && (empty() || ((*this)[0].term >= before && back().term <= maximum));
  }
  [[nodiscard]] bool shares_storage_with(const EntryLog& other) const {
    return data_ == other.data_;
  }
  void reserve(std::size_t) {}  // Chunk allocation is incremental.
  void push_back(Entry entry) {
    detach();
    data_->valid = data_->valid && entry.term != 0 && (empty() || back().term <= entry.term);
    if (size() % kChunkSize == 0) {
      auto chunk = std::make_shared<std::vector<Entry>>();
      chunk->reserve(kChunkSize);
      chunk->push_back(std::move(entry));
      data_->chunks.push_back(std::move(chunk));
    } else {
      auto& chunk = data_->chunks.back();
      if (chunk.use_count() != 1) chunk = std::make_shared<std::vector<Entry>>(*chunk);
      chunk->push_back(std::move(entry));
    }
    ++data_->size;
  }
  void set(std::size_t index, Entry entry) {
    detach();
    auto chunk = std::make_shared<std::vector<Entry>>(*data_->chunks[index / kChunkSize]);
    (*chunk)[index % kChunkSize] = std::move(entry);
    data_->chunks[index / kChunkSize] = std::move(chunk);
    validate_terms();
  }
  void resize(std::size_t count) {
    if (count >= size()) {
      while (size() < count) push_back({});
      return;
    }
    detach();
    data_->chunks.resize((count + kChunkSize - 1) / kChunkSize);
    if (count % kChunkSize != 0) {
      auto chunk = std::make_shared<std::vector<Entry>>(*data_->chunks.back());
      chunk->resize(count % kChunkSize);
      data_->chunks.back() = std::move(chunk);
    }
    data_->size = count;
    validate_terms();
  }
  void clear() { data_ = std::make_shared<Data>(); }
  void pop_back() { resize(size() - 1); }

  class Iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = Entry;
    using difference_type = std::ptrdiff_t;
    using pointer = const Entry*;
    using reference = const Entry&;
    Iterator() = default;
    Iterator(const EntryLog* log, std::size_t index) : log_(log), index_(index) {}
    reference operator*() const { return (*log_)[index_]; }
    pointer operator->() const { return &**this; }
    Iterator& operator++() {
      ++index_;
      return *this;
    }
    Iterator operator++(int) {
      auto old = *this;
      ++*this;
      return old;
    }
    Iterator operator+(difference_type distance) const {
      return {log_, static_cast<std::size_t>(static_cast<difference_type>(index_) + distance)};
    }
    bool operator==(const Iterator&) const = default;

   private:
    const EntryLog* log_ = nullptr;
    std::size_t index_ = 0;
  };
  [[nodiscard]] Iterator begin() const { return {this, 0}; }
  [[nodiscard]] Iterator end() const { return {this, size()}; }
  bool operator==(const EntryLog& other) const {
    return data_ == other.data_ ||
           (size() == other.size() && std::equal(begin(), end(), other.begin()));
  }

 private:
  static constexpr std::size_t kChunkSize = 256;
  struct Data {
    std::vector<std::shared_ptr<std::vector<Entry>>> chunks;
    std::size_t size = 0;
    bool valid = true;
  };
  void detach() {
    if (data_.use_count() != 1) data_ = std::make_shared<Data>(*data_);
  }
  void validate_terms() {
    data_->valid = true;
    std::uint64_t previous = 0;
    for (const auto& entry : *this) {
      if (entry.term == 0 || entry.term < previous) data_->valid = false;
      previous = entry.term;
    }
  }
  std::shared_ptr<Data> data_;
};

}  // namespace choreoos::raft
