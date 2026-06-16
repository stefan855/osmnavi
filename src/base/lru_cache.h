#pragma once

// A thread-safe LRU cache with constant-time access.

#include <list>
#include <mutex>
#include <optional>
#include <string>

#include "absl/container/flat_hash_map.h"

template <typename TKey, typename TValue>
class LRUCache {
 public:
  explicit LRUCache(size_t capacity) : capacity_(capacity) {}

  std::optional<TValue> get(const TKey& key) {
    std::lock_guard lock(mutex_);
    auto it = map_.find(key);
    if (it == map_.end()) {
      return std::nullopt;
    }
    list_.splice(list_.begin(), list_, it->second);
    return it->second->second;
  }

  void put(const TKey& key, const TValue& value) {
    std::lock_guard lock(mutex_);
    auto it = map_.find(key);
    if (it != map_.end()) {
      // Update value.
      it->second->second = value;
      list_.splice(list_.begin(), list_, it->second);
      return;
    }
    if (map_.size() == capacity_) {
      map_.erase(list_.back().first);
      list_.pop_back();
    }
    list_.emplace_front(key, value);
    map_[key] = list_.begin();
  }

  bool contains(const TKey& key) const {
    std::lock_guard lock(mutex_);
    return map_.count(key) > 0;
  }

  size_t size() const {
    std::lock_guard lock(mutex_);
    return map_.size();
  }

  size_t capacity() const { return capacity_; }

 private:
  using TPair = std::pair<TKey, TValue>;
  std::list<TPair> list_;
  absl::flat_hash_map<TKey, typename std::list<TPair>::iterator> map_;
  size_t capacity_;
  mutable std::mutex mutex_;
};
