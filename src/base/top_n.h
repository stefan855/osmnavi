#pragma once

#include <algorithm>
#include <array>
#include <span>

#include "base/util.h"

// Collects the "top" N items by keeping a sorted array of them. If
// 'keep_greater' is true, then the largest items are kept, otherwise the
// smallest items. The array is sorted in a way that puts the top item at
// position 0.
//
// Note 1:
// Uses stable_sort, to force deterministic results.
// Note 2:
// N_ITEMS has to be small ([1..16]), otherwise the chosen method of keeping a
// sorted array might be too slow.
template <typename T, uint8_t N_ITEMS, bool keep_greater>
class TopN {
 public:
  void Add(const T& val) {
    if (filled_size_ < items_.size()) {
      items_.at(filled_size_++) = val;
      if constexpr (keep_greater) {
        std::stable_sort(items_.begin(), items_.begin() + filled_size_,
                         std::greater<>());
      } else {
        std::stable_sort(items_.begin(), items_.begin() + filled_size_);
      }
    } else {
      if constexpr (keep_greater) {
        if (val > items_.back()) {
          items_.back() = val;
          std::stable_sort(items_.begin(), items_.end(), std::greater<>());
        }
      } else {
        if (val < items_.back()) {
          items_.back() = val;
          std::stable_sort(items_.begin(), items_.end());
        }
      }
    }
  }

  std::span<const T> span() const {
    return std::span<const T>(&items_.at(0), filled_size_);
  }
  const T& top() const { return items_.front(); }
  const T& bottom() const { return items_.at(filled_size_ - 1); }
  size_t empty() const { return filled_size_ == 0; }
  size_t size() const { return filled_size_; }
  size_t filled() const { return filled_size_ == N_ITEMS; }

 private:
  static_assert(N_ITEMS > 0 && N_ITEMS <= 16);

  // items_ is sorted in increasing order and has the smallest element at
  // position 0.
  std::array<T, N_ITEMS> items_;
  size_t filled_size_ = 0;
};
