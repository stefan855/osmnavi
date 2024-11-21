#pragma once

#include <algorithm>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "base/util.h"

class FrequencyTable {
 public:
  struct Entry {
    std::string key;
    int64_t ref_count = 0;
    int64_t example_id = 0;
  };

  FrequencyTable() { Clear(); }

  void Clear() {
    added_ = 0;
    freq_table_.clear();
  }

  // Add an element to the table.
  void Add(std::string_view str, int64_t example_id) {
    added_++;
    auto iter = freq_table_.find(str);
    InternalEntry* e = nullptr;
    if (iter != freq_table_.end()) {
      e = &iter->second;
    } else {
      e = &freq_table_[str];
    }
    if (e->ref_count == 0 ||
        (absl::ToInt64Nanoseconds(absl::Now() - absl::Time()) % e->ref_count) ==
            0) {
      e->example_id = example_id;
    }
    e->ref_count++;
  }

  // Get a vector containing all seen entries and sorted by decreasing reference
  // count. Entries with the same reference count are sorted by increasing key.
  std::vector<Entry> GetSortedElements() const {
    std::vector<Entry> result;
    for (const auto& [key, value] : freq_table_) {
      result.emplace_back(key, value.ref_count, value.example_id);
    }
    std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) {
      if (a.ref_count != b.ref_count) {
        return a.ref_count > b.ref_count;
      } else {
        return a.key < b.key;
      }
    });
    return result;
  }

  // Return the maximum size of a key from the keys in 'v' in the range
  // [0..n_pos-1].
  static size_t MaxKeySize(const std::vector<Entry>& v, size_t n_top) {
    size_t m = 0;
    for (size_t i = 0; i < v.size() && i < n_top; ++i) {
      m = std::max(m, v.at(i).key.size());

    }
    return m;
  }

  // Total number of added elements, i.e. the sum of all frequencies in the
  // table.
  uint64_t Total() const { return added_; }

  // Total number of added elements.
  uint64_t TotalUnique() const { return freq_table_.size(); }

 private:
  struct InternalEntry {
    int64_t ref_count = 0;
    int64_t example_id = 0;
  };
  bool sorted_ = false;
  uint64_t added_ = 0;
  absl::flat_hash_map<std::string, InternalEntry> freq_table_;
};
