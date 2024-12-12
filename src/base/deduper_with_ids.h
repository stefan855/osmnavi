#pragma once

#include <algorithm>
#include <deque>
#include <limits>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "base/util.h"

// De-duplicate a series of objects (for instance strings) of templated type and
// assign consecutive ids 0,1,2,.. to them. When the same ("==") object is
// inserted multiple times, identical ids are returned and the object is only
// stored once.
//
// As an optional second step, the ids can be sorted by decreasing popularity.
// This saves memory in case ids have to be var-encoded and tends to be more
// cache friendly because the popular objects are clustered.
//
// Example:
//   DeDuperWithIds<std::string> d;
//   d.Add("aaa");           // -> return id 0
//   d.Add("bbb");           // -> return id 1
//   d.Add("ccc");           // -> return id 2
//   d.Add("bbb");           // -> return id 1
//   d.GetObjVector();       // -> {"aaa", "bbb", "ccc"}
//   d.SortByPopularity();
//   d.GetObjVector();       // -> {"bbb", "aaa", "ccc"}
//   d.GetSortMapping();     // -> {1, 0, 2}

template <typename TObj>
class DeDuperWithIds {
 public:
  // Add object 'obj' and return the id for it. If the object hasn't been seen
  // before, then a new id will be returned, otherwise the id of the already
  // stored object is returned.
  uint32_t Add(const TObj& obj) {
    CHECK_S(!sorted_);
    added_++;
    auto iter = lookup_.find(&obj);
    if (iter != lookup_.end()) {
      iter->second->ref_count++;
      return iter->second->id;
    }
    uint32_t new_idx = lookup_.size();
    objs_.push_back(obj);
    entries_.push_back({.id = new_idx, .ref_count = 1});
    lookup_[&objs_.back()] = &entries_.back();
    return new_idx;
  }

  // Number of calls to Add().
  uint32_t num_added() const { return added_; }
  // Number of objects kept internally after de-duping.
  uint32_t num_unique() const { return entries_.size(); }

  // Sort the objects by decreasing popularity (reference count) and disallow
  // any further calls to Add(). The sort is stable, i.e. the relative id-order
  // of objects with the same popularity is unchanged.
  void SortByPopularity() {
    CHECK_S(!sorted_);
    CHECK_EQ_S(entries_.size(), lookup_.size());
    lookup_.clear();  // Lookup not not needed anymore, so clear it.
    std::stable_sort(entries_.begin(), entries_.end(),
                     [](const auto& a, const auto& b) {
                       if (a.ref_count != b.ref_count) {
                         return a.ref_count > b.ref_count;
                       } else {
                         return a.id < b.id;
                       }
                     });
    sorted_ = true;
  }

  // If SortByPopularity() has been called, then this functions returns a vector
  // mapping the original id values to the new id values after sorting. The
  // vector has size num_unique().
  std::vector<uint32_t> GetSortMapping() const {
    CHECK_S(sorted_);
    std::vector<uint32_t> v(entries_.size(),
                            std::numeric_limits<uint32_t>::max());
    for (size_t pos = 0; pos < entries_.size(); ++pos) {
      v.at(entries_.at(pos).id) = pos;
    }
    return v;
  }

  // Get object that has id. Note that even after sorting, the original id has
  // to be used.
  const TObj& GetObj(uint32_t id) const { return objs_.at(id); }

  // If SortByPopularity() was called, then return the vector of objects in the
  // new sorted order. Otherwise, return the vector of the original order, which
  // corresponds to the ids returned by Add().
  // The returned vector has size num_unique().
  std::vector<TObj> GetObjVector() const {
    CHECK_EQ_S(entries_.size(), objs_.size());
    std::vector<TObj> v;
    v.reserve(entries_.size());
    for (const Entry& e : entries_) {
      v.push_back(objs_.at(e.id));
    }
    return v;
  }

 private:
  friend void TestDeDuperWithIdsInt();
  friend void TestDeDuperWithIdsString();

  struct Entry {
    uint32_t id;
    uint32_t ref_count;
  };

  // Hash function, needed because hash table contains pointers.
  struct TPtrHash {
    size_t operator()(const TObj* p) const { return std::hash<TObj>{}(*p); }
  };

  // Equal function, needed because hash table contains pointers.
  struct TPtrEqual {
    bool operator()(const TObj* a, const TObj* b) const { return (*a) == (*b); }
  };

  // Get Information we have for object identified by 'id'.
  const Entry& GetEntryAt(uint32_t id) const { return entries_.at(id); }

  bool sorted_ = false;
  uint32_t added_ = 0;
  // Objects TObj and Entry are not stored together. This way we avoid sorting a
  // vector with potentially large objects. We use deques to allow stable
  // pointers into the data, see below.
  std::deque<TObj> objs_;
  std::deque<Entry> entries_;
  // The map uses pointers into the objs_/entries_ vectors to avoid double
  // allocating objects of type TObj. Since the vectors are actually deques, the
  // pointers are stable.
  absl::flat_hash_map<const TObj*, Entry*, TPtrHash, TPtrEqual> lookup_;
};
