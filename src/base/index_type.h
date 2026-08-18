#pragma once

#include <cmath>
#include <compare>
#include <iostream>
#include <limits>

#include "base/constants.h"
#include "base/util.h"

enum class IndexTypeName {
  GNodeIdx,
  GEdgeIdx,
  GWayIdx,
  MNodeIdx,
  MEdgeIdx,
  MWayIdx,
  RVisIdx  // Used in routing code.
};

template <IndexTypeName type_name>
class IndexType {
 public:
  constexpr IndexType() : IndexType(0u) {}
  constexpr explicit IndexType(uint32_t c) : idx_(c) {}
  constexpr explicit IndexType(uint64_t c)
      : IndexType(static_cast<uint32_t>(c)) {
    assert(c <= std::numeric_limits<uint32_t>::max());
  }
  // Unsigned integers are probably an error, so forbid them here.
  explicit IndexType(int8_t c) = delete;
  explicit IndexType(int16_t c) = delete;
  explicit IndexType(int32_t c) = delete;
  explicit IndexType(int64_t c) = delete;

  // Coordinate value as integer.
  constexpr inline uint32_t v() const { return idx_; }
  constexpr inline uint64_t v64() const { return static_cast<uint64_t>(idx_); }

  // "spaceship" operator, automatically defines ==, !=, <, <=, >, >=.
  constexpr auto operator<=>(const IndexType<type_name>& other) const {
    return idx_ <=> other.idx_;
  }

  // Unclear why this has to be defined. C++...
  constexpr bool operator==(const IndexType<type_name>& other) const {
    return idx_ == other.idx_;
  }
  constexpr bool operator!=(const IndexType<type_name>& other) const {
    return !(idx_ == other.idx_);
  }

  // Prefix operator ++idx.
  constexpr IndexType<type_name>& operator++() {
    CHECK_LT_S(idx_, MAXU32);
    ++idx_;
    return *this;
  }

#if 0
  This would be useful for iterating backwards, but it fails with a runtime
  error when going to -1.
  // Prefix operator --idx.
  constexpr IndexType<type_name>& operator--() {
    CHECK_GT_S(idx_, 0);
    --idx_;
    return *this;
  }
#endif

#if 0
  // Postfix increment operator required for iota_view.
  IndexType<type_name> operator++(int) {
    IndexType<type_name> old(*this);
    ++(*this);
    return old;
  }
#endif

  constexpr IndexType<type_name> operator+(uint32_t rhs) const {
    CHECK_LE_S(static_cast<uint64_t>(idx_) + rhs, MAXU32);
    return IndexType<type_name>(idx_ + rhs);
  }

  constexpr IndexType<type_name> operator-(uint32_t rhs) const {
    CHECK_GE_S(idx_, rhs);
    return IndexType<type_name>(idx_ - rhs);
  }

  // Compare with normal size_t
  friend auto operator<=>(IndexType<type_name> lhs, size_t rhs) {
    return lhs.idx_ <=> rhs;
  }
  friend auto operator<=>(size_t lhs, IndexType<type_name> rhs) {
    return lhs <=> rhs.idx_;
  }
  // Needed when comparing different types.
  constexpr bool operator==(size_t other) const { return idx_ == other; }

  // Output to <<, needed for CHECK_* macros.
  friend std::ostream& operator<<(std::ostream& os,
                                  const IndexType<type_name>& i) {
    os << i.idx_;
    return os;
  }

 private:
  uint32_t idx_;
};

using GWayIdx = IndexType<IndexTypeName::GWayIdx>;
static_assert(sizeof(GWayIdx) == 4);
CHECK_IS_MM_OK(GWayIdx);

using MWayIdx = IndexType<IndexTypeName::MWayIdx>;
using MNodeIdx = IndexType<IndexTypeName::MNodeIdx>;
using MEdgeIdx = IndexType<IndexTypeName::MEdgeIdx>;
using RVisIdx = IndexType<IndexTypeName::RVisIdx>;

