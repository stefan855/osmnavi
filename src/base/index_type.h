#pragma once

#include <cmath>
#include <compare>
#include <iostream>
#include <limits>

#include "base/constants.h"
#include "base/util.h"

// This is only needed to create separate template instances that can not be
// mixed but have the same behavior.
enum class IndexTypeName {
  GNodeIdx,  // Currently unused.
  GEdgeIdx,  // Currently unused.
  GWayIdx,
  MNodeIdx,
  MEdgeIdx,
  MWayIdx,
  RVisIdx,     // Used in routing code.
};

template <typename T, IndexTypeName type_name>
class IndexType {
 private:
  static_assert(std::is_unsigned_v<T>);
  using SelfType = IndexType<T, type_name>;

 public:
  constexpr IndexType() : IndexType(0u) {}
  constexpr explicit IndexType(uint16_t c) : v_(static_cast<T>(c)) {
    if constexpr (sizeof(T) < sizeof(uint16_t)) {
      assert(c <= std::numeric_limits<T>::max());
    }
  }
  constexpr explicit IndexType(uint32_t c) : v_(static_cast<T>(c)) {
    if constexpr (sizeof(T) < sizeof(uint32_t)) {
      assert(c <= std::numeric_limits<T>::max());
    }
  }
  constexpr explicit IndexType(uint64_t c) : IndexType(static_cast<T>(c)) {
    if constexpr (sizeof(T) < sizeof(uint64_t)) {
      assert(c <= std::numeric_limits<T>::max());
    }
  }
  // Unsigned integers are probably an error, so forbid them here.
  explicit IndexType(int8_t c) = delete;
  explicit IndexType(int16_t c) = delete;
  explicit IndexType(int32_t c) = delete;
  explicit IndexType(int64_t c) = delete;

  // Coordinate value as integer.
  constexpr inline T v() const { return v_; }
  constexpr inline uint64_t v64() const { return static_cast<uint64_t>(v_); }

  // "spaceship" operator, automatically defines ==, !=, <, <=, >, >=.
  constexpr auto operator<=>(const SelfType& other) const {
    return v_ <=> other.v_;
  }

  // Unclear why this has to be defined. C++...
  constexpr bool operator==(const SelfType& other) const {
    return v_ == other.v_;
  }
  constexpr bool operator!=(const SelfType& other) const {
    return !(v_ == other.v_);
  }

  // Prefix operator ++idx.
  SelfType& operator++() {
    CHECK_LT_S(v_, std::numeric_limits<T>::max());
    ++v_;
    return *this;
  }

#if 0
  This would be useful for iterating backwards, but it fails with a runtime
  error when going to -1.
  // Prefix operator --idx.
  constexpr SelfType& operator--() {
    CHECK_GT_S(v_, 0);
    --v_;
    return *this;
  }
#endif

#if 0
  // Postfix increment operator required for iota_view.
  SelfType operator++(int) {
    SelfType old(*this);
    ++(*this);
    return old;
  }
#endif

  constexpr SelfType operator+(SelfType rhs) const {
    assert(v_ <= std::numeric_limits<T>::max() - rhs.v_);
    return SelfType(v_ + rhs.v_);
  }

  constexpr SelfType operator-(SelfType rhs) const {
    assert(v_ >= rhs.v_);
    return SelfType(v_ - rhs.v_);
  }

  constexpr SelfType operator+(T rhs) const {
    assert(v_ <= std::numeric_limits<T>::max() - rhs);
    return SelfType(v_ + rhs);
  }

  constexpr SelfType operator-(T rhs) const {
    assert(v_ >= rhs);
    return SelfType(v_ - rhs);
  }

  constexpr SelfType operator/(uint64_t rhs) const {
    return SelfType(v_ / rhs);
  }

  constexpr SelfType& operator+=(T rhs) {
    assert(v_ <= std::numeric_limits<T>::max() - rhs);
    v_ += rhs;
    return *this;
  }

  // Compare with normal size_t
  friend auto operator<=>(SelfType lhs, size_t rhs) { return lhs.v_ <=> rhs; }
  friend auto operator<=>(size_t lhs, SelfType rhs) { return lhs <=> rhs.v_; }
  // Needed when comparing different types.
  constexpr bool operator==(size_t other) const { return v_ == other; }

  // Output to <<, needed for CHECK_* macros.
  friend std::ostream& operator<<(std::ostream& os, const SelfType& i) {
    os << i.v_;
    return os;
  }

 protected:
  T v_;
};

using GWayIdx = IndexType<uint32_t, IndexTypeName::GWayIdx>;
static_assert(sizeof(GWayIdx) == 4);
CHECK_IS_MM_OK(GWayIdx);

using MWayIdx = IndexType<uint32_t, IndexTypeName::MWayIdx>;
using MNodeIdx = IndexType<uint32_t, IndexTypeName::MNodeIdx>;
using MEdgeIdx = IndexType<uint32_t, IndexTypeName::MEdgeIdx>;
using RVisIdx = IndexType<uint32_t, IndexTypeName::RVisIdx>;
