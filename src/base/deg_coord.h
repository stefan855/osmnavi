#pragma once

#include <cmath>
#include <compare>
#include <limits>

#include "base/util.h"

namespace {
template <typename T>
concept TAllowedIntDegE6 =
    std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>;
};

// Encapsulate a degree coordinate (typically lat or lon) in a class.
//
// To store a floating point coordinate such as 47.341234, it is multiplied by
// DegE6::MulFactor() and rounded to the next integer.
// A MulFactor of 10^6 yields a resolution of roughly 11 cm at the equator,
// which is good enough for our purposes (OSM has 10^7).
//
// This class mostly hides the multiplication factor and makes code that uses
// such coordinates shorter, more readable and less error prone.
class DegE6 {
 public:
  DegE6() : DegE6(0) {}
  constexpr explicit DegE6(int32_t c) : coordinate_(c) {}
  constexpr explicit DegE6(int64_t c) : DegE6(static_cast<int32_t>(c)) {
    assert(c >= std::numeric_limits<int32_t>::min());
    assert(c <= std::numeric_limits<int32_t>::max());
    // CHECK_GE_S(c, std::numeric_limits<int32_t>::min());
    // CHECK_LE_S(c, std::numeric_limits<int32_t>::max());
  }
  constexpr explicit DegE6(float c) : DegE6(static_cast<double>(c)) {}
  constexpr explicit DegE6(double c) {
    auto val = std::llround(c * MUL_FACTOR);
    assert(val >= std::numeric_limits<int32_t>::min());
    assert(val <= std::numeric_limits<int32_t>::max());
    // CHECK_GE_S(val, std::numeric_limits<int32_t>::min());
    // CHECK_LE_S(val, std::numeric_limits<int32_t>::max());
    coordinate_ = static_cast<int32_t>(val);
  }
  // Unsigned integers are probably an error, so forbid it here.
  explicit DegE6(uint8_t c) = delete;
  explicit DegE6(uint16_t c) = delete;
  explicit DegE6(uint32_t c) = delete;
  explicit DegE6(uint64_t c) = delete;

  // Create from OSM coordinate, which is deg * 10^7.
  constexpr static DegE6 FromOSM(int64_t deg_e7) {
    return DegE6(deg_e7);
    // return DegE6((deg_e7 + 5) / 10);
  }

  // TODO DegE6 remove.
  constexpr int32_t ToOSM() const {
    return coordinate_;
    // return coordinate_ * 10;
  }

  // Assignment.
  template <TAllowedIntDegE6 T>
  constexpr inline DegE6& operator=(T v) {
    DegE6 c(v);
    coordinate_ = c.v();
    return *this;
  }

  // Coordinate value as integer.
  constexpr inline int32_t v() const { return coordinate_; }
  constexpr inline int64_t v64() const {
    return static_cast<int64_t>(coordinate_);
  }

  constexpr float AsFloat() const {
    return static_cast<float>(coordinate_) / static_cast<float>(MUL_FACTOR);
  }

  constexpr double AsDouble() const {
    return static_cast<double>(coordinate_) / static_cast<double>(MUL_FACTOR);
  }

  constexpr double ToRad() const {
    return static_cast<double>(coordinate_) / static_cast<double>(MUL_FACTOR) *
           M_PI / 180.0;
  }

  static constexpr int32_t MulFactor() { return MUL_FACTOR; }

  // "spaceship" operator, automatically defines ==, !=, <, <=, >, >=.
  constexpr auto operator<=>(const DegE6& other) const {
    return coordinate_ <=> other.coordinate_;
  }

  // Unclear why this has to be defined. C++...
  bool operator==(const DegE6& other) const {
    return coordinate_ == other.coordinate_;
  }
  bool operator!=(const DegE6& other) const {
    return !(coordinate_ == other.coordinate_);
  }

  friend DegE6 operator-(const DegE6& lhs, const DegE6& rhs) {
    // TODO: handle over/underflows?
    return DegE6(lhs.v64() - rhs.v64());
  }

 private:
  int32_t coordinate_;
  static constexpr int32_t MUL_FACTOR = 10'000'000;
};
static_assert(sizeof(DegE6) == 4);
CHECK_IS_MM_OK(DegE6);
