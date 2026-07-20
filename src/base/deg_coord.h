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
// The template is used to create two identical types that can't be mixed and
// are used throughout the code: LatE6: Store latitude coordinates with
// resolution 10^6. LonE6: Store longitude coordinates with resolution 10^6.
//
// Internals:
// To store a floating point coordinate such as 47.341234, it is multiplied by
// DegE6::MulFactor() and rounded to the next integer.
// A MulFactor of 10^6 yields a resolution of roughly 11 cm at the equator,
// which is good enough for our purposes (OSM has 10^7).
//
// This class mostly hides the multiplication factor and makes code that uses
// such coordinates shorter, more readable and less error prone.
enum class DegE6Type { DegE6Latitude, DegE6Longitude };
template <DegE6Type deg_type>
class DegE6Base {
 public:
  constexpr DegE6Base() : DegE6Base(0) {}
  constexpr explicit DegE6Base(int32_t c) : coordinate_(c) {}
  constexpr explicit DegE6Base(int64_t c) : DegE6Base(static_cast<int32_t>(c)) {
    assert(c >= std::numeric_limits<int32_t>::min());
    assert(c <= std::numeric_limits<int32_t>::max());
    // CHECK_GE_S(c, std::numeric_limits<int32_t>::min());
    // CHECK_LE_S(c, std::numeric_limits<int32_t>::max());
  }
  constexpr explicit DegE6Base(float c) : DegE6Base(static_cast<double>(c)) {}
  constexpr explicit DegE6Base(double c) {
    auto val = std::llround(c * MUL_FACTOR);
    assert(val >= std::numeric_limits<int32_t>::min());
    assert(val <= std::numeric_limits<int32_t>::max());
    // CHECK_GE_S(val, std::numeric_limits<int32_t>::min());
    // CHECK_LE_S(val, std::numeric_limits<int32_t>::max());
    coordinate_ = static_cast<int32_t>(val);
  }
  // Unsigned integers are probably an error, so forbid it here.
  explicit DegE6Base(uint8_t c) = delete;
  explicit DegE6Base(uint16_t c) = delete;
  explicit DegE6Base(uint32_t c) = delete;
  explicit DegE6Base(uint64_t c) = delete;

  // Create from OSM coordinate, which is deg * 10^7.
  constexpr static DegE6Base<deg_type> FromOSM(int64_t deg_e7) {
    return DegE6Base((deg_e7 + 5) / 10);
  }

  // Assignment.
  template <TAllowedIntDegE6 T>
  constexpr inline DegE6Base<deg_type>& operator=(T v) {
    DegE6Base<deg_type> c(v);
    coordinate_ = c.v();
    return *this;
  }

  // Coordinate value as integer.
  constexpr inline int32_t v() const { return coordinate_; }
  constexpr inline int64_t v64() const {
    return static_cast<int64_t>(coordinate_);
  }

  constexpr inline float AsFloat() const {
    return static_cast<float>(coordinate_) / static_cast<float>(MUL_FACTOR);
  }

  constexpr inline double AsDouble() const {
    return static_cast<double>(coordinate_) / static_cast<double>(MUL_FACTOR);
  }

  constexpr inline double ToRad() const {
    return static_cast<double>(coordinate_) / static_cast<double>(MUL_FACTOR) *
           M_PI / 180.0;
  }

  static constexpr int32_t MulFactor() { return MUL_FACTOR; }

  // "spaceship" operator, automatically defines ==, !=, <, <=, >, >=.
  constexpr auto operator<=>(const DegE6Base<deg_type>& other) const {
    return coordinate_ <=> other.coordinate_;
  }

  // Unclear why this has to be defined. C++...
  constexpr bool operator==(const DegE6Base<deg_type>& other) const {
    return coordinate_ == other.coordinate_;
  }
  constexpr bool operator!=(const DegE6Base<deg_type>& other) const {
    return !(coordinate_ == other.coordinate_);
  }

  constexpr friend DegE6Base operator-(const DegE6Base<deg_type>& lhs,
                             const DegE6Base<deg_type>& rhs) {
    // TODO: handle over/underflows?
    return DegE6Base<deg_type>(lhs.v64() - rhs.v64());
  }

 private:
  int32_t coordinate_;
  static constexpr int32_t MUL_FACTOR = 1'000'000;
};

using LatE6 = DegE6Base<DegE6Type::DegE6Latitude>;
using LonE6 = DegE6Base<DegE6Type::DegE6Longitude>;
static_assert(sizeof(LatE6) == 4);
static_assert(sizeof(LonE6) == 4);
CHECK_IS_MM_OK(LatE6);
CHECK_IS_MM_OK(LonE6);

struct LatLon {
  LatE6 lat;
  LonE6 lon;

  auto operator<=>(const LatLon&) const = default;
};
CHECK_IS_MM_OK(LatLon);
