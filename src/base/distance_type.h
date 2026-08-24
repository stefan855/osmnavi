#pragma once

#include <cmath>
#include <compare>
#include <iostream>
#include <limits>

#include "base/util.h"

// Encapsulate a geometrical distance in a class.
// Internally, values are stored as centimeters using a uint32_t, which is
// enough to handle distances on earth.
//
// When using uints, the unit is centimeters, when using doubles, the unit is
// meters.
//
// This class makes code that uses distances shorter, more readable and less
// error prone.
class DistanceType {
 public:
  constexpr DistanceType() : DistanceType(0u) {}
  // Centimeters.
  constexpr explicit DistanceType(uint32_t c) : centimeters_(c) {}
  constexpr explicit DistanceType(uint64_t c)
      : DistanceType(static_cast<uint32_t>(c)) {
    assert(c <= std::numeric_limits<uint32_t>::max());
  }
  // Meters
  constexpr explicit DistanceType(double c) {
    assert(c >= 0.0);
    auto val = std::llround(c * MUL_FACTOR);
    assert(val <= std::numeric_limits<uint32_t>::max());
    centimeters_ = static_cast<uint32_t>(val);
  }
  // Unsigned integers are probably an error, so forbid it here.
  explicit DistanceType(int8_t c) = delete;
  explicit DistanceType(int16_t c) = delete;
  explicit DistanceType(int32_t c) = delete;
  explicit DistanceType(int64_t c) = delete;

  // Return as centimeters.
  constexpr inline uint32_t cm() const { return centimeters_; }

  // Return as meters.
  constexpr inline double meters() const {
    return static_cast<double>(centimeters_) / static_cast<double>(MUL_FACTOR);
  }

  static constexpr int32_t MulFactor() { return MUL_FACTOR; }

  // "spaceship" operator, automatically defines ==, !=, <, <=, >, >=.
  constexpr auto operator<=>(const DistanceType& other) const {
    return centimeters_ <=> other.centimeters_;
  }

  // Unclear why this has to be defined. C++...
  constexpr bool operator==(const DistanceType& other) const {
    return centimeters_ == other.centimeters_;
  }
  constexpr bool operator!=(const DistanceType& other) const {
    return centimeters_ != other.centimeters_;
  }

  constexpr static DistanceType PositiveDiff(const DistanceType& a,
                                             const DistanceType& b) {
    return a >= b ? DistanceType(a.cm() - b.cm())
                  : DistanceType(b.cm() - a.cm());
  }

  // Compare with normal uint64_t.
  friend auto operator<=>(const DistanceType lhs, uint64_t rhs) {
    return lhs.centimeters_ <=> rhs;
  }
  friend auto operator<=>(uint64_t lhs, const DistanceType rhs) {
    return lhs <=> rhs.centimeters_;
  }
  // Needed when comparing different types.
  constexpr bool operator==(uint64_t other) const {
    return centimeters_ == other;
  }

  constexpr DistanceType operator+(const DistanceType rhs) const {
    assert(centimeters_ <= MAXU32 - rhs.centimeters_);
    return DistanceType(centimeters_ + rhs.centimeters_);
  }

  constexpr DistanceType operator-(const DistanceType rhs) const {
    assert(*this >= rhs);
    return DistanceType(centimeters_ - rhs.centimeters_);
  }

  constexpr DistanceType operator/(uint64_t rhs) const {
    return DistanceType(centimeters_ / rhs);
  }

  // Output to <<, needed for CHECK_* macros.
  friend std::ostream& operator<<(std::ostream& os, const DistanceType& d) {
    os << d.cm();
    return os;
  }

 private:
  uint32_t centimeters_;
  static constexpr int32_t MUL_FACTOR = 100;  // 1 meter has 100 cm
};

CHECK_IS_MM_OK(DistanceType);
