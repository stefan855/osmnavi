#pragma once

#include <cmath>
#include <compare>
#include <iostream>
#include <limits>

#include "base/constants.h"
#include "base/util.h"

class DurationMS {
 public:
  constexpr DurationMS() : DurationMS(0u) {}
  constexpr explicit DurationMS(uint32_t c) : ms_(c) {}
  constexpr explicit DurationMS(uint64_t c)
      : DurationMS(static_cast<uint32_t>(c)) {
    assert(c <= MAXU32);
  }
  // Unsigned integers are probably an error, so forbid them here.
  explicit DurationMS(int8_t c) = delete;
  explicit DurationMS(int16_t c) = delete;
  explicit DurationMS(int32_t c) = delete;
  explicit DurationMS(int64_t c) = delete;

  constexpr inline uint32_t ms() const { return ms_; }
  constexpr inline uint64_t ms64() const { return ms_; }
  constexpr double seconds() { return static_cast<double>(ms_) / 1000.0; }

  // "spaceship" operator, automatically defines ==, !=, <, <=, >, >=.
  constexpr auto operator<=>(const DurationMS& other) const {
    return ms_ <=> other.ms_;
  }

  // Unclear why this has to be defined. C++...
  constexpr bool operator==(const DurationMS& other) const {
    return ms_ == other.ms_;
  }
  constexpr bool operator!=(const DurationMS& other) const {
    return !(ms_ == other.ms_);
  }

  constexpr DurationMS operator+(DurationMS rhs) const {
    assert(ms_ <= MAXU32 - rhs.ms_);
    return DurationMS(ms_ + rhs.ms_);
  }

  constexpr DurationMS operator-(DurationMS rhs) const {
    assert(ms_ >= rhs.ms_);
    return DurationMS(ms_ - rhs.ms_);
  }

  constexpr DurationMS operator+(uint32_t rhs) const {
    assert(ms_ <= MAXU32 - rhs);
    return DurationMS(ms_ + rhs);
  }

  constexpr DurationMS operator-(uint32_t rhs) const {
    assert(ms_ >= rhs);
    return DurationMS(ms_ - rhs);
  }

  constexpr DurationMS operator/(uint64_t rhs) const {
    return DurationMS(ms_ / rhs);
  }

  constexpr DurationMS operator*(uint64_t rhs) const {
    return DurationMS(ms_ * rhs);
  }

  constexpr DurationMS& operator+=(uint32_t rhs) {
    assert(ms_ <= MAXU32 - rhs);
    ms_ += rhs;
    return *this;
  }

  // Compare with normal size_t
  friend auto operator<=>(DurationMS lhs, size_t rhs) {
    return lhs.ms_ <=> rhs;
  }
  friend auto operator<=>(size_t lhs, DurationMS rhs) {
    return lhs <=> rhs.ms_;
  }
  // Needed when comparing different types.
  constexpr bool operator==(size_t other) const { return ms_ == other; }

  // Output to <<, needed for CHECK_* macros.
  friend std::ostream& operator<<(std::ostream& os, const DurationMS& i) {
    os << i.ms_;
    return os;
  }

 protected:
  uint32_t ms_;
};

CHECK_IS_MM_OK(DurationMS);
