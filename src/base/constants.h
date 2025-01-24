#pragma once

#include <limits>
#include <cstdint>

constexpr int32_t INF32 = std::numeric_limits<int32_t>::max();
constexpr int64_t INF64 = std::numeric_limits<int64_t>::max();

constexpr uint32_t INFU30 = (1u << 30) - 1;
constexpr uint32_t INFU31 = (1u << 31) - 1;
constexpr uint32_t INFU32 = std::numeric_limits<uint32_t>::max();
constexpr uint64_t INFU64 = std::numeric_limits<uint64_t>::max();

constexpr int64_t TEN_POW_7 = 10'000'000;
