#pragma once

#include <limits>
#include <cstdint>

constexpr int32_t INF32 = std::numeric_limits<int32_t>::max();
constexpr int64_t INF64 = std::numeric_limits<int64_t>::max();

constexpr uint32_t MAXU16 = std::numeric_limits<uint16_t>::max();
constexpr uint32_t INFU16 = MAXU16;

constexpr uint32_t MAXU30 = (1u << 30) - 1;
constexpr uint32_t INFU30 = MAXU30;

constexpr uint32_t MAXU31 = (1u << 31) - 1;
constexpr uint32_t INFU31 = MAXU31;

constexpr uint32_t MAXU32 = std::numeric_limits<uint32_t>::max();
constexpr uint32_t INFU32 = MAXU32;

constexpr uint64_t MAXU64 = std::numeric_limits<uint64_t>::max();
constexpr uint64_t INFU64 = MAXU64;

constexpr int64_t TEN_POW_7 = 10'000'000;
