#pragma once

#include "base/util.h"

constexpr int max_varint_bytes = 10;

// A variable length buffer that keeps track of the bytes that have been
// written so far and grows/reallocates memory as necessary.
class WriteBuff {
 public:
  WriteBuff(std::uint32_t initial_size = 0) { MakeAvailable(initial_size); }

  ~WriteBuff() {
    if (data_ != nullptr) {
      free(data_);
    }
  }

  inline void MakeAvailable(std::uint32_t needed) {
    if (needed <= allocated_ - used_) {
      return;
    }
    std::uint32_t new_size = std::max(allocated_ + 2 * needed, 2 * allocated_);
    std::uint8_t* new_buff = static_cast<std::uint8_t*>(calloc(1, new_size));
    if (data_ != nullptr) {
      assert(used_ <= allocated_);
      assert(new_size > allocated_);
      memcpy(new_buff, data_, used_);
      free(data_);
    }
    allocated_ = new_size;
    data_ = new_buff;
  }

  inline void CopyBytes(const std::uint8_t* from, std::uint32_t size) {
    MakeAvailable(size);
    memcpy(data_ + used_, from, size);
    used_ += size;
  }

  inline void CopyFrom(const WriteBuff& other) {
    CopyBytes(other.base_ptr(), other.used());
  }

  inline std::uint8_t* base_ptr() const { return data_; }
  inline std::uint8_t* write_ptr() { return data_ + used_; }
  inline std::uint32_t used() const { return used_; }
  inline std::uint32_t allocated() const { return allocated_; }
  inline void clear() { used_ = 0; }
  inline std::uint32_t add_byte(std::uint8_t byte) {
    MakeAvailable(1);
    data_[used_] = byte;
    used_++;
    assert(used_ <= allocated_);
    return 1;
  }
  // Increment write position. Must have enough bytes available.
  inline void advance(std::uint32_t cnt) {
    MakeAvailable(cnt);
    used_ += cnt;
    assert(used_ <= allocated_);
  }

 private:
  std::uint8_t* data_ = nullptr;
  std::uint32_t allocated_ = 0;
  std::uint32_t used_ = 0;
};

// Encodes an unsigned 64 bit integer into a a byte array.
// Maximum needed size is for the encoding is 'max_varint_bytes'.
// Return the number of bytes written to 'ptr'.
inline std::uint32_t EncodeUInt(std::uint64_t v, WriteBuff* buff) {
  buff->MakeAvailable(max_varint_bytes);
  std::uint8_t* ptr = buff->write_ptr();
  std::uint32_t cnt = 0;
  while (v > 127) {
    ptr[cnt++] = (std::uint8_t)128 | (std::uint8_t)(v & 127);
    v = v >> 7;
  }
  ptr[cnt++] = (std::uint8_t)(v & 127);
  buff->advance(cnt);
  return cnt;
}

inline std::uint32_t DecodeUInt(const std::uint8_t* ptr, std::uint64_t* v) {
  int cnt = 0;
  *v = ptr[cnt] & 127;
  while (ptr[cnt++] & 128) {
    (*v) = (*v) | (((std::uint64_t)(ptr[cnt] & 127)) << (7 * cnt));
  }
  return cnt;
}

inline std::uint32_t EncodeInt(std::int64_t v, WriteBuff* buff) {
  // ZigZag encoding, see https://github.com/stepchowfun/typical
  std::uint64_t uv = (v >> 63) ^ (v << 1);
  return EncodeUInt(uv, buff);
}

inline std::uint32_t DecodeInt(const std::uint8_t* ptr, std::int64_t* v) {
  std::uint64_t uv;
  int cnt = DecodeUInt(ptr, &uv);
  // ZigZag decoding, see https://github.com/stepchowfun/typical
  *v = (uv >> 1) ^ -(uv & 1);
  return cnt;
}

inline std::uint32_t EncodeString(const std::string_view value,
                                  WriteBuff* buff) {
  std::uint32_t bytes = EncodeUInt(value.size(), buff);
  buff->CopyBytes((std::uint8_t*)value.data(), value.size());
  return bytes + value.size();
}

inline std::uint32_t DecodeString(const std::uint8_t* ptr,
                                  std::string_view* value) {
  std::uint64_t value_size;
  std::uint64_t bytes = DecodeUInt(ptr, &value_size);
  *value = std::string_view((char*)(ptr + bytes), value_size);
  return bytes + value_size;
}

namespace {
constexpr std::uint32_t max_way_node_hist = 8;
#define HIST_DEFAULT                                           \
  {0000000000ull, 0000000000ull, 1500000000ull, 3000000000ull, \
   4500000000ull, 6000000000ull, 7500000000ull, 9000000000ull};

// Find the index of the closest hist entry to 'value'.
// Note: The found entry can be smaller or larger than 'value'.
void FindClosestHist(const std::uint64_t* hist, const std::uint64_t value,
                     unsigned int* found_idx, std::uint64_t* found_delta) {
  *found_delta = hist[0] > value ? hist[0] - value : value - hist[0];
  *found_idx = 0;
  for (unsigned int i = 1; i < max_way_node_hist; ++i) {
    std::uint64_t delta = hist[i] > value ? hist[i] - value : value - hist[i];
    if (delta < *found_delta) {
      *found_delta = delta;
      *found_idx = i;
    }
  }
}

}  // namespace

// Encode way nodes, using a compression window to take adavantage of similar
// but non-consecutive values. The maximal theoretical space needed is 10 + 11 *
// node_ids.size() bytes.
// Note: The number of nodes is not encoded, this has to be done separately if
// needed.
inline std::uint32_t EncodeNodeIds(const std::vector<uint64_t>& nodes,
                                   WriteBuff* buff) {
  CHECK_S(!nodes.empty());

  // Encode first id.
  std::uint64_t hist[max_way_node_hist] = HIST_DEFAULT;
  std::uint32_t cnt = EncodeUInt(nodes[0], buff);
  hist[0] = nodes[0];
  std::size_t hist_index = 1;

  for (unsigned int i = 1; i < nodes.size(); ++i) {
    // Encode ids 1..N, using values in 'hist' for delta compression.
    unsigned int found_hist_idx;
    std::uint64_t delta;
    FindClosestHist(hist, nodes[i], &found_hist_idx, &delta);
    const bool negative = hist[found_hist_idx] > nodes[i];

    if (delta < 15) {
      // Output one byte with bit-layout |negative:1|hist-ref:3|delta:4|.
      cnt +=
          buff->add_byte((negative ? 128 : 0) | (found_hist_idx << 4) | delta);
    } else {
      // Output one byte with bit-layout |negative:1|hist-ref:3|delta(=15):4|.
      cnt += buff->add_byte((negative ? 128 : 0) | (found_hist_idx << 4) | 15);
      // Output the real delta as uint.
      cnt += EncodeUInt(delta, buff);
    }

    // Update history:
    if (delta >= 4) {
      hist[hist_index++ % max_way_node_hist] = nodes[i];
    }
  }
  return cnt;
}

// Decode num_ids node ids from 'ptr'.
inline std::uint32_t DecodeNodeIds(const std::uint8_t* ptr,
                                   std::uint32_t num_ids,
                                   std::vector<uint64_t>* ids) {
  std::uint64_t hist[max_way_node_hist] = HIST_DEFAULT;
  CHECK_GT_S(num_ids, 0u);
  CHECK_S(ids->empty());

  // Decode first id.
  std::uint64_t v;
  std::uint32_t cnt = DecodeUInt(ptr, &v);
  ids->push_back(v);
  hist[0] = v;
  std::size_t hist_index = 1;

  for (unsigned int i = 1; i < num_ids; ++i) {
    bool negative = ptr[cnt] >= 128;
    unsigned int found_hist_idx = (ptr[cnt] >> 4) & 7;
    std::uint64_t delta = ptr[cnt] & 15;
    cnt += 1;

    if (delta == 15) {
      // delta encoded separately.
      cnt += DecodeUInt(ptr + cnt, &delta);
    }
    if (negative) {
      v = hist[found_hist_idx] - delta;
    } else {
      v = hist[found_hist_idx] + delta;
    }
    // Update history:
    if (delta >= 4) {
      hist[hist_index++ % max_way_node_hist] = v;
    }
    ids->push_back(v);
  }
  return cnt;
}
