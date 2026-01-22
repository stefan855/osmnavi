#pragma once

#include <fcntl.h>  // open, O_*
#include <stdio.h>
#include <sys/stat.h>  // mode constants
#include <unistd.h>    // write, lseek, close

#include <algorithm>
#include <bit>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "base/util.h"
#include "base/varbyte.h"
#include "graph/graph_def.h"

namespace {
// Check that we're on a little endian machine, which is true both for Intel and
// ARM. This is needed because we memory-map C++ POD (plain old data = "simple",
// C-style) structs into files and want to read them across platforms.
static_assert(std::endian::native == std::endian::little);

template <typename T>
size_t VectorDataSizeInBytes(const std::vector<T>& v) {
  return v.size() * sizeof(T);
}

void FileAbortOnError(const char* msg) {
  std::perror(msg);
  ABORT_S();
}

// Warning, as a side effect, this changes the write position to the end of the
// file.
size_t GetFileSize(int fd) {
  off_t offset = ::lseek(fd, 0, SEEK_END);
  if (offset < 0) FileAbortOnError("SEEK_END");
  CHECK_GE_S(offset, 0);
  return static_cast<size_t>(offset);
}

void WriteDataBuffer(int fd, const uint8_t* buff, size_t size) {
  size_t written = 0;
  while (written < size) {
    ssize_t ret = ::write(fd, buff + written, size - written);
    if (ret < 0) FileAbortOnError("write");
    written += static_cast<size_t>(ret);
  }
}

void AppendData(const std::string& name, int fd, const uint8_t* buff,
                size_t size, size_t align = 8) {
  off_t file_size_before = ::lseek(fd, 0, SEEK_END);
  if (file_size_before < 0) FileAbortOnError("SEEK_END");
  WriteDataBuffer(fd, buff, size);
  size_t mod = (size % align);
  if (mod > 0) {
    size_t padding = align - mod;
    std::vector<uint8_t> txt(padding, 0xFF);
    WriteDataBuffer(fd, txt.data(), padding);
  }
  LOG_S(INFO) << absl::StrFormat(
      "Bytes:%6llu pad:%1llu fsize:%llu %s", 
      size, (align - mod) % 8, GetFileSize(fd), name); 
}

void WriteDataTo(int fd, off_t pos, const uint8_t* buff, size_t size) {
  if (::lseek(fd, pos, SEEK_SET) == (off_t)-1) FileAbortOnError("SEEK_SET");
  WriteDataBuffer(fd, buff, size);
}

// Returns the bits needed to store the maximum value in data.
// Returns 0 if data is empty or all values are 0.
template <typename uint_type>
uint8_t DetermineBitWidth(const std::vector<uint_type>& data) {
  const auto iter = std::max_element(data.begin(), data.end());
  return static_cast<uint8_t>(std::bit_width(*iter));
}
}  // namespace

// Vector of elements of type 'T'. The actual data is stored in a blob starting
// at 'offset' and has 'num' elements.
template <typename T>
struct MMVec64 {
  uint64_t num__;
  // Data starts at base_ptr + offset__. Base pointer is the start of the memory
  // mapped file.
  uint64_t offset__;

  std::span<const T> span(const uint8_t* base_ptr) const {
    return std::span<const T>((const T*)(base_ptr + offset__), num__);
  }

  const T& at(const uint8_t* base_ptr, size_t pos) const {
    CHECK_LT_S(pos, num__);
    const T* data = (T*)(base_ptr + offset__);
    return data[pos];
  }

  uint64_t size() const { return num__; }
  uint64_t offset() const { return offset__; }

  // The (global) offset to the first byte after the end of the vector.
  uint64_t end_offset() const { return offset__ + num__ * sizeof(T); }

  void WriteDataBlob(const std::string& name, int fd, const std::vector<T>& v) {
    num__ = v.size();
    offset__ = GetFileSize(fd);
    CHECK_EQ_S(offset__ & 7, 0) << "not aligned:" << offset__;
    AppendData(name, fd, (const uint8_t*)v.data(), VectorDataSizeInBytes(v));
  }
};
CHECK_IS_POD(MMVec64<char>);

// Implements a fixed size, memory mapped vector of unsigned integers of a
// fixed bit width.
class MMCompressedUIntVec {
 public:
  // Get the value at position 'pos'.
  uint64_t at(const uint8_t* base_ptr, uint64_t pos) const {
    CHECK_LT_S(pos, num__);
    if (bit_width__ == 64) {
      // Handle 64 bits here because the formulas below wont work for it, for
      // instance "1ull << bit_width__".
      return ((uint64_t*)(base_ptr + offset__))[pos];
    }
    // Which global bit position does the int start at?
    const uint64_t bit_pos = (pos * bit_width__);
    // Points to the first uint64_t element that contains some of the bits.
    const uint64_t* const arr =
        ((uint64_t*)(base_ptr + offset__)) + (bit_pos / 64);
    // Start position within arr[0].
    const uint8_t bit_pos_mod = bit_pos % 64;
    uint64_t retval = (arr[0] >> bit_pos_mod) & low_mask(bit_width__);
    const uint8_t bits0 = 64 - bit_pos_mod;
    if (bits0 >= bit_width__) {
      // We have all bits.
      return retval;
    }
    // We need to get the remaining bits from start of next element.
    // Note that retval contains only needed bits, everything else is 0.
    uint8_t bits1 = bit_width__ - bits0;
    return retval + ((arr[1] & low_mask(bits1)) << bits0);
  }

  uint64_t size() const { return num__; }
  uint64_t offset() const { return offset__; }
  uint32_t bit_width() const { return bit_width__; }

  template <typename uint_type>
  void WriteDataBlob(const std::string& name, int fd,
                     const std::vector<uint_type>& data) {
    num__ = data.size();
    bit_width__ = DetermineBitWidth(data);
    offset__ = 0;  // temporary, while filling in data.

    uint8_t* buff = static_cast<std::uint8_t*>(calloc(1, NumDataBytes()));
    for (size_t pos = 0; pos < data.size(); ++pos) {
      // offset__ has to be 0 to write directly into 'buff'.
      Set(buff, pos, data.at(pos));
    }
    offset__ = GetFileSize(fd);
    CHECK_EQ_S(offset__ & 7, 0) << "not aligned:" << offset__;
    AppendData(name + absl::StrCat(" bitwidth:", bit_width__), fd, buff,
               NumDataBytes());
    free(buff);
  }

  uint64_t num__ : 56;
  uint64_t bit_width__ : 8;
  // Offset into the memory area that stores the vector. For files this is the
  // offset from the beginning of the file.
  uint64_t offset__;

 private:
  // Set the value at position 'pos'.
  void Set(uint8_t* base_ptr, uint64_t pos, uint64_t value) {
    CHECK_LT_S(pos, num__);
    if (bit_width__ == 64) {
      // Handle 64 bits here because the formulas below wont work for it, for
      // instance "1ull << bit_width__".
      ((uint64_t*)(base_ptr + offset__))[pos] = value;
    }
    // Which global bit position does the int start at?
    const uint64_t bit_pos = (pos * bit_width__);
    // arr[0] points to the first element that contains some of the bits.
    uint64_t* const arr = ((uint64_t*)(base_ptr + offset__)) + (bit_pos / 64);
    // Start position within arr[0].
    const uint8_t bit_pos_mod = bit_pos % 64;
    const uint8_t bits0 = 64 - bit_pos_mod;
    if (bits0 >= bit_width__) {
      // All bits in arr[0].
      arr[0] = (arr[0] & ~(low_mask(bit_width__) << bit_pos_mod)) |
               (value << bit_pos_mod);
    } else {
      // We need to set bits0 bits at the upper end of arr[0] and bits1 bits at
      // the lower end of arr[1].
      const uint8_t bits1 = bit_width__ - bits0;
      // LOG_S(INFO) << "bits0:" << (int)bits0 << " bits1:" << (int)bits1;
      arr[0] =
          (arr[0] & ~(low_mask(bits0) << bit_pos_mod)) | (value << bit_pos_mod);
      arr[1] = (arr[1] & ~low_mask(bits1)) | (value >> bits0);
    }
  }

  // The size of the data allocated at 'offset'. Note that this is always a
  // multiple of sizeof(uint64_t), i.e. 8.
  size_t NumDataBytes() const {
    return bit_width__ == 0 ? sizeof(uint64_t)
                            : ((num__ * bit_width__ + 63ull) / 64ull) * 8ull;
  }

 private:
  static uint64_t low_mask(uint8_t bits) { return (1ull << bits) - 1; }
};
CHECK_IS_POD(MMCompressedUIntVec);

struct MMTurnCostsTable {
  MMVec64<std::uint8_t> arr__;

  std::span<const uint8_t> at(const uint8_t* base_ptr, uint32_t pos) const {
    const uint8_t* ptr = &arr__.at(base_ptr, pos);
    return std::span<const uint8_t>(ptr + 1, *ptr);
  }

  std::vector<uint32_t> WriteDataBlob(
      const std::string& name, int fd,
      const std::vector<TurnCostData>& turn_costs) {
    std::vector<uint32_t> idx_to_pos;
    std::vector<uint8_t> arr;

    for (const TurnCostData& tcd : turn_costs) {
      idx_to_pos.push_back(arr.size());
      CHECK_LT_S(tcd.turn_costs.size(), 256);
      arr.push_back(tcd.turn_costs.size());
      for (uint8_t tc : tcd.turn_costs) {
        arr.push_back(tc);
      }
    }

    arr__.WriteDataBlob(name, fd, arr);
    return idx_to_pos;
  }
};

struct MMStringsTable {
  MMVec64<char> arr__;

  std::string_view at(const uint8_t* base_ptr, uint32_t pos) const {
    const char* ptr = &arr__.at(base_ptr, pos);
    const size_t len = strlen(ptr);
    return std::string_view(ptr, len);
  }

  std::vector<uint32_t> WriteDataBlob(const std::string& name, int fd,
                                      const std::vector<std::string>& strings) {
    std::vector<uint32_t> idx_to_pos;
    std::vector<char> arr;

    for (const std::string& str : strings) {
      idx_to_pos.push_back(arr.size());
      // Checkfail if the string contains any zeroes.
      CHECK_EQ_S(str.find('\0'), str.npos) << str;
      // Store the string as a null terminated string.
      for (char ch : str) {
        arr.push_back(ch);
      }
      arr.push_back('\0');
    }

    arr__.WriteDataBlob(name, fd, arr);
    return idx_to_pos;
  }
};

#if 0
struct MMComplexTurnRestriction {
  struct Spec {
    uint32_t start_pos;
    uint16_t len;
    bool forbidden;
  };

  MMVec64<Spec> string_specs__;
  MMVec64<char> chars__;

  std::string_view at(const uint8_t* base_ptr, uint32_t idx) {
    const StringSpec& spec = string_specs__.at(base_ptr, idx);
    return std::string_view(&(chars__.at(base_ptr, spec.start_pos)), spec.len);
  }

  void WriteDataBlob(const std::string& name, int fd,
                     const std::vector<std::string>& strings) {
    std::vector<StringSpec> specs;
    std::vector<char> arr;
    for (const std::string& str : strings) {
      specs.push_back({.start_pos = static_cast<uint32_t>(arr.size()),
                       .len = static_cast<uint32_t>(str.size())});
      for (size_t i = 0; i < str.size(); ++i) {
        arr.push_back(str.at(i));
      }
    }
    CHECK_LT_S(arr.size(), std::numeric_limits<uint32_t>::max());
    string_specs__.WriteDataBlob(name + "-specs", fd, specs);
    chars__.WriteDataBlob(name + "-chars", fd, arr);
  }
};
#endif

// A vector that stores OSM ids. It favors low memory consumption over access
// speed.
// Each record in the vector stores kOSMIdsGroupSize delta encoded ids,
// except for the last record which might have less.
// The delta encoded ids are stored in a contiguous area of memory (see
// id_blob_start()) after the end of the regular vector.
// Memory Layout:
//   |MMVec64|num__|MMVec64-data|id-blob|
constexpr size_t kOSMIdsGroupSize = 64;
// constexpr size_t kOSMIdsIdBits = 40;
// constexpr size_t kOSMIdsBlobOffsetBits = 24;
struct MMGroupedOSMIds {
  // TODO: This could be stored as two compressed vectors.
  struct IdGroup {
    int64_t first_id;
    uint64_t blob_offset;
    // int64_t first_id : kOSMIdsIdBits;
    // uint64_t blob_offset : kOSMIdsBlobOffsetBits;
  };

  MMVec64<IdGroup> mmgroups;

  // Number of Ids stored in total.
  uint32_t num__;

  int64_t at(const uint8_t* base_ptr, uint32_t pos) const {
    size_t gidx = pos / kOSMIdsGroupSize;
    const IdGroup& group = mmgroups.at(base_ptr, gidx);
    int64_t prev_id = group.first_id;
    uint32_t skip = pos % kOSMIdsGroupSize;
    uint32_t cnt = 0;
    const uint8_t* ptr = base_ptr + (mmgroups.end_offset() + group.blob_offset);
    while (skip > 0) {
      skip--;
      int64_t id;
      cnt += DeltaDecodeInt64(ptr + cnt, prev_id, &id);
      prev_id = id;
    }
    return prev_id;
  }

  // The number of ids.
  uint64_t size() const { return num__; }

  // The blob that contains the delta encoded ids start at blob_start().
  // IdGroup.blob_offset has to be added to it.
  uint64_t id_blob_start() const { return mmgroups.end_offset(); }

  // Initialise the memory mapped vector with the data from 'ids'.
  void WriteDataBlob(const std::string& name, int fd,
                     const std::vector<int64_t>& ids) {
    num__ = ids.size();

    // Create the groups vector and save the delta encrypted ids in 'buff'.
    uint32_t num_groups = (num__ + kOSMIdsGroupSize - 1) / kOSMIdsGroupSize;
    std::vector<MMGroupedOSMIds::IdGroup> groups(num_groups);
    WriteBuff buff;
    for (uint32_t gidx = 0; gidx < groups.size(); ++gidx) {
      MMGroupedOSMIds::IdGroup& g = groups.at(gidx);
      uint32_t first_pos = gidx * kOSMIdsGroupSize;
      int64_t prev_id = ids.at(first_pos);
      // CHECK_LT_S(std::llabs(prev_id), 1ll << (kOSMIdsIdBits - 1));
      // CHECK_LT_S(buff.used(), 1ull << kOSMIdsBlobOffsetBits);
      g.first_id = prev_id;
      g.blob_offset = buff.used();
      for (uint32_t pos = first_pos + 1;
           pos < std::min(first_pos + kOSMIdsGroupSize, ids.size()); ++pos) {
        // Push delta.
        int64_t id = ids.at(pos);
        DeltaEncodeInt64(prev_id, id, &buff);
        prev_id = id;
      }
    }

    // Write groups vector.
    mmgroups.WriteDataBlob(name, fd, groups);
    // Write blob containing the deltas
    CHECK_EQ_S(id_blob_start(), GetFileSize(fd));
    AppendData(name + ":blob", fd, buff.base_ptr(), buff.used());
  }
};
CHECK_IS_POD(MMGroupedOSMIds);
