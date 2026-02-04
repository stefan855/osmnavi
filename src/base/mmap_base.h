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

#define ABS_BLOB_PTR(struct_ptr, rel_blob_off) \
  (((const uint8_t*)struct_ptr) + rel_blob_off)

// Check that we're on a little endian machine, which is true both for Intel and
// ARM. This is needed because we memory-map C++ POD (plain old data = "simple",
// C-style) structs into files and want to read them across platforms.
static_assert(std::endian::native == std::endian::little);

template <typename T>
size_t VectorDataSizeInBytes(const std::vector<T>& v) {
  return v.size() * sizeof(T);
}

inline void FileAbortOnError(const char* msg) {
  std::perror(msg);
  ABORT_S();
}

// Warning, as a side effect, this changes the write position to the end of the
// file.
inline size_t GetFileSize(int fd) {
  off_t offset = ::lseek(fd, 0, SEEK_END);
  if (offset < 0) FileAbortOnError("SEEK_END");
  CHECK_GE_S(offset, 0);
  return static_cast<size_t>(offset);
}

inline void WriteDataBuffer(int fd, const uint8_t* buff, size_t size) {
  size_t written = 0;
  while (written < size) {
    ssize_t ret = ::write(fd, buff + written, size - written);
    if (ret < 0) FileAbortOnError("write");
    written += static_cast<size_t>(ret);
  }
}

inline void AppendData(const std::string& name, int fd, const uint8_t* buff,
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
  LOG_S(INFO) << absl::StrFormat("Bytes:%6llu pad:%1llu fsize:%llu %s", size,
                                 (align - mod) % 8, GetFileSize(fd), name);
}

inline void WriteDataTo(int fd, off_t pos, const uint8_t* buff, size_t size) {
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

// Vector of elements of type 'T'. The actual data is stored in a blob starting
// at 'relative_blob_offset__' and has 'num' elements.
template <typename T>
class MMVec64 {
 public:
  // A view on the vector data, allowing iteration.
  std::span<const T> span() const { return std::span<const T>(&at(0), num__); }

  // Access to a single element, check-fails when pos is out of bounds.
  const T& at(size_t pos) const {
    CHECK_LT_S(pos, num__);
    const T* data = (T*)ABS_BLOB_PTR(this, relative_blob_offset__);
    return data[pos];
  }

  uint64_t size() const { return num__; }

 private:
  friend void TestMMVec64();

  // Number of elements in the vector.
  uint64_t num__;
  // Data starts at this + relative_blob_offset__
  int64_t relative_blob_offset__;

  uint64_t num_data_bytes() const { return num__ * sizeof(T); };

  DISALLOW_COPY_ASSIGN_MOVE(MMVec64);

 public:  // but only accessible during construction.
  MMVec64() : num__(0), relative_blob_offset__(0){};

  // Write a data blob to the end of the file 'fd'.
  //
  // 'global_object_offset': Global file offset in bytes of the object that this
  // method is called on. This is used to compute the offset of the appended
  // blob relative to the start of the object.
  uint64_t WriteDataBlob(const std::string& name, int64_t global_object_offset,
                         int fd, const std::vector<T>& v) {
    num__ = v.size();
    const int64_t abs_blob_offset = GetFileSize(fd);
    CHECK_EQ_S(abs_blob_offset & 7, 0) << "not aligned:" << abs_blob_offset;
    // By construction, this should be positive.
    relative_blob_offset__ = abs_blob_offset - global_object_offset;
    /// Check-fail if we can't correctly compute the size of the data array.
    CHECK_EQ_S(VectorDataSizeInBytes(v), num_data_bytes());
    AppendData(absl::StrCat(name, " reloff:", relative_blob_offset__), fd,
               (const uint8_t*)v.data(), num_data_bytes());
    return abs_blob_offset;
  }
};
CHECK_IS_MM_OK(MMVec64<char>);

class MMBitset {
 public:
  // Access to a single element, check-fails when pos is out of bounds.
  bool at(size_t pos) const {
    CHECK_LT_S(pos, num__);
    const uint64_t* data =
        (uint64_t*)ABS_BLOB_PTR(this, relative_blob_offset__);
    // LOG_S(INFO) << "XX:" << data[pos / 64];
    return (data[pos / 64] & (1ull << (pos % 64))) != 0;
  }
  uint64_t size() const { return num__; }

 private:
  friend void TestMMBitset();

  // Number of bits.
  uint64_t num__;
  // Data starts at this + relative_blob_offset__
  int64_t relative_blob_offset__;

  uint64_t num_data_bytes() const {
    return ((num__ + 63) / 64) * sizeof(uint64_t);
  };

  DISALLOW_COPY_ASSIGN_MOVE(MMBitset);

 public:  // but only accessible during construction.
  MMBitset() : num__(0), relative_blob_offset__(0){};

  // Write a data blob to the end of the file 'fd'.
  //
  // 'global_object_offset': Global file offset in bytes of the object that this
  // method is called on. This is used to compute the offset of the appended
  // blob relative to the start of the object.
  uint64_t WriteDataBlob(const std::string& name, int64_t global_object_offset,
                         int fd, const std::vector<bool>& v) {
    num__ = v.size();
    const int64_t abs_blob_offset = GetFileSize(fd);
    CHECK_EQ_S(abs_blob_offset & 7, 0) << "not aligned:" << abs_blob_offset;
    // By construction, this should be positive.
    relative_blob_offset__ = abs_blob_offset - global_object_offset;
    std::vector<uint64_t> bitset(((num__ + 63) / 64), 0ull);
    for (uint32_t pos = 0; pos < num__; ++pos) {
      if (v.at(pos)) {
        bitset.at(pos / 64) += (1ull << (pos % 64));
      }
    }
    AppendData(absl::StrCat(name, " reloff:", relative_blob_offset__), fd,
               (const uint8_t*)bitset.data(), num_data_bytes());
    return abs_blob_offset;
  }
};
CHECK_IS_MM_OK(MMBitset);

// Implements a fixed size, memory mapped vector of unsigned integers of a
// fixed bit width.
class MMCompressedUIntVec {
 public:
  // Get the value at position 'pos'.
  uint64_t at(uint64_t pos) const {
    CHECK_LT_S(pos, num__);
    const uint64_t* data =
        (uint64_t*)ABS_BLOB_PTR(this, relative_blob_offset__);
    if (bit_width__ != 64) {
      // Which global bit position does the int start at?
      const uint64_t bit_pos = (pos * bit_width__);
      // Points to the first uint64_t element that contains some of the bits.
      const uint64_t* const arr = data + (bit_pos / 64);
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
    } else {
      // Handle 64 bits here because the formulas won't work for it, for
      // instance "1ull << bit_width__".
      return (data)[pos];
    }
  }

  uint32_t size() const { return num__; }
  uint32_t bit_width() const { return bit_width__; }

 private:
  friend void TestMMCompressedUIntVec();

  uint32_t num__;
  uint16_t bit_width__;
  // Offset into the memory area that stores the vector. For files this is the
  // offset from the beginning of the file.
  uint64_t relative_blob_offset__;

  static uint64_t low_mask(uint8_t bits) { return (1ull << bits) - 1; }

  // Set the value in array 'arr_ptr' at position 'pos'.
  void Set(uint64_t* arr_ptr, uint64_t pos, uint64_t value) {
    CHECK_LT_S(pos, num__);
    if (bit_width__ == 64) {
      // Handle 64 bits here because the formulas below wont work for it, for
      // instance "1ull << bit_width__".
      (arr_ptr)[pos] = value;
      return;
    }
    // Which global bit position does the int start at?
    const uint64_t bit_pos = (pos * bit_width__);
    // arr[0] points to the first element that contains some of the bits.
    uint64_t* const arr = (arr_ptr) + (bit_pos / 64);
    // Start position within arr[0].
    const uint8_t bit_pos_mod = bit_pos % 64;
    const uint8_t bits0 = 64 - bit_pos_mod;
    if (bits0 >= bit_width__) {
      // All bits in arr[0].
      arr[0] = (arr[0] & ~(low_mask(bit_width__) << bit_pos_mod)) |
               (value << bit_pos_mod);
    } else {
      // We need to set bits0 bits at the upper end of arr[0] and bits1 bits
      // at the lower end of arr[1].
      const uint8_t bits1 = bit_width__ - bits0;
      // LOG_S(INFO) << "bits0:" << (int)bits0 << " bits1:" << (int)bits1;
      arr[0] =
          (arr[0] & ~(low_mask(bits0) << bit_pos_mod)) | (value << bit_pos_mod);
      arr[1] = (arr[1] & ~low_mask(bits1)) | (value >> bits0);
    }
  }

  // The size of data blob in bytes. Note that this is always a multiple of
  // sizeof(uint64_t), i.e. 8.
  size_t num_data_bytes() const {
    return bit_width__ == 0 ? sizeof(uint64_t)
                            : ((num__ * bit_width__ + 63ull) / 64ull) * 8ull;
  }

  DISALLOW_COPY_ASSIGN_MOVE(MMCompressedUIntVec);

 public:
  MMCompressedUIntVec() : num__(0), bit_width__(0), relative_blob_offset__(0){};

  // Write a data blob to the end of the file 'fd'.
  //
  // 'global_object_offset': Global file offset in bytes of the object that
  // this method is called on. This is used to compute the offset of the
  // appended blob relative to the start of the object.
  template <typename uint_type>
  void WriteDataBlob(const std::string& name, int64_t global_object_offset,
                     int fd, const std::vector<uint_type>& data) {
    CHECK_LT_S(data.size(), MAXU32);
    num__ = data.size();
    bit_width__ = DetermineBitWidth(data);
    CHECK_LE_S(bit_width__, 64);

    uint64_t* buff = static_cast<std::uint64_t*>(calloc(1, num_data_bytes()));
    for (size_t pos = 0; pos < data.size(); ++pos) {
      Set(buff, pos, data.at(pos));
    }
    const int64_t abs_blob_offset = GetFileSize(fd);
    CHECK_EQ_S(abs_blob_offset & 7, 0) << "not aligned:" << abs_blob_offset;
    relative_blob_offset__ = abs_blob_offset - global_object_offset;
    AppendData(absl::StrCat(name, " bitwidth:", bit_width__,
                            " reloff:", relative_blob_offset__),
               fd, (uint8_t*)buff, num_data_bytes());
    free(buff);
  }
};
CHECK_IS_MM_OK(MMCompressedUIntVec);

class MMTurnCostsTable {
 public:
  std::span<const uint8_t> at(uint32_t pos) const {
    const uint8_t* ptr = &arr__.at(pos);
    return std::span<const uint8_t>(ptr + 1, *ptr);
  }

 private:
  MMVec64<std::uint8_t> arr__;
  DISALLOW_COPY_ASSIGN_MOVE(MMTurnCostsTable);

 public:
  MMTurnCostsTable() : arr__(){};

  // Write a data blob to the end of the file 'fd'.
  //
  // 'global_object_offset': Global file offset in bytes of the object that
  // this method is called on. This is used to compute the offset of the
  // appended blob relative to the start of the object.
  std::vector<uint32_t> WriteDataBlob(
      const std::string& name, int64_t global_object_offset, int fd,
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

    arr__.WriteDataBlob(
        name, global_object_offset + offsetof(MMTurnCostsTable, arr__), fd,
        arr);
    return idx_to_pos;
  }
};
CHECK_IS_MM_OK(MMTurnCostsTable);

class MMStringsTable {
 public:
  std::string_view at(uint32_t pos) const {
    const char* ptr = &arr__.at(pos);
    const size_t len = strlen(ptr);
    return std::string_view(ptr, len);
  }

 private:
  MMVec64<char> arr__;
  DISALLOW_COPY_ASSIGN_MOVE(MMStringsTable);

 public:
  MMStringsTable() : arr__(){};

  // Write a data blob to the end of the file 'fd'.
  //
  // 'global_object_offset': Global file offset in bytes of the object that
  // this method is called on. This is used to compute the offset of the
  // appended blob relative to the start of the object.
  std::vector<uint32_t> WriteDataBlob(const std::string& name,
                                      int64_t global_object_offset, int fd,
                                      const std::vector<std::string>& strings) {
    std::vector<uint32_t> idx_to_pos;
    std::vector<char> arr;

    for (const std::string& str : strings) {
      idx_to_pos.push_back(arr.size());
      // Check-fail if the string contains any zeroes.
      CHECK_EQ_S(str.find('\0'), str.npos) << str;
      // Store the string as a null terminated string.
      for (char ch : str) {
        arr.push_back(ch);
      }
      arr.push_back('\0');
    }

    arr__.WriteDataBlob(
        name, global_object_offset + offsetof(MMStringsTable, arr__), fd, arr);
    return idx_to_pos;
  }
};
CHECK_IS_MM_OK(MMStringsTable);

// A vector that stores OSM ids. It favors low memory consumption over access
// speed.
// Each record in the vector stores kOSMIdsGroupSize delta encoded ids,
// except for the last record which might have less.
// The delta encoded ids are stored in a contiguous area of memory (see
// id_blob_start()) after the end of the regular vector.
constexpr size_t kOSMIdsGroupSize = 64;
struct MMGroupedOSMIds {
 public:
  int64_t at(uint32_t pos) const {
    CHECK_LT_S(pos, num__);
    size_t gidx = pos / kOSMIdsGroupSize;
    const IdGroup& group = mmgroups__.at(gidx);
    int64_t prev_id = group.first_id;
    uint32_t skip = pos % kOSMIdsGroupSize;
    uint32_t cnt = 0;
    const uint8_t* ptr = ABS_BLOB_PTR(this, group.relative_blob_offset__);
    // const uint8_t* ptr = base_ptr + (mmgroups__.end_offset() +
    // group.blob_offset);
    while (skip > 0) {
      skip--;
      int64_t id;
      cnt += DeltaDecodeInt64(ptr + cnt, prev_id, &id);
      prev_id = id;
    }
    return prev_id;
  }

  // Find the position of 'id' in the list of ids. Returns -1 of not found.
  // Warning: This is slow, that is O(#nodes).
  int64_t find_idx(int64_t id) const {
    for (uint32_t gidx = 0; gidx < mmgroups__.size(); ++gidx) {
      const IdGroup& group = mmgroups__.at(gidx);
      const uint8_t* ptr = ABS_BLOB_PTR(this, group.relative_blob_offset__);
      // Number of entries in the group, has special case for the last group.
      const uint32_t stop =
          (gidx + 1 < mmgroups__.size())
              ? kOSMIdsGroupSize
              : ((num__ % kOSMIdsGroupSize == 0) ? kOSMIdsGroupSize
                                                 : (num__ % kOSMIdsGroupSize));
      CHECK_GT_S(stop, 0);

      int64_t prev_id = group.first_id;
      uint32_t read_cnt = 0;
      uint32_t pos = 0;
      for (;;) {
        if (id == prev_id) {
          return gidx * kOSMIdsGroupSize + pos;
        }
        if (++pos == stop) {
          break;
        }
        int64_t id;
        read_cnt += DeltaDecodeInt64(ptr + read_cnt, prev_id, &id);
        prev_id = id;
      }
    }
    return -1;
  }

  // The number of ids.
  uint64_t size() const { return num__; }

 private:
  struct IdGroup {
    int64_t first_id;
    // Offset from the start of MMGroupedOSMIds object to the start of the
    // data for this group.
    int64_t relative_blob_offset__;
  };

  // Number of Ids stored in total.
  uint32_t num__;
  MMVec64<IdGroup> mmgroups__;
  DISALLOW_COPY_ASSIGN_MOVE(MMGroupedOSMIds);

 public:
  MMGroupedOSMIds() : mmgroups__(){};

  // Initialise the memory mapped vector with the data from 'ids'.
  // 'global_object_offset' if the global offset of the object this method is
  // called from.
  void WriteDataBlob(const std::string& name, int64_t global_object_offset,
                     int fd, const std::vector<int64_t>& ids) {
    num__ = ids.size();

    // Create the groups vector.
    uint32_t num_groups = (num__ + kOSMIdsGroupSize - 1) / kOSMIdsGroupSize;
    std::vector<MMGroupedOSMIds::IdGroup> groups(num_groups, {0});

    // Compute the delta encodings for each individual group and store the
    // deltas in one big buffer, keeping the start_id and offset for each
    // group in the mmgroups__ vector.
    WriteBuff buff;
    const int64_t abs_blobs_offset = GetFileSize(fd);
    for (uint32_t gidx = 0; gidx < groups.size(); ++gidx) {
      MMGroupedOSMIds::IdGroup& group = groups.at(gidx);
      uint32_t first_pos = gidx * kOSMIdsGroupSize;
      int64_t prev_id = ids.at(first_pos);
      // CHECK_LT_S(std::llabs(prev_id), 1ll << (kOSMIdsIdBits - 1));
      // CHECK_LT_S(buff.used(), 1ull << kOSMIdsBlobOffsetBits);
      group.first_id = prev_id;
      group.relative_blob_offset__ =
          abs_blobs_offset + buff.used() - global_object_offset;
      for (uint32_t pos = first_pos + 1;
           pos < std::min(first_pos + kOSMIdsGroupSize, ids.size()); ++pos) {
        // Push delta.
        int64_t id = ids.at(pos);
        DeltaEncodeInt64(prev_id, id, &buff);
        prev_id = id;
      }
    }

    // Write blob containing the deltas
    CHECK_EQ_S(abs_blobs_offset, GetFileSize(fd));
    AppendData(name + ":blob", fd, buff.base_ptr(), buff.used());

    // Write groups vector.
    mmgroups__.WriteDataBlob(
        name, global_object_offset + offsetof(MMGroupedOSMIds, mmgroups__), fd,
        groups);
  }
};
CHECK_IS_MM_OK(MMGroupedOSMIds);
