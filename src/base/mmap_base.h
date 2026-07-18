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
#include "base/deg_coord.h"
#include "base/encode_coords.h"
#include "base/util.h"
#include "base/varbyte.h"
#include "graph/graph_def.h"

#define ABS_BLOB_PTR(struct_ptr, rel_blob_off) \
  (((const uint8_t*)struct_ptr) + rel_blob_off)

// Check that we're on a little endian machine, which is true both for Intel and
// ARM. This is needed because we memory-map C++ POD (plain old data = "simple",
// C-style) structs into files and want to read them across platforms.
static_assert(std::endian::native == std::endian::little);

#if 0
struct LatLon {
  LatE6 lat;
  LonE6 lon;
};
#endif

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

// Vector of elements of type 'T'. The actual data is stored in a blob starting
// at 'relative_blob_offset__' and has 'num' elements.
template <typename T>
class MMVec64 {
 public:
  // A view on the vector data, allowing iteration.
  std::span<const T> span() const {
    if (num__ > 0) {
      return std::span<const T>(&at(0), num__);
    } else {
      return std::span<const T>();  // empty span.
    }
  }

  // Access to a single element, check-fails when pos is out of bounds.
  const T& at(size_t pos) const {
    CHECK_LT_S(pos, num__);
    const T* data = (T*)ABS_BLOB_PTR(this, relative_blob_offset__);
    return data[pos];
  }

  uint64_t size() const { return num__; }
  uint64_t num_data_bytes() const { return num__ * sizeof(T); };

 private:
  friend void TestMMVec64();

  // Number of elements in the vector.
  uint64_t num__;
  // Data starts at this + relative_blob_offset__
  int64_t relative_blob_offset__;

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
  uint64_t num_data_bytes() const {
    return ((num__ + 63) / 64) * sizeof(uint64_t);
  };

 private:
  friend void TestMMBitset();

  // Number of bits.
  uint64_t num__;
  // Data starts at this + relative_blob_offset__
  int64_t relative_blob_offset__;

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

namespace {
// Returns the bits needed to store the maximum value in data.
// Returns 0 if data is empty or all values are 0.
template <typename uint_type>
uint8_t DetermineBitWidth(const std::vector<uint_type>& data) {
  const auto iter = std::max_element(data.begin(), data.end());
  return static_cast<uint8_t>(std::bit_width(*iter));
}
}  // namespace

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

  // The size of data blob in bytes. Note that this is always a multiple of
  // sizeof(uint64_t), i.e. 8.
  size_t num_data_bytes() const {
    return bit_width__ == 0 ? sizeof(uint64_t)
                            : ((num__ * bit_width__ + 63ull) / 64ull) * 8ull;
  }

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

// Each entry in the turn cost table is an array of turn costs, one for each
// outgoing edge of the node referencing the entry. The complete turn cost table
// is stored in a contiguous array of uint8_t. Each individual entry is stored
// as "<len><turn cost_1>...<turn_cost_n>". WriteDataBlob() return a vector that
// for each entry contains the start position in the array.
class MMTurnCostsTable {
 public:
  std::span<const uint8_t> at(uint32_t pos) const {
    const uint8_t* ptr = &arr__.at(pos);
    // ptr points to the number of elements.
    // ptr + 1 points to the start of the array.
    return std::span<const uint8_t>(ptr + 1, *ptr);
  }

  uint64_t num_data_bytes() const { return arr__.num_data_bytes(); }
  // Number of unique turn cost tables.
  uint64_t size() const { return num_entries_; }

 private:
  MMVec64<std::uint8_t> arr__;
  uint64_t num_entries_;  // Number of turn costs tables encoded in arr__.
  DISALLOW_COPY_ASSIGN_MOVE(MMTurnCostsTable);

 public:
  MMTurnCostsTable() : arr__(), num_entries_(0){};

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
    num_entries_ = idx_to_pos.size();
    return idx_to_pos;
  }
};
CHECK_IS_MM_OK(MMTurnCostsTable);

// All strings are stored as in one contiguous array of characters, using 0 to
// terminate a strings. WriteDataBlob() returns a vector containing the starting
// position for every string.
class MMStringsTable {
 public:
  std::string_view at(uint32_t pos) const {
    const char* ptr = &arr__.at(pos);
    const size_t len = strlen(ptr);
    return std::string_view(ptr, len);
  }

  uint64_t num_data_bytes() const { return arr__.num_data_bytes(); }
  // Number of unique strings.
  uint64_t size() const { return num_entries_; }

 private:
  MMVec64<char> arr__;
  uint64_t num_entries_;  // Number of strings encoded in arr__.
  DISALLOW_COPY_ASSIGN_MOVE(MMStringsTable);

 public:
  MMStringsTable() : arr__(), num_entries_(0){};

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
    num_entries_ = idx_to_pos.size();
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
  uint64_t num_data_bytes() const {
    return mmgroups__.num_data_bytes() + blob_size_in_bytes__;
  }

 private:
  struct IdGroup {
    int64_t first_id;
    // Offset from the start of MMGroupedOSMIds object to the start of the
    // data for this group.
    int64_t relative_blob_offset__;
  };

  // Number of Ids stored in total.
  uint32_t num__;
  uint32_t blob_size_in_bytes__;
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
    blob_size_in_bytes__ = buff.used();

    // Write groups vector.
    mmgroups__.WriteDataBlob(
        name, global_object_offset + offsetof(MMGroupedOSMIds, mmgroups__), fd,
        groups);
  }
};
CHECK_IS_MM_OK(MMGroupedOSMIds);

// constexpr size_t kShapeCoordsGroupSize = 64;
struct MMShapeCoords {
 public:
  struct Result {
    std::vector<LatLon> latlon;
    bool use_reverse_edge;  // When true then search at reverse edge.
  };

  // Gets the list of shape coordinates (excluding start and end node) for
  // edge 'edge_idx'.
  // 'base':       The latlon of the start node of the edge 'edge_idx'.
  // 'edge_idx':   Edge we want to get the shape coordinates from.
  // 'res':        Result of the operation.
  // If res->use_reverse_edge is true, then the latlon from the reverse edge has
  // to be retrieved with an additional call.
  // Otherwise, res->latlon is either empty or contains the list of shadow
  // coordinates without start/end nodes.
  //
  // Note: Use MMCluster::get_shape_coords() instead of the functions defined
  // here, it has a much simpler API.
  void get(const LatLon base, uint32_t edge_idx, Result* res) const {
    const CoordGroup& group = GetGroup(edge_idx);
    res->latlon.clear();
    {
      const uint8_t hval = group.GetHeaderVal(edge_idx % kShapeCoordsGroupSize);
      res->use_reverse_edge = (hval == CoordGroup::STORED_AT_REVERSE_EDGE);
      if (res->use_reverse_edge || hval == 0) {
        return;
      }
    }

    // Skip stuff.
    uint32_t cnt = 0;
    const uint8_t* ptr = ABS_BLOB_PTR(this, group.relative_blob_offset__);
    uint32_t skip = edge_idx % kShapeCoordsGroupSize;
    for (uint32_t off = 0; off < skip; ++off) {
      uint8_t header = group.GetHeaderVal(off);
      if (header == 0 || header == CoordGroup::STORED_AT_REVERSE_EDGE) {
        continue;
      }
      uint32_t num_coords = header;
      if (header == CoordGroup::LENGTH_GREATER_EQUAL_14) {
        cnt += DecodeUInt(ptr + cnt, &num_coords);
        num_coords += 14;
      }
      cnt += DecodeShapeCoords(ptr + cnt, num_coords, {LatE6(0), LonE6(0)},
                               &res->latlon);
    }

    // Now read the data.
    const uint8_t hval = group.GetHeaderVal(edge_idx % kShapeCoordsGroupSize);
    CHECK_S(hval != 0 && hval != CoordGroup::STORED_AT_REVERSE_EDGE) << hval;
    uint32_t num_coords = hval;
    if (hval == CoordGroup::LENGTH_GREATER_EQUAL_14) {
      cnt += DecodeUInt(ptr + cnt, &num_coords);
      num_coords += 14;
    }
    DecodeShapeCoords(ptr + cnt, num_coords, base, &res->latlon);
  }

  // Check if an entry has an empty shape coord list and return true if so,
  // false if the list is non-empty.
  // 'use_reverse_edge': Tells you to retrieve the shape coords from the reverse
  // edge.
  //
  // Note: Use MMCluster::get_shape_coords() instead of the functions defined
  // here, it has a much simpler API.
  bool is_empty(uint32_t edge_idx, bool* use_reverse_edge) const {
    const CoordGroup& group = GetGroup(edge_idx);
    const uint8_t hval = group.GetHeaderVal(edge_idx % kShapeCoordsGroupSize);
    *use_reverse_edge = (hval == CoordGroup::STORED_AT_REVERSE_EDGE);
    return (hval == 0 || hval == CoordGroup::STORED_AT_REVERSE_EDGE);
  }

  bool has_coords(uint32_t edge_idx) const {
    const CoordGroup& group = GetGroup(edge_idx);
    const uint8_t hval = group.GetHeaderVal(edge_idx % kShapeCoordsGroupSize);
    return hval != 0;
  }

  // The number of ids.
  uint64_t size() const { return num__; }
  uint64_t num_data_bytes() const {
    return mmgroups__.num_data_bytes() + blob_size_in_bytes__;
  }

 private:
  static constexpr size_t kShapeCoordsGroupSize = 64;
  struct CoordGroup {
    // <kShapeCoordsGroupSize> x 4-bit.
    // For each edge we store a 4-bit value.
    //   0-13: number of coordinate pairs (excluding start/end).
    //   14:   length > 13, stored in-stream.
    //   15:   coords stored at reverse edge.
    enum { LENGTH_GREATER_EQUAL_14 = 14, STORED_AT_REVERSE_EDGE = 15 };
    uint8_t header_arr[32] = {0};
    // Offset from the start of MMShapeCoords object to the start of the
    // data for this group.
    int64_t relative_blob_offset__;

    uint8_t GetHeaderVal(uint8_t offset) const {
      CHECK_LE_S(offset, kShapeCoordsGroupSize);
      CHECK_LT_S(offset / 2, sizeof(header_arr));
      uint8_t val = header_arr[offset / 2];
      if (offset & 1) {
        return val & 15u;
      } else {
        return val >> 4;
      }
    }

    // Set a value at 'offset', which is [0..kShapeCoordsGroupSize].
    // Each value is at most 4 bits.
    void SetHeaderVal(uint8_t offset, uint8_t v) {
      CHECK_LE_S(offset, kShapeCoordsGroupSize);
      CHECK_LE_S(v, 15u);
      uint32_t p = offset / 2;
      CHECK_LT_S(p, sizeof(header_arr));
      if (offset & 1) {
        // Set low bits;
        header_arr[p] = v | (header_arr[p] & ~(15u));
      } else {
        // Set high bits;
        header_arr[p] = (v << 4) | (header_arr[p] & ~(15u << 4));
      }
    }
  };

  const CoordGroup& GetGroup(uint32_t edge_idx) const {
    CHECK_LT_S(edge_idx, num__);
    size_t gidx = edge_idx / kShapeCoordsGroupSize;
    return mmgroups__.at(gidx);
  }

  // Number of entries that store information about a list of shape coords. This
  // equals the number of edges in the cluster.
  uint32_t num__;
  uint32_t blob_size_in_bytes__;
  MMVec64<CoordGroup> mmgroups__;
  DISALLOW_COPY_ASSIGN_MOVE(MMShapeCoords);

 public:
  MMShapeCoords() : mmgroups__(){};

  // 'length':           Dim=#edges. length of latlon span in 'latlon'. Includes
  //                     the start and end node of the edge.
  // 'use_reverse_edge': Dim=#edges. true if the shape coords ate the reverse
  //                      edge should be used. 'length' must be 0.
  // 'latlon':           Coordinates, consecutive spans of 'length' for each
  //                     edge.
  void WriteDataBlob(const std::string& name, int64_t global_object_offset,
                     int fd, const std::vector<uint16_t>& length,
                     const std::vector<bool>& use_reverse_edge,
                     const std::vector<LatLon>& latlon) {
    CHECK_EQ_S(length.size(), use_reverse_edge.size());

    // Create the groups vector.
    num__ = length.size();
    uint32_t num_groups =
        (num__ + kShapeCoordsGroupSize - 1) / kShapeCoordsGroupSize;
    std::vector<CoordGroup> groups(num_groups, {0});

    // Compute the encodings for each individual sequence of coordinates and put
    // the all in the same write buffer, remembering the relative start position
    // for each group.
    const int64_t abs_blobs_offset = GetFileSize(fd);
    WriteBuff buff;
    size_t coord_pos = 0;
    for (uint32_t gidx = 0; gidx < groups.size(); ++gidx) {
      CoordGroup& group = groups.at(gidx);
      uint32_t first_pos = gidx * kShapeCoordsGroupSize;
      group.relative_blob_offset__ =
          abs_blobs_offset + buff.used() - global_object_offset;
      for (uint32_t offset = 0; offset < kShapeCoordsGroupSize; ++offset) {
        const uint32_t pos = first_pos + offset;
        if (pos >= length.size()) {
          break;
        }
        if (use_reverse_edge.at(pos)) {
          CHECK_EQ_S(length.at(pos), 0);
          group.SetHeaderVal(offset, CoordGroup::STORED_AT_REVERSE_EDGE);
        } else if (length.at(pos) == 0) {
          group.SetHeaderVal(offset, 0);
        } else {
          CHECK_GT_S(length.at(pos), 2) << length.at(pos);
          // Length after removing start/end node.
          const uint16_t naked_length = length.at(pos) - 2;
          if (naked_length < 14) {
            group.SetHeaderVal(offset, naked_length);
          } else {
            CHECK_GE_S(naked_length, 14);
            group.SetHeaderVal(offset, CoordGroup::LENGTH_GREATER_EQUAL_14);
            EncodeUInt(naked_length - 14, &buff);  // store length in-stream.
          }
          EncodeShapeCoords(latlon.at(coord_pos),
                            std::span<const LatLon>(&latlon.at(coord_pos + 1),
                                                      naked_length),
                            &buff);
        }

        coord_pos += length.at(pos);
      }
    }
    CHECK_EQ_S(coord_pos, latlon.size());

    // Write blob containing coord deltas
    CHECK_EQ_S(abs_blobs_offset, GetFileSize(fd));
    AppendData(name + ":blob", fd, buff.base_ptr(), buff.used());
    blob_size_in_bytes__ = buff.used();

    // Write groups vector.
    mmgroups__.WriteDataBlob(
        name, global_object_offset + offsetof(MMShapeCoords, mmgroups__), fd,
        groups);
  }
};
CHECK_IS_MM_OK(MMShapeCoords);
