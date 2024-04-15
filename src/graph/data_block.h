#pragma once
#include <stdio.h>

#include <cstdlib>

#include "base/simple_mem_pool.h"
#include "base/varbyte.h"
#include "graph/graph_def.h"

// Contains 'num_records' records, encoded into a buffer 'buff', using
// 'buff_size' bytes.
// The records are stored contiguously in 'buff' and are sorted by their unique
// uint64 id.
// Each record starts with a varuint-encoded delta id, the delta
// being relative to the previous record. The id of the first record is relative
// to 'start_id'.
// To find a record in the data block by id, the records in buff are scanned
// from the beginning until the record is found or the id becomes too large.
// After inspecting the id field (encoded as delta id, see above), the scanner
// uses a skip callback function to jump the next record in the buffer. This
// callback can be implemented in different ways, such as parsing the full
// record or using an existing size attribute to compute the position of the
// next record.
class DataBlockTable {
 public:
  struct DataBlock {
    std::int64_t start_id = 0;
    std::uint32_t num_records = 0;
    std::uint32_t buff_size = 0;
    std::uint8_t* buff = nullptr;
  };

  DataBlockTable(std::uint64_t alloc_unit = 1ull << 24 /* 16 MB */)
      : pool_(alloc_unit) {}

  void AddBlock(std::int64_t start_id, std::uint32_t num_records,
                const WriteBuff& buff) {
    std::uint8_t* target = pool_.AllocBytes(buff.used());
    std::memcpy(target, buff.base_ptr(), buff.used());
    blocks_.push_back({.start_id = start_id,
                       .num_records = num_records,
                       .buff_size = buff.used(),
                       .buff = target});
    total_records_ += num_records;
  }

  void Sort() {
    std::sort(blocks_.begin(), blocks_.end(),
              [](const DataBlock& a, const DataBlock& b) {
                return a.start_id < b.start_id;
              });
  }

  // Find the block that might contain the record with 'id'.
  // Returns nullptr if no block exists that might contain the record.
  const DataBlock* FindBlock(std::int64_t id) const {
    // 'std::upper_bound' returns the first element strictly greater.
    // We're interested in the element before the returned one.
    auto it = std::upper_bound(blocks_.begin(), blocks_.end(), id,
                               [](std::int64_t value, const DataBlock& b) {
                                 return value < b.start_id;
                               });
    if (it != blocks_.begin()) {
      return &(*(it - 1));
    }
    return nullptr;
  }

  const std::uint8_t* FindRecord(
      std::uint64_t id, std::uint32_t (*skip_func)(const std::uint8_t*)) const {
    const DataBlock* b = FindBlock(id);
    if (b == nullptr) {
      return nullptr;
    }
    std::uint64_t running_id = b->start_id;
    std::uint32_t cnt = 0;
    for (std::uint32_t pos = 0; pos < b->num_records; ++pos) {
      std::uint64_t delta;
      cnt += DecodeUInt(b->buff + cnt, &delta);
      running_id += delta;
      if (running_id < id) {
        cnt += skip_func(b->buff + cnt);
        continue;
      }
      if (running_id == id) return b->buff + cnt;
      break;  // running_id > id, i.e.not found.
    }
    return nullptr;
  }

  void PrintStruct() {
    for (unsigned int i = 0; i < blocks_.size(); ++i) {
      LOG_S(INFO) << absl::StrFormat("block %u start_id %lld num:%u", i,
                                     blocks_.at(i).start_id,
                                     blocks_.at(i).num_records);
    }
  }

  std::uint64_t total_records() const { return total_records_; }
  std::uint64_t mem_allocated() const {
    return pool_.MemAllocated() + blocks_.size() * sizeof(DataBlock);
  }
  const std::vector<DataBlock>& GetBlocks() const { return blocks_; }

 private:
  std::vector<DataBlock> blocks_;
  SimpleMemPool pool_;
  std::uint64_t total_records_ = 0;
};

class NodeBuilder {
 public:
  struct VNode {
    std::uint64_t id;
    std::int64_t lat;
    std::int64_t lon;
  };

 private:
  struct InternalBlockIter {
    const DataBlockTable::DataBlock* b_ = nullptr;
    std::uint32_t pos_;
    std::uint32_t cnt_;
    VNode n_;
    void Start(const DataBlockTable::DataBlock* block) {
      assert(block != nullptr);
      b_ = block;
      pos_ = 0;
      cnt_ = 0;
      n_.id = b_->start_id;
      n_.lat = 0;
      n_.lon = 0;
    }
    bool Next() {
      if (b_ == nullptr || pos_ >= b_->num_records) {
        return false;
      }
      std::uint64_t udelta;
      std::int64_t idelta;
      cnt_ += DecodeUInt(b_->buff + cnt_, &udelta);
      n_.id += udelta;
      cnt_ += DecodeInt(b_->buff + cnt_, &idelta);
      n_.lat += idelta;
      cnt_ += DecodeInt(b_->buff + cnt_, &idelta);
      n_.lon += idelta;
      ++pos_;
      return true;
    }
  };

 public:
  void AddNode(const VNode& node) {
    if (num_records_ == 0) {
      start_id_ = node.id;
      prev_id_ = node.id;  // First emitted id should have delta 0.
      prev_lat_ = 0;
      prev_lon_ = 0;
    }
    assert(node.id >= prev_id_);
    EncodeUInt(node.id - prev_id_, &buff_);
    EncodeInt(node.lat - prev_lat_, &buff_);
    EncodeInt(node.lon - prev_lon_, &buff_);
    prev_id_ = node.id;
    prev_lat_ = node.lat;
    prev_lon_ = node.lon;
    num_records_++;
  }

  void AddBlockToTable(DataBlockTable* t) {
    if (num_records_ > 0) {
      t->AddBlock(start_id_, num_records_, buff_);
    }
    Clear();
  }

  std::uint32_t pending_nodes() const { return num_records_; }

  void Clear() {
    num_records_ = 0;
    buff_.clear();
  }

  const WriteBuff& GetBuff() { return buff_; }

  static bool FindNode(const DataBlockTable& t, const std::uint64_t id,
                       VNode* node) {
    const DataBlockTable::DataBlock* b = t.FindBlock(id);
    if (b == nullptr) {
      return false;
    }
    InternalBlockIter iter;
    iter.Start(b);
    while (iter.Next()) {
      if (iter.n_.id < id) {
        continue;
      } else if (iter.n_.id == id) {
        *node = iter.n_;
        return true;
      }
      return false;
    }
    return false;
  }

  class GlobalNodeIter {
   public:
    GlobalNodeIter() = delete;
    GlobalNodeIter(const DataBlockTable& t) : t_(t), block_index_(0) {}

    const VNode* Next() {
      if (internal_iter_.Next()) {
        return &internal_iter_.n_;
      }
      while (block_index_ < t_.GetBlocks().size()) {
        internal_iter_.Start(&t_.GetBlocks().at(block_index_));
        block_index_++;
        if (internal_iter_.Next()) {
          return &internal_iter_.n_;
        } else {
          // Should not happen, block has no entries.
          assert(false);
        }
      }
      return nullptr;
    }

   private:
    const DataBlockTable& t_;
    std::size_t block_index_;
    InternalBlockIter internal_iter_;
  };

 private:
  WriteBuff buff_;
  std::uint64_t start_id_;
  std::uint64_t prev_id_;
  std::int64_t prev_lat_;
  std::int64_t prev_lon_;
  std::uint32_t num_records_ = 0;
};
