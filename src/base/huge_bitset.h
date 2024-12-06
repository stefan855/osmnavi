#pragma once
#include <stdio.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <vector>

// Implements a bitset for numbers >= 0.
// The bits are stored in blocks of memory, which are stored in a vector that
// grows as needed.
// The maximal bit number is 2^35-1, to help protect against bugs. It can be
// changed at construction time.
// TODO: use a hash set or something similar as long as it is more memory
// efficient.
class HugeBitset {
 public:
  // Create a bitset, allowing for bits 0..max_bit_no-1. Only memory that is
  // needed for positive bits is actually allocated.
  HugeBitset(std::uint64_t max_bit_no = 1ull << 35)
      : allocated_bits_(0ull), max_bit_no_(max_bit_no) {}
  ~HugeBitset() {
    for (std::uint64_t* block : blocks_) {
      free(block);
    }
  }

  bool SetBit(std::uint64_t bit_no, bool value) {
    make_available(bit_no);
    std::uint64_t* block = blocks_.at(bit_no / bits_per_block_);
    std::uint64_t* u64 = &block[(bit_no % bits_per_block_) / 64];
    std::uint64_t u64_mask = 1ull << (bit_no % 64);
    bool retval = ((*u64) & u64_mask) != 0;
    if (value) {
      // Set bit.
      *u64 = (*u64) | u64_mask;
    } else {
      *u64 = (*u64) & ~u64_mask;
    }
    return retval;
  }

  bool GetBit(std::uint64_t bit_no) const {
    if (bit_no >= allocated_bits_) {
      return false;
    }
    std::uint64_t* block = blocks_.at(bit_no / bits_per_block_);
    std::uint64_t u64 = block[(bit_no % bits_per_block_) / 64];
    std::uint64_t u64_mask = 1ull << (bit_no % 64);
    return (u64 & u64_mask) != 0;
  }

  void Clear() {
    for (std::uint64_t* ptr : blocks_) {
      memset(ptr, 0, block_size_);
    }
  }

  // Returns the next true bit >= start_no. Returns NumAllocatedBits() at the
  // end.
  std::uint64_t NextBit(std::uint64_t start_no) const {
    if (start_no >= allocated_bits_) {
      return allocated_bits_;
    }
    std::uint64_t* block = blocks_.at(start_no / bits_per_block_);
    std::uint64_t u64 = block[(start_no % bits_per_block_) / 64];
    // Is there a bit in the current 8 bytes, with position >= start_no?
    while (start_no % 64 != 0) {
      if (u64 & (1ull << (start_no % 64))) {
        return start_no;
      }
      ++start_no;
    }
    // 'start_no' is 64 bit aligned. Now loop and check every uint64, which is
    // faster.
    while (start_no < allocated_bits_) {
      block = blocks_.at(start_no / bits_per_block_);
      do {
        std::uint64_t u64 = block[(start_no % bits_per_block_) / 64];
        if (u64 != 0) {
          // return start_no + FindLeastBit(u64);
          // gcc only: return start_no + __builtin_ffsll(u64) - 1;
          while ((u64 & (1ull << (start_no % 64))) == 0) {
            start_no++;
          }
          return start_no;
        }
        start_no += 64;
      } while (start_no % bits_per_block_ !=
               0);  // Stop when new block is starts.
    }
    return allocated_bits_;
  }

  // Count the bits that are '1'.
  std::uint64_t CountBits() const {
    std::uint64_t cnt = 0;
    for (std::uint64_t i = NextBit(0); i < NumAllocatedBits();
         i = NextBit(i + 1ull)) {
      cnt++;
    }
    return cnt;
  }

  std::uint64_t NumAllocatedBits() const { return allocated_bits_; }
  std::uint64_t NumUsedBytes() const {
    return blocks_.size() * (sizeof(blocks_[0]) + block_size_);
  }

 private:
  void make_available(std::uint64_t bit) {
    // std::cout << "make available bit " << bit << std::endl;
    assert(bit <
           max_bit_no_);  // Fail if bit >= 32b, this is probably an error.
    while (bit >= allocated_bits_) {
      blocks_.push_back(
          static_cast<std::uint64_t*>(std::calloc(1, block_size_)));
      allocated_bits_ += bits_per_block_;
      // std::cout << "alloc block " << blocks_.size() - 1
      //           << " available bits:" << allocated_bits_ << std::endl;
    }
  }

  static constexpr std::uint64_t block_size_ = 1ull << 20;  // 1 MB
  static constexpr std::uint64_t bits_per_block_ = block_size_ * 8;

  std::vector<std::uint64_t*> blocks_;
  std::uint64_t allocated_bits_;
  const std::uint64_t max_bit_no_;
};
