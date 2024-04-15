#pragma once

#include <stdio.h>

#include <cstdlib>

#include "base/util.h"

// Allocate memory from large blocks of memory and free all memory when the pool
// is destructed.
// 'alloc_unit' is the size of memory allocated internally from the heap.
// Ideally it is much larger than the largest memory allocation done with
// AllocBytes().
class SimpleMemPool {
 public:
  SimpleMemPool(std::uint64_t alloc_unit = 1ull << 24 /* 16 MB */)
      : current_(nullptr),
        alloc_unit_(alloc_unit),
        num_pieces_(0),
        total_used_(0) {}
  ~SimpleMemPool() {
    Piece* head = current_;
    while (head != nullptr) {
      Piece* b = head;
      head = b->next;
      free(b->buff);
      delete b;
    }
    current_ = nullptr;
  }

  // Allocate 'num' bytes from the pool and return a pointer to it.
  std::uint8_t* AllocBytes(const std::uint64_t num) {
    if (current_ == nullptr || current_->used + num > alloc_unit_) {
      if (num > alloc_unit_) {
        ABORT_S() << absl::StrFormat(
            "Requested bytes %llu more than allowed %llu", num, alloc_unit_);
      }
      num_pieces_++;
      Piece* b = new Piece;
      b->next = current_;
      b->used = 0;
      b->buff = static_cast<std::uint8_t*>(std::calloc(1, alloc_unit_));
      current_ = b;
    }
    std::uint8_t* const ptr = current_->buff + current_->used;
    current_->used += num;
    total_used_ += num;
    return ptr;
  }

  // Allocate 'num' bytes from the pool and return a pointer to it.
  // The returned pointer is aligned to a multiple of 8.
  std::uint64_t* Alloc64BitAligned(const std::uint64_t num) {
    if ((current_->used & 7) != 0) {
      current_->used += (8 - (current_->used & 7));
    }
    return (std::uint64_t*)AllocBytes(num);
  }

  std::uint64_t MemConsumed() const {
    return num_pieces_ * (alloc_unit_ + sizeof(Piece));
  }

  std::uint64_t MemAllocated() const { return total_used_; }

 private:
  struct Piece {
    std::uint8_t* buff;
    std::uint64_t used;
    Piece* next;
  };
  Piece* current_;
  const std::uint64_t alloc_unit_;
  std::uint64_t num_pieces_;
  std::uint64_t total_used_;
};
