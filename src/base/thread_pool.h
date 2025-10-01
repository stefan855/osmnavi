#pragma once
#include <stdio.h>

#include <condition_variable>
#include <deque>
#include <future>
#include <mutex>

/*
  ** Code Example:
  ThreadPool pool;
  for (int i = 0; i < 4; ++i) {
    // See C++ lambda expressions on how to pass more parameters from context.
    pool.AddWork([i](int thread_idx) {
      usleep(i * 10000);
      std::cerr << "Work" << i << " Thread" << thread_idx << std::endl;
    });

  pool.Start(2);  // Start with two threads.
  pool.WaitAllFinished();

  ** Output:
  Work0 Thread0
  Work1 Thread1
  Work2 Thread0
  Work3 Thread1
*/

class ThreadPool {
 public:
  // A function that does a piece of work. It has one argument (the thread
  // index 0..n_threads), and no return value. Additional arguments may be
  // stored in the function object, for instance by using a lambda expression.
  using TWorkerFunc = std::function<void(int thread_idx)>;

  ThreadPool() : wait_all_finished_(false) {}

  ~ThreadPool() {
    // Need to call WaitAllFinished() before destroying ThreadPool.
    assert(threads_.empty() || wait_all_finished_);
  }

  // Add one piece of work.
  // Note that work will be started when Start() is called. Can only be called
  // before Start().
  void AddWork(TWorkerFunc worker_func) {
    assert(threads_.empty());
    {
      std::unique_lock<std::mutex> l(mutex_);
      queue_.push_back(worker_func);
    }
    cond_var_.notify_one();
  }

  // Start work with 'n_threads' threads.
  // Note that it is not allowed to call AddWork() after Start().
  void Start(int n_threads) {
    assert(threads_.empty());
    assert(n_threads > 0);
    for (int i = 0; i < n_threads; ++i) {
      threads_.emplace_back(worker_loop, std::ref(*this), i);
    }
  }

  void WaitAllFinished() {
    assert(!threads_.empty());  // Start() has been called.
    {
      std::unique_lock<std::mutex> l(mutex_);
      wait_all_finished_ = true;
    }
    cond_var_.notify_all();
    for (auto& thread : threads_) {
      thread.join();
    }
  }

 private:
  bool wait_all_finished_;
  std::mutex mutex_;
  std::condition_variable cond_var_;
  std::vector<std::thread> threads_;
  std::deque<TWorkerFunc> queue_;

  static void worker_loop(ThreadPool& tp, int thread_idx) {
    while (true) {
      TWorkerFunc worker_func;
      {
        std::unique_lock<std::mutex> l(tp.mutex_);
        if (tp.queue_.empty()) {
          if (tp.wait_all_finished_) {
            return;
          }
          tp.cond_var_.wait(
              l, [&] { return tp.wait_all_finished_ || !tp.queue_.empty(); });
          if (tp.queue_.empty()) {
            if (tp.wait_all_finished_) {
              return;
            } else {
              assert(false);  // We never should get here.
            }
          }
        }
        worker_func = tp.queue_.front();
        tp.queue_.pop_front();
      }
      worker_func(thread_idx);
    }
  }
};

// Helper class to process data in an array/vector using a ThreadPool. The array
// is partitioned into contiguous chunks, which are processed be threads.
// ChunkDataT can be used to store data for each thread. For instance, it can be
// used to store in which thread a chunk was executed in.
template <typename ChunkDataT=int>
class ArrayChunker {
 public:
  struct Chunk {
    const size_t start;
    const size_t stop;
    ChunkDataT chunk_data;
  };

  ArrayChunker(size_t array_size, uint32_t chunk_size,
               ChunkDataT chunk_data_default = (int)-1)
      : chunk_size_(chunk_size) {
    chunks.reserve(array_size / chunk_size + 1);
    for (size_t i = 0; i < array_size; i += chunk_size) {
      chunks.emplace_back(i, std::min(array_size, i + chunk_size),
                          chunk_data_default);
    }
  }

  size_t ChunkNo(size_t pos) const { return pos / chunk_size_; }
  const Chunk& ChunkAt(size_t pos) const { return chunks.at(ChunkNo(pos)); }

  std::vector<Chunk> chunks;

 private:
  uint32_t chunk_size_;
};
