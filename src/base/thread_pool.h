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
      // From now on if a worker finishes and there is no work left then it will
      // terminate the thread.
    }
    // Tell all threads to start running.
    cond_var_.notify_all();
    for (auto& thread : threads_) {
      // Wait until the thread finishes.
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
            // Work has started (tp.wait_all_finished_) but there is no more
            // work available, so finish the thread and make it ready to be
            // "joined" by the main thread.
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

/*
 * void worker(int id, SerializingLock& ser_lock) {
 *   .. parallel section ..
 *
 *   std::unique_lock<std::mutex> lock(ser_lock.mtx);
 *   ser_lock.wait_until_its_my_row(id, lock); // blocks; returns with lock held
 *   .. serialized section ..
 *   ser_lock.advance(lock);           // marks this row done, wakes next thread
 *
 *   .. parallel section ..
 * }
 */

// A serializing lock allows to serialize output when using a thread pool, but
// still do a lot of work in parallel.
//
// This for example can be used to serialize writing to a file. Each worker
// creates the data it wants to write in parallel. When it is done, it will use
// the serializing lock to wait writing the output until it is its turn.
//
// For this, each worker unit in the thread pool must know its 'serial number'
// 0,1,2,3...N and the ordering must correspond to how the worker units where
// added to the thread pool above.
//
// For a code example see above.
class SerializingLock {
 public:
  std::mutex mtx;

  // 'lock' must already be held (locked on mtx) when this is called.
  // Blocks until it's thread `id`'s turn; returns with 'lock' still held.
  void wait_until_its_my_row(int id, std::unique_lock<std::mutex>& lock) {
    cv_.wait(lock, [&] { return next_ == id; });
  }

  // Call this once you're done with your row, while still holding 'lock'.
  // Advances the turn, releases the lock, and wakes the waiting threads.
  void advance(std::unique_lock<std::mutex>& lock) {
    ++next_;
    lock.unlock();
    cv_.notify_all();
  }

 private:
  std::condition_variable cv_;
  int next_ = 0;
};

// Helper class to process data in an array/vector using a ThreadPool. The array
// is partitioned into contiguous chunks, which are processed be threads.
// ChunkDataT can be used to store data for each thread. For instance, it can be
// used to store in which thread a chunk was executed in.
template <typename ChunkDataT = int>
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
