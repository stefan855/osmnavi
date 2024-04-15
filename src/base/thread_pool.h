#pragma once
#include <stdio.h>

#include <condition_variable>
#include <deque>
#include <future>
#include <mutex>

/*
  Code Example.

  // Callback function that receives the unit of work
  void MyRealWorkerFunc(int thread_idx, int* unit) {
    // .... Do something with 'unit' ....
    usleep(10000);
  }

  void UseThreadPool() {
    std::vector<int> units;  // Data for each unit of work.
    {
      ThreadPool<int> pool(MyRealWorkerFunc);
      for (int i = 0; i < 1000; ++i) {
        units.push_back(i);
      }
      for (std::size_t i = 0; i < units.size(); ++i) {
        pool.AddWorkUnit(&units[i]);
      }
      pool.Start(20);
      pool.WaitAllFinished();
    }
  }
*/

template <typename TWorkUnit>
class ThreadPool {
 public:
  // worker_func is the callback functuion for each unit of work.
  // It is called with parameters
  //   thread_idx: Identifies the thread, i.e. in the range [0..n_threads-1]
  //   worker_unit*: A pointer to the data for this work unit.
  ThreadPool(std::function<void(int, TWorkUnit)> worker_func)
      : worker_func_(worker_func), wait_all_finished_(false) {}

  ~ThreadPool() {
    // Need to call WaitAllFinished() before destroying ThreadPool.
    assert(wait_all_finished_ == true);
  }

  void AddWorkUnit(TWorkUnit work_unit) {
    {
      std::unique_lock<std::mutex> l(mutex_);
      queue_.push_back(work_unit);
    }
    cond_var_.notify_one();
  }

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
  const std::function<void(int, TWorkUnit)> worker_func_;
  bool wait_all_finished_;
  std::mutex mutex_;
  std::condition_variable cond_var_;
  std::vector<std::thread> threads_;
  std::deque<TWorkUnit> queue_;

  static void worker_loop(ThreadPool& tp, int thread_idx) {
    while (true) {
      TWorkUnit work_unit;
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
        work_unit = tp.queue_.front();
        tp.queue_.pop_front();
      }
      tp.worker_func_(thread_idx, work_unit);
    }
  }
};
