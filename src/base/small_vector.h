#pragma once

template <typename T, uint32_t num_internal>
class SmallVector {
 public:
  SmallVector() = delete;

  SmallVector(uint32_t size)
      : arr_(size > num_internal ? (T*)malloc(size * sizeof(T)) : small_),
        size_(size) {
    assert(size < 256);
  }

  SmallVector(uint32_t size, const T& dflt) : SmallVector(size) {
    assign(size, dflt);
  }

  SmallVector(const std::vector<T>& data) : SmallVector(data.size()) {
    assign(data);
  }

  // Copy constructor.
  SmallVector(const SmallVector<T, num_internal>& other)
      : SmallVector(other.size()) {
    memcpy(arr_, other.arr_, size() * sizeof(T));
    // LOG_S(INFO) << "CC";
  }

  ~SmallVector() {
    if (!internal()) {
      free(arr_);
    }
  }

  uint32_t size() const { return size_; }

  T& at(uint32_t pos) {
    CHECK_LT_S(pos, size());
    return arr_[pos];
  }

  const T& at(uint32_t pos) const {
    CHECK_LT_S(pos, size());
    return arr_[pos];
  }

  void assign(uint32_t num, const T& dflt) {
    for (uint32_t pos = 0; pos < num; ++pos) {
      at(pos) = dflt;
    }
  }

  void assign(const std::vector<T>& data) {
    for (uint32_t pos = 0; pos < size(); ++pos) {
      at(pos) = data.at(pos);
    }
  }

  bool operator==(const SmallVector<T, num_internal>& other) const {
    if (size() != other.size()) return false;
    for (uint32_t pos = 0; pos < size(); ++pos) {
      if (at(pos) != other.at(pos)) return false;
    }
    return true;
  }

  const char* data() const { return (const char*)arr_; }

  bool internal() const { return size_ <= num_internal; }

 private:
  T* arr_;
  const uint8_t size_;
  T small_[num_internal];
};
