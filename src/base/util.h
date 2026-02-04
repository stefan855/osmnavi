#pragma once

#include <malloc.h>

#include <charconv>
#include <filesystem>
#include <optional>
#include <regex>
#include <string>
#include <vector>

#include "absl/strings/str_format.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "logging/loguru.h"

#define CHECK_IS_POD(type_to_check)                             \
  static_assert(std::is_standard_layout<type_to_check>::value); \
  static_assert(std::is_trivial<type_to_check>::value);

#define CHECK_IS_MM_OK(type_to_check) \
  static_assert(std::is_standard_layout<type_to_check>::value);

// Disallow constructors that copy, assign or move the object.
#define DISALLOW_COPY_ASSIGN_MOVE(TypeName) \
  TypeName(const TypeName&);                \
  void operator=(const TypeName&);          \
  TypeName(TypeName&&)

enum class Verbosity : int {
  Quiet = 0,
  Brief = 1,
  Warning = 2,
  Verbose = 3,
  Debug = 4,
  Trace = 5
};

inline Verbosity ParseVerbosityFlag(std::string_view flag) {
  if (std::string_view("quiet").starts_with(flag)) {
    return Verbosity::Quiet;
  }
  if (std::string_view("brief").starts_with(flag)) {
    return Verbosity::Brief;
  }
  if (std::string_view("warning").starts_with(flag)) {
    return Verbosity::Warning;
  }
  if (std::string_view("verbose").starts_with(flag)) {
    return Verbosity::Verbose;
  }
  if (std::string_view("debug").starts_with(flag)) {
    return Verbosity::Debug;
  }
  if (std::string_view("trace").starts_with(flag)) {
    return Verbosity::Trace;
  }
  ABORT_S() << "Unknown verbosity value <" << flag << ">";
}

inline void InitLogging(int argc, char* argv[]) {
  loguru::g_preamble_date = false;
  loguru::g_preamble_uptime = false;
  loguru::g_preamble_thread = false;
  loguru::init(argc, argv);
}

// Compute the relative difference of two double variables.
// This can be used to compare two double with some tolerance:
//   if (RelativeDifference(d1, d2) << 0.000001) ...
// Idea from https://c-faq.com/fp/fpequal.html.
inline double RelativeDifference(double a, double b) {
  double abs_a = std::abs(a);
  double abs_b = std::abs(b);
  double capacity = std::max(abs_a, abs_b);
  return capacity == 0.0 ? 0.0 : std::abs(a - b) / capacity;
}

#define CHECK_DOUBLE_EQ_S(a, b, tolerance)      \
  CHECK_S(RelativeDifference(a, b) < tolerance) \
      << absl::StrFormat("a=%.19f and b=%.19f are different", a, b);

// Object that measures time execution time of a function and prints information
// about start, end and elapsed time.
// Just put the macro FUNC_TIME() on the first line of you function.
#define FUNC_TIMER() \
  FuncTimer __func_timer(std::string(__func__) + "()", __FILE__, __LINE__);
class FuncTimer {
 public:
  FuncTimer(std::string_view text, std::string_view path, int line)
      : text_(text),
        filename_(path.substr(path.find_last_of("/\\") + 1)),
        line_(line),
        start_(absl::Now()) {
    RAW_LOG_F(INFO, absl::StrFormat(" %s %s:%d | %s: started",
                                    absl::FormatTime("%H:%M:%E3S", start_,
                                                     absl::LocalTimeZone()),
                                    filename_, line_, text_)
                        .c_str());
  }
  ~FuncTimer() {
    RAW_LOG_F(INFO, absl::StrFormat(" %s %s:%d | %s: finished >>> secs: %.3f",
                                    absl::FormatTime("%H:%M:%E3S", absl::Now(),
                                                     absl::LocalTimeZone()),
                                    filename_, line_, text_,
                                    ToDoubleSeconds(absl::Now() - start_))
                        .c_str());
  }

 private:
  const std::string text_;
  const std::string filename_;
  int line_;
  const absl::Time start_;
};

// Macro that returns vec[idx], after checking that idx >= 0 and idx <=
// vec.size(). Logs a fatal error if idx >= vec.size();
#define VECTOR_AT(vec, idx)                                  \
  ({                                                         \
    if (!(idx >= 0) || idx >= vec.size()) {                  \
      LOG_S(FATAL) << idx << " not in range " << vec.size(); \
    }                                                        \
    vec[idx];                                                \
  })

inline bool ConsumePrefixIf(std::string_view prefix, std::string_view* str) {
  if (!str->starts_with(prefix)) {
    return false;
  }
  str->remove_prefix(prefix.size());
  return true;
}

template <typename T, typename ContainerT>
bool SpanContains(const T& search, const ContainerT& container) {
  for (const auto& x : container) {
    if (x == search) {
      return true;
    }
  }
  return false;
}

inline bool StrSpanContains(std::string_view search,
                            const std::vector<std::string_view>& v) {
  return SpanContains(search, v);
}

// Add space characters to the right or the left of a string to make it 'width'
// characters wide.
inline std::string PadString(std::string_view str, size_t width,
                             bool add_right = true) {
  int needed = (int)width - (int)str.size();
  if (needed <= 0) return std::string(str);

  std::string res;
  if (add_right) {
    res = str;
    res.append(needed, ' ');
  } else {
    res.append(needed, ' ');
    res.append(str);
  }
  return res;
}

inline std::string WildCardToRegex(const std::string& wildcard) {
  static const std::string escape_chars = "\\$()*+.?[]^{|}";
  std::string result;
  for (const char c : wildcard) {
    if (c == '?') {
      result.push_back('.');
    } else if (c == '*') {
      result += ".*";
    } else {
      if (escape_chars.find(c) != std::string::npos) {
        result.push_back('\\');
      }
      result.push_back(c);
    }
  }
  return result;
}

// Returns a list of all files (plain files and directories) that are
// matched by 'wildcard'. Note that * and ? can only be used in the
// filename, not in the directory part of the path. Returns the full path
// for each file. Good example: '/tmp/*.csv' Bad example:
// '/tmp/file*/test.txt'
inline std::vector<std::string> GetFilesWithWildcard(
    const std::filesystem::path wildcard) {
  std::vector<std::string> files;
  const std::regex re(WildCardToRegex(wildcard));

  for (const auto& entry :
       std::filesystem::directory_iterator(wildcard.parent_path())) {
    if (std::regex_match(entry.path().string(), re)) {
      files.push_back(entry.path().string());
    }
  }
  return files;
}

#define FindInMapOrFail(container, key) \
  FindInMapOrFailInternal(container, key, __FILE__, __LINE__)

template <typename Map>
auto FindInMapOrFailInternal(const Map& container,
                             const typename Map::key_type& key,
                             std::string filename, int line) -> const
    typename Map::mapped_type& {
  auto it = container.find(key);
  CHECK_S(it != container.end()) << "can't find element for key <" << key
                                 << "> file:" << filename << ":" << line;
  return it->second;
}

template <typename Map>
auto FindInMapOptional(const Map& container, const typename Map::key_type& key)
    -> std::optional<typename Map::mapped_type> {
  auto it = container.find(key);
  if (it != container.end()) {
    return it->second;
  }
  return std::nullopt;
}

template <typename Map>
auto FindInMapOrDefault(const Map& container, const typename Map::key_type& key,
                        const typename Map::mapped_type& dflt) -> const
    typename Map::mapped_type& {
  auto it = container.find(key);
  if (it != container.end()) {
    it->second;
  }
  return dflt;
}

inline void LogMemoryUsage() {
  struct mallinfo2 info;
  info = mallinfo2();
  LOG_S(INFO) << " ********** Mallinfo **********";
  LOG_S(INFO) << "Total heap:      " << info.arena;
  LOG_S(INFO) << "Total mapped:    " << info.hblkhd;
  LOG_S(INFO) << "Total heap used: " << info.uordblks;
  LOG_S(INFO) << "Total heap free: " << info.fordblks;
}

inline uint64_t PseudoRandom64(uint64_t state) {
  return state * 6364136223846793005ULL + 1ULL;
}
