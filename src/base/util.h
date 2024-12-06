#pragma once

#include <malloc.h>

#include <charconv>
#include <filesystem>
#include <regex>
#include <string>
#include <vector>

#include "absl/strings/str_format.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "logging/loguru.h"

enum class ResType : int { Success = 0, Ignore = 1, Error = 2 };

inline void InitLogging(int argc, char* argv[]) {
  loguru::g_preamble_date = false;
  loguru::g_preamble_uptime = false;
  loguru::g_preamble_thread = false;
  loguru::init(argc, argv);
}

// Object that measures time execution time of a function and prints information
// about start, end and elapsed time.
// Just put the macro FUNC_TIME() on the first line of you function.
#define FUNC_TIMER() FuncTimer __func_timer(__func__, __FILE__, __LINE__);
class FuncTimer {
 public:
  FuncTimer(std::string_view text, std::string_view path, int line)
      : text_(text),
        filename_(path.substr(path.find_last_of("/\\") + 1)),
        line_(line),
        start_(absl::Now()) {
    RAW_LOG_F(INFO, absl::StrFormat(" %s %s:%d | %s() started",
                                    absl::FormatTime("%H:%M:%E3S", start_,
                                                     absl::LocalTimeZone()),
                                    filename_, line_, text_)
                        .c_str());
  }
  ~FuncTimer() {
    RAW_LOG_F(INFO, absl::StrFormat(" %s %s:%d | %s() finished >>> secs: %.3f",
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

inline void LogMemoryUsage() {
  struct mallinfo2 info;
  info = mallinfo2();
  LOG_S(INFO) << " ********** Mallinfo **********";
  LOG_S(INFO) << "Total heap:      " << info.arena;
  LOG_S(INFO) << "Total mapped:    " << info.hblkhd;
  LOG_S(INFO) << "Total heap used: " << info.uordblks;
  LOG_S(INFO) << "Total heap free: " << info.fordblks;
}
