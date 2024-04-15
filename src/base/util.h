#pragma once

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

class FuncTimer {
 public:
  FuncTimer(std::string_view text) : text_(text), start_(absl::Now()) {
    LOG_S(INFO) << text_ << " started";
  }
  ~FuncTimer() {
    LOG_S(INFO) << absl::StrFormat("%s finished >>> secs: %.3f", text_,
                                   ToDoubleSeconds(absl::Now() - start_));
  }

 private:
  const std::string text_;
  const absl::Time start_;
};

inline bool ConsumePrefixIf(std::string_view prefix, std::string_view* str) {
  if (!str->starts_with(prefix)) {
    return false;
  }
  str->remove_prefix(prefix.size());
  return true;
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
