#pragma once
#include <osmpbf/osmpbf.h>
#include <stdio.h>

#include <cstdlib>
#include <string_view>

#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "graph/graph_def.h"
#include "graph/routing_attrs.h"
#include "logging/loguru.h"

class OSMTagHelper {
 public:
  OSMTagHelper(const OSMPBF::StringTable& string_table)
      : string_table_(string_table), empty_string_("") {}

  std::string_view GetValue(
      const google::protobuf::RepeatedField<unsigned int>& keys,
      const google::protobuf::RepeatedField<unsigned int>& vals,
      std::string_view key) const {
    CHECK_EQ_S(keys.size(), vals.size());
    for (int i = 0; i < keys.size(); ++i) {
      if (string_table_.s(keys[i]) == key) {
        return string_table_.s(vals[i]);
      }
    }
    return "";
  }

  template <typename T>
  std::string_view GetValue(const T& obj, std::string_view key) const {
    return GetValue(obj.keys(), obj.vals(), key);
  }

  const std::string& ToString(unsigned int tagnum) const {
    if (tagnum < static_cast<unsigned int>(string_table_.s().size())) {
      return string_table_.s(tagnum);
    }
    return empty_string_;
  }

  template <typename Vec>
  std::string GetLoggingStr(int64_t id, const Vec& keys, const Vec& vals,
                            bool one_line = false) const {
    std::string_view sep = one_line ? " :: " : "\n";
    CHECK_EQ_S(keys.size(), vals.size());
    std::string res = absl::StrCat("id=", id, sep);
    for (int i = 0; i < (int)keys.size(); ++i) {
      absl::StrAppend(&res, string_table_.s(keys[i]), "=",
                      string_table_.s(vals[i]), sep);
    }
    return res;
  }

  template <typename T>
  std::string GetLoggingStr(const T& obj, bool oneline = false) const {
    return GetLoggingStr(obj.id(), obj.keys(), obj.vals(), oneline);
  }

  const OSMPBF::StringTable& GetStringTable() const { return string_table_; }

 private:
  // Needed first for the initialization of the tag fields.
  const OSMPBF::StringTable& string_table_;
  // This is needed to return references to string instead of string views.
  const std::string empty_string_;
};

// Parse speed in the format [<country_code>:][rural|urban|[[zone]<maxspeed>]].
// <country_code> is a two letter code such as "DE" or "CH".
// 'maxspeed' can be omitted, the other returned attributes have to be set.
// Returns true if at least one of the components has been parsed successfully.
inline bool ParseCountrySpeedParts(std::string_view val, uint16_t* ncc,
                                   ENVIRONMENT_TYPE* et,
                                   std::uint16_t* maxspeed = nullptr) {
  if (val.empty() || std::isdigit(val.at(0))) {
    // Empty value or value that starts with digit can't match anything.
    return false;
  }

  auto colon_pos = val.find(':');
  if (colon_pos != std::string_view::npos) {
    *ncc = TwoLetterCountryCodeToNum(val.substr(0, colon_pos));
    if (*ncc == 0) {
      // TODO: optional logging mode needed
      // LOG_S(INFO) << "Invalid CC in " << val;
    }
    val = val.substr(colon_pos + 1);
  }

  if (val == "rural") {
    *et = ET_RURAL;
    return true;
  } else if (val == "urban") {
    *et = ET_URBAN;
    return true;
  } else if (maxspeed != nullptr && ConsumePrefixIf("zone", &val)) {
    if (ParseNumericMaxspeed(val, maxspeed)) {
      return true;
    }
  }
  return *ncc > 0;
}
