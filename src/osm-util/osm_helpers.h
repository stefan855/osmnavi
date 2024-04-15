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
 private:
  // Needed first for the initialization of the tag fields.
  const OSMPBF::StringTable& string_table_;

 public:
  OSMTagHelper(const OSMPBF::StringTable& string_table)
      : string_table_(string_table),
        access_(FindTagNum("access")),
        admin_level_(FindTagNum("admin_level")),
        boundary_(FindTagNum("boundary")),
        ferry_(FindTagNum("ferry")),
        highway_(FindTagNum("highway")),
        junction_(FindTagNum("junction")),
        maxspeed_(FindTagNum("maxspeed")),
        motor_vehicle_(FindTagNum("motor_vehicle")),
        motorcar_(FindTagNum("motorcar")),
        name_(FindTagNum("name")),
        oneway_(FindTagNum("oneway")),
        restriction_(FindTagNum("restriction")),
        restriction_motorcar_(FindTagNum("restriction:motorcar")),
        route_(FindTagNum("route")),
        type_(FindTagNum("type")) {}

  struct FilterExp {
    // At least one of the two following booleans have to be true.
    // If both are true, then the expression key + ": " + val is matched.
    bool match_key;
    bool match_val;
    // When true, then 're' has to match at least once, if false, then it must
    // never match.
    bool negative;
    std::regex re;
  };

  const int access_;
  const int admin_level_;
  const int boundary_;
  const int ferry_;
  const int highway_;
  const int junction_;
  const int maxspeed_;
  const int motor_vehicle_;
  const int motorcar_;
  const int name_;
  const int oneway_;
  const int restriction_;
  const int restriction_motorcar_;
  const int route_;
  const int type_;

  std::string_view GetValue(
      const google::protobuf::RepeatedField<unsigned int>& keys,
      const google::protobuf::RepeatedField<unsigned int>& vals,
      int key) const {
    CHECK_EQ_S(keys.size(), vals.size());
    if (key >= 0) {
      for (int i = 0; i < keys.size(); ++i) {
        if (keys[i] == static_cast<unsigned int>(key)) {
          return string_table_.s(vals[i]);
        }
      }
    }
    return "";
  }

  template <typename T>
  std::string_view GetValue(const T& obj, int key) const {
    return GetValue(obj.keys(), obj.vals(), key);
  }

  std::string_view GetValueByStr(
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
  std::string_view GetValueByStr(const T& obj, std::string_view key) const {
    return GetValueByStr(obj.keys(), obj.vals(), key);
  }

  bool EqualValue(const google::protobuf::RepeatedField<unsigned int>& keys,
                  const google::protobuf::RepeatedField<unsigned int>& vals,
                  int key, std::string_view value) const {
    CHECK_S(!value.empty());
    return value == GetValue(keys, vals, key);
  }

  template <typename T>
  bool EqualValue(const T& obj, int key, std::string_view value) const {
    return EqualValue(obj.keys(), obj.vals(), key, value);
  }

  std::string_view ToString(unsigned int tagnum) const {
    if (tagnum < static_cast<unsigned int>(string_table_.s().size())) {
      return string_table_.s(tagnum);
    }
    return "";
  }

  std::string GetLoggingStr(
      int64_t id, const google::protobuf::RepeatedField<unsigned int>& keys,
      const google::protobuf::RepeatedField<unsigned int>& vals) const {
    CHECK_EQ_S(keys.size(), vals.size());
    std::string res = absl::StrCat("id=", id, "\n");
    for (int i = 0; i < keys.size(); ++i) {
      absl::StrAppend(&res, string_table_.s(keys[i]), "=",
                      string_table_.s(vals[i]), "\n");
    }
    return res;
  }

  template <typename T>
  std::string GetLoggingStr(const T& obj) const {
    return GetLoggingStr(obj.id(), obj.keys(), obj.vals());
  }

  static std::vector<FilterExp> ParseMatchFilters(std::string_view expression) {
    std::vector<FilterExp> res;
    for (std::string_view str :
         absl::StrSplit(expression, '|', absl::SkipEmpty())) {
      FilterExp exp;
      exp.negative = ConsumePrefixIf("-", &str);
      exp.match_val = ConsumePrefixIf("val:", &str);
      exp.match_key = !exp.match_val && ConsumePrefixIf("key:", &str);
      if (!exp.match_val && !exp.match_key) {
        if (ConsumePrefixIf("keyval:", &str)) {
          exp.match_val = true;
          exp.match_key = true;
        } else {
          ABORT_S() << "wrong syntax in filter expression <" << expression
                    << ">";
        }
      }
      CHECK_S(!str.empty());
      exp.re = std::regex(std::string(str), std::regex_constants::icase);
      res.push_back(exp);
    }
    return res;
  }

  bool MatchFilters(const google::protobuf::RepeatedField<unsigned int>& keys,
                    const google::protobuf::RepeatedField<unsigned int>& vals,
                    const std::vector<FilterExp>& filters) const {
    if (filters.empty()) return false;
    for (const FilterExp& f : filters) {
      bool res = false;
      ;
      if (f.match_key && f.match_val) {
        // Check if this filter can be satisied.
        for (int i = 0; i < keys.size(); ++i) {
          std::string tmp = absl::StrCat(string_table_.s(keys[i]), "=",
                                         string_table_.s(vals[i]));
          res = std::regex_search(tmp, f.re);
          if (res) break;  // found a match.
        }
      } else if (f.match_val) {
        for (int i = 0; i < vals.size(); ++i) {
          res = std::regex_search(string_table_.s(vals[i]), f.re);
          if (res) break;  // found a match.
        }
      } else {
        CHECK_S(f.match_key);
        for (int i = 0; i < keys.size(); ++i) {
          res = std::regex_search(string_table_.s(keys[i]), f.re);
          if (res) break;  // found a match.
        }
      }
      if (f.negative == res) {
        return false;
      }
    }
    return true;
  }

  template <typename T>
  bool MatchFilters(const T& obj, const std::vector<FilterExp>& filters) const {
    return MatchFilters(obj.keys(), obj.vals(), filters);
  }

  const OSMPBF::StringTable& GetStringTable() const { return string_table_; }

 private:
  int FindTagNum(std::string_view str) const {
    for (int i = 0; i < string_table_.s().size(); ++i) {
      if (string_table_.s(i) == str) {
        return i;
      }
    }
    return -2;
  }
};

inline HIGHWAY_LABEL ParseHighwayLabel(const OSMTagHelper& tagh,
                                       const OSMPBF::Way& way) {
  const std::string_view val = tagh.GetValue(way, tagh.highway_);
  if (val.empty()) return HW_MAX;
  return HighwayLabelToEnum(val);
}

// Only call for ways which are for cars.
// Returns -1 for reverse direction only, 0 for both directions, 1 for forward
// direction only.
inline DIRECTION CarDirectionsOld(const OSMTagHelper& tagh,
                                  const OSMPBF::Way& way) {
  const std::string_view oneway_val = tagh.GetValue(way, tagh.oneway_);
  if (!oneway_val.empty()) {
    if (oneway_val == "no") {
      return DIR_BOTH;
    }
    if (oneway_val == "yes") {
      return DIR_FORWARD;
    }
    if (oneway_val == "-1") {
      return DIR_BACKWARD;
    }
    return DIR_MAX;
  }

  if (tagh.EqualValue(way, tagh.junction_, "roundabout")) {
    return DIR_FORWARD;
  }

  const std::string_view highway_val = tagh.GetValue(way, tagh.highway_);
  if (!highway_val.empty() &&
      (highway_val == "motorway" || highway_val == "motorway_link")) {
    return DIR_FORWARD;
  }

  return DIR_BOTH;
}

namespace {
const char* TypeToString(OSMPBF::Relation::MemberType type) {
  if (type == OSMPBF::Relation::NODE) return "NODE";
  if (type == OSMPBF::Relation::WAY) return "WAY";
  if (type == OSMPBF::Relation::RELATION) return "RELATION";
  return "UNKNOWN";
}

bool TurnRestrictionCollectIds(const OSMTagHelper& tagh,
                               const OSMPBF::Relation& relation,
                               std::vector<std::int64_t>* from_ids,
                               std::vector<std::int64_t>* to_ids,
                               std::vector<std::int64_t>* via_ids,
                               OSMPBF::Relation::MemberType* via_type) {
  std::int64_t running_id = 0;
  for (int i = 0; i < relation.memids().size(); ++i) {
    running_id += relation.memids(i);
    const std::string_view role = tagh.ToString(relation.roles_sid(i));
    const OSMPBF::Relation::MemberType type = relation.types(i);
    if (role == "from") {
      if (type == OSMPBF::Relation::WAY) {
        from_ids->push_back(running_id);
      } else {
        LOG_S(INFO) << absl::StrFormat(
            "TR: Illegal type %s role:%s relation %lld running_id %lld",
            TypeToString(type), std::string(role).c_str(), relation.id(),
            running_id);
        return false;
      }
    } else if (role == "to") {
      if (type == OSMPBF::Relation::WAY) {
        to_ids->push_back(running_id);
      } else {
        LOG_S(INFO) << absl::StrFormat(
            "TR: Illegal type %s role:%s relation %lld running_id %lld",
            TypeToString(type), std::string(role).c_str(), relation.id(),
            running_id);
        return false;
      }
    } else if (role == "via") {
      if ((type == OSMPBF::Relation::WAY || type == OSMPBF::Relation::NODE) &&
          (via_ids->empty() || type == *via_type)) {
        via_ids->push_back(running_id);
        *via_type = type;
      } else {
        LOG_S(INFO) << absl::StrFormat(
            "TR: Illegal type %s role:%s relation %lld running_id %lld",
            TypeToString(type), std::string(role).c_str(), relation.id(),
            running_id);
        return false;
      }
    }
  }
  return true;
}
}  // namespace

// See https://wiki.openstreetmap.org/w/index.php?title=Relation:restriction
// TODO: Handle 'except' tag.
// TODO: Handle 'restriction[:<transportation mode>]:conditional' tag.
// TODO: Handle multiple from/to ways for 'no_entry'/'no_exit' restrictions.
// TODO: Check if 'to' way is oneway in wrong direction, i.e. no TR needed.
// TODO: Reject ways that are closed circles
//       https://www.openstreetmap.org/relation/15824717.
inline ResType ParseTurnRestriction(const OSMTagHelper& tagh,
                                    const OSMPBF::Relation& relation,
                                    TurnRestriction* tr) {
  if (tagh.GetValue(relation, tagh.type_) != "restriction") {
    return ResType::Ignore;  // not a restriction.
  }
  std::string_view restriction = tagh.GetValue(relation, tagh.restriction_);
  if (restriction.empty()) {
    restriction = tagh.GetValue(relation, tagh.restriction_motorcar_);
  }
  if (restriction.empty()) {
    LOG_S(INFO) << absl::StrFormat(
        "TR: Can't get 'restriction' tag in relation %llu", relation.id());
    return ResType::Error;
  }

  if (restriction.starts_with("only_")) {
    tr->mode = TurnMode::OnlyThis;
    restriction = restriction.substr(5);
  } else if (restriction.starts_with("no_")) {
    tr->mode = TurnMode::NotThis;
    restriction = restriction.substr(3);
  } else {
    if (!restriction.empty()) {
      LOG_S(INFO) << absl::StrFormat(
          "TR: Unknown restriction <%s> in relation %llu",
          std::string(restriction).c_str(), relation.id());
    }
    return ResType::Error;
  }
  if (restriction == "left_turn") {
    tr->direction = TurnDirection::LeftTurn;
  } else if (restriction == "right_turn") {
    tr->direction = TurnDirection::RightTurn;
  } else if (restriction == "straight_on") {
    tr->direction = TurnDirection::StraightOn;
  } else if (restriction == "u_turn") {
    tr->direction = TurnDirection::UTurn;
  } else {
    LOG_S(INFO) << absl::StrFormat(
        "TR: Unknown direction <%s> in relation %llu",
        std::string(restriction).c_str(), relation.id());
    return ResType::Error;
  }

  std::vector<std::int64_t> from_ids;
  std::vector<std::int64_t> to_ids;
  std::vector<std::int64_t> via_ids;
  OSMPBF::Relation::MemberType via_type = OSMPBF::Relation::NODE;
  if (!TurnRestrictionCollectIds(tagh, relation, &from_ids, &to_ids, &via_ids,
                                 &via_type)) {
    return ResType::Error;
  }
  if (from_ids.size() != 1 || to_ids.size() != 1 || via_ids.empty() ||
      (via_type == OSMPBF::Relation::NODE && via_ids.size() != 1)) {
    LOG_S(INFO) << absl::StrFormat(
        "TR: Can't handle from|via|to: %u|%u|%u via-type:%s relation_id:%lld",
        from_ids.size(), via_ids.size(), to_ids.size(), TypeToString(via_type),
        relation.id());
    return ResType::Error;
  }

  tr->relation_id = relation.id();
  tr->from_way_id = from_ids.at(0);
  tr->to_way_id = to_ids.at(0);
  tr->via_is_node = via_type == OSMPBF::Relation::NODE;
  tr->via_ids = std::move(via_ids);
  return ResType::Success;
}

// Parse speed in the format [<country_code>:][rural|urban|[[zone]<maxspeed>]].
// <country_code> is a two letter code such as "DE" or "CH".
// 'maxspeed' can be ommitted, the ther return attributes have to be set.
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
      LOG_S(INFO) << "Invalid CC in " << val;
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
