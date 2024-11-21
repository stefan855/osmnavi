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

  std::string GetLoggingStr(
      int64_t id, const google::protobuf::RepeatedField<unsigned int>& keys,
      const google::protobuf::RepeatedField<unsigned int>& vals,
      bool online = false) const {
    std::string_view sep = online ? " :: " : "\n";
    CHECK_EQ_S(keys.size(), vals.size());
    std::string res = absl::StrCat("id=", id, sep);
    for (int i = 0; i < keys.size(); ++i) {
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
                                    bool logging_on, TurnRestriction* tr) {
  if (tagh.GetValue(relation, "type") != "restriction") {
    return ResType::Ignore;  // not a restriction.
  }
  std::string_view restriction = tagh.GetValue(relation, "restriction");
  if (restriction.empty()) {
    restriction = tagh.GetValue(relation, "restriction:motorcar");
  }
  if (restriction.empty()) {
    if (logging_on) {
      LOG_S(INFO) << absl::StrFormat(
          "TR: Can't get 'restriction' tag in relation %llu", relation.id());
    }
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
      if (logging_on) {
        LOG_S(INFO) << absl::StrFormat(
            "TR: Unknown restriction <%s> in relation %llu",
            std::string(restriction).c_str(), relation.id());
      }
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
    if (logging_on) {
      LOG_S(INFO) << absl::StrFormat(
          "TR: Unknown direction <%s> in relation %llu",
          std::string(restriction).c_str(), relation.id());
    }
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
    if (logging_on) {
      LOG_S(INFO) << absl::StrFormat(
          "TR: Can't handle from|via|to: %u|%u|%u via-type:%s relation_id:%lld",
          from_ids.size(), via_ids.size(), to_ids.size(),
          TypeToString(via_type), relation.id());
    }
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
