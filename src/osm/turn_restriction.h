#pragma once
#include <osmpbf/osmpbf.h>

#include <string_view>

#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "graph/graph_def.h"
#include "logging/loguru.h"
#include "osm/id_chain.h"
#include "osm/osm_helpers.h"

namespace {
inline void LogMissingWay(std::string_view type, int64_t way_id,
                          int64_t rel_id) {
  LOG_S(INFO) << absl::StrFormat("TR: '%s' way %lld in rel %lld not found",
                                 type, way_id, rel_id);
}

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

inline bool ConnectTurnRestriction(const Graph& g, bool log_turn_restrictions,
                                   TurnRestriction* tr) {
  IdChain chain;
  IdChain::IdPair from;
  if (!chain.CreateIdPair(g, tr->from_way_id, &from)) {
    if (log_turn_restrictions) {
      LogMissingWay("from", tr->from_way_id, tr->relation_id);
    }
    return false;
  }
  chain.AddIdPair(from);
  if (tr->via_is_node) {
    chain.AddIdPair({.id1 = tr->via_ids.at(0), .id2 = tr->via_ids.at(0)});
  } else {
    for (int64_t id : tr->via_ids) {
      IdChain::IdPair via;
      if (!chain.CreateIdPair(g, id, &via)) {
        if (log_turn_restrictions) {
          LogMissingWay("via", id, tr->relation_id);
        }
        return false;
      }
      chain.AddIdPair(via);
    }
  }

  IdChain::IdPair to;
  if (!chain.CreateIdPair(g, tr->to_way_id, &to)) {
    if (log_turn_restrictions) {
      LogMissingWay("to", tr->to_way_id, tr->relation_id);
    }
    return false;
  }
  chain.AddIdPair(to);
  bool success = chain.success();
  if (log_turn_restrictions) {
    LOG_S(INFO) << absl::StrFormat("TR: %u %s: %s match in relation %lld",
                                   chain.get_chain().size(),
                                   success ? "success" : "error",
                                   chain.GetChainCodeString(), tr->relation_id);
  }
  return success;
}
