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

enum class TurnMode : std::uint8_t {
  Forbidden = 0,
  Mandatory = 1,
};

enum class TurnDirection : std::uint8_t {
  LeftTurn = 0,
  RightTurn = 1,
  StraightOn = 2,
  UTurn = 3,
  NoEntry = 4,
  NoExit = 5,
};

// Turn restriction parsed from a relation. "no_entry/no_exit" restrictions with
// multiple from/to ways are split into multiple turn restrictions with only one
// from/to-way each.
struct TurnRestriction {
  std::int64_t relation_id = -1;
  std::int64_t from_way_id = -1;
  std::vector<std::int64_t> via_ids;
  std::int64_t to_way_id = -1;
  bool via_is_node : 1 = 0;  // via is a node (true) or way(s) (false).
  TurnMode mode : 1 = TurnMode::Forbidden;
  TurnDirection direction : 3 = TurnDirection::LeftTurn;

  // The following values are derived from the sequence from_way_id -> via_ids
  // -> to_way_id. These are indexes into graph.nodes, not osm node ids!
  //
  // * (from_node_idx, first_via_node_idx) is the last edge in the from_way that
  // connects to the first node in "via" (which is one node or more ways).
  // * (last_via_node_idx, to_node_idx) is the first edge in the to_way that
  // connects at the end of "via".
  std::uint32_t from_node_idx = INFU32;
  std::uint32_t from_way_idx = INFU32;
  std::uint32_t first_via_node_idx = INFU32;
  std::uint32_t last_via_node_idx = INFU32;
  std::uint32_t to_node_idx = INFU32;
  std::uint32_t to_way_idx = INFU32;
};

struct TRResult {
  std::vector<TurnRestriction> trs;
  int64_t num_success = 0;
  int64_t max_success_via_ways = 0;
  int64_t num_error = 0;
  int64_t num_error_connection = 0;
};

namespace {
inline void LogMissingWay(std::string_view type, int64_t way_id,
                          int64_t rel_id) {
  LOG_S(INFO) << absl::StrFormat(
      "TR: connect error '%s' way %lld in rel %lld not found", type, way_id,
      rel_id);
}

inline const char* TypeToString(OSMPBF::Relation::MemberType type) {
  if (type == OSMPBF::Relation::NODE) return "NODE";
  if (type == OSMPBF::Relation::WAY) return "WAY";
  if (type == OSMPBF::Relation::RELATION) return "RELATION";
  return "UNKNOWN";
}

inline bool TurnRestrictionCollectIds(const OSMTagHelper& tagh,
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

inline uint32_t FindConnectedNodeIdx(const Graph& g, uint32_t way_idx,
                                     uint32_t node_idx) {
  // LOG_S(INFO) << "AA0:" << g.ways.at(way_idx).id;
  for (const GEdge& e : gnode_all_edges(g, node_idx)) {
    if (e.way_idx == way_idx) {
      return e.other_node_idx;
    }
  }
  return INFU32;
}

// Check that the turn restriction is properly connected.
inline bool ConnectTurnRestriction(const Graph& g, bool logging_on,
                                   TurnRestriction* tr) {
  IdChain chain(tr->relation_id);
  IdChain::IdPair from;
  if (!chain.CreateIdPairFromWay(g, tr->from_way_id, &from)) {
    if (logging_on) {
      LogMissingWay("from", tr->from_way_id, tr->relation_id);
    }
    return false;
  }
  chain.AddIdPair(from);
  // LOG_S(INFO) << "CC0 way_idx:" << from.way_idx;
  // LOG_S(INFO) << "CC1 way_idx:" << chain.get_chain().back().way_idx;
  if (tr->via_is_node) {
    CHECK_EQ_S(tr->via_ids.size(), 1);
    uint32_t node_idx = g.FindNodeIndex(tr->via_ids.at(0));
    if (node_idx >= g.nodes.size()) {
      LOG_S(INFO) << absl::StrFormat("TR: via node %lld in rel %lld not found",
                                     tr->via_ids.at(0), tr->relation_id);
      return false;
    }
    chain.AddIdPair({.node_idx1 = node_idx, .node_idx2 = node_idx});
  } else {
    CHECK_GE_S(tr->via_ids.size(), 1);
    for (int64_t way_id : tr->via_ids) {
      IdChain::IdPair via;
      if (!chain.CreateIdPairFromWay(g, way_id, &via)) {
        if (logging_on) {
          LogMissingWay("via", way_id, tr->relation_id);
        }
        return false;
      }
      chain.AddIdPair(via);
    }
  }

  IdChain::IdPair to;
  if (!chain.CreateIdPairFromWay(g, tr->to_way_id, &to)) {
    if (logging_on) {
      LogMissingWay("to", tr->to_way_id, tr->relation_id);
    }
    return false;
  }
  chain.AddIdPair(to);
  // LOG_S(INFO) << "CC2 way_idx:" << to.way_idx;
  // LOG_S(INFO) << "CC3 way_idx:" << chain.get_chain().back().way_idx;
  bool success = chain.success();
  if (success) {
    const std::vector<IdChain::IdPair>& v = chain.get_chain();
    CHECK_GE_S(v.size(), 3);
    tr->first_via_node_idx = v.front().node_idx2;
    tr->last_via_node_idx = v.back().node_idx1;
    tr->from_node_idx =
        FindConnectedNodeIdx(g, v.front().way_idx, tr->first_via_node_idx);
    tr->from_way_idx = v.front().way_idx;
    tr->to_node_idx =
        FindConnectedNodeIdx(g, v.back().way_idx, tr->last_via_node_idx);
    tr->to_way_idx = v.back().way_idx;
    success = (tr->from_node_idx != INFU32 && tr->to_node_idx != INFU32);
  }
  if (logging_on) {
    LOG_S(INFO) << absl::StrFormat("TR: %u %s: %s match in relation %lld",
                                   chain.get_chain().size(),
                                   success ? "success" : "error",
                                   chain.GetChainCodeString(), tr->relation_id);
    LOG_S(INFO) << absl::StrFormat(
        "TR: Connecting nodes from:%lld via-in:%lld via-out:%lld to:%lld",
        GetGNodeIdSafe(g, tr->from_node_idx),
        GetGNodeIdSafe(g, tr->first_via_node_idx),
        GetGNodeIdSafe(g, tr->last_via_node_idx),
        GetGNodeIdSafe(g, tr->to_node_idx));
  }

  return success;
}

inline CondensedTurnRestrictionKey CreateCondensedTurnRestrictionKey(
    const TurnRestriction& tr) {
  return {.from_node_idx = tr.from_node_idx,
          .from_way_idx = tr.from_way_idx,
          .via_node_idx = tr.last_via_node_idx};
}

inline CondensedTurnRestrictionData CreateCondensedTurnRestrictionData(
    const Graph& g, std::span<const TurnRestriction> trs) {
  CondensedTurnRestrictionData d;
  const GNode& node = g.nodes.at(trs.front().from_node_idx);
  uint32_t num_edges =
      gnode_edge_stop(g, trs.front().from_node_idx) - node.edges_start_pos;
  CHECK_GT_S(num_edges, 0) << node.node_id;
  // Set the 'num_edges' lowest bits.
  d.allowed_edge_bits = (1u << num_edges) - 1;
  d.uturn_allowed = 0;
  d.osm_relation_id = -1;
  for (const TurnRestriction& tr : trs) {
    // Search the edge ending at the via node.
    bool found = false;
    for (size_t offset = 0; offset < num_edges; ++offset) {
      const GEdge& e = g.edges.at(node.edges_start_pos + offset);

      if (e.way_idx == tr.from_way_idx &&
          e.other_node_idx == tr.last_via_node_idx) {
        found = true;
        if (tr.mode == TurnMode::Forbidden) {
          // Remove the bit belonging to the edge.
          d.allowed_edge_bits = d.allowed_edge_bits & ~(1u << offset);
        } else {
          // Remove all bits except the bit belonging to the edge.
          d.allowed_edge_bits = d.allowed_edge_bits & (1u << offset);
        }
        // Keep the last relation for debugging.
        d.osm_relation_id = tr.relation_id;
        break;
      }
    }
    CHECK_S(found) << tr.relation_id;
  }
  return d;
}

}  // namespace

// See https://wiki.openstreetmap.org/w/index.php?title=Relation:restriction
// TODO: Handle 'except' tag.
// TODO: Handle 'restriction[:<transportation mode>]:conditional' tag.
// TODO: Handle multiple from/to ways for 'no_entry'/'no_exit' restrictions.
//       no_entry: multiple 'from' ways.
//       no_exit:  multiple 'to' ways.
// TODO: Check if 'to' way is oneway in wrong direction, i.e. no TR needed.
// TODO: handle vehicle type restriction:bicycle, restriction:hgv, ...
// TODO: Reject ways that are closed circles
//       https://www.openstreetmap.org/relation/15824717.
inline void ParseTurnRestriction(const Graph& g, const OSMTagHelper& tagh,
                                 const OSMPBF::Relation& relation,
                                 bool logging_on, TRResult* res) {
  if (tagh.GetValue(relation, "type") != "restriction") {
    return;  // not a restriction.
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
    res->num_error += 1;
    return;
  }

  TurnMode mode = TurnMode::Forbidden;
  if (restriction.starts_with("only_")) {
    mode = TurnMode::Mandatory;
    restriction = restriction.substr(5);
  } else if (restriction.starts_with("no_")) {
    mode = TurnMode::Forbidden;
    restriction = restriction.substr(3);
  } else {
    if (!restriction.empty()) {
      if (logging_on) {
        LOG_S(INFO) << absl::StrFormat(
            "TR: prefix has to be no_ or only_ in restriction <%s> in relation "
            "%llu",
            std::string(tagh.GetValue(relation, "restriction")).c_str(),
            relation.id());
      }
    }
    res->num_error += 1;
    return;
  }

  TurnDirection direction = TurnDirection::LeftTurn;
  if (restriction == "left_turn") {
    direction = TurnDirection::LeftTurn;
  } else if (restriction == "right_turn") {
    direction = TurnDirection::RightTurn;
  } else if (restriction == "straight_on") {
    direction = TurnDirection::StraightOn;
  } else if (restriction == "u_turn") {
    direction = TurnDirection::UTurn;
  } else if (mode == TurnMode::Forbidden && restriction == "entry") {
    direction = TurnDirection::NoEntry;
  } else if (mode == TurnMode::Forbidden && restriction == "exit") {
    direction = TurnDirection::NoExit;
  } else {
    if (logging_on) {
      LOG_S(INFO) << absl::StrFormat(
          "TR: Unknown direction <%s> in relation %llu",
          std::string(restriction).c_str(), relation.id());
    }
    res->num_error += 1;
    return;
  }

  std::vector<std::int64_t> from_ids;
  std::vector<std::int64_t> to_ids;
  std::vector<std::int64_t> via_ids;
  OSMPBF::Relation::MemberType via_type = OSMPBF::Relation::NODE;
  if (!TurnRestrictionCollectIds(tagh, relation, &from_ids, &to_ids, &via_ids,
                                 &via_type)) {
    res->num_error += 1;
    return;
  }
  if (from_ids.size() < 1 ||
      (from_ids.size() > 1 && direction != TurnDirection::NoEntry) ||
      to_ids.size() < 1 ||
      (to_ids.size() > 1 && direction != TurnDirection::NoExit) ||
      via_ids.empty() ||
      (via_type == OSMPBF::Relation::NODE && via_ids.size() != 1)) {
    if (logging_on) {
      LOG_S(INFO) << absl::StrFormat(
          "TR: Can't handle from|via|to: %u|%u|%u via-type:%s direction:%s "
          "relation_id:%lld",
          from_ids.size(), via_ids.size(), to_ids.size(),
          TypeToString(via_type),
          std::string(tagh.GetValue(relation, "restriction")).c_str(),
          relation.id());
    }
    res->num_error += 1;
    return;
  }

  for (auto from_id : from_ids) {
    for (auto to_id : to_ids) {
      TurnRestriction tr = {.relation_id = relation.id(),
                            .from_way_id = from_id,
                            .via_ids = via_ids,
                            .to_way_id = to_id,
                            .via_is_node = via_type == OSMPBF::Relation::NODE,
                            .mode = mode,
                            .direction = direction};
      if (ConnectTurnRestriction(g, logging_on, &tr)) {
        res->trs.push_back(tr);
        res->num_success += 1;
        if (tr.via_ids.size() > (size_t)res->max_success_via_ways) {
          res->max_success_via_ways = tr.via_ids.size();
        }
      } else {
        res->num_error += 1;
        res->num_error_connection += 1;
      }
    }
  }
}

// Sort the simple turn restrictions by the edge that triggers the
// restriction, i.e. the edge of the from way that connects to the via node.
inline void SortSimpleTurnRestrictions(std::vector<TurnRestriction>* trs) {
  std::sort(trs->begin(), trs->end(),
            [](const TurnRestriction& a, const TurnRestriction& b) {
              if (a.from_node_idx != b.from_node_idx) {
                return a.from_node_idx < b.from_node_idx;
              }
              if (a.from_way_idx != b.from_way_idx) {
                return a.from_way_idx < b.from_way_idx;
              }
              if (a.last_via_node_idx != b.last_via_node_idx) {
                return a.last_via_node_idx < b.last_via_node_idx;
              }
              return a.to_way_idx < b.to_way_idx;
            });
}

// Compute simple turn restrictions for the turn restrictions in 'trs'.
// Note that trs needs to be sorted by SortSimpleTurnRestrictions() and must
// contain simple restrictions only.
inline CondensedTurnRestrictionMap ComputeCondensedTurnRestrictions(
    const Graph& g, const std::vector<TurnRestriction>& trs) {
  CondensedTurnRestrictionMap res;
  if (trs.empty()) {
    return res;
  }
  size_t start = 0;
  for (size_t i = 0; i < trs.size(); ++i) {
    CHECK_S(trs.at(i).via_is_node);
    if (i == trs.size() - 1 ||
        CreateCondensedTurnRestrictionKey(trs.at(i)) !=
            CreateCondensedTurnRestrictionKey(trs.at(i + 1))) {
      res[CreateCondensedTurnRestrictionKey(trs.at(start))] =
          CreateCondensedTurnRestrictionData(
              g, std::span<const TurnRestriction>(&(trs.at(start)), i - start));
      start = i + 1;
    }
  }
  return res;
}
