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
#include "osm/turn_restriction_defs.h"

struct TRResult {
  std::vector<TurnRestriction> trs;
  int64_t num_success = 0;
  int64_t max_success_via_ways = 0;
  int64_t num_error = 0;
  int64_t num_error_connection = 0;
};

namespace {
inline void LogMissingWay(Verbosity verbosity, std::string_view type,
                          int64_t way_id, int64_t rel_id) {
  if (verbosity < Verbosity::Warning) return;
  LOG_S(INFO) << absl::StrFormat(
      "TR: connect error '%s' way %lld in rel %lld not found", type, way_id,
      rel_id);
}

inline void LogClosedWay(Verbosity verbosity, int64_t node_id, int64_t way_id,
                         int64_t rel_id) {
  if (verbosity < Verbosity::Warning) return;
  LOG_S(INFO) << absl::StrFormat(
      "TR: connect error way %lld is a closed circle, in rel %lld node id %lld",
      way_id, rel_id, node_id);
}

inline const char* TypeToString(OSMPBF::Relation::MemberType type) {
  if (type == OSMPBF::Relation::NODE) return "NODE";
  if (type == OSMPBF::Relation::WAY) return "WAY";
  if (type == OSMPBF::Relation::RELATION) return "RELATION";
  return "UNKNOWN";
}

inline bool TurnRestrictionCollectIds(const OSMTagHelper& tagh,
                                      Verbosity verbosity,
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
        if (verbosity >= Verbosity::Warning) {
          LOG_S(INFO) << absl::StrFormat(
              "TR: Illegal type %s role:%s relation %lld running_id %lld",
              TypeToString(type), std::string(role).c_str(), relation.id(),
              running_id);
        }
        return false;
      }
    } else if (role == "to") {
      if (type == OSMPBF::Relation::WAY) {
        to_ids->push_back(running_id);
      } else {
        if (verbosity >= Verbosity::Warning) {
          LOG_S(INFO) << absl::StrFormat(
              "TR: Illegal type %s role:%s relation %lld running_id %lld",
              TypeToString(type), std::string(role).c_str(), relation.id(),
              running_id);
        }
        return false;
      }
    } else if (role == "via") {
      if ((type == OSMPBF::Relation::WAY || type == OSMPBF::Relation::NODE) &&
          (via_ids->empty() || type == *via_type)) {
        via_ids->push_back(running_id);
        *via_type = type;
      } else {
        if (verbosity >= Verbosity::Warning) {
          LOG_S(INFO) << absl::StrFormat(
              "TR: Illegal type %s role:%s relation %lld running_id %lld",
              TypeToString(type), std::string(role).c_str(), relation.id(),
              running_id);
        }
        return false;
      }
    }
  }
  return true;
}

#if 0
inline uint32_t FindConnectedNodeIdx(const Graph& g, uint32_t way_idx,
                                     uint32_t node_idx, bool dir_forward) {
  LOG_S(INFO) << absl::StrFormat("KK0 way:%lld node:%lld forward:%d",
                                 GetGWayIdSafe(g, way_idx),
                                 GetGNodeIdSafe(g, node_idx), dir_forward);
  for (const GEdge& e : gnode_all_edges(g, node_idx)) {
    if (e.way_idx == way_idx) {
      // TODO: make vehicle type a parameter.
      if ((dir_forward && RoutableForward(g, e, VH_MOTOR_VEHICLE)) ||
          (!dir_forward && RoutableBackward(g, e, VH_MOTOR_VEHICLE))) {
        return e.other_node_idx;
      }
    }
  }
  LOG_S(INFO) << "KK1";
  return INFU32;
}
#endif

inline uint32_t CountIncoming(const Graph& g, uint32_t node_idx) {
  uint32_t count = 0;
  for (const GEdge& e : gnode_all_edges(g, node_idx)) {
    count += (e.both_directions | e.inverted);
  }
  return count;
}

// Check that the turn restriction is properly connected.
inline bool ConnectTurnRestriction(const Graph& g, Verbosity verbosity,
                                   TurnRestriction* tr) {
  IdChain chain(tr->relation_id);
  IdChain::IdPair from;
  if (!chain.CreateIdPairFromWay(g, tr->from_way_id, &from)) {
    if (verbosity >= Verbosity::Warning) {
      LogMissingWay(verbosity, "from", tr->from_way_id, tr->relation_id);
    }
    return false;
  }
  if (from.node_idx1 == from.node_idx2) {
    // TODO: Some cases could actually be handled properly, for instance when
    // the from-way is a closed oneway. To avoid the code complexity we're
    // currently not doing it.
    LogClosedWay(verbosity, GetGNodeIdSafe(g, from.node_idx1), tr->from_way_id,
                 tr->relation_id);
    return false;
  }
  chain.AddIdPair(from);

  if (tr->via_is_node) {
    CHECK_EQ_S(tr->via_ids.size(), 1);
    uint32_t node_idx = g.FindNodeIndex(tr->via_ids.at(0));
    if (node_idx >= g.nodes.size()) {
      if (verbosity >= Verbosity::Warning) {
        LOG_S(INFO) << absl::StrFormat(
            "TR: via node %lld in rel %lld not found", tr->via_ids.at(0),
            tr->relation_id);
      }
      return false;
    }
    chain.AddIdPair({.node_idx1 = node_idx, .node_idx2 = node_idx});
  } else {
    CHECK_GE_S(tr->via_ids.size(), 1);
    for (int64_t way_id : tr->via_ids) {
      IdChain::IdPair via;
      if (!chain.CreateIdPairFromWay(g, way_id, &via)) {
        LogMissingWay(verbosity, "via", way_id, tr->relation_id);
        return false;
      }
      if (via.node_idx1 == via.node_idx2) {
        LogClosedWay(verbosity, GetGNodeIdSafe(g, via.node_idx1), way_id,
                     tr->relation_id);
        return false;
      }
      chain.AddIdPair(via);
    }
  }

  IdChain::IdPair to;
  if (!chain.CreateIdPairFromWay(g, tr->to_way_id, &to)) {
    LogMissingWay(verbosity, "to", tr->to_way_id, tr->relation_id);
    return false;
  }
  if (to.node_idx1 == to.node_idx2) {
    LogClosedWay(verbosity, GetGNodeIdSafe(g, to.node_idx1), tr->to_way_id,
                 tr->relation_id);
    return false;
  }
  chain.AddIdPair(to);
  bool success = chain.success();

  // Build the node path.
  if (success) {
    const std::vector<IdChain::IdPair>& ch = chain.get_chain();
    CHECK_S(tr->path.empty());
    CHECK_GE_S(ch.size(), 3);

    {
      // Add last segment of from-way.
      const IdChain::IdPair& p = ch.front();
      CHECK_NE_S(p.way_idx, INFU32);
      const std::vector<uint32_t> v = p.GetWayNodeIndexes(g);
      tr->path.push_back({.from_node_idx = v.at(v.size() - 2),
                          .way_idx = p.way_idx,
                          .to_node_idx = v.at(v.size() - 1)});
      tr->path_start_node_idx = v.at(v.size() - 2);
    }

    if (!tr->via_is_node) {
      // Iterate the via-ways.
      for (size_t i = 1; i < ch.size() - 1; ++i) {
        const IdChain::IdPair& p = ch.at(i);
        const auto node_indexes = p.GetWayNodeIndexes(g);
        for (size_t pos = 0; pos < node_indexes.size() - 1; ++pos) {
          tr->path.push_back({.from_node_idx = node_indexes.at(pos),
                              .way_idx = p.way_idx,
                              .to_node_idx = node_indexes.at(pos + 1)});
        }
      }
    }

    {
      // Add first segment of to-way.
      const IdChain::IdPair& p = ch.back();
      CHECK_NE_S(p.way_idx, INFU32);
      const std::vector<uint32_t> v = p.GetWayNodeIndexes(g);
      tr->path.push_back({.from_node_idx = v.at(0),
                          .way_idx = p.way_idx,
                          .to_node_idx = v.at(1)});
    }
  }

  if (success) {
    // Now check that the path is sane and determine edge_idx for each edge in
    // the path.
    int64_t prev_idx = -1;
    for (TurnRestriction::TREdge& entry : tr->path) {
      CHECK_S(prev_idx == -1 || prev_idx == entry.from_node_idx)
          << tr->relation_id;
      prev_idx = entry.to_node_idx;

      entry.edge_idx = gnode_find_forward_edge_idx(
          g, entry.from_node_idx, entry.to_node_idx, entry.way_idx);
      const GEdge& edge = g.edges.at(entry.edge_idx);
      if (edge.inverted || edge.other_node_idx != entry.to_node_idx) {
        if (verbosity >= Verbosity::Warning) {
          LOG_S(INFO) << absl::StrFormat(
              "TR: edge %lld -> %lld on way %lld not found (relation %lld)",
              GetGNodeIdSafe(g, entry.from_node_idx),
              GetGNodeIdSafe(g, entry.to_node_idx),
              GetGWayIdSafe(g, entry.way_idx), tr->relation_id);
        }
        success = false;
      }
    }
  }

  // Check if this complex turn restriction can be converted into a simple
  // turn restriction. This is possible when all except the last two nodes
  // have no additional incoming edges.
  // Don't do it for know, since handling of complex turn restrictions is
  // coming anyways.
  if (verbosity >= Verbosity::Verbose && success && !tr->via_is_node) {
    const std::vector<IdChain::IdPair>& v = chain.get_chain();
    size_t pos = 1;
    for (; pos < v.size() - 1; ++pos) {
      if (CountIncoming(g, v.at(pos).node_idx1) != 1) {
        break;
      }
    }
    if (pos >= v.size() - 1) {
      LOG_S(INFO) << absl::StrFormat(
          "TR: Could convert complex->simple turn restriction %lld hops:%llu",
          tr->relation_id, v.size());
    } else if (pos > 1) {
      LOG_S(INFO) << absl::StrFormat(
          "TR: Could simplify (pos=%llu of %llu) complex turn restriction %lld",
          pos, v.size(), tr->relation_id);
    }
  }

  if ((success && verbosity >= Verbosity::Debug) ||
      (!success && verbosity >= Verbosity::Warning)) {
    LOG_S(INFO) << absl::StrFormat("TR: %u %s: %s match in relation %lld",
                                   chain.get_chain().size(),
                                   success ? "success" : "error",
                                   chain.GetChainCodeString(), tr->relation_id);
    if (tr->path.size() >= 2) {
      LOG_S(INFO) << absl::StrFormat(
          "TR: Connecting nodes from:%lld via-in:%lld via-out:%lld to:%lld",
          GetGNodeIdSafe(g, tr->path.front().from_node_idx),
          GetGNodeIdSafe(g, tr->path.front().to_node_idx),
          GetGNodeIdSafe(g, tr->path.back().from_node_idx),
          GetGNodeIdSafe(g, tr->path.back().to_node_idx));
    }
  }

  return success;
}

// Computes a bitset representing the allowed outgoing edges at the via node,
// applying all turn restrictions in 'trs' at once.
//
// The turn restrictions in 'trs' must all have identical keys, i.e.
// identical (from_node_idx,from_way_idx, last_via_node_idx).
inline bool CreateSimpleTurnRestrictionData(
    const Graph& g, Verbosity verbosity,
    const std::span<const TurnRestriction> trs, SimpleTurnRestrictionData* d) {
  CHECK_S(!trs.empty());
  const TurnRestriction& first_tr = trs.front();
  CHECK_EQ_S(first_tr.path.size(), 2) << first_tr.relation_id;

  const uint32_t from_node_idx = first_tr.path.front().from_node_idx;
  const uint32_t from_way_idx = first_tr.path.front().way_idx;
  const uint32_t via_node_idx = first_tr.path.front().to_node_idx;
  const GNode& via_node = g.nodes.at(via_node_idx);
  uint32_t num_edges =
      gnode_edge_stop(g, via_node_idx) - via_node.edges_start_pos;
  CHECK_GT_S(num_edges, 0) << via_node.node_id;

  // Set the 'num_edges' lowest bits.
  d->allowed_edge_bits = (1u << num_edges) - 1;
  d->osm_relation_id = first_tr.relation_id;

  bool found = false;
  for (const TurnRestriction& tr : trs) {
    // Search the edge ending at via_node.
    for (size_t offset = 0; offset < num_edges; ++offset) {
      const GEdge& e = g.edges.at(via_node.edges_start_pos + offset);

      if (e.inverted) {
        // Remove the bit belonging to the edge.
        d->allowed_edge_bits = d->allowed_edge_bits & ~(1u << offset);
      } else if (e.way_idx == tr.path.back().way_idx &&
                 e.other_node_idx == tr.path.back().to_node_idx) {
        found = true;
        if (tr.forbidden) {
          // Remove the bit belonging to the edge.
          d->allowed_edge_bits = d->allowed_edge_bits & ~(1u << offset);
        } else {
          // Remove all bits except the bit belonging to the edge.
          d->allowed_edge_bits = d->allowed_edge_bits & (1u << offset);
        }
      }
    }
  }

  // Exclude the uturn if it isn't allowed for the via node.
  const GEdge& from_edge =
      gnode_find_edge(g, from_node_idx, via_node_idx, from_way_idx);
  // If the edge is inverted then this is a data error, because the turn
  // restriction has a from edge that only exists in the wrong direction.
  // This should have been caught before, therefore we check fail here.
  CHECK_S(!from_edge.inverted) << first_tr.relation_id;
  if (!from_edge.car_uturn_allowed) {
    for (size_t offset = 0; offset < num_edges; ++offset) {
      const GEdge& e = g.edges.at(via_node.edges_start_pos + offset);
      if (!e.inverted && e.other_node_idx == from_node_idx &&
          e.way_idx == from_way_idx) {
        // Remove the bit belonging to the edge.
        d->allowed_edge_bits = d->allowed_edge_bits & ~(1u << offset);
      }
    }
  }

  // Were there any matching edges? Otherwise, ignore this set of turn
  // restrictions. This can for example happen when a turn restriction forbids
  // to go into a road that is already oneway in the other direction, i.e. the
  // turn is disallowed anyways.
  if (!found && verbosity >= Verbosity::Warning) {
    LOG_S(INFO) << absl::StrFormat(
        "TR: No matching edges from_node:%lld via_node:%lld to "
        "node:%lld, example relation:%lld",
        g.nodes.at(from_node_idx).node_id, via_node.node_id, via_node.node_id,
        d->osm_relation_id);
  }
  return found;
}

}  // namespace

// See https://wiki.openstreetmap.org/w/index.php?title=Relation:restriction
// TODO: make vehicle type a parameter.
// TODO: Handle 'except' tag.
// TODO: Handle 'restriction[:<transportation mode>]:conditional' tag.
// TODO: Handle restriction:bicycle? values 'stop' and 'give_way'?
//       https://wiki.openstreetmap.org/wiki/Key:restriction:bicycle
// TODO: Handle multiple from/to ways for 'no_entry'/'no_exit' restrictions.
//       no_entry: multiple 'from' ways.
//       no_exit:  multiple 'to' ways.
// TODO: Check if 'to' way is oneway in wrong direction, i.e. no TR needed.
// TODO: handle vehicle type restriction:bicycle, restriction:hgv, ...
// TODO: Reject ways that are closed circles
//       https://www.openstreetmap.org/relation/15824717.
inline void ParseTurnRestriction(const Graph& g, const OSMTagHelper& tagh,
                                 const OSMPBF::Relation& relation,
                                 Verbosity verbosity, TRResult* res) {
  if (tagh.GetValue(relation, "type") != "restriction") {
    return;  // not a restriction.
  }
  std::string_view restriction = tagh.GetValue(relation, "restriction");
  if (restriction.empty()) {
    restriction = tagh.GetValue(relation, "restriction:motorcar");
  }
  if (restriction.empty()) {
    if (verbosity >= Verbosity::Warning) {
      LOG_S(INFO) << absl::StrFormat(
          "TR: Can't get 'restriction' tag in relation %llu", relation.id());
    }
    res->num_error += 1;
    return;
  }

  bool forbidden = true;
  if (restriction.starts_with("only_")) {
    forbidden = false;
    restriction = restriction.substr(5);
  } else if (restriction.starts_with("no_")) {
    forbidden = true;
    restriction = restriction.substr(3);
  } else {
    if (!restriction.empty()) {
      if (verbosity >= Verbosity::Warning) {
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
  } else if (forbidden && restriction == "entry") {
    direction = TurnDirection::NoEntry;
  } else if (forbidden && restriction == "exit") {
    direction = TurnDirection::NoExit;
  } else {
    if (verbosity >= Verbosity::Warning) {
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
  if (!TurnRestrictionCollectIds(tagh, verbosity, relation, &from_ids, &to_ids,
                                 &via_ids, &via_type)) {
    res->num_error += 1;
    return;
  }
  if (from_ids.size() < 1 ||
      (from_ids.size() > 1 && direction != TurnDirection::NoEntry) ||
      to_ids.size() < 1 ||
      (to_ids.size() > 1 && direction != TurnDirection::NoExit) ||
      via_ids.empty() ||
      (via_type == OSMPBF::Relation::NODE && via_ids.size() != 1)) {
    if (verbosity >= Verbosity::Warning) {
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
                            .forbidden = forbidden,
                            .direction = direction};
      if (ConnectTurnRestriction(g, verbosity, &tr)) {
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

#if 0
// Sort the turn restrictions by the edge that triggers the
// restriction, i.e. the edge of the from way that connects to the via node.
inline void SortTurnRestrictions(std::vector<TurnRestriction>* trs) {
  std::sort(trs->begin(), trs->end(),
            [](const TurnRestriction& a, const TurnRestriction& b) {
              if (a.GetTriggerKey() != b.GetTriggerKey()) {
                return a.GetTriggerKey() < b.GetTriggerKey();
              }
              return a.path.back() < b.path.back();
            });
}
#endif

// Compute simple turn restrictions for the turn restrictions in 'trs'.
// Note that trs needs to be sorted by SortTurnRestrictions() and must
// contain simple restrictions only.
inline SimpleTurnRestrictionMap ComputeSimpleTurnRestrictionMap(
    const Graph& g, Verbosity verbosity,
    const std::vector<TurnRestriction>& trs) {
  SimpleTurnRestrictionMap res;
  if (trs.empty()) {
    return res;
  }
  size_t start = 0;
  for (size_t i = 0; i < trs.size(); ++i) {
    CHECK_S(trs.at(i).via_is_node);
    if (i == trs.size() - 1 ||
        trs.at(i).GetTriggerKey() != trs.at(i + 1).GetTriggerKey()) {
      SimpleTurnRestrictionData d;
      if (CreateSimpleTurnRestrictionData(
              g, verbosity,
              std::span<const TurnRestriction>(&(trs.at(start)), 1 + i - start),
              &d)) {
        res[trs.at(start).GetTriggerKey()] = d;
      }
      start = i + 1;
    }
  }
  return res;
}

inline void MarkSimpleViaNodes(Graph* g) {
  for (const auto& [key, data] : g->simple_turn_restriction_map) {
    g->nodes.at(key.to_node_idx).simple_turn_restriction_via_node = 1;
  }
}

#if 0
// Create a ComplexTurnRestrictionMap, which stores for every TriggerKey the
// first position in trs where that key occurs. Note that trs needs to be sorted
// by SortTurnRestrictions() and must contain complex turn  restrictions only.
inline ComplexTurnRestrictionMap ComputeComplexTurnRestrictionMap(
    Verbosity verbosity, const std::vector<TurnRestriction>& trs) {
  ComplexTurnRestrictionMap res;
  if (trs.empty()) {
    return res;
  }
  size_t start = 0;
  for (size_t i = 0; i < trs.size(); ++i) {
    CHECK_S(!trs.at(i).via_is_node);
    if (i == trs.size() - 1 ||
        trs.at(i).GetTriggerKey() != trs.at(i + 1).GetTriggerKey()) {
      res[trs.at(start).GetTriggerKey()] = start;
      start = i + 1;
    }
  }
  return res;
}
#endif

inline void MarkComplexTriggerEdges(Graph* g) {
  for (const auto& [key, pos] : g->complex_turn_restriction_map) {
    gnode_find_edge(*g, key.from_node_idx, key.to_node_idx, key.way_idx)
        .complex_turn_restriction_trigger = 1;
  }
}
