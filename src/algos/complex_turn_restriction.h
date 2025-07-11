#pragma once

#include "base/constants.h"
#include "base/deduper_with_ids.h"
#include "graph/compact_graph.h"
#include "graph/graph_def.h"

// Complex turn restriction vector data structures.
struct CTRPosition {
  // The referenced turn restriction is stored in
  // g.complex_turn_restrictions.at(ctr_idx).
  uint32_t ctr_idx;
  // All edges including the edge at 'position' have been traveled.
  uint32_t position;

  bool operator==(const CTRPosition& other) const {
    return ctr_idx == other.ctr_idx && position == other.position;
  }

  // Tries to add an edge to the path defined by (ctr_idx, position). There are
  // three cases distinguished:
  //
  // 1) Returns FOLLOW if the edge is he next edge in the turn restriction.
  // Increments position by 1.
  // 2) Returns LEAVE when the edge is not part of the path of the turn
  // restriction.
  // 3) Returns FORBID when the edge is forbidden by the turn restriction.
  enum Status { FOLLOW, LEAVE, FORBID };

  // Called when adding an edge to a complex turn restriction that has already
  // been triggered and ends at the previous edge. Therefore, it is not
  // necessary to check 'from_idx' of the new edge, it is already known that it
  // starts at the correct node.
  Status AddNextEdge(const Graph& g, const GEdge& edge) {
    const TurnRestriction& ctr = g.complex_turn_restrictions.at(ctr_idx);
    // We're not allowed to point to the last edge in the path.
    CHECK_LT_S(position + 1, ctr.path.size());
    const TurnRestriction::TREdge& tr_edge = ctr.path.at(position + 1);
    const bool ctr_edge_match = tr_edge.way_idx == edge.way_idx &&
                                tr_edge.to_node_idx == edge.other_node_idx;
    return UpdateNewEdge(ctr.path.size(), ctr_edge_match, ctr.forbidden);
  }

  Status AddNextEdge(const CompactDirectedGraph& cg, uint32_t edge_idx) {
    const CompactDirectedGraph::ComplexTurnRestriction& ctr =
        cg.GetComplexTRS().at(ctr_idx);
    // We're not allowed to point to the last edge in the path.
    CHECK_LT_S(position + 1, ctr.path.size());
    const bool ctr_edge_match = (edge_idx == ctr.path.at(position + 1));
    // LOG_S(INFO) << "BB: match:" << ctr_edge_match << " " << edge_idx << "=?"
    //             << ctr.path.at(position + 1);
    return UpdateNewEdge(ctr.path.size(), ctr_edge_match, ctr.forbidden);
  }

  // Compute the new status of the CTR. Increments the position when the
  // returned status is FOLLOW.
  Status UpdateNewEdge(uint32_t ctr_path_size, bool ctr_edge_match,
                       bool ctr_forbidden) {
    if (position + 2 < ctr_path_size) {
      // We don't reach the last leg.
      if (ctr_edge_match) {
        position += 1;
        return FOLLOW;
      } else {
        return LEAVE;
      }
    } else {
      CHECK_EQ_S(position + 2, ctr_path_size);
      // Little table that list for each combination of (match, forbidden) flags
      // the desired result.
      //
      // match forbidden result
      //   0       0     FORBID
      //   0       1     LEAVE, .i.e ok.
      //   1       0     LEAVE, .i.e ok.
      //   1       1     FORBID
      if (ctr_edge_match == ctr_forbidden) {
        return FORBID;
      } else {
        return LEAVE;
      }
    }
  }
};

using ActiveCtrs = std::vector<CTRPosition>;

// Add an edge to the path currently followed by active_ctrs. Modifies
// active_ctrs by advancing and removing individual turn restrictions to reflect
// the added edge.
//
// Returns true if it is allowed to follow the edge, and modifies active_ctrs to
// reflect the new state. Returns false if some turn restriction prohibits
// following the edge. In this case, active_ctrs is unchanged.
template <class GraphType, typename EdgeType>
inline bool ActiveCtrsAddNextEdge(const GraphType& gt, const EdgeType& edge,
                                  ActiveCtrs* active_ctrs) {
  ActiveCtrs new_ctrs;
  for (CTRPosition ctrp : *active_ctrs) {
    switch (ctrp.AddNextEdge(gt, edge)) {
      case CTRPosition::FOLLOW:
        new_ctrs.push_back(ctrp);
        break;
      case CTRPosition::LEAVE:
        break;
      case CTRPosition::FORBID:
        return false;
        break;
      default:
        ABORT_S();
    }
  }
  *active_ctrs = new_ctrs;
  return true;
}

// This deduper is used to allocate unique Ids for CTR configurations.
using CTRDeDuper = DeDuperWithIds<ActiveCtrs>;

inline std::string ActiveCtrsToDebugString(const Graph& g,
                                           const ActiveCtrs& active_ctrs) {
  std::string res;
  for (const CTRPosition ctr_pos : active_ctrs) {
    const TurnRestriction& tr = g.complex_turn_restrictions.at(ctr_pos.ctr_idx);
    const TurnRestriction::TREdge& tr_edge = tr.path.at(ctr_pos.position);
    absl::StrAppend(&res,
                    absl::StrFormat("%s(TR:%lld pos:%u f:%lld t:%lld w:%lld)",
                                    res.size() > 0 ? " " : "", tr.relation_id,
                                    ctr_pos.position,
                                    GetGNodeIdSafe(g, tr_edge.from_node_idx),
                                    GetGNodeIdSafe(g, tr_edge.to_node_idx),
                                    GetGWayIdSafe(g, tr_edge.way_idx)));
  }
  return res;
}

namespace std {
template <>
struct hash<ActiveCtrs> {
  size_t operator()(const ActiveCtrs& ctr_config) const {
    // Use already defined std::string_view hashes.
    return std::hash<std::string_view>{}(std::string_view(
        (char*)ctr_config.data(), ctr_config.size() * sizeof(ctr_config[0])));
  }
};
}  // namespace std
