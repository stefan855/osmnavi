#pragma once

/*
 * We want to label the areas of the road network that have restricted access.
 * When these areas are properly labeled, then we can use this during shortest
 * route search to disallow traveling restricted edges except for the start and
 * the end of the route. We also make sure that all restricted areas end up in
 * the same cluster during clustering, so we never need to handle restricted
 * edges when travelling across clusters.
 *
 * Problems:
 * 1) Edges in service (or similar) areas are often not labeled as restricted,
 * although they are only reachable through the restricted area. They need to be
 * added to the restricted areas.
 *
 * 2) Ways with different restriction per direction (e.g. forward:restricted,
 * backward:free) make the definition of restriction areas somewhat fuzzy.
 *
 * 3) On an edge, each vehicle type may have its own restriction.
 *
 * Algorithm:
 * 1) Label edges in the graph as either restricted or free edges. Edges with
 * access ACC_CUSTOMERS, ACC_DELIVERY, ACC_DESTINATION are directly labelled as
 * LABEL_RESTRICTED. Edges only reachable through these restricted edges are
 * LABEL_RESTRICTED_SECONDARY.
 *
 * 2) All other edges are labeled as LABEL_FREE, unless the labeling algorithm
 * finds something strange, which causes them to be labelled
 * LABEL_STRANGE.
 *
 * 3) Process: Iterate through all unset edges and for each unset edge, label
 * the reachable subnetwork with 'TEMPORARY'. If the detected subnetwork is
 * small, contains only service-type roads and connects to restricted edges,
 * then relabel it as LABEL_RESTRICTED_SECONDARY, otherwise as LABEL_FREE. In a
 * few cases the subnetwork looks strange, e.g. when at is small and contains
 * some non-service type roads. In this cases it is labeled
 * LABEL_STRANGE.
 */

#include <queue>
#include <vector>

#include "base/util.h"
#include "graph/graph_def.h"

namespace {
// Starting with node start_idx', follow all edges with label 'follow_label'.
// Mark all followed edges with new label 'set_label'.
struct LabelEdgesResult {
  uint32_t count = 0;
  bool found_restricted = false;
  bool only_residential_street_types = true;
};

inline LabelEdgesResult LabelCarEdges(std::uint32_t start_idx,
                                      GEdge::RESTRICTION follow_label,
                                      GEdge::RESTRICTION set_label, Graph* g) {
  LabelEdgesResult res;
  std::vector<std::uint32_t> queue;
  queue.push_back(start_idx);
  while (!queue.empty()) {
    const uint32_t node_idx = queue.back();
    queue.pop_back();
    GNode& n = g->nodes.at(node_idx);
    for (uint32_t pos = 0; pos < gnode_total_edges(n); ++pos) {
      GEdge& e = n.edges[pos];
      if (e.car_label == follow_label) {
        // LOG_S(INFO) << "Set label " << e.car_label << " to " << follow_label;
        e.car_label = set_label;
        queue.push_back(e.other_node_idx);
        res.count += 1;
        const HIGHWAY_LABEL hw = g->ways.at(e.way_idx).highway_label;
        if (hw != HW_TERTIARY && hw != HW_RESIDENTIAL &&
            hw != HW_UNCLASSIFIED && hw != HW_LIVING_STREET &&
            hw != HW_SERVICE && hw != HW_TRACK && hw != HW_CYCLEWAY &&
            hw != HW_FOOTWAY && hw != HW_PEDESTRIAN && hw != HW_PATH &&
            hw != HW_BRIDLEWAY) {
          res.only_residential_street_types = false;
        }
      } else if (e.car_label == GEdge::LABEL_RESTRICTED) {
        res.found_restricted = true;
      }
    }
  }
  return res;
}
}  // namespace

void inline LabelAllCarEdges(Graph* g) {
  FUNC_TIMER();
  for (uint32_t i = 0; i < g->nodes.size(); ++i) {
    const auto res =
        LabelCarEdges(i, GEdge::LABEL_UNSET, GEdge::LABEL_TEMPORARY, g);
    if (res.count == 0) {
      // Start node is already done, looks good.
    } else if (res.count < 500 && res.found_restricted &&
               res.only_residential_street_types) {
      LOG_S(INFO) << "Mark secondary:" << res.count
                  << " id:" << g->nodes.at(i).node_id;
      const auto res2 = LabelCarEdges(i, GEdge::LABEL_TEMPORARY,
                                      GEdge::LABEL_RESTRICTED_SECONDARY, g);
      CHECK_EQ_S(res.count, res2.count);
    } else if (!res.found_restricted) {
      // This is a network without any restricted roads, looks fine.
      const auto res2 =
          LabelCarEdges(i, GEdge::LABEL_TEMPORARY, GEdge::LABEL_FREE, g);
      CHECK_EQ_S(res.count, res2.count);
    } else if (res.count > 10000) {
      LOG_S(INFO) << "Mark large:" << res.count
                  << " id:" << g->nodes.at(i).node_id;
      // This is a large network, looks fine.
      const auto res2 =
          LabelCarEdges(i, GEdge::LABEL_TEMPORARY, GEdge::LABEL_FREE, g);
      CHECK_EQ_S(res.count, res2.count);
    } else if (g->nodes.at(i).large_component == 0) {
      LOG_S(INFO) << "Mark small comp:" << res.count
                  << " id:" << g->nodes.at(i).node_id;
      // Small component, and it didn't fit 'secondary', mark as free.
      const auto res2 =
          LabelCarEdges(i, GEdge::LABEL_TEMPORARY, GEdge::LABEL_FREE, g);
      CHECK_EQ_S(res.count, res2.count);
    } else {
      LOG_S(INFO) << "Mark strange:" << res.count
                  << " id:" << g->nodes.at(i).node_id
                  << " found restricted:" << res.found_restricted
                  << " only residential:" << res.only_residential_street_types
                  << " large_comp:" << g->nodes.at(i).large_component;

      // Something is not understood right now.
      const auto res2 =
          LabelCarEdges(i, GEdge::LABEL_TEMPORARY, GEdge::LABEL_STRANGE, g);
      CHECK_EQ_S(res.count, res2.count);
    }
  }
}
