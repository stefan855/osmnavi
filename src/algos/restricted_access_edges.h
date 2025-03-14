#pragma once

/*
 * We want to label the areas of the road network that have destination-only or
 * otherwise restricted access. When these areas are properly labeled, then we
 * can use this during shortest route search to disallow traveling restricted
 * edges except for the start and the end of the route. We also make sure that
 * all restricted access areas end up in the same cluster during clustering, so
 * we never need to handle restricted edges when travelling across clusters.
 *
 * Problems to solve:
 * 1) Nodes can't be labeled 'restricted' or 'free', but edges can.
 * 2) Some ways are restricted in one direction, but unrestricted in the other.
 * 3) Some unrestricted edges in service areas (or similar) are only reachable
 * through restricted edges. They should be labeled restricted-secondary.
 * 4) On an edge, every vehicle type may have its own restriction.
 *
 * Algorithm:
 * Target: Label edges in the graph as either restricted, restricted-secondary
 * or free edges.
 *
 * 2) Edges with access ACC_CUSTOMERS, ACC_DELIVERY, ACC_DESTINATION are labeled
 * as LABEL_RESTRICTED on creation of the edge, all other edges are LABEL_UNSET.
 *
 * 3) Edges only reachable through these restricted edges are
 * LABEL_RESTRICTED_SECONDARY.
 *
 * 4) All other edges are labeled as LABEL_FREE.
 *
 * 5) If the labeling algorithm finds something strange, car_label_strange is
 * set on the edge, to help with debugging.
 *
 * 6) Process: Iterate through all unset edges and for each unset edge, label
 * the reachable subnetwork LABEL_TEMPORARY. If the detected subnetwork is
 * small, contains only service-type roads and connects to restricted edges,
 * then relabel it as LABEL_RESTRICTED_SECONDARY, otherwise as LABEL_FREE. In a
 * few cases the subnetwork looks strange, e.g. it is small and contains some
 * non-service type roads. In this case the edge flag car_label_strange is set
 * in addition to labelling it also as LABEL_RESTRICTED_SECONDARY.
 */

#include <queue>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "base/util.h"
#include "graph/graph_def.h"

namespace {
// Starting with node start_idx', follow all edges with label 'follow_label'.
// Mark all followed edges with new label 'set_label'.
struct LabelEdgesResult {
  // Number of labeled edges.
  uint32_t count = 0;
  // At least one restricted edge was seen and not followed.
  bool found_restricted = false;
  // Only residential street types have been followed.
  bool only_residential_street_types = true;
};

// Label all reachable edges that have edge flag 'follow_label' with
// 'set_label'.
inline LabelEdgesResult LabelCarEdges(std::uint32_t start_idx,
                                      GEdge::RESTRICTION follow_label,
                                      GEdge::RESTRICTION set_label,
                                      bool set_strange, Graph* g) {
  CHECK_NE_S(follow_label, GEdge::LABEL_RESTRICTED);
  LabelEdgesResult res;
  std::vector<std::uint32_t> queue;
  queue.push_back(start_idx);
  while (!queue.empty()) {
    const uint32_t node_idx = queue.back();
    queue.pop_back();

    // Iterate over the undirected graph and label all edges, also inverted and
    // non-unique ones.
    for (GEdge& e : gnode_all_edges(*g, node_idx)) {
      if (e.car_label == follow_label) {
        e.car_label = set_label;
        e.car_label_strange = set_strange;
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

// Labels all car edges in the graph that have GEdge::LABEL_UNSET, i.e. all
// edges that were not labelled ACC_CUSTOMERS, ACC_DELIVERY, ACC_DESTINATION
// during creation.
inline void LabelAllCarEdges(Graph* g, Verbosity verbosity) {
  constexpr bool strange_no = false;
  constexpr bool strange_yes = true;
  FUNC_TIMER();
  for (uint32_t i = 0; i < g->nodes.size(); ++i) {
    // Label all edges reachable through unset edges as temporary. Then decide
    // what to do with these edges.
    const LabelEdgesResult res = LabelCarEdges(
        i, GEdge::LABEL_UNSET, GEdge::LABEL_TEMPORARY, strange_no, g);

    if (res.count == 0) {
      // Start node is already done or completely restricted, nothing to do.
    } else if (res.count < 500 && res.found_restricted &&
               res.only_residential_street_types) {
      if (verbosity >= Verbosity::Debug) {
        LOG_S(INFO) << "Mark secondary:" << res.count
                    << " node_id:" << g->nodes.at(i).node_id;
      }
      const auto res2 =
          LabelCarEdges(i, GEdge::LABEL_TEMPORARY,
                        GEdge::LABEL_RESTRICTED_SECONDARY, strange_no, g);
      CHECK_EQ_S(res.count, res2.count);
    } else if (!res.found_restricted) {
      // This is a network without any restricted roads, label as free.
      const auto res2 = LabelCarEdges(i, GEdge::LABEL_TEMPORARY,
                                      GEdge::LABEL_FREE, strange_no, g);
      CHECK_EQ_S(res.count, res2.count);
    } else if (res.count > 10000) {
      if (verbosity >= Verbosity::Debug) {
        LOG_S(INFO) << "Mark large:" << res.count
                    << " node_id:" << g->nodes.at(i).node_id;
      }
      // This is a large network, label as free.
      const auto res2 = LabelCarEdges(i, GEdge::LABEL_TEMPORARY,
                                      GEdge::LABEL_FREE, strange_no, g);
      CHECK_EQ_S(res.count, res2.count);
    } else if (g->nodes.at(i).large_component == 0) {
      if (verbosity >= Verbosity::Debug) {
        LOG_S(INFO) << "Mark small comp:" << res.count
                    << " node_id:" << g->nodes.at(i).node_id;
      }
      // Small component, and it didn't fit 'secondary', label as free.
      const auto res2 = LabelCarEdges(i, GEdge::LABEL_TEMPORARY,
                                      GEdge::LABEL_FREE, strange_no, g);
      CHECK_EQ_S(res.count, res2.count);
    } else {
      CHECK_S(res.found_restricted);
      if (verbosity >= Verbosity::Verbose) {
        LOG_S(INFO) << "Mark strange:" << res.count
                    << " node_id:" << g->nodes.at(i).node_id
                    << " country:" << CountryNumToString(g->nodes.at(i).ncc)
                    << " found restricted:" << res.found_restricted
                    << " only residential:" << res.only_residential_street_types
                    << " large_comp:" << g->nodes.at(i).large_component;
      }

      // Something is not well understood right now. Label as restricted but
      // also mark as strange for debugging.
      const auto res2 =
          LabelCarEdges(i, GEdge::LABEL_TEMPORARY,
                        GEdge::LABEL_RESTRICTED_SECONDARY, strange_yes, g);
      CHECK_EQ_S(res.count, res2.count);
    }
  }
}

// Return all nodes in the restricted access area at 'node_idx' that are both
// connected to free and also to restricted edges. These are the nodes where a
// transition from free to the restricted-access area (and vice versa) can
// happen.
inline absl::flat_hash_set<uint32_t> GetRestrictedAccessTransitionNodes(
    const Graph& g, std::uint32_t node_idx) {
  absl::flat_hash_set<uint32_t> done;
  absl::flat_hash_set<uint32_t> transition_nodes;
  std::vector<std::uint32_t> queue;
  queue.push_back(node_idx);
  done.insert(node_idx);
  while (!queue.empty()) {
    const uint32_t node_idx = queue.back();
    queue.pop_back();
    bool has_free = false;
    bool has_restricted = false;
    for (const GEdge& e : gnode_all_edges(g, node_idx)) {
      if (e.car_label == GEdge::LABEL_FREE) {
        has_free = true;
      } else {
        has_restricted = true;
        if (!done.contains(e.other_node_idx)) {
          queue.push_back(e.other_node_idx);
          done.insert(e.other_node_idx);
        }
      }
    }
    if (has_free && has_restricted) {
      transition_nodes.insert(node_idx);
    }
  }
  return transition_nodes;
}
