#pragma once

// Find out how predictable the paths are at each crossing. This works by
// determining a "default" outgoing edge at every crossing and counting
// how often a non-default is taken on the whole path.

#include <algorithm>
#include <fstream>
#include <memory>
#include <queue>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "algos/mm_router_defs.h"
#include "algos/routing_defs.h"
#include "algos/routing_metric.h"
#include "base/util.h"

MEdgeIdx GetBestTurnCostFollowEdge(const MMCluster& mc, MEdgeIdx edge_idx) {
  std::span<const uint8_t> tc = mc.get_turn_costs(edge_idx);
  if (tc.size() == 0) {
    return MEdgeIdx(INFU32);
  }
  uint32_t minpos = 0;
  for (uint32_t pos = 1; pos < tc.size(); ++pos) {
    if (tc[pos] < tc[minpos]) minpos = pos;
  }
  return mc.edge_start_idx(mc.get_edge(edge_idx).target_idx()) + minpos;
}

// Given the edge 'prev_edge_idx', return edge_idx of the default following
// edge. Returns INFU32 if no edge available.
MEdgeIdx GetBestWayIdxFollowEdge(const MMCluster& mc,
                                  MEdgeIdx prev_edge_idx) {
  // Use turn cost as proxy for "best continuation".
  std::span<const uint8_t> tc = mc.get_turn_costs(prev_edge_idx);
  if (tc.size() == 0) {
    return MEdgeIdx(INFU32);
  }

  const MWayIdx prev_way_idx = mc.edge_to_way.at(prev_edge_idx);
  MNodeIdx node_idx = mc.get_edge(prev_edge_idx).target_idx();
  MEdgeIdx start_edge_idx = mc.edge_start_idx(node_idx);
  uint32_t found_off = INFU32;

  for (uint32_t offset : mc.edge_offsets(node_idx)) {
    MEdgeIdx edge_idx = start_edge_idx + offset;
    if (mc.edge_to_way.at(edge_idx) == prev_way_idx) {
      CHECK_LT_S(offset, tc.size());
      // uturns often stay on the same way, so don't allow these..
      if (tc[offset] < TURN_COST_U_TURN_COMPRESSED &&
          (found_off == INFU32 || (tc[offset] < tc[found_off]))) {
        found_off = offset;
      }
    }
  }
  if (found_off == INFU32) {
    return MEdgeIdx(INFU32);
  }
  return start_edge_idx + found_off;
}

// Given the edge 'prev_edge_idx', return edge_idx of the default following
// edge. Returns INFU32 if no edge available.
MEdgeIdx GetBestStreetnameFollowEdge(const MMCluster& mc,
                                      MEdgeIdx prev_edge_idx) {
  // Use turn cost as proxy for "best continuation".
  std::span<const uint8_t> tc = mc.get_turn_costs(prev_edge_idx);
  if (tc.size() == 0) {
    return MEdgeIdx(INFU32);
  }

  const MWayIdx prev_way_idx = mc.edge_to_way.at(prev_edge_idx);
  std::string_view prev_streetname = mc.get_streetname(prev_way_idx);
  if (prev_streetname.empty()) {
    return MEdgeIdx(INFU32);
  }
  MNodeIdx node_idx = mc.get_edge(prev_edge_idx).target_idx();
  MEdgeIdx start_edge_idx = mc.edge_start_idx(node_idx);
  uint32_t found_off = INFU32;

  for (uint32_t offset : mc.edge_offsets(node_idx)) {
    MEdgeIdx edge_idx = start_edge_idx + offset;
    MWayIdx way_idx = mc.edge_to_way.at(edge_idx);
    if (prev_streetname == mc.get_streetname(way_idx)) {
      CHECK_LT_S(offset, tc.size());
      // uturns often stay on the same way, so don't allow these..
      if (tc[offset] < TURN_COST_U_TURN_COMPRESSED &&
          (found_off == INFU32 || tc[offset] < tc[found_off])) {
        found_off = offset;
      }
    }
  }
  if (found_off == INFU32) {
    return MEdgeIdx(INFU32);
  }
  return start_edge_idx + found_off;
}

// Check how well edge prediction would work.
void AnalyzePath(const MMCluster& mc, const std::vector<MEdgeIdx>& path) {
  // Wrong, but will display non-lowest-cost, which is correct.
  MEdgeIdx prev_edge_idx = path.at(0);
  uint32_t num_special = 0;
  LOG_S(INFO) << "Analyze path of length " << path.size();

  for (uint32_t i = 1; i < path.size(); ++i) {
    // uint32_t expand_node_idx = mc.get_edge(prev_edge_idx).target_idx();
    // Based on the previous edge, compute the default next edge.
    // uint32_t p = 0;
    MEdgeIdx predicted_edge = GetBestStreetnameFollowEdge(mc, prev_edge_idx);
    if (predicted_edge == INFU32) {
      // p = 1;
      predicted_edge = GetBestWayIdxFollowEdge(mc, prev_edge_idx);
    }
    if (predicted_edge == INFU32) {
      // p = 2;
      predicted_edge = GetBestTurnCostFollowEdge(mc, prev_edge_idx);
    }
    // LOG_S(INFO) << "predicted edge:" << predicted_edge << " p:" << p;

    MEdgeIdx edge_idx = path.at(i);
    // uint32_t way_idx = mc.edge_to_way.at(edge_idx);
    bool predicted_selected = (edge_idx == predicted_edge);
    num_special += !predicted_selected;
#if 0
    LOG_S(INFO) << absl::StrFormat(
        "%3u Edge predicted:%s node:%llu->%llu way_id:%llu streetname:<%s>", i,
        predicted_selected ? "*" : "-", mc.get_node_id(expand_node_idx),
        mc.get_node_id(mc.get_edge(edge_idx).target_idx()),
        mc.get_way_to_way_id(way_idx), mc.get_streetname(way_idx));
#endif
    prev_edge_idx = edge_idx;
  }
  LOG_S(INFO) << absl::StrFormat("AnalyzePath(): %u of %llu are special", num_special,
                                 path.size());
}
