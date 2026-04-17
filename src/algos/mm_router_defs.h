#pragma once

#include <vector>

#include "base/util.h"
#include "graph/mmgraph_def.h"

// A point on an edge. The position of the point is given as 'fraction', which
// is measured from the beginning of the edge. Fraction is in the range [0..1].
struct MMEdgePoint {
  uint32_t distance_cm = 0;
  // fraction_pos [0...1] gives the distance of the point from the start of the
  // edge. 0 means that the point is at the start, i.e. at fe.from_node_idx. 1
  // means that the start is at the end of the edge.
  // All values between indicate a location between start and end.
  float fraction = 0.0;
  int32_t lat_at_fraction = 0;
  int32_t lon_at_fraction = 0;
  MMFullEdge fe = {};

  float GetWeightFractionWhenStarting() const {
    return std::max(0.0f, std::min(1.0f, 1.0f - fraction));
  }
  float GetWeightFractionWhenFinishing() const {
    return std::max(0.0f, std::min(1.0f, fraction));
  }

  std::string DebugString(const MMGraph& mg, uint32_t origin_lat,
                          uint32_t origin_lon) const {
    const MMCluster& mc = mg.clusters.at(fe.cluster_id);
    return absl::StrFormat(
        "Closest Edge for (%.7f, %.7f) dist:%.2fm n0:%lli n1:%lli fc:%.2f",
        origin_lat, origin_lon, distance_cm / 100.0,
        mc.get_node_id(fe.from_node_idx),
        mc.get_node_id(fe.edge(mc).target_idx()), fraction);
  }
};

// A location on the map, defined by one or more edge-points.
// (lat, lon) if for information only.
struct MMGeoAnchor {
  // The point selected by the user on the map.
  MMLatLon point = {0, 0};

  std::vector<MMEdgePoint> edge_points;

  void AddEdge(const MMCluster& mc, float fraction, const MMFullEdge& fe) {
    LOG_S(INFO) << "Add edge with fraction: " << fraction;
    edge_points.push_back({.fraction = fraction, .fe = fe});
  }

  void AddEdge(const MMCluster& mc, float fraction, uint32_t from_node_idx,
               uint32_t offset) {
    AddEdge(mc, fraction, {from_node_idx, mc.cluster_id, offset});
  }

  void AddStartNode(const MMCluster& mc, uint32_t node_idx) {
    for (uint32_t off : mc.edge_offsets(node_idx)) {
      AddEdge(mc, 0.0, node_idx, off);
    }
  }

  void AddTargetNode(const MMCluster& mc, uint32_t node_idx) {
    for (const MMFullEdge& fe : mm_get_incoming_edges_slow(mc, node_idx)) {
      AddEdge(mc, 1.0, fe);
    }
  }

  // Find the position of and edge_idx in 'edge_points'. Returns -1 if edge_idx
  // doesn't exist in 'edge_points'.
  int64_t FindPosByEdgeIdx(const MMCluster& mc, uint32_t edge_idx) const {
    for (uint32_t pos = 0; pos < edge_points.size(); ++pos) {
      if (edge_points.at(pos).fe.edge_idx(mc) == edge_idx) {
        return pos;
      }
    }
    return -1;
  }

  bool valid() const { return !edge_points.empty(); }
};

struct MMRoutingResult {
  bool found = false;
  // If a route was found, the distance from start to target node.
  uint32_t found_distance = 0;
  MMFullEdge start_edge;
  MMFullEdge target_edge;
  uint32_t num_shortest_route_nodes = 0;
  std::vector<uint32_t> route_v_idx;  // Filled iff found == true.

  // Internal stats.
  uint32_t num_visited = 0;  // edges or nodes, depending on router type.
  uint32_t num_complex_turn_restriction_keys = 0;
  float complex_turn_restriction_keys_reduction_factor = 0;
};
