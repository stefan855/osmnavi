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

  std::string DebugString(const MMCluster& mc, uint32_t origin_lat,
                          uint32_t origin_lon) const {
    CHECK_EQ_S(mc.cluster_id, fe.cluster_id);
    return absl::StrFormat(
        "Closest Edge for (%.7f, %.7f) dist:%.2fm n0:%lli n1:%lli fc:%.2f",
        origin_lat / TEN_POW_7_DBL, origin_lon / TEN_POW_7_DBL,
        distance_cm / 100.0, mc.get_node_id(fe.from_node_idx),
        mc.get_node_id(fe.edge(mc).target_idx()), fraction);
  }

  std::string DebugString(const MMGraph& mg, uint32_t origin_lat,
                          uint32_t origin_lon) const {
    return DebugString(mg.clusters.at(fe.cluster_id), origin_lat, origin_lon);
  }
};

// A location on the map, defined by one or more edge-points.
// (lat, lon) if for information only.
struct MMGeoAnchor {
  // The point selected by the user on the map.
  MMLatLon point = {0, 0};

  std::vector<MMEdgePoint> edge_points;

  void AddEdge(const MMCluster& mc, float fraction, const MMFullEdge& fe) {
    // LOG_S(INFO) << "Add edge with fraction: " << fraction;
    edge_points.push_back({.fraction = fraction, .fe = fe});
  }
  void AddEdge(const MMGraph& mg, float fraction, const MMFullEdge& fe) {
    AddEdge(fe.mc(mg), fraction, fe);
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

  // Find the position of an edge_idx in 'edge_points'. Returns INFU32 if
  // edge_idx doesn't exist in 'edge_points'.
  uint32_t FindPosByEdgeIdx(const MMCluster& mc, uint32_t edge_idx) const {
    for (uint32_t pos = 0; pos < edge_points.size(); ++pos) {
      if (edge_points.at(pos).fe.edge_idx(mc) == edge_idx) {
        return pos;
      }
    }
    return INFU32;
  }

  bool valid() const { return !edge_points.empty(); }

  struct Info {
    uint32_t cluster_id = INFU32;  // Cluster id of the first edge.
    uint32_t num_inner_edges = 0;
    uint32_t num_cross_edges = 0;
    bool all_same_cluster = false;  // true iff all edges have same cluster id.
  };

  Info GetInfo(const MMGraph& mg) const {
    Info res;
    for (const auto& ep : edge_points) {
      if (res.cluster_id == INFU32) {
        res.cluster_id = ep.fe.cluster_id;
        res.all_same_cluster = true;
      } else {
        if (ep.fe.cluster_id != res.cluster_id) {
          res.all_same_cluster = false;
        }
      }
      const MMCluster& mc = ep.fe.mc(mg);
      if (ep.fe.edge(mc).cluster_border_edge()) {
        res.num_cross_edges++;
      } else {
        res.num_inner_edges++;
      }
    }
    return res;
  }
};

struct MMClusterRouterStatus {
  bool finished = false;
  bool found = false;
  uint32_t last_v_idx = INFU32;
};

struct MMRoutingResult {
  MMEdgePoint start;
  MMEdgePoint target;
  std::vector<MMFullEdge> full_edges;
  std::vector<uint32_t> min_metrics;
  std::vector<uint32_t> edge_weights;
  uint32_t final_metric = 0;  // Metric after travelling the last edge.
  // Indicates if the first/last edge are part of the given anchors.
  bool start_is_anchor = false;
  bool target_is_anchor = false;
  double time_for_expand_hybrid_clusters = 0.0;

  uint32_t distance_cm(const MMGraph& mg, uint32_t fe_pos) const {
    const MMFullEdge& fe = full_edges.at(fe_pos);
    const MMCluster& mc = fe.mc(mg);
    uint32_t dist = mc.edge_to_distance.at(fe.edge_idx(mc));
    if (fe_pos > 0 && fe_pos + 1 < full_edges.size()) {
      // Not first and/or last edge.
      return dist;
    }
    // Handle also the case when there is only one edge, i.e. start is equal to
    // target.
    float del_frac_start = (fe_pos == 0 ? start.fraction : 0.0);
    float del_frac_target =
        (fe_pos == full_edges.size() - 1 ? 1.0 - target.fraction : 0.0);
    return std::lround(dist * (1.0 - del_frac_start - del_frac_target));
  }
};

#if 0
struct MMRoutingResult {
  MMEdgePoint start;
  MMEdgePoint target;
  std::vector<MMFullEdge> full_edges;
  std::vector<uint32_t> edge_weights;
  std::vector<bool> gaps;

  uint32_t final_metric = 0;  // Metric after travelling the last edge.

  uint32_t distance_cm(const MMCluster& mc, uint32_t fe_pos) const {
    uint32_t dist = mc.edge_to_distance.at(full_edges.at(fe_pos).edge_idx(mc));
    if (fe_pos > 0 && fe_pos + 1 < full_edges.size()) {
      // Not first and/or last edge.
      return dist;
    }
    // Handle also the case when there is only one edge, i.e. start is equal to
    // target.
    float del_frac_start = (fe_pos == 0 ? start.fraction : 0.0);
    float del_frac_target =
        (fe_pos == full_edges.size() - 1 ? 1.0 - target.fraction : 0.0);
    return std::lround(dist * (1.0 - del_frac_start - del_frac_target));
  }

  uint32_t distance_cm(const MMGraph& mg, uint32_t fe_pos) const {
    return distance_cm(full_edges.at(fe_pos).mc(mg), fe_pos);
  }
};

#endif
