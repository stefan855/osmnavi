#pragma once

#include <vector>

#include "base/util.h"
#include "graph/mmgraph_def.h"

// A point on an edge. The position of the point is given as 'to_fraction',
// which is measured from the beginning of the edge. The value is in the range
// [0..1].
struct EdgePoint {
  uint32_t distance_to_seg_cm = 0;
  // 'to_fraction' [0...1] gives the distance *to* the point on the edge.
  // 0.0 means that the point is at the beginning of the edge, i.e. at
  // fe.from_node_idx. 1.0 means that the start is at the end of the edge. All
  // values between indicate a location between start and end.
  float to_fraction = 0.0;
  LatLon ll_at_fraction;
  // LatE6 lat_at_fraction;
  // LonE6 lon_at_fraction;
  MMFullEdge fe = {};

  float GetFromFraction() const {
    return std::max(0.0f, std::min(1.0f, 1.0f - to_fraction));
  }
  float GetToFraction() const {
    return std::max(0.0f, std::min(1.0f, to_fraction));
  }

  std::string DebugString(const MMCluster& mc, LatE6 origin_lat = {},
                          LonE6 origin_lon = {}) const {
    CHECK_EQ_S(mc.cluster_id, fe.cluster_id);
    return absl::StrFormat(
        "Closest Edge to (%.7f, %.7f) dist:%.2fm cl:%u n0:%lli n1:%lli fc:%.2f",
        origin_lat.AsDouble(), origin_lon.AsDouble(),
        distance_to_seg_cm / 100.0, fe.cluster_id,
        mc.get_node_id(fe.from_node_idx),
        mc.get_node_id(fe.edge(mc).target_idx()), to_fraction);
  }

  std::string DebugString(const MMGraph& mg, LatE6 origin_lat = {},
                          LonE6 origin_lon = {}) const {
    return DebugString(mg.clusters.at(fe.cluster_id), origin_lat, origin_lon);
  }
};

// A location on the map, defined by one or more edge-points.
// 'point' if for information only.
class GeoAnchor {
 public:
  GeoAnchor() : point_({}){};
  GeoAnchor(const LatLon& point) : point_(point) {}

  void AddEdge(const EdgePoint& ep) { push_edge(ep); }
  void AddEdge(const MMCluster& mc, float to_fraction, const MMFullEdge& fe) {
    // LOG_S(INFO) << "Add edge with fraction: " << fraction;
    push_edge({.to_fraction = to_fraction, .fe = fe});
  }
  void AddEdge(const MMGraph& mg, float to_fraction, const MMFullEdge& fe) {
    AddEdge(fe.mc(mg), to_fraction, fe);
  }

  void AddEdge(const MMCluster& mc, float to_fraction, uint32_t from_node_idx,
               uint32_t offset) {
    AddEdge(mc, to_fraction, {from_node_idx, mc.cluster_id, offset});
  }

  void AddStartNode(const MMCluster& mc, uint32_t node_idx) {
    for (uint32_t off : mc.edge_offsets(node_idx)) {
      AddEdge(mc, /*to_fraction=*/0.0, node_idx, off);
    }
  }

  void AddTargetNode(const MMCluster& mc, uint32_t node_idx) {
    for (const MMFullEdge& fe : mm_get_incoming_edges_slow(mc, node_idx)) {
      AddEdge(mc, /*to_fraction=*/1.0, fe);
    }
  }

  // Find the position of an edge_idx in 'edge_points'. Returns INFU32 if
  // edge_idx doesn't exist in 'edge_points'.
  uint32_t FindPosByEdgeIdx(const MMCluster& mc, uint32_t edge_idx) const {
    for (uint32_t pos = 0; pos < edge_points_.size(); ++pos) {
      if (edge_points_.at(pos).fe.edge_idx(mc) == edge_idx) {
        return pos;
      }
    }
    return INFU32;
  }

  struct Info {
    // Cluster id of the full edges. Note that it is enforced that all edge
    // points belong to the same cluster.
    uint32_t cluster_id;
    // Non-cross-cluster edges.
    uint32_t num_inner_edges;
    // Cross-cluster edges.
    uint32_t num_cross_edges;
    // All edges connect the same pair of nodes. This normally happens when a
    // two way road is added as edge point, then edges are a->b and b->a.
    bool same_node_pair;
    bool has_cross_cluster_id() const {
      return same_node_pair && (num_inner_edges == 0) && (num_cross_edges > 0);
    }
    // Id of the connected cluster, only valid when has_cross_cluster_id().
    uint32_t cross_cluster_id;
  };

  Info GetInfo(const MMGraph& mg) const {
    Info res = {.cluster_id = INFU32,
                .num_inner_edges = 0,
                .num_cross_edges = 0,
                .same_node_pair = false,
                .cross_cluster_id = INFU32};

    for (size_t i = 0; i < edge_points_.size(); ++i) {
      const EdgePoint ep = edge_points_.at(i);
      const MMCluster& mc = mg.mc(ep.fe.cluster_id);
      if (ep.fe.edge(mc).cross_cluster_edge()) {
        res.num_cross_edges++;
      } else {
        res.num_inner_edges++;
      }
      if (i == 0) {
        res.cluster_id = ep.fe.cluster_id;
        res.same_node_pair = true;
      } else {  // (i > 0)
        // Check if same_node_pair is still true.
        const EdgePoint& prev = edge_points_.at(i - 1);
        CHECK_EQ_S(ep.fe.cluster_id, prev.fe.cluster_id);
        // We're in the same cluster, so compare from/to node indices.
        const uint32_t ep_from = ep.fe.from_node_idx;
        const uint32_t ep_to = ep.fe.target_idx(mc);
        const uint32_t prev_from = prev.fe.from_node_idx;
        const uint32_t prev_to = prev.fe.target_idx(mc);
        const bool match = (ep_from == prev_from && ep_to == prev_to) ||
                           (ep_from == prev_to && ep_to == prev_from);
        if (!match) {
          res.same_node_pair = false;
        }
      }
    }
    if (res.has_cross_cluster_id()) {
      MMFullEdge fe = edge_points_.front().fe;
      res.cross_cluster_id = fe.GetCrossClusterId(fe.mc(mg));
      CHECK_NE_S(res.cluster_id, res.cross_cluster_id);
    }
    return res;
  }

  // Create a new anchor that contains all edge points that exist in 'new
  // cluster'. Note that this converts cross cluster edges if possible and drops
  // edges that can't be adapted to 'new_cluster_id'.
  //
  // This can be triggered by putting target on a cross cluster edge and
  // start on an inner edge that is directly connected to the cross-cluster
  // edge. One of the two possible clusters will trigger adaption.
  GeoAnchor AdaptToCluster(const MMGraph& mg, uint32_t new_cluster_id) const {
    LOG_S(INFO) << "Adapt edge point to " << new_cluster_id;
    GeoAnchor a(point_);
    for (const EdgePoint& ep : edge_points_) {
      LOG_S(INFO) << "Adapt edge: " << ep.DebugString(mg);
      if (ep.fe.cluster_id == new_cluster_id) {
        // Copy as is.
        LOG_S(INFO) << "Adapt: Copy";
        a.push_edge(ep);
      } else if (ep.fe.IsCrossClusterEdge(ep.fe.mc(mg)) &&
                 ep.fe.GetCrossClusterId(ep.fe.mc(mg)) == new_cluster_id) {
        EdgePoint new_ep = ep;
        new_ep.fe = ep.fe.GetDualCrossClusterEdge(mg);
        a.push_edge(new_ep);
        LOG_S(INFO) << "Adapted edge: " << new_ep.DebugString(mg);
      } else {
        LOG_S(INFO) << "Adapt: Drop";
      }
    }
    return a;
  }

  const std::vector<EdgePoint>& edge_points() const { return edge_points_; }
  const LatLon& point() const { return point_; }

 private:
  void push_edge(const EdgePoint& ep) {
    if (!edge_points_.empty()) {
      CHECK_EQ_S(edge_points_.back().fe.cluster_id, ep.fe.cluster_id);
    }
    edge_points_.push_back(ep);
  }

  // The point selected by the user on the map.
  LatLon point_;
  std::vector<EdgePoint> edge_points_;
};

struct MMClusterRouterStatus {
  bool finished = false;
  bool found = false;
  uint32_t last_v_idx = INFU32;
};

struct MMRoutingResult {
  EdgePoint start;
  EdgePoint target;
  std::vector<MMFullEdge> full_edges;
  std::vector<uint32_t> min_metrics;
  // std::vector<uint32_t> edge_metric;
  uint32_t final_metric = 0;  // Metric after travelling the last edge.
  // Indicates if the first/last edge are part of the given anchors.
  bool start_is_anchor = false;
  bool target_is_anchor = false;

  // Statistics
  int32_t num_path_full_clusters = 0;  // <= 2 for start and end cluster.
  int32_t num_vis_start = 0;           // Hybrid clusters in the path.
  int32_t num_vis_target = 0;          // Hybrid clusters in the path.
  int32_t num_vis_hybrid = 0;          // Hybrid clusters in the path.
  double time_for_route_algorithm = 0.0;
  double time_for_assemble = 0.0;
  double time_for_expand_hybrid_clusters = 0.0;

  // Return the amount of 'metric' that was used on edge 'fe_pos'.
  uint32_t edge_metric(uint32_t fe_pos) const {
    return min_metrics.at(fe_pos) -
           (fe_pos > 0 ? min_metrics.at(fe_pos - 1) : 0);
  }

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
    float del_frac_start = (fe_pos == 0 ? start.to_fraction : 0.0);
    float del_frac_target =
        (fe_pos == full_edges.size() - 1 ? 1.0 - target.to_fraction : 0.0);
    return std::lround(dist * (1.0 - del_frac_start - del_frac_target));
  }
};
