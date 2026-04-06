#pragma once

#include <vector>

#include "base/util.h"
#include "graph/mmgraph_def.h"

// A point on an edge. 'fraction' is mesaured from the begiining of the edge and
// is in the range [0..1].
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

  std::vector<MMEdgePoint> edges;

  void AddEdge(const MMCluster& mc, uint32_t from_node_idx, uint32_t offset,
               float fraction) {
    LOG_S(INFO) << "Add edge with fraction: " << fraction;
    edges.push_back(
        {.fraction = fraction, .fe = {from_node_idx, mc.cluster_id, offset}});
  }

  void AddStartNode(const MMCluster& mc, uint32_t node_idx) {
    for (uint32_t off : mc.edge_offsets(node_idx)) {
      AddEdge(mc, node_idx, off, 0.0);
    }
  }

  int FindPos(const MMCluster& mc, uint32_t edge_idx) const {
    for (uint32_t pos = 0; pos < edges.size(); ++pos) {
      if (edges.at(pos).fe.edge_idx(mc) == edge_idx) {
        return pos;
      }
    }
    return -1;
  }

  bool valid() const { return !edges.empty(); }
};
