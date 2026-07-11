#pragma once

#include "absl/strings/str_format.h"
#include "algos/mm_router_defs.h"
#include "base/top_n.h"
#include "base/util.h"
#include "geometry/distance_from_segment.h"
#include "graph/mmgraph_def.h"

namespace {

struct ClosestEdge {
  MMFullEdge fe;
  DistanceToSegment dts;
#if 0
  // For edges that have shape coordinates, this is the position of the shape
  // segment in this list.
  //
  // -1:   segment between start node and the first coordinate pair in shape
  //       coords.
  // last: segment between the last coordinate pair and the target node..
  //
  // Note that in this case, dts relates to the shape segment, not to the full
  // edge 'fe'.
  bool has_shape_edge_pos;
  int shape_edge_pos;
#endif

  // "spaceship" operator, automatically defines ==, !=, <, <=, >, >=.
  // auto operator<=>(const ClosestEdge&) const = default;
  auto operator<=>(const ClosestEdge& other) const { return dts <=> other.dts; }
};

struct ClusterInfo {
  uint32_t cluster_id;
  uint32_t point_to_border_cm;
  uint32_t point_to_center_cm;
};

// Return the minimal distance for (lat, lon) to any of the four border lines of
// 'br'. Returns 0 if (lat, lon) is inside the bounding rect.
uint32_t DistanceToBoundingRect(DegE6 lat, DegE6 lon, MMBoundingRect br) {
  if (lat >= br.min.lat && lat <= br.max.lat && lon >= br.min.lon &&
      lon <= br.max.lon) {
    return 0;
  }
  const double dist1 =
      FastPointToSegmentDistance(lat, lon, br.min.lat, br.min.lon, br.min.lat,
                                 br.max.lon)
          .distance_cm;
  const double dist2 =
      FastPointToSegmentDistance(lat, lon, br.min.lat, br.max.lon, br.max.lat,
                                 br.max.lon)
          .distance_cm;
  const double dist3 =
      FastPointToSegmentDistance(lat, lon, br.max.lat, br.max.lon, br.max.lat,
                                 br.min.lon)
          .distance_cm;
  const double dist4 =
      FastPointToSegmentDistance(lat, lon, br.max.lat, br.min.lon, br.min.lat,
                                 br.min.lon)
          .distance_cm;
  uint32_t min_dist =
      static_cast<uint32_t>(std::min({dist1, dist2, dist3, dist4}));
  return min_dist;
}

// At latitude 'lat', how many longitude degrees do we travel for 10 km?
DegE6 Get10kmLongitudeAtLatitude(DegE6 lat) {
  // Compute an estimate of how long one kilometer of longitude is (in 10^-7
  // degrees) at a specific latitude.
  // Compute length for one degree longitude at 'lat'.
  const double distance_cm_for_one_degree =
      calculate_distance(lat, DegE6(0.0), lat, DegE6(1.0));
  return DegE6(1.0 / (distance_cm_for_one_degree / (100.0 * 1000.0 * 10.0)));
}

// Find clusters that are within a 10km range of the point (lat, lon).
// The returned list is sorted by increasing distance of the point to the border
// of the cluster and secondly by increasing distance to the center of the
// cluster. The distance of the point is 0 if it is inside the border of the
// cluster.
std::vector<ClusterInfo> FindGoodClusters(const MMGraph& mg, DegE6 lat,
                                          DegE6 lon) {
  std::vector<ClusterInfo> result;

  // Roughly ten kilometers in lat direction.
  // constexpr int64_t lat_10km = 1'111'111;
  constexpr DegE6 lat_10km(360.0 * kEarthRadiusCm / (100.0 * 1000.0 * 10.0));
  // Roughly ten kilometers in lon direction.
  const DegE6 lon_10km = Get10kmLongitudeAtLatitude(lat);

  for (const MMClusterBoundingRect& cl_br : mg.sorted_bounding_rects.span()) {
    const MMBoundingRect& br = cl_br.bounding_rect;
    if (lat.v64() < br.min.lat.v64() - lat_10km.v64() ||
        lat.v64() > br.max.lat.v64() + lat_10km.v64()) {
      continue;
    }
    if (std::abs(lat.AsDouble()) < 85.0 &&
        (lon.v64() < br.min.lon.v64() - lon_10km.v64() ||
         lon.v64() > br.max.lon.v64() + lon_10km.v64())) {
      continue;
    }

    ClusterInfo ci = {.cluster_id = cl_br.cluster_id};
    ci.point_to_border_cm = DistanceToBoundingRect(lat, lon, br);
    ci.point_to_center_cm = calculate_distance(
        lat, lon, DegE6((br.min.lat.AsDouble() + br.max.lat.AsDouble()) / 2.0),
        DegE6((br.min.lon.AsDouble() + br.max.lon.AsDouble()) / 2.0));

    if (ci.point_to_border_cm < 10 * 1000 * 100) {  // 10 km
      // LOG_S(INFO) << absl::StrFormat(
      //     "Accept cluster %u border dist:%u center dist:%u", ci.cluster_id,
      //     ci.point_to_border_cm, ci.point_to_center_cm);
      result.push_back(ci);
    } else {
      // LOG_S(INFO) << "Reject border distance " << ci.point_to_border_cm;
    }
  }
  // LOG_S(INFO) << "FindGoodClusters #clusters=" << result.size();

  std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) {
    if (a.point_to_border_cm != b.point_to_border_cm) {
      return a.point_to_border_cm < b.point_to_border_cm;
    }
    if (a.point_to_center_cm != b.point_to_center_cm) {
      return a.point_to_center_cm < b.point_to_center_cm;
    }
    return a.cluster_id < b.cluster_id;
  });

  return result;
}

}  // namespace

inline GeoAnchor FindClosestEdges(const MMGraph& mg, DegE6 lat, DegE6 lon) {
  LOG_S(INFO) << absl::StrFormat("FindClosestEdges() search for (%.6f, %.6f)",
                                 lat.AsDouble(), lon.AsDouble());
  const std::vector<ClusterInfo> good_clusters = FindGoodClusters(mg, lat, lon);

  TopN<ClosestEdge, 1, /*keep_greater=*/false> topn;
  topn.Add({.fe = {.from_node_idx = INFU32}, .dts = {.distance_cm = INFU31}});

  uint32_t count_scanned = 0;
  for (const ClusterInfo& ci : good_clusters) {
    if (topn.top().dts.distance_cm < ci.point_to_border_cm) {
      /*
      LOG_S(INFO) << absl::StrFormat(
          "Ignore cluster %u bc border distance %u > found distance %.f",
          ci.cluster_id, ci.point_to_border_cm, topn.top().dts.distance_cm);
          */
      continue;
    }
    count_scanned++;
    const MMCluster& mc = mg.clusters.at(ci.cluster_id);
    for (uint32_t n0_idx = 0; n0_idx < mc.nodes.size(); ++n0_idx) {
      const MMLatLon& n0_coord = mc.node_to_latlon(n0_idx);
      for (uint32_t e_idx : mc.edge_indices(n0_idx)) {
        // TODO: Getting consecutive shape coords for all edges can be
        // made much faster by caching the last read pos in the blob.
        if (mc.edge_shape_coords.has_coords(e_idx)) {
          uint32_t n1_idx = mc.get_edge(e_idx).target_idx();
          // LOG_S(INFO) << absl::StrFormat("Check closest edge %ld %ld %.2fm",
          //                                mc.get_node_id(n0_idx),
          //                                mc.get_node_id(n1_idx), 0.0);
          std::vector<MMLatLon> coords = mc.get_shape_coords(n0_idx, e_idx);
          CHECK_S(!coords.empty());
          for (int pos = -1; pos < static_cast<int64_t>(coords.size()); ++pos) {
            MMLatLon c0 = pos == -1 ? n0_coord : coords.at(pos);
            MMLatLon c1 = pos + 1 == static_cast<int64_t>(coords.size())
                              ? mc.node_to_latlon(n1_idx)
                              : coords.at(pos + 1);
            const DistanceToSegment d = FastPointToSegmentDistance(
                lat, lon, c0.lat, c0.lon, c1.lat, c1.lon);
            // LOG_S(INFO) << absl::StrFormat("  distance %.2fm",
            //                                d.distance_cm / 100.0);
            if (!topn.filled() ||
                d.distance_cm < topn.bottom().dts.distance_cm) {
              // LOG_S(INFO) << absl::StrFormat(
              //     "New closest shape edge %ld %ld %.2fm",
              //     mc.get_node_id(n0_idx), mc.get_node_id(n1_idx),
              //     d.distance_cm / 100.0);

              topn.Add(
                  {.fe = {.from_node_idx = n0_idx,
                          .cluster_id = mc.cluster_id,
                          .edge_offset = e_idx - mc.edge_start_idx(n0_idx)},
                   .dts = d});
            }
          }
        } else {
          uint32_t n1_idx = mc.get_edge(e_idx).target_idx();
          const MMLatLon& n1_coord = mc.node_to_latlon(n1_idx);
          const DistanceToSegment d = FastPointToSegmentDistance(
              lat, lon, n0_coord.lat, n0_coord.lon, n1_coord.lat, n1_coord.lon);
          if (!topn.filled() || d.distance_cm < topn.bottom().dts.distance_cm) {
            // LOG_S(INFO) << absl::StrFormat(
            //     "New closest non-shape edge %ld %ld %.2fm",
            //     mc.get_node_id(n0_idx), mc.get_node_id(n1_idx),
            //     d.distance_cm / 100.0);
            topn.Add({.fe = {.from_node_idx = n0_idx,
                             .cluster_id = mc.cluster_id,
                             .edge_offset = e_idx - mc.edge_start_idx(n0_idx)},
                      .dts = d});
          }
        }
      }
    }
  }
  LOG_S(INFO) << absl::StrFormat("%u of %llu clusters scanned", count_scanned,
                                 good_clusters.size());

  GeoAnchor a({lat, lon});
  for (const ClosestEdge& ce : topn.span()) {
    if (ce.fe.from_node_idx != INFU32) {  // Valid entry?
      a.AddEdge({.distance_cm = static_cast<uint32_t>(ce.dts.distance_cm),
                 .to_fraction = static_cast<float>(ce.dts.fraction_closest),
                 .lat_at_fraction = ce.dts.lat_closest,
                 .lon_at_fraction = ce.dts.lon_closest,
                 .fe = ce.fe});
      // LOG_S(INFO) << absl::StrFormat("fraction_closest:%.3f",
      //                                ce.dts.fraction_closest);
    }
  }

  CHECK_LE_S(a.edge_points().size(), 1);  // We collect at most one edge.
  if (a.edge_points().size() == 1) {
    // Find backward edge
    const EdgePoint& ep = a.edge_points().front();
    const MMCluster& mc = ep.fe.mc(mg);
    uint32_t backward_idx = mc.find_edge_idx(
        ep.fe.target_idx(mc), ep.fe.from_node_idx, ep.fe.way_idx(mc));
    if (backward_idx != INFU32) {
      a.AddEdge({.distance_cm = ep.distance_cm,
                 .to_fraction = 1.0f - ep.to_fraction,
                 .lat_at_fraction = ep.lat_at_fraction,
                 .lon_at_fraction = ep.lon_at_fraction,
                 .fe = MMFullEdge::CreateWithEdgeIdx(mc, ep.fe.target_idx(mc),
                                                     backward_idx)});
    }
  }

  // This returns an entry with valid=false in case there are no results.
  return a;
}
