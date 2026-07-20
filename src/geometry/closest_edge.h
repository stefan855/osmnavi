#pragma once

#include "absl/strings/str_format.h"
#include "algos/mm_router_defs.h"
#include "base/top_n.h"
#include "base/util.h"
#include "geometry/distance_to_segment.h"
#include "graph/mmgraph_def.h"

namespace {

struct ClosestEdge {
  MMFullEdge fe;
  // Distance information for the shape segment, or full edge if no shape coords
  // are available.
  DistanceToSegment shape_dts;

  // For edges that have shape coordinates, this is the position of the shape
  // segment in the extended shape coordinate list.
  //
  // -1:   No shape coordinates exist for this edge.
  // 0..:  segment starts at this position in the *extended* shape coordinate
  //       list.
  int shape_coords_pos;

  // "spaceship" operator, automatically defines ==, !=, <, <=, >, >=.
  // auto operator<=>(const ClosestEdge&) const = default;
  auto operator<=>(const ClosestEdge& other) const {
    return shape_dts <=> other.shape_dts;
  }
};

struct ClusterInfo {
  uint32_t cluster_id;
  uint32_t point_to_border_cm;
  uint32_t point_to_center_cm;
};

// Return the minimal distance for (lat, lon) to any of the four border lines of
// 'br'. Returns 0 if (lat, lon) is inside the bounding rect.
uint32_t DistanceToBoundingRect(LatLon pt, MMBoundingRect br) {
  if (pt.lat >= br.min.lat && pt.lat <= br.max.lat && pt.lon >= br.min.lon &&
      pt.lon <= br.max.lon) {
    return 0;
  }
  const double dist1 = FastPointToSegmentDistance(pt, {br.min.lat, br.min.lon},
                                                  {br.min.lat, br.max.lon})
                           .distance_to_seg_cm;
  const double dist2 = FastPointToSegmentDistance(pt, {br.min.lat, br.max.lon},
                                                  {br.max.lat, br.max.lon})
                           .distance_to_seg_cm;
  const double dist3 = FastPointToSegmentDistance(pt, {br.max.lat, br.max.lon},
                                                  {br.max.lat, br.min.lon})
                           .distance_to_seg_cm;
  const double dist4 = FastPointToSegmentDistance(pt, {br.max.lat, br.min.lon},
                                                  {br.min.lat, br.min.lon})
                           .distance_to_seg_cm;
  uint32_t min_dist =
      static_cast<uint32_t>(std::min({dist1, dist2, dist3, dist4}));
  return min_dist;
}

// At latitude 'lat', how many longitude degrees do we travel for 10 km?
LonE6 Get10kmLongitudeAtLatitude(LatE6 lat) {
  // Compute an estimate of how long one kilometer of longitude is (in 10^-7
  // degrees) at a specific latitude.
  // Compute length for one degree longitude at 'lat'.
  const double distance_cm_for_one_degree =
      calculate_distance({lat, LonE6(0.0)}, {lat, LonE6(1.0)});
  return LonE6(1.0 / (distance_cm_for_one_degree / (100.0 * 1000.0 * 10.0)));
}

// Find clusters that are within a 10km range of the point (lat, lon).
// The returned list is sorted by increasing distance of the point to the border
// of the cluster and secondly by increasing distance to the center of the
// cluster. The distance of the point is 0 if it is inside the border of the
// cluster.
std::vector<ClusterInfo> FindGoodClusters(const MMGraph& mg, LatLon pt) {
  std::vector<ClusterInfo> result;

  // Roughly ten kilometers in lat direction.
  // constexpr int64_t lat_10km = 1'111'111;
  constexpr LatE6 lat_10km(360.0 * kEarthRadiusCm / (100.0 * 1000.0 * 10.0));
  // Roughly ten kilometers in lon direction.
  const LonE6 lon_10km = Get10kmLongitudeAtLatitude(pt.lat);

  for (const MMClusterBoundingRect& cl_br : mg.sorted_bounding_rects.span()) {
    const MMBoundingRect& br = cl_br.bounding_rect;
    if (pt.lat.v64() < br.min.lat.v64() - lat_10km.v64() ||
        pt.lat.v64() > br.max.lat.v64() + lat_10km.v64()) {
      continue;
    }
    if (std::abs(pt.lat.AsDouble()) < 85.0 &&
        (pt.lon.v64() < br.min.lon.v64() - lon_10km.v64() ||
         pt.lon.v64() > br.max.lon.v64() + lon_10km.v64())) {
      continue;
    }

    ClusterInfo ci = {.cluster_id = cl_br.cluster_id};
    ci.point_to_border_cm = DistanceToBoundingRect(pt, br);
    ci.point_to_center_cm = calculate_distance(
        pt.lat, pt.lon, LatE6((br.min.lat.AsDouble() + br.max.lat.AsDouble()) / 2.0),
        LonE6((br.min.lon.AsDouble() + br.max.lon.AsDouble()) / 2.0));

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

// If an edge has shape coordinates, then the ce.shape_dts.to_fraction relates
// only to the shape segment that was closest.
//
// Returns the to_fraction that corresponds to the full length of the edge.
inline double ComputeGlobalEdgeFraction(const MMGraph& mg,
                                        const ClosestEdge& ce) {
  const MMCluster& mc = ce.fe.mc(mg);
  if (ce.shape_coords_pos < 0) {
    // LOG_S(INFO) << "AA0 no shape coords";
    // No shape coordinates, we're already done.
    return ce.shape_dts.fraction_closest;
  }
  CHECK_GE_S(ce.shape_coords_pos, 0);
  std::vector<LatLon> coords = mc.get_shape_coords(
      ce.fe.from_node_idx, ce.fe.edge_idx(mc), /*extend=*/true);
  // Last segment must exist.
  CHECK_LT_S(ce.shape_coords_pos + 1, coords.size());

  const uint32_t total_dist = mc.edge_to_distance.at(ce.fe.edge_idx(mc));
  uint64_t sum_dist = 0;
  // LOG_S(INFO) << absl::StrFormat(
  //     "AA1 shape coords #seg:%lu scpos:%d scfrac:%.2f len:%u",
  //     coords.size() - 1, ce.shape_coords_pos, ce.shape_dts.fraction_closest,
  //     total_dist);
  for (int pos = 0; pos <= ce.shape_coords_pos; ++pos) {
    uint32_t dist = calculate_distance(coords.at(pos), coords.at(pos + 1));
    sum_dist += (pos != ce.shape_coords_pos)
                    ? dist
                    : std::lround(dist * ce.shape_dts.fraction_closest);
    // LOG_S(INFO) << absl::StrFormat("AA2 segment %d dist:%u sum%lu", pos,
    // dist, sum_dist);
  }
  // Don't accept shape coordinates with a total distance of 0.
  CHECK_GT_S(total_dist, 0);
  // LOG_S(INFO) << absl::StrFormat(
  //     "AA3 global fraction:%.2f",
  //     static_cast<double>(sum_dist) / static_cast<double>(total_dist));
  return static_cast<double>(sum_dist) / static_cast<double>(total_dist);
}

GeoAnchor ConvertClosestEdgesToAnchor(
    const MMGraph& mg, LatLon pt,
    const TopN<ClosestEdge, 1, /*keep_greater=*/false>& topn) {
  GeoAnchor a(pt);
  for (const ClosestEdge& ce : topn.span()) {
    if (ce.fe.from_node_idx != INFU32) {  // Valid entry?
      a.AddEdge(
          {.distance_to_seg_cm =
               static_cast<uint32_t>(ce.shape_dts.distance_to_seg_cm),
           // .to_fraction =
           // static_cast<float>(ce.shape_dts.fraction_closest),
           .to_fraction = static_cast<float>(ComputeGlobalEdgeFraction(mg, ce)),
           .coord_at_fraction = ce.shape_dts.coord_closest,
           .fe = ce.fe});
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
      a.AddEdge({.distance_to_seg_cm = ep.distance_to_seg_cm,
                 .to_fraction = 1.0f - ep.to_fraction,
                 .coord_at_fraction = ep.coord_at_fraction,
                 .fe = MMFullEdge::CreateWithEdgeIdx(mc, ep.fe.target_idx(mc),
                                                     backward_idx)});
    }
  }

  // This returns an entry with valid=false in case there are no results.
  return a;
}

}  // namespace

inline GeoAnchor FindClosestEdges(const MMGraph& mg, LatLon pt) {
  LOG_S(INFO) << absl::StrFormat("FindClosestEdges() search for (%.6f, %.6f)",
                                 pt.lat.AsDouble(), pt.lon.AsDouble());
  const std::vector<ClusterInfo> good_clusters = FindGoodClusters(mg, pt);

  TopN<ClosestEdge, 1, /*keep_greater=*/false> topn;
  topn.Add({.fe = {.from_node_idx = INFU32},
            .shape_dts = {.distance_to_seg_cm = INFU31},
            .shape_coords_pos = -1});

  uint32_t count_scanned = 0;
  for (const ClusterInfo& ci : good_clusters) {
    if (topn.top().shape_dts.distance_to_seg_cm < ci.point_to_border_cm) {
      /*
      LOG_S(INFO) << absl::StrFormat(
          "Ignore cluster %u bc border distance %u > found distance %.f",
          ci.cluster_id, ci.point_to_border_cm,
      topn.top().shape_dts.distance_to_seg_cm);
          */
      continue;
    }
    count_scanned++;
    const MMCluster& mc = mg.clusters.at(ci.cluster_id);
    for (uint32_t n0_idx = 0; n0_idx < mc.nodes.size(); ++n0_idx) {
      const LatLon& n0_coord = mc.node_to_latlon(n0_idx);
      for (uint32_t e_idx : mc.edge_indices(n0_idx)) {
        if (mc.edge_shape_coords.has_coords(e_idx)) {
          // ======== Shape Coords.
          //
          uint32_t n1_idx = mc.get_edge(e_idx).target_idx();
          // LOG_S(INFO) << absl::StrFormat("Check closest edge %ld %ld %.2fm",
          //                                mc.get_node_id(n0_idx),
          //                                mc.get_node_id(n1_idx), 0.0);
          // TODO: Getting consecutive shape coords for all edges can be
          // made much faster by caching the last read pos in the blob.
          std::vector<LatLon> coords = mc.get_shape_coords(n0_idx, e_idx);
          CHECK_S(!coords.empty());
          for (int pos = -1; pos < static_cast<int64_t>(coords.size()); ++pos) {
            LatLon c0 = pos == -1 ? n0_coord : coords.at(pos);
            LatLon c1 = pos + 1 == static_cast<int64_t>(coords.size())
                            ? mc.node_to_latlon(n1_idx)
                            : coords.at(pos + 1);
            const DistanceToSegment d =
                FastPointToSegmentDistance(pt, c0, c1);
            // LOG_S(INFO) << absl::StrFormat("  distance %.2fm",
            //                                d.distance_to_seg_cm / 100.0);
            if (!topn.filled() ||
                d.distance_to_seg_cm <
                    topn.bottom().shape_dts.distance_to_seg_cm) {
              // LOG_S(INFO) << absl::StrFormat(
              //     "New closest shape edge %ld %ld %.2fm",
              //     mc.get_node_id(n0_idx), mc.get_node_id(n1_idx),
              //     d.distance_to_seg_cm / 100.0);

              topn.Add(
                  {.fe = {.from_node_idx = n0_idx,
                          .cluster_id = mc.cluster_id,
                          .edge_offset = e_idx - mc.edge_start_idx(n0_idx)},
                   .shape_dts = d,
                   // We start iterating at -1, but extended shape coords start
                   // at 0, so store pos + 1.
                   .shape_coords_pos = pos + 1});
            }
          }
        } else {
          // ======== Straight line (no shape coords).
          uint32_t n1_idx = mc.get_edge(e_idx).target_idx();
          const LatLon& n1_coord = mc.node_to_latlon(n1_idx);
          const DistanceToSegment d =
              FastPointToSegmentDistance(pt, n0_coord, n1_coord);
          if (!topn.filled() ||
              d.distance_to_seg_cm <
                  topn.bottom().shape_dts.distance_to_seg_cm) {
            // LOG_S(INFO) << absl::StrFormat(
            //     "New closest non-shape edge %ld %ld %.2fm",
            //     mc.get_node_id(n0_idx), mc.get_node_id(n1_idx),
            //     d.distance_to_seg_cm / 100.0);
            topn.Add({.fe = {.from_node_idx = n0_idx,
                             .cluster_id = mc.cluster_id,
                             .edge_offset = e_idx - mc.edge_start_idx(n0_idx)},
                      .shape_dts = d,
                      .shape_coords_pos = -1});
          }
        }
      }
    }
  }
  LOG_S(INFO) << absl::StrFormat("%u of %llu clusters scanned", count_scanned,
                                 good_clusters.size());

  return ConvertClosestEdgesToAnchor(mg, pt, topn);

#if 0
  GeoAnchor a({lat, lon});
  for (const ClosestEdge& ce : topn.span()) {
    if (ce.fe.from_node_idx != INFU32) {  // Valid entry?
      a.AddEdge(
          {.distance_to_seg_cm =
               static_cast<uint32_t>(ce.shape_dts.distance_to_seg_cm),
           .to_fraction = static_cast<float>(ce.shape_dts.fraction_closest),
           .lat_at_fraction = ce.shape_dts.lat_closest,
           .lon_at_fraction = ce.shape_dts.lon_closest,
           .fe = ce.fe});
      // LOG_S(INFO) << absl::StrFormat("fraction_closest:%.3f",
      //                                ce.shape_dts.fraction_closest);
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
      a.AddEdge({.distance_to_seg_cm = ep.distance_to_seg_cm,
                 .to_fraction = 1.0f - ep.to_fraction,
                 .lat_at_fraction = ep.lat_at_fraction,
                 .lon_at_fraction = ep.lon_at_fraction,
                 .fe = MMFullEdge::CreateWithEdgeIdx(mc, ep.fe.target_idx(mc),
                                                     backward_idx)});
    }
  }

  // This returns an entry with valid=false in case there are no results.
  return a;
#endif
}
