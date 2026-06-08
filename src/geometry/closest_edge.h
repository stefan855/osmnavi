#pragma once

#include "absl/strings/str_format.h"
#include "algos/mm_router_defs.h"
#include "base/top_n.h"
#include "base/util.h"
#include "geometry/distance_from_segment.h"
#include "graph/mmgraph_def.h"

namespace {

struct ClosestEdge {
  DistanceToSegment dts;
  MMFullEdge fe;
  bool valid = false;

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
uint32_t DistanceToBoundingRect(int32_t lat, int32_t lon, MMBoundingRect br) {
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
#if 0
  LOG_S(INFO) << absl::StrFormat(
      "DistanceToBoundingRect (cm): %.f %.f %.f %.f min:%u", dist1, dist2, dist3,
      dist4, min_dist);
#endif
  return min_dist;
}

// At latitude 'lat', how many degrees (in 10^-7 degrees units) do we travel for
// 10 km?
int64_t Get10kmLongitudeAtLatitude(int32_t lat) {
  // Compute an estimate of how long one kilometer of longitude is (in 10^-7
  // degrees) at a specific latitude.
  // Compute length for one degree longitude at 'lat'.
  double length_one_deg_in_km =
      calculate_distance(lat, 0, lat, TEN_POW_7) / (100.0 * 1000.0);
  return 1'000'00000 / length_one_deg_in_km;
}

// Find clusters that are within a 10km range of the point (lat, lon).
// The returned list is sorted by increasing distance of the point to the border
// of the cluster and secondly by increasing distance to the center of the
// cluster. The distance of the point is 0 if it is inside the border of the
// cluster.
std::vector<ClusterInfo> FindGoodClusters(const MMGraph& mg, int32_t lat,
                                          int32_t lon) {
  std::vector<ClusterInfo> result;

  // Roughly ten kilometers in lat direction.
  constexpr int64_t lat_10km = 111'1111;
  // Roughly ten kilometers in lon direction.
  const int64_t lon_10km = Get10kmLongitudeAtLatitude(lat);

  for (const MMClusterBoundingRect& cl_br : mg.sorted_bounding_rects.span()) {
    const MMBoundingRect& br = cl_br.bounding_rect;
    if ((int64_t)lat < (int64_t)br.min.lat - lat_10km ||
        (int64_t)lat > (int64_t)br.max.lat + lat_10km) {
      continue;
    }
    if (std::abs(lat) < 85ll * TEN_POW_7 &&
        ((int64_t)lon < (int64_t)br.min.lon - lon_10km ||
         (int64_t)lon > (int64_t)br.max.lon + lon_10km)) {
      continue;
    }

    ClusterInfo ci = {.cluster_id = cl_br.cluster_id};
    ci.point_to_border_cm = DistanceToBoundingRect(lat, lon, br);
    ci.point_to_center_cm = calculate_distance(
        lat, lon, ((int64_t)br.min.lat + (int64_t)br.max.lat) / 2,
        ((int64_t)br.min.lon + (int64_t)br.max.lon) / 2);

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

inline GeoAnchor FindClosestEdges(const MMGraph& mg, int32_t lat, int32_t lon) {
  LOG_S(INFO) << absl::StrFormat("FindClosestEdges() search for (%u, %u)", lat,
                                 lon);
  const std::vector<ClusterInfo> good_clusters = FindGoodClusters(mg, lat, lon);

  TopN<ClosestEdge, 1, /*keep_greater=*/false> topn;
  topn.Add({.dts = {.distance_cm = MAXU31},
            .fe = {.from_node_idx = MAXU32},
            .valid = false});

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
        uint32_t n1_idx = mc.get_edge(e_idx).target_idx();
        const MMLatLon& n1_coord = mc.node_to_latlon(n1_idx);
        const DistanceToSegment d = FastPointToSegmentDistance(
            lat, lon, n0_coord.lat, n0_coord.lon, n1_coord.lat, n1_coord.lon);
        if (!topn.filled() || d.distance_cm < topn.bottom().dts.distance_cm) {
          topn.Add({.dts = d,
                    .fe = {.from_node_idx = n0_idx,
                           .cluster_id = mc.cluster_id,
                           .edge_offset = e_idx - mc.edge_start_idx(n0_idx)},
                    .valid = true});
        }
      }
    }
  }
  LOG_S(INFO) << absl::StrFormat("%u of %llu clusters scanned", count_scanned,
                                 good_clusters.size());

  GeoAnchor a({lat, lon});
  for (const ClosestEdge& ce : topn.span()) {
    if (ce.valid) {
      a.AddEdge({.distance_cm = static_cast<uint32_t>(ce.dts.distance_cm),
                 .to_fraction = static_cast<float>(ce.dts.fraction_closest),
                 .lat_at_fraction = ce.dts.lat_closest,
                 .lon_at_fraction = ce.dts.lon_closest,
                 .fe = ce.fe});
      // LOG_S(INFO) << absl::StrFormat("fraction_closest:%.3f",
      //                                ce.dts.fraction_closest);
    }
  }

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
