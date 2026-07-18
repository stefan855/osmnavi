#pragma once

// TODO: This code is badly written, and not needed anymore for the new router
// on memory mapped data. Remove it as soon as possible.

#include <numeric>
#include <vector>

#include "base/deg_coord.h"
#include "base/util.h"
#include "geometry/distance.h"
#include "graph/graph_def.h"

struct ClosestNodeResult {
  uint32_t node_pos;
  int64_t dist;
};

// Computes distance to each node in the graph and chooses the node with the
// shortest distance.
inline ClosestNodeResult FindClosestNodeSlow(const Graph& g, LatE6 lat,
                                             LonE6 lon) {
  uint32_t found_pos = INFU32;
  int64_t min_dist = INF64;
  for (uint32_t i = 0; i < g.nodes.size(); ++i) {
    const GNode& n = g.nodes.at(i);
    int64_t dist = calculate_distance({lat, lon}, n.ll);
    if (dist < min_dist) {
      min_dist = dist;
      found_pos = i;
    }
  }
  return {.node_pos = found_pos, .dist = min_dist};
}

namespace {
// Find the first element that is >= 'lon'
//
// Note: std::lower_bound() doesn't work with a vector of indexes to something
// else, so I have to implement my own binary search...
inline int64_t LowerBoundBinSearch(const Graph& g,
                                   const std::vector<uint32_t>& idx,
                                   LonE6 lon) {
  uint64_t L = 0;
  uint64_t R = idx.size();
  while (L < R) {
    uint64_t M = (L + R) / 2;
    // Does element at M satisfies condition?
    if (lon <= g.nodes.at(idx.at(M)).ll.lon) {
      // Yes, so it must be an M or before.
      R = M;
    } else {
      // No, so it must be at pos > M.
      L = M + 1;
    }
  }
  return L;
}

struct FastSearchData {
  const LatLon ll;
  // Distance of one degree in lon direction at 'lat'.
  const double dist_one_deg_lon;
  int64_t min_dist;
  // Given min_dist above, estimated maximal search range in lon direction.
  int64_t max_dlon;
  uint32_t found_pos;
};

// Checks if the current point 'pos' is closer than 'min_dist' and updates
// min_dist and found_pos accordingly.
//
// Return false if the current point is outside the possible range for lon, true
// if inside.
inline bool UpdateMin(const Graph& g, int64_t pos, FastSearchData* fsdata) {
  const GNode& n = g.nodes.at(pos);
  const int64_t dlon = std::abs(fsdata->ll.lon.v64() - n.ll.lon.v64());
  if (dlon > fsdata->max_dlon) {
    return false;
  }
#if 0
  const int64_t dlat = std::abs(fsdata->lat - n.lat);
  if (dlat > fsdata->max_dlon) {
    return true;
  }
#endif
  int64_t dist = calculate_distance(fsdata->ll, n.ll);
  if (dist < fsdata->min_dist) {
    fsdata->min_dist = dist;
    fsdata->found_pos = pos;
    fsdata->max_dlon =
        // Add 1% to max_dlon to correct for boundary errors.
        // Divide by 2 because the point can be at the pole.
        std::llround(1.01 * TEN_POW_7_DBL * (dist / fsdata->dist_one_deg_lon));
  }
  return true;
}

// Compute how much distance it is to go one longitudinal degree on latitude
// 'lat'.
//
// This is used to estimate how far from point (lat,0) we have go in +/-lon
// direction before we can be sure that the distance will be larger than some
// given dist. With this, the search of nodes can be reduced dramatically when
// finding close nodes (because the distance in +/-lon direction is reduced).
// We compute this value for 180 degrees and divide it by 180 to get the value
// for one degree. This is better because there might be a shorter route through
// the north/south pole than when staying on a fixed latitude, which makes the
// search range larger. Additionally we divide by 2.0, because the point at +180
// longitude could be very close to the pole.
double ComputeOneDegreeHeuristic(LatE6 lat) {
  return static_cast<double>(
             calculate_distance({lat, LonE6(0.0)}, {lat, LonE6(180.0)})) /
         (180.0 * 2.0);
}

}  // namespace

inline std::vector<uint32_t> SortNodeIndexesByLon(const Graph& g) {
  std::vector<uint32_t> idx(g.nodes.size());
  std::iota(idx.begin(), idx.end(), 0);  // Fills it with 0..N-1
  std::sort(idx.begin(), idx.end(), [&g](uint32_t a, uint32_t b) {
    return g.nodes.at(a).ll.lon < g.nodes.at(b).ll.lon;
  });
  return idx;
}

// Finds the node with the shortest distance in g. idx must contain the
// lon-sorted indexes created by SortNodeIndexesByLon(g);
//
// Note: This uses a speed-up heuristic that is not guaranteed to return she
// node with the shortest distance. Especially as (lat,lon) get closer to the
// poles of the earth, the computation might be wrong and return a non-optimal
// node. Still, the heuristic seems to work very well for Switzerland, i.e.
// there were no errors when comparing 10k computations.
inline ClosestNodeResult FindClosestNodeFast(const Graph& g,
                                             const std::vector<uint32_t>& idx,
                                             LatE6 lat, LonE6 lon) {
  // Find first element >= the given lon.
  int64_t posf = LowerBoundBinSearch(g, idx, lon);  // first element >= 'lon'.
  // Search both forward (posf)and backward (posb) from the found position.
  int64_t posb = posf - 1;
  FastSearchData fsdata = {.ll = {lat, lon},
                           .dist_one_deg_lon = ComputeOneDegreeHeuristic(lat),
                           .min_dist = INF64,
                           .max_dlon = INF64,
                           .found_pos = INFU32};
  int count = 0;
  while (posf < (int64_t)idx.size() || posb >= 0) {
    count++;
    // LOG_S(INFO) << absl::StrFormat("#els:%u posf:%d posb:%d", idx.size(),
    // posf, posb);
    if (posf < (int64_t)idx.size()) {
      if (UpdateMin(g, idx.at(posf), &fsdata)) {
        posf++;
      } else {
        // Outside if possible range, disable searching in this direction.
        posf = idx.size();
      }
    }
    if (posb >= 0) {
      if (UpdateMin(g, idx.at(posb), &fsdata)) {
        posb--;
      } else {
        // Outside if possible range, disable searching in this direction.
        posb = -1;
      }
    }
  }
  return {.node_pos = fsdata.found_pos, .dist = fsdata.min_dist};
}

#if 0
// TODO: Verify that the heuristic used in FindClosestNodeFast() is 100%
// accurate.
void CheckHeuristic() {
  int64_t deg1 = TEN_POW_7;
  for (int64_t lat = -89; lat < 90; lat++) {
    int64_t d1 = 180 * calculate_distance(lat * deg1, 0, lat * deg1, deg1);
    int64_t d90 = 2 * calculate_distance(lat * deg1, 0, lat * deg1, 90 * deg1);
    int64_t d180 = calculate_distance(lat * deg1, 0, lat * deg1, 180 * deg1);
    int64_t d270 =
        2 * calculate_distance(lat * deg1, 0, lat * deg1, 270 * deg1);
    LOG_S(INFO) << absl::StrFormat("lat:%d d1:%d d90:%d d180:%d d270:%d", lat,
                                   d1, d90, d180, d270);
    LOG_S(INFO) << absl::StrFormat("d1/d90:%.3f d1/180:%.3f d1/270:%.3f",
                                   ((double)d1) / d90, ((double)d1) / d180,
                                   ((double)d1) / d270);
    /*
    int64_t d179 = calculate_distance(lat * deg1, 0, lat * deg1, 179 * deg1);
    int64_t d181 = calculate_distance(lat * deg1, 0, lat * deg1, 181 * deg1);
    LOG_S(INFO) << absl::StrFormat(
        "d179/d180:%.8f 180/180:%.8f d181/d180:%.8f",
        ((double)d179 * 180.0 / 179.0) / d180, ((double)d180) / d180,
        ((double)d181 * 180.0 / 179.0) / d180);
        */
  }
}
#endif
