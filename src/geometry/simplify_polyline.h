#pragma once

#include <cmath>

#include "base/constants.h"
#include "base/deg_coord.h"
#include "base/distance_type.h"
#include "geometry/closest_edge.h"
#include "geometry/distance.h"
#include "graph/data_block.h"

// Simplify a polyline by removing points that only deviate marginally from a
// straight line (or are extremely close to the previous node).
//
// To do this, for each sequence of three points, the perpendicular distance of
// the middle point is computed, and also the angle.
//
// This is used to remove nodes from a shape line before storing or using the
// shapeline.
inline void SimplifyPolyline(std::vector<NodeBuilder::VNode>* coords,
                             bool debug = false) {
  for (uint32_t pos = 0; pos < coords->size() - 2;) {
    NodeBuilder::VNode A = coords->at(pos);
    NodeBuilder::VNode M = coords->at(pos + 1);
    NodeBuilder::VNode B = coords->at(pos + 2);

    DistanceToSegment dts = FastPointToSegmentDistance(M.ll, A.ll, B.ll);

    int32_t bearing1 = true_north_bearing(A.ll, M.ll);
    int32_t bearing2 = true_north_bearing(M.ll, B.ll);
    int32_t angle_at_m = std::abs(angle_between_edges(bearing1, bearing2));

    if (debug) {
      LOG_S(INFO) << absl::StrFormat(
          "Distance of middle point d1:(%7d,%7d) d2:(%7d,%7d): %5.2fm "
          "len1:%5.2fm len2:%5.2f angle:%d",
          M.ll.lat.v() - A.ll.lat.v(), M.ll.lon.v() - A.ll.lon.v(),
          B.ll.lat.v() - M.ll.lat.v(), B.ll.lon.v() - M.ll.lon.v(),
          dts.distance_to_seg.meters(), calculate_distance(A.ll, M.ll).meters(),
          calculate_distance(M.ll, B.ll).meters(), angle_at_m);
      LOG_S(INFO) << absl::StrFormat("  Ids %ld -> %ld -> %ld", A.id, M.id,
                                     B.id);
    }

    if ((dts.distance_to_seg.cm() <= 15 && angle_at_m <= 3) ||
        (dts.distance_to_seg.cm() <= 19 && angle_at_m <= 1) ||
        (dts.distance_to_seg.cm() <= 5 && angle_at_m <= 5)) {
      if (debug) {
        LOG_S(INFO) << "  Remove shape coord " << coords->size() << " -> "
                    << coords->size() - 1;
      }
      coords->erase(coords->begin() + pos + 1);
      // Stay at 'pos'
    } else if (calculate_distance(M.ll, A.ll).cm() < 20) {
      // Distance of A->M is below 20cm. this occurs sometimes, often it might
      // be a mistake, so ignore M.
      if (debug) {
        LOG_S(INFO) << "  Ignore shape coord " << coords->size() << " -> "
                    << coords->size() - 1 << " id=" << A.id
                    << " id removed=" << M.id;
      }
      coords->erase(coords->begin() + pos + 1);
      // Stay at 'pos'
    } else {
      if (debug) {
        LOG_S(INFO) << "  Keep shape coord " << coords->size();
      }
      ++pos;
    }
  }
}
