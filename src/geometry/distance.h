#pragma once

#include <cmath>

#include "base/constants.h"
#include "base/deg_coord.h"

// Compute distance between two points on the surface of the earth, using the
// haversine formula.
// Returned distance is in centimeters.
inline int64_t calculate_distance(DegE6 lat1, DegE6 lon1, DegE6 lat2,
                                  DegE6 lon2) {
  double lat1_rad = lat1.AsDouble() * (std::numbers::pi / 180.0);
  double lon1_rad = lon1.AsDouble() * (std::numbers::pi / 180.0);
  double lat2_rad = lat2.AsDouble() * (std::numbers::pi / 180.0);
  double lon2_rad = lon2.AsDouble() * (std::numbers::pi / 180.0);
  double f_dlat = std::sin((lat2_rad - lat1_rad) / 2.0);
  double f_dlon = std::sin((lon2_rad - lon1_rad) / 2.0);
  double a = f_dlat * f_dlat +
             std::cos(lat1_rad) * std::cos(lat2_rad) * f_dlon * f_dlon;
  return std::llround(2.0 * kEarthRadiusCm *
                      std::atan2(std::sqrt(a), std::sqrt(1.0 - a)));
}

inline int64_t calculate_distance(MMLatLon ll1, MMLatLon ll2) {
  return calculate_distance(ll1.lat, ll1.lon, ll2.lat, ll2.lon);
}

namespace {
// Length of a segment on a longitude circle in cm, given its size in degrees.
// This assumes that the earth is a perfect sphere (although it isn't).
inline int64_t length_lat_segment_cm(const DegE6 lat_diff) {
  constexpr double kEarthCircumferenceThroughPoles =
      kEarthRadiusCm * std::numbers::pi * 2;

  return std::llround(kEarthCircumferenceThroughPoles / 360.0 *
                      lat_diff.AsFloat());
}
}  // namespace

// Return the counterclockwise angle α ([0..359] deg) between an edge and the
// latitude circle at the starting point of the edge.
//
//               +
//      edge   * |
//           *   |
//         *     |
//       *       | height
//     *         |
//   *  α        |
// + ------------------- latitude circle
//
// The edge length is a parameter. The height is easy to compute from the
// latitude difference of start and endpoint of the edge (see
// length_lat_segment_cm()). Angle α is computed from the formula
//   sin(α) = height / edge_length.
//
inline int32_t angle_to_east_degrees(DegE6 lat1, DegE6 lon1, DegE6 lat2,
                                     DegE6 lon2, uint32_t edge_length_cm) {
  if (edge_length_cm == 0) {
    return 0;
  }

  const DegE6 lat_diff(std::abs(lat2.v64() - lat1.v64()));
  const double height_cm = length_lat_segment_cm(lat_diff);

  int64_t angle = 90;
  // asin is only defined for [-1..1]. Here all numbers are positive, so
  // make sure h/l < 1 and assume 90deg for h/l >= 1, which might occur due to
  // rounding errors.
  if (height_cm < edge_length_cm) {
    angle = std::llround(
        180.0 * (std::asin(height_cm / edge_length_cm) / std::numbers::pi));
  }

  // length and height are positive, so angle should be positive too.
  CHECK_GE_S(angle, 0);
  angle = angle % 360;

  // Compute result, depending on the quadrant the target of the edge is
  // relative to the origin of the edge.
  if (lat2 >= lat1) {
    if (lon2 >= lon1) {  // 1. Quadrant
      return angle;
    } else {  // 2. Quadrant
      return 180 - angle;
    }
  } else {
    if (lon2 >= lon1) {  // 4. Quadrant
      return (360 - angle) % 360;
    } else {  // 3. Quadrant
      return 180 + angle;
    }
  }
}

// Compute the angle change between two consecutive edges. Input for each edge
// is the angle computed by angle_to_east_degrees().
//
// The angle returned is in the range [-179..180]. A value of 0 indicates no
// change in direction, and angle of 180 degrees denotes a full u-turn. Negative
// values happen when the second edge goes to the right side, positive values
// when it goes to the left side of the first edge.
inline int32_t angle_between_edges(int32_t edge_angle_1, int32_t edge_angle_2) {
  int32_t a = edge_angle_2 - edge_angle_1;
  if (a > 180) {
    a = a - 360;
  } else if (a <= -180) {
    a = a + 360;
  }
  return a;
}
