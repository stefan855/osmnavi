#pragma once

#include <cmath>

namespace {
constexpr int64_t kEarthRadiusCm = 637100000;

// Length of a segment on a longitude circle in cm, given its size in degrees.
// This assumes that the earth is a perfect sphere (although it isn't). This is
// necessary because the calculate_distance() does the same.
inline int64_t length_lat_segment_cm(int64_t lat_diff_100nano) {
  constexpr double kEarthCircumferenceThroughPoles =
      kEarthRadiusCm * std::numbers::pi * 2;

  return std::llround((kEarthCircumferenceThroughPoles * lat_diff_100nano) /
                      (360ll * 10000000ll));
}

}  // namespace

// Compute distance between two points on the surface of the earth, using the
// haversine formula. Input coordinates are in units of 10^-7 degrees, i.e. they
// are multiplied by 10^7. This format is used in openstreetmap. Returned
// distance is in centimeters.
inline int64_t calculate_distance(int32_t lat1_100nano, int32_t lon1_100nano,
                                  int32_t lat2_100nano, int32_t lon2_100nano) {
  double lat1 = lat1_100nano * (std::numbers::pi / 180.0 / 10000000.0);
  double lon1 = lon1_100nano * (std::numbers::pi / 180.0 / 10000000.0);
  double lat2 = lat2_100nano * (std::numbers::pi / 180.0 / 10000000.0);
  double lon2 = lon2_100nano * (std::numbers::pi / 180.0 / 10000000.0);
  double f_dlat = std::sin((lat2 - lat1) / 2.0);
  double f_dlon = std::sin((lon2 - lon1) / 2.0);
  double a =
      f_dlat * f_dlat + std::cos(lat1) * std::cos(lat2) * f_dlon * f_dlon;
  return std::llround(2.0 * kEarthRadiusCm *
                      std::atan2(std::sqrt(a), std::sqrt(1.0 - a)));
}

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
inline int32_t angle_to_east_degrees(int32_t lat1_100nano, int32_t lon1_100nano,
                                     int32_t lat2_100nano, int32_t lon2_100nano,
                                     uint32_t edge_length_cm) {
  if (edge_length_cm == 0) {
    return 0;
  }

  const int64_t lat_diff_100nano = std::abs(static_cast<int64_t>(lat2_100nano) -
                                            static_cast<int64_t>(lat1_100nano));
  const double height_cm = length_lat_segment_cm(lat_diff_100nano);

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
  if (lat2_100nano >= lat1_100nano) {
    if (lon2_100nano >= lon1_100nano) {  // 1. Quadrant
      return angle;
    } else {  // 2. Quadrant
      return 180 - angle;
    }
  } else {
    if (lon2_100nano >= lon1_100nano) {  // 4. Quadrant
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
