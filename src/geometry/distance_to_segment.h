#pragma once

#include <algorithm>
#include <cmath>
#include <iostream>

#include "base/constants.h"
#include "geometry/geometry.h"

// Describes the distance from a point to a line segment.
struct DistanceToSegment {
  double distance_to_seg_cm = 0;
  double fraction_closest = 0.0;
  LatLon coord_closest;

  // "spaceship" operator, automatically defines ==, !=, <, <=, >, >=.
  auto operator<=>(const DistanceToSegment&) const = default;
};

// Compute the length of a specific longitude offset in cm on a circle of
// latitude at 'lat'.
constexpr double LonDistanceAtLat(LonE6 lon, LatE6 lat) {
  return kEarthRadiusCm * lon.ToRad() * std::cos(lat.ToRad());
}

// Return the length of a circle of longitude of length 'lat'.
constexpr double CmPerLatitudeDegree(LatE6 lat) {
  return kEarthRadiusCm * lat.ToRad();
}

// Given a length in centimeters, return the integer lat offset that corresponds
// to it.
constexpr LatE6 LatDistanceForLength(int32_t length_cm) {
  double fraction = static_cast<double>(length_cm) / kEarthCircumReferenceCm;
  return LatE6(360.0 * fraction);
}

// Compute the (x, y) distance from (lat0, lon0) to (lat1, lon1) in
// centimeters. Uses a flat (x,y) coordinate system with (lat0, lon0) at (0,0)
// for computation. Works reasonably well only for small distances.
constexpr void FastEarthDistanceXY(LatLon p0, LatLon p1, double* x, double* y) {
  *x = kEarthRadiusCm * LonE6(p1.lon.v64() - p0.lon.v64()).ToRad() *
       std::cos(p0.lat.ToRad());
  *y = kEarthRadiusCm * LatE6(p1.lat.v64() - p0.lat.v64()).ToRad();
}

// Compute distance of (lat_p, lon_p) to segment (lat_a,lon_a) -> (lat_b,lon_b).
// Returns the distance and the fraction of AB that has to be travelled to get
// to the closest point.
constexpr DistanceToSegment FastPointToSegmentDistance(LatLon p, LatLon a,
                                                       LatLon b) {
  // LatE6 lat_p, LonE6 lon_p, LatE6 lat_a, LonE6 lon_a, LatE6 lat_b, LonE6
  // lon_b) { Convert to local coordinates (A is origin)
  double x_p, y_p, x_b, y_b;
  // FastEarthDistanceXY(lat_a, lon_a, lat_p, lon_p, &x_p, &y_p);
  // FastEarthDistanceXY(lat_a, lon_a, lat_b, lon_b, &x_b, &y_b);
  FastEarthDistanceXY(a, p, &x_p, &y_p);
  FastEarthDistanceXY(a, b, &x_b, &y_b);

  double dx = x_b;  // x_a is 0
  double dy = y_b;  // y_a is 0
  double len_sq = dx * dx + dy * dy;

  if (len_sq < 1e-10) {
    // Segment is a point
    return {.distance_to_seg_cm = std::hypot(x_p, y_p),
            .fraction_closest = 0.0,
            .coord_closest = a};
  }

  // Project point onto line: t = dot(AP, AB) / |AB|^2
  // Note that
  //   - phi = arc between AP and AB.
  //   - dot(AP, AB) = |AP|*|AB|*cos(phi).
  //   - l = |AP| * cos(phi) is the length of the projected line of AP unto AB.
  //   - t = dot(AP, AB) / |AB|^2 = |AP|*|AB|*cos(phi) / |AB|^2 = l / |AB|.
  //   - i.e. t is the fraction of l over AB.
  double t = (x_p * dx + y_p * dy) / len_sq;

  // Clamp t to [0, 1] to stay on segment
  double fraction_closest = std::max(0.0, std::min(1.0, t));

  // Calculate distance to clamped point
  double closest_x = fraction_closest * dx;
  double closest_y = fraction_closest * dy;

  return {.distance_to_seg_cm = std::hypot(x_p - closest_x, y_p - closest_y),
          .fraction_closest = fraction_closest,
          .coord_closest = {
              LatE6(a.lat.v64() +
                    static_cast<int64_t>(fraction_closest *
                                         (b.lat.v64() - a.lat.v64()))),
              LonE6(a.lon.v64() +
                    static_cast<int64_t>(fraction_closest *
                                         (b.lon.v64() - a.lon.v64())))}};
}
