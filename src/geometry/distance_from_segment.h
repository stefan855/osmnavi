#pragma once

#include <algorithm>
#include <cmath>
#include <iostream>

#include "base/constants.h"
#include "geometry/geometry.h"

// Describes the distance from a point to a line segment.
struct DistanceToSegment {
  double distance_cm = 0;
  double fraction_closest = 0.0;
  int32_t lat_closest = 0;
  int32_t lon_closest = 0;
  double t = 0.0;

  // "spaceship" operator, automatically defines ==, !=, <, <=, >, >=.
  auto operator<=>(const DistanceToSegment&) const = default;
};

// Convert lat/lon coordinate (given as deg * 10^7) to radians.
constexpr inline double IntDegToRad(int32_t int_deg) {
  return int_deg * (M_PI / 180.0 / TEN_POW_7);
}

// Compute the length of a specific longitude offset in cm on a circle of
// latitude at 'lat'.
constexpr double LonDistanceAtLat(int32_t lon, int32_t lat) {
  const double lat_rad = IntDegToRad(lat);
  return kEarthRadiusCm * IntDegToRad(lon) * std::cos(lat_rad);
}

// Return the length of a circle of longitude of length 'lat'.
constexpr double CmPerLatitudeDegree(int32_t lat) {
  return kEarthRadiusCm * IntDegToRad(lat);
}

// Given a length in centimeters, return the integer lat offset that corresponds
// to it.
constexpr int32_t LatDistanceForLength(int32_t length_cm) {
  double fraction =
      static_cast<double>(length_cm) / kEarthCircumReferenceCm;
  return 360 * TEN_POW_7_DBL * fraction;
}

// Compute the (x, y) distance from (lat0, lon0) to (lat1, lon1) in
// centimeters. Uses a flat (x,y) coordinate system with (lat0, lon0) at (0,0)
// for computation. Works reasonably well only for small distances.
constexpr void FastEarthDistanceXY(int32_t lat0, int32_t lon0, int32_t lat1,
                                   int32_t lon1, double* x, double* y) {
  double lat0_rad = IntDegToRad(lat0);
  *x = kEarthRadiusCm * IntDegToRad(lon1 - lon0) * std::cos(lat0_rad);
  *y = kEarthRadiusCm * IntDegToRad(lat1 - lat0);
}

// Compute distance of (lat_p, lon_p) to segment (lat_a,lon_a) -> (lat_b,lon_b).
// Returns the distance and the fraction of AB that has to be travelled to get
// to the closest point.
constexpr DistanceToSegment FastPointToSegmentDistance(
    int32_t lat_p, int32_t lon_p, int32_t lat_a, int32_t lon_a, int32_t lat_b,
    int32_t lon_b) {
  // Convert to local coordinates (A is origin)
  double x_p, y_p, x_b, y_b;
  FastEarthDistanceXY(lat_a, lon_a, lat_p, lon_p, &x_p, &y_p);
  FastEarthDistanceXY(lat_a, lon_a, lat_b, lon_b, &x_b, &y_b);

  double dx = x_b;  // x_a is 0
  double dy = y_b;  // y_a is 0
  double len_sq = dx * dx + dy * dy;

  if (len_sq < 1e-10) {
    // Segment is a point
    return {.distance_cm = std::hypot(x_p, y_p),
            .fraction_closest = 0.0,
            .lat_closest = lat_a,
            .lon_closest = lon_a,
            .t = 0.0};
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

  return {.distance_cm = std::hypot(x_p - closest_x, y_p - closest_y),
          .fraction_closest = fraction_closest,
          .lat_closest =
              lat_a + static_cast<int32_t>(fraction_closest * (lat_b - lat_a)),
          .lon_closest =
              lon_a + static_cast<int32_t>(fraction_closest * (lon_b - lon_a)),
          .t = t};
}

// Example usage demonstrating the separation
int ComputeTestDistance2() {
  int32_t lat_p = 488566000, lon_p = 23522000;   // Paris
  int32_t lat_a = 515074000, lon_a = -1278000;   // London
  int32_t lat_b = 525200000, lon_b = 134050000;  // Berlin

  // 1. Fast path: Just get distance (e.g., for filtering or sorting)
  DistanceToSegment dist =
      FastPointToSegmentDistance(lat_p, lon_p, lat_a, lon_a, lat_b, lon_b);

  std::cout << "Distance: " << (dist.distance_cm / 100.0) << " meters\n";
  LOG_S(INFO) << absl::StrFormat("Distance:  %.8f", dist.distance_cm);
  LOG_S(INFO) << absl::StrFormat("T clamped: %.8f", dist.fraction_closest);
  LOG_S(INFO) << absl::StrFormat("T:         %.8f", dist.t);
  LOG_S(INFO) << absl::StrFormat("lat:       %.8f",
                                 dist.lat_closest / TEN_POW_7_DBL);
  LOG_S(INFO) << absl::StrFormat("lon:       %.8f",
                                 dist.lon_closest / TEN_POW_7_DBL);
  return 0;
}

#if 0

// *************************************************************************
// *************************************************************************
// *************************************************************************
class EquirectangularApproximation {
 private:
  // static constexpr double EARTH_RADIUS_METERS = 6371000.0;
  static constexpr double DEG_TO_RAD = M_PI / 180.0;

  static double to_radians(double degrees) { return degrees * DEG_TO_RAD; }

  // Convert lat/lon to local x,y coordinates (meters) relative to reference
  // point (lat_a, lon_a)
  static void lat_lon_to_local(double lat_ref, double lon_ref, double lat,
                               double lon, double& x, double& y) {
    double lat_ref_rad = to_radians(lat_ref);

    x = kEarthRadiusCm * to_radians(lon - lon_ref) * std::cos(lat_ref_rad);
    y = kEarthRadiusCm * to_radians(lat - lat_ref);
  }

  // Convert local x,y back to lat/lon
  static void local_to_lat_lon(double lat_ref, double lon_ref, double x,
                               double y, double& lat, double& lon) {
    double lat_ref_rad = to_radians(lat_ref);

    lat = lat_ref + to_radians(y / kEarthRadiusCm);
    lon = lon_ref + to_radians(x / (kEarthRadiusCm * std::cos(lat_ref_rad)));
  }

 public:
  struct ClosestPointResult {
    double closest_lat;
    double closest_lon;
    bool on_segment;  // true if strictly between A and B, false if at an
                      // endpoint
  };

  /**
   * Computes ONLY the distance from point P to segment AB.
   * Optimized for speed: avoids inverse trigonometric conversions back to
   * lat/lon.
   */
  static double distance_point_to_segment(double lat_p, double lon_p,
                                          double lat_a, double lon_a,
                                          double lat_b, double lon_b) {
    // Convert to local coordinates (A is origin)
    double x_p, y_p, x_b, y_b;
    lat_lon_to_local(lat_a, lon_a, lat_p, lon_p, x_p, y_p);
    lat_lon_to_local(lat_a, lon_a, lat_b, lon_b, x_b, y_b);

    double dx = x_b;  // x_a is 0
    double dy = y_b;  // y_a is 0
    double len_sq = dx * dx + dy * dy;

    if (len_sq < 1e-10) {
      // Segment is a point
      return std::hypot(x_p, y_p);
    }

    // Project point onto line: t = dot(AP, AB) / |AB|^2
    double t = (x_p * dx + y_p * dy) / len_sq;
    LOG_S(INFO) << absl::StrFormat("AA0 T param: %.8f", t);

    // Clamp t to [0, 1] to stay on segment
    double t_clamped = std::max(0.0, std::min(1.0, t));

    // Calculate distance to clamped point
    double closest_x = t_clamped * dx;
    double closest_y = t_clamped * dy;

    return std::hypot(x_p - closest_x, y_p - closest_y);
  }

  /**
   * Computes the closest point coordinates (lat/lon) on segment AB to point P.
   * Includes the inverse conversion from local meters back to lat/lon.
   * Use this only when you need the actual coordinates.
   */
#if 0
  static ClosestPointResult get_closest_point_on_segment(
      double lat_p, double lon_p, double lat_a, double lon_a, double lat_b,
      double lon_b) {
    ClosestPointResult result;

    // Convert to local coordinates
    double x_p, y_p, x_b, y_b;
    lat_lon_to_local(lat_a, lon_a, lat_p, lon_p, x_p, y_p);
    lat_lon_to_local(lat_a, lon_a, lat_b, lon_b, x_b, y_b);

    double dx = x_b;
    double dy = y_b;
    double len_sq = dx * dx + dy * dy;

    if (len_sq < 1e-10) {
      // Segment is a point
      result.closest_lat = lat_a;
      result.closest_lon = lon_a;
      result.on_segment = true;
      return result;
    }

    // Project point onto line
    double t = (x_p * dx + y_p * dy) / len_sq;
    LOG_S(INFO) << absl::StrFormat("AA1 T closes point param: %.8f", t);

    // Determine if strictly on segment (excluding endpoints)
    // Using a small epsilon to avoid floating point noise at exact endpoints
    const double eps = 1e-9;
    result.on_segment = (t > eps && t < (1.0 - eps));

    // Clamp t
    double t_clamped = std::max(0.0, std::min(1.0, t));

    // Calculate closest point in local space
    double closest_x = t_clamped * dx;
    double closest_y = t_clamped * dy;

    // Convert back to lat/lon
    local_to_lat_lon(lat_a, lon_a, closest_x, closest_y, result.closest_lat,
                     result.closest_lon);

    return result;
  }
#endif

  static ClosestPointResult get_closest_point_on_segment(
      double lat_p, double lon_p, double lat_a, double lon_a, double lat_b,
      double lon_b) {
    ClosestPointResult result;

    // ... (Keep the distance calculation logic exactly as is to get 't') ...
    // Recalculate t here for clarity in this snippet
    double x_p, y_p, x_b, y_b;
    lat_lon_to_local(lat_a, lon_a, lat_p, lon_p, x_p, y_p);
    lat_lon_to_local(lat_a, lon_a, lat_b, lon_b, x_b, y_b);

    double dx = x_b;
    double dy = y_b;
    double len_sq = dx * dx + dy * dy;

    if (len_sq < 1e-10) {
      result.closest_lat = lat_a;
      result.closest_lon = lon_a;
      result.on_segment = true;
      return result;
    }

    double t = (x_p * dx + y_p * dy) / len_sq;
    double t_clamped = std::max(0.0, std::min(1.0, t));
    result.on_segment = (t > 1e-9 && t < (1.0 - 1e-9));

    // CORRECTED: Direct linear interpolation instead of inverse projection
    result.closest_lat = lat_a + t_clamped * (lat_b - lat_a);
    result.closest_lon = lon_a + t_clamped * (lon_b - lon_a);

    return result;
  }
};

// Example usage demonstrating the separation
int ComputeTestDistance() {
  double lat_p = 48.8566, lon_p = 2.3522;   // Paris
  double lat_a = 51.5074, lon_a = -0.1278;  // London
  double lat_b = 52.5200, lon_b = 13.4050;  // Berlin

  // 1. Fast path: Just get distance (e.g., for filtering or sorting)
  double dist = EquirectangularApproximation::distance_point_to_segment(
      lat_p, lon_p, lat_a, lon_a, lat_b, lon_b);

  std::cout << "Distance: " << (dist / 100) << " meters\n";

  // 2. Slow path: Only call this if you actually need the coordinates
  // (e.g., if the distance is below a threshold)
  if (dist < 50000000.0) {  // Arbitrary threshold
    auto closest = EquirectangularApproximation::get_closest_point_on_segment(
        lat_p, lon_p, lat_a, lon_a, lat_b, lon_b);

    std::cout << "Closest point: " << closest.closest_lat << ", "
              << closest.closest_lon << "\n";
    std::cout << "Strictly on segment: " << (closest.on_segment ? "yes" : "no")
              << "\n";
  }

  return 0;
}

#endif
