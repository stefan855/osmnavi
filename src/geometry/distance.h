#pragma once

#include <cmath>

// Compute distance between two points on the surface of the earth, using the
// haversine formula. Input coordinates are in units of 10^-7 degrees, i.e. they
// are multiplied by 10^7. This format is used in openstreetmap. Returned
// distance is in centimeters.
inline int64_t calculate_distance(int32_t lat1_100nano, int32_t lon1_100nano,
                                  int32_t lat2_100nano, int32_t lon2_100nano) {
  constexpr int64_t kEarthRadiusCm = 637100000;
  double lat1 = lat1_100nano * (std::numbers::pi / 180.0 / 10000000.0);
  double lon1 = lon1_100nano * (std::numbers::pi / 180.0 / 10000000.0);
  double lat2 = lat2_100nano * (std::numbers::pi / 180.0 / 10000000.0);
  double lon2 = lon2_100nano * (std::numbers::pi / 180.0 / 10000000.0);
  double f_dlat = std::sin((lat2 - lat1) / 2.0);
  double f_dlon = std::sin((lon2 - lon1) / 2.0);
  double a =
      f_dlat * f_dlat + std::cos(lat1) * std::cos(lat2) * f_dlon * f_dlon;
  return std::round(2.0 * kEarthRadiusCm *
                    std::atan2(std::sqrt(a), std::sqrt(1.0 - a)));
}
