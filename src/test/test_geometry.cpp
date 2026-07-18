#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include <cmath>

#include "base/deg_coord.h"
#include "base/util.h"
#include "geometry/distance_to_segment.h"

void TestHelpers() {
  FUNC_TIMER();
  double km_cm = 1.0 * 1000.0 * 100.0;
  CHECK_DOUBLE_EQ_S(LatDistanceForLength(km_cm).AsDouble(),
                    (km_cm / kEarthCircumReferenceCm) * 360.0,
                    0.01);
}

void TestDistanceToSegment() {
  FUNC_TIMER();

  constexpr DegE6 lat_a(1.0) , lon_a(1.0);
  constexpr DegE6 lat_b(1.0), lon_b(2.0);

  {
    constexpr double expected = CmPerLatitudeDegree(DegE6(100));
    DegE6 lat_p(lat_a.v() - 100), lon_p(1.5);
    DistanceToSegment dist =
        FastPointToSegmentDistance(lat_p, lon_p, lat_a, lon_a, lat_b, lon_b);
    LOG_S(INFO) << absl::StrFormat("Distance:           %.8f",
                                   dist.distance_to_seg_cm);
    LOG_S(INFO) << absl::StrFormat("T fraction_closest: %.8f",
                                   dist.fraction_closest);
    CHECK_DOUBLE_EQ_S(dist.distance_to_seg_cm, expected, 0.01);
    CHECK_DOUBLE_EQ_S(dist.fraction_closest, 0.5, 0.01)
  }
  {
    constexpr DegE6 lat_p(lat_a.v() - 100), lon_p(lon_a.v() - 100);
    // We expect a distance of ~1.4 meters.
    constexpr double expected = std::hypot(LonDistanceAtLat(DegE6(100), lat_a),
                                           CmPerLatitudeDegree(DegE6(100)));

    DistanceToSegment dist =
        FastPointToSegmentDistance(lat_p, lon_p, lat_a, lon_a, lat_b, lon_b);
    LOG_S(INFO) << absl::StrFormat("Distance:           %.8f",
                                   dist.distance_to_seg_cm);
    LOG_S(INFO) << absl::StrFormat("T fraction_closest: %.8f",
                                   dist.fraction_closest);
    CHECK_DOUBLE_EQ_S(dist.distance_to_seg_cm, expected, 0.01);
    CHECK_DOUBLE_EQ_S(dist.fraction_closest, 0.0, 0.01)
  }
  {
    constexpr DegE6 lat_p(lat_b.v() + 100), lon_p(lon_b.v() + 100);
    // We expect a distance of ~1.4 meters.
    constexpr double expected = std::hypot(LonDistanceAtLat(DegE6(100), lat_a),
                                           CmPerLatitudeDegree(DegE6(100)));

    DistanceToSegment dist =
        FastPointToSegmentDistance(lat_p, lon_p, lat_a, lon_a, lat_b, lon_b);
    LOG_S(INFO) << absl::StrFormat("Distance:           %.8f",
                                   dist.distance_to_seg_cm);
    LOG_S(INFO) << absl::StrFormat("T fraction_closest: %.8f",
                                   dist.fraction_closest);
    CHECK_DOUBLE_EQ_S(dist.distance_to_seg_cm, expected, 0.01);
    CHECK_DOUBLE_EQ_S(dist.fraction_closest, 1.0, 0.01)
  }
}

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  if (argc != 1) {
    ABORT_S() << absl::StrFormat("usage: %s", argv[0]);
  }

  TestDistanceToSegment();
  TestHelpers();

  LOG_S(INFO)
      << "\n\033[1;32m*****************************\nTesting successfully "
         "finished\n*****************************\033[0m";
  return 0;
}
