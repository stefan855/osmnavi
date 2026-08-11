#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include <cmath>

#include "base/deg_coord.h"
#include "base/util.h"
#include "geometry/distance.h"
#include "geometry/distance_to_segment.h"
#include "geometry/fast_country_polygons.h"
#include "geometry/line_clipping.h"
#include "geometry/polygon.h"
#include "geometry/tiled_country_lookup.h"

void TestHelpers() {
  FUNC_TIMER();
  double km_cm = 1.0 * 1000.0 * 100.0;
  CHECK_DOUBLE_EQ_S(LatDistanceForLength(km_cm).AsDouble(),
                    (km_cm / kEarthCircumReferenceCm) * 360.0, 0.01);
}

void TestDistanceToSegment() {
  FUNC_TIMER();

  constexpr LatE6 lat_a(1.0);
  constexpr LonE6 lon_a(1.0);
  constexpr LatE6 lat_b(1.0);
  constexpr LonE6 lon_b(2.0);

  {
    constexpr double expected = CmPerLatitudeDegree(LatE6(100));
    LatE6 lat_p(lat_a.v() - 100);
    LonE6 lon_p(1.5);
    DistanceToSegment dist = FastPointToSegmentDistance(
        {lat_p, lon_p}, {lat_a, lon_a}, {lat_b, lon_b});
    LOG_S(INFO) << absl::StrFormat("Distance:           %.8f",
                                   dist.distance_to_seg_cm);
    LOG_S(INFO) << absl::StrFormat("T fraction_closest: %.8f",
                                   dist.fraction_closest);
    CHECK_DOUBLE_EQ_S(dist.distance_to_seg_cm, expected, 0.01);
    CHECK_DOUBLE_EQ_S(dist.fraction_closest, 0.5, 0.01)
  }
  {
    constexpr LatE6 lat_p(lat_a.v() - 100);
    constexpr LonE6 lon_p(lon_a.v() - 100);
    // We expect a distance of ~1.4 meters.
    constexpr double expected = std::hypot(LonDistanceAtLat(LonE6(100), lat_a),
                                           CmPerLatitudeDegree(LatE6(100)));

    DistanceToSegment dist = FastPointToSegmentDistance(
        {lat_p, lon_p}, {lat_a, lon_a}, {lat_b, lon_b});
    LOG_S(INFO) << absl::StrFormat("Distance:           %.8f",
                                   dist.distance_to_seg_cm);
    LOG_S(INFO) << absl::StrFormat("T fraction_closest: %.8f",
                                   dist.fraction_closest);
    CHECK_DOUBLE_EQ_S(dist.distance_to_seg_cm, expected, 0.01);
    CHECK_DOUBLE_EQ_S(dist.fraction_closest, 0.0, 0.01)
  }
  {
    constexpr LatE6 lat_p(lat_b.v() + 100);
    constexpr LonE6 lon_p(lon_b.v() + 100);
    // We expect a distance of ~1.4 meters.
    constexpr double expected = std::hypot(LonDistanceAtLat(LonE6(100), lat_a),
                                           CmPerLatitudeDegree(LatE6(100)));

    DistanceToSegment dist = FastPointToSegmentDistance(
        {lat_p, lon_p}, {lat_a, lon_a}, {lat_b, lon_b});
    LOG_S(INFO) << absl::StrFormat("Distance:           %.8f",
                                   dist.distance_to_seg_cm);
    LOG_S(INFO) << absl::StrFormat("T fraction_closest: %.8f",
                                   dist.fraction_closest);
    CHECK_DOUBLE_EQ_S(dist.distance_to_seg_cm, expected, 0.01);
    CHECK_DOUBLE_EQ_S(dist.fraction_closest, 1.0, 0.01)
  }
}

void TestCalculateDistance() {
  LatE6 lat_paris(48.8566);
  LonE6 lon_paris(2.3522);
  LatE6 lat_berlin(52.5200);
  LonE6 lon_berlin(13.4050);
  const uint32_t dist_cm =
      calculate_distance(lat_paris, lon_paris, lat_berlin, lon_berlin);
  CHECK_DOUBLE_EQ_S(dist_cm, 878 * 1000 * 100, 0.001);
  const uint32_t dist_rev_cm =
      calculate_distance(lat_berlin, lon_berlin, lat_paris, lon_paris);
  CHECK_DOUBLE_EQ_S(dist_cm, dist_rev_cm, 0.00001);
}

#if 0
uint32_t ComputeEastAngle(double lat1, double lon1, double lat2, double lon2) {
  const LatE6 lat1n(lat1);
  const LonE6 lon1n(lon1);
  const LatE6 lat2n(lat2);
  const LonE6 lon2n(lon2);
  const uint32_t length_cm = calculate_distance(lat1n, lon1n, lat2n, lon2n);
  return angle_to_east_degrees({lat1n, lon1n}, {lat2n, lon2n}, length_cm);
}

int32_t CheckEastAngle(double lat1, double lon1, double lat2, double lon2,
                       int32_t expected) {
  int32_t angle = ComputeEastAngle(lat1, lon1, lat2, lon2);
  LOG_S(INFO) << absl::StrFormat("Edge (%.1f,%.1f) to (%.1f,%.1f) has angle %d",
                                 lat1, lon1, lat2, lon2, angle);
  CHECK_EQ_S(angle, expected)
      << absl::StrFormat("Edge (%.7f,%.7f) to (%.7f,%.7f) has angle %d", lat1,
                         lon1, lat2, lon2, angle);
  return angle;
}

void TestEdgeEastAngles() {
  FUNC_TIMER();
  // Check the angles of edges going from lat/long (0,0) to some points on the
  // rectangle of +-1 degree around (0,0).
  // The angle of (0,0) to (0.5,1) was manually verified to be approximately 27
  // degrees, using the following python formula:
  //   math.asin(0.5/math.sqrt(1+0.5*0.5)) / math.pi * 180
  CheckEastAngle(0.0, 0.0, 0.0, 1.0, /*expected=*/0);
  CheckEastAngle(0.0, 0.0, 0.5, 1.0, /*expected=*/27);
  CheckEastAngle(0.0, 0.0, 1.0, 1.0, /*expected=*/45);
  CheckEastAngle(0.0, 0.0, 1.0, 0.5, /*expected=*/63);

  CheckEastAngle(0.0, 0.0, 1.0, 0.0, /*expected=*/90);
  CheckEastAngle(0.0, 0.0, 1.0, -0.5, /*expected=*/117);
  CheckEastAngle(0.0, 0.0, 1.0, -1.0, /*expected=*/135);
  CheckEastAngle(0.0, 0.0, 0.5, -1.0, /*expected=*/153);

  CheckEastAngle(0.0, 0.0, 0.0, -1.0, /*expected=*/180);
  CheckEastAngle(0.0, 0.0, -0.5, -1.0, /*expected=*/207);
  CheckEastAngle(0.0, 0.0, -1.0, -1.0, /*expected=*/225);
  CheckEastAngle(0.0, 0.0, -1.0, -0.5, /*expected=*/243);

  CheckEastAngle(0.0, 0.0, -1.0, 0.0, /*expected=*/270);
  CheckEastAngle(0.0, 0.0, -1.0, 0.5, /*expected=*/297);
  CheckEastAngle(0.0, 0.0, -1.0, 1.0, /*expected=*/315);
  CheckEastAngle(0.0, 0.0, -0.5, 1.0, /*expected=*/333);

  // This code shifts the edge (0,0) to (1,1) north in every step.
  // The angle should grow as we get closer to the pole because the latitude
  // circle is shrinking.
  int32_t prev_angle = 45;
  for (uint32_t i = 4; i < 90; i += 5) {
    const LatE6 lat1(static_cast<double>(i));
    const LonE6 lon1(0.0);
    const LatE6 lat2(static_cast<double>(i + 1));
    const LonE6 lon2(1.0);
    uint32_t length_cm = calculate_distance(lat1, lon1, lat2, lon2);
    int32_t angle =
        angle_to_east_degrees({lat1, lon1}, {lat2, lon2}, length_cm);
    LOG_S(INFO) << absl::StrFormat("angle of (%u,%u)->(%u,%u) is %u", i, 0,
                                   i + 1, 1, angle);
    CHECK_GE_S(angle, prev_angle);
    prev_angle = angle;
  }
}

void TestEastAngleBetweenEdges() {
  FUNC_TIMER();
  CHECK_EQ_S(angle_between_edges(0, 0), 0);
  CHECK_EQ_S(angle_between_edges(1, 0), -1);
  CHECK_EQ_S(angle_between_edges(0, 1), 1);
  CHECK_EQ_S(angle_between_edges(30, 0), -30);
  CHECK_EQ_S(angle_between_edges(0, 30), 30);

  CHECK_EQ_S(angle_between_edges(0, 179), 179);
  CHECK_EQ_S(angle_between_edges(0, 180), 180);
  CHECK_EQ_S(angle_between_edges(0, 181), -179);

  CHECK_EQ_S(angle_between_edges(179, 0), -179);
  CHECK_EQ_S(angle_between_edges(180, 0), 180);
  CHECK_EQ_S(angle_between_edges(180, 180), 0);
  CHECK_EQ_S(angle_between_edges(181, 0), 179);

  CHECK_EQ_S(angle_between_edges(269, 0), 91);
  CHECK_EQ_S(angle_between_edges(270, 0), 90);
  CHECK_EQ_S(angle_between_edges(271, 0), 89);

  CHECK_EQ_S(angle_between_edges(0, 359), -1);

  CHECK_EQ_S(angle_between_edges(90, 269), 179);
  CHECK_EQ_S(angle_between_edges(90, 270), 180);
  CHECK_EQ_S(angle_between_edges(90, 271), -179);
}

void TestRealEastAngles() {
  FUNC_TIMER();
  // Test a real junction: https://www.openstreetmap.org/node/28581626
  // Mid-Node:   28581626, latlon=47.3779735, 8.5267194
  // Nodes with estimated angles to east beam:
  // 1. North:    3946827990, latlon=47.3784253, 8.5269862 angle:70
  // 2. West:    12456090319, latlon=47.3779953, 8.5266785 angle:140
  // 3. South:      28581602, latlon=47.3773794, 8.5263690 angle:250
  // 4. East:     6165393392, latlon=47.3779501, 8.5267669 angle:320

  // All edges go from mid point to outer point. The estimated angles (see
  // above) have been adjusted to the expected angles (see below) after checking
  // they are not deviating significantly.
  CheckEastAngle(47.3779735, 8.5267194, 47.3784253, 8.5269862, 68);
  CheckEastAngle(47.3779735, 8.5267194, 47.3779953, 8.5266785, 142);
  CheckEastAngle(47.3779735, 8.5267194, 47.3773794, 8.5263690, 248);
  CheckEastAngle(47.3779735, 8.5267194, 47.3779501, 8.5267669, 324);
}
#endif

void TestLineClipping() {
  LOG_S(INFO) << "TestLineClipping() started";
  TwoPoint r = {10.0, 10.0, 20.0, 20.0};

  {
    TwoPoint line = {5.0, 5.0, 20.0, 15.0};
    CHECK_S(ClipLineCohenSutherland(r, &line));
    LOG_S(INFO) << absl::StrFormat("line: %f %f %f %f", line.x0, line.y0,
                                   line.x1, line.y1);
  }

  LOG_S(INFO) << "TestLineClipping() finished";
}

void TestFastPolygonContains() {
  LOG_S(INFO) << "TestFastPolygonContains() started";
  const int16_t country = 123;

  {
    //   *
    //  *
    // *
    FastCountryPolygons p;
    p.AddLine(0, 0, 100, 100, country);
    CHECK_EQ_S(p.CountIntersections(0, 50), 1);
    CHECK_EQ_S(p.CountIntersections(60, 50), 0);
    CHECK_EQ_S(p.CountIntersections(100, 50), 0);
    CHECK_EQ_S(p.CountIntersections(200, 50), 0);
    CHECK_EQ_S(p.CountIntersections(51, 50), 0);
    CHECK_EQ_S(p.CountIntersections(49, 50), 1);
    CHECK_EQ_S(p.CountIntersections(00, 0), 0);
    CHECK_EQ_S(p.CountIntersections(00, 100), 1);
  }

  {
    // *
    //  *
    //   *
    FastCountryPolygons p;
    p.AddLine(0, 100, 100, 0, country);
    CHECK_EQ_S(p.CountIntersections(0, 50), 1);
    CHECK_EQ_S(p.CountIntersections(60, 50), 0);
    CHECK_EQ_S(p.CountIntersections(100, 50), 0);
    CHECK_EQ_S(p.CountIntersections(200, 50), 0);
    CHECK_EQ_S(p.CountIntersections(51, 50), 0);
    CHECK_EQ_S(p.CountIntersections(49, 50), 1);
    CHECK_EQ_S(p.CountIntersections(0, 0), 0);
    CHECK_EQ_S(p.CountIntersections(0, 100), 1);
  }
  {
    //   *
    //  * *
    // *   *
    FastCountryPolygons p;
    p.AddLine(0, 0, 100, 100, country);
    p.AddLine(100, 100, 200, 0, country);
    CHECK_EQ_S(p.CountIntersections(200, 0), 0);
    CHECK_EQ_S(p.CountIntersections(0, 100), 2);
    CHECK_EQ_S(p.CountIntersections(0, 0), 0);
    CHECK_EQ_S(p.CountIntersections(100, 50), 1);
  }

  {
    // ***
    FastCountryPolygons p;
    p.AddLine(0, 0, 100, 0, country);
    CHECK_EQ_S(p.CountIntersections(-100, 0), 0);
    CHECK_EQ_S(p.CountIntersections(0, 0), 0);
    CHECK_EQ_S(p.CountIntersections(100, 0), 0);
    CHECK_EQ_S(p.CountIntersections(200, 0), 0);
  }

  {
    //  *
    //  *
    //  *
    FastCountryPolygons p;
    p.AddLine(100, 0, 100, 100, country);
    CHECK_EQ_S(p.CountIntersections(0, 0), 0);
    CHECK_EQ_S(p.CountIntersections(0, 100), 1);
  }

  {
    // Check that binary search works on the lines sorted by y-axis.
    // For this, insert lines in random order and expect 'p.PrepareData()' to
    // sort them properly.
    FastCountryPolygons p;
    const int y_ray = 5100;
    const int height = 100;
    int expected_intersections = 0;
    std::srand(1);  // Get always the same pseudo-random numbers.
    for (int i = 0; i < 10000; ++i) {
      const int y_val = rand() % 10000;
      if (y_val >= y_ray - height && y_val < y_ray) {
        expected_intersections++;
      }
      p.AddLine(100, y_val, 100, y_val + 100, country + i);
    }
    CHECK_GT_S(expected_intersections, 0);
    p.PrepareData();
    CHECK_EQ_S(p.CountIntersections(0, y_ray), expected_intersections);
  }

  {
    std::vector<FastCountryPolygons::Line> lines;
    lines.push_back({.x0 = 0,
                     .y0 = 0,
                     .x1 = 0,
                     .y1 = 0,
                     .country_num_1 = 1,
                     .country_num_2 = 0});
    lines.push_back({.x0 = 0,
                     .y0 = 0,
                     .x1 = 0,
                     .y1 = 0,
                     .country_num_1 = 2,
                     .country_num_2 = 0});
    FastCountryPolygons::MergeDupLines(&lines);
    CHECK_EQ_S(lines.size(), 1u);
    CHECK_EQ_S(lines.at(0).country_num_1, 1);
    CHECK_EQ_S(lines.at(0).country_num_2, 2);
  }

  LOG_S(INFO) << "TestFastPolygonContains() finished";
}

void TestTiledCountryLookup() {
  LOG_S(INFO) << "TestTiledCountryLookup() started";
  const int16_t country = 123;
  const int32_t kDegreeUnits = TiledCountryLookup::kDegreeUnits;
  // Tiling has step size 10 degrees.
  const int32_t tile_size = kDegreeUnits * 10;

  {
    FastCountryPolygons p;
    // Vertical line fully crossing tiles with x,y: (1,0) and (1,1).
    // tile (1,2) is partially crossed (it contains the end of the line).
    // This causes tiles (0,0) and (0,1) to be assigned to 'country'. Tile (0,2)
    // is partially assigned to 'country, and tile 0,3) has no country.
    p.AddLine(tile_size + 1, -1, tile_size + 1, 2 * tile_size + 1000, country);
    TiledCountryLookup tiler(p, tile_size);
    CHECK_EQ_S(tiler.GetCountryNum(LonE6(1), LatE6(1)), country);

    // CHECK_EQ_S(p.CountIntersections(0, 50), 1);
  }
  LOG_S(INFO) << "TestTiledCountryLookup() finished";
}

uint32_t ComputeBearing(double lat1, double lon1, double lat2, double lon2) {
  const LatE6 lat1n(lat1);
  const LonE6 lon1n(lon1);
  const LatE6 lat2n(lat2);
  const LonE6 lon2n(lon2);
  const uint32_t length_cm = calculate_distance(lat1n, lon1n, lat2n, lon2n);
  return true_north_bearing({lat1n, lon1n}, {lat2n, lon2n}, length_cm);
}

int32_t CheckBearing(double lat1, double lon1, double lat2, double lon2,
                     int32_t expected) {
  int32_t angle = ComputeBearing(lat1, lon1, lat2, lon2);
  LOG_S(INFO) << absl::StrFormat("Edge (%.1f,%.1f) to (%.1f,%.1f) has angle %d",
                                 lat1, lon1, lat2, lon2, angle);
  CHECK_EQ_S(angle, expected)
      << absl::StrFormat("Edge (%.7f,%.7f) to (%.7f,%.7f) has angle %d", lat1,
                         lon1, lat2, lon2, angle);
  return angle;
}

void TestEdgeBearings() {
  FUNC_TIMER();
  // Check the angles of edges going from lat/long (0,0) to some points on the
  // rectangle of +-1 degree around (0,0).
  // The angle of (0,0) to (0.5,1) was manually verified to be approximately 27
  // degrees, using the following python formula:
  //   math.asin(0.5/math.sqrt(1+0.5*0.5)) / math.pi * 180
  CheckBearing(0.0, 0.0, 1.0, 0.0, /*expected=*/0);
  CheckBearing(0.0, 0.0, 1.0, 0.5, /*expected=*/27);
  CheckBearing(0.0, 0.0, 1.0, 1.0, /*expected=*/45);
  CheckBearing(0.0, 0.0, 0.5, 1.0, /*expected=*/63);

  CheckBearing(0.0, 0.0, 0.0, 1.0, /*expected=*/90);
  CheckBearing(0.0, 0.0, -0.5, 1.0, /*expected=*/117);
  CheckBearing(0.0, 0.0, -1.0, 1.0, /*expected=*/135);
  CheckBearing(0.0, 0.0, -1.0, 0.5, /*expected=*/153);

  CheckBearing(0.0, 0.0, -1.0, 0.0, /*expected=*/180);
  CheckBearing(0.0, 0.0, -1.0, -0.5, /*expected=*/207);
  CheckBearing(0.0, 0.0, -1.0, -1.0, /*expected=*/225);
  CheckBearing(0.0, 0.0, -0.5, -1.0, /*expected=*/243);

  CheckBearing(0.0, 0.0, 0.0, -1.0, /*expected=*/270);
  CheckBearing(0.0, 0.0, 0.5, -1.0, /*expected=*/297);
  CheckBearing(0.0, 0.0, 1.0, -1.0, /*expected=*/315);
  CheckBearing(0.0, 0.0, 1.0, -0.5, /*expected=*/333);

  // This code shifts the edge (0,0) to (1,1) north in every step.
  // The angle should shrink as we get closer to the pole because the latitude
  // circle is shrinking.
  int32_t prev_angle = 45;
  for (uint32_t i = 4; i < 90; i += 5) {
    const LatE6 lat1(static_cast<double>(i));
    const LonE6 lon1(0.0);
    const LatE6 lat2(static_cast<double>(i + 1));
    const LonE6 lon2(1.0);
    uint32_t length_cm = calculate_distance(lat1, lon1, lat2, lon2);
    int32_t angle = true_north_bearing({lat1, lon1}, {lat2, lon2}, length_cm);
    LOG_S(INFO) << absl::StrFormat("angle of (%u,%u)->(%u,%u) is %u", i, 0,
                                   i + 1, 1, angle);
    CHECK_LE_S(angle, prev_angle);
    prev_angle = angle;
  }
}

void TestSomeRealBearings() {
  FUNC_TIMER();
  // Test a real junction: https://www.openstreetmap.org/node/28581626
  // Mid-Node:   28581626, latlon=47.3779735, 8.5267194
  // Nodes with estimated angles to east beam:
  // 1. North:    3946827990, latlon=47.3784253, 8.5269862 angle:20
  // 2. West:    12456090319, latlon=47.3779953, 8.5266785 angle:310
  // 3. South:      28581602, latlon=47.3773794, 8.5263690 angle:200
  // 4. East:     6165393392, latlon=47.3779501, 8.5267669 angle:130

  // All edges go from mid point to outer point. The estimated angles (see
  // above) have been adjusted to the expected angles (see below) after checking
  // they are not deviating significantly.
  CheckBearing(47.3779735, 8.5267194, 47.3784253, 8.5269862, 22);
  CheckBearing(47.3779735, 8.5267194, 47.3779953, 8.5266785, 308);
  CheckBearing(47.3779735, 8.5267194, 47.3773794, 8.5263690, 202);
  CheckBearing(47.3779735, 8.5267194, 47.3779501, 8.5267669, 126);
}

void TestBearingBetweenEdges() {
  FUNC_TIMER();
  CHECK_EQ_S(angle_between_edges(0, 0), 0);
  CHECK_EQ_S(angle_between_edges(1, 0), -1);
  CHECK_EQ_S(angle_between_edges(0, 1), 1);
  CHECK_EQ_S(angle_between_edges(30, 0), -30);
  CHECK_EQ_S(angle_between_edges(0, 30), 30);

  CHECK_EQ_S(angle_between_edges(0, 179), 179);
  CHECK_EQ_S(angle_between_edges(0, 180), -180);
  CHECK_EQ_S(angle_between_edges(0, 181), -179);

  CHECK_EQ_S(angle_between_edges(179, 0), -179);
  CHECK_EQ_S(angle_between_edges(180, 0), -180);
  CHECK_EQ_S(angle_between_edges(180, 180), 0);
  CHECK_EQ_S(angle_between_edges(181, 0), 179);

  CHECK_EQ_S(angle_between_edges(269, 0), 91);
  CHECK_EQ_S(angle_between_edges(270, 0), 90);
  CHECK_EQ_S(angle_between_edges(271, 0), 89);

  CHECK_EQ_S(angle_between_edges(0, 359), -1);

  CHECK_EQ_S(angle_between_edges(90, 269), 179);
  CHECK_EQ_S(angle_between_edges(90, 270), -180);
  CHECK_EQ_S(angle_between_edges(90, 271), -179);
}

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  if (argc != 1) {
    ABORT_S() << absl::StrFormat("usage: %s", argv[0]);
  }

  TestDistanceToSegment();
  TestHelpers();
  TestCalculateDistance();
#if 0
  TestEdgeEastAngles();
  TestEastAngleBetweenEdges();
  TestRealEastAngles();
#endif
  TestLineClipping();
  TestFastPolygonContains();
  TestTiledCountryLookup();
  TestEdgeBearings();
  TestSomeRealBearings();
  TestBearingBetweenEdges();

  LOG_S(INFO)
      << "\n\033[1;32m*****************************\nTesting successfully "
         "finished\n*****************************\033[0m";
  return 0;
}
