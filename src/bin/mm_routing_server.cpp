// #define CPPHTTPLIB_OPENSSL_SUPPORT
#include <math.h>

#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>
#include <string_view>

#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "algos/complex_turn_restriction.h"
#include "algos/mm_cluster_router.h"
#include "base/argli.h"
#include "base/thread_pool.h"
#include "base/top_n.h"
#include "base/util.h"
#include "cpp-httplib/httplib.h"
#include "geometry/distance_from_segment.h"
#include "graph/mmgraph_def.h"

struct MMRoutingResult {
  bool found = false;
  // If a route was found, the distance from start to target node.
  uint32_t found_distance = 0;
  uint32_t num_shortest_route_nodes = 0;
  std::vector<uint32_t> route_v_idx;  // Filled iff found == true.

  // Internal stats.
  uint32_t num_visited = 0;  // edges or nodes, depending on router type.
  uint32_t num_complex_turn_restriction_keys = 0;
  float complex_turn_restriction_keys_reduction_factor = 0;
};

namespace {

#if 0
MMClusterBoundingRect ComputePointRec(int32_t lat, int32_t lon, max_dist_cm) {
  MMClusterBoundingRect br;

  return br;
}
#endif

struct ClosestEdge {
  DistanceToSegment dts;
  MMFullEdge fe;
  bool valid = false;

  std::string DebugString(const MMGraph& mg, uint32_t origin_lat,
                          uint32_t origin_lon) const {
    const MMCluster& mc = mg.clusters.at(fe.cluster_id);
    return absl::StrFormat(
        "Closest Edge for (%.7f, %.7f) dist:%.2fm n0:%lli n1:%lli fc:%.2f",
        origin_lat, origin_lon, dts.distance_cm / 100.0,
        mc.get_node_id(fe.from_node_idx),
        mc.get_node_id(fe.edge(mc).target_idx()), dts.fraction_closest);
  }

  // "spaceship" operator, automatically defines ==, !=, <, <=, >, >=.
  // auto operator<=>(const ClosestEdge&) const = default;
  auto operator<=>(const ClosestEdge& other) const { return dts <=> other.dts; }
};

MMGeoAnchor FindClosestEdges(const MMGraph& mg, int32_t lat, int32_t lon) {
#if 0
  // Bounding rect containing (lat, lon) in the middle. The bounding rect
  // shrinks as we find line segments that are closer.
  MMBoundingRect PointRec;
#endif

  TopN<ClosestEdge, 1, /*keep_greater=*/false> topn;
  topn.Add({.dts = {.distance_cm = MAXU31},
            .fe = {.from_node_idx = MAXU32},
            .valid = false});

  for (const MMClusterBoundingRect& br : mg.sorted_bounding_rects.span()) {
    const MMCluster& mc = mg.clusters.at(br.cluster_id);
    for (uint32_t n0_idx = 0; n0_idx < mc.nodes.size(); ++n0_idx) {
      const MMLatLon& n0_coord = mc.node_to_latlon.at(n0_idx);
      for (uint32_t e_idx : mc.edge_indices(n0_idx)) {
        uint32_t n1_idx = mc.get_edge(e_idx).target_idx();
        const MMLatLon& n1_coord = mc.node_to_latlon.at(n1_idx);
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

  MMGeoAnchor a = {.point = {.lat = lat, .lon = lon}};
  for (const ClosestEdge& ce : topn.span()) {
    if (ce.valid) {
      a.edges.push_back({.distance_cm = ce.dts.distance_cm,
                         .fraction = ce.dts.fraction_closest,
                         .lat_at_fraction = ce.dts.lat_closest,
                         .lon_at_fraction = ce.dts.lon_closest,
                         .fe = ce.fe});
      LOG_S(INFO) << absl::StrFormat("fraction_closest:%.3f",
                                     ce.dts.fraction_closest);
    }
  }

  if (a.edges.size() == 1) {
    // Find backward edge
    const MMEdgePoint& ep = a.edges.front();
    const MMCluster& mc = ep.fe.mc(mg);
    uint32_t backward_idx = mc.find_edge_idx(
        ep.fe.target_idx(mc), ep.fe.from_node_idx, ep.fe.way_idx(mc));
    if (backward_idx != INFU32) {
      a.edges.push_back(
          {.distance_cm = ep.distance_cm,
           .fraction = 1.0 - ep.fraction,
           .lat_at_fraction = ep.lat_at_fraction,
           .lon_at_fraction = ep.lon_at_fraction,
           .fe = MMFullEdge::Create(mc, ep.fe.target_idx(mc), backward_idx)});
    }
  }

  // This returns an entry with valid=false in case there are no results.
  return a;
}

#if 0
// Function to calculate the bearing between two points.
double CalculateAngle(double lat1, double lon1, double lat2, double lon2) {
  // Convert latitude and longitude from degrees to radians.
  double lat1_rad = lat1 * M_PI / 180.0;
  double lon1_rad = lon1 * M_PI / 180.0;
  double lat2_rad = lat2 * M_PI / 180.0;
  double lon2_rad = lon2 * M_PI / 180.0;

  // Calculate the difference in longitude.
  double delta_lon = lon2_rad - lon1_rad;

  // Calculate the bearing in radians.
  double y = sin(delta_lon) * cos(lat2_rad);
  double x = cos(lat1_rad) * sin(lat2_rad) -
             sin(lat1_rad) * cos(lat2_rad) * cos(delta_lon);
  double bearing_rad = atan2(y, x);

  // Convert from radians to degrees.
  double bearing_deg = bearing_rad * 180.0 / M_PI;

  // Normalize the bearing to ensure it is within the range [0, 360).
  bearing_deg = fmod((bearing_deg + 360), 360);

  return bearing_deg;
}
#endif

using CoordinatePair = struct {
  double lat;
  double lon;
};

std::string EncodeCoordinate(int value) {
  std::string encoded = "";
  int chunk;
  while (value >= 0x20) {
    chunk = (value & 0x1F) | 0x20;
    encoded += static_cast<char>(chunk + 63);
    value >>= 5;
  }
  encoded += static_cast<char>(value + 63);
  return encoded;
}

std::string EncodePolyline(const std::vector<CoordinatePair>& coordinates,
                           int precision = 5) {
  std::string encoded = "";
  int prev_lat = 0;
  int prev_lon = 0;

  const double factor = std::pow(10.0, precision);

  for (const auto& coord : coordinates) {
    int lat = static_cast<int>(std::round(coord.lat * factor));
    int lon = static_cast<int>(std::round(coord.lon * factor));

    int delta_lat = lat - prev_lat;
    int delta_lon = lon - prev_lon;

    prev_lat = lat;
    prev_lon = lon;

    encoded += EncodeCoordinate((delta_lat << 1) ^ (delta_lat >> 31));
    encoded += EncodeCoordinate((delta_lon << 1) ^ (delta_lon >> 31));
  }

  return encoded;
}

void decode_polyline(const std::string& encoded) {
  size_t i = 0;  // what byte are we looking at

  constexpr double kPolylinePrecision = 1E5;
  constexpr double kInvPolylinePrecision = 1.0 / kPolylinePrecision;

  auto deserialize = [&encoded, &i](const int previous) {
    int byte, shift = 0, result = 0;
    do {
      byte = static_cast<int>(encoded[i++]) - 63;
      result |= (byte & 0x1f) << shift;
      shift += 5;
    } while (byte >= 0x20);
    return previous + (result & 1 ? ~(result >> 1) : (result >> 1));
  };

  std::vector<double> lonv, latv;
  int last_lon = 0, last_lat = 0;
  LOG_S(INFO) << "Decode polyline <" << encoded << "> to the following:";
  while (i < encoded.length()) {
    int lat = deserialize(last_lat);
    int lon = deserialize(last_lon);

    LOG_S(INFO) << "  Coords "
                << static_cast<float>(static_cast<double>(lat) *
                                      kInvPolylinePrecision)
                << ", "
                << static_cast<float>(static_cast<double>(lon) *
                                      kInvPolylinePrecision);

    latv.emplace_back(
        static_cast<float>(static_cast<double>(lat) * kInvPolylinePrecision));
    lonv.emplace_back(
        static_cast<float>(static_cast<double>(lon) * kInvPolylinePrecision));

    last_lon = lon;
    last_lat = lat;
  }
}

// HTTP
httplib::Server svr;

struct JsonResult {
  nlohmann::json j;
  double sum_dist = 0.0;
  double sum_duration = 0.0;
  std::string last_name;
};

float GetLon(const MMCluster& mc, uint32_t n_idx) {
  const MMLatLon& c = mc.node_to_latlon.at(n_idx);
  return c.lon / static_cast<float>(TEN_POW_7);
}

float GetLat(const MMCluster& mc, uint32_t n_idx) {
  const MMLatLon& c = mc.node_to_latlon.at(n_idx);
  return c.lat / static_cast<float>(TEN_POW_7);
}

std::string GetEdgeName(const MMCluster& mc, const MMFullEdge& fe) {
  uint32_t way_idx = fe.way_idx(mc);
  std::string_view streetname = mc.get_streetname(way_idx);
  int64_t way_id = mc.grouped_way_to_osm_id.at(way_idx);
  int64_t n0_id = mc.grouped_node_to_osm_id.at(fe.from_node_idx);
  int64_t n1_id = mc.grouped_node_to_osm_id.at(fe.edge(mc).target_idx());
  return absl::StrFormat("<%s> (w:%lld %lld->%lld)", streetname, way_id, n0_id,
                         n1_id);
}

double Round1(double val) { return std::round(val * 10.0) / 10.0; }

#if 0
JsonResult CreateOneStep(const MMCluster& mc, uint32_t start_node_idx,
                         const MMClusterRouter& router,
                         const MMRoutingResult& res, uint32_t pos,
                         bool arrival = false) {
  uint32_t v_idx = res.route_v_idx.at(pos);
  const MMClusterRouter::VisitedEdge& ve = router.GetVEdge(v_idx);
  const MMClusterRouter::VisitedEdge* prev_edge = nullptr;
  uint32_t from_node_idx = INFU32;

  if (ve.from_v_idx != INFU32) {
    prev_edge = &router.GetVEdge(ve.from_v_idx);
    from_node_idx = router.GetGraphEdge(mc, ve.from_v_idx).target_idx();
  } else {
    prev_edge = nullptr;
    from_node_idx = start_node_idx;
  }
  const MMFullEdge fe =
      MMFullEdge::Create(mc, from_node_idx, router.GetGraphEdgeIdx(mc, v_idx));

  std::vector<CoordinatePair> coords;
  coords.push_back(
      {.lat = GetLat(mc, from_node_idx), .lon = GetLon(mc, from_node_idx)});

  double dist = 0;
  double duration = 0;
  if (!arrival) {
    coords.push_back({.lat = GetLat(mc, fe.target_idx(mc)),
                      .lon = GetLon(mc, fe.target_idx(mc))});
    // convert to seconds.
    duration = (prev_edge != nullptr ? ve.min_weight - prev_edge->min_weight
                                     : ve.min_weight) /
               1000.0;
    dist = mc.edge_to_distance.at(router.GetGraphEdgeIdx(mc, v_idx)) / 100.0;
  }

  nlohmann::json maneuver = {
      {"bearing_after", 0},
      {"bearing_before", 0},
      {"location", {GetLon(mc, from_node_idx), GetLat(mc, from_node_idx)}},
      {"modifier", "ModifierContinue"},
      {"type", (arrival    ? "arrive"
                : pos == 0 ? "depart"
                           : "new name")}};  // This shows as "Continue".

  nlohmann::json step = {{"geometry", EncodePolyline(coords)},
                         {"maneuver", maneuver},
                         {"name", GetEdgeName(mc, fe)},
                         {"duration", Round1(duration)},
                         {"distance", Round1(dist)}};
  return {.j = step,
          .sum_dist = dist,
          .sum_duration = duration,
          .last_name = GetEdgeName(mc, fe)};
}
#endif

class StepsData {
 public:
  StepsData(const MMCluster& mc, const MMEdgePoint& start,
            const MMEdgePoint& target, const MMClusterRouter& router,
            const std::vector<uint32_t>& route_v_idx)
      : start_(start), target_(target) {
    CHECK_S(!route_v_idx.empty());
    uint32_t prev_weight = 0;
    for (uint32_t pos = 0; pos < route_v_idx.size(); ++pos) {
      const uint32_t v_idx = route_v_idx.at(pos);
      if (pos == 0) {
        fes_.push_back(start_.fe);
        CHECK_EQ_S(start_.fe.edge_idx(mc), router.GetGraphEdgeIdx(mc, v_idx));
      } else {
        uint32_t from_node_idx = fes_.back().target_idx(mc);
        fes_.push_back(MMFullEdge::Create(mc, from_node_idx,
                                          router.GetGraphEdgeIdx(mc, v_idx)));
      }
      if (pos == route_v_idx.size() - 1) {
        CHECK_EQ_S(target_.fe.edge_idx(mc), fes_.back().edge_idx(mc));
      }
      const MMClusterRouter::VisitedEdge& ve = router.GetVEdge(v_idx);
      weights_.push_back(ve.min_weight - prev_weight);
      prev_weight = ve.min_weight;
    }
    CHECK_EQ_S(route_v_idx.size(), fes_.size());
    CHECK_EQ_S(weights_.size(), fes_.size());
  }

  size_t num_steps() const { return fes_.size(); }

  JsonResult CreateOneStep(const MMCluster& mc, uint32_t pos) const {
    MMFullEdge fe = fes_.at(pos);

    float from_lon = GetLon(mc, fe.from_node_idx);
    float from_lat = GetLat(mc, fe.from_node_idx);
    float to_lon = GetLon(mc, fe.target_idx(mc));
    float to_lat = GetLat(mc, fe.target_idx(mc));
    if (pos == 0) {
      from_lon = start_.lon_at_fraction / TEN_POW_7_DBL;
      from_lat = start_.lat_at_fraction / TEN_POW_7_DBL;
    }
    if (pos + 1 == num_steps()) {
      to_lon = target_.lon_at_fraction / TEN_POW_7_DBL;
      to_lat = target_.lat_at_fraction / TEN_POW_7_DBL;
    }

    std::vector<CoordinatePair> coords;
    coords.push_back({.lat = from_lat, .lon = from_lon});
    coords.push_back({.lat = to_lat, .lon = to_lon});
    // convert to seconds.
    const double duration = weights_.at(pos) / 1000.0;
    const double dist = mc.edge_to_distance.at(fe.edge_idx(mc)) / 100.0;

    nlohmann::json maneuver = {{"bearing_after", 0},
                               {"bearing_before", 0},
                               {"location", {from_lon, from_lat}},
                               {"modifier", "ModifierContinue"},
                               {"type", (pos == 0 ? "depart" : "continue")}};

    nlohmann::json step = {{"geometry", EncodePolyline(coords)},
                           {"maneuver", maneuver},
                           {"name", GetEdgeName(mc, fe)},
                           {"duration", Round1(duration)},
                           {"distance", Round1(dist)}};
    return {.j = step,
            .sum_dist = dist,
            .sum_duration = duration,
            .last_name = GetEdgeName(mc, fe)};
  }

  JsonResult CreateArrivalStep(const MMCluster& mc) const {
    MMFullEdge fe = fes_.back();

    float to_lon = target_.lon_at_fraction / TEN_POW_7_DBL;
    float to_lat = target_.lat_at_fraction / TEN_POW_7_DBL;

    std::vector<CoordinatePair> coords;
    coords.push_back({.lat = to_lat, .lon = to_lon});

    nlohmann::json maneuver = {{"bearing_after", 0},
                               {"bearing_before", 0},
                               {"location", {to_lon, to_lat}},
                               {"type", "arrive"}};

    nlohmann::json step = {{"geometry", EncodePolyline(coords)},
                           {"maneuver", maneuver},
                           {"name", GetEdgeName(mc, fe)},
                           {"duration", 0},
                           {"distance", 0}};
    return {.j = step};
  }

 private:
  const MMEdgePoint start_;
  const MMEdgePoint target_;
  std::vector<MMFullEdge> fes_;
  std::vector<uint32_t> weights_;
};

JsonResult CreateSteps(const MMCluster& mc, const StepsData& steps_data) {
  JsonResult result;
  result.j = nlohmann::json::array();

  for (uint32_t pos = 0; pos < steps_data.num_steps(); ++pos) {
    JsonResult tmp = steps_data.CreateOneStep(mc, pos);
    result.sum_dist += tmp.sum_dist;
    result.sum_duration += tmp.sum_duration;
    // TODO: Collapse if name is same.
    result.j.push_back(tmp.j);
  }
  result.j.push_back(steps_data.CreateArrivalStep(mc).j);
  return result;
}

#if 0
JsonResult CreateSteps(const MMCluster& mc, uint32_t start_node_idx,
                       const MMClusterRouter& router,
                       const MMRoutingResult& res) {
  JsonResult result;
  result.j = nlohmann::json::array();

  for (uint32_t pos = 0; pos < res.route_v_idx.size(); ++pos) {
    JsonResult tmp = CreateOneStep(mc, start_node_idx, router, res, pos);
    result.sum_dist += tmp.sum_dist;
    result.sum_duration += tmp.sum_duration;
    // TODO: Collapse if name is same.
    result.j.push_back(tmp.j);
  }
  // Add final 'arrival' step.
  result.j.push_back(CreateOneStep(mc, start_node_idx, router, res,
                                   res.route_v_idx.size() - 1,
                                   /*arrival=*/true)
                         .j);
  return result;
}
#endif

nlohmann::json RouteToJson(const MMCluster& mc, const MMEdgePoint& start,
                           const MMEdgePoint& target,
                           const MMClusterRouter& router,
                           const MMRoutingResult& res) {
  if (res.route_v_idx.empty()) {
    return {{"code", "NoRoute"}};
  }

  nlohmann::json waypoints = nlohmann::json::array();
  {
    waypoints.push_back(
        {{"distance", std::roundf(start.distance_cm / 10.0) / 10.0},
         {"name", GetEdgeName(mc, start.fe)},
         {"location",
          {start.lon_at_fraction / TEN_POW_7_DBL,
           start.lat_at_fraction / TEN_POW_7_DBL}}});
  }
  {
    waypoints.push_back(
        {{"distance", std::roundf(target.distance_cm / 10.0) / 10.0},
         {"name", GetEdgeName(mc, target.fe)},
         {"location",
          {target.lon_at_fraction / TEN_POW_7_DBL,
           target.lat_at_fraction / TEN_POW_7_DBL}}});
  }

  // We currently support only one leg, so the only thing to fill are the steps.
  StepsData steps_data(mc, start, target, router, res.route_v_idx);

  JsonResult res_steps = CreateSteps(mc, steps_data);
  nlohmann::json leg = {{"steps", res_steps.j},
                        {"summary", "step_summary"},
                        {"duration", Round1(res_steps.sum_duration)},
                        {"distance", Round1(res_steps.sum_dist)}};
  nlohmann::json route = {{"legs", {leg}},
                          {"duration", Round1(res_steps.sum_duration)},
                          {"distance", Round1(res_steps.sum_dist)}};
  nlohmann::json routes = {route};

  return {{"code", "Ok"}, {"waypoints", waypoints}, {"routes", routes}};
}

nlohmann::json ComputeRoute(const MMGraph& mmg, bool hybrid, double lon1,
                            double lat1, double lon2, double lat2) {
  FUNC_TIMER();

  absl::Time start_time = absl::Now();
  const MMGeoAnchor start =
      FindClosestEdges(mmg, std::llround(lat1 * TEN_POW_7_DBL),
                       std::llround(lon1 * TEN_POW_7_DBL));
  const MMGeoAnchor target =
      FindClosestEdges(mmg, std::llround(lat2 * TEN_POW_7_DBL),
                       std::llround(lon2 * TEN_POW_7_DBL));
  LOG_S(INFO) << absl::StrFormat("**** Find closest edges: %.2f secs",
                                 ToDoubleSeconds(absl::Now() - start_time));

  for (const auto& e : start.edges) {
    LOG_S(INFO) << e.DebugString(mmg, std::llround(lat1 * TEN_POW_7_DBL),
                                 std::llround(lon1 * TEN_POW_7_DBL));
  }
  for (const auto& e : target.edges) {
    LOG_S(INFO) << e.DebugString(mmg, std::llround(lat2 * TEN_POW_7_DBL),
                                 std::llround(lon2 * TEN_POW_7_DBL));
  }

  if (!start.valid() || !target.valid() ||
      start.edges.front().fe.cluster_id != target.edges.front().fe.cluster_id) {
    return {{"code", "NoRoute"}};
  }

  CHECK_EQ_S(start.edges.front().fe.cluster_id,
             target.edges.front().fe.cluster_id);
  start_time = absl::Now();
  MMClusterWrapper mcw = {.mc = start.edges.front().fe.mc(mmg)};
  mcw.FillEdgeWeights(VH_MOTORCAR, RoutingMetricTime());
  LOG_S(INFO) << absl::StrFormat("**** Create cluster wrapper: %.2f secs",
                                 ToDoubleSeconds(absl::Now() - start_time));

  start_time = absl::Now();
  MMClusterRouter router;
  router.Route(mcw, start,
               {.handle_restricted_access = true, .include_dead_end = true},
               target);
  LOG_S(INFO) << "Finished routing";
  LOG_S(INFO) << absl::StrFormat("**** Route(): %.2f secs",
                                 ToDoubleSeconds(absl::Now() - start_time));

  start_time = absl::Now();
  const auto& vis = router.GetVisitedEdges();

  MMRoutingResult res;
  /*
  res.route_v_idx = router.GetShortestPathToTarget(
      mcw.mc, target.fe.edge_idx(mcw.mc), target.dts.fraction_closest);
  */
  res.route_v_idx = router.GetShortestPathFromTarget(mcw.mc, target);
  res.found = !res.route_v_idx.empty();
  res.found_distance = res.route_v_idx.empty()
                           ? INFU32
                           : vis.at(res.route_v_idx.back()).min_weight;
  res.num_shortest_route_nodes =
      res.route_v_idx.empty() ? 0 : res.route_v_idx.size() + 1;
  res.num_visited = vis.size();

  LOG_S(INFO) << "Result metric:" << res.found_distance;

  if (res.route_v_idx.empty()) {
    return {{"code", "NoRoute"}};
  }

  // const double elapsed = ToDoubleSeconds(absl::Now() - start_time);

  int start_edge_pos = -1;
  int target_edge_pos = -1;
  CHECK_S(!res.route_v_idx.empty());
  start_edge_pos = start.FindPos(
      mcw.mc, router.GetGraphEdgeIdx(mcw.mc, res.route_v_idx.front()));
  target_edge_pos = target.FindPos(
      mcw.mc, router.GetGraphEdgeIdx(mcw.mc, res.route_v_idx.back()));

  CHECK_GE_S(start_edge_pos, 0);
  CHECK_GE_S(target_edge_pos, 0);

  nlohmann::json jres =
      RouteToJson(mcw.mc, start.edges.at(start_edge_pos),
                  target.edges.at(target_edge_pos), router, res);
  LOG_S(INFO) << absl::StrFormat("**** create json result(): %.2f secs",
                                 ToDoubleSeconds(absl::Now() - start_time));
  return jres;

  // return {{"code", "NoRoute"}};

#if 0
  auto res = router.Route(start_idx, target_idx, metric, opt);
  const double elapsed = ToDoubleSeconds(absl::Now() - start);

  uint32_t num_len_gt_1 = 0;
  uint32_t max_len = 0;
  router.TurnRestrictionSpecialStats(&num_len_gt_1, &max_len);

  LOG_S(INFO) << absl::StrFormat(
      "Metr:%u %s secs:%6.3f #n:%5u vis:%9u cTR:%4u(%3.0f%% max:%u lgt1:%u) "
      "%s",
      res.found_distance, res.found ? "SUC" : "ERR", elapsed,
      res.num_shortest_route_nodes, res.num_visited,
      res.num_complex_turn_restriction_keys,
      res.complex_turn_restriction_keys_reduction_factor * 100.0, max_len,
      num_len_gt_1, router.Name(metric, opt));

  return RouteToJson(g, n1.dist, n2.dist, router, res);
#endif
}

}  // namespace

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);

  const Argli argli(argc, argv,
                    {
                        {.name = "inputfile",
                         .type = "string",
                         .positional = true,
                         .required = true,
                         .desc = "Input <graph>.ser or OSM <name>.pbf file "
                                 "(such as planet file)."},
                        {.name = "n_threads",
                         .type = "int",
                         .dflt = "12",
                         .desc = "Number of threads to use"},
                    });

  const std::string filename = argli.GetString("inputfile");
  // const int n_threads = argli.GetInt("n_threads");

  int fd = ::open(filename.c_str(), O_RDONLY | O_CLOEXEC, 0644);
  if (fd < 0) FileAbortOnError("open");

  const uint64_t file_size = GetFileSize(fd);
  void* ptr = mmap(NULL, file_size, PROT_READ, MAP_SHARED, fd, 0);
  if (ptr == MAP_FAILED) {
    perror("mmap");
    ::close(fd);
    ABORT_S();
  }
  ::close(fd);
  const MMGraph& mmg = *((MMGraph*)ptr);

  svr.Get("/hi", [&](const httplib::Request&, httplib::Response& res) {
    res.set_content("Hello World!", "text/plain");
  });

  svr.Get(
      "/route/(v1[hybrid]*)/driving/([0-9.]+),([0-9.]+);([0-9.]+),([0-9.]+)",
      [&mmg](const httplib::Request& req, httplib::Response& res) {
        const absl::Time start = absl::Now();

        nlohmann::json result;
        std::string_view comp = req.matches.str(1);
        if (comp != "v1" && comp != "v1hybrid") {
          result = {{"code", "InvalidUrl"}};
        } else {
          const bool hybrid = req.matches.str(1) != "v1";
          LOG_S(INFO) << "Serving routing request";
          LOG_S(INFO) << "Arg1:" << req.matches.str(2);
          LOG_S(INFO) << "Arg2:" << req.matches.str(3);
          LOG_S(INFO) << "Arg3:" << req.matches.str(4);
          LOG_S(INFO) << "Arg4:" << req.matches.str(5);
          double lon1, lat1, lon2, lat2;
          if (!absl::SimpleAtod(req.matches.str(2), &lon1) ||
              !absl::SimpleAtod(req.matches.str(3), &lat1) ||
              !absl::SimpleAtod(req.matches.str(4), &lon2) ||
              !absl::SimpleAtod(req.matches.str(5), &lat2)) {
            result = {{"code", "InvalidQuery"}};
          } else {
            result = ComputeRoute(mmg, hybrid, lon1, lat1, lon2, lat2);
          }
        }
        res.set_header("Access-Control-Allow-Origin", "*");
        res.set_content(result.dump(), "application/json");
        LOG_S(INFO) << result.dump(2);
        LOG_S(INFO) << absl::StrFormat("**** elapsed: %.2f secs",
                                       ToDoubleSeconds(absl::Now() - start));
      });

  decode_polyline("ar~_Hwcft@Ny@");
  decode_polyline("qq~_Hqeft@");
  LOG_S(INFO) << ComputeRoute(mmg, /*hybrid=*/false, 8.720121, 47.3476881,
                              8.7204095, 47.3476057)
                     .dump(2);

  LOG_S(INFO) << "Listening...";
  svr.listen("0.0.0.0", 8081);
}
