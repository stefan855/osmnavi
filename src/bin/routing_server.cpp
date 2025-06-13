// #define CPPHTTPLIB_OPENSSL_SUPPORT
#include <math.h>

#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>
#include <string>
#include <string_view>

#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "algos/edge_router.h"
#include "base/argli.h"
#include "base/thread_pool.h"
#include "base/util.h"
#include "cpp-httplib/httplib.h"
#include "gd.h"
#include "geometry/closest_node.h"
#include "graph/build_graph.h"

namespace {

#include <cmath>
#include <iostream>

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

float GetLon(const GNode& n) { return n.lon / static_cast<float>(TEN_POW_7); }

float GetLat(const GNode& n) { return n.lat / static_cast<float>(TEN_POW_7); }

std::string GetEdgeName(const Graph& g, const CTRDeDuper& dd,
                        const EdgeRouter::VisitedEdge& ve) {
  if (ve.key.GetType() == GEdgeKey::CLUSTER) {
    return "cluster edge";
  }
  const GWay& way = g.ways.at(ve.key.GetEdge(g, dd).way_idx);
  return absl::StrFormat("%s (w:%lld %lld->%lld)",
                         way.streetname == nullptr ? "<null>" : way.streetname,
                         way.id, ve.key.FromNode(g, dd).node_id,
                         ve.key.ToNode(g, dd).node_id);
}

double Round1(double val) { return std::round(val * 10.0) / 10.0; }

JsonResult CreateOneStep(const Graph& g, const EdgeRouter& router,
                         const RoutingResult& res, uint32_t pos,
                         bool arrival = false) {
  const CTRDeDuper& dd = router.GetCTRDeDuper();
  uint32_t v_idx = res.route_v_idx.at(pos);
  const EdgeRouter::VisitedEdge& ve = router.GetVEdge(v_idx);
  const EdgeRouter::VisitedEdge* prev_edge = nullptr;
  if (ve.prev_v_idx != INFU32) {
    prev_edge = &router.GetVEdge(ve.prev_v_idx);
  }
  const GNode& to_node = ve.key.ToNode(g, dd);
  const GNode& from_node = arrival ? to_node : ve.key.FromNode(g, dd);
  std::vector<CoordinatePair> coords;
  coords.push_back({.lat = GetLat(from_node), .lon = GetLon(from_node)});
  double dist = 0;
  double duration = 0;
  if (!arrival) {
    coords.push_back({.lat = GetLat(to_node), .lon = GetLon(to_node)});
    // convert to seconds.
    duration = (prev_edge != nullptr ? ve.min_metric - prev_edge->min_metric
                                     : ve.min_metric) /
               1000.0;
    if (ve.key.GetType() == GEdgeKey::CLUSTER) {
      dist = 11111;
    } else {
      dist = ve.key.GetEdge(g, dd).distance_cm / 100.0;
    }
  }

  nlohmann::json maneuver = {
      {"bearing_after", 0},
      {"bearing_before", 0},
      {"location", {GetLon(from_node), GetLat(from_node)}},
      {"modifier", "ModifierContinue"},
      {"type", (arrival    ? "arrive"
                : pos == 0 ? "depart"
                           : "new name")}};  // This shows as "Continue".

  nlohmann::json step = {{"geometry", EncodePolyline(coords)},
                         {"maneuver", maneuver},
                         {"name", GetEdgeName(g, dd, ve)},
                         {"duration", Round1(duration)},
                         {"distance", Round1(dist)}};
  return {.j = step,
          .sum_dist = dist,
          .sum_duration = duration,
          .last_name = GetEdgeName(g, dd, ve)};
}

JsonResult CreateSteps(const Graph& g, const EdgeRouter& router,
                       const RoutingResult& res) {
  JsonResult result;
  result.j = nlohmann::json::array();

  for (uint32_t pos = 0; pos < res.route_v_idx.size(); ++pos) {
    JsonResult tmp = CreateOneStep(g, router, res, pos);
    result.sum_dist += tmp.sum_dist;
    result.sum_duration += tmp.sum_duration;
    // TODO: Collapse if name is same.
    result.j.push_back(tmp.j);
  }
  // Add final 'arrival' step.
  result.j.push_back(CreateOneStep(g, router, res, res.route_v_idx.size() - 1,
                                   /*arrival=*/true)
                         .j);
  return result;
}

nlohmann::json RouteToJson(const Graph& g, int64_t dist_p1, int64_t dist_p2,
                           const EdgeRouter& router, const RoutingResult& res) {
  if (res.route_v_idx.empty()) {
    return {{"code", "NoRoute"}};
  }
  const CTRDeDuper& dd = router.GetCTRDeDuper();

  nlohmann::json waypoints = nlohmann::json::array();
  {
    const EdgeRouter::VisitedEdge& ve =
        router.GetVEdge(res.route_v_idx.front());
    const GNode& n = ve.key.FromNode(g, dd);
    waypoints.push_back({{"distance", std::roundf(dist_p1 / 10.0) / 10.0},
                         {"name", GetEdgeName(g, dd, ve)},
                         {"location", {GetLon(n), GetLat(n)}}});
  }
  {
    const EdgeRouter::VisitedEdge& ve = router.GetVEdge(res.route_v_idx.back());
    const GNode& n = ve.key.ToNode(g, dd);
    waypoints.push_back({{"distance", std::roundf(dist_p2 / 10.0) / 10.0},
                         {"name", GetEdgeName(g, dd, ve)},
                         {"location", {GetLon(n), GetLat(n)}}});
  }

  // We currently support only one leg, so the only thing to fill are the steps.
  JsonResult res_steps = CreateSteps(g, router, res);
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

nlohmann::json ComputeRoute(const Graph& g,
                            const std::vector<uint32_t>& sorted_node_indexes,
                            bool hybrid, double lon1, double lat1, double lon2,
                            double lat2) {
  FUNC_TIMER();
  const ClosestNodeResult n1 = FindClosestNodeFast(
      g, sorted_node_indexes, std::llround(lat1 * TEN_POW_7),
      std::llround(lon1 * TEN_POW_7));
  const ClosestNodeResult n2 = FindClosestNodeFast(
      g, sorted_node_indexes, std::llround(lat2 * TEN_POW_7),
      std::llround(lon2 * TEN_POW_7));

  if (n1.node_pos == INFU32 || n2.node_pos == INFU32) {
    LOG_S(INFO) << "Can't find endpoints";
    return {{"code", "NoRoute"}};
  }
  uint32_t start_idx = n1.node_pos;
  uint32_t target_idx = n2.node_pos;
  {
    const GNode gn1 = g.nodes.at(start_idx);
    const GNode gn2 = g.nodes.at(target_idx);
    LOG_S(INFO) << absl::StrFormat(
        "Routing from node %lld (%.4f, %.4f) to %lld (%.4f, %.4f)", gn1.node_id,
        GetLat(gn1), GetLon(gn1), gn2.node_id, GetLat(gn2), GetLon(gn2));
  }

  RoutingMetricTime metric;
  RoutingOptions opt;
  opt.use_astar_heuristic = true;
  if (hybrid) {
    opt.SetHybridOptions(g, start_idx, target_idx);
  } else {
    opt.MayFillBridgeNodeId(g, target_idx);
  }

  const absl::Time start = absl::Now();
  EdgeRouter router(g, 0);
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
}
}  // namespace

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);

  const Argli argli(argc, argv,
                    {
                        {.name = "pbf",
                         .type = "string",
                         .positional = true,
                         .required = true,
                         .desc = "Input OSM pbf file (such as planet file)."},
                        {.name = "n_threads",
                         .type = "int",
                         .dflt = "10",
                         .desc = "Number of threads to use"},

                    });

  const std::string pbf = argli.GetString("pbf");
  const int n_threads = argli.GetInt("n_threads");

  // Read Road Network.
  build_graph::BuildGraphOptions opt = {.pbf = pbf, .n_threads = n_threads};
  build_graph::GraphMetaData meta = build_graph::BuildGraph(opt);
  const Graph& g = meta.graph;
  std::vector<uint32_t> sorted_node_indexes = SortNodeIndexesByLon(g);
  // Remove all nodes that aren't in a large component.
  sorted_node_indexes.erase(
      std::remove_if(begin(sorted_node_indexes), end(sorted_node_indexes),
                     [&g](uint32_t node_idx) {
                       return g.nodes.at(node_idx).large_component == 0;
                     }),
      end(sorted_node_indexes));

  std::string root("/tmp");
  std::string admin_root("../../data");

  if (argc == 2) {
    root = argv[1];
    admin_root = argv[1];
  }

  svr.Get("/hi", [&](const httplib::Request&, httplib::Response& res) {
    res.set_content("Hello World!", "text/plain");
  });

#if 0
  // Match the request path against a regular expression
  // and extract its captures
  svr.Get(R"(/tiles/([^/]+)/([^/]+)/([^/]+)/([^/]+)\.png)",
          [&layers](const httplib::Request& req, httplib::Response& res) {
            res.set_content(CreatePNG(layers[req.matches.str(1)],
                                      atoi(req.matches.str(2).c_str()),
                                      atoi(req.matches.str(3).c_str()),
                                      atoi(req.matches.str(4).c_str())),
                            "image/png");
          });
#endif

  svr.Get(
      "/route/(v1[hybrid]*)/driving/([0-9.]+),([0-9.]+);([0-9.]+),([0-9.]+)",
      [&g, &sorted_node_indexes](const httplib::Request& req,
                                 httplib::Response& res) {
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
            result = ComputeRoute(g, sorted_node_indexes, hybrid, lon1, lat1,
                                  lon2, lat2);
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
  LOG_S(INFO) << ComputeRoute(g, sorted_node_indexes, /*hybrid=*/false,
                              8.720121, 47.3476881, 8.7204095, 47.3476057)
                     .dump(2);

  LOG_S(INFO) << CalculateAngle(0, 0, 1, 0);
  LOG_S(INFO) << CalculateAngle(0, 0, 1, 1);
  LOG_S(INFO) << CalculateAngle(0, 0, 0, 1);
  LOG_S(INFO) << CalculateAngle(0, 0, -1, 1);
  LOG_S(INFO) << CalculateAngle(0, 0, -1, 0);
  LOG_S(INFO) << CalculateAngle(0, 0, -1, -1);
  LOG_S(INFO) << CalculateAngle(0, 0, 0, -1);
  LOG_S(INFO) << CalculateAngle(0, 0, 1, -1);

  LOG_S(INFO) << "Listening...";
  svr.listen("0.0.0.0", 8081);
}
