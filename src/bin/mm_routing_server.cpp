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
#include "algos/mm_hybrid_router.h"
#include "base/argli.h"
#include "base/thread_pool.h"
#include "base/util.h"
#include "cpp-httplib/httplib.h"
#include "geometry/closest_edge.h"
#include "geometry/tiles.h"
#include "graph/mmgraph_def.h"

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

namespace {

// This is a global pointer to the last router data that was produced.
static std::shared_ptr<MMHybridRouter::RouterData> g_last_router_data;

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

struct JsonResult {
  nlohmann::json j;
  double sum_dist = 0.0;
  double sum_duration = 0.0;
  std::string last_name;
};

float GetLon(const MMCluster& mc, uint32_t n_idx) {
  return mc.node_to_lon(n_idx) / static_cast<float>(TEN_POW_7);
}

float GetLat(const MMCluster& mc, uint32_t n_idx) {
  return mc.node_to_lat(n_idx) / static_cast<float>(TEN_POW_7);
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

class StepsData {
 public:
  StepsData(const MMGraph& mg, const MMRoutingResult& res) : res_(res) {}

  size_t num_steps() const { return res_.full_edges.size(); }

  JsonResult CreateOneStep(const MMGraph& mg, uint32_t pos) const {
    const MMFullEdge& fe = res_.full_edges.at(pos);
    const MMCluster& mc = fe.mc(mg);

    float from_lon = GetLon(mc, fe.from_node_idx);
    float from_lat = GetLat(mc, fe.from_node_idx);
    float to_lon = GetLon(mc, fe.target_idx(mc));
    float to_lat = GetLat(mc, fe.target_idx(mc));
    if (pos == 0) {
      from_lon = res_.start.lon_at_fraction / TEN_POW_7_DBL;
      from_lat = res_.start.lat_at_fraction / TEN_POW_7_DBL;
    }
    if (pos + 1 == num_steps()) {
      to_lon = res_.target.lon_at_fraction / TEN_POW_7_DBL;
      to_lat = res_.target.lat_at_fraction / TEN_POW_7_DBL;
    }

    std::vector<CoordinatePair> coords;
    coords.push_back({.lat = from_lat, .lon = from_lon});
    coords.push_back({.lat = to_lat, .lon = to_lon});
    // convert to seconds.
    const double duration = res_.edge_metric(pos) / 1000.0;
    const double dist = res_.distance_cm(mg, pos) / 100.0;

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

  JsonResult CreateArrivalStep(const MMGraph& mg) const {
    MMFullEdge fe = res_.full_edges.back();

    float to_lon = res_.target.lon_at_fraction / TEN_POW_7_DBL;
    float to_lat = res_.target.lat_at_fraction / TEN_POW_7_DBL;

    std::vector<CoordinatePair> coords;
    coords.push_back({.lat = to_lat, .lon = to_lon});

    nlohmann::json maneuver = {{"bearing_after", 0},
                               {"bearing_before", 0},
                               {"location", {to_lon, to_lat}},
                               {"type", "arrive"}};

    nlohmann::json step = {{"geometry", EncodePolyline(coords)},
                           {"maneuver", maneuver},
                           {"name", GetEdgeName(fe.mc(mg), fe)},
                           {"duration", 0},
                           {"distance", 0}};
    return {.j = step};
  }

 private:
  const MMRoutingResult& res_;
};

JsonResult CreateSteps(const MMGraph& mg, const StepsData& steps_data) {
  JsonResult result;
  result.j = nlohmann::json::array();

  for (uint32_t pos = 0; pos < steps_data.num_steps(); ++pos) {
    JsonResult tmp = steps_data.CreateOneStep(mg, pos);
    result.sum_dist += tmp.sum_dist;
    result.sum_duration += tmp.sum_duration;
    // TODO: Collapse if name is same.
    result.j.push_back(tmp.j);
  }
  result.j.push_back(steps_data.CreateArrivalStep(mg).j);
  return result;
}

nlohmann::json RouteToJson(const MMGraph& mg, const MMRoutingResult& res) {
  CHECK_S(!res.full_edges.empty());

  nlohmann::json waypoints = nlohmann::json::array();
  {
    waypoints.push_back(
        {{"distance", std::roundf(res.start.distance_cm / 10.0) / 10.0},
         {"name", GetEdgeName(res.start.fe.mc(mg), res.start.fe)},
         {"location",
          {res.start.lon_at_fraction / TEN_POW_7_DBL,
           res.start.lat_at_fraction / TEN_POW_7_DBL}}});
  }
  {
    waypoints.push_back(
        {{"distance", std::roundf(res.target.distance_cm / 10.0) / 10.0},
         {"name", GetEdgeName(res.target.fe.mc(mg), res.target.fe)},
         {"location",
          {res.target.lon_at_fraction / TEN_POW_7_DBL,
           res.target.lat_at_fraction / TEN_POW_7_DBL}}});
  }

  // We currently support only one leg, so the only thing to fill are the steps.
  StepsData steps_data(mg, res);

  JsonResult res_steps = CreateSteps(mg, steps_data);
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

nlohmann::json ComputeRoute(const MMGraph& mg, bool hybrid, double lon1,
                            double lat1, double lon2, double lat2) {
  FUNC_TIMER();

  LOG_S(INFO) << "Search " << lon1 << " " << lat1;
  GeoAnchor start;
  GeoAnchor target;
  double find_closest_time;
  {
    absl::Time start_time = absl::Now();
    start = FindClosestEdges(mg, std::llround(lat1 * TEN_POW_7_DBL),
                             std::llround(lon1 * TEN_POW_7_DBL));
    target = FindClosestEdges(mg, std::llround(lat2 * TEN_POW_7_DBL),
                              std::llround(lon2 * TEN_POW_7_DBL));
    find_closest_time = ToDoubleSeconds(absl::Now() - start_time);
  }
  LOG_S(INFO) << absl::StrFormat("**** Find closest edges: %.2f secs",
                                 find_closest_time);

  for (const auto& e : start.edge_points()) {
    LOG_S(INFO) << e.DebugString(mg, std::llround(lat1 * TEN_POW_7_DBL),
                                 std::llround(lon1 * TEN_POW_7_DBL));
  }
  for (const auto& e : target.edge_points()) {
    LOG_S(INFO) << e.DebugString(mg, std::llround(lat2 * TEN_POW_7_DBL),
                                 std::llround(lon2 * TEN_POW_7_DBL));
  }
  LOG_S(INFO) << absl::StrFormat("Found: start:%d  target:%d",
                                 !start.edge_points().empty(),
                                 !target.edge_points().empty());

  if (start.edge_points().empty() || target.edge_points().empty()) {
    return {{"code", "NoRoute"}};
  }

  MMRoutingResult res;
  double routing_time;
  {
    absl::Time start_time = absl::Now();
    MMHybridRouter router;

    // Keep the router data of the last route computation in a global shared
    // pointer for examination. The shared pointer can be copied atomically
    // before examination.
    MMHybridRouter::RouterData* router_data = new MMHybridRouter::RouterData;
    res = router.Route(mg, start, target, router_data);
    g_last_router_data.reset(router_data);
    routing_time = ToDoubleSeconds(absl::Now() - start_time);
  }
  LOG_S(INFO) << "Finished routing";
  LOG_S(INFO) << absl::StrFormat("**** Start/End Cl: %9llu",
                                 res.num_path_full_clusters);
  LOG_S(INFO) << absl::StrFormat("**** Result edges: %9llu",
                                 res.full_edges.size());
  LOG_S(INFO) << absl::StrFormat("**** Final metric: %9u", res.final_metric);
  LOG_S(INFO) << absl::StrFormat("**** Start  vis:   %9llu", res.num_vis_start);
  LOG_S(INFO) << absl::StrFormat("**** Target vis:   %9llu",
                                 res.num_vis_target);
  LOG_S(INFO) << absl::StrFormat("**** Hybrid vis:   %9llu",
                                 res.num_vis_hybrid);

  if (res.full_edges.empty()) {
    return {{"code", "NoRoute"}};
  }

  CHECK_S(!res.full_edges.empty());
  nlohmann::json jres;
  double json_time;
  {
    absl::Time start_time = absl::Now();
    jres = RouteToJson(mg, res);
    json_time = ToDoubleSeconds(absl::Now() - start_time);
  }
  LOG_S(INFO) << absl::StrFormat("**** Find closest edges:       %.3f secs",
                                 find_closest_time);
  LOG_S(INFO) << absl::StrFormat("**** Route:                    %.3f secs",
                                 routing_time);
  LOG_S(INFO) << absl::StrFormat("       Routing algorithms:     %.3f secs",
                                 res.time_for_route_algorithm);
  LOG_S(INFO) << absl::StrFormat("       Path assemble:          %.3f secs",
                                 res.time_for_assemble);
  LOG_S(INFO) << absl::StrFormat("       Expand hybrid clusters: %.3f secs",
                                 res.time_for_expand_hybrid_clusters);
  LOG_S(INFO) << absl::StrFormat("**** Create Json:              %.3f secs",
                                 json_time);
  return jres;
}

void HandleFileRequest(const httplib::Request& req, httplib::Response& res,
                       const std::string& filename,
                       const std::string& content_type) {
  std::ifstream file(filename);
  if (!file.is_open()) {
    res.status = 404;
    res.set_content("File not found", "text/plain");
    return;
  }

  std::ostringstream content;
  content << file.rdbuf();
  file.close();
  res.set_content(content.str(), content_type);
}

}  // namespace

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);

  const Argli argli(
      argc, argv,
      {
          {.name = "inputfile",
           .type = "string",
           .positional = true,
           .required = true,
           .desc = "Input <graph>.ser or OSM <name>.pbf file "
                   "(such as planet file)."},
          {.name = "cert_dir",
           .type = "string",
           .dflt = "cert",
           .desc = "location of the cert file, only used when using https"},
      });

  const std::string filename = argli.GetString("inputfile");
  const std::string cert_dir = argli.GetString("cert_dir");

#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
  std::string cert_path = cert_dir + "/cert.pem";
  std::string key_path = cert_dir + "/key.pem";
  CheckFileExists(cert_path);
  CheckFileExists(key_path);
  httplib::SSLServer svr(cert_path.c_str(), key_path.c_str());
#else
  httplib::Server svr;
#endif

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
  const MMGraph& mg = *((MMGraph*)ptr);
  CHECK_EQ_S(mg.magic, kMagic);
  CHECK_EQ_S(mg.version_major, kVersionMajor);
  CHECK_EQ_S(mg.version_minor, kVersionMinor);

  svr.Get("/hi", [&](const httplib::Request&, httplib::Response& res) {
    res.set_content("Hello World!", "text/plain");
  });

  svr.Get("/", [](const httplib::Request& req, httplib::Response& res) {
    HandleFileRequest(req, res, "../src/html/leaflet.html", "text/html");
  });

  svr.Get(
      "/favicon.ico", [](const httplib::Request& req, httplib::Response& res) {
        HandleFileRequest(req, res, "../src/html/favicon.ico", "image/x-icon");
      });

  svr.Get("/start_icon.png", [](const httplib::Request& req,
                                httplib::Response& res) {
    HandleFileRequest(req, res, "../src/html/start_icon.png", "image/png");
  });

  svr.Get("/target_icon.png", [](const httplib::Request& req,
                                 httplib::Response& res) {
    HandleFileRequest(req, res, "../src/html/target_icon.png", "image/png");
  });

  svr.Get(
      "/route/(v1[hybrid]*)/driving/"
      "(-?[0-9.]+),(-?[0-9.]+);(-?[0-9.]+),(-?[0-9.]+)",
      [&mg](const httplib::Request& req, httplib::Response& res) {
        const absl::Time overall_start = absl::Now();

#if 0
        LOG_S(INFO) << "=== Request Dump ===";
        LOG_S(INFO) << "Method: " << req.method;
        LOG_S(INFO) << "Path: " << req.path;
        LOG_S(INFO) << "Headers:";
        for (const auto& h : req.headers) {
          LOG_S(INFO) << "  " << h.first << ": " << h.second;
        }
        LOG_S(INFO) << "Body: " << req.body;
        LOG_S(INFO) << "====================";
#endif

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
            result = ComputeRoute(mg, hybrid, lon1, lat1, lon2, lat2);
          }
        }
        {
          auto result_start = absl::Now();
          res.set_header("Access-Control-Allow-Origin", "*");
          res.set_content(result.dump(), "application/json");
          LOG_S(INFO) << absl::StrFormat(
              "**** Web result creation:      %.3f secs",
              ToDoubleSeconds(absl::Now() - result_start));
        }
        // LOG_S(INFO) << result.dump(2);
        LogMemoryUsage();
        LOG_S(INFO) << absl::StrFormat(
            "**** elapsed: %.2f secs",
            ToDoubleSeconds(absl::Now() - overall_start));
      });

  // Fill in empty data.
  g_last_router_data.reset(new MMHybridRouter::RouterData);
  svr.Get(R"(/tiles/last_route/([^/]+)/([^/]+)/([^/]+)\.png)",
          [&mg](const httplib::Request& req, httplib::Response& res) {
            // Keep it alive while we serve the request.
            std::shared_ptr<MMHybridRouter::RouterData> rd = g_last_router_data;
            // This will change on each route, so it shouldn't be cached.
            res.set_header("Cache-Control",
                           "no-store, no-cache, must-revalidate, max-age=0");
            res.set_header("Pragma", "no-cache");
            res.set_header("Expires", "0");
            res.set_content(CreatePNGForHybridRouting(
                                mg, *rd, atoi(req.matches.str(1).c_str()),
                                atoi(req.matches.str(2).c_str()),
                                atoi(req.matches.str(3).c_str())),
                            "image/png");
          });

  const MMTileData mm_tile_data(mg);
  // Match the request path against a regular expression
  // and extract its captures
  svr.Get(R"(/tiles/([^/]+)/([^/]+)/([^/]+)/([^/]+)\.png)",
          [&mm_tile_data](const httplib::Request& req, httplib::Response& res) {
            res.set_content(CreatePNG(mm_tile_data, req.matches.str(1),
                                      atoi(req.matches.str(2).c_str()),
                                      atoi(req.matches.str(3).c_str()),
                                      atoi(req.matches.str(4).c_str())),
                            "image/png");
          });

  // Try something on startup:
  decode_polyline("ar~_Hwcft@Ny@");
  decode_polyline("qq~_Hqeft@");
  LOG_S(INFO) << ComputeRoute(mg, /*hybrid=*/false, 8.720121, 47.3476881,
                              8.7204095, 47.3476057)
                     .dump(2);

  mg.PrintInfo();
  LOG_S(INFO) << "Listening...";

  svr.listen("0.0.0.0", 8081);
}
