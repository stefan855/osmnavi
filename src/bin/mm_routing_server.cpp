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
#include "base/deg_coord.h"
#include "base/lru_cache.h"
#include "base/thread_pool.h"
#include "base/util.h"
#include "geometry/closest_edge.h"
#include "geometry/tiles.h"
#include "graph/mmgraph_def.h"
#include "httplib.h"

namespace {
struct RouteKey {
  double lat1;
  double lon1;
  double lat2;
  double lon2;
  bool operator==(const RouteKey& other) const {
    return other.lat1 == lat1 && other.lon1 == lon1 && other.lat2 == lat2 &&
           other.lon2 == lon2;
  }
};
}  // namespace

// Hash function specialization
template <>
struct std::hash<RouteKey> {
  size_t operator()(const RouteKey& k) const noexcept {
    // Simple hash combine pattern
    size_t h1 = std::hash<double>{}(k.lat1);
    size_t h2 = std::hash<double>{}(k.lon1);
    size_t h3 = std::hash<double>{}(k.lat2);
    size_t h4 = std::hash<double>{}(k.lon2);

    // Combine hashes (XOR + rotate)
    size_t result = h1;
    result ^= h2 + 0x9e3779b9 + (result << 6) + (result >> 2);
    result ^= h3 + 0x9e3779b9 + (result << 6) + (result >> 2);
    result ^= h4 + 0x9e3779b9 + (result << 6) + (result >> 2);
    return result;
  }
};

namespace {

// This is a global pointer to the last router data that was produced.
static std::shared_ptr<MMHybridRouter::RouterData> g_last_router_data;

// Cache the results of route requests.
static LRUCache<RouteKey, std::string> g_route_result_cache(8);

// Cache the result for 'FindClosestEdges()'.
inline GeoAnchor FindClosestEdgesWithCache(const MMGraph& mg, DegE6 lat,
                                           DegE6 lon) {
  static LRUCache<uint64_t, GeoAnchor> g_closest_edge_cache(32);

  // Combine both lat and lon into one uint64_t, so we don't have to create a
  // hash function.
  uint64_t key = static_cast<uint32_t>(lat.v());
  key = (key << 32) + static_cast<uint32_t>(lon.v());

  std::optional<GeoAnchor> res = g_closest_edge_cache.get(key);
  if (res.has_value()) {
    return res.value();
  }
  GeoAnchor anch = FindClosestEdges(mg, lat, lon);
  g_closest_edge_cache.put(key, anch);
  return anch;
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

std::string EncodePolyline(const std::vector<MMLatLon>& coordinates) {
  // constexpr int precision = 5;
  std::string encoded = "";
  int prev_lat = 0;
  int prev_lon = 0;

  // const double factor = std::pow(10.0, precision);

  for (const auto& coord : coordinates) {
    // int lat = static_cast<int>(std::round(coord.lat * factor));
    // int lon = static_cast<int>(std::round(coord.lon * factor));

    //  Get a compilation error if DegE6 doesn't have a factor of 10^6 anymore.
    static_assert(DegE6::MulFactor() == 1'000'000);
    int lat = (coord.lat.v() + 5) / 10;  // Scale to 10^5.
    int lon = (coord.lon.v() + 5) / 10;  // Scale to 10^5.

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

DegE6 GetLon(const MMCluster& mc, uint32_t n_idx) {
  return mc.node_to_lon(n_idx);
}

DegE6 GetLat(const MMCluster& mc, uint32_t n_idx) {
  return mc.node_to_lat(n_idx);
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

    DegE6 from_lon = GetLon(mc, fe.from_node_idx);
    DegE6 from_lat = GetLat(mc, fe.from_node_idx);
    DegE6 to_lon = GetLon(mc, fe.target_idx(mc));
    DegE6 to_lat = GetLat(mc, fe.target_idx(mc));
    if (pos == 0) {
      from_lon = res_.start.lon_at_fraction;
      from_lat = res_.start.lat_at_fraction;
    }
    if (pos + 1 == num_steps()) {
      to_lon = res_.target.lon_at_fraction;
      to_lat = res_.target.lat_at_fraction;
    }

    std::vector<MMLatLon> coords;
    coords.push_back({.lat = from_lat, .lon = from_lon});
    // TODO: handle start/end segment.
    if (pos != 0 && pos + 1 != num_steps()) {
      const std::vector<MMLatLon> v =
          mc.get_shape_coords(fe.from_node_idx, fe.edge_idx(mc));
      // TODO: Not yet supported by gcc: coords.append_range(v);
      coords.insert(coords.end(), v.cbegin(), v.cend());
    }
    coords.push_back({.lat = to_lat, .lon = to_lon});

    // convert to seconds.
    const double duration = res_.edge_metric(pos) / 1000.0;
    const double dist = res_.distance_cm(mg, pos) / 100.0;

    nlohmann::json maneuver = {
        {"bearing_after", 0},
        {"bearing_before", 0},
        {"location", {from_lon.AsDouble(), from_lat.AsDouble()}},
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

    DegE6 to_lon = res_.target.lon_at_fraction;
    DegE6 to_lat = res_.target.lat_at_fraction;

    std::vector<MMLatLon> coords;
    coords.push_back({.lat = to_lat, .lon = to_lon});

    nlohmann::json maneuver = {
        {"bearing_after", 0},
        {"bearing_before", 0},
        {"location", {to_lon.AsDouble(), to_lat.AsDouble()}},
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
          {res.start.lon_at_fraction.AsDouble(),
           res.start.lat_at_fraction.AsDouble()}}});
  }
  {
    waypoints.push_back(
        {{"distance", std::roundf(res.target.distance_cm / 10.0) / 10.0},
         {"name", GetEdgeName(res.target.fe.mc(mg), res.target.fe)},
         {"location",
          {res.target.lon_at_fraction.AsDouble(),
           res.target.lat_at_fraction.AsDouble()}}});
  }

  // We currently support only one leg, so the only thing to fill are the
  // steps.
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

nlohmann::json ComputeRoute(const MMGraph& mg, DegE6 lat1, DegE6 lon1,
                            DegE6 lat2, DegE6 lon2) {
  FUNC_TIMER();

  LOG_S(INFO) << "Search " << lat1.AsDouble() << " " << lon1.AsDouble();
  GeoAnchor start;
  GeoAnchor target;
  double find_closest_time;
  {
    absl::Time start_time = absl::Now();
    start = FindClosestEdgesWithCache(mg, lat1, lon1);
    target = FindClosestEdgesWithCache(mg, lat2, lon2);
    find_closest_time = ToDoubleSeconds(absl::Now() - start_time);
  }
  LOG_S(INFO) << absl::StrFormat("**** Find closest edges: %.2f secs",
                                 find_closest_time);

  for (const auto& e : start.edge_points()) {
    LOG_S(INFO) << e.DebugString(mg, lat1, lon1);
  }
  for (const auto& e : target.edge_points()) {
    LOG_S(INFO) << e.DebugString(mg, lat2, lon2);
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
  CHECK_EQ_S(mg.magic, kMMMagic);
  CHECK_EQ_S(mg.version_major, kMMVersionMajor);
  CHECK_EQ_S(mg.version_minor, kMMVersionMinor);

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

        // nlohmann::json result;
        std::string_view comp = req.matches.str(1);

        std::string res_str;
        if (comp != "v1" && comp != "v1hybrid") {
          // result = {{"code", "InvalidUrl"}};
          res_str = nlohmann::json({{"code", "InvalidUrl"}}).dump();
        } else {
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
            // result = {{"code", "InvalidQuery"}};
            res_str = nlohmann::json({{"code", "InvalidQuery"}}).dump();
          } else {
            const RouteKey route_key = {lon1, lat1, lon2, lat2};
            std::optional<std::string> cache_res =
                g_route_result_cache.get(route_key);
            if (cache_res.has_value()) {
              res_str = cache_res.value();
              LOG_S(INFO) << "Return cached result length " << res_str.size()
                          << " bytes";
            } else {
              nlohmann::json result = ComputeRoute(mg, DegE6(lat1), DegE6(lon1),
                                                   DegE6(lat2), DegE6(lon2));
              res_str = result.dump();
              g_route_result_cache.put(route_key, res_str);
            }
          }
        }
        {
          auto result_start = absl::Now();
          res.set_header("Access-Control-Allow-Origin", "*");
          // res.set_content(result.dump(), "application/json");
          res.set_content(res_str, "application/json");
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

  const MMGraphTileData mm_tile_data(mg);
  // Match the request path against a regular expression
  // and extract its captures
  svr.Get(R"(/tiles/([^/]+)/([^/]+)/([^/]+)/([^/]+)\.png)",
          [&mm_tile_data](const httplib::Request& req, httplib::Response& res) {
            res.set_content(CreateMMGraphPNG(mm_tile_data, req.matches.str(1),
                                             atoi(req.matches.str(2).c_str()),
                                             atoi(req.matches.str(3).c_str()),
                                             atoi(req.matches.str(4).c_str())),
                            "image/png");
          });

  // Try something on startup:
  decode_polyline("ar~_Hwcft@Ny@");
  decode_polyline("qq~_Hqeft@");
  LOG_S(INFO) << ComputeRoute(mg, DegE6(47.3476881), DegE6(8.720121),
                              DegE6(47.3476057), DegE6(8.7204095))
                     .dump(2);

  mg.PrintInfo();
  LOG_S(INFO) << "Listening...";

  svr.listen("0.0.0.0", 8081);
}
