// #define CPPHTTPLIB_OPENSSL_SUPPORT
#include <math.h>

#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <optional>
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
inline GeoAnchor FindClosestEdgesWithCache(const MMGraph& mg, LatLon pt) {
  static LRUCache<uint64_t, GeoAnchor> g_closest_edge_cache(32);

  // Combine both lat and lon into one uint64_t, so we don't have to create a
  // hash function.
  uint64_t key = static_cast<uint32_t>(pt.lat.v());
  key = (key << 32) + static_cast<uint32_t>(pt.lon.v());

  std::optional<GeoAnchor> res = g_closest_edge_cache.get(key);
  if (res.has_value()) {
    return res.value();
  }
  GeoAnchor anch = FindClosestEdges(mg, pt);
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

std::string EncodePolyline(const std::vector<LatLon>& coordinates) {
  // constexpr int precision = 5;
  std::string encoded = "";
  int prev_lat = 0;
  int prev_lon = 0;

  // const double factor = std::pow(10.0, precision);

  for (const auto& coord : coordinates) {
    // int lat = static_cast<int>(std::round(coord.lat * factor));
    // int lon = static_cast<int>(std::round(coord.lon * factor));

    //  Get a compilation error if LatE6/LonE6 doesn't have a factor of 10^6
    //  anymore.
    static_assert(LatE6::MulFactor() == 1'000'000);
    static_assert(LonE6::MulFactor() == 1'000'000);
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

#if 0
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
#endif

struct JsonData {
  nlohmann::json j;
  double sum_dist = 0.0;
  double sum_duration = 0.0;
};

std::string GetStreetName(const MMGraph& mg, const MMFullEdge& fe) {
  const MMCluster& mc = fe.mc(mg);
  MWayIdxT way_idx = fe.way_idx(mc);
  return absl::StrFormat(
      "%s (%s)", mc.get_streetname(way_idx),
      HighwayLabelToString(mc.get_wsa(way_idx).highway_label_));
}

std::string GetEdgeName(const MMGraph& mg, const MMFullEdge& fe) {
  const MMCluster& mc = fe.mc(mg);
  MWayIdxT way_idx = fe.way_idx(mc);
  std::string_view streetname = mc.get_streetname(way_idx);
  int64_t way_id = mc.grouped_way_to_osm_id.at(way_idx);
  int64_t n0_id = mc.grouped_node_to_osm_id.at(fe.from_node_idx);
  int64_t n1_id = mc.grouped_node_to_osm_id.at(fe.edge(mc).target_idx());
  return absl::StrFormat("<%s> (w:%lld %lld->%lld)", streetname, way_id, n0_id,
                         n1_id);
}

double Round1(double val) { return std::round(val * 10.0) / 10.0; }

// Given a start edge point, compute the vector of shape coordinates that
// represent the start sequence of coordinates on that edge.
std::vector<LatLon> ComputeStartShapeCoords(const MMCluster& mc,
                                            const EdgePoint& ep) {
  const MMFullEdge fe = ep.fe;
  const std::vector<LatLon> shapes =
      mc.get_shape_coords_extended(fe.from_node_idx, fe.edge_idx(mc));
  // We start traveling on the edge at this distance.
  const uint32_t fraction_dist =
      mc.edge_to_distance.at(fe.edge_idx(mc)).cm() * ep.to_fraction;
  uint64_t sum_dist = 0;
  std::vector<LatLon> res = {ep.coord_at_fraction};
  for (uint32_t pos = 0; pos + 1 < shapes.size(); ++pos) {
    if (sum_dist >= fraction_dist) {
      res.push_back(shapes.at(pos));
    }
    sum_dist += calculate_distance(shapes.at(pos), shapes.at(pos + 1)).cm();
  }
  res.push_back(shapes.back());
  return res;
}

// Given is a target edge point and an existing shape coordinate list 'coords'
// that starts at 'at_fraction' on the edge and continues to the end.
// This function removes coordinates from the end of the list and terminates the
// (potentially new) final segment at ep.coord_at_fraction.
//
// This idea behind this function is to terminate the last edge in a route
// properly, in two scenarios:
//   1) In a route with more than one edge. In this case the last path segment
//      uses all shape coordinates except the ones after the target point.
//   2) In a very short route with only one edge. In this case, the shape
//      coordinate list has missing parts in the beginning and the end.
void TerminateTargetShapeCoords(const MMCluster& mc, const EdgePoint& ep,
                                double at_fraction,
                                std::vector<LatLon>* shapes) {
  CHECK_GE_S(ep.to_fraction, at_fraction) << "Target has to be after start";
  // How much distance do we have to travel until we cut the vector?
  const uint32_t fraction_dist =
      mc.edge_to_distance.at(ep.fe.edge_idx(mc)).cm() *
      (ep.to_fraction - at_fraction);
  uint64_t sum_dist = 0;
  for (uint32_t pos = 0; pos + 1 < shapes->size(); ++pos) {
    sum_dist += calculate_distance(shapes->at(pos), shapes->at(pos + 1)).cm();
    if (sum_dist >= fraction_dist) {
      // Terminate current segment.
      shapes->at(pos + 1) = ep.coord_at_fraction;
      shapes->resize(pos + 2);  // Delete all elements after position pos + 1.
      break;
    }
  }
}

class StepsData {
 public:
  StepsData(const MMGraph& mg, const MMRoutingResult& res) : res_(res) {}

  size_t num_steps() const { return res_.full_edges.size(); }

  // Create the description of one step in the route. This includes the edges in
  // the range [from_pos..to_pos].
  JsonData CreateOneStep(const MMGraph& mg, uint32_t from_pos,
                         uint32_t to_pos) const {
    std::vector<LatLon> total_coords;
    uint64_t sum_duration = 0;
    uint64_t sum_distance = 0;
    int16_t prev_bearing = 0;

    for (uint32_t pos = from_pos; pos <= to_pos; ++pos) {
      const MMFullEdge& fe = res_.full_edges.at(pos);
      const MMCluster& mc = fe.mc(mg);
      sum_duration += res_.edge_metric(pos);
      sum_distance += res_.distance(mg, pos).cm();

      // Compute the shape coordinates for the edge at pos. The most complicated
      // case occurs when there is only one edge, i.e. the start and target are
      // on the same edge. In this case, shape coordinates at the start *and* at
      // the end of the edge might have to be cut.
      std::vector<LatLon> coords;
      double start_at_fraction;  // The shape list starts at this fraction.
      if (pos == 0) {
        coords = ComputeStartShapeCoords(mc, res_.start);
        start_at_fraction = res_.start.to_fraction;
        // We dont have a prev, so use the start bearing of the route.
        CHECK_GE_S(coords.size(), 2);
        prev_bearing = true_north_bearing(coords.at(0), coords.at(1));
      } else {
        coords =
            mc.get_shape_coords_extended(fe.from_node_idx, fe.edge_idx(mc));
        start_at_fraction = 0.0;
      }
      if (pos + 1 == num_steps()) {
        TerminateTargetShapeCoords(mc, res_.target, start_at_fraction, &coords);
      }

      if (pos == from_pos) {
        std::swap(coords, total_coords);
      } else {
        CHECK_S(total_coords.back() == coords.front()) << fe.DebugString(mg);
        // Append.
        // TODO: Not yet supported by gcc: coords.append_range(v);
        total_coords.insert(total_coords.end(), coords.cbegin() + 1,
                            coords.cend());
      }
    }
    CHECK_GE_S(total_coords.size(), 2);

    // convert to seconds/meters.
    const double duration = sum_duration / 1000.0;
    const double dist = sum_distance / 100.0;
    const int16_t bearing =
        true_north_bearing(total_coords.at(0), total_coords.at(1));

    const MMFullEdge& fe = res_.full_edges.at(from_pos);
    nlohmann::json maneuver = {
        {"location",
         {total_coords.front().lon.AsDouble(),
          total_coords.front().lat.AsDouble()}},
        {"bearing_before", prev_bearing},
        {"bearing_after", bearing},
        {"type", (from_pos == 0 ? "depart" : "continue")},
        {"modifier", "ModifierContinue"}};

    nlohmann::json step = {{"geometry", EncodePolyline(total_coords)},
                           {"maneuver", maneuver},
                           {"name", GetStreetName(mg, fe)},
                           {"duration", Round1(duration)},
                           {"distance", Round1(dist)}};

    // Use the final bearing as 'prev_bearing' in the next step.
    prev_bearing = true_north_bearing(total_coords.at(total_coords.size() - 2),
                                      total_coords.at(total_coords.size() - 1));

    return {.j = step, .sum_dist = dist, .sum_duration = duration};
  }

  JsonData CreateArrivalStep(const MMGraph& mg) const {
    MMFullEdge fe = res_.full_edges.back();

    LatLon to_coord = res_.target.coord_at_fraction;
    std::vector<LatLon> coords;
    coords.push_back(to_coord);

    nlohmann::json maneuver = {
        {"location", {to_coord.lon.AsDouble(), to_coord.lat.AsDouble()}},
        {"bearing_before", 90},
        {"bearing_after", 90},
        {"type", "arrive"}};

    nlohmann::json step = {{"geometry", EncodePolyline(coords)},
                           {"maneuver", maneuver},
                           {"name", GetEdgeName(mg, fe)},
                           {"duration", 0},
                           {"distance", 0}};
    return {.j = step};
  }

  const MMRoutingResult& GetRoutingResult() const { return res_; }

 private:
  const MMRoutingResult& res_;
};

JsonData CreateSteps(const MMGraph& mg, const StepsData& steps_data) {
  JsonData result;
  result.j = nlohmann::json::array();
  const MMRoutingResult& r = steps_data.GetRoutingResult();

  for (uint32_t pos = 0; pos < steps_data.num_steps(); ++pos) {
    const MMFullEdge fe = r.full_edges.at(pos);
    const HIGHWAY_LABEL hw_tag = fe.get_wsa(fe.mc(mg)).highway_label_;
    const std::string name = GetStreetName(mg, fe);
    uint32_t start_pos = pos;
    // Find the end of the sequence of the current highway tag and street name.
    while (pos + 1 < steps_data.num_steps()) {
      const MMFullEdge fe_next = r.full_edges.at(pos + 1);
      if (fe_next.get_wsa(fe_next.mc(mg)).highway_label_ != hw_tag) {
        break;
      }
      if (GetStreetName(mg, fe_next) != name) {
        break;
      }
      ++pos;
    }

    JsonData tmp = steps_data.CreateOneStep(mg, start_pos, pos);
    result.sum_dist += tmp.sum_dist;
    result.sum_duration += tmp.sum_duration;
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
        {{"distance",
          std::roundf(res.start.distance_to_seg.cm() / 10.0) / 10.0},
         {"name", GetEdgeName(mg, res.start.fe)},
         {"location",
          {res.start.coord_at_fraction.lon.AsDouble(),
           res.start.coord_at_fraction.lat.AsDouble()}}});
  }
  {
    waypoints.push_back(
        {{"distance",
          std::roundf(res.target.distance_to_seg.cm() / 10.0) / 10.0},
         {"name", GetEdgeName(mg, res.target.fe)},
         {"location",
          {res.target.coord_at_fraction.lon.AsDouble(),
           res.target.coord_at_fraction.lat.AsDouble()}}});
  }

  // We currently support only one leg, so the only thing to fill are the
  // steps.
  StepsData steps_data(mg, res);

  JsonData res_steps = CreateSteps(mg, steps_data);
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

nlohmann::json ComputeRoute(const MMGraph& mg, LatLon start_pt,
                            LatLon target_pt) {
  FUNC_TIMER();

  GeoAnchor start;
  GeoAnchor target;
  double find_closest_time;
  {
    absl::Time start_time = absl::Now();
    start = FindClosestEdgesWithCache(mg, start_pt);
    target = FindClosestEdgesWithCache(mg, target_pt);
    find_closest_time = ToDoubleSeconds(absl::Now() - start_time);
  }
  LOG_S(INFO) << absl::StrFormat("**** Find closest edges: %.2f secs",
                                 find_closest_time);

  for (const auto& e : start.edge_points()) {
    LOG_S(INFO) << e.DebugStringExt(mg, start_pt.lat, start_pt.lon);
  }
  for (const auto& e : target.edge_points()) {
    LOG_S(INFO) << e.DebugStringExt(mg, target_pt.lat, target_pt.lon);
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
              nlohmann::json result = ComputeRoute(
                  mg, {LatE6(lat1), LonE6(lon1)}, {LatE6(lat2), LonE6(lon2)});
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
  // decode_polyline("ar~_Hwcft@Ny@");
  // decode_polyline("qq~_Hqeft@");
  LOG_S(INFO) << ComputeRoute(mg, {LatE6(47.3476881), LonE6(8.720121)},
                              {LatE6(47.3476057), LonE6(8.7204095)})
                     .dump(2);

  mg.PrintInfo();
  LOG_S(INFO) << "Listening...";

  svr.listen("0.0.0.0", 8081);
}
