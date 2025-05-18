// #define CPPHTTPLIB_OPENSSL_SUPPORT
#include <math.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>

#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/str_split.h"
#include "base/thread_pool.h"
#include "base/util.h"
#include "cpp-httplib/httplib.h"
#include "gd.h"
#include "geometry/line_clipping.h"

namespace {

// HTTP
httplib::Server svr;

// HTTPS
// httplib::SSLServer svr;

constexpr std::int64_t num_pixels = 256;

// using WorldPoint = std::pair<double, double>;
// using PixelPoint = std::pair<std::int32_t, std::int32_t>;

struct WorldPoint {
  double x;
  double y;
};

struct PixelPoint {
  int32_t x;
  int32_t y;
};

WorldPoint LatLonToMercator(double lat, double lon) {
  lon += 180.0;
  const double x = lon * (num_pixels / 360.0);

  if (lat < -85.0511289) lat = -85.0511289;
  if (lat > 85.0511289) lat = 85.0511289;
  const double lat_radians = lat * (M_PI / 180.0);
  const double y_mercator_radians =
      M_PI - std::log(std::tan((M_PI / 4.0) + (lat_radians / 2.0)));
  const double y = y_mercator_radians * (num_pixels / (2.0 * M_PI));

  return {x, y};
}

PixelPoint ZoomPoint(const WorldPoint p, std::uint8_t zoom) {
  std::int32_t x = (1 << zoom) * p.x;
  std::int32_t y = (1 << zoom) * p.y;
  return {x, y};
}

WorldPoint TileToWorld(int zoom, int tile_x, int tile_y) {
  return {(num_pixels * tile_x) / static_cast<double>(1 << zoom),
          (num_pixels * tile_y) / static_cast<double>(1 << zoom)};
}

enum Color {
  RED,
  GREEN,
  BLUE,
  MAGENTA,
  YELLOW,
  BLACK,
  VIOLET,
  OLIVE,
  LBLUE,
  DGREEN,
  DRED,
  BROWN,
  GREY,
  GBLUE,
  ORANGE,
  LGREEN,
  GREENBL,
  LBROWN,
  PINK,
  DPINK,
  RED25,
  RED50,
  RED75,
  NUM_COLORS
};

struct LineSegment {
  // World coordinates.
  double x0;
  double y0;
  double x1;
  double y1;
  Color color;
  bool is_circle;
};

// roughly 4km in world coordinates on equator.
constexpr double MaxXSmall = 256.0 / 10000;
struct LayerData {
  std::string filename;
  std::vector<LineSegment> small;
  std::vector<LineSegment> large;
  std::vector<LineSegment> self_edges;
};

// If numstr contains a dot then parse it as double, otherwise parse it as
// integer and divide by 10^7, assuming unit is 100 nanodegrees.
double ConvertToDouble(std::string_view numstr) {
  if (absl::StrContains(numstr, '.')) {
    double num;
    if (!absl::SimpleAtod(numstr, &num)) {
      LOG_S(INFO) << "Can not convert string to double: <" << numstr << ">";
    }
    return num;
  } else {
    int64_t num;
    if (!absl::SimpleAtoi(numstr, &num)) {
      LOG_S(INFO) << "Can not convert string to int64: <" << numstr << ">";
    }
    return num / 10000000.0;
  }
}

void LoadSegments(const std::string& filename, LayerData* data) {
  LOG_S(INFO) << "Load segments from " << filename;
  std::ifstream file(filename);
  if (!file.is_open()) {
    perror(filename.c_str());
    exit(EXIT_FAILURE);
  }

  uint32_t self_edges = 0;
  WorldPoint prev_point = {};
  bool polymode = false;

  for (std::string line; std::getline(file, line);) {
    std::vector<std::string_view> row = absl::StrSplit(line, ',');

    CHECK_GE_S(row.size(), 4u);
    double lat = ConvertToDouble(row.at(2));
    double lon = ConvertToDouble(row.at(3));
    WorldPoint p = LatLonToMercator(lat, lon);

    LineSegment seg;
    if (row.at(0) == "line") {
      polymode = false;
      CHECK_EQ_S(row.size(), 6u);
      lat = ConvertToDouble(row.at(4));
      lon = ConvertToDouble(row.at(5));
      WorldPoint p1 = LatLonToMercator(lat, lon);
      seg = {p.x, p.y, p1.x, p1.y, BLACK};
    } else if (row.at(0) == "poly-start") {
      CHECK_EQ_S(row.size(), 4u);
      prev_point = p;
      polymode = true;
      continue;
    } else {
      CHECK_EQ_S(row.size(), 4u);
      CHECK_EQ_S(row.at(0), "pt");
      CHECK_S(polymode);
      seg = {prev_point.x, prev_point.y, p.x, p.y, BLACK};
      prev_point = p;
    }

    if (seg.x0 > seg.x1) {
      std::swap(seg.x0, seg.x1);
      std::swap(seg.y0, seg.y1);
    }

    if (row.at(1) == "blue") {
      seg.color = BLUE;
    } else if (row.at(1) == "green") {
      seg.color = GREEN;
    } else if (row.at(1) == "red") {
      seg.color = RED;
    } else if (row.at(1) == "mag") {
      seg.color = MAGENTA;
    } else if (row.at(1) == "yel") {
      seg.color = YELLOW;
    } else if (row.at(1) == "black") {
      seg.color = BLACK;
    } else if (row.at(1) == "violet") {
      seg.color = VIOLET;
    } else if (row.at(1) == "olive") {
      seg.color = OLIVE;
    } else if (row.at(1) == "lblue") {
      seg.color = LBLUE;
    } else if (row.at(1) == "dgreen") {
      seg.color = DGREEN;
    } else if (row.at(1) == "dred") {
      seg.color = DRED;
    } else if (row.at(1) == "brown") {
      seg.color = BROWN;
    } else if (row.at(1) == "grey") {
      seg.color = GREY;
    } else if (row.at(1) == "gblue") {
      seg.color = GBLUE;
    } else if (row.at(1) == "orange") {
      seg.color = ORANGE;
    } else if (row.at(1) == "lgreen") {
      seg.color = LGREEN;
    } else if (row.at(1) == "greenbl") {
      seg.color = GREENBL;
    } else if (row.at(1) == "lbrown") {
      seg.color = LBROWN;
    } else if (row.at(1) == "pink") {
      seg.color = PINK;
    } else if (row.at(1) == "dpink") {
      seg.color = DPINK;
    } else if (row.at(1) == "red25") {
      seg.color = RED25;
    } else if (row.at(1) == "red50") {
      seg.color = RED50;
    } else if (row.at(1) == "red75") {
      seg.color = RED75;
    } else {
      LOG_S(FATAL) << "unknow color " << row.at(1);
    }

    if (seg.x0 == seg.x1 && seg.y0 == seg.y1) {
      self_edges++;
      seg.is_circle = true;
      constexpr double kExt = 256.0 / 4000000;  // ~10m
      seg.x0 -= kExt;
      seg.y0 -= kExt;
      seg.x1 += kExt;
      seg.y1 += kExt;
      data->small.push_back(seg);
    } else if (seg.x1 - seg.x0 < MaxXSmall) {
      seg.is_circle = false;
      data->small.push_back(seg);
    } else {
      seg.is_circle = false;
      data->large.push_back(seg);
    }
  }
  LOG_S(INFO) << absl::StrFormat(
      "finished LoadSegments, %d small + %d large,  %d self-edges",
      data->small.size(), data->large.size(), self_edges);
}

void LoadFiles(const std::string& wildcard, LayerData* data) {
  LOG_S(INFO) << "Load files from " << wildcard;
  for (const std::filesystem::path path : GetFilesWithWildcard(wildcard)) {
    LoadSegments(path, data);
  }
  std::stable_sort(
      data->small.begin(), data->small.end(),
      [](const LineSegment& a, const LineSegment& b) { return a.x0 < b.x0; });
  LOG_S(INFO) << "finished sorting " << data->small.size() << " small segments";
}

// Switzerland: 8.71, 47.37
// Rehovot: 34.81, 31.9

std::string CreatePNG(const LayerData& layer_data, int zoom, int tile_x,
                      int tile_y) {
  gdImagePtr im = gdImageCreateTrueColor(256, 256);
  gdImageSaveAlpha(im, 1);
  gdImageAlphaBlending(im, false);
  int trans_color = gdImageColorAllocateAlpha(im, 0, 0, 0, 127);
  gdImageFill(im, 0, 0, trans_color);

  int colors[NUM_COLORS];
  colors[RED] = gdImageColorAllocate(im, 255, 0, 0);
  colors[GREEN] = gdImageColorAllocate(im, 0, 255, 0);
  colors[BLUE] = gdImageColorAllocate(im, 0, 0, 255);
  colors[MAGENTA] = gdImageColorAllocate(im, 255, 0, 255);
  colors[YELLOW] = gdImageColorAllocate(im, 255, 213, 0);
  colors[BLACK] = gdImageColorAllocate(im, 0, 0, 0);
  colors[VIOLET] = gdImageColorAllocate(im, 138, 43, 226);
  colors[OLIVE] = gdImageColorAllocate(im, 186, 184, 108);
  colors[LBLUE] = gdImageColorAllocate(im, 173, 216, 230);
  colors[DGREEN] = gdImageColorAllocate(im, 2, 100, 64);
  colors[DRED] = gdImageColorAllocate(im, 165, 0, 0);
  colors[BROWN] = gdImageColorAllocate(im, 139, 69, 19);
  colors[GREY] = gdImageColorAllocate(im, 128, 128, 128);
  colors[GBLUE] = gdImageColorAllocate(im, 0, 255, 255);
  colors[ORANGE] = gdImageColorAllocate(im, 255, 128, 0);
  colors[LGREEN] = gdImageColorAllocate(im, 0, 179, 0);
  colors[GREENBL] = gdImageColorAllocate(im, 0, 204, 153);
  colors[LBROWN] = gdImageColorAllocate(im, 181, 101, 29);
  colors[PINK] = gdImageColorAllocate(im, 255, 167, 182);
  colors[DPINK] = gdImageColorAllocate(im, 255, 20, 147);
  colors[RED25] = gdImageColorAllocate(im, 255, 196, 196);
  colors[RED50] = gdImageColorAllocate(im, 255, 137, 137);
  colors[RED75] = gdImageColorAllocate(im, 225, 78, 78);

  gdImageSetThickness(im, 0);
  gdImageRectangle(im, 0, 0, 255, 255,
                   gdImageColorAllocateAlpha(im, 0, 0, 0, 100));

  {
    WorldPoint tile_wp0 = TileToWorld(zoom, tile_x, tile_y);
    WorldPoint tile_wp1 = TileToWorld(zoom, tile_x + 1, tile_y + 1);
    TwoPoint viewport = {tile_wp0.x, tile_wp0.y, tile_wp1.x, tile_wp1.y};
    const PixelPoint tile_px = ZoomPoint(tile_wp0, zoom);
    const int offx = tile_px.x;
    const int offy = tile_px.y;

    gdImageSetThickness(im, zoom < 15 ? 2 : 3);
    for (const LineSegment& seg : layer_data.large) {
      TwoPoint line = {seg.x0, seg.y0, seg.x1, seg.y1};
      if (ClipLineCohenSutherland(viewport, &line)) {
        PixelPoint p0 = ZoomPoint({line.x0, line.y0}, zoom);
        PixelPoint p1 = ZoomPoint({line.x1, line.y1}, zoom);
        gdImageLine(im, p0.x - offx, p0.y - offy, p1.x - offx, p1.y - offy,
                    colors[seg.color]);
      }
    }

    auto it = std::lower_bound(
        layer_data.small.begin(), layer_data.small.end(),
        viewport.x0 - MaxXSmall,
        [](const LineSegment& s, double value) { return s.x0 < value; });
    uint32_t count = 0;
    for (; it != layer_data.small.end(); it++) {
      const LineSegment& seg = *it;
      if (seg.x0 > viewport.x1) break;
      count++;
      if (!seg.is_circle) {
        TwoPoint line = {seg.x0, seg.y0, seg.x1, seg.y1};
        if (ClipLineCohenSutherland(viewport, &line)) {
          PixelPoint p0 = ZoomPoint({line.x0, line.y0}, zoom);
          PixelPoint p1 = ZoomPoint({line.x1, line.y1}, zoom);
          gdImageLine(im, p0.x - offx, p0.y - offy, p1.x - offx, p1.y - offy,
                      colors[seg.color]);
        }
      } else if (seg.x0 <= viewport.x1 && seg.x1 >= viewport.x0 &&
                 seg.y0 <= viewport.y1 && seg.y1 >= viewport.y0) {
        // Circle
        PixelPoint p0 = ZoomPoint({seg.x0, seg.y0}, zoom);
        PixelPoint p1 = ZoomPoint({seg.x1, seg.y1}, zoom);
        int32_t w = (p1.x - p0.x);
        int32_t h = (p1.y - p0.y);
        gdImageFilledEllipse(im, p0.x + w / 2 - offx, p0.y + h / 2 - offy, w, h,
                             colors[seg.color]);
      }
    }
  }

  int size = 0;
  void* ptr = gdImagePngPtr(im, &size);
  std::string buff((char*)ptr, size);
  gdImageDestroy(im);
  return buff;
}

void AddLayer(std::string key, std::string filename,
              std::map<std::string, LayerData>* layers) {
  (*layers)[key] = {.filename = filename};
}

void LoadLayers(std::map<std::string, LayerData>* layers) {
  FUNC_TIMER();
  ThreadPool pool;
  for (const auto& [key, _] : *layers) {
    LayerData& data = (*layers)[key];
    CHECK_S(!data.filename.empty());
    pool.AddWork([&](int) { LoadFiles(data.filename, &data); });
  }
  pool.Start(22);
  pool.WaitAllFinished();
}

}  // namespace

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);

  if (argc > 2) {
    LOG_S(FATAL) << absl::StrFormat("usage: %s [<dir>]", argv[0]);
  }

  std::string root("/tmp");
  std::string admin_root("../../data");

  if (argc == 2) {
    root = argv[1];
    admin_root = argv[1];
  }
  LOG_S(INFO) << "sizeof(LineSegment):" << sizeof(LineSegment);

  std::map<std::string, LayerData> layers;
  AddLayer("graph_motorcar", root + "/graph_motorcar.csv", &layers);
  AddLayer("graph_bicycle", root + "/graph_bicycle.csv", &layers);

  AddLayer("pb_dijks_forward", root + "/pb_dijks_forward.csv", &layers);
  AddLayer("pb_dijks_backward", root + "/pb_dijks_backward.csv", &layers);
  AddLayer("pb_dijks_forward_hybrid", root + "/pb_dijks_forward_hybrid.csv",
           &layers);
  AddLayer("pb_astar_forward", root + "/pb_astar_forward.csv", &layers);
  AddLayer("pb_astar_backward", root + "/pb_astar_backward.csv", &layers);
  AddLayer("pb_astar_forward_hybrid", root + "/pb_astar_forward_hybrid.csv",
           &layers);

  AddLayer("uw_dijks_forward_hybrid", root + "/uw_dijks_forward_hybrid.csv",
           &layers);
  AddLayer("uw_astar_forward_hybrid", root + "/uw_astar_forward_hybrid.csv",
           &layers);
  AddLayer("as_dijks_forward_hybrid", root + "/as_dijks_forward_hybrid.csv",
           &layers);
  AddLayer("as_astar_forward_hybrid", root + "/as_astar_forward_hybrid.csv",
           &layers);
  AddLayer("ln_dijks_forward_hybrid", root + "/ln_dijks_forward_hybrid.csv",
           &layers);
  AddLayer("ln_astar_forward_hybrid", root + "/ln_astar_forward_hybrid.csv",
           &layers);

  AddLayer("ALL", admin_root + "/admin/??_*.csv", &layers);
  AddLayer("CH", admin_root + "/admin/CH_756_*.csv", &layers);
  AddLayer("DE", admin_root + "/admin/DE_276_*.csv", &layers);

  AddLayer("IL", admin_root + "/admin/IL_376_*.csv", &layers);
  AddLayer("JO", admin_root + "/admin/JO_400_*.csv", &layers);
  AddLayer("UA", admin_root + "/admin/UA_804_*.csv", &layers);
  AddLayer("RU", admin_root + "/admin/RU_643_*.csv", &layers);

  AddLayer("louvain", root + "/louvain.csv", &layers);
  AddLayer("cross", root + "/cross.csv", &layers);

  AddLayer("traffic", root + "/traffic.csv", &layers);
  AddLayer("experimental1", root + "/experimental1.csv", &layers);
  AddLayer("experimental2", root + "/experimental2.csv", &layers);
  AddLayer("experimental3", root + "/experimental3.csv", &layers);
  AddLayer("experimental4", root + "/experimental4.csv", &layers);
  AddLayer("experimental5", root + "/experimental5.csv", &layers);
  AddLayer("experimental6", root + "/experimental6.csv", &layers);
  AddLayer("experimental7", root + "/experimental7.csv", &layers);
  AddLayer("experimental8", root + "/experimental8.csv", &layers);
  AddLayer("experimental9", root + "/experimental9.csv", &layers);

  LoadLayers(&layers);

  svr.Get("/hi", [&](const httplib::Request&, httplib::Response& res) {
    res.set_content("Hello World!", "text/plain");
  });

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

  LOG_S(INFO) << "Listening...";
  svr.listen("0.0.0.0", 8080);
}
