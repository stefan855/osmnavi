#pragma once
#include <math.h>

#include <fstream>
#include <string>
#include <string_view>

#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/str_split.h"
#include "base/util.h"
#include "gd.h"
#include "geometry/line_clipping.h"

enum TileColor : int {
  RED,
  GREEN,
  BLUE,
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
  MAGENTA,
  DPINK,
  RED25,
  RED50,
  RED75,
  NUM_COLORS
};

namespace {

constexpr int64_t kPixelsZoom0 = 256;
constexpr double kMaxMercatorLat = 85.0511289;
constexpr double kMaxMercatorLatRad = kMaxMercatorLat / 180.0 * M_PI;

struct WorldPoint {
  double x;
  double y;
};

struct PixelPoint {
  int32_t x;
  int32_t y;
};

WorldPoint LatLonToPixelMercator(double lat, double lon) {
  lon += 180.0;
  const double x = lon * (kPixelsZoom0 / 360.0);

  if (lat < -kMaxMercatorLat) lat = -kMaxMercatorLat;
  if (lat > kMaxMercatorLat) lat = kMaxMercatorLat;
  const double lat_radians = lat * (M_PI / 180.0);
  const double y_mercator_radians =
      M_PI - std::log(std::tan((M_PI / 4.0) + (lat_radians / 2.0)));
  const double y = y_mercator_radians * (kPixelsZoom0 / (2.0 * M_PI));

  return {x, y};
}

#if 0
void PixelMercatorToLatLon(double x_pixel, double y_pixel, double* out_lat,
                           double* out_lon) {
  double lon_rad = (x_pixel / kPixelsZoom0) * (2.0 * M_PI) - M_PI;
  double n = y_pixel / kPixelsZoom0;
  double merc_y_log = M_PI * (1.0 - 2.0 * n);
  double lat_rad = 2.0 * std::atan(std::exp(merc_y_log)) - (M_PI / 2.0);
  if (lat_rad > kMaxMercatorLatRad) lat_rad = kMaxMercatorLatRad;
  if (lat_rad < -kMaxMercatorLatRad) lat_rad = -kMaxMercatorLatRad;
  *out_lon = lon_rad * (180.0 / M_PI);
  *out_lat = lat_rad * (180.0 / M_PI);
  while (*out_lon <= -180.0) *out_lon += 360.0;
  while (*out_lon > 180.0) *out_lon -= 360.0;
}
#endif

PixelPoint ZoomPoint(const WorldPoint p, std::uint8_t zoom) {
  std::int32_t x = (1 << zoom) * p.x;
  std::int32_t y = (1 << zoom) * p.y;
  return {x, y};
}

WorldPoint TileToWorld(int zoom, int tile_x, int tile_y) {
  return {(kPixelsZoom0 * tile_x) / static_cast<double>(1 << zoom),
          (kPixelsZoom0 * tile_y) / static_cast<double>(1 << zoom)};
}

}  // namespace

struct MMTileData {
  MMTileData(const MMGraph& mg) : mg(mg) {
    for (const MMCluster& mc : mg.clusters.span()) {
      const WorldPoint mc_min =
          LatLonToPixelMercator(mc.bounding_rect.min.lat / TEN_POW_7_DBL,
                                mc.bounding_rect.min.lon / TEN_POW_7_DBL);
      const WorldPoint mc_max =
          LatLonToPixelMercator(mc.bounding_rect.max.lat / TEN_POW_7_DBL,
                                mc.bounding_rect.max.lon / TEN_POW_7_DBL);
      TwoPoint tp = {
          .x0 = mc_min.x, .y0 = mc_min.y, .x1 = mc_max.x, .y1 = mc_max.y};
      if (tp.x0 > tp.x1) {
        std::swap(tp.x0, tp.x1);
      }
      if (tp.y0 > tp.y1) {
        std::swap(tp.y0, tp.y1);
      }
      cluster_rects.push_back(tp);
    }
    CHECK_EQ_S(mg.clusters.size(), cluster_rects.size());
  }

  const MMGraph& mg;
  // The bound rectangle for each cluster, with min=(x0,y0) and max=(x1,y1).
  std::vector<TwoPoint> cluster_rects;
};

namespace {

struct PNGContext {
  gdImagePtr im;
  int colors[NUM_COLORS];
  TwoPoint viewport;
  const int zoom;
  int offx;
  int offy;

  PNGContext(int zoom, int tile_x, int tile_y)
      : im(gdImageCreateTrueColor(256, 256)), zoom(zoom) {
    gdImageSaveAlpha(im, 1);
    gdImageAlphaBlending(im, false);
    gdImageFill(im, 0, 0, gdImageColorAllocateAlpha(im, 0, 0, 0, 127));
    gdImageSetThickness(im, 0);
    gdImageRectangle(im, 0, 0, 255, 255,
                     gdImageColorAllocateAlpha(im, 0, 0, 0, 100));
    int thickness = zoom < 9 ? 1 : (zoom < 15 ? 3 : 4);
    gdImageSetThickness(im, thickness);
    // LOG_S(INFO) << "zoom:" << zoom << " thickness:" << thickness;

    colors[RED] = gdImageColorAllocate(im, 255, 0, 0);
    colors[GREEN] = gdImageColorAllocate(im, 0, 255, 0);
    colors[BLUE] = gdImageColorAllocate(im, 0, 0, 255);
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
    colors[MAGENTA] = gdImageColorAllocate(im, 255, 0, 255);
    colors[DPINK] = gdImageColorAllocate(im, 255, 20, 147);
    colors[RED25] = gdImageColorAllocate(im, 255, 196, 196);
    colors[RED50] = gdImageColorAllocate(im, 255, 137, 137);
    colors[RED75] = gdImageColorAllocate(im, 225, 78, 78);

    const WorldPoint tile_wp0 = TileToWorld(zoom, tile_x, tile_y);
    const WorldPoint tile_wp1 = TileToWorld(zoom, tile_x + 1, tile_y + 1);
    // The clipping region of the viewport in world coordinates.
    viewport = {tile_wp0.x, tile_wp0.y, tile_wp1.x, tile_wp1.y};
    const PixelPoint tile_px = ZoomPoint(tile_wp0, zoom);
    offx = tile_px.x;
    offy = tile_px.y;
  }
  ~PNGContext() { gdImageDestroy(im); };

  std::string PNGAsString() {
    int size = 0;
    void* ptr = gdImagePngPtr(im, &size);
    return std::string((char*)ptr, size);
  }
};

using EdgeColorFunc =
    std::function<int(const MMCluster& mc, uint32_t edge_idx)>;

using EdgeSelectFunc = std::function<bool(
    const MMCluster& mc, uint32_t from_idx, uint32_t edge_idx)>;

bool edge_select_all(const MMCluster& mc, uint32_t from_idx,
                     uint32_t edge_idx) {
  return true;
}

std::string CreatePNGInternal(
    const MMTileData& d, int zoom, int tile_x, int tile_y,
    EdgeColorFunc edge_color_func,
    EdgeSelectFunc edge_select_func = edge_select_all) {
  PNGContext pd(zoom, tile_x, tile_y);

  {
    for (uint32_t cluster_id = 0; cluster_id < d.mg.clusters.size();
         ++cluster_id) {
      {
        const TwoPoint& r = d.cluster_rects.at(cluster_id);
        if (r.x0 > pd.viewport.x1 || r.y0 > pd.viewport.y1 ||
            r.x1 < pd.viewport.x0 || r.y1 < pd.viewport.y0) {
          continue;
        }
      }

      const MMCluster& mc = d.mg.clusters.at(cluster_id);
      // Iterate backwards because the more important edges are at the
      // beginning and should be overwriting less important edge from the end.
      for (int32_t node_idx = mc.nodes.size() - 1; node_idx >= 0; --node_idx) {
        const MMLatLon latlon0 = mc.node_to_latlon(node_idx);
        const WorldPoint wp0 = LatLonToPixelMercator(
            latlon0.lat / TEN_POW_7_DBL, latlon0.lon / TEN_POW_7_DBL);
        for (uint32_t edge_idx : mc.edge_indices(node_idx)) {
          if (!edge_select_func(mc, node_idx, edge_idx)) {
            continue;
          }
          const MMLatLon latlon1 =
              mc.node_to_latlon(mc.get_edge(edge_idx).target_idx());
          const WorldPoint wp1 = LatLonToPixelMercator(
              latlon1.lat / TEN_POW_7_DBL, latlon1.lon / TEN_POW_7_DBL);
          TwoPoint line{.x0 = wp0.x, .y0 = wp0.y, .x1 = wp1.x, .y1 = wp1.y};
          if (line.x0 > line.x1) {
            std::swap(line.x0, line.x1);
            std::swap(line.y0, line.y1);
          }
          if (ClipLineCohenSutherland(pd.viewport, &line)) {
            int color = pd.colors[edge_color_func(mc, edge_idx)];
            PixelPoint p0 = ZoomPoint({line.x0, line.y0}, zoom);
            PixelPoint p1 = ZoomPoint({line.x1, line.y1}, zoom);
            // if (p0.x == p1.x && p0.y == p1.y) continue;
            gdImageLine(pd.im, p0.x - pd.offx, p0.y - pd.offy, p1.x - pd.offx,
                        p1.y - pd.offy, color);
          }
        }
      }
    }
  }

  return pd.PNGAsString();
  /*
  int size = 0;
  void* ptr = gdImagePngPtr(pd.im, &size);
  std::string buff((char*)ptr, size);
  gdImageDestroy(pd.im);
  return buff;
  */
}

}  // namespace
#if 0
void test_tiles(double lat, double lon) {
  WorldPoint wp = LatLonToPixelMercator(lat, lon);
  LOG_S(INFO) << absl::StrFormat("From Lat/Lon (%.4f,%.4f) to Merc (%.4f,%.4f)",
                                 lat, lon, wp.x, wp.y);
  double out_lat, out_lon;
  PixelMercatorToLatLon(wp.x, wp.y, &out_lat, &out_lon);
  // pixel_to_latlon(wp.x, wp.y, out_lat, out_lon);
  LOG_S(INFO) << absl::StrFormat("From Merc (%.4f,%.4f) to Lat/Lon (%.4f,%.4f)",
                                 wp.x, wp.y, out_lat, out_lon);
}
#endif

std::string CreatePNG(const MMTileData& d, std::string what, int zoom,
                      int tile_x, int tile_y) {
  if (what == "graph_motorcar") {
    return CreatePNGInternal(d, zoom, tile_x, tile_y,
                             [](const MMCluster& mc, uint32_t edge_idx) -> int {
                               int color = BLUE;
                               if (mc.get_edge(edge_idx).bridge()) {
                                 color = RED;
                               } else if (mc.get_edge(edge_idx).dead_end()) {
                                 color = GREEN;
                               }
                               return color;
                             });
  } else if (what == "clusters") {
    return CreatePNGInternal(d, zoom, tile_x, tile_y,
                             [](const MMCluster& mc, uint32_t edge_idx) -> int {
                               if (mc.get_edge(edge_idx).cross_cluster_edge()) {
                                 return MAGENTA;
                               } else {
                                 return mc.color_no % NUM_COLORS;
                               }
                             });
  } else if (what == "restricted") {
    return CreatePNGInternal(
        d, zoom, tile_x, tile_y,
        [](const MMCluster& mc, uint32_t edge_idx) -> int { return GBLUE; },
        [](const MMCluster& mc, uint32_t from_idx, uint32_t edge_idx) -> bool {
          return mc.get_edge(edge_idx).restricted();
        });
  } else {
    LOG_S(INFO) << "not supported: " << what;
    return "";
  }
}
void DrawLineInternal(PNGContext& pd, const MMLatLon& latlon0,
                      const MMLatLon& latlon1, TileColor line_color) {
  const WorldPoint wp0 = LatLonToPixelMercator(latlon0.lat / TEN_POW_7_DBL,
                                               latlon0.lon / TEN_POW_7_DBL);
  const WorldPoint wp1 = LatLonToPixelMercator(latlon1.lat / TEN_POW_7_DBL,
                                               latlon1.lon / TEN_POW_7_DBL);
  TwoPoint line{.x0 = wp0.x, .y0 = wp0.y, .x1 = wp1.x, .y1 = wp1.y};
  if (line.x0 > line.x1) {
    std::swap(line.x0, line.x1);
    std::swap(line.y0, line.y1);
  }
  if (ClipLineCohenSutherland(pd.viewport, &line)) {
    int color = pd.colors[line_color];
    PixelPoint p0 = ZoomPoint({line.x0, line.y0}, pd.zoom);
    PixelPoint p1 = ZoomPoint({line.x1, line.y1}, pd.zoom);
    gdImageLine(pd.im, p0.x - pd.offx, p0.y - pd.offy, p1.x - pd.offx,
                p1.y - pd.offy, color);
  }
}

void DrawClusterRouter(PNGContext& pd, const MMHybridRouter::RouterData& rd,
                       MMHybridRouter::RouterType source) {
  const MMClusterRouter& r = *rd.router[source];
  const std::vector<MMClusterRouter::VisitedEdge>& vec = r.GetVisitedEdges();
  const MMCluster& mc = r.mc();
  TileColor color = (TileColor)(mc.color_no % NUM_COLORS);
  for (uint32_t v_idx = 0; v_idx < vec.size(); ++v_idx) {
    const MMClusterRouter::VisitedEdge vis = vec.at(v_idx);
    if (vis.min_metric == INFU32) {
      continue;
    }
    const uint32_t edge_idx = r.GetGraphEdgeIdx(v_idx);

    // Find out at which node this edge starts.
    uint32_t from_node_idx = INFU32;
    if (vis.from_v_idx == INFU32) {
      // Check if it is a start edge. In this case we can find out the
      // starting node.
      const GeoAnchor& starta = r.GetStartAnchor();
      const uint32_t start_edge_pos = starta.FindPosByEdgeIdx(mc, edge_idx);
      if (start_edge_pos != INFU32) {
        from_node_idx =
            starta.edge_points().at(start_edge_pos).fe.from_node_idx;
      }
    } else {
      uint32_t prev_edge_idx = r.GetGraphEdgeIdx(vis.from_v_idx);
      from_node_idx = mc.get_edge(prev_edge_idx).target_idx();
    }

    if (from_node_idx != INFU32) {
      const MMLatLon latlon0 = mc.node_to_latlon(from_node_idx);
      const MMLatLon latlon1 =
          mc.node_to_latlon(mc.get_edge(edge_idx).target_idx());
      /*
      TileColor color = source == MMHybridRouter::START
                            ? (vis.done ? GREEN : LGREEN)
                            : (vis.done ? BLUE : LBLUE);
      */
      DrawLineInternal(
          pd, latlon0, latlon1,
          mc.get_edge(edge_idx).cross_cluster_edge() ? MAGENTA : color);
    }
  }
}

std::string CreatePNGForHybridRouting(const MMGraph& mg,
                                      const MMHybridRouter::RouterData& rd,
                                      int zoom, int tile_x, int tile_y) {
  if (rd.router[MMHybridRouter::START].get() == nullptr) return "";
  PNGContext pd(zoom, tile_x, tile_y);

  for (const auto& [key, hvis] : rd.hybrid_map) {
    const MMOutgoingEdge& out_edge =
        MMHybridRouter::out_edge_from_hybrid_key(mg, key);
    const MMCluster& mc = mg.mc(out_edge.from_cluster_id);
    const TileColor color =
        hvis.done ? (TileColor)(mc.color_no % NUM_COLORS) : GREY;
    const MMLatLon latlon0 = mc.node_to_latlon(out_edge.from_node_idx);
    const MMLatLon latlon1 = mc.node_to_latlon(out_edge.to_node_idx);
    DrawLineInternal(pd, latlon0, latlon1, MAGENTA);
    MMLatLon prev_latlon;
    if (hvis.prev_source == MMHybridRouter::HYBRID) {
      const MMOutgoingEdge& prev_out_edge =
          MMHybridRouter::out_edge_from_hybrid_key(mg, hvis.prev_key_or_v_idx);
      const MMCluster& prev_mc = mg.mc(prev_out_edge.from_cluster_id);
      prev_latlon = prev_mc.node_to_latlon(prev_out_edge.to_node_idx);
    } else {
      CHECK_S(hvis.prev_source == MMHybridRouter::START ||
              hvis.prev_source == MMHybridRouter::TARGET);
      uint32_t edge_idx =
          rd.router[hvis.prev_source]->GetGraphEdgeIdx(hvis.prev_key_or_v_idx);
      const MMCluster& prev_mc = rd.mcw[hvis.prev_source]->mc;
      prev_latlon =
          prev_mc.node_to_latlon(prev_mc.get_edge(edge_idx).target_idx());
    }
    DrawLineInternal(pd, prev_latlon, latlon0, color);
  }

  DrawClusterRouter(pd, rd, MMHybridRouter::START);
  if (rd.router[MMHybridRouter::START].get() !=
      rd.router[MMHybridRouter::TARGET].get()) {
    DrawClusterRouter(pd, rd, MMHybridRouter::TARGET);
  }

  return pd.PNGAsString();
}
