#pragma once
#include <stdio.h>

#include <algorithm>
#include <cstdlib>
#include <string_view>
#include <unordered_map>

#include "base/country_code.h"
#include "geometry/fast_country_polygons.h"
#include "geometry/line_clipping.h"
#include "logging/loguru.h"

// Lookup the country code for a point in lon/lat coordinates.
// Uses precomputed tiles to answer queries fast. Only when the tile touches
// multiple countries it uses an underlying FastCountryPolygons instance to
// determine the country of the given point.
class TiledCountryLookup {
 public:
  // Units that incoming coordinates must have. This is 100 nanodegrees.
  static constexpr int32_t kDegreeUnits = 10000000;

  // Size of a tile in the lat/lon coordinate system using kDegreeUnits.
  // The default size 1 << 20 is roughly 10^6, i.e. 1/10 degrees.
  const int32_t tile_size_;

  TiledCountryLookup(std::string_view wildcard, int32_t tile_size = 1 << 20)
      : tile_size_(tile_size) {
    // Large tile sizes create an overflow in the 32 bit integers representing
    // lon.
    CHECK_LE_S(tile_size_, kDegreeUnits * 10);
    fast_contains_.LoadPolygonsFromWildcard(wildcard);
    fast_contains_.PrepareData();
    InitTiles();
  }

  TiledCountryLookup(const FastCountryPolygons& p, int32_t tile_size = 1 << 20)
      : tile_size_(tile_size) {
    // Large tile sizes create an overflow in the 32 bit integers representing
    // lon.
    CHECK_LE_S(tile_size_, kDegreeUnits * 10);
    fast_contains_ = p;
    fast_contains_.PrepareData();
    InitTiles();
  }

  // Get country code (or 0) for a point in lon/lat coordinates. The coordinates
  // are in 'kDegreeUnits', see above.
  uint16_t GetCountryNum(const int32_t p_x, const int32_t p_y,
                         const int64_t debug_id = 0) const {
    auto it = tile_to_country_.find(TileKey(p_x, p_y));
    if (it == tile_to_country_.end()) {
      return 0;
    } else if (it->second > 0) {
      return it->second;
    }
    return fast_contains_.GetCountryNum(p_x, p_y);
  }

 private:
  int32_t TileOrigin(int32_t coord) const {
    // In C++, -11/10 is -1.
    // In Python, -11//10 is -2.  (// forces integer division).
    // The tile origin should be <= actual coordinate. But with C++ arithmetic
    // this does not work as in Python.
    if (coord >= 0) {
      return (coord / tile_size_) * tile_size_;
    } else {
      return ((coord - tile_size_ + 1) / tile_size_) * tile_size_;
    }
  }

  uint64_t TileKey(int32_t lon, int32_t lat) const {
    int64_t norm_lon = 180ll * kDegreeUnits + TileOrigin(lon);
    int64_t norm_lat = 180ll * kDegreeUnits + TileOrigin(lat);
    CHECK_GE_S(norm_lon, 0);
    CHECK_GE_S(norm_lat, 0);
    return ((static_cast<uint64_t>(norm_lon) / tile_size_) << 32) +
           (static_cast<uint64_t>(norm_lat) / tile_size_);
  }

  static bool LineIntersectsTile(const FastCountryPolygons::Line& line,
                                 const TwoPoint& tile_rect) {
    TwoPoint tp_line = {.x0 = static_cast<double>(line.x0),
                        .y0 = static_cast<double>(line.y0),
                        .x1 = static_cast<double>(line.x1),
                        .y1 = static_cast<double>(line.y1)};
    return ClipLineCohenSutherland(tile_rect, &tp_line);
  }

  void InitTiles() {
    FuncTimer timer("TiledCountryLookup::InitTiles()");

    // Iterate over all country polygon line segments, and for each line mark
    // all tiles that the line segment crosses as "mixed", by setting the tile
    // country to 0.
    int64_t num_lines = 0;
    for (const auto& line_vector : fast_contains_.get_line_vectors()) {
      for (const FastCountryPolygons::Line& line : line_vector) {
        num_lines++;
        // Use bounding rect of line to compute affected tiles.
        int32_t tile_x0 = TileOrigin(line.x0);
        int32_t tile_x1 = TileOrigin(line.x1);
        int32_t tile_y0 = TileOrigin(line.y0);
        int32_t tile_y1 = TileOrigin(line.y1);

        if (tile_x0 > tile_x1) std::swap(tile_x0, tile_x1);
        if (tile_y0 > tile_y1) std::swap(tile_y0, tile_y1);
        TwoPoint tile_rect;
        for (int32_t tile_x = tile_x0; tile_x <= tile_x1;
             tile_x += tile_size_) {
          tile_rect.x0 = tile_x;
          tile_rect.x1 = tile_x + tile_size_;
          for (int32_t tile_y = tile_y0; tile_y <= tile_y1;
               tile_y += tile_size_) {
            if (tile_to_country_.contains(TileKey(tile_x, tile_y))) {
              // Tile is already known to be mixed.
              continue;
            }
            tile_rect.y0 = tile_y;
            tile_rect.y1 = tile_y + tile_size_;
            if (LineIntersectsTile(line, tile_rect)) {
              tile_to_country_[TileKey(tile_x, tile_y)] = 0;
            }
          }
        }
      }
    }

    // For all tiles that are not marked as mixed, precompute the country that
    // belongs to the tile.
    int64_t num_mixed_country = tile_to_country_.size();
    int64_t num_tiles = 0;
    int64_t num_with_country = 0;
    int64_t num_with_no_country = 0;
    for (int32_t lon = -180 * kDegreeUnits; lon <= 180 * kDegreeUnits;
         lon += tile_size_) {
      int32_t tile_x = TileOrigin(lon);
      for (int32_t lat = -90 * kDegreeUnits; lat <= 90 * kDegreeUnits;
           lat += tile_size_) {
        num_tiles++;
        int32_t tile_y = TileOrigin(lat);
        if (!tile_to_country_.contains(TileKey(tile_x, tile_y))) {
          uint16_t country_num = fast_contains_.GetCountryNum(lon, lat);
          if (country_num > 0) {
            tile_to_country_[TileKey(lon, lat)] = country_num;
            num_with_country++;
          } else {
            num_with_no_country++;
          }
        }
      }
    }
    LOG_S(INFO) << "#tiles:" << num_tiles
                << " #total entries:" << tile_to_country_.size()
                << " #with country:" << num_with_country
                << " #mixed country:" << num_mixed_country
                << " #with no country:" << num_with_no_country;
  }

  FastCountryPolygons fast_contains_;
  // Maps the tile key to the country. There are three cases:
  // 1) Tile key doesn't exist: No country assigned to this tile.
  // 2) Value is 0: mixed results in the tile, evaluate fully.
  // 3) Value is > 0: All points in the tile have the same country (the value).
  std::unordered_map<uint64_t, uint16_t> tile_to_country_;
};
