#pragma once

#include <span>
#include <vector>

#include "base/deg_coord.h"
// #include "base/mmap_base.h"
#include "base/util.h"
#include "base/varbyte.h"

namespace {
// Manages lat/lon and deltas that we have seen for the previous point.
// Predicts the current delta.
struct ShapeCoordOracle {
  ShapeCoordOracle(LatE6 lat, LonE6 lon)
      : lat(lat), lon(lon), dlat(0), dlon(0) {}
  LatE6 lat;
  LonE6 lon;
  int64_t dlat;
  int64_t dlon;

  // Simply predicts that the new dlat is the same as the old one/
  int64_t predict_new_lat() const { return lat.v64() + dlat; }

  // Given a new dlat, estimate the new dlon.
  int64_t predict_new_lon(LatE6 new_lat) const {
    if (std::abs(dlat) < 30) {
      return lon.v64() + dlon;
    }

    double factor = static_cast<double>(new_lat.v64() - lat.v64()) /
                    static_cast<double>(dlat);

    return std::lround(lon.v64() + factor * dlon);
    // return lon.v64() + dlon;
  }

  void push_new(LatE6 new_lat, LonE6 new_lon) {
    dlat = new_lat.v64() - lat.v64();
    dlon = new_lon.v64() - lon.v64();
    lat = new_lat;
    lon = new_lon;
  }
};
}  // namespace

// 'base' is the latlon of the start node of the edge.
// 'latlon' contains the shape coordinates of an edge for encoding, excluding
// the start- and end-node of the edge. Does forward encoding, using the first
// element to form deltas and ignoring the last element.
inline void EncodeShapeCoords(const LatLon base,
                              std::span<const LatLon> latlon,
                              WriteBuff* buff) {
  static size_t counter = 0;
  const bool debug = (++counter % 50000 == 0);

  CHECK_GT_S(latlon.size(), 0);
  ShapeCoordOracle oracle(base.lat, base.lon);

  if (debug) {
    LOG_S(INFO) << "Encode shape coord list len:" << latlon.size();
  }

  for (size_t i = 0; i < latlon.size(); ++i) {
    LatLon curr = latlon[i];
    DeltaEncodeInt64(oracle.predict_new_lat(), curr.lat.v64(), buff);
    DeltaEncodeInt64(oracle.predict_new_lon(curr.lat), curr.lon.v64(), buff);

    if (debug) {
      LOG_S(INFO) << absl::StrFormat(
          "shape coords pos:%3lu coord:(%ld,%ld) delta:(%5ld,%5ld) "
          "odelta:(%5ld,%5ld)",
          i, curr.lat.v64(), curr.lon.v64(), curr.lat.v64() - oracle.lat.v64(),
          curr.lon.v64() - oracle.lon.v64(),
          curr.lat.v64() - oracle.predict_new_lat(),
          curr.lon.v64() - oracle.predict_new_lon(curr.lat));
    }

    oracle.push_new(curr.lat, curr.lon);
  }
}

// 'base' is the latlon of the start node of the edge.
// 'latlon' contains the shape coordinates of an edge for encoding, excluding
// the start- and end-node of the edge. Does forward encoding, using the first
// element to form deltas and ignoring the last element.
inline uint32_t DecodeShapeCoords(const uint8_t* ptr, uint32_t num_coords,
                                  const LatLon base,
                                  std::vector<LatLon>* latlon) {
  latlon->clear();
  ShapeCoordOracle oracle(base.lat, base.lon);
  uint32_t cnt = 0;
  while (num_coords-- > 0) {
    int64_t lat;
    int64_t lon;
    cnt += DeltaDecodeInt64(ptr + cnt, oracle.predict_new_lat(), &lat);
    cnt +=
        DeltaDecodeInt64(ptr + cnt, oracle.predict_new_lon(LatE6(lat)), &lon);
    latlon->emplace_back(LatE6(lat), LonE6(lon));
    oracle.push_new(latlon->back().lat, latlon->back().lon);
  }
  return cnt;
}
