#pragma once

#include <string_view>

#include "graph/graph_def.h"

class RoutingMetric {
 public:
  virtual int32_t Compute(const WaySharedAttrs& wsa, VEHICLE vt,
                          const DIRECTION dir, const GEdge& edge) const = 0;
  virtual std::string_view Name() const = 0;
};

class RoutingMetricDistance : public RoutingMetric {
 public:
  inline int32_t Compute(const WaySharedAttrs& wsa, VEHICLE vt,
                         const DIRECTION dir,
                         const GEdge& edge) const override final {
    return edge.distance_cm;
  }

  std::string_view Name() const override final { return "distance(cm)"; }
};

class RoutingMetricTime : public RoutingMetric {
 public:
  inline int32_t Compute(const WaySharedAttrs& wsa, VEHICLE vt,
                         const DIRECTION dir,
                         const GEdge& edge) const override final {
    uint32_t km_per_hour = GetRAFromWSA(wsa, vt, dir).maxspeed;
    CHECK_GT_S(km_per_hour, 0)
        << RoutingAttrsDebugString(GetRAFromWSA(wsa, vt, dir));
    // Compute how long it takes in milliseconds.
    return (36ull * edge.distance_cm) / km_per_hour;
  }

  std::string_view Name() const override final { return "time(ms)"; }
};
