#pragma once

#include <string_view>

#include "graph/graph_def.h"
#include "graph/turn_costs.h"

class RoutingMetric {
 public:
  virtual uint32_t Compute(
      const WaySharedAttrs& wsa, VEHICLE vt, const DIRECTION dir,
      const GEdge& edge,
      uint32_t compressed_turn_cost = TURN_COST_ZERO_COMPRESSED) const = 0;
  virtual std::string_view Name() const = 0;
};

class RoutingMetricDistance : public RoutingMetric {
 public:
  inline uint32_t Compute(const WaySharedAttrs& wsa, VEHICLE vt,
                          const DIRECTION dir, const GEdge& edge,
                          uint32_t compressed_turn_cost =
                              TURN_COST_ZERO_COMPRESSED) const override final {
    return edge.distance_cm;
  }

  std::string_view Name() const override final { return "distance(cm)"; }
};

class RoutingMetricTime : public RoutingMetric {
 public:
  inline uint32_t Compute(const WaySharedAttrs& wsa, VEHICLE vt,
                          const DIRECTION dir, const GEdge& edge,
                          uint32_t compressed_turn_cost =
                              TURN_COST_ZERO_COMPRESSED) const override final {
    uint32_t km_per_hour = GetRAFromWSA(wsa, vt, dir).maxspeed;
    CHECK_GT_S(km_per_hour, 0)
        << RoutingAttrsDebugString(GetRAFromWSA(wsa, vt, dir));
    // Compute how long it takes in milliseconds.
    return ((36ull * edge.distance_cm) / km_per_hour) +
           decompress_turn_cost(compressed_turn_cost) * 100;
  }

  std::string_view Name() const override final { return "time(ms)"; }
};
