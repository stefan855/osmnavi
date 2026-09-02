#pragma once

#include <string_view>

#include "graph/graph_def.h"
#include "graph/turn_costs.h"

class RoutingMetric {
 public:
  constexpr virtual uint32_t Compute(
      const WaySharedAttrs& wsa, VEHICLE vt, const DIRECTION dir,
      DistanceType edge_distance, double speed_fraction = 1.0,
      DurationMS turn_cost = TURN_COST_ZERO) const = 0;
  constexpr virtual std::string_view Name() const = 0;
  constexpr virtual bool IsTimeMetric() const { return false; }
};

class RoutingMetricDistance : public RoutingMetric {
 public:
  constexpr inline uint32_t Compute(
      const WaySharedAttrs& wsa, VEHICLE vt, const DIRECTION dir,
      DistanceType edge_distance, double speed_fraction = 1.0,
      DurationMS turn_cost = TURN_COST_ZERO) const override final {
    return edge_distance.cm();
  }

  constexpr std::string_view Name() const override final {
    return "distance(cm)";
  }
};

class RoutingMetricTime : public RoutingMetric {
 public:
  constexpr inline uint32_t Compute(
      const WaySharedAttrs& wsa, VEHICLE vt, const DIRECTION dir,
      DistanceType edge_distance, double speed_fraction = 1.0,
      DurationMS turn_cost = TURN_COST_ZERO) const override final {
    double km_per_hour = GetRAFromWSA(wsa, vt, dir).maxspeed * speed_fraction;
    CHECK_GT_S(km_per_hour, 0.0)
        << RoutingAttrsDebugString(GetRAFromWSA(wsa, vt, dir))
        << " speed fraction:" << speed_fraction;
    // Compute how long it takes in milliseconds.
    return ((36ull * edge_distance.cm()) / km_per_hour) + turn_cost.ms();
  }

  constexpr std::string_view Name() const override final { return "time(ms)"; }
  constexpr virtual bool IsTimeMetric() const { return true; }
};
