#pragma once

#include <string_view>

#include "graph/graph_def.h"

class RoutingMetric {
 public:
  virtual int32_t Compute(const GWay& way, const GEdge& edge) const = 0;
  virtual std::string_view Name() const = 0;
};

class RoutingMetricDistance : public RoutingMetric {
 public:
  int32_t Compute(const GWay& way, const GEdge& edge) const override final {
    return edge.distance_cm;
  }

  std::string_view Name() const override final { return "Distance(cm)"; }
};

class RoutingMetricTime : public RoutingMetric {
 public:
  int32_t Compute(const GWay& way, const GEdge& edge) const override final {
    uint32_t km_per_hour = way.ri[edge.contra_way].maxspeed;
    if (km_per_hour == 0) {
      km_per_hour = 50u;
      if (way.highway_label == HW_MOTORWAY ||
          way.highway_label == HW_MOTORWAY_LINK) {
        km_per_hour = 100u;
      }
    }
    // Compute how long it takes in milliseconds.
    return (36ull * edge.distance_cm) / km_per_hour;
  }

  std::string_view Name() const override final { return "Time(ms)"; }
};
