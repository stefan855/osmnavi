#pragma once

#include <vector>

#include "geometry/distance.h"
#include "graph/graph_def.h"
#include "graph/graph_def_utils.h"

#if 0
// For an edge in the graph, this data describes the turn costs when arriving
// at the target node of the edge and continuing on an edge starting at the
// target node (i.e. an outgoing edge).
//
// Note that the dimension of the 'turn_costs' vector of edge e must be equal
// the number of outgoing edges at the target node of e.
//
// A special value indicates that a turn is not possible. This means that a turn
// restriction forbids the turn, a barrier disallows passing the target node, a
// u-turn is not allowed or something else.
struct TurnCostData {
  // TODO: Use a more memory-efficient data structure than a vector.
  std::vector<uint8_t> turn_costs;

  bool operator==(const TurnCostData& other) const {
    return turn_costs == other.turn_costs;
  }
};
#endif

constexpr uint32_t TURN_COST_INFINITY = 1'000'000'000u;  // ~11.5 days
constexpr uint32_t TURN_COST_ZERO = 0;
constexpr uint32_t TURN_COST_U_TURN = 20000;

namespace {
// turn costs in milliseconds are compressed into 63 buckets, which
// represent the following values.
static constexpr size_t compressed_turn_cost_values_dim = 64;
static constexpr uint32_t
    compressed_turn_cost_values[compressed_turn_cost_values_dim] = {
        0,        100,      200,      300,
        400,      500,      600,      700,
        800,      900,      1000,     1100,
        1200,     1300,     1400,     1500,
        1600,     1900,     2200,     2500,
        2800,     3300,     3800,     4300,
        5000,     5700,     6600,     7600,
        8700,     10000,    11500,    13200,
        17200,    22400,    29100,    37800,
        49200,    63900,    83100,    108000,
        140400,   182600,   237300,   308500,
        401100,   521400,   677800,   881200,
        1145500,  1489200,  1935900,  2516700,
        3271700,  4253200,  5529200,  7188000,
        9344300,  12147700, 15791900, 20529500,
        26688400, 34694900, 45103400, TURN_COST_INFINITY};

};  // namespace

// Compress a turn cost value (milliseconds) to compressed format
// using 6 bits. Note that all values > 45103400 are compressed to INF and are
// treated as non-routable.
inline constexpr uint32_t compress_turn_cost(uint32_t cost) {
  if (cost == 0) return 0;
  // std::lower_bound finds the first element >= cost.
  size_t pos =
      std::lower_bound(
          compressed_turn_cost_values,
          compressed_turn_cost_values + compressed_turn_cost_values_dim, cost) -
      compressed_turn_cost_values;
  if (pos < compressed_turn_cost_values_dim - 1) {
    assert(pos > 0);
    // Note that by construction both values are positive.
    const auto diff = compressed_turn_cost_values[pos] - cost;
    const auto diff_prev = cost - compressed_turn_cost_values[pos - 1];
    if (diff >= diff_prev) {
      return pos - 1;
    } else {
      return pos;
    }
  } else {
    return compressed_turn_cost_values_dim - 1;
  }
}

constexpr uint32_t TURN_COST_ZERO_COMPRESSED =
    compress_turn_cost(TURN_COST_ZERO);
constexpr uint32_t TURN_COST_INFINITY_COMPRESSED =
    compress_turn_cost(TURN_COST_INFINITY);
static_assert(TURN_COST_ZERO_COMPRESSED == 0);
static_assert(TURN_COST_INFINITY_COMPRESSED ==
              compressed_turn_cost_values_dim - 1);

inline constexpr uint32_t decompress_turn_cost(uint32_t compressed_cost) {
  if (compressed_cost == 0) return 0;
  CHECK_LT_S(compressed_cost, compressed_turn_cost_values_dim);
  return compressed_turn_cost_values[compressed_cost];
}

#if 0
// Classify a turn based on an entry edge and an exit edge.
enum TURN_TYPE : uint8_t {
  TT_STRAIGHT,
  TT_CURVE,
  TT_STOP_TO_LEFT,
  TT_LOWER_TO_LEFT,
  TT_HIGHER_TO_LEFT,
  TT_MAX
};

struct TurnClassification {
  TURN_TYPE tt;
};

inline TURN_TYPE ClassifyTurn(const Graph& g, VEHICLE vh, const GEdge& e1,
                       const GEdge& e2, int32_t turning_angle) {
  return TT_CURVE;
}
#endif

// Return the maximal velocity (km/h) in a curve for a typical car.
// arc_length_cm: The distance for which we have a turning_angle. Typically this
//                is 1/2 of the length of the incoming edge plus 1/2 of the
//                length of the outgoing edge.
// turning_angle: angle change in degrees between incoming and outgoing edge.
//                0 degrees indicate a straight continuation, 180 degrees
//                indicate a u-turn.
//
// See https://www.lernhelfer.de/schuelerlexikon/physik/artikel/kurvenfahrten
// Formula for max. v through curve with length s and angle-change a:
//           v-max: 3.6 * sqrt(u * g * s / (a * pi / 180))
//           u:     coefficient of static friction (Haftreibungskoeffizient)
//                  depends on conditions (rain etc.) surface and more.
//                  Range for cars is 0.4..0.8 (on asphalt roads).
//                  We use 0.5, which is on the conservative side.
//           g:     9.81
//           s:     arc length (Bogenlänge)
//           a:     angle change in degrees
// Example for a curve with length 100m and 90 degree change in direction:
//   v-max = 63.615 km/h = 3.6 * math.sqrt(0.5*9.81*100/(90*math.pi/180))
inline double MaxCurveVelocity(uint32_t arc_length_cm, int32_t turning_angle) {
  return std::sqrt((3.6 * 3.6 * 0.5 * 9.81 * 0.01 * arc_length_cm) /
                   (turning_angle * std::numbers::pi / 180.0));
}

// This computes the distance (cm) for accelerating or decelerating a vehicle
// from speed1_kmh to speed2_kmh.
//
// Uses the following formula for speed changes:
//     dist = (v1^2 - v2^2) / (2 * a).
inline uint32_t DistanceForSpeedChange(VEHICLE vh, double speed1_kmh,
                                       double speed2_kmh) {
  // Rough assumptions about vehicle specific acceleration/deceleration
  // constants.
  // TODO: Maybe this should be moved to a config file.
  constexpr double acc_car = 3;
  constexpr double dec_car = 2;
  constexpr double acc_hgv = 2;
  constexpr double dec_hgv = 1.5;
  constexpr double acc_bicycle = 1.5;
  constexpr double dec_bicycle = 1.5;

  const double sp1 = speed1_kmh / 3.6;
  const double sp2 = speed2_kmh / 3.6;
  double a;
  switch (vh) {
    case VH_MOTORCAR:
    case VH_MOTORCYCLE:
      a = (sp1 >= sp2) ? dec_car : acc_car;
      break;
    case VH_BICYCLE:
    case VH_MOPED:
    case VH_HORSE:
      a = (sp1 >= sp2) ? dec_bicycle : acc_bicycle;
      break;
    case VH_PSV:
    case VH_BUS:
    case VH_HGV:
      a = (sp1 >= sp2) ? dec_hgv : acc_hgv;
      break;
    default:
      ABORT_S() << vh;
  }

  return std::abs(std::lround(100 * (sp1 * sp1 - sp2 * sp2) / (2 * a)));
}

// Compute the time loss in milliseconds that is caused by a curve that
// doesn't allow max speed. The time loss is computed for the full distance of
// edge 'e'.
inline uint32_t TimeLossFromCurve(const Graph& g, VEHICLE vh, const GEdge& e,
                                  double curve_speed) {
  const double max_speed =
      GetRAFromWSA(GetWSA(g, e.way_idx), vh, EDGE_DIR(e)).maxspeed;
  if (curve_speed >= max_speed || e.distance_cm == 0) {
    return 0;
  }
  double time_max_speed_ms = e.distance_cm / (max_speed * 100.0 / 3.6);
  double time_curve_speed_ms = e.distance_cm / (curve_speed * 100.0 / 3.6);

  return std::max(0l, std::lround(time_curve_speed_ms - time_max_speed_ms));
}

namespace {

// Compute the time loss in milliseconds of a turn from edge e1 to e2. A smaller
// angle has higher cost.
uint32_t TurnAngleTimeLoss(const Graph& g, VEHICLE vh, const GEdge& e1,
                           const GEdge& e2, int32_t turning_angle) {
  turning_angle = std::labs(turning_angle);
  if (turning_angle <= 20) {
    return 0;
  } else if (turning_angle <= 60) {
    return 500;
  } else if (turning_angle <= 120) {
    return 2000;
  } else if (turning_angle <= 179) {
    return 4000;
  } else {
    // u-turn.
    return TURN_COST_U_TURN;
  }
}

// TODO: handle direction of blockage.
bool VehicleBlockedAtNode(VEHICLE vh, const NodeTags* node_tags) {
  return (node_tags != nullptr && node_tags->has_barrier &&
          (node_tags->vh_acc[vh] <= ACC_PRIVATE ||
           node_tags->vh_acc[vh] >= ACC_MAX));
}

// Is the U-Turn represented by 'n3p' allowed?
//
// The code checks for various situations, including when a vehicle would be
// trapped at the target node with no way to continue the travel, which is true
// at the end of a street or when facing a restricted access area.
//
// TODO: handle vehicle types properly.
inline bool IsUTurnAllowed(const Graph& g, VEHICLE vh,
                           const NodeTags* node_tags, const N3Path& n3p) {
  CHECK_EQ_S(n3p.node0_idx, n3p.node2_idx);

  const GEdge& edge0 = n3p.edge0(g);
  const GWay& way0 = g.ways.at(edge0.way_idx);

  // TODO: Is it clear that TRUNK and higher should have no automatic u-turns?
  if (way0.highway_label <= HW_TRUNK_LINK) {
    return false;
  }

  // Special case: Way is an area and both edges are on this way.
  if (way0.area && edge0.way_idx == n3p.edge1(g).way_idx) {
    return true;
  }

  // Special case, vehicle is blocked at node and returns on the same way.
  if (VehicleBlockedAtNode(vh, node_tags) &&
      edge0.way_idx == n3p.edge1(g).way_idx) {
    return true;
  }

  // No check which kind of continuation edges there are.
  bool found_continuation = false;
  bool found_free_continuation = false;
  for (const GEdge& out : gnode_forward_edges(g, edge0.target_idx)) {
    if (out.target_idx != n3p.node0_idx && out.target_idx != edge0.target_idx) {
      found_continuation = true;
      found_free_continuation |= (out.car_label == GEdge::LABEL_FREE);
    }
  }

  if (!found_continuation) {
    return true;
  }
  if (edge0.car_label != GEdge::LABEL_FREE && !found_free_continuation) {
    return true;
  }
  return false;
}

inline void ProcessSimpleTurnRestrictions(
    const Graph& g, const IndexedTurnRestrictions& indexed_trs, VEHICLE vh,
    uint32_t start_node_idx, uint32_t edge_idx, TurnCostData* tcd) {
  const GEdge& e = g.edges.at(edge_idx);
  const TurnRestriction::TREdge trigger_key = {.from_node_idx = start_node_idx,
                                               .way_idx = e.way_idx,
                                               .to_node_idx = e.target_idx};
  auto iter = indexed_trs.map_to_first.find(trigger_key);
  if (iter == indexed_trs.map_to_first.end()) {
    return;
  }
  CHECK_S(indexed_trs.sorted_trs.at(iter->second).GetTriggerKey() ==
          trigger_key);

  const GNode& via_node = g.nodes.at(e.target_idx);
  const uint32_t num_edges = via_node.num_forward_edges;
  CHECK_EQ_S(num_edges, tcd->turn_costs.size())
      << GetGNodeIdSafe(g, e.target_idx);

  for (size_t trs_pos = iter->second;
       trs_pos < indexed_trs.sorted_trs.size() &&
       indexed_trs.sorted_trs.at(trs_pos).GetTriggerKey() == trigger_key;
       ++trs_pos) {
    const TurnRestriction& tr = indexed_trs.sorted_trs.at(trs_pos);
    CHECK_EQ_S(tr.path.size(), 2) << tr.relation_id;

    bool found = false;
    for (size_t offset = 0; offset < num_edges; ++offset) {
      const GEdge& e = g.edges.at(via_node.edges_start_pos + offset);
      CHECK_S(!e.inverted) << tr.relation_id;

      // Does the edge match the second leg of the turn restriction?
      if (e.way_idx == tr.path.back().way_idx &&
          e.target_idx == tr.path.back().to_node_idx) {
        found = true;
        if (tr.forbidden) {
          // Remove the bit belonging to the edge.
          tcd->turn_costs.at(offset) = TURN_COST_INFINITY_COMPRESSED;
        } else {
          for (size_t offset2 = 0; offset2 < num_edges; ++offset2) {
            tcd->turn_costs.at(offset) =
                (offset2 == offset ? TURN_COST_ZERO_COMPRESSED
                                   : TURN_COST_INFINITY_COMPRESSED);
          }
        }
      }
    }
    if (!found) {
      LOG_S(INFO) << absl::StrFormat(
          "Warning, no match found for turn restriction %lld", tr.relation_id);
    }
  }
}

// Check if there is a turn restrictions matching the first edge in n3p.
//
// Return TRStatus::EMPTY if no turn restriction was found, or the TRStatus of
// the second edge in n3p.
inline TRStatus CheckSimpleTurnRestriction(
    const Graph& g, const IndexedTurnRestrictions& indexed_trs,
    const N3Path& n3p) {
  TRStatus result = TRStatus::EMPTY;
  const GEdge& e0 = n3p.edge0(g);
  const std::span<const TurnRestriction> trs =
      indexed_trs.FindTurnRestrictions({.from_node_idx = n3p.node0_idx,
                                        .way_idx = e0.way_idx,
                                        .to_node_idx = e0.target_idx});
  if (trs.empty()) return result;
  const GEdge& e1 = n3p.edge1(g);
  for (const TurnRestriction& tr : trs) {
    CHECK_EQ_S(tr.path.size(), 2) << tr.relation_id;
    const bool matches_edge = (e1.way_idx == tr.path.back().way_idx &&
                               e1.target_idx == tr.path.back().to_node_idx);
    if (tr.forbidden == matches_edge) {
      result = TRStatus::FORBIDDEN;
    } else {
      result = TRStatus::ALLOWED;
    }
  }
  if (result == TRStatus::EMPTY) {
    LOG_S(INFO) << absl::StrFormat(
        "Warning, no match found for turn restriction %lld",
        trs.front().relation_id);
  }
  return result;
}

// Compute costs for obstacles that are not blocking but "cost" time.
uint32_t NodeTagsCost(const Graph& g, N3Path n3p) {
  uint32_t cost = 0;  // unit is millisecond

  const NodeTags* attr = g.FindNodeTags(n3p.node1(g).node_id);
  if (attr != nullptr) {
    // TODO: handle direction
    if (attr->bit_stop) {
      cost += 3'000;
    }
    // TODO: handle direction
    if (attr->bit_traffic_signals) {
      cost += 20'000;
    }
    if (attr->bit_crossing) {
      cost += 3'000;
    }
    if (attr->bit_railway_crossing) {
      if (attr->bit_railway_crossing_barrier) {
        // Check if the previous node is closer than 50m and already had a
        // railway barrier. If so, then discount the current "barrier", it
        // probably doesn't exist.
        // See https://www.openstreetmap.org/node/103007646
        const NodeTags* attr_prev = g.FindNodeTags(n3p.node0(g).node_id);
        if (attr_prev != nullptr && attr_prev->bit_railway_crossing &&
            attr_prev->bit_railway_crossing_barrier &&
            n3p.edge0(g).distance_cm < 5000) {
          cost += 500;
        } else {
          // TODO: Maybe guess how busy the railway is?
          cost += 20'000;
        }
      } else {
        cost += 500;
      }
    }
    if (attr->bit_traffic_calming) {
      cost += 1'000;
    }
  }
  return cost;
}

uint32_t CurveCost(const Graph& g, VEHICLE vh, const N3Path& n3p) {
  const GNode& node0 = n3p.node0(g);
  const GNode& node1 = n3p.node1(g);
  const GNode& node2 = n3p.node2(g);

  const int32_t edge0_angle = angle_to_east_degrees(
      node0.lat, node0.lon, node1.lat, node1.lon, n3p.edge0(g).distance_cm);
  const int32_t edge1_angle = angle_to_east_degrees(
      node1.lat, node1.lon, node2.lat, node2.lon, n3p.edge1(g).distance_cm);
  const int32_t turning_angle = angle_between_edges(edge0_angle, edge1_angle);

  return TurnAngleTimeLoss(g, vh, n3p.edge0(g), n3p.edge1(g), turning_angle);
}

//
uint32_t CrossingCost(const Graph& g, VEHICLE vh, const N3Path& n3p) {
  return 0;
}

// Compute the (uncompressed) turn cost for the specific turn 'n3p'.
inline uint32_t ComputeTurnCostForN3Path(
    const Graph& g, VEHICLE vh, const IndexedTurnRestrictions& indexed_trs,
    const N3Path& n3p) {
  const TRStatus tr_status = CheckSimpleTurnRestriction(g, indexed_trs, n3p);
  if (tr_status == TRStatus::FORBIDDEN) {
    return TURN_COST_INFINITY;
  }

  // Is this a u-turn that is forbidden, given the type of crossing at the
  // middle node? Note that a positive turn restriction from above always
  // allows a u-turn.
  const NodeTags* node_tags = g.FindNodeTags(n3p.node1(g).node_id);
  const bool uturn = (n3p.node0_idx == n3p.node2_idx);
  if (uturn) {
    if (tr_status == TRStatus::ALLOWED) {
      return TURN_COST_U_TURN;
    }
    if (!IsUTurnAllowed(g, vh, node_tags, n3p)) {
      return TURN_COST_INFINITY;
    }
    return TURN_COST_U_TURN;
  }

  // Check if we're blocked by the middle node. We know this is not a u-turn.
  if (VehicleBlockedAtNode(vh, node_tags)) {
    const GWay& way0 = g.ways.at(n3p.edge0(g).way_idx);
    if (!way0.area || n3p.edge0(g).way_idx != n3p.edge1(g).way_idx) {
      return TURN_COST_INFINITY;
    }
  }

  // So far we know we can do the turn and it is not a u-turn.
  //
  // Compute three time losses and use the maximum:
  // 1) Time loss because of node (stop sign, signals, etc.)
  // 2) Time loss because of curve.
  // 3) Real crossing

  const uint32_t cost_node_tags = NodeTagsCost(g, n3p);
  const uint32_t cost_curve = CurveCost(g, vh, n3p);
  const uint32_t cost_crossing = CrossingCost(g, vh, n3p);

  return std::max(cost_node_tags, std::max(cost_curve, cost_crossing));
}

}  // namespace

// Compute all turn costs for an edge at the target node.
// vh: type of the vehicle.
// indexed_trs: Container containing the simple turn restrictions.
// fe: The edge for which we compute turn costs at the target node.
// Returns the resulting turn costs data for fe.target_node(g).
inline TurnCostData ComputeTurnCostsForEdge(
    const Graph& g, VEHICLE vh, const IndexedTurnRestrictions& indexed_trs,
    const FullEdge fe) {
  // Create a tcd with the needed dimension.
  const GNode& crossing_node = fe.target_node(g);
  TurnCostData tcd{
      {crossing_node.num_forward_edges, TURN_COST_ZERO_COMPRESSED}};

  for (uint32_t off = 0; off < crossing_node.num_forward_edges; ++off) {
    tcd.turn_costs.at(off) = compress_turn_cost(ComputeTurnCostForN3Path(
        g, vh, indexed_trs, N3Path::Create(g, fe, {fe.target_idx(g), off})));
    // {.start_idx = fe.target_idx(g), .offset = off})));
  }
  return tcd;
}
