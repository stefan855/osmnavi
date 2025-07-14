#pragma once

#include <vector>

#include "geometry/distance.h"
#include "graph/graph_def.h"

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

constexpr uint32_t TURN_COST_INF = 1'000'000'000u;  // ~11.5 days
constexpr uint32_t TURN_COST_ZERO = 0;
constexpr uint32_t TURN_COST_U_TURN = 10000;


namespace {
// turn costs in milliseconds are compressed into 63 buckets, which
// represent the following values.
static constexpr size_t compressed_turn_cost_values_dim = 64;
static constexpr uint32_t
    compressed_turn_cost_values[compressed_turn_cost_values_dim] = {
        0,        100,      200,      300,          400,      500,
        600,      700,      800,      900,          1000,     1100,
        1200,     1300,     1400,     1500,         1600,     1900,
        2200,     2500,     2800,     3300,         3800,     4300,
        5000,     5700,     6600,     7600,         8700,     10000,
        11500,    13200,    17200,    22400,        29100,    37800,
        49200,    63900,    83100,    108000,       140400,   182600,
        237300,   308500,   401100,   521400,       677800,   881200,
        1145500,  1489200,  1935900,  2516700,      3271700,  4253200,
        5529200,  7188000,  9344300,  12147700,     15791900, 20529500,
        26688400, 34694900, 45103400, TURN_COST_INF};
}  // namespace

// Compress a turn cost value (milli seconds) to compressed format
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
constexpr uint32_t TURN_COST_INF_COMPRESSED = compress_turn_cost(TURN_COST_INF);
static_assert(TURN_COST_ZERO_COMPRESSED == 0);
static_assert(TURN_COST_INF_COMPRESSED == compressed_turn_cost_values_dim - 1);

inline constexpr uint32_t decompress_turn_cost(uint32_t compressed_cost) {
  if (compressed_cost == 0) return 0;
  CHECK_LT_S(compressed_cost, compressed_turn_cost_values_dim);
  return compressed_turn_cost_values[compressed_cost];
}

namespace {

// Compute the cost of a turn from edge e1 to e2. A smaller angle has higher
// cost. The unit of the value returned is in milli seconds.
//
// TODO: Use the following formula
// https://www.lernhelfer.de/schuelerlexikon/physik/artikel/kurvenfahrten
// Formula max. v through curve with length s and angle-change a:
//   v-max = 3.6 * sqrt(u * g * s / (a * pi / 180))
//           u = coefficient of static friction (Haftreibungskoeffizient)
//               depends on conditions (rain etc.) surface and more.
//               Range for cars is 0.4..0.8 (on asphalt roads).
//           g = 9.81
//           s = arc length (Bogenlänge)
//           a = angle change in degrees
// Example for a curve with length 100m and 90 degree change in direction:
//   v-max = 69.70 km/h = 3.6 * math.sqrt(0.6*9.81*100/(90*3.14/180))
uint32_t turn_angle_cost(const GEdge& e1, const GEdge& e2,
                         uint32_t turning_angle) {
  if (turning_angle > 160) {
    return 0;
  } else if (turning_angle > 120) {
    return 500;
  } else if (turning_angle > 60) {
    return 2000;
  } else if (turning_angle > 0) {
    return 4000;
  } else {
    // u-turn.
    return TURN_COST_U_TURN;
  }
}

// Given a node and an outgoing edge 'out_edge', is it allowed to make a
// u-turn at out_edge.other_node_idx and return to node_idx? The code checks
// for situations where a vehicle would be trapped at out_edge.other_node_idx
// with no way to continue the travel, which is true at the end of a street or
// when facing a restricted access area.
// TODO: handle vehicle types properly.
inline bool IsUTurnAllowedEdge(const Graph& g, uint32_t node_idx,
                               const GEdge& out_edge) {
  // TODO: Is it clear that TRUNK and higher should have no automatic u-turns?
  if (g.ways.at(out_edge.way_idx).highway_label <= HW_TRUNK_LINK) {
    return false;
  }
  uint32_t to_idx = out_edge.other_node_idx;
  bool restr = (out_edge.car_label != GEdge::LABEL_FREE);
  bool found_u_turn = false;
  bool found_continuation = false;
  bool found_free_continuation = false;

  for (const GEdge& o : gnode_forward_edges(g, to_idx)) {
    found_u_turn |= (o.other_node_idx == node_idx);
    if (o.other_node_idx != node_idx && o.other_node_idx != to_idx) {
      found_continuation = true;
      found_free_continuation |= (o.car_label == GEdge::LABEL_FREE);
    }
  }

  if (found_u_turn) {
    if (!found_continuation) {
      return true;
    }
    if (!restr && !found_free_continuation) {
      return true;
    }
  }
  return false;
}

inline void ProcessSimpleTurnRestrictions(
    const Graph& g, const IndexedTurnRestrictions& indexed_trs, VEHICLE vh,
    uint32_t start_node_idx, uint32_t edge_idx, TurnCostData* tcd) {
  const GEdge& e = g.edges.at(edge_idx);
  const TurnRestriction::TREdge trigger_key = {.from_node_idx = start_node_idx,
                                               .way_idx = e.way_idx,
                                               .to_node_idx = e.other_node_idx};
  auto iter = indexed_trs.map_to_first.find(trigger_key);
  if (iter == indexed_trs.map_to_first.end()) {
    return;
  }
  CHECK_S(indexed_trs.sorted_trs.at(iter->second).GetTriggerKey() ==
          trigger_key);

  const GNode& via_node = g.nodes.at(e.other_node_idx);
  const uint32_t num_edges = via_node.num_edges_forward;
  CHECK_EQ_S(num_edges, tcd->turn_costs.size())
      << GetGNodeIdSafe(g, e.other_node_idx);

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
          e.other_node_idx == tr.path.back().to_node_idx) {
        found = true;
        if (tr.forbidden) {
          // Remove the bit belonging to the edge.
          tcd->turn_costs.at(offset) = TURN_COST_INF_COMPRESSED;
        } else {
          for (size_t offset2 = 0; offset2 < num_edges; ++offset2) {
            tcd->turn_costs.at(offset) =
                (offset2 == offset ? TURN_COST_ZERO_COMPRESSED
                                   : TURN_COST_INF_COMPRESSED);
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

}  // namespace

// Compute turn costs of an incoming edge at a crossing point.
// vh: type of the vehicle.
// start_node_idx: start node of the incoming edge
// edge_idx: incoming edge. edge.other_node_idx is the crossing point.
// turn_restriction_data:
// node_attr:
// tcd: resulting turn costs data. Any incoming data is overwritten.
inline TurnCostData ComputeTurnCostsForEdge(
    const Graph& g, VEHICLE vh, uint32_t start_node_idx, uint32_t edge_idx,
    const IndexedTurnRestrictions& indexed_trs, const NodeAttribute* node_attr
    /* ,TurnCostData* tcd*/) {
  // CHECK_NOTNULL_S(tcd);
  const GEdge& e = g.edges.at(edge_idx);
  const GNode& target_node = g.nodes.at(e.other_node_idx);

  // Set default.
  // tcd->turn_costs.assign(target_node.num_edges_forward,
  //                       TURN_COST_ZERO_COMPRESSED);
  TurnCostData tcd{{target_node.num_edges_forward, TURN_COST_ZERO_COMPRESSED}};

  // Handle automatically allowed u-turns.
  // TODO: this might contradict a turn restriction or node attributes induced
  // u-turns. In this case, the others should win.
  if (!IsUTurnAllowedEdge(g, start_node_idx, e)) {
    for (uint32_t off = 0; off < target_node.num_edges_forward; ++off) {
      if (g.edges.at(target_node.edges_start_pos + off).other_node_idx ==
          start_node_idx) {
        tcd.turn_costs.at(off) = TURN_COST_INF_COMPRESSED;
      }
    }
  }

  // Handle node barriers. Check if the vehicle can pass target_node when
  // entering the node through 'e'.
  if (node_attr != nullptr && node_attr->has_barrier &&
      (node_attr->vh_acc[vh] <= ACC_PRIVATE ||
       node_attr->vh_acc[vh] >= ACC_MAX)) {
    const bool incoming_way_area = g.ways.at(e.way_idx).area;
    // Iterate outgoing edges.
    for (uint32_t off = 0; off < target_node.num_edges_forward; ++off) {
      const GEdge& out = g.edges.at(target_node.edges_start_pos + off);
      // If the out-edge stays on the same way as the in-edge, then there are
      // two cases when the turn is allowed:
      //   * the way is an area.
      //   * out-edge is a u-turn on the same way.
      //
      // Consider the following, complicated case. There are several bollard
      // nodes that are part of two ways marked as areas:
      // https://www.openstreetmap.org/node/8680967810
      // Note that there are parallel edges from both ways between the
      // common bollard nodes. Because of this, allowing only u-turns on the
      // same way is important, otherwise one could easily change ways at
      // every bollard node by doing a u-turn on the other way.
      if ((e.way_idx == out.way_idx) &&
          (incoming_way_area ||
           (out.other_node_idx == start_node_idx))) {  // u-turn
        ;                                              // Allowed, do nothing.
      } else {
        // Forbidden.
        tcd.turn_costs.at(off) = TURN_COST_INF_COMPRESSED;
      }
    }
  }

  ProcessSimpleTurnRestrictions(g, indexed_trs, vh, start_node_idx, edge_idx,
                                &tcd);

  // ==========================================================================
  // So far, all turned costs stored in tcd.turn_costs are on/off, i.e. either
  // TURN_COST_ZERO_COMPRESSED or TURN_COST_INF_COMPRESSED.
  // Now compute small increments and recompress the values that were changed.
  // ==========================================================================

  uint32_t static_cost = 0;  // unit is milli second
  if (node_attr != nullptr) {
    if (node_attr->bit_stop) {
      static_cost += 3000;
    }
    if (node_attr->bit_traffic_signals) {
      static_cost += 10000;
    }
    if (node_attr->bit_crossing) {
      static_cost += 3000;
    }
    if (node_attr->bit_railway_crossing) {
      // We don't know how busy the railway is...
      static_cost += 30000;
    }
    if (node_attr->bit_traffic_calming) {
      static_cost += 1000;
    }
  }

  // TODO: Compute turn cost for given velocity and angle
  const GNode& start_node = g.nodes.at(start_node_idx);
  const uint32_t edge1_angle =
      angle_to_east_degrees(target_node.lat, target_node.lon, start_node.lat,
                            start_node.lon, e.distance_cm);
  for (uint32_t off = 0; off < target_node.num_edges_forward; ++off) {
    if (tcd.turn_costs.at(off) != TURN_COST_INF_COMPRESSED) {
      const GEdge e2 = g.edges.at(target_node.edges_start_pos + off);
      const GNode other_node = g.nodes.at(e2.other_node_idx);
      const uint32_t edge2_angle =
          angle_to_east_degrees(target_node.lat, target_node.lon,
                                other_node.lat, other_node.lon, e2.distance_cm);
      const uint32_t turning_angle =
          angle_between_edges(edge1_angle, edge2_angle);
      tcd.turn_costs.at(off) = compress_turn_cost(
          static_cost + turn_angle_cost(e, e2, turning_angle));
    }
  }

  return tcd;
}
