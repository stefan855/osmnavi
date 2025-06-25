#pragma once

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
};

bool ComputeTurnCosts(const Graph& g, uint32_t start_node_idx,
                      uint32_t edge_idx,
                      const SimpleTurnRestrictionData* turn_restriction_data,
                      const NodeAttribute* node_attr, TurnCostData* tcd) {
  constexpr uint32_t FORBIDDEN_VAL = 0;
  constexpr uint32_t NO_TURN_COST_VAL = 1;
  CHECK_NOTNULL_S(tcd);
  const GEdge& e = g.edges.at(edge_idx);
  // const GNode& start_node = g.nodes.at(start_node_idx);
  const GNode& target_node = g.nodes.at(e.other_node_idx);

  // Check if the vehicle can't pass the node.
  if (node_attr != nullptr) {
    ACCESS acc = node_attr->vh_acc[VH_MOTORCAR];
    if (acc <= ACC_PRIVATE && acc >= ACC_MAX) {
      tcd->turn_costs.assign(target_node.num_edges_forward, FORBIDDEN_VAL);
      return true;
    }
  }

  // Set default.
  tcd->turn_costs.assign(target_node.num_edges_forward, NO_TURN_COST_VAL);

  // Handle disallowed u-turns.
  if (!e.car_uturn_allowed) {
    for (uint32_t off = 0; off < target_node.num_edges_forward; ++off) {
      if (g.edges.at(target_node.edges_start_pos + off).other_node_idx ==
          start_node_idx) {
        tcd->turn_costs.at(off) = FORBIDDEN_VAL;
      }
    }
  }

  // Handle optional turn restriction data.
  if (turn_restriction_data != nullptr) {
    for (uint32_t off = 0; off < target_node.num_edges_forward; ++off) {
      if ((turn_restriction_data->allowed_edge_bits & (1u << off)) == 0) {
        tcd->turn_costs.at(off) = FORBIDDEN_VAL;
      }
    }
  }

  // TODO: Compute fixed turn cost for crossing, traffic lights, etc.
  uint32_t cost_tenth_sec = 0;
  if (node_attr != nullptr) {
    if (node_attr->bit_stop) {
      cost_tenth_sec += 30;
    }
    if (node_attr->bit_traffic_signals) {
      cost_tenth_sec += 100;
    }
    if (node_attr->bit_crossing) {
      cost_tenth_sec += 30;
    }
    if (node_attr->bit_railway_crossing) {
      // We don't know how busy the railway is...
      cost_tenth_sec += 300;
    }
    if (node_attr->bit_traffic_calming) {
      cost_tenth_sec += 10;
    }
  }

  // TODO: Compute turn cost for given velocity and angle
#if 0
  const uint32_t incoming_degrees =
      angle_to_east_degrees(lat1n, lon1n, lat2n, lon2n, length_cm);
#endif
  for (uint32_t off = 0; off < target_node.num_edges_forward; ++off) {
    if (tcd->turn_costs.at(off) != FORBIDDEN_VAL) {
    }
  }

  return true;
}

namespace std {
template <>
struct hash<TurnCostData> {
  size_t operator()(const TurnCostData& tcd) const {
    return std::hash<std::string_view>{}(
        std::string_view((char*)tcd.turn_costs.data(), tcd.turn_costs.size()));
  }
};
}  // namespace std
