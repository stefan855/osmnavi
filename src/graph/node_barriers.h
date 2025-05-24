#pragma once

#include "absl/strings/str_format.h"
#include "base/util.h"
#include "graph/build_graph.h"
#include "graph/graph_def.h"
#include "graph/routing_attrs.h"
#include "osm/access.h"
#include "osm/osm_helpers.h"

// Extract barrier information stored with some nodes.
// Returns false if there is no data, true if it has.
// Per vehicle access is returned in node_attrs.
bool ConsumeNodeTags(const OSMTagHelper& tagh, int64_t node_id,
                     const google::protobuf::RepeatedField<int>& keys_vals,
                     int kv_start, int kv_stop, NodeAttributes* node_attrs) {
  node_attrs->node_id = -1;
  if (kv_start >= kv_stop) {
    return false;
  }

  // We apply access tags are in the following order:
  //   1) "barrier=" type sets the default.
  //   2) "access=" overrides everything that barrier type might have set.
  //   3) "motor_vehicle=" sets access for all motorized traffic.
  //   4) "bicycle=", "foot=", "horse=" etc. set access for individual vehicle
  //      types.
  ACCESS access = ACC_MAX;
  ACCESS motor_vehicle_access = ACC_MAX;
  std::string_view barrier;
  std::string_view locked;

  // Parse global tags: "access", "barrier", "motor_vehicle", "locked".
  for (int i = kv_start; i < kv_stop; i += 2) {
    const std::string_view key = tagh.ToString(keys_vals.at(i));
    if (key == "access") {
      access = AccessToEnum(tagh.ToString(keys_vals.at(i + 1)));
      if (access == ACC_MAX) {
        LOG_S(INFO) << absl::StrFormat("Invalid access=<%s> in node %lld",
                                       tagh.ToString(keys_vals.at(i + 1)),
                                       node_id);
      }
    } else if (key == "motor_vehicle") {
      motor_vehicle_access = AccessToEnum(tagh.ToString(keys_vals.at(i + 1)));
      if (motor_vehicle_access == ACC_MAX) {
        LOG_S(INFO) << absl::StrFormat("Invalid access=<%s> in node %lld",
                                       tagh.ToString(keys_vals.at(i + 1)),
                                       node_id);
      }
    } else if (key == "barrier") {
      barrier = tagh.ToString(keys_vals.at(i + 1));
    } else if (key == "locked") {
      locked = tagh.ToString(keys_vals.at(i + 1));
    }
  }

  if (barrier.empty()) {
    return false;
  }

  /*
  These attributes are assumed to block passage through the node for all vehicle
  types. Since this is the default also for unknown barrier types, the list
  isn't actually needed.

  static const std::string_view full_blocks[] = {
      "debris",    "chain",        "barrier_board", "jersey_barrier",
      "log",       "rope",         "yes",           "hampshire_gate",
      "sliding_beam", "sliding_gate",  "spikes",
      "wedge",     "wicket_gate",
  };
  */
  // Barrier types that don't block passage for any vehicle by default.
  static const std::string_view no_blocks[] = {
      "border_control", "bump_gate", "cattle_grid", "coupure",
      "entrance",       "lift_gate", "sally_port",  "toll_booth"};

  auto& vh_acc = node_attrs->vh_acc;

  if (access != ACC_MAX) {
    // The barrier type doesn't matter, use access.
    std::fill_n(vh_acc, VH_MAX, access);
  } else {
    // There was no all-overriding "access" tag, set access based on barrier.
    std::fill_n(vh_acc, VH_MAX, ACC_NO);
    if (barrier == "stile" || barrier == "turnstile" ||
        barrier == "full-height_turnstile" || barrier == "kerb" ||
        barrier == "kissing_gate") {
      vh_acc[VH_FOOT] = ACC_YES;
    } else if (barrier == "cycle_barrier" || barrier == "bar" ||
               barrier == "swing_gate") {
      vh_acc[VH_FOOT] = ACC_YES;
      vh_acc[VH_BICYCLE] = ACC_YES;
    } else if (barrier == "bollard" || barrier == "block" ||
               barrier == "motorcycle_barrier" || barrier == "planter") {
      vh_acc[VH_FOOT] = ACC_YES;
      vh_acc[VH_BICYCLE] = ACC_YES;
      vh_acc[VH_MOPED] = ACC_YES;
    } else if (barrier == "gate") {
      if (locked != "yes") {
        std::fill_n(vh_acc, VH_MAX, ACC_YES);
      }
    } else if (barrier == "horse_stile") {
      vh_acc[VH_FOOT] = ACC_YES;
      vh_acc[VH_HORSE] = ACC_YES;
    } else if (barrier == "bus_trap") {
      vh_acc[VH_FOOT] = ACC_YES;
      vh_acc[VH_BICYCLE] = ACC_YES;
      vh_acc[VH_MOPED] = ACC_YES;
      vh_acc[VH_PSV] = ACC_YES;
      vh_acc[VH_HGV] = ACC_YES;
      vh_acc[VH_BUS] = ACC_YES;
    } else if (SpanContains(no_blocks, barrier)) {
      std::fill_n(vh_acc, VH_MAX, ACC_YES);  // blocks no traffic by default.
    } else {
      ;  // Assume everything else is blocking. This includes full_blocks.
    }
  }

  if (motor_vehicle_access != ACC_MAX) {
    for (uint8_t vh = VH_MOTORCAR; vh < VH_MAX; ++vh) {
      if (VehicleIsMotorized((VEHICLE)vh)) {
        vh_acc[(VEHICLE)vh] = motor_vehicle_access;
      }
    }
  }

  // Now apply all more specific access tags such as bicycle=yes.
  for (int i = kv_start; i < kv_stop; i += 2) {
    VEHICLE vh = VehicleToEnum(tagh.ToString(keys_vals.at(i)));
    if (vh != VH_MAX) {
      ACCESS acc = AccessToEnum(tagh.ToString(keys_vals.at(i + 1)));
      if (acc != ACC_MAX) {
        vh_acc[vh] = acc;
      } else {
        LOG_S(INFO) << absl::StrFormat("Invalid access=<%s> in node %lld",
                                       tagh.ToString(keys_vals.at(i + 1)),
                                       node_id);
      }
    }
  }

  for (const ACCESS acc : node_attrs->vh_acc) {
    if (acc != ACC_YES) {
      node_attrs->node_id = node_id;
      return true;
    }
  }
  return false;
}

// Iterate through all nodes and for each check if it has a barrier which blocks
// traffic. If it does, then add a simple turn restriction that forbids passing
// the obstacle.
//
// Note that traffic within ways having the attribute area=yes is not blocked,
// i.e. it is always possible to continue within the area, even if a border node
// of the area has a blocking barrier.
void StoreNodeBarrierData(build_graph::GraphMetaData* meta) {
  FUNC_TIMER();

  Graph& g = meta->graph;
  const std::vector<NodeAttributes>& attrs = g.node_attrs;
  size_t attr_idx = 0;
  std::set<uint32_t> way_set;
  std::set<uint32_t> way_with_area_set;

  for (size_t node_idx = 0; node_idx < g.nodes.size(); ++node_idx) {
    while (attr_idx < attrs.size() &&
           attrs.at(attr_idx).node_id < g.nodes.at(node_idx).node_id) {
      attr_idx++;
    }
    if (attr_idx >= attrs.size()) {
      break;
    }

    // LOG_S(INFO) << "Compare " << attrs.at(attr_idx).node_id << " to "
    //             << g.nodes.at(node_idx).node_id;
    if (attrs.at(attr_idx).node_id == g.nodes.at(node_idx).node_id) {
      const GNode& n = g.nodes.at(node_idx);
      const NodeAttributes na = attrs.at(attr_idx++);
      LOG_S(INFO) << "Examine node " << n.node_id;
      const uint32_t num_edges = gnode_num_edges(g, node_idx);
      bool same_target = false;
      bool self_edge = false;
      if (num_edges == 2) {
        uint32_t other1 = g.edges.at(n.edges_start_pos).other_node_idx;
        uint32_t other2 = g.edges.at(n.edges_start_pos + 1).other_node_idx;
        self_edge = (other1 == node_idx || other2 == node_idx);
        same_target = (other1 == other2);
      }
      way_set.clear();
      way_with_area_set.clear();
      for (const GEdge& e : gnode_all_edges(g, node_idx)) {
        const GWay& w = g.ways.at(e.way_idx);
        if (w.area) {
          way_with_area_set.insert(e.way_idx);
        } else {
          way_set.insert(e.way_idx);
        }
        if (w.area && !w.closed_way) {
          LOG_S(INFO) << "Way with area but not closed " << w.id;
        }
      }

      bool access_allowed = (na.vh_acc[VH_MOTORCAR] > ACC_PRIVATE &&
                             na.vh_acc[VH_MOTORCAR] < ACC_MAX);
      LOG_S(INFO) << absl::StrFormat(
          "Node barrier on node %lld forward:%u all:%u #w-norm:%llu "
          "#w-area:%llu same-target:%u self-edge:%d acc:%d",
          n.node_id, n.num_edges_forward, num_edges, way_set.size(),
          way_with_area_set.size(), same_target, self_edge,
          (int)access_allowed);

      if (access_allowed) {
        meta->stats.num_node_barrier_free++;
        continue;  // Nothing to do.
      }

      // Iterate overall all incoming edges.
      LOG_S(INFO) << "AA1";
      for (const FullGEdge& in_ge : gnode_incoming_edges(g, node_idx)) {
        LOG_S(INFO) << "AA2";
        const GNode other = g.nodes.at(in_ge.start_idx);
        GEdge& in = g.edges.at(other.edges_start_pos + in_ge.offset);
        CHECK_EQ_S(in.other_node_idx, node_idx);  // Is it really incoming?
        const bool in_way_area = g.ways.at(in.way_idx).area;
        uint32_t allowed_edge_bits = 0;
        for (uint32_t offset = 0; offset < n.num_edges_forward; ++offset) {
          const GEdge& out = g.edges.at(n.edges_start_pos + offset);
          // If the out-edge stays on the same way as the in-edge then there are
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
          if ((in.way_idx == out.way_idx) &&
              (in_way_area ||
               (out.other_node_idx == in_ge.start_idx))) {  // u-turn
            // Allowed to stay within area, set bit.
            allowed_edge_bits = (allowed_edge_bits | (1u << offset));
          }
        }
        if (allowed_edge_bits == 0) {
          // There is even no u-turn possible (might be a data error).
          LOG_S(INFO) << "AA3";
          meta->stats.num_edge_barrier_no_uturn++;
        } else {
          LOG_S(INFO) << "AA4";
          // Add/merge a pseudo turn restriction.
          const TurnRestriction::TREdge tr_key = {
              .from_node_idx = in_ge.start_idx,
              .way_idx = in.way_idx,
              .to_node_idx = in.other_node_idx,
              .edge_idx = 0};
          auto iter = g.simple_turn_restriction_map.find(tr_key);
          if (iter != g.simple_turn_restriction_map.end()) {
            SimpleTurnRestrictionData& data = iter->second;
            if (num_edges == 2) {
              data.allowed_edge_bits =
                  data.allowed_edge_bits & allowed_edge_bits;
              data.from_node = 1;
              in.car_uturn_allowed = 1;
              meta->stats.num_edge_barrier_merged++;
              LOG_S(INFO) << absl::StrFormat(
                  "Preexisting TR for barrier (merged), rel:%lld node:%lld "
                  "#edges:%u",
                  data.id, n.node_id, num_edges);
            } else {
              LOG_S(INFO) << absl::StrFormat(
                  "Preexisting TR for barrier (ignored), rel:%lld node:%lld "
                  "#edges:%u",
                  data.id, n.node_id, num_edges);
            }
          } else {
            // Create a pseudo turn-restriction for this edge+barrier
            g.simple_turn_restriction_map[tr_key] = {
                .allowed_edge_bits = allowed_edge_bits,
                .from_relation = 0,
                .from_node = 1,
                .id = n.node_id};
            g.nodes.at(node_idx).simple_turn_restriction_via_node = 1;
            in.car_uturn_allowed = 1;
            meta->stats.num_edge_barrier_block++;
          }
        }
      }
    }
  }
}
