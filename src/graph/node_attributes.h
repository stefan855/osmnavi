#pragma once

#include "absl/strings/str_format.h"
#include "base/util.h"
#include "graph/build_graph.h"
#include "graph/graph_def.h"
#include "graph/routing_attrs.h"
#include "osm/access.h"
#include "osm/osm_helpers.h"

namespace {
struct NodeTagExtract {
  ACCESS access = ACC_MAX;
  ACCESS motor_vehicle_access = ACC_MAX;
  std::string_view barrier;
  std::string_view locked;
  std::string_view highway;
  std::string_view direction;
  std::string_view crossing;
  std::string_view crossing_barrier;
  std::string_view public_transport;
  std::string_view traffic_calming;
  std::string_view railway;
};

NodeTagExtract ExtractNodeTags(
    const OSMTagHelper& tagh, int64_t node_id,
    const google::protobuf::RepeatedField<int>& keys_vals, int kv_start,
    int kv_stop) {
  NodeTagExtract ex;
  for (int i = kv_start; i < kv_stop; i += 2) {
    const std::string_view key = tagh.ToString(keys_vals.at(i));
    const std::string_view val = tagh.ToString(keys_vals.at(i + 1));
    if (key == "access") {
      ex.access = AccessToEnum(val);
      if (ex.access == ACC_MAX) {
        LOG_S(INFO) << absl::StrFormat("Invalid access=<%s> in node %lld", val,
                                       node_id);
      }
    } else if (key == "motor_vehicle") {
      ex.motor_vehicle_access = AccessToEnum(val);
      if (ex.motor_vehicle_access == ACC_MAX) {
        LOG_S(INFO) << absl::StrFormat("Invalid access=<%s> in node %lld", val,
                                       node_id);
      }
    } else if (key == "barrier") {
      ex.barrier = val;
    } else if (key == "locked") {
      ex.locked = val;
    } else if (key == "highway") {
      ex.highway = val;
    } else if (key == "direction") {
      ex.direction = val;
    } else if (key == "crossing") {
      ex.crossing = val;
    } else if (key == "crossing:barrier") {
      ex.crossing_barrier = val;
    } else if (key == "public_transport") {
      ex.public_transport = val;
    } else if (key == "traffic_calming") {
      ex.traffic_calming = val;
    } else if (key == "railway") {
      ex.railway = val;
    }
  }
  return ex;
}

void SetNodeAttrBits(const NodeTagExtract& ex, NodeAttribute* a) {
  if (ex.highway == "turning_circle") {
    a->bit_turning_circle = 1;
  }

  if (ex.highway == "stop") {
    a->bit_stop = 1;
  }

  // Cars. The marker
  if (ex.highway == "traffic_signals") {
    a->bit_traffic_signals = 1;
  }

  // Pedstrians/bikes: Node in the middle of the crossing that is part of the
  // way for pedestrians/bikes over the street.
  if (ex.highway == "crossing") {
    a->bit_crossing = 1;
    if (ex.crossing == "traffic_signals") {
      a->bit_traffic_signals = 1;
    }
  }

  if (StrSpanContains({"crossing", "level_crossing", "tram_crossing",
                       "tram_level_crossing"},
                      ex.railway)) {
    a->bit_railway_crossing = 1;
    a->bit_railway_crossing_barrier = StrSpanContains(
        {"yes", "full", "half", "double_half", "gate", "chain", "gates"},
        ex.crossing_barrier);
  }

  if (ex.public_transport == "stop_position" ||
      ex.public_transport == "platform" || ex.public_transport == "station") {
    a->bit_public_transport = 1;
  }

  if (!ex.traffic_calming.empty() && ex.traffic_calming != "no") {
    a->bit_traffic_calming = 1;
  }
}

// Assign the allowed vehicle types for a barrier. For instance, a turnstile
// only allows pedestrians.
inline void SetBarrierToVehicle(
    const OSMTagHelper& tagh, int64_t node_id,
    const google::protobuf::RepeatedField<int>& keys_vals, int kv_start,
    int kv_stop, const NodeTagExtract& ex, NodeAttribute* node_attr) {
  // For barriers, we apply access tags in the following order:
  //   1) "barrier=" type sets the default.
  //   2) "access=" overrides everything that barrier type might have set.
  //   3) "motor_vehicle=" sets access for all motorized traffic.
  //   4) "bicycle=", "foot=", "horse=" etc. set access for individual vehicle
  //      types.

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

  auto& vh_acc = node_attr->vh_acc;
  if (ex.access != ACC_MAX) {
    // The barrier type doesn't matter, use access.
    std::fill_n(vh_acc, VH_MAX, ex.access);
  } else {
    // There was no all-overriding "access" tag, set access based on barrier.
    std::fill_n(vh_acc, VH_MAX, ACC_NO);
    if (ex.barrier == "stile" || ex.barrier == "turnstile" ||
        ex.barrier == "full-height_turnstile" || ex.barrier == "kerb" ||
        ex.barrier == "kissing_gate" || ex.barrier == "log") {
      vh_acc[VH_FOOT] = ACC_YES;
    } else if (ex.barrier == "cycle_barrier" || ex.barrier == "bar" ||
               ex.barrier == "swing_gate") {
      vh_acc[VH_FOOT] = ACC_YES;
      vh_acc[VH_BICYCLE] = ACC_YES;
    } else if (ex.barrier == "bollard" || ex.barrier == "block" ||
               ex.barrier == "motorcycle_barrier" || ex.barrier == "planter") {
      vh_acc[VH_FOOT] = ACC_YES;
      vh_acc[VH_BICYCLE] = ACC_YES;
      vh_acc[VH_MOPED] = ACC_YES;
    } else if (ex.barrier == "gate") {
      if (ex.locked != "yes") {
        std::fill_n(vh_acc, VH_MAX, ACC_YES);
      }
    } else if (ex.barrier == "horse_stile") {
      vh_acc[VH_FOOT] = ACC_YES;
      vh_acc[VH_HORSE] = ACC_YES;
    } else if (ex.barrier == "bus_trap") {
      vh_acc[VH_FOOT] = ACC_YES;
      vh_acc[VH_BICYCLE] = ACC_YES;
      vh_acc[VH_MOPED] = ACC_YES;
      vh_acc[VH_PSV] = ACC_YES;
      vh_acc[VH_HGV] = ACC_YES;
      vh_acc[VH_BUS] = ACC_YES;
    } else if (SpanContains(no_blocks, ex.barrier)) {
      std::fill_n(vh_acc, VH_MAX, ACC_YES);  // blocks no traffic by default.
    } else {
      LOG_S(INFO) << "Unknown barrier: " << ex.barrier << ":" << node_id;
      ;  // Assume everything else is blocking. This includes full_blocks.
    }
  }

  if (ex.motor_vehicle_access != ACC_MAX) {
    for (uint8_t vh = VH_MOTORCAR; vh < VH_MAX; ++vh) {
      if (VehicleIsMotorized((VEHICLE)vh)) {
        vh_acc[(VEHICLE)vh] = ex.motor_vehicle_access;
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

  for (const ACCESS acc : node_attr->vh_acc) {
    if (acc != ACC_YES) {
      node_attr->has_barrier = 1;
      return;
    }
  }
}
}  // namespace

// Extract node attributes including barrier information stored in the keys_vals
// of nodes. Returns true if relevant attributes were found, false if not.
// The attributes themselves are returned in 'node_attr'.
NodeAttribute ParseOSMNodeTags(
    const OSMTagHelper& tagh, int64_t node_id,
    const google::protobuf::RepeatedField<int>& keys_vals, int kv_start,
    int kv_stop) {
  NodeAttribute attr;
  if (kv_start >= kv_stop) {
    return attr;
  }

  std::fill_n(attr.vh_acc, VH_MAX, ACC_NO);

  const NodeTagExtract ex =
      ExtractNodeTags(tagh, node_id, keys_vals, kv_start, kv_stop);
  SetNodeAttrBits(ex, &attr);

  if (!ex.barrier.empty()) {
    SetBarrierToVehicle(tagh, node_id, keys_vals, kv_start, kv_stop, ex, &attr);
  }

  if (!attr.empty()) {
    attr.node_id = node_id;
  }
  return attr;
}

// Iterate through all nodes and check if it has a barrier which blocks traffic.
// If it does, then add a simple turn restriction that forbids passing the
// obstacle.
//
// Note that traffic within ways having the attribute area=yes is not blocked,
// i.e. it is always possible to continue within the area, even if a border node
// of the area has a blocking barrier.
void StoreNodeBarrierDataObsolete(Graph* g,
                                  build_graph::BuildGraphStats* stats) {
  FUNC_TIMER();

  const std::vector<NodeAttribute>& attrs = g->node_attrs_sorted;
  size_t attr_idx = 0;
  std::set<uint32_t> way_set;
  std::set<uint32_t> way_with_area_set;

  for (size_t node_idx = 0; node_idx < g->nodes.size(); ++node_idx) {
    while (attr_idx < attrs.size() &&
           attrs.at(attr_idx).node_id < g->nodes.at(node_idx).node_id) {
      attr_idx++;
    }
    if (attr_idx >= attrs.size()) {
      break;
    }

    // LOG_S(INFO) << "Compare " << attrs.at(attr_idx).node_id << " to "
    //             << g->nodes.at(node_idx).node_id;
    if (attrs.at(attr_idx).node_id == g->nodes.at(node_idx).node_id) {
      const GNode& n = g->nodes.at(node_idx);
      const NodeAttribute na = attrs.at(attr_idx++);
      if (!na.has_barrier) {
        continue;
      }

      LOG_S(INFO) << "Examine node " << n.node_id;
      const uint32_t num_edges = gnode_num_edges(*g, node_idx);
      bool same_target = false;
      bool self_edge = false;
      if (num_edges == 2) {
        uint32_t other1 = g->edges.at(n.edges_start_pos).other_node_idx;
        uint32_t other2 = g->edges.at(n.edges_start_pos + 1).other_node_idx;
        self_edge = (other1 == node_idx || other2 == node_idx);
        same_target = (other1 == other2);
      }
      way_set.clear();
      way_with_area_set.clear();
      for (const GEdge& e : gnode_all_edges(*g, node_idx)) {
        const GWay& w = g->ways.at(e.way_idx);
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
        stats->num_node_barrier_free++;
        continue;  // Nothing to do.
      }

      // Iterate overall all incoming edges.
      for (const FullEdge& in_ge : gnode_incoming_edges(*g, node_idx)) {
        const GNode other = g->nodes.at(in_ge.start_idx);
        GEdge& in = g->edges.at(other.edges_start_pos + in_ge.offset);
        CHECK_EQ_S(in.other_node_idx, node_idx);  // Is it really incoming?
        const bool in_way_area = g->ways.at(in.way_idx).area;
        uint32_t allowed_edge_bits = 0;
        // Iterate outgoing edges at the same node.
        for (uint32_t offset = 0; offset < n.num_edges_forward; ++offset) {
          const GEdge& out = g->edges.at(n.edges_start_pos + offset);
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
            // Allowed, set bit.
            allowed_edge_bits = (allowed_edge_bits | (1u << offset));
          }
        }
        if (allowed_edge_bits == 0) {
          // There is even no u-turn possible (might be a data error).
          stats->num_edge_barrier_no_uturn++;
        } else {
          // Add/merge a pseudo turn restriction.
          const TurnRestriction::TREdge tr_key = {
              .from_node_idx = in_ge.start_idx,
              .way_idx = in.way_idx,
              .to_node_idx = in.other_node_idx,
              .edge_idx = 0};
          auto iter = g->simple_turn_restriction_map.find(tr_key);
          if (iter != g->simple_turn_restriction_map.end()) {
            SimpleTurnRestrictionData& data = iter->second;
            if (num_edges == 2) {
              data.allowed_edge_bits =
                  data.allowed_edge_bits & allowed_edge_bits;
              data.from_node = 1;
              in.car_uturn_allowed = 1;
              stats->num_edge_barrier_merged++;
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
            g->simple_turn_restriction_map[tr_key] = {
                .allowed_edge_bits = allowed_edge_bits,
                .from_relation = 0,
                .from_node = 1,
                .id = n.node_id};
            g->nodes.at(node_idx).simple_turn_restriction_via_node = 1;
            in.car_uturn_allowed = 1;
            stats->num_edge_barrier_block++;
          }
        }
      }
    }
  }
}
