#pragma once

#include "absl/strings/str_format.h"
#include "base/util.h"
#include "graph/build_graph.h"
#include "graph/graph_def.h"
#include "graph/graph_def_utils.h"
#include "graph/routing_attrs.h"
#include "osm/access.h"
#include "osm/osm_helpers.h"

namespace {

// Handle crossings of footways with streets.
//   highway=crossing
//   crossing=*
//   crossing:signals=*
//   crossing:markings=*
//   See https://wiki.openstreetmap.org/wiki/Tag:highway=crossing
void SetCrossingBits(const ParsedTagInfo& pti, int64_t node_id, NodeTags* nt) {
  if (pti.FindValue({KEY_BIT_HIGHWAY}) != "crossing") {
    return;
  }
  nt->bit_crossing = 1;

  // Handle "crossing=*"
  std::string_view val = pti.FindValue({KEY_BIT_CROSSING});
  if (!val.empty()) {
    if (val == "traffic_signals") {
      // Crossing with traffic lights.
      nt->bit_crossing_traffic_signals = 1;
    } else if (val == "uncontrolled") {
      // Crossing with markings, without traffic lights.
      nt->bit_crossing_traffic_signals = 0;
      nt->bit_crossing_markings = 1;
    } else if (val == "marked" || val == "zebra") {
      // Crossing with markings, maybe with traffic lights.
      nt->bit_crossing_markings = 1;
    }
  }

  // Handle "crossing:signals=*"
  val = pti.FindValue({KEY_BIT_CROSSING, KEY_BIT_SIGNALS});
  if (!val.empty()) {
    if (val == "yes") {
      nt->bit_crossing_traffic_signals = 1;
    } else if (val == "no") {
      nt->bit_crossing_traffic_signals = 0;
    }
  }

  // Handle "crossing:markings=*"
  val = pti.FindValue({KEY_BIT_CROSSING, KEY_BIT_MARKINGS});
  if (!val.empty()) {
    if (val == "no") {
      nt->bit_crossing_markings = 0;
    } else {
      nt->bit_crossing_markings = 1;
    }
  }
}

void SetNodeTagBits(VEHICLE vt, const ParsedTagInfo& pti, int64_t node_id,
                    NodeTags* nt) {
  // Handle
  //   direction
  //   traffic_sign:direction
  //   traffic_signals:direction
  std::string_view val;
  val = pti.FindValue({KEY_BIT_DIRECTION});
  if (!val.empty()) {
    nt->direction = DirectionToEnum(val);
  }
  val = pti.FindValue({KEY_BIT_TRAFFIC_SIGN, KEY_BIT_DIRECTION});
  if (!val.empty()) {
    nt->direction = DirectionToEnum(val);
  }
  val = pti.FindValue({KEY_BIT_TRAFFIC_SIGNALS, KEY_BIT_DIRECTION});
  if (!val.empty()) {
    nt->direction = DirectionToEnum(val);
  }

  // Handle
  //   highway=turning_circle
  //   highway=stop and stop=all
  //   highway=give_way
  //   highway=traffic_signals
  val = pti.FindValue({KEY_BIT_HIGHWAY});
  if (val == "turning_circle") {
    nt->bit_turning_circle = 1;
  }

  if (val == "stop") {
    nt->bit_stop = 1;
    nt->stop_all = pti.FindValue({KEY_BIT_STOP}) == "all";
  }

  if (val == "give_way") {
    nt->bit_give_way = 1;
  }

  if (val == "traffic_signals") {
    nt->bit_traffic_signals = 1;
  }

  // Handle railway=*
  // crossing/tram_crossing: footway crossing tram/railway
  // level_crossing/tram_level_crossing: street crossing tram/railway.
  if (StrSpanContains(pti.FindValue({KEY_BIT_RAILWAY}),
                      {"crossing", "tram_crossing", "level_crossing",
                       "tram_level_crossing"})) {
    nt->bit_railway_crossing = 1;
    nt->bit_railway_crossing_barrier = StrSpanContains(
        pti.FindValue({KEY_BIT_CROSSING, KEY_BIT_BARRIER}),
        {"yes", "full", "half", "double_half", "gate", "chain", "gates"});
  }

  // Handle public_transport=*
  val = pti.FindValue({KEY_BIT_PUBLIC_TRANSPORT});
  if (val == "stop_position" || val == "platform" || val == "station") {
    nt->bit_public_transport = 1;
  }

  // Handle traffic_calming=*
  val = pti.FindValue({KEY_BIT_TRAFFIC_CALMING});
  if (!val.empty() && val != "no") {
    nt->bit_traffic_calming = 1;
  }
}

#if 0
// Get per-vehicle acccess for barrier of type 'barrier'.
bool GetBarrierAccess(const ParsedTagInfo& pti, int64_t node_id,
                      ACCESS vh_acc[VH_MAX]) {
  /*
  These tags are assumed to block passage through the node for all vehicle
  types. Since this is the default also for unknown barrier types, the list
  isn't actually needed.
  static const std::string_view full_blocks[] = {
      "debris",    "chain",        "barrier_board", "jersey_barrier",
      "log",       "rope",         "yes",           "hampshire_gate",
      "sliding_beam", "sliding_gate",  "spikes",
      "wedge",     "wicket_gate",
  };
  */

  static const std::string_view no_blocks[] = {
      "border_control", "bump_gate", "cattle_grid", "coupure",
      "entrance",       "lift_gate", "sally_port",  "toll_booth"};

  std::string_view barrier = pti.FindValue({KEY_BIT_BARRIER});
  if (barrier.empty()) {
    return false;
  }
  std::fill_n(vh_acc, VH_MAX, ACC_NO);
  if (barrier == "stile" || barrier == "turnstile" ||
      barrier == "full-height_turnstile" || barrier == "kerb" ||
      barrier == "kissing_gate" || barrier == "log") {
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
    if (pti.FindValue({KEY_BIT_LOCKED}) != "yes") {
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
    // blocks no traffic.
    std::fill_n(vh_acc, VH_MAX, ACC_YES);
  } else {
    LOG_S(INFO) << "Unknown barrier: " << barrier << ":" << node_id;
    // Assume everything else is blocking. This includes "full_blocks".
  }
  return true;
}
#endif

std::vector<std::bitset<VH_MAX>> ComputeBarrierVehicleAllowed() {
  std::vector<std::bitset<VH_MAX>> res;
  for (const BarrierDef bd : g_barrier_def_vector) {
    std::bitset<VH_MAX> bs;
    for (VEHICLE vh = VH_MOTORCAR; vh < VH_MAX; vh = (VEHICLE)(vh + 1)) {
      if (SpanContains(VehicleToString(vh),
                       absl::StrSplit(bd.allowed_vehicles, ','))) {
        bs.set(vh);
      }
    }
    res.push_back(bs);
  }
  return res;
}

// Assign the allowed vehicle types for a barrier. For instance, a turnstile
// only allows pedestrians.
// For barriers, we apply access tags in the following order:
//   1) "barrier=" type sets the default.
//   2) "access=" overrides everything that barrier type might have set.
//   3) "motor_vehicle=" sets access for all motorized traffic.
//   4) "bicycle=", "foot=", "horse=" etc. set access for individual vehicle
//      types.
inline void SetBarrierRestrictions(VEHICLE vt, const ParsedTagInfo& pti,
                                   int64_t node_id, NodeTags* node_tags) {
  node_tags->barrier_type = BarrierToEnum(pti.FindValue({KEY_BIT_BARRIER}));
  if (node_tags->barrier_type == BARRIER_MAX) {
    return;  // No 'barrier=*'
  }

  // A vector that for each barrier has the allowed vehicles as a std::bitset.
  static const std::vector<std::bitset<VH_MAX>> barrier_allowed_vhs =
      ComputeBarrierVehicleAllowed();

  ACCESS access = barrier_allowed_vhs.at(node_tags->barrier_type).test(vt)
                      ? ACC_YES
                      : ACC_NO;

  AccessPerDirection apd;
  if (vt == VH_MOTORCAR) {
    apd = CarAccess(pti.tagh(), node_id, pti.tags(),
                    {.acc_forw = access, .acc_backw = access});
  } else if (vt == VH_BICYCLE) {
    apd = BicycleAccess(pti.tagh(), node_id, pti.tags(),
                        {.acc_forw = access, .acc_backw = access});
  } else {
    ABORT_S() << "vehicle type not (yet) supported:" << (int)vt;
  }

  node_tags->acc_forw = apd.acc_forw;
  node_tags->acc_backw = apd.acc_backw;
  node_tags->has_access_restriction =
      (node_tags->acc_forw != ACC_YES || node_tags->acc_backw != ACC_YES);
}

}  // namespace

// Extract node tags including barrier information stored in the keys_vals
// of nodes. Returns true if relevant tags were found, false if not.
// The tags themselves are returned in 'node_tags'.
NodeTags ParseOSMNodeTags(VEHICLE vt, const ParsedTagInfo& pti,
                          int64_t node_id) {
  if (pti.tags().empty()) {
    return {};
  }

  // LOG_S(INFO) << "BB node:" << node_id;
  NodeTags nt;
  SetCrossingBits(pti, node_id, &nt);
  SetNodeTagBits(vt, pti, node_id, &nt);
  SetBarrierRestrictions(vt, pti, node_id, &nt);

  if (!nt.empty()) {
    nt.node_id = node_id;
  }
  return nt;
}

// Iterate through all nodes and check if it has a barrier which blocks traffic.
// If it does, then add a simple turn restriction that forbids passing the
// obstacle.
//
// Note that traffic within ways having the tag area=yes is not blocked,
// i.e. it is always possible to continue within the area, even if a border node
// of the area has a blocking barrier.
void StoreNodeBarrierData_Obsolete(Graph* g,
                                   build_graph::BuildGraphStats* stats) {
  FUNC_TIMER();

  const std::vector<NodeTags>& v_tags = g->node_tags_sorted;
  size_t nt_idx = 0;
  std::set<uint32_t> way_set;
  std::set<uint32_t> way_with_area_set;

  for (size_t node_idx = 0; node_idx < g->nodes.size(); ++node_idx) {
    while (nt_idx < v_tags.size() &&
           v_tags.at(nt_idx).node_id < g->nodes.at(node_idx).node_id) {
      nt_idx++;
    }
    if (nt_idx >= v_tags.size()) {
      break;
    }

    // LOG_S(INFO) << "Compare " << v_tags.at(nt_idx).node_id << " to "
    //             << g->nodes.at(node_idx).node_id;
    if (v_tags.at(nt_idx).node_id == g->nodes.at(node_idx).node_id) {
      const GNode& n = g->nodes.at(node_idx);
      const NodeTags nt = v_tags.at(nt_idx++);
      if (!nt.has_access_restriction) {
        continue;
      }

      LOG_S(INFO) << "Examine node " << n.node_id;
      const uint32_t num_edges = gnode_num_all_edges(*g, node_idx);
      bool same_target = false;
      bool self_edge = false;
      if (num_edges == 2) {
        uint32_t other1 = g->edges.at(n.edges_start_pos).target_idx;
        uint32_t other2 = g->edges.at(n.edges_start_pos + 1).target_idx;
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

      // TODO: handle access direction.
      bool access_allowed =
          (!nt.has_access_restriction ||
           (RoutableAccess(nt.acc_forw) && RoutableAccess(nt.acc_backw)));
      LOG_S(INFO) << absl::StrFormat(
          "Node barrier on node %lld forward:%u all:%u #w-norm:%llu "
          "#w-area:%llu same-target:%u self-edge:%d acc:%d",
          n.node_id, n.num_forward_edges, num_edges, way_set.size(),
          way_with_area_set.size(), same_target, self_edge,
          (int)access_allowed);

      if (access_allowed) {
        stats->num_node_barrier_free++;
        continue;  // Nothing to do.
      }

      // Iterate overall all incoming edges.
      for (const FullEdge& in_fe : gnode_incoming_edges(*g, node_idx)) {
        const GNode other = g->nodes.at(in_fe.start_idx());
        GEdge& in = g->edges.at(other.edges_start_pos + in_fe.offset());
        CHECK_EQ_S(in.target_idx, node_idx);  // Is it really incoming?
        const bool in_way_area = g->ways.at(in.way_idx).area;
        uint32_t allowed_edge_bits = 0;
        // Iterate outgoing edges at the same node.
        for (uint32_t offset = 0; offset < n.num_forward_edges; ++offset) {
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
               (out.target_idx == in_fe.start_idx()))) {  // u-turn
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
              .from_node_idx = in_fe.start_idx(),
              .way_idx = in.way_idx,
              .to_node_idx = in.target_idx,
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
