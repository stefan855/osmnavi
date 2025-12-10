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
