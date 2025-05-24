#include <map>
#include <mutex>

#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "algos/restricted_access_edges.h"
#include "base/huge_bitset.h"
#include "geometry/distance.h"
#include "geometry/polygon.h"
#include "graph/build_clusters.h"
#include "graph/build_graph.h"
#include "graph/data_block.h"
#include "graph/graph_def.h"
#include "graph/node_barriers.h"
#include "graph/routing_attrs.h"
#include "graph/routing_config.h"
#include "osm/access.h"
#include "osm/key_bits.h"
#include "osm/maxspeed.h"
#include "osm/oneway.h"
#include "osm/osm_helpers.h"
#include "osm/read_osm_pbf.h"
#include "osm/surface.h"

namespace build_graph {
namespace {
struct NodeCountry {
  int64_t id;
  uint16_t ncc;
};

// Data needed while constructing the way representation of type GWay.
struct WayContext {
  GWay way;
  const OSMPBF::Way& osm_way;
  const std::vector<ParsedTag> ptags;
  PerCountryConfig::ConfigValue config_forw;
  PerCountryConfig::ConfigValue config_backw;
};

void ValidateGraph(const Graph& g) {
  FUNC_TIMER();
  CHECK_LT_S(g.ways.size(), 1ull << WAY_IDX_BITS);
  CHECK_LT_S(g.nodes.size(), 1ull << 32);
  CHECK_LT_S(g.edges.size(), 1ull << 32);
  CHECK_LE_S(g.clusters.size(), MAX_CLUSTER_ID);

  // Check that we don't have duplicate edges (from, to, way_idx) between two
  // nodes.
  for (uint32_t node_idx = 0; node_idx < g.nodes.size() - 1; ++node_idx) {
    const GNode& node = g.nodes.at(node_idx);
    if (node_idx > 0) {
      // nodes are sorted by increasing OSM node_id.
      CHECK_GT_S(node.node_id, g.nodes.at(node_idx - 1).node_id) << node_idx;
    }
    for (uint32_t off1 = 0; off1 < node.num_edges_forward; ++off1) {
      const GEdge& e1 = g.edges.at(node.edges_start_pos + off1);
      for (uint32_t off2 = off1 + 1; off2 < node.num_edges_forward; ++off2) {
        const GEdge& e2 = g.edges.at(node.edges_start_pos + off2);
        if (e1.other_node_idx == e2.other_node_idx &&
            e1.way_idx == e2.way_idx) {
          LOG_S(INFO) << absl::StrFormat(
              "Duplicate edge %lld to %lld way_id:%lld", node.node_id,
              GetGNodeIdSafe(g, e1.other_node_idx),
              GetGWayIdSafe(g, e1.way_idx));
          for (const GEdge& e : gnode_forward_edges(g, node_idx)) {
            LOG_S(INFO) << absl::StrFormat("edge %lld to %lld way_id:%lld",
                                           node.node_id,
                                           GetGNodeIdSafe(g, e.other_node_idx),
                                           GetGWayIdSafe(g, e.way_idx));
          }
          // ABORT_S();
        }
      }
    }
  }
}
}  // namespace

void ConsumeWayStoreSeenNodesWorker(const OSMTagHelper& tagh,
                                    const OSMPBF::Way& osm_way, std::mutex& mut,
                                    HugeBitset* node_ids,
                                    BuildGraphStats* stats) {
  if (HighwayLabelToEnum(tagh.GetValue(osm_way, "highway")) == HW_MAX) {
    return;
  }
  {
    std::unique_lock<std::mutex> l(mut);
    stats->num_ways_with_highway_tag++;
    const size_t ref_size = osm_way.refs().size();
    if (ref_size > 0) {
      stats->num_edges_with_highway_tag += (osm_way.refs().size() - 1);
      stats->num_noderefs_with_highway_tag += osm_way.refs().size();
    }
    std::int64_t running_id = 0;
    for (int ref_idx = 0; ref_idx < osm_way.refs().size(); ++ref_idx) {
      running_id += osm_way.refs(ref_idx);
      node_ids->SetBit(running_id, true);
    }
  }
}

#if 0
namespace {
// Extract access information stored with node.
// Returns false if there is no access restriction.
// Returns true if the node has some access restrictions. The per vehicle access
// is returned in node_attrs.
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

}  // namespace
#endif

void ConsumeNodeBlob(const OSMTagHelper& tagh,
                     const OSMPBF::PrimitiveBlock& prim_block, std::mutex& mut,
                     const HugeBitset& touched_nodes_ids,
                     DataBlockTable* node_table, GraphMetaData* meta) {
  NodeBuilder builder;
  NodeAttributes node_attrs;

  for (const OSMPBF::PrimitiveGroup& pg : prim_block.primitivegroup()) {
    const auto& keys_vals = pg.dense().keys_vals();
    NodeBuilder::VNode node = {.id = 0, .lat = 0, .lon = 0};

    // kv_start points to terminating 0-element of previous node. Before the
    // loop it is therefore on position -1.
    int kv_start = -1;
    for (int i = 0; i < pg.dense().id_size(); ++i) {
      node.id += pg.dense().id(i);
      node.lat += pg.dense().lat(i);
      node.lon += pg.dense().lon(i);

      int kv_stop = ++kv_start;
      while (kv_stop < keys_vals.size() && keys_vals.at(kv_stop) != 0) {
        kv_stop++;
      }
      // Difference is even.
      CHECK_EQ_F((kv_stop - kv_start) & 1, 0);

      if (touched_nodes_ids.GetBit(node.id)) {
        if (ConsumeNodeTags(tagh, node.id, keys_vals, kv_start, kv_stop,
                            &node_attrs)) {
          // Store node_attrs for this node.
          std::unique_lock<std::mutex> l(mut);
          meta->graph.node_attrs.push_back(node_attrs);
        }
        builder.AddNode(node);
        if (builder.pending_nodes() >= 128) {
          std::unique_lock<std::mutex> l(mut);
          builder.AddBlockToTable(node_table);
        }
      }
      kv_start = kv_stop;
    }
  }
  if (builder.pending_nodes() > 0) {
    std::unique_lock<std::mutex> l(mut);
    builder.AddBlockToTable(node_table);
  }
}

WayTaggedZones ExtractWayZones(const OSMTagHelper& tagh,
                               const std::vector<ParsedTag>& ptags) {
  constexpr uint64_t selector_bits =
      GetBitMask(KEY_BIT_MAXSPEED) | GetBitMask(KEY_BIT_ZONE) |
      GetBitMask(KEY_BIT_TRAFFIC) | GetBitMask(KEY_BIT_MOTORROAD);
  constexpr uint64_t modifier_bits = GetBitMask(KEY_BIT_FORWARD) |
                                     GetBitMask(KEY_BIT_BACKWARD) |
                                     GetBitMask(KEY_BIT_BOTH_WAYS);
  WayTaggedZones info;
  for (const ParsedTag& pt : ptags) {
    if ((pt.bits & selector_bits) == 0) {
      continue;
    }

    switch (pt.bits & ~modifier_bits) {
      case GetBitMask(KEY_BIT_MAXSPEED):
      case GetBitMask(KEY_BIT_MAXSPEED, KEY_BIT_SOURCE):
      case GetBitMask(KEY_BIT_MAXSPEED, KEY_BIT_TYPE):
      case GetBitMask(KEY_BIT_MAXSPEED, KEY_BIT_ZONE):
      case GetBitMask(KEY_BIT_ZONE, KEY_BIT_TRAFFIC): {
        uint16_t ncc = INVALID_NCC;
        ENVIRONMENT_TYPE et = ET_ANY;
        std::string_view val = tagh.ToString(pt.val_st_idx);
        if (ParseCountrySpeedParts(val, &ncc, &et)) {
          if (et == ET_RURAL || et == ET_URBAN) {
            if (BitIsContained(KEY_BIT_FORWARD, pt.bits)) {
              info.et_forw = et;
            } else if (BitIsContained(KEY_BIT_BACKWARD, pt.bits)) {
              info.et_backw = et;
            } else {
              info.et_forw = et;
              info.et_backw = et;
            }
          }
          if (ncc != INVALID_NCC) {
            info.ncc = ncc;
          }
        }
      }
      case GetBitMask(KEY_BIT_MOTORROAD): {
        IS_MOTORROAD im =
            tagh.ToString(pt.val_st_idx) == "yes" ? IM_YES : IM_NO;
        if (BitIsContained(KEY_BIT_FORWARD, pt.bits)) {
          info.im_forw = im;
        } else if (BitIsContained(KEY_BIT_BACKWARD, pt.bits)) {
          info.im_backw = im;
        } else {
          info.im_forw = im;
          info.im_backw = im;
        }
      }
    }
  }
  return info;
}

namespace {
// String that contains the tags relevant for routing in readable form. This
// is used to log tag-signatures and frequencies, for instance to find
// interesting cases of usage of tags.
std::string CreateWayTagSignature(const OSMTagHelper& tagh,
                                  const OSMPBF::Way& osm_way) {
  std::vector<std::string> tagvals;
  for (int pos = 0; pos < osm_way.keys().size(); ++pos) {
    std::string_view key = tagh.ToString(osm_way.keys().at(pos));
    std::string_view val = tagh.ToString(osm_way.vals().at(pos));
    if (absl::StrContains(key, "lane") || absl::StrContains(key, "zone") ||
        absl::StrContains(key, "motorroad") || absl::StrContains(key, "car") ||
        absl::StrContains(key, "oneway") ||
        absl::StrContains(key, "junction") ||
        absl::StrContains(key, "tracktype") ||
        absl::StrContains(key, "carriageway") /* "Fahrbahn" ??*/ ||
        absl::StrContains(key, "maxspeed") || absl::StrContains(key, "cycle") ||
        absl::StrContains(key, "overtaking") ||  // yes, no, caution
        absl::StrContains(key, "sidewalk") || absl::StrContains(key, "turn") ||
        absl::StrContains(key, "smoothness") ||
        absl::StrContains(key, "vehicle") || absl::StrContains(key, "access") ||
        absl::StrContains(key, "surface") || absl::StrContains(key, "busway") ||
        val == "lane" || val == "psv" || val == "bus" || val == "taxi" ||
        absl::StrContains(val, "cycle") || absl::StrContains(val, "vehicle") ||
        absl::StrContains(key, "psv") || absl::StrContains(key, "taxi") ||
        absl::StrContains(key, "horse") || absl::StrContains(key, "forward") ||
        absl::StrContains(key, "backward") || key == "highway") {
      if (!absl::StrContains(key, "destination") &&
          !absl::StrContains(key, "cs_dir:") &&
          !absl::StrContains(key, "fixme") &&
          !absl::StrContains(key, "placement") &&
          !absl::StrContains(key, "description") &&
          !absl::StrContains(key, ":note") &&
          !absl::StrContains(key, "note:") &&
          !absl::StrContains(key, "check_date")) {
        tagvals.push_back(
            absl::StrCat(key, "=", tagh.ToString(osm_way.vals().at(pos))));
      }
    }
  }
  std::string res;
  if (tagvals.size() > 0) {
    std::sort(tagvals.begin(), tagvals.end());
    for (const std::string& s : tagvals) {
      absl::StrAppend(&res, s, " ## ");
    }
  }
  return res;
}

// Given a way, return a list of all way nodes with their countries.
std::vector<NodeCountry> ExtractNodeCountries(const GraphMetaData& meta,
                                              const OSMPBF::Way& osm_way,
                                              bool* missing_nodes,
                                              uint32_t* dup_segments) {
  *missing_nodes = false;
  std::vector<NodeCountry> node_countries;
  std::int64_t running_id = 0;
  for (int ref_idx = 0; ref_idx < osm_way.refs().size(); ++ref_idx) {
    running_id += osm_way.refs(ref_idx);
    // Check for repeated nodes/segments. If found, remove them.
    if (node_countries.size() > 0 && node_countries.back().id == running_id) {
      // Repeated node. This is an error that causes headaches, for instance
      // it create edges of length 0.
      (*dup_segments) += 1;
      continue;
    }
    if (node_countries.size() > 1) {
      for (uint32_t i = 0; i < node_countries.size() - 1; ++i) {
        bool dup_segment = false;
        if (node_countries.at(i).id == running_id) {
          // The current node was seen before in the same way.
          // Check if adding running_id would create a duplicate segment.
          // The segment that is added is
          //     node_countries.back().id ----> running_id.
          if (node_countries.at(i + 1).id == node_countries.back().id) {
            dup_segment = true;
            break;
          }
          if (i > 0 &&
              node_countries.at(i - 1).id == node_countries.back().id) {
            dup_segment = true;
            break;
          }
        }
        if (dup_segment) {
          (*dup_segments) += 1;
          continue;
        }
      }
    }
    NodeBuilder::VNode node;
    if (!NodeBuilder::FindNode(*meta.node_table, running_id, &node)) {
      // Node is referenced by osm_way, but the node was not loaded.
      // This happens often when a clipped country file does contain a way but
      // not all the nodes of the way.
      // LOG_S(INFO) << "Way " << osm_way.id() << " references non-existing"
      //                " node " << running_id;
      *missing_nodes = true;
      continue;
    }
    uint16_t ncc = meta.tiler->GetCountryNum(node.lon, node.lat);
    node_countries.push_back({.id = running_id, .ncc = ncc});
  }
  if (*missing_nodes) {
    LOG_S(INFO) << "Way " << osm_way.id() << " has missing node(s) -- country:"
                << (node_countries.empty()
                        ? "<empty>"
                        : CountryNumToString(node_countries.front().ncc));
    return {};
  }
  return node_countries;
}

// Extract the country of a way from "node_countries" and store it in
// way.ncc and way.uniform_country. Note: way.ncc contains the
// country of the first way node and is always set (unless the first way node
// has no country).
void SetWayCountryCode(const std::vector<NodeCountry>& node_countries,
                       GWay* way) {
  CHECK_GT_S(node_countries.size(), 1u);
  way->ncc = node_countries.front().ncc;
  way->uniform_country = 1;
  for (size_t i = 1; i < node_countries.size(); ++i) {
    if (node_countries.at(i).ncc != way->ncc) {
      way->uniform_country = 0;
      /*
      if (node_countries.at(i).ncc < way->ncc &&
          node_countries.at(i).ncc != INVALID_NCC) {
        way->ncc = node_countries.front().ncc;
      }
      */
      return;
    }
  }
}

// Should only be called when mutex-locked.
// Marks all nodes referenced by a way as "seen". If a node is already "seen"
// then it marks it also as "needed", because this means that the point is an
// intersection of two ways.
// Also marks the nodes of edges connecting different countries as "needed".
// This keeps edge distances short when we are not sure about speed limits,
// access rules etc.
void MarkSeenAndNeeded(GraphMetaData* meta, const std::vector<NodeCountry>& ncs,
                       const OSMPBF::Way& osm_way) {
  if (meta->opt.keep_all_nodes) {
    for (const NodeCountry& nc : ncs) {
      meta->way_nodes_seen->SetBit(nc.id, true);
      meta->way_nodes_needed->SetBit(nc.id, true);
    }
    return;
  }

  // Mark nodes at country-crossing edges as 'needed' .
  for (size_t i = 0; i < ncs.size() - 1; ++i) {
    if (ncs.at(i).ncc != ncs.at(i + 1).ncc) {
      meta->way_nodes_needed->SetBit(ncs.at(i).id, true);
      meta->way_nodes_needed->SetBit(ncs.at(i + 1).id, true);
    }
  }

  for (const NodeCountry& nc : ncs) {
    if (meta->way_nodes_seen->GetBit(nc.id)) {
      // Node was seen >= 2 times, Mark as 'needed'.
      meta->way_nodes_needed->SetBit(nc.id, true);
    } else {
      // Not seen before, so mark as 'seen'.
      meta->way_nodes_seen->SetBit(nc.id, true);
    }
  }
  // Start and end nodes are both 'needed'.
  meta->way_nodes_needed->SetBit(ncs.front().id, true);
  meta->way_nodes_needed->SetBit(ncs.back().id, true);
  if (ncs.front().id == ncs.back().id) {
    meta->stats.num_ways_closed++;
  }

  // Find loops in the way nodes and mark all nodes in the loop as needed.
  for (size_t i = 0; i < ncs.size() - 1; ++i) {
    const NodeCountry& nc1 = ncs.at(i);
    for (size_t j = i + 1; j < ncs.size(); ++j) {
      if (ncs.at(j).id == nc1.id) {
        // [i..j] is a loop.
        if (j - i <= 3) {
          LOG_S(INFO) << absl::StrFormat(
              "LOOP of length %llu in way %lld node %lld", j - i, osm_way.id(),
              nc1.id);
        }
        for (size_t k = i; k < j; ++k) {
          meta->way_nodes_needed->SetBit(ncs.at(k).id, true);
        }
      }
    }
  }
}

std::uint16_t LimitedMaxspeed(const PerCountryConfig::ConfigValue& cv,
                              std::uint16_t extracted_maxspeed) {
  std::uint16_t m = cv.dflt.maxspeed;
  if (extracted_maxspeed > 0) {
    m = extracted_maxspeed;
  }
  if (cv.speed_limit > 0 && cv.speed_limit < m) {
    m = cv.speed_limit;
  }
  return m;
}
}  // namespace

inline void ComputeCarWayRoutingData(const OSMTagHelper& tagh,
                                     const WayContext& wc,
                                     WaySharedAttrs* wsa) {
  RoutingAttrs ra_forw = wc.config_forw.dflt;
  RoutingAttrs ra_backw = wc.config_backw.dflt;

  const DIRECTION direction =
      CarRoadDirection(tagh, wc.way.highway_label, wc.way.id, wc.ptags);
  if (direction == DIR_MAX) {
    return;  // For instance wrong vehicle type or some tagging error.
  }
  ra_forw.dir = IsDirForward(direction);
  ra_backw.dir = IsDirBackward(direction);
  CarRoadSurface(tagh, wc.way.id, wc.ptags, &ra_forw, &ra_backw);

  // Set access.
  CarAccess(tagh, wc.way.id, wc.ptags, &ra_forw, &ra_backw);
  bool acc_forw = RoutableAccess(ra_forw.access);
  bool acc_backw = RoutableAccess(ra_backw.access);
  if ((!IsDirForward(direction) || !acc_forw) &&
      (!IsDirBackward(direction) || !acc_backw)) {
    if (acc_forw || acc_backw) {
      LOG_S(INFO) << absl::StrFormat(
          "way %lld has no valid direction for cars\n%s", wc.way.id,
          tagh.GetLoggingStr(wc.osm_way));
    }
    return;  // For instance when "private" or "no".
  }

  // Set maxspeed.
  std::uint16_t maxspeed_forw;
  std::uint16_t maxspeed_backw;
  CarMaxspeedFromWay(tagh, wc.way.id, wc.ptags, &maxspeed_forw,
                     &maxspeed_backw);
  ra_forw.maxspeed = LimitedMaxspeed(wc.config_forw, maxspeed_forw);
  ra_backw.maxspeed = LimitedMaxspeed(wc.config_backw, maxspeed_backw);

  // Store final routing attributes.
  if (acc_forw && IsDirForward(direction) && ra_forw.maxspeed > 0) {
    wsa->ra[RAinWSAIndex(VH_MOTORCAR, DIR_FORWARD)] = ra_forw;
  }

  if (acc_backw && IsDirBackward(direction) && ra_backw.maxspeed > 0) {
    wsa->ra[RAinWSAIndex(VH_MOTORCAR, DIR_BACKWARD)] = ra_backw;
  }
}

inline void ComputeBicycleWayRoutingData(const OSMTagHelper& tagh,
                                         const WayContext& wc,
                                         WaySharedAttrs* wsa) {
  RoutingAttrs ra_forw = wc.config_forw.dflt;
  RoutingAttrs ra_backw = wc.config_backw.dflt;

  const DIRECTION direction =
      BicycleRoadDirection(tagh, wc.way.highway_label, wc.way.id, wc.ptags);
  if (direction == DIR_MAX) {
    return;  // For instance wrong vehicle type or some tagging error.
  }
  ra_forw.dir = IsDirForward(direction);
  ra_backw.dir = IsDirBackward(direction);

  // Set access.
  BicycleAccess(tagh, wc.way.id, wc.ptags, &ra_forw, &ra_backw);
  bool acc_forw = RoutableAccess(ra_forw.access);
  bool acc_backw = RoutableAccess(ra_backw.access);
  if ((!IsDirForward(direction) || !acc_forw) &&
      (!IsDirBackward(direction) || !acc_backw)) {
    if (acc_forw || acc_backw) {
      LOG_S(INFO) << absl::StrFormat(
          "way %lld has no valid direction for bicycles\n%s", wc.way.id,
          tagh.GetLoggingStr(wc.osm_way));
    }
    return;  // For instance when "private" or "no".
  }

  // Set maxspeed.
  std::uint16_t maxspeed_forw;
  std::uint16_t maxspeed_backw;
  BicycleMaxspeedFromWay(tagh, wc.way.id, wc.ptags, &maxspeed_forw,
                         &maxspeed_backw);
  ra_forw.maxspeed = LimitedMaxspeed(wc.config_forw, maxspeed_forw);
  ra_backw.maxspeed = LimitedMaxspeed(wc.config_backw, maxspeed_backw);

  // Store final routing attributes.
  if (acc_forw && IsDirForward(direction) && ra_forw.maxspeed > 0) {
    wsa->ra[RAinWSAIndex(VH_BICYCLE, DIR_FORWARD)] = ra_forw;
  }

  if (acc_backw && IsDirBackward(direction) && ra_backw.maxspeed > 0) {
    wsa->ra[RAinWSAIndex(VH_BICYCLE, DIR_BACKWARD)] = ra_backw;
  }
}

namespace {
void MayStoreWayTagStats(const OSMTagHelper& tagh, const OSMPBF::Way& osm_way,
                         std::mutex& mut, GraphMetaData* meta) {
  if (meta->opt.log_way_tag_stats &&
      !tagh.GetValue(osm_way, "highway").empty()) {
    std::string statkey = CreateWayTagSignature(tagh, osm_way);
    if (!statkey.empty()) {
      std::unique_lock<std::mutex> l(mut);
      meta->stats.way_tag_stats.Add(statkey, osm_way.id());
    }
  }
}

void LogCountryConflict(const WayTaggedZones& rural, const GWay way) {
  if (rural.ncc != INVALID_NCC && way.uniform_country && rural.ncc != way.ncc) {
    LOG_S(INFO) << absl::StrFormat(
        "Defined country different from tagged country %d:%d %s:%s in "
        "way:%lld",
        rural.ncc, way.ncc, CountryNumToString(rural.ncc),
        CountryNumToString(way.ncc), way.id);
  }
}

inline void EncodeNodeIds(const std::vector<NodeCountry>& node_countries,
                          WriteBuff* node_ids_buff) {
  std::vector<uint64_t> node_ids;
  for (const NodeCountry& nc : node_countries) {
    node_ids.push_back(nc.id);
  }
  EncodeUInt(node_ids.size(), node_ids_buff);
  EncodeNodeIds(node_ids, node_ids_buff);
}
}  // namespace

// Check if osm_way is part of the routable network (routable by car etc.) and
// add a record to graph.ways. Also updates 'seen nodes' and 'needed nodes'
// bitsets.
void ConsumeWayWorker(const OSMTagHelper& tagh, const OSMPBF::Way& osm_way,
                      std::mutex& mut, DeDuperWithIds<WaySharedAttrs>* deduper,
                      GraphMetaData* meta) {
  if (osm_way.refs().size() <= 1) {
    LOG_S(INFO) << absl::StrFormat("Ignore way %lld of length %lld",
                                   osm_way.id(), osm_way.refs().size());
    std::unique_lock<std::mutex> l(mut);
    meta->stats.num_ways_too_short++;
    return;
  }

  MayStoreWayTagStats(tagh, osm_way, mut, meta);
  const HIGHWAY_LABEL highway_label =
      HighwayLabelToEnum(tagh.GetValue(osm_way, "highway"));

  if (highway_label == HW_MAX) {
    return;  // Not interesting for routing, ignore.
  }

  WayContext wc = {.osm_way = osm_way, .ptags = ParseTags(tagh, osm_way)};
  wc.way.id = osm_way.id();
  wc.way.highway_label = highway_label;
  wc.way.area = tagh.GetValue(osm_way, "area") == "yes";
  CHECK_LT_S(wc.way.highway_label, HW_MAX);

  bool missing_nodes = false;
  uint32_t dup_segments = 0;
  const std::vector<NodeCountry> node_countries =
      ExtractNodeCountries(*meta, osm_way, &missing_nodes, &dup_segments);
  if (missing_nodes || node_countries.size() <= 1) {
    std::unique_lock<std::mutex> l(mut);
    meta->stats.num_ways_missing_nodes++;
    return;
  }
  // Sets way.ncc and way.uniform_country.
  SetWayCountryCode(node_countries, &wc.way);
  wc.way.closed_way = (node_countries.front().id == node_countries.back().id);
  const WayTaggedZones rural = ExtractWayZones(tagh, wc.ptags);
  LogCountryConflict(rural, wc.way);

  WaySharedAttrs wsa;
  // Set all numbers to 0, enums to first value.
  memset(&wsa.ra, 0, sizeof(wsa.ra));

  for (const VEHICLE vt : WaySharedAttrs::RA_VEHICLES) {
    // TODO: Use real country instead of always using CH (Switzerland).
    wc.config_forw = meta->per_country_config->GetDefault(
        /*wc.way.ncc*/ NCC_CH, wc.way.highway_label, vt, rural.et_forw,
        rural.im_forw);
    wc.config_backw = meta->per_country_config->GetDefault(
        /*wc.way.ncc*/ NCC_CH, wc.way.highway_label, vt, rural.et_backw,
        rural.im_backw);

    if (vt == VH_MOTORCAR) {
      ComputeCarWayRoutingData(tagh, wc, &wsa);
    } else if (vt == VH_BICYCLE) {
      // ComputeBicycleWayRoutingData(tagh, wc, &wsa);
    } else if (vt == VH_FOOT) {
      // TODO
    } else {
      ABORT_S() << "Invalid vehicle type " << vt;
    }
  }

  if (!WSAAnyRoutable(wsa)) return;

  WriteBuff node_ids_buff;
  EncodeNodeIds(node_countries, &node_ids_buff);
  // const uint32_t prev_num_unique = deduper->num_unique();

  // Run modifications of global data behind a mutex.
  {
    meta->stats.num_ways_dup_segments += dup_segments;
    std::unique_lock<std::mutex> l(mut);
    wc.way.node_ids =
        meta->graph.unaligned_pool_.AllocBytes(node_ids_buff.used());
    memcpy(wc.way.node_ids, node_ids_buff.base_ptr(), node_ids_buff.used());

    std::string_view streetname = tagh.GetValue(wc.osm_way, "name");
    if (!streetname.empty()) {
      // allocate a 0-terminated char*.
      wc.way.streetname =
          (char*)meta->graph.unaligned_pool_.AllocBytes(streetname.size() + 1);
      memcpy(wc.way.streetname, streetname.data(), streetname.size());
      wc.way.streetname[streetname.size()] = '\0';
    }

    MarkSeenAndNeeded(meta, node_countries, osm_way);

    wc.way.wsa_id = deduper->Add(wsa);
    meta->graph.ways.push_back(wc.way);

#if 0
    if (prev_num_unique != deduper->num_unique()) {
      LOG_S(INFO) << "Number of unique way-routingattrs "
                  << deduper->num_unique() << " for way " << wc.way.id;
      for (uint32_t i = 0; i < WaySharedAttrs::RA_MAX; ++i) {
        LOG_S(INFO) << "  " << i << ":" << RoutingAttrsDebugString(wsa.ra[i]);
      }
    }
#endif
  }
}

namespace {
void AddEdge(Graph& g, const size_t start_idx, const size_t other_idx,
             const bool inverted, const bool contra_way,
             const bool both_directions, const size_t way_idx,
             const std::uint64_t distance_cm, bool car_restricted) {
  GNode& n = g.nodes.at(start_idx);
  const GNode& other = g.nodes.at(other_idx);
  const int64_t edge_start = n.edges_start_pos;
  const int64_t edge_stop = gnode_edge_stop(g, start_idx);
  int64_t ep;
  if (inverted) {
    for (ep = edge_stop - 1; ep >= edge_start; --ep) {
      if (g.edges.at(ep).other_node_idx == INFU32) break;
    }
    // n.num_edges_inverted++;
  } else {
    for (ep = edge_start; ep < edge_stop; ++ep) {
      if (g.edges.at(ep).other_node_idx == INFU32) break;
    }
    CHECK_LT_S(n.num_edges_forward, MAX_NUM_EDGES_OUT) << n.node_id;
    n.num_edges_forward++;
  }
  CHECK_S(ep >= edge_start && ep < edge_stop);
  CHECK_S(other_idx != INFU32);
  GEdge& e = g.edges.at(ep);
  e.other_node_idx = other_idx;
  e.way_idx = way_idx;
  e.distance_cm = distance_cm;
  e.unique_other = 0;
  e.bridge = 0;
  e.to_bridge = 0;
  e.contra_way = contra_way ? 1 : 0;
  e.inverted = inverted ? 1 : 0;
  e.both_directions = both_directions ? 1 : 0;
  e.cross_country = n.ncc != other.ncc;
  e.car_label = car_restricted ? GEdge::LABEL_RESTRICTED : GEdge::LABEL_UNSET;
  e.car_label_strange = 0;
  e.car_uturn_allowed = 0;
  e.complex_turn_restriction_trigger = 0;
}

void MarkUniqueOther(std::span<GEdge> edges) {
  for (size_t i = 0; i < edges.size(); ++i) {
    size_t k = 0;
    while (k < i) {
      // TODO: C++26 allows .at() with bounds checking.
      if (edges[i].other_node_idx == edges[k].other_node_idx) {
        break;
      }
      k++;
    }
    edges[i].unique_other = (i == k);
  }
}

void FindLargeComponents(Graph* g) {
  ComponentAnalyzer a(*g);
  g->large_components = a.FindLargeComponents();
  ComponentAnalyzer::MarkLargeComponents(g);
}

void ConsumeRelation(const OSMTagHelper& tagh, const OSMPBF::Relation& osm_rel,
                     GraphMetaData* meta, TRResult* result) {
  ParseTurnRestriction(meta->graph, tagh, osm_rel,
                       meta->opt.verb_turn_restrictions, result);
}

// Read the ways that might useful for routing, remember the nodes ids touched
// by these ways, then read the node coordinates and store them in
// 'node_table'.
void LoadNodeCoordinates(OsmPbfReader* reader, DataBlockTable* node_table,
                         GraphMetaData* meta) {
  HugeBitset touched_nodes_ids;
  // Read ways and remember the touched nodes in 'touched_nodes_ids'.
  reader->ReadWays([&touched_nodes_ids, meta](const OSMTagHelper& tagh,
                                              const OSMPBF::Way& way,
                                              int thread_idx, std::mutex& mut) {
    ConsumeWayStoreSeenNodesWorker(tagh, way, mut, &touched_nodes_ids,
                                   &meta->stats);
  });

  // Read all the node coordinates for nodes in 'touched_nodes_ids'.
  reader->ReadBlobs(
      OsmPbfReader::ContentNodes,
      [&touched_nodes_ids, node_table, meta](
          const OSMTagHelper& tagh, const OSMPBF::PrimitiveBlock& prim_block,
          int thread_idx, std::mutex& mut) {
        ConsumeNodeBlob(tagh, prim_block, mut, touched_nodes_ids, node_table,
                        meta);
      });
  // Make node table searchable, so we can look up lat/lon by node_id.
  node_table->Sort();
  // Sort graph.node_attrs.
  std::sort(meta->graph.node_attrs.begin(), meta->graph.node_attrs.end(),
            [](const auto& a, const auto& b) { return a.node_id < b.node_id; });
}

void SortGWays(GraphMetaData* meta) {
  FUNC_TIMER();
  // Sort by ascending way_id.
  std::sort(meta->graph.ways.begin(), meta->graph.ways.end(),
            [](const GWay& a, const GWay& b) { return a.id < b.id; });
}

void MarkNodesWithAttributesAsNeeded(GraphMetaData* meta) {
  FUNC_TIMER();
  for (const NodeAttributes& na : meta->graph.node_attrs) {
    if (meta->way_nodes_seen->GetBit(na.node_id)) {
      meta->way_nodes_needed->SetBit(na.node_id, true);
    }
  }
}

void AllocateGNodes(GraphMetaData* meta) {
  FUNC_TIMER();
  meta->graph.nodes.reserve(meta->way_nodes_needed->CountBits());
  NodeBuilder::GlobalNodeIter iter(*meta->node_table);
  const NodeBuilder::VNode* node;
  while ((node = iter.Next()) != nullptr) {
    if (meta->way_nodes_needed->GetBit(node->id)) {
      GNode n;
      n.node_id = node->id;
      n.large_component = 0;
      n.cluster_id = INVALID_CLUSTER_ID;
      n.cluster_border_node = 0;
      n.edges_start_pos = 0;
      n.num_edges_forward = 0;
      n.dead_end = 0;
      n.ncc = INVALID_NCC;
      n.simple_turn_restriction_via_node = 0;
      n.lat = node->lat;
      n.lon = node->lon;
      meta->graph.nodes.push_back(n);
    }
  }
}

void SetCountryInGNodes(GraphMetaData* meta) {
  FUNC_TIMER();
  // TODO: run with thread pool.
  for (GNode& n : meta->graph.nodes) {
    n.ncc = meta->tiler->GetCountryNum(n.lon, n.lat);
  }
}

// Compute for each node how many edges it has. The temporary edge count is
// stored in n.edges_start_pos.
void ComputeEdgeCountsWorker(size_t start_pos, size_t stop_pos,
                             GraphMetaData* meta, std::mutex& mut) {
  Graph& graph = meta->graph;

  for (size_t way_idx = start_pos; way_idx < stop_pos; ++way_idx) {
    const GWay& way = graph.ways.at(way_idx);
    const WaySharedAttrs& wsa = GetWSA(graph, way);

    std::vector<uint32_t> node_idx =
        meta->graph.GetGWayNodeIndexes(*(meta->way_nodes_needed), way);
    {
      // Update edge counts.
      std::unique_lock<std::mutex> l(mut);
      int64_t prev_idx = -1;
      for (const size_t idx : node_idx) {
        if (prev_idx >= 0) {
          // All edges should be routable for at least one vehicle type.
          CHECK_S(WSAAnyRoutable(wsa, DIR_FORWARD) ||
                  WSAAnyRoutable(wsa, DIR_BACKWARD))
              << way.id;

          // We "abuse" edges_start_pos as edge-counter while building the
          // graph.
          graph.nodes.at(prev_idx).edges_start_pos++;
          graph.nodes.at(idx).edges_start_pos++;

          // TODO: self edges?
          // CHECK_NE_S(idx1, idx2) << way.id;
        }
        prev_idx = idx;
      }
    }
  };
}

void ComputeEdgeCounts(GraphMetaData* meta) {
  FUNC_TIMER();
  std::mutex mut;
  const size_t unit_length = 25000;
  ThreadPool pool;
  for (size_t start_pos = 0; start_pos < meta->graph.ways.size();
       start_pos += unit_length) {
    pool.AddWork([meta, &mut, start_pos, unit_length](int thread_idx) {
      const size_t stop_pos =
          std::min(start_pos + unit_length, meta->graph.ways.size());
      ComputeEdgeCountsWorker(start_pos, stop_pos, meta, mut);
    });
  }
  pool.Start(meta->opt.n_threads);
  pool.WaitAllFinished();
}

void AllocateEdgeArrays(GraphMetaData* meta) {
  FUNC_TIMER();
  size_t edge_start = 0;
  for (GNode& n : meta->graph.nodes) {
    size_t num_edges = n.edges_start_pos;  // Read temporary counter.
    n.edges_start_pos = edge_start;
    edge_start += num_edges;
  }
  CHECK_S(meta->graph.edges.empty());

  meta->graph.edges.reserve(edge_start);
  meta->graph.edges.resize(edge_start, {.other_node_idx = INFU32});
}

void PopulateEdgeArraysWorker(size_t start_pos, size_t stop_pos,
                              GraphMetaData* meta, std::mutex& mut) {
  Graph& graph = meta->graph;
  std::vector<uint64_t> ids;
  std::vector<size_t> node_idx;
  std::vector<uint64_t> dist_sums;

  // Compute distances between the nodes of the way and store
  for (size_t way_idx = start_pos; way_idx < stop_pos; ++way_idx) {
    const GWay& way = graph.ways.at(way_idx);
    ids.clear();
    node_idx.clear();
    dist_sums.clear();
    // Decode node_ids.
    std::uint64_t num_nodes;
    std::uint8_t* ptr = way.node_ids;
    ptr += DecodeUInt(ptr, &num_nodes);
    ptr += DecodeNodeIds(ptr, num_nodes, &ids);

    // Compute distances sum from start and store in distance vector.
    // Non-existing nodes add 0 to the distance.
    NodeBuilder::VNode prev_node = {.id = 0, .lat = 0, .lon = 0};
    int64_t sum = 0;
    for (const uint64_t id : ids) {
      if (meta->way_nodes_seen->GetBit(id)) {
        NodeBuilder::VNode node;
        if (!NodeBuilder::FindNode(*meta->node_table, id, &node)) {
          // Should not happen, all 'seen' nodes should exist.
          ABORT_S() << absl::StrFormat("Way:%llu has missing node %llu", way.id,
                                       id);
        }
        // Sum up distance so far.
        if (prev_node.id != 0) {
          sum += calculate_distance(prev_node.lat, prev_node.lon, node.lat,
                                    node.lon);
        }
        prev_node = node;
      }
      dist_sums.push_back(sum);
      if (meta->way_nodes_needed->GetBit(id)) {
        std::size_t idx = graph.FindNodeIndex(id);
        CHECK_S(idx >= 0 && idx < graph.nodes.size());
        node_idx.push_back(idx);
      } else {
        node_idx.push_back(std::numeric_limits<size_t>::max());
      }
    }
    CHECK_EQ_S(ids.size(), dist_sums.size());
    CHECK_EQ_S(ids.size(), node_idx.size());

    // Go through 'needed' nodes (skip the others) and output edges.
    {
      const WaySharedAttrs& wsa = GetWSA(graph, way);
      const ACCESS acc_car_f =
          GetRAFromWSA(wsa, VH_MOTORCAR, DIR_FORWARD).access;
      const ACCESS acc_car_b =
          GetRAFromWSA(wsa, VH_MOTORCAR, DIR_BACKWARD).access;
      const bool restr_car_f = RestrictedAccess(acc_car_f);
      const bool restr_car_b = RestrictedAccess(acc_car_b);

      std::unique_lock<std::mutex> l(mut);
      int last_pos = -1;
      for (size_t pos = 0; pos < ids.size(); ++pos) {
        uint64_t id = ids.at(pos);
        if (meta->way_nodes_needed->GetBit(id)) {
          if (last_pos >= 0) {
            // Emit edge.
            std::size_t idx1 = node_idx.at(last_pos);
            std::size_t idx2 = node_idx.at(pos);
            uint64_t distance_cm = dist_sums.at(pos) - dist_sums.at(last_pos);
            // Store edges with the summed up distance.

            if (WSAAnyRoutable(wsa, DIR_FORWARD) &&
                WSAAnyRoutable(wsa, DIR_BACKWARD)) {
              AddEdge(graph, idx1, idx2, /*inverted=*/false,
                      /*contra_way=*/false,
                      /*both_directions=*/true, way_idx, distance_cm,
                      restr_car_f);
              AddEdge(graph, idx2, idx1, /*inverted=*/false,
                      /*contra_way=*/true,
                      /*both_directions=*/true, way_idx, distance_cm,
                      restr_car_b);
            } else if (WSAAnyRoutable(wsa, DIR_FORWARD)) {
              AddEdge(graph, idx1, idx2, /*inverted=*/false,
                      /*contra_way=*/false,
                      /*both_directions=*/false, way_idx, distance_cm,
                      restr_car_f);
              AddEdge(graph, idx2, idx1, /*inverted=*/true,
                      /*contra_way=*/true,
                      /*both_directions=*/false, way_idx, distance_cm,
                      restr_car_f);
            } else {
              CHECK_S(WSAAnyRoutable(wsa, DIR_BACKWARD)) << way.id;
              AddEdge(graph, idx2, idx1, /*inverted=*/false,
                      /*contra_way=*/true,
                      /*both_directions=*/false, way_idx, distance_cm,
                      restr_car_b);
              AddEdge(graph, idx1, idx2, /*inverted=*/true,
                      /*contra_way=*/false,
                      /*both_directions=*/false, way_idx, distance_cm,
                      restr_car_b);
            }
          }
          last_pos = pos;
        }
      }
    }
  }
}

void PopulateEdgeArrays(GraphMetaData* meta) {
  FUNC_TIMER();
  std::mutex mut;
  const size_t unit_length = 25000;
  ThreadPool pool;
  for (size_t start_pos = 0; start_pos < meta->graph.ways.size();
       start_pos += unit_length) {
    pool.AddWork([meta, &mut, start_pos, unit_length](int thread_idx) {
      const size_t stop_pos =
          std::min(start_pos + unit_length, meta->graph.ways.size());
      PopulateEdgeArraysWorker(start_pos, stop_pos, meta, mut);
    });
  }
  pool.Start(meta->opt.n_threads);
  pool.WaitAllFinished();
}

#if 0
// Iterate through all nodes and for each check if it has a barrier which blocks
// traffic. If it does, then add a simple turn restriction that forbids passing
// the obstacle.
//
// Note that traffic within ways having the attribute area=yes is not blocked,
// i.e. it is always possible to continue within the area, even if a border node
// of the area has a blocking barrier.
void StoreNodeBarrierData(GraphMetaData* meta) {
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
#endif

void MarkUniqueEdges(GraphMetaData* meta) {
  FUNC_TIMER();
  for (uint32_t i = 0; i < meta->graph.nodes.size(); ++i) {
    MarkUniqueOther(gnode_all_edges(meta->graph, i));
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

void LoadTurnRestrictions(OsmPbfReader* reader, GraphMetaData* meta) {
  std::vector<TRResult> results(reader->n_threads());
  reader->ReadRelations([&meta, &results](const OSMTagHelper& tagh,
                                          const OSMPBF::Relation& osm_rel,
                                          int thread_idx, std::mutex& mut) {
    ConsumeRelation(tagh, osm_rel, meta, &results.at(thread_idx));
  });
  for (const TRResult& res : results) {
    meta->stats.num_turn_restriction_success += res.num_success;
    meta->stats.max_turn_restriction_via_ways = std::max(
        meta->stats.max_turn_restriction_via_ways, res.max_success_via_ways);
    meta->stats.num_turn_restriction_error += res.num_error;
    meta->stats.num_turn_restriction_error_connection +=
        res.num_error_connection;
    for (const TurnRestriction& tr : res.trs) {
      if (tr.via_is_node) {
        CHECK_EQ_S(tr.path.size(), 2);
        meta->simple_turn_restrictions.push_back(tr);
      } else {
        meta->graph.complex_turn_restrictions.push_back(tr);
      }
    }
  }

  SortTurnRestrictions(&(meta->simple_turn_restrictions));
  meta->graph.simple_turn_restriction_map = ComputeSimpleTurnRestrictionMap(
      meta->graph, meta->opt.verb_turn_restrictions,
      meta->simple_turn_restrictions);
  MarkSimpleViaNodes(&(meta->graph));

  SortTurnRestrictions(&(meta->graph.complex_turn_restrictions));
  meta->graph.complex_turn_restriction_map = ComputeComplexTurnRestrictionMap(
      meta->opt.verb_turn_restrictions, meta->graph.complex_turn_restrictions);
  MarkComplexTriggerEdges(&(meta->graph));
}

void ComputeShortestPathsInAllClusters(GraphMetaData* meta) {
  FUNC_TIMER();
  RoutingMetricTime metric;
  if (!meta->graph.clusters.empty()) {
    ThreadPool pool;
    for (GCluster& cluster : meta->graph.clusters) {
      pool.AddWork([meta, &metric, &cluster](int) {
        // TODO: support the other vehicle types.
        build_clusters::ComputeShortestClusterPaths(meta->graph, metric,
                                                    VH_MOTORCAR, &cluster);
      });
    }
    // Faster with few threads only.
    pool.Start(std::min(5, meta->opt.n_threads));
    pool.WaitAllFinished();
  }
}

void ClusterGraph(const BuildGraphOptions& opt, GraphMetaData* meta) {
  FUNC_TIMER();
  build_clusters::ExecuteLouvain(opt.n_threads, opt.align_clusters_to_ncc,
                                 &meta->graph);
  build_clusters::UpdateGraphClusterInformation(opt.align_clusters_to_ncc,
                                                &meta->graph);

  if (opt.align_clusters_to_ncc && opt.merge_tiny_clusters) {
    build_clusters::MergeTinyClusters(&(meta->graph));
    build_clusters::UpdateGraphClusterInformation(opt.align_clusters_to_ncc,
                                                  &meta->graph);
  }

  // build_clusters::StoreClusterInformation(gvec, &meta->graph);
  build_clusters::PrintClusterInformation(meta->graph);

  ComputeShortestPathsInAllClusters(meta);

  if (meta->opt.check_shortest_cluster_paths) {
    // Check if astar and dijkstra find the same shortest paths.
    build_clusters::CheckShortestClusterPaths(meta->graph, meta->opt.n_threads);
  }
  build_clusters::AssignClusterColors(&(meta->graph));
}

void FillStats(const OsmPbfReader& reader, GraphMetaData* meta) {
  FUNC_TIMER();
  const Graph& g = meta->graph;
  BuildGraphStats& stats = meta->stats;

  stats.num_nodes_in_pbf = reader.CountEntries(OsmPbfReader::ContentNodes);
  stats.num_ways_in_pbf = reader.CountEntries(OsmPbfReader::ContentWays);
  stats.num_relations_in_pbf =
      reader.CountEntries(OsmPbfReader::ContentRelations);

  for (const GWay& w : g.ways) {
    const WaySharedAttrs& wsa = GetWSA(g, w);
    const RoutingAttrs ra_forw = GetRAFromWSA(wsa, VH_MOTORCAR, DIR_FORWARD);
    const RoutingAttrs ra_backw = GetRAFromWSA(wsa, VH_MOTORCAR, DIR_BACKWARD);

    if (RoutableAccess(ra_forw.access) && RoutableAccess(ra_backw.access) &&
        ra_forw.maxspeed != ra_backw.maxspeed) {
      stats.num_ways_diff_maxspeed += 1;
    }
    if ((RoutableAccess(ra_forw.access) && ra_forw.maxspeed == 0) ||
        (RoutableAccess(ra_backw.access) && ra_backw.maxspeed == 0)) {
      stats.num_ways_no_maxspeed += 1;
    }
    stats.num_ways_has_country += w.uniform_country;
    stats.num_ways_has_streetname += w.streetname == nullptr ? 0 : 1;
    stats.num_ways_oneway_car +=
        (wsa.ra[0].access == ACC_NO) != (wsa.ra[1].access == ACC_NO);
    if ((FreeAccess(wsa.ra[0].access) && RestrictedAccess(wsa.ra[1].access)) ||
        (RestrictedAccess(wsa.ra[0].access) && FreeAccess(wsa.ra[1].access))) {
      LOG_S(INFO) << "Way has mixed restrictions: " << w.id;
      stats.num_ways_mixed_restricted_car += 1;
    }
  }

  for (size_t node_idx = 0; node_idx < g.nodes.size(); ++node_idx) {
    const GNode& n = g.nodes.at(node_idx);
    if (n.cluster_id != INVALID_CLUSTER_ID) {
      stats.num_nodes_in_cluster++;
    }
    if (n.large_component == 0) {
      stats.num_nodes_in_small_component++;
    }
    stats.num_nodes_no_country += (n.ncc == INVALID_NCC ? 1 : 0);
    stats.num_nodes_simple_tr_via += n.simple_turn_restriction_via_node;
    stats.num_edges_at_simple_tr_via +=
        n.simple_turn_restriction_via_node ? n.num_edges_forward : 0;

    int64_t num_edges_inverted = 0;
    int64_t num_edges_forward = 0;
    for (const GEdge& e : gnode_all_edges(g, node_idx)) {
      const GNode& other = g.nodes.at(e.other_node_idx);
      // const bool edge_dead_end = n.dead_end || other.dead_end;
      if (!e.unique_other) {
        stats.num_edges_non_unique++;
      }
      num_edges_inverted += e.inverted;
      num_edges_forward += (e.inverted == 0);

      if (!e.inverted && e.other_node_idx != node_idx) {
        stats.sum_edge_length_cm += e.distance_cm;
        if (e.distance_cm == 0) {
          LOG_S(INFO) << "Edge with length 0 from " << n.node_id << " to "
                      << other.node_id;
        }
        stats.min_edge_length_cm =
            std::min(stats.min_edge_length_cm, (int64_t)e.distance_cm);
        stats.max_edge_length_cm =
            std::max(stats.max_edge_length_cm, (int64_t)e.distance_cm);
        stats.num_edges_forward_car_restr_unset +=
            (e.car_label == GEdge::LABEL_UNSET);
        stats.num_edges_forward_car_restr_free +=
            (e.car_label == GEdge::LABEL_FREE);
        stats.num_edges_forward_car_restricted +=
            (e.car_label == GEdge::LABEL_RESTRICTED);
        stats.num_edges_forward_car_restricted2 +=
            (e.car_label == GEdge::LABEL_RESTRICTED_SECONDARY);
        stats.num_edges_forward_car_strange += e.car_label_strange;
        CHECK_S(e.car_label_strange == 0 ||
                e.car_label == GEdge::LABEL_RESTRICTED_SECONDARY);
        CHECK_NE_S(e.car_label, GEdge::LABEL_TEMPORARY);
        stats.num_edges_forward_car_forbidden +=
            !RoutableAccess(GetRAFromWSA(g, e, VH_MOTORCAR).access);
      }
      stats.num_cross_country_edges += (e.contra_way && e.cross_country);
      stats.num_cross_country_restricted +=
          (e.contra_way && e.cross_country && e.car_label != GEdge::LABEL_FREE);
      if (n.cluster_id != other.cluster_id &&
          n.cluster_id != INVALID_CLUSTER_ID &&
          other.cluster_id != INVALID_CLUSTER_ID) {
        stats.num_cross_cluster_edges += 1;
        stats.num_cross_cluster_restricted +=
            (e.car_label != GEdge::LABEL_FREE);
        if (e.car_label != GEdge::LABEL_FREE) {
          LOG_S(INFO) << absl::StrFormat(
              "Cross cluster restricted edge %lld to %lld, label:%d way:%lld",
              n.node_id, other.node_id, (int)e.car_label,
              g.ways.at(e.way_idx).id);
        }
        // By construction, this is 0, because dead-ends are omitted from
        // clusters.
        // stats.num_cross_cluster_restricted_dead_end +=
        //     (e.car_label != GEdge::LABEL_FREE && edge_dead_end);
      }
    }
    stats.max_edges =
        std::max(stats.max_edges, num_edges_forward + num_edges_inverted);
    stats.num_edges_forward += num_edges_forward;
    stats.num_edges_inverted += num_edges_inverted;
    stats.max_edges_out = std::max(stats.max_edges_out, num_edges_forward);
    stats.max_edges_inverted =
        std::max(stats.max_edges_inverted, num_edges_inverted);
  }
}

void PrintStats(const GraphMetaData& meta) {
  const Graph& graph = meta.graph;
  const BuildGraphStats& stats = meta.stats;

  LOG_S(INFO) << "=========== Pbf Stats ============";
  LOG_S(INFO) << absl::StrFormat("Nodes:              %12lld",
                                 stats.num_nodes_in_pbf);
  LOG_S(INFO) << absl::StrFormat("Ways:               %12lld",
                                 stats.num_ways_in_pbf);
  LOG_S(INFO) << absl::StrFormat("Relations:          %12lld",
                                 stats.num_relations_in_pbf);
  LOG_S(INFO) << absl::StrFormat("Ways with hw tag:   %12lld",
                                 stats.num_ways_with_highway_tag);
  LOG_S(INFO) << absl::StrFormat("Edges with hw tag:  %12lld",
                                 stats.num_edges_with_highway_tag);
  LOG_S(INFO) << absl::StrFormat("Noderefs with hw tag:%11lld",
                                 stats.num_noderefs_with_highway_tag);
  LOG_S(INFO) << absl::StrFormat("Ways too short:     %12lld",
                                 stats.num_ways_too_short);
  LOG_S(INFO) << absl::StrFormat("Ways with missing nodes:%8lld",
                                 stats.num_ways_missing_nodes);
  LOG_S(INFO) << absl::StrFormat("Ways dup segments:  %12lld",
                                 stats.num_ways_dup_segments);

  LOG_S(INFO) << "========= Various Stats ==========";
  LOG_S(INFO) << absl::StrFormat("Num var-nodes:      %12lld",
                                 meta.node_table->total_records());
  LOG_S(INFO) << absl::StrFormat(
      "  bytes/var-node:   %12.2f",
      static_cast<double>(meta.node_table->mem_allocated()) /
          meta.node_table->total_records());
  LOG_S(INFO) << absl::StrFormat("Num t-restr success: %11lld",
                                 stats.num_turn_restriction_success);
  LOG_S(INFO) << absl::StrFormat("Num t-restr errors:   %10lld",
                                 stats.num_turn_restriction_error);
  LOG_S(INFO) << absl::StrFormat("Num t-restr simple:  %11lld",
                                 meta.simple_turn_restrictions.size());
  LOG_S(INFO) << absl::StrFormat("Num t-restr complex: %11lld",
                                 graph.complex_turn_restrictions.size());
  LOG_S(INFO) << absl::StrFormat("Num t-restr comb/simple:%8lld",
                                 graph.simple_turn_restriction_map.size());
  LOG_S(INFO) << absl::StrFormat("Max t-restr via ways: %10llu",
                                 stats.max_turn_restriction_via_ways);
  LOG_S(INFO) << absl::StrFormat("Num t-restr errors conn:%8lld",
                                 stats.num_turn_restriction_error_connection);
  LOG_S(INFO) << absl::StrFormat("Num node barrier attrs:%9lld",
                                 meta.graph.node_attrs.size());
  LOG_S(INFO) << absl::StrFormat("Num node barrier free:%10lld",
                                 stats.num_node_barrier_free);
  LOG_S(INFO) << absl::StrFormat("Num edge barrier block:%9lld",
                                 stats.num_edge_barrier_block);
  LOG_S(INFO) << absl::StrFormat("Num edge barrier merged:%8lld",
                                 stats.num_edge_barrier_merged);
  LOG_S(INFO) << absl::StrFormat("Num edge barrier no-uturn:%6lld",
                                 stats.num_edge_barrier_no_uturn);

  LOG_S(INFO) << "========= Graph Stats ============";
  std::int64_t way_bytes = graph.ways.size() * sizeof(GWay);
  std::int64_t way_added_bytes = graph.unaligned_pool_.MemAllocated();
  std::int64_t way_shared_attrs_bytes =
      graph.way_shared_attrs.size() * sizeof(WaySharedAttrs);
  LOG_S(INFO) << absl::StrFormat("Ways selected:      %12lld",
                                 graph.ways.size());

  LOG_S(INFO) << absl::StrFormat("  Nodes \"seen\":     %12lld",
                                 meta.way_nodes_seen->CountBits());

  LOG_S(INFO) << absl::StrFormat("  Nodes \"needed\":   %12lld",
                                 meta.way_nodes_needed->CountBits());

  LOG_S(INFO) << absl::StrFormat("  No maxspeed:      %12lld",
                                 stats.num_ways_no_maxspeed);
  LOG_S(INFO) << absl::StrFormat("  Diff maxspeed/dir:%12lld",
                                 stats.num_ways_diff_maxspeed);
  LOG_S(INFO) << absl::StrFormat("  Has country:      %12lld",
                                 stats.num_ways_has_country);
  LOG_S(INFO) << absl::StrFormat(
      "  Has no country:   %12lld",
      graph.ways.size() - stats.num_ways_has_country);
  LOG_S(INFO) << absl::StrFormat("  Has streetname:   %12lld",
                                 stats.num_ways_has_streetname);
  LOG_S(INFO) << absl::StrFormat("  Oneway for cars:  %12lld",
                                 stats.num_ways_oneway_car);
  LOG_S(INFO) << absl::StrFormat("  Mixed restr cars: %12lld",
                                 stats.num_ways_mixed_restricted_car);
  LOG_S(INFO) << absl::StrFormat("  Closed ways:      %12lld",
                                 stats.num_ways_closed);
  LOG_S(INFO) << absl::StrFormat("  Bytes per way     %12.2f",
                                 (double)way_bytes / graph.ways.size());
  LOG_S(INFO) << absl::StrFormat("  Added per way     %12.2f",
                                 (double)way_added_bytes / graph.ways.size());
  LOG_S(INFO) << absl::StrFormat("  Total Bytes:      %12lld",
                                 way_bytes + way_added_bytes);

  LOG_S(INFO) << absl::StrFormat("Needed nodes:       %12lld",
                                 graph.nodes.size());
  LOG_S(INFO) << absl::StrFormat("  Deadend nodes:    %12lld",
                                 stats.num_dead_end_nodes);
  LOG_S(INFO) << absl::StrFormat("  Node in cluster:  %12lld",
                                 stats.num_nodes_in_cluster);
  LOG_S(INFO) << absl::StrFormat("  Node in small comp:%11lld",
                                 stats.num_nodes_in_small_component);
  LOG_S(INFO) << absl::StrFormat("  Nodes no country: %12lld",
                                 stats.num_nodes_no_country);

  std::int64_t node_bytes = graph.nodes.size() * sizeof(GNode);
  std::int64_t edge_memory = graph.edges.size() * sizeof(GEdge);
  LOG_S(INFO) << absl::StrFormat("  Bytes per node    %12.2f",
                                 (double)node_bytes / graph.nodes.size());
  LOG_S(INFO) << absl::StrFormat("  Edge Mem per node %12.2f",
                                 (double)edge_memory / graph.nodes.size());
  LOG_S(INFO) << absl::StrFormat("  Total Bytes:      %12lld",
                                 node_bytes + edge_memory);

  LOG_S(INFO) << "Edges";
  LOG_S(INFO) << absl::StrFormat("  Num out:          %12lld",
                                 stats.num_edges_forward);
  LOG_S(INFO) << absl::StrFormat("  Num inverted:     %12lld",
                                 stats.num_edges_inverted);
  LOG_S(INFO) << absl::StrFormat("  Num non-unique:   %12lld",
                                 stats.num_edges_non_unique);
  LOG_S(INFO) << absl::StrFormat("  Car restr unset:  %12lld",
                                 stats.num_edges_forward_car_restr_unset);
  LOG_S(INFO) << absl::StrFormat("  Car restr free:   %12lld",
                                 stats.num_edges_forward_car_restr_free);
  LOG_S(INFO) << absl::StrFormat("  Car restricted:   %12lld",
                                 stats.num_edges_forward_car_restricted);
  LOG_S(INFO) << absl::StrFormat("  Car restricted2:  %12lld",
                                 stats.num_edges_forward_car_restricted2);
  LOG_S(INFO) << absl::StrFormat("  Car strange:      %12lld",
                                 stats.num_edges_forward_car_strange);
  LOG_S(INFO) << absl::StrFormat("  Car forbidden:    %12lld",
                                 stats.num_edges_forward_car_forbidden);

  LOG_S(INFO) << absl::StrFormat("  Min edge length:  %12lld",
                                 stats.min_edge_length_cm);
  LOG_S(INFO) << absl::StrFormat("  Max edge length:  %12lld",
                                 stats.max_edge_length_cm);
  LOG_S(INFO) << absl::StrFormat(
      "  Avg edge length   %12.0f",
      (double)stats.sum_edge_length_cm / stats.num_edges_forward);
  LOG_S(INFO) << absl::StrFormat(
      "  Num edges/node:   %12.2f",
      static_cast<double>(stats.num_edges_inverted + stats.num_edges_forward) /
          graph.nodes.size());
  LOG_S(INFO) << absl::StrFormat("  Max edges:        %12lld", stats.max_edges);
  LOG_S(INFO) << absl::StrFormat("  Max edges out:    %12lld",
                                 stats.max_edges_out);
  LOG_S(INFO) << absl::StrFormat("  Max edges inverted: %10lld",
                                 stats.max_edges_inverted);

  LOG_S(INFO) << absl::StrFormat("  Cross country edges:%10lld",
                                 stats.num_cross_country_edges);
  LOG_S(INFO) << absl::StrFormat("  Cross country restr:%10lld",
                                 stats.num_cross_country_restricted);

  LOG_S(INFO) << absl::StrFormat("  Cross cluster edges:%10lld",
                                 stats.num_cross_cluster_edges);
  LOG_S(INFO) << absl::StrFormat("  Cross cluster restr:%10lld",
                                 stats.num_cross_cluster_restricted);
  // LOG_S(INFO) << absl::StrFormat("  Cross clust restr+de:%9lld",
  //                                stats.num_cross_cluster_restricted_dead_end);

  LOG_S(INFO) << "========= Memory Stats ===========";
  LOG_S(INFO) << absl::StrFormat("Bitset memory:      %12.2f MB",
                                 (meta.way_nodes_seen->NumUsedBytes() +
                                  meta.way_nodes_needed->NumUsedBytes()) /
                                     1000000.0);
  LOG_S(INFO) << absl::StrFormat("Varnode memory:     %12.2f MB",
                                 meta.node_table->mem_allocated() / 1000000.0);
  LOG_S(INFO) << absl::StrFormat("Node graph memory:  %12.2f MB",
                                 (node_bytes) / 1000000.0);
  LOG_S(INFO) << absl::StrFormat("Edge graph memory:  %12.2f MB",
                                 (edge_memory) / 1000000.0);
  LOG_S(INFO) << absl::StrFormat("Way graph memory:   %12.2f MB",
                                 (way_bytes + way_added_bytes) / 1000000.0);
  LOG_S(INFO) << absl::StrFormat("Way shared attrs:   %12.2f MB",
                                 way_shared_attrs_bytes / 1000000.0);
  LOG_S(INFO) << absl::StrFormat("Total graph memory: %12.2f MB",
                                 (node_bytes + edge_memory + way_bytes +
                                  way_added_bytes + way_shared_attrs_bytes) /
                                     1000000.0);
}

void PrintWayTagStats(const GraphMetaData& meta) {
  const FrequencyTable& ft = meta.stats.way_tag_stats;
  const std::vector<FrequencyTable::Entry> v = ft.GetSortedElements();
  for (size_t i = 0; i < v.size(); ++i) {
    const FrequencyTable::Entry& e = v.at(i);
    char cc[3] = "--";  // Way not found, can easily happen because many ways
                        // are not stored in the graph.
    {
      const GWay* way = meta.graph.FindWay(e.example_id);
      if (way != nullptr) {
        strcpy(cc, ">1");
        if (way->uniform_country) {
          CountryNumToTwoLetter(way->ncc, cc);
        }
      }
    }

    RAW_LOG_F(INFO, "%7lu %10ld id:%10ld %s %s", i, e.ref_count, e.example_id,
              cc, std::string(e.key).c_str());
  }
  LOG_S(INFO) << absl::StrFormat(
      "Number of different ways tag configurations: %ld, total %ld (%.2f%%)",
      ft.TotalUnique(), ft.Total(), (100.0 * ft.TotalUnique()) / ft.Total());
}
}  // namespace

void MarkUTurnAllowedEdges(Graph* g) {
  FUNC_TIMER();
  for (uint32_t from_idx = 0; from_idx < g->nodes.size(); ++from_idx) {
    for (GEdge& e : gnode_forward_edges(*g, from_idx)) {
      e.car_uturn_allowed = IsUTurnAllowedEdge(*g, from_idx, e);
    }
  }
}

GraphMetaData BuildGraph(const BuildGraphOptions& opt) {
  GraphMetaData meta;
  meta.opt = opt;
  meta.way_nodes_seen.reset(new HugeBitset);
  meta.way_nodes_needed.reset(new HugeBitset);
  meta.node_table.reset(new DataBlockTable);

  // Reading is fastest with 7 threads on my hardware.
  OsmPbfReader reader(opt.pbf, std::min(7, opt.n_threads));

  // Combine a few setup operations that can be done in parallel.
  {
    ThreadPool pool;
    // Read file structure of pbf file.
    pool.AddWork([&reader](int thread_idx) { reader.ReadFileStructure(); });
    // Read country polygons and initialise tiler.
    pool.AddWork([&meta](int thread_idx) {
      meta.tiler.reset(new TiledCountryLookup(
          meta.opt.admin_filepattern,
          /*tile_size=*/TiledCountryLookup::kDegreeUnits / 5));
    });
    // Read routing config.
    pool.AddWork([&meta](int) {
      meta.per_country_config.reset(new PerCountryConfig);
      meta.per_country_config->ReadConfig(meta.opt.routing_config);
    });
    pool.Start(std::min(meta.opt.n_threads, 3));
    pool.WaitAllFinished();
  }

  LoadNodeCoordinates(&reader, meta.node_table.get(), &meta);

  {
    DeDuperWithIds<WaySharedAttrs> deduper;
    reader.ReadWays([&deduper, &meta](const OSMTagHelper& tagh,
                                      const OSMPBF::Way& way, int thread_idx,
                                      std::mutex& mut) {
      ConsumeWayWorker(tagh, way, mut, &deduper, &meta);
    });
    {
      // Sort and build the vector with shared way attributes.
      deduper.SortByPopularity();
      meta.graph.way_shared_attrs = deduper.GetObjVector();
      const std::vector<uint32_t> mapping = deduper.GetSortMapping();
      for (GWay& w : meta.graph.ways) {
        w.wsa_id = mapping.at(w.wsa_id);
      }
      LOG_S(INFO) << absl::StrFormat(
          "Shared way attributes de-duping %u -> %u (%.2f%%)",
          deduper.num_added(), deduper.num_unique(),
          (100.0 * deduper.num_unique()) / std::max(1u, deduper.num_added()));
    }
  }
  SortGWays(&meta);
  MarkNodesWithAttributesAsNeeded(&meta);

  AllocateGNodes(&meta);
  SetCountryInGNodes(&meta);

  // Now we know exactly which ways we have and which nodes are needed.
  // Create edges.
  ComputeEdgeCounts(&meta);
  AllocateEdgeArrays(&meta);
  PopulateEdgeArrays(&meta);
  for (const GEdge& e : meta.graph.edges) {
    CHECK_S(e.other_node_idx != INFU32);
  }
  MarkUniqueEdges(&meta);

  // =========================================================================

#if 0
  {
    NodeBuilder::VNode n;
    bool found = NodeBuilder::FindNode(*meta.node_table, 207718684, &n);
    LOG_S(INFO) << "Search node 207718684 expect lat:473492652 lon:87012469";
    LOG_S(INFO) << absl::StrFormat("Find node   %llu        lat:%llu lon:%llu",
                                   207718684, found ? n.lat : 0,
                                   found ? n.lon : 0);
  }
#endif

  LoadTurnRestrictions(&reader, &meta);
  StoreNodeBarrierData(&meta);
  FindLargeComponents(&meta.graph);
  meta.stats.num_dead_end_nodes = ApplyTarjan(meta.graph);
  LabelAllCarEdges(&meta.graph, Verbosity::Brief);
  MarkUTurnAllowedEdges(&(meta.graph));

  ClusterGraph(meta.opt, &meta);

  // Output.
  if (meta.opt.log_way_tag_stats) {
    PrintWayTagStats(meta);
  }
  ValidateGraph(meta.graph);
  FillStats(reader, &meta);
  PrintStats(meta);
  LogMemoryUsage();
  return meta;
}
}  // namespace build_graph
