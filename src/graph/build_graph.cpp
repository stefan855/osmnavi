#include <map>
#include <mutex>

#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "algos/restricted_edges.h"
#include "base/huge_bitset.h"
#include "geometry/distance.h"
#include "geometry/polygon.h"
#include "graph/build_clusters.h"
#include "graph/build_graph.h"
#include "graph/data_block.h"
#include "graph/graph_def.h"
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

void ConsumeNodeBlob(const OSMTagHelper& tagh,
                     const OSMPBF::PrimitiveBlock& prim_block, std::mutex& mut,
                     const HugeBitset& touched_nodes_ids,
                     DataBlockTable* node_table) {
  NodeBuilder builder;
  for (const OSMPBF::PrimitiveGroup& pg : prim_block.primitivegroup()) {
    NodeBuilder::VNode node = {.id = 0, .lat = 0, .lon = 0};
    for (int i = 0; i < pg.dense().id_size(); ++i) {
      node.id += pg.dense().id(i);
      node.lat += pg.dense().lat(i);
      node.lon += pg.dense().lon(i);
      if (touched_nodes_ids.GetBit(node.id)) {
        builder.AddNode(node);
        if (builder.pending_nodes() >= 128) {
          std::unique_lock<std::mutex> l(mut);
          builder.AddBlockToTable(node_table);
        }
      }
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
// String that contains the tags relevant for routing in readable form. This is
// used to log tag-signatures and frequencies, for instance to find interesting
// cases of usage of tags.
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
                                              const OSMPBF::Way& osm_way) {
  std::vector<NodeCountry> node_countries;
  std::int64_t running_id = 0;
  for (int ref_idx = 0; ref_idx < osm_way.refs().size(); ++ref_idx) {
    running_id += osm_way.refs(ref_idx);
    NodeBuilder::VNode node;
    if (!NodeBuilder::FindNode(*meta.node_table, running_id, &node)) {
      // Node is referenced by osm_way, but the node was not loaded.
      // This happens often when a clipped country file does contain a way but
      // not all the nodes of the way.
      // LOG_S(INFO) << "Way " << osm_way.id() << " references non-existing"
      //                " node " << running_id;
      continue;
    }
    uint16_t ncc = meta.tiler->GetCountryNum(node.lon, node.lat);
    node_countries.push_back({.id = running_id, .ncc = ncc});
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
void MarkSeenAndNeeded(GraphMetaData* meta,
                       const std::vector<NodeCountry>& ncs) {
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
    wsa->ra[RAinWSAIndex(VH_MOTOR_VEHICLE, DIR_FORWARD)] = ra_forw;
  }

  if (acc_backw && IsDirBackward(direction) && ra_backw.maxspeed > 0) {
    wsa->ra[RAinWSAIndex(VH_MOTOR_VEHICLE, DIR_BACKWARD)] = ra_backw;
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
  CHECK_LT_S(wc.way.highway_label, HW_MAX);
  const std::vector<NodeCountry> node_countries =
      ExtractNodeCountries(*meta, osm_way);
  if (node_countries.size() <= 1) {
    std::unique_lock<std::mutex> l(mut);
    meta->stats.num_ways_missing_nodes++;
    return;
  }
  // Sets way.ncc and way.uniform_country.
  SetWayCountryCode(node_countries, &wc.way);
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

    if (vt == VH_MOTOR_VEHICLE) {
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

    MarkSeenAndNeeded(meta, node_countries);

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
    // n.num_edges_out++;
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
#if 0
  // Should not happen because we preallocated needed space.
  ABORT_S() << absl::StrFormat(
      "Cannot store edge at node:%lld start_idx:%u num_edges:%u other_idx:%u "
      "out:%d way_idx:%u dist:%llu",
      n.node_id, start_idx, num_edges, other_idx, out, way_idx, distance_cm);
#endif
}

#if 0
void MarkUniqueOther(GNode* n) {
  GEdge* edges = n->edges;
  for (size_t i = 0; i < gnode_total_edges(*n); ++i) {
    size_t k = 0;
    while (k < i) {
      if (edges[i].other_node_idx == edges[k].other_node_idx) {
        break;
      }
      k++;
    }
    edges[i].unique_other = (i == k);
  }
}
#endif

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

void ApplyTarjan(Graph& g, GraphMetaData* meta) {
  FUNC_TIMER();
  if (g.large_components.empty()) {
    ABORT_S() << "g.large_components is empty";
  }
  Tarjan t(g);
  std::vector<Tarjan::BridgeInfo> bridges;
  for (const Graph::Component& comp : g.large_components) {
    bridges.clear();
    t.FindBridges(comp, &bridges);
    // Sort bridges in descending order of subtrees. If a dead-end contains
    // other, smaller dead-ends, then only the dead-end with the largest subtree
    // is actually used.
    std::sort(bridges.begin(), bridges.end(),
              [](const Tarjan::BridgeInfo& a, const Tarjan::BridgeInfo& b) {
                return a.subtree_size > b.subtree_size;
              });
    for (const Tarjan::BridgeInfo& bridge : bridges) {
      MarkBridgeEdge(g, bridge.from_node_idx, bridge.to_node_idx);
      MarkBridgeEdge(g, bridge.to_node_idx, bridge.from_node_idx);
      meta->stats.num_dead_end_nodes +=
          MarkDeadEndNodes(g, bridge.to_node_idx, bridge.subtree_size);
    }
  }
  LOG_S(INFO) << absl::StrFormat(
      "Graph has %u (%.2f%%) dead end nodes.", meta->stats.num_dead_end_nodes,
      (100.0 * meta->stats.num_dead_end_nodes) / g.nodes.size());
}

void ConsumeRelation(const OSMTagHelper& tagh, const OSMPBF::Relation& osm_rel,
                     std::mutex& mut, GraphMetaData* meta) {
  // TODO: Handle turn restrictions (currently we're only reading them).
  TurnRestriction tr;
  ResType rt =
      ParseTurnRestriction(tagh, osm_rel, meta->opt.log_turn_restrictions, &tr);
  if (rt == ResType::Ignore) return;

  if (rt == ResType::Success &&
      ConnectTurnRestriction(meta->graph, meta->opt.log_turn_restrictions,
                             &tr)) {
    std::unique_lock<std::mutex> l(mut);
    meta->turn_restrictions.push_back(tr);
  } else {
    std::unique_lock<std::mutex> l(mut);
    meta->stats.num_turn_restriction_errors++;
  }
}

// Read the ways that might useful for routing, remember the nodes ids touched
// by these ways, then read the node coordinates and store them in 'node_table'.
void LoadNodeCoordinates(OsmPbfReader* reader, DataBlockTable* node_table,
                         BuildGraphStats* stats) {
  HugeBitset touched_nodes_ids;
  // Read ways and remember the touched nodes in 'touched_nodes_ids'.
  reader->ReadWays([&touched_nodes_ids, stats](const OSMTagHelper& tagh,
                                               const OSMPBF::Way& way,
                                               std::mutex& mut) {
    ConsumeWayStoreSeenNodesWorker(tagh, way, mut, &touched_nodes_ids, stats);
  });

  // Read all the node coordinates for nodes in 'touched_nodes_ids'.
  reader->ReadBlobs(
      OsmPbfReader::ContentNodes,
      [&touched_nodes_ids, node_table](const OSMTagHelper& tagh,
                                       const OSMPBF::PrimitiveBlock& prim_block,
                                       std::mutex& mut) {
        ConsumeNodeBlob(tagh, prim_block, mut, touched_nodes_ids, node_table);
      });
  // Make node table searchable, so we can look up lat/lon by node_id.
  node_table->Sort();
}

void SortGWays(GraphMetaData* meta) {
  FUNC_TIMER();
  // Sort by ascending way_id.
  std::sort(meta->graph.ways.begin(), meta->graph.ways.end(),
            [](const GWay& a, const GWay& b) { return a.id < b.id; });
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
      // n.num_edges_inverted = 0;
      // n.num_edges_out = 0;
      n.dead_end = 0;
      n.ncc = INVALID_NCC;
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

    std::vector<size_t> node_idx =
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

          graph.nodes.at(prev_idx).edges_start_pos++;
          graph.nodes.at(idx).edges_start_pos++;

          // TODO: self edges?
          // CHECK_NE_S(idx1, idx2) << way.id;

#if 0

          if (WSAAnyRoutable(wsa, DIR_FORWARD) &&
              WSAAnyRoutable(wsa, DIR_BACKWARD)) {
            graph.nodes.at(prev_idx).num_edges_out++;
            graph.nodes.at(idx).num_edges_out++;
          } else if (WSAAnyRoutable(wsa, DIR_FORWARD)) {
            graph.nodes.at(prev_idx).num_edges_out++;
            graph.nodes.at(idx).num_edges_inverted++;
          } else {
            CHECK_S(WSAAnyRoutable(wsa, DIR_BACKWARD)) << way.id;
            graph.nodes.at(prev_idx).num_edges_inverted++;
            graph.nodes.at(idx).num_edges_out++;
          }
#endif
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
  meta->graph.edges.resize(edge_start, {.other_node_idx = INFU32});

#if 0
    for (GNode& n : meta->graph.nodes) {
      const std::uint32_t num_edges = gnode_total_edges(n);
      CHECK_GT_S(num_edges, 0u);
      if (num_edges > 0) {
        n.edges = (GEdge*)meta->graph.aligned_pool_.AllocBytes(num_edges *
                                                               sizeof(GEdge));
        for (size_t k = 0; k < num_edges; ++k) {
          n.edges[k].other_node_idx = INFU32;
        }
      }
    }
#endif
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
          GetRAFromWSA(wsa, VH_MOTOR_VEHICLE, DIR_FORWARD).access;
      const ACCESS acc_car_b =
          GetRAFromWSA(wsa, VH_MOTOR_VEHICLE, DIR_BACKWARD).access;
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

void MarkUniqueEdges(GraphMetaData* meta) {
  FUNC_TIMER();
  /*
  for (GNode& n : meta->graph.nodes) {
    MarkUniqueOther(&n);
  }
  */
  for (uint32_t i = 0; i < meta->graph.nodes.size(); ++i) {
    MarkUniqueOther(gnode_all_edges(meta->graph, i));
  }
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
                                                    VH_MOTOR_VEHICLE, &cluster);
      });
    }
    // Faster with few threads only.
    pool.Start(std::min(5, meta->opt.n_threads));
    pool.WaitAllFinished();
  }
}

void ExecuteLouvain(const bool merge_tiny_clusters, GraphMetaData* meta) {
  FUNC_TIMER();
  build_clusters::ExecuteLouvainStages(meta->opt.n_threads, &meta->graph);
  build_clusters::UpdateGraphClusterInformation(&meta->graph);

  if (merge_tiny_clusters) {
    build_clusters::MergeTinyClusters(&(meta->graph));
    build_clusters::UpdateGraphClusterInformation(&meta->graph);
  }

  // build_clusters::StoreClusterInformation(gvec, &meta->graph);
  build_clusters::PrintClusterInformation(meta->graph);

  ComputeShortestPathsInAllClusters(meta);

  if (meta->opt.check_shortest_cluster_paths) {
    // Check if astar and dijkstra find the same shortest paths.
    build_clusters::CheckShortestClusterPaths(meta->graph, meta->opt.n_threads);
  }
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
    const RoutingAttrs ra_forw =
        GetRAFromWSA(wsa, VH_MOTOR_VEHICLE, DIR_FORWARD);
    const RoutingAttrs ra_backw =
        GetRAFromWSA(wsa, VH_MOTOR_VEHICLE, DIR_BACKWARD);

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

    int64_t num_edges_inverted = 0;
    int64_t num_edges_out = 0;
    for (const GEdge& edge : gnode_all_edges(g, node_idx)) {
      // for (size_t edge_pos = 0; edge_pos < gnode_total_edges(n); ++edge_pos)
      // { const GEdge& edge = n.edges[edge_pos];
      if (!edge.unique_other) {
        stats.num_edges_non_unique++;
      }
      num_edges_inverted += edge.inverted;
      num_edges_out += (edge.inverted == 0);

      if (!edge.inverted && edge.other_node_idx != node_idx) {
        stats.sum_edge_length_cm += edge.distance_cm;
        if (edge.distance_cm == 0) {
          LOG_S(INFO) << "Edge with length 0 from " << n.node_id << " to "
                      << g.nodes.at(edge.other_node_idx).node_id;
        }
        stats.min_edge_length_cm =
            std::min(stats.min_edge_length_cm, (int64_t)edge.distance_cm);
        stats.max_edge_length_cm =
            std::max(stats.max_edge_length_cm, (int64_t)edge.distance_cm);
        stats.num_edges_out_car_restr_unset +=
            (edge.car_label == GEdge::LABEL_UNSET);
        stats.num_edges_out_car_restr_free +=
            (edge.car_label == GEdge::LABEL_FREE);
        stats.num_edges_out_car_restricted +=
            (edge.car_label == GEdge::LABEL_RESTRICTED);
        stats.num_edges_out_car_restricted2 +=
            (edge.car_label == GEdge::LABEL_RESTRICTED_SECONDARY);
        stats.num_edges_out_car_strange +=
            (edge.car_label == GEdge::LABEL_STRANGE);
        CHECK_NE_S(edge.car_label, GEdge::LABEL_TEMPORARY);
        /*
            RestrictedAccess(GetRAFromWSA(g, edge, VH_MOTOR_VEHICLE).access);
            */
        stats.num_edges_out_car_forbidden +=
            !RoutableAccess(GetRAFromWSA(g, edge, VH_MOTOR_VEHICLE).access);
      }
      stats.num_cross_country_edges += (edge.contra_way && edge.cross_country);
    }
    stats.num_edges_inverted += num_edges_inverted;
    stats.num_edges_out += num_edges_out;
    stats.max_edges_inverted =
        std::max(stats.max_edges_inverted, num_edges_inverted);
    stats.max_edges_out = std::max(stats.max_edges_out, num_edges_out);
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
  LOG_S(INFO) << absl::StrFormat("Missing-nodes ways: %12lld",
                                 stats.num_ways_missing_nodes);

  LOG_S(INFO) << "========= Various Stats ==========";
  LOG_S(INFO) << absl::StrFormat("Num var-nodes:      %12lld",
                                 meta.node_table->total_records());
  LOG_S(INFO) << absl::StrFormat(
      "  bytes/var-node:   %12.2f",
      static_cast<double>(meta.node_table->mem_allocated()) /
          meta.node_table->total_records());
  LOG_S(INFO) << absl::StrFormat("Num turn restrictions:%10lld",
                                 meta.turn_restrictions.size());
  LOG_S(INFO) << absl::StrFormat("Num turn restr errors:%10lld",
                                 stats.num_turn_restriction_errors);

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
                                 stats.num_edges_out);
  LOG_S(INFO) << absl::StrFormat("  Num inverted:     %12lld",
                                 stats.num_edges_inverted);
  LOG_S(INFO) << absl::StrFormat("  Num non-unique:   %12lld",
                                 stats.num_edges_non_unique);
  LOG_S(INFO) << absl::StrFormat("  Car restr unset:  %12lld",
                                 stats.num_edges_out_car_restr_unset);
  LOG_S(INFO) << absl::StrFormat("  Car restr free:   %12lld",
                                 stats.num_edges_out_car_restr_free);
  LOG_S(INFO) << absl::StrFormat("  Car restricted:   %12lld",
                                 stats.num_edges_out_car_restricted);
  LOG_S(INFO) << absl::StrFormat("  Car restricted2:  %12lld",
                                 stats.num_edges_out_car_restricted2);
  LOG_S(INFO) << absl::StrFormat("  Car strange:      %12lld",
                                 stats.num_edges_out_car_strange);
  LOG_S(INFO) << absl::StrFormat("  Car forbidden:    %12lld",
                                 stats.num_edges_out_car_forbidden);

  LOG_S(INFO) << absl::StrFormat("  Min edge length:  %12lld",
                                 stats.min_edge_length_cm);
  LOG_S(INFO) << absl::StrFormat("  Max edge length:  %12lld",
                                 stats.max_edge_length_cm);
  LOG_S(INFO) << absl::StrFormat(
      "  Avg edge length   %12.0f",
      (double)stats.sum_edge_length_cm / stats.num_edges_out);
  LOG_S(INFO) << absl::StrFormat(
      "  Num edges/node:   %12.2f",
      static_cast<double>(stats.num_edges_inverted + stats.num_edges_out) /
          graph.nodes.size());
  LOG_S(INFO) << absl::StrFormat("  Max edges out:    %12lld",
                                 stats.max_edges_out);
  LOG_S(INFO) << absl::StrFormat("  Max edges inverted: %10lld",
                                 stats.max_edges_inverted);
  LOG_S(INFO) << absl::StrFormat("  Cross country edges:%10lld",
                                 stats.num_cross_country_edges);

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

  LoadNodeCoordinates(&reader, meta.node_table.get(), &meta.stats);

  {
    DeDuperWithIds<WaySharedAttrs> deduper;
    reader.ReadWays([&deduper, &meta](const OSMTagHelper& tagh,
                                      const OSMPBF::Way& way, std::mutex& mut) {
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

  // TurnRestriction currently not used.
  reader.ReadRelations(
      [&meta](const OSMTagHelper& tagh, const OSMPBF::Relation& osm_rel,
              std::mutex& mut) { ConsumeRelation(tagh, osm_rel, mut, &meta); });

  AllocateGNodes(&meta);
  SetCountryInGNodes(&meta);

  // Now we know exactly which ways we have and which nodes are needed.
  // Creade edges.
  ComputeEdgeCounts(&meta);
  AllocateEdgeArrays(&meta);
  PopulateEdgeArrays(&meta);
  MarkUniqueEdges(&meta);
  for (const GEdge& e : meta.graph.edges) {
    CHECK_S(e.other_node_idx != INFU32);
  }

  // =========================================================================

  {
    NodeBuilder::VNode n;
    bool found = NodeBuilder::FindNode(*meta.node_table, 207718684, &n);
    LOG_S(INFO) << "Search node 207718684 expect lat:473492652 lon:87012469";
    LOG_S(INFO) << absl::StrFormat("Find node   %llu        lat:%llu lon:%llu",
                                   207718684, found ? n.lat : 0,
                                   found ? n.lon : 0);
  }

  FindLargeComponents(&meta.graph);
  ApplyTarjan(meta.graph, &meta);
  LabelAllCarEdges(&meta.graph);

  ExecuteLouvain(meta.opt.merge_tiny_clusters, &meta);

  // Output.
  if (meta.opt.log_way_tag_stats) {
    PrintWayTagStats(meta);
  }
  FillStats(reader, &meta);
  PrintStats(meta);
  LogMemoryUsage();
  return meta;
}
}  // namespace build_graph
