#include <map>
#include <mutex>

#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "base/huge_bitset.h"
#include "geometry/distance.h"
#include "geometry/polygon.h"
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
  // const std::vector<NodeCountry> node_countries;
  // WayTaggedZones rural;
  PerCountryConfig::ConfigValue config_forw;
  PerCountryConfig::ConfigValue config_backw;
  // std::string_view streetname;
  // WriteBuff node_ids_buff;
};

}  // namespace

void ConsumeWayStoreSeenNodesWorker(const OSMTagHelper& tagh,
                                    const OSMPBF::Way& osm_way, std::mutex& mut,
                                    HugeBitset* node_ids,
                                    MetaStatsData* stats) {
  if (HighwayLabelToEnum(tagh.GetValue(osm_way, "highway")) == HW_MAX) {
    return;
  }
  {
    std::unique_lock<std::mutex> l(mut);
    stats->num_ways_with_highway_tag++;
    stats->num_edges_with_highway_tag += (osm_way.refs().size() - 1);
    stats->num_noderefs_with_highway_tag += osm_way.refs().size();
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
std::vector<NodeCountry> ExtractNodeCountries(const MetaData& meta,
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
void MarkSeenAndNeeded(MetaData* meta, const std::vector<NodeCountry>& ncs) {
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
                         std::mutex& mut, MetaData* meta) {
  if (meta->stats.log_way_tag_stats &&
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
                      MetaData* meta) {
  if (osm_way.refs().size() <= 1) {
    LOG_S(INFO) << absl::StrFormat("Ignore way %lld of length %lld",
                                   osm_way.id(), osm_way.refs().size());
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
  bool success = false;

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
  const uint32_t prev_num_unique = deduper->num_unique();

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

    if (prev_num_unique != deduper->num_unique()) {
      LOG_S(INFO) << "Number of unique way-routingattrs "
                  << deduper->num_unique() << " for way " << wc.way.id;
      for (uint32_t i = 0; i < WaySharedAttrs::RA_MAX; ++i) {
        LOG_S(INFO) << "  " << i << ":" << RoutingAttrsDebugString(wsa.ra[i]);
      }
    }
  }
}

}  // namespace build_graph
