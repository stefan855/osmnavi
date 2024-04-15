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
#include "osm-util/key_bits.h"
#include "osm-util/osm_helpers.h"

namespace build_graph {

namespace {
// Compute a priority that is lower for broad tags (such as "oneway=yes") iand
// higher for more specific tags (such as "oneway:bicycle=no").
// Executing modifications on data in increasing priority order properly
// overwrites broad values with more specific values.
uint16_t ComputeTagPriority(uint64_t bitset) {
  uint16_t p = 0;
  if (bitset & BITSET_MODIFIERS) {
    p += 1;
  }
  if (bitset & BITSET_LANES_INNER) {
    p += 10;
  }
  if (bitset & BITSET_VEHICLE_CLASSES) {
    p += 100;
  }
  if (bitset & BITSET_VEHICLES) {
    p += 1000;
  }
  return p;
}

void SortParsedTagsByPriority(std::vector<ParsedTag>& tags) {
  std::stable_sort(
      tags.begin(), tags.end(), [](const ParsedTag& a, const ParsedTag& b) {
        return ComputeTagPriority(a.bits) < ComputeTagPriority(b.bits);
      });
}
}  // namespace

std::vector<ParsedTag> ParseTags(const OSMTagHelper& tagh,
                                 const OSMPBF::Way& osm_way) {
  std::vector<ParsedTag> ptags;
  for (int pos = 0; pos < osm_way.keys().size(); ++pos) {
    // TODO: force string_view vector.
    std::string_view key = tagh.ToString(osm_way.keys().at(pos));
    uint64_t bits = 0;
    for (std::string_view part : absl::StrSplit(key, ':')) {
      int b = GetKeyPartBitFast(part);
      if (b >= 0) {
        // Differentiate between "lanes" at the start of the key versus in the
        // inner part of the key.
        if (b == KEY_BIT_LANES && bits != 0) {
          b = KEY_BIT_LANES_INNER;
        }
        bits |= GetBitMask(static_cast<uint8_t>(b));
      } else {
        bits = 0;
        break;
      }
    }
    if (bits != 0) {
      ptags.push_back(
          {.bits = bits,
           .val_st_idx = static_cast<uint32_t>(osm_way.vals().at(pos))});
    }
  }
  SortParsedTagsByPriority(ptags);
  return ptags;
}

void ConsumeWayStoreSeenNodesWorker(const OSMTagHelper& tagh,
                                    const OSMPBF::Way& osm_way, std::mutex& mut,
                                    HugeBitset* node_ids) {
  if (ParseHighwayLabel(tagh, osm_way) == HW_MAX) {
    return;
  }
  {
    std::unique_lock<std::mutex> l(mut);
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
        if (builder.pending_nodes() >= 64) {
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

WayRural ExtractWayRural(const OSMTagHelper& tagh,
                         const std::vector<ParsedTag>& ptags) {
  constexpr uint64_t selector_bits =
      GetBitMask(KEY_BIT_MAXSPEED) | GetBitMask(KEY_BIT_ZONE) |
      GetBitMask(KEY_BIT_TRAFFIC) | GetBitMask(KEY_BIT_MOTORROAD);
  constexpr uint64_t modifier_bits = GetBitMask(KEY_BIT_FORWARD) |
                                     GetBitMask(KEY_BIT_BACKWARD) |
                                     GetBitMask(KEY_BIT_BOTH_WAYS);
  WayRural info;
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
// Return a valid maxspeed in the range [0..INFINITE_MAXSPEED], or -1 in case
// there was an error.
int GetSingleMaxspeed(std::string_view val) {
  if (val.empty()) {
    return -1;
  }
  uint16_t maxspeed = 0;
  if (!ParseNumericMaxspeed(val, &maxspeed)) {
    return -1;
  }
  return maxspeed;
}

// Return maxspeed or -1 if it couldn't be extracted.
int InterpretMaxspeedValue(std::string_view val, bool lanes) {
  int maxspeed = -1;
  if (!lanes) {
    maxspeed = GetSingleMaxspeed(val);
  } else {
    for (std::string_view sub : absl::StrSplit(val, '|')) {
      int sub_maxspeed = GetSingleMaxspeed(sub);
      // Only replace if value is "better".
      if (sub_maxspeed > maxspeed) {
        maxspeed = sub_maxspeed;
      }
    }
  }
  return maxspeed;
}
}  // namespace

void CarMaxspeed(const OSMTagHelper& tagh, std::int64_t way_id,
                 const std::vector<ParsedTag>& ptags, RoutingAttrs* ra_forw,
                 RoutingAttrs* ra_backw, bool logging_on) {
  // Special (hard) cases:
  constexpr uint64_t selector_bits = GetBitMask(KEY_BIT_MAXSPEED);
  constexpr uint64_t modifier_bits =
      GetBitMask(KEY_BIT_FORWARD) | GetBitMask(KEY_BIT_BACKWARD) |
      GetBitMask(KEY_BIT_BOTH_WAYS) | GetBitMask(KEY_BIT_LANES_INNER) |
      GetBitMask(KEY_BIT_VEHICLE) | GetBitMask(KEY_BIT_MOTOR_VEHICLE) |
      GetBitMask(KEY_BIT_MOTORCAR);
  // Start with the defaults from config.
  uint16_t maxspeed_forw = ra_forw->maxspeed;
  uint16_t maxspeed_backw = ra_backw->maxspeed;

  for (const ParsedTag& pt : ptags) {
    if ((pt.bits & selector_bits) == 0) {
      continue;
    }

    switch (pt.bits & ~modifier_bits) {
      case GetBitMask(KEY_BIT_MAXSPEED): {
        std::string_view val = tagh.ToString(pt.val_st_idx);
        int maxspeed = InterpretMaxspeedValue(
            val, BitIsContained(KEY_BIT_LANES_INNER, pt.bits));
        if (maxspeed > 0 && maxspeed <= INFINITE_MAXSPEED) {
          if (BitIsContained(KEY_BIT_FORWARD, pt.bits)) {
            ra_forw->maxspeed = maxspeed;
          } else if (BitIsContained(KEY_BIT_BACKWARD, pt.bits)) {
            ra_backw->maxspeed = maxspeed;
          } else {
            // forward/backward are missing, either nothing or both_ways is
            // specified, so set both.
            ra_forw->maxspeed = maxspeed;
            ra_backw->maxspeed = maxspeed;
          }
          break;
        }
      }
      default:
        if (logging_on && (pt.bits & GetBitMask(KEY_BIT_SOURCE, KEY_BIT_TYPE,
                                                KEY_BIT_ZONE)) == 0) {
          LOG_S(INFO) << absl::StrFormat(
              "CarMaxspeed(): way %lld: can't handle %s=%s", way_id,
              KeyPartBitsToString(pt.bits), tagh.ToString(pt.val_st_idx));
        }
    }
  }
}

namespace {
ACCESS InterpretAccessValue(std::string_view val, bool lanes) {
  if (!lanes) {
    return AccessToEnum(val);
  }
  ACCESS acc = ACC_MAX;
  for (std::string_view sub : absl::StrSplit(val, '|')) {
    ACCESS sub_acc = AccessToEnum(sub);
    // Only replace if value is "better".
    if (sub_acc != ACC_MAX && (acc == ACC_MAX || sub_acc > acc)) {
      acc = sub_acc;
    }
  }
  return acc;
}
}  // namespace

void CarAccess(const OSMTagHelper& tagh, std::int64_t way_id,
               const std::vector<ParsedTag>& ptags, RoutingAttrs* ra_forw,
               RoutingAttrs* ra_backw, bool logging_on) {
  // Special (hard) cases:
  // 1) access:lanes:backward=motorcar;motorcycle|hgv (TODO!)
  // 2) lanes:motor_vehicle=yes|no|yes
  // 3) access:both_ways=no ::
  //    access:lanes:backward=yes|no ::
  //    access:lanes:forward=yes|no ::
  constexpr uint64_t selector_bits =
      GetBitMask(KEY_BIT_ACCESS) | GetBitMask(KEY_BIT_VEHICLE) |
      GetBitMask(KEY_BIT_MOTOR_VEHICLE) | GetBitMask(KEY_BIT_MOTORCAR);
  constexpr uint64_t modifier_bits =
      GetBitMask(KEY_BIT_FORWARD) | GetBitMask(KEY_BIT_BACKWARD) |
      GetBitMask(KEY_BIT_BOTH_WAYS) | GetBitMask(KEY_BIT_LANES_INNER);
  // Start with the defaults from config.
  ACCESS acc_forw = ra_forw->access;
  ACCESS acc_backw = ra_backw->access;

  for (const ParsedTag& pt : ptags) {
    if ((pt.bits & selector_bits) == 0) {
      continue;
    }

    switch (pt.bits & ~modifier_bits) {
      case GetBitMask(KEY_BIT_ACCESS):
      case GetBitMask(KEY_BIT_ACCESS, KEY_BIT_VEHICLE):
      case GetBitMask(KEY_BIT_ACCESS, KEY_BIT_MOTOR_VEHICLE):
      case GetBitMask(KEY_BIT_ACCESS, KEY_BIT_MOTORCAR):
      // Instead of access:motorcar=... one can say motorcar=...
      case GetBitMask(KEY_BIT_VEHICLE):
      case GetBitMask(KEY_BIT_MOTOR_VEHICLE):
      case GetBitMask(KEY_BIT_MOTORCAR): {
        std::string_view val = tagh.ToString(pt.val_st_idx);
        ACCESS acc = InterpretAccessValue(
            val, BitIsContained(KEY_BIT_LANES_INNER, pt.bits));
        if (acc == ACC_MAX) {
          // In general, if we don't know the access value, then it has to be
          // interpreted as a 'no'.
          // For instance, 'agricultural' or 'forestry' will go here.
          acc = ACC_NO;
        }
        if (BitIsContained(KEY_BIT_FORWARD, pt.bits)) {
          ra_forw->access = acc;
        } else if (BitIsContained(KEY_BIT_BACKWARD, pt.bits)) {
          ra_backw->access = acc;
        } else {
          // forward/backward are missing, either nothing or both_ways is
          // specified, so set both.
          ra_forw->access = acc;
          ra_backw->access = acc;
        }
        break;
      }
      default:
        if (logging_on) {
          LOG_S(INFO) << absl::StrFormat(
              "CarAccess(): way %lld: can't handle %s=%s", way_id,
              KeyPartBitsToString(pt.bits), tagh.ToString(pt.val_st_idx));
        }
    }
  }
}

DIRECTION RoadDirection(const OSMTagHelper& tagh, const HIGHWAY_LABEL hw,
                        int64_t way_id, const std::vector<ParsedTag>& ptags,
                        bool logging_on) {
  constexpr uint64_t selector_bits =
      GetBitMask(KEY_BIT_ONEWAY) | GetBitMask(KEY_BIT_VEHICLE) |
      GetBitMask(KEY_BIT_MOTOR_VEHICLE) | GetBitMask(KEY_BIT_MOTORCAR);

  DIRECTION dir = DIR_BOTH;
  bool found_oneway = false;
  if (hw == HW_MOTORWAY || hw == HW_MOTORWAY_JUNCTION ||
      hw == HW_MOTORWAY_LINK) {
    dir = DIR_FORWARD;
  }

  for (const ParsedTag& pt : ptags) {
    if (GetBitMask(KEY_BIT_JUNCTION) == pt.bits && !found_oneway &&
        tagh.ToString(pt.val_st_idx) == "roundabout") {
      dir = DIR_FORWARD;
      continue;
    }

    if (!BitIsContained(KEY_BIT_ONEWAY, pt.bits)) {
      continue;
    }

    if (BitsetIncludedIn(pt.bits, selector_bits)) {
      std::string_view oneway_val = tagh.ToString(pt.val_st_idx);
      if (oneway_val == "yes") {
        dir = DIR_FORWARD;
      } else if (oneway_val == "-1") {
        dir = DIR_BACKWARD;
      } else if (oneway_val == "no") {
        dir = DIR_BOTH;
      } else {
        // For instance "reversible" or "alternating", which are not handled.
        dir = DIR_MAX;
      }
      found_oneway = true;
    } else {
      if (logging_on) {
        LOG_S(INFO) << absl::StrFormat(
            "RoadDirection(): way %lld: can't handle %s=%s", way_id,
            KeyPartBitsToString(pt.bits), tagh.ToString(pt.val_st_idx));
      }
    }
  }
  return dir;
}

namespace {
std::string CreateWayTagSignature(const OSMTagHelper& tagh,
                                  const OSMPBF::Way& osm_way) {
  std::vector<std::string> tagvals;
  for (int pos = 0; pos < osm_way.keys().size(); ++pos) {
    std::string_view key = tagh.ToString(osm_way.keys().at(pos));
    std::string_view val = tagh.ToString(osm_way.vals().at(pos));
    if (absl::StrContains(key, "lanes") || absl::StrContains(key, "zone") ||
        absl::StrContains(key, "motorroad") ||
        absl::StrContains(key, "oneway") ||
        absl::StrContains(key, "junction") ||
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
    // tagvals.push_back(
    //       absl::StrCat("highway=", tagh.GetValue(osm_way, tagh.highway_)));
    std::sort(tagvals.begin(), tagvals.end());
    for (const std::string& s : tagvals) {
      absl::StrAppend(&res, s, " :: ");
    }
  }
  return res;
}

struct NodeCountry {
  int64_t id;
  uint16_t ncc;
};

std::vector<NodeCountry> ExtractNodeCountries(const MetaData& meta,
                                              const OSMPBF::Way& osm_way) {
  std::vector<NodeCountry> node_countries;
  std::int64_t running_id = 0;
  for (int ref_idx = 0; ref_idx < osm_way.refs().size(); ++ref_idx) {
    running_id += osm_way.refs(ref_idx);
    NodeBuilder::VNode node;
    if (!NodeBuilder::FindNode(meta.nodes, running_id, &node)) {
      // Node is referenced by way, but was not loaded.
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
void ExtractWayCountryFromNodes(const std::vector<NodeCountry>& node_countries,
                                GWay* way) {
  CHECK_GT_S(node_countries.size(), 1u);
  way->ncc = node_countries.front().ncc;
  way->uniform_country = 1;
  for (size_t i = 1; i < node_countries.size(); ++i) {
    if (node_countries.at(i).ncc != way->ncc) {
      way->uniform_country = 0;
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
void MarkSeenAndNeeded(MetaData* meta, const std::vector<NodeCountry>& v) {
  for (size_t i = 0; i < v.size() - 1; ++i) {
    if (v.at(i).ncc != v.at(i + 1).ncc) {
      meta->way_nodes_needed.SetBit(v.at(i).id, true);
      meta->way_nodes_needed.SetBit(v.at(i + 1).id, true);
    }
  }

  for (const NodeCountry& nc : v) {
    if (meta->way_nodes_seen.GetBit(nc.id)) {
      // Nodes referenced by multiple ways are 'needed'.
      meta->way_nodes_needed.SetBit(nc.id, true);
    } else {
      meta->way_nodes_seen.SetBit(nc.id, true);
    }
  }
  meta->way_nodes_needed.SetBit(v.front().id, true);
  meta->way_nodes_needed.SetBit(v.back().id, true);
  if (v.front().id == v.back().id) {
    meta->hlp.num_ways_closed++;
  }
}
}  // namespace

bool ComputeWayRoutingData(const MetaData& meta, const OSMTagHelper& tagh,
                           const OSMPBF::Way& osm_way, GWay* way) {
  const std::vector<ParsedTag> ptags = ParseTags(tagh, osm_way);

  const WayRural rural = ExtractWayRural(tagh, ptags);
  if (rural.ncc != INVALID_NCC && way->uniform_country &&
      rural.ncc != way->ncc) {
    LOG_S(INFO) << absl::StrFormat(
        "Defined country different from computed country %d:%d %s:%s in "
        "way:%lld",
        rural.ncc, way->ncc, CountryNumToString(rural.ncc),
        CountryNumToString(way->ncc), way->id);
  }

  // TODO(HACK): Use NCC_CH to get routable data in all countries
  RoutingAttrs ra_forw = meta.per_country_config.GetDefault(
      /*way->ncc*/ NCC_CH, way->highway_label, VH_MOTOR_VEHICLE, rural.et_forw,
      rural.im_forw);

  RoutingAttrs ra_backw = meta.per_country_config.GetDefault(
      /*way->ncc*/ NCC_CH, way->highway_label, VH_MOTOR_VEHICLE, rural.et_backw,
      rural.im_backw);

  way->dir_dontuse = RoadDirection(tagh, way->highway_label, way->id, ptags);
  if (way->dir_dontuse == DIR_MAX) {
    return false;  // For instance wrong vehicle type or some tagging error.
  }

  CarAccess(tagh, way->id, ptags, &ra_forw, &ra_backw);
  bool rforw = RoutableAccess(ra_forw.access);
  bool rbackw = RoutableAccess(ra_backw.access);
  if ((!IsDirForward(way->dir_dontuse) || !rforw) &&
      (!IsDirBackward(way->dir_dontuse) || !rbackw)) {
    if (rforw || rbackw) {
      LOG_S(INFO) << absl::StrFormat("way %lld has no valid direction\n%s",
                                     osm_way.id(), tagh.GetLoggingStr(osm_way));
    }
    return false;  // For instance when "private" or "no".
  }
  CarMaxspeed(tagh, way->id, ptags, &ra_forw, &ra_backw);

  if (rforw && IsDirForward(way->dir_dontuse) && ra_forw.maxspeed > 0) {
    way->ri[0] = ra_forw;
  } else {
    std::memset(&way->ri[0], 0, sizeof(way->ri[0]));
  }

  if (rbackw && IsDirBackward(way->dir_dontuse) && ra_backw.maxspeed > 0) {
    way->ri[1] = ra_backw;
  } else {
    std::memset(&way->ri[1], 0, sizeof(way->ri[1]));
  }

  // TODO way->surface = 0;
  return way->ri[0].access != ACC_NO || way->ri[1].access != ACC_NO;
}

void ConsumeWayWorker(const OSMTagHelper& tagh, const OSMPBF::Way& osm_way,
                      std::mutex& mut, MetaData* meta) {
  if (meta->hlp.way_tag_stats &&
      !tagh.GetValue(osm_way, tagh.highway_).empty()) {
    std::string statkey = CreateWayTagSignature(tagh, osm_way);
    if (!statkey.empty()) {
      std::unique_lock<std::mutex> l(mut);
      WayTagStat& stat = meta->hlp.way_tag_statmap[statkey];
      stat.count += 1;
      if ((absl::ToInt64Nanoseconds(absl::Now() - absl::Time()) % stat.count) ==
          0) {
        stat.example_way_id = osm_way.id();
      }
    }
  }

  const HIGHWAY_LABEL highway_label = ParseHighwayLabel(tagh, osm_way);
  if (highway_label < HW_MAX) {
    if (osm_way.refs().size() <= 1) {
      LOG_S(INFO) << absl::StrFormat("Ignore way %lld of length %lld",
                                     osm_way.id(), osm_way.refs().size());
      return;
    }

    GWay way;
    way.id = osm_way.id();
    way.highway_label = highway_label;
    CHECK_LT_S(way.highway_label, HW_MAX);

    const std::vector<NodeCountry> node_countries =
        ExtractNodeCountries(*meta, osm_way);
    if (node_countries.size() <= 1) {
      std::unique_lock<std::mutex> l(mut);
      meta->hlp.num_ways_deleted++;
      return;
    }
    ExtractWayCountryFromNodes(node_countries, &way);

    // Add routing data.
    if (!ComputeWayRoutingData(*meta, tagh, osm_way, &way)) {
      return;
    }

    // Get streetname tag.
    way.streetname = nullptr;
    std::string_view streetname_tag =
        tagh.GetValue(osm_way.keys(), osm_way.vals(), tagh.name_);

    // Encode nodes ids.
    std::vector<uint64_t> node_ids;
    for (const NodeCountry& nc : node_countries) {
      node_ids.push_back(nc.id);
    }
    WriteBuff node_ids_buff;
    EncodeUInt(node_ids.size(), &node_ids_buff);
    EncodeNodeIds(node_ids, &node_ids_buff);

    // Run modifications of global data behind a mutex.
    {
      std::unique_lock<std::mutex> l(mut);
      way.node_ids =
          meta->graph.unaligned_pool_.AllocBytes(node_ids_buff.used());
      memcpy(way.node_ids, node_ids_buff.base_ptr(), node_ids_buff.used());

      if (!streetname_tag.empty()) {
        // allocate a 0-terminated char*.
        way.streetname = (char*)meta->graph.unaligned_pool_.AllocBytes(
            streetname_tag.size() + 1);
        memcpy(way.streetname, streetname_tag.data(), streetname_tag.size());
        way.streetname[streetname_tag.size()] = '\0';
      }

      MarkSeenAndNeeded(meta, node_countries);
      meta->hlp.num_ways_selected++;
      meta->graph.ways.push_back(way);
    }
  }
}

}  // namespace build_graph
