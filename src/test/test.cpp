#include <osmpbf/osmpbf.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include <memory>

#include "base/argli.h"
#include "base/country_code.h"
#include "base/deduper_with_ids.h"
#include "base/huge_bitset.h"
#include "base/simple_mem_pool.h"
#include "base/thread_pool.h"
#include "base/util.h"
#include "base/varbyte.h"
#include "geometry/closest_node.h"
#include "geometry/distance.h"
#include "geometry/line_clipping.h"
#include "geometry/polygon.h"
#include "graph/build_graph.h"
#include "graph/data_block.h"
#include "graph/graph_def.h"
#include "graph/routing_config.h"
#include "osm/access.h"
#include "osm/admin_boundary.h"
#include "osm/key_bits.h"
#include "osm/maxspeed.h"
#include "osm/oneway.h"
#include "osm/osm_helpers.h"

struct OsmWayWrapper {
  std::unique_ptr<OSMPBF::StringTable> t;
  std::unique_ptr<OSMTagHelper> tagh;
  std::vector<ParsedTag> ptags;

  OSMPBF::Way osm_way;

  int FindStringPos(std::string_view str) const {
    for (int pos = 0; pos < t->s().size(); pos++) {
      if (t->s(pos) == str) {
        return pos;
      }
    }
    return -1;
  }

  // Find or add a string in string table 't' and return its position.
  int FindOrAddString(std::string_view str) {
    int pos = FindStringPos(str);
    if (pos >= 0) return pos;
    t->mutable_s()->Add(std::string(str));
    return t->s().size() - 1;
  }
};

// Create the osm data structures for a way with key value tags filled from
// 'tags. Tags are separated by " :: " and hanve the format
// <tagname>=<tagvalue>. Example: "highway=secondary :: oneway=yes ::
// maxspeed=80 :: maxspeed:hgv=60"
OsmWayWrapper FillWayData(std::string_view tags) {
  OsmWayWrapper w;
  w.t.reset(new OSMPBF::StringTable);
  if (!tags.empty()) {
    for (std::string_view tag : absl::StrSplit(tags, " :: ")) {
      std::vector<std::string_view> kv =
          absl::StrSplit(tag, absl::MaxSplits('=', 1));
      CHECK_EQ_S(kv.size(), 2)
          << "Can not split tag <" << tag << "> in " << tags;
      w.osm_way.mutable_keys()->Add(w.FindOrAddString(kv.at(0)));
      w.osm_way.mutable_vals()->Add(w.FindOrAddString(kv.at(1)));
    }
  }
  w.osm_way.set_id(1);
  w.tagh.reset(new OSMTagHelper(*w.t));
  w.ptags = ParseTags(*w.tagh, w.osm_way);
  return w;
}

void TestKeyPartBits() {
  FUNC_TIMER();

  CHECK_EQ_S(KEY_BIT_ACCESS, GetKeyPartBitFast("access"));
  CHECK_EQ_S(KEY_BIT_ADVISORY, GetKeyPartBitFast("advisory"));
  CHECK_EQ_S(KEY_BIT_BACKWARD, GetKeyPartBitFast("backward"));
  CHECK_EQ_S(KEY_BIT_BICYCLE, GetKeyPartBitFast("bicycle"));
  CHECK_EQ_S(KEY_BIT_BICYCLE_ROAD, GetKeyPartBitFast("bicycle_road"));
  CHECK_EQ_S(KEY_BIT_BOTH, GetKeyPartBitFast("both"));
  CHECK_EQ_S(KEY_BIT_BOTH_WAYS, GetKeyPartBitFast("both_ways"));
  CHECK_EQ_S(KEY_BIT_BRIDGE, GetKeyPartBitFast("bridge"));
  CHECK_EQ_S(KEY_BIT_BUS, GetKeyPartBitFast("bus"));
  CHECK_EQ_S(KEY_BIT_CHANGE, GetKeyPartBitFast("change"));
  CHECK_EQ_S(KEY_BIT_CONDITIONAL, GetKeyPartBitFast("conditional"));
  CHECK_EQ_S(KEY_BIT_CYCLEWAY, GetKeyPartBitFast("cycleway"));
  CHECK_EQ_S(KEY_BIT_FOOT, GetKeyPartBitFast("foot"));
  CHECK_EQ_S(KEY_BIT_FOOTWAY, GetKeyPartBitFast("footway"));
  CHECK_EQ_S(KEY_BIT_FORWARD, GetKeyPartBitFast("forward"));
  CHECK_EQ_S(KEY_BIT_HGV, GetKeyPartBitFast("hgv"));
  CHECK_EQ_S(KEY_BIT_HIGHWAY, GetKeyPartBitFast("highway"));
  CHECK_EQ_S(KEY_BIT_HORSE, GetKeyPartBitFast("horse"));
  CHECK_EQ_S(KEY_BIT_INCLINE, GetKeyPartBitFast("incline"));
  CHECK_EQ_S(KEY_BIT_JUNCTION, GetKeyPartBitFast("junction"));
  CHECK_EQ_S(KEY_BIT_LANE, GetKeyPartBitFast("lane"));
  CHECK_EQ_S(KEY_BIT_LANE_MARKINGS, GetKeyPartBitFast("lane_markings"));
  CHECK_EQ_S(KEY_BIT_LANES, GetKeyPartBitFast("lanes"));
  CHECK_EQ_S(KEY_BIT_LANES_INNER, GetKeyPartBitFast("lanes_inner"));
  CHECK_EQ_S(KEY_BIT_LAYER, GetKeyPartBitFast("layer"));
  CHECK_EQ_S(KEY_BIT_LEFT, GetKeyPartBitFast("left"));
  CHECK_EQ_S(KEY_BIT_LIT, GetKeyPartBitFast("lit"));
  CHECK_EQ_S(KEY_BIT_MAXSPEED, GetKeyPartBitFast("maxspeed"));
  CHECK_EQ_S(KEY_BIT_MOPED, GetKeyPartBitFast("moped"));
  CHECK_EQ_S(KEY_BIT_MOTOR_VEHICLE, GetKeyPartBitFast("motor_vehicle"));
  CHECK_EQ_S(KEY_BIT_MOTORCAR, GetKeyPartBitFast("motorcar"));
  CHECK_EQ_S(KEY_BIT_MOTORCYCLE, GetKeyPartBitFast("motorcycle"));
  CHECK_EQ_S(KEY_BIT_MOTORROAD, GetKeyPartBitFast("motorroad"));
  CHECK_EQ_S(KEY_BIT_NAME, GetKeyPartBitFast("name"));
  CHECK_EQ_S(KEY_BIT_ONEWAY, GetKeyPartBitFast("oneway"));
  CHECK_EQ_S(KEY_BIT_PRACTICAL, GetKeyPartBitFast("practical"));
  CHECK_EQ_S(KEY_BIT_PSV, GetKeyPartBitFast("psv"));
  CHECK_EQ_S(KEY_BIT_RIGHT, GetKeyPartBitFast("right"));
  CHECK_EQ_S(KEY_BIT_SERVICE, GetKeyPartBitFast("service"));
  CHECK_EQ_S(KEY_BIT_SIDEWALK, GetKeyPartBitFast("sidewalk"));
  CHECK_EQ_S(KEY_BIT_SMOOTHNESS, GetKeyPartBitFast("smoothness"));
  CHECK_EQ_S(KEY_BIT_SOURCE, GetKeyPartBitFast("source"));
  CHECK_EQ_S(KEY_BIT_SURFACE, GetKeyPartBitFast("surface"));
  CHECK_EQ_S(KEY_BIT_TOLL, GetKeyPartBitFast("toll"));
  CHECK_EQ_S(KEY_BIT_TRACKTYPE, GetKeyPartBitFast("tracktype"));
  CHECK_EQ_S(KEY_BIT_TRAFFIC, GetKeyPartBitFast("traffic"));
  CHECK_EQ_S(KEY_BIT_TUNNEL, GetKeyPartBitFast("tunnel"));
  CHECK_EQ_S(KEY_BIT_TURN, GetKeyPartBitFast("turn"));
  CHECK_EQ_S(KEY_BIT_TYPE, GetKeyPartBitFast("type"));
  CHECK_EQ_S(KEY_BIT_VARIABLE, GetKeyPartBitFast("variable"));
  CHECK_EQ_S(KEY_BIT_VEHICLE, GetKeyPartBitFast("vehicle"));
  CHECK_EQ_S(KEY_BIT_WIDTH, GetKeyPartBitFast("width"));
  CHECK_EQ_S(KEY_BIT_ZONE, GetKeyPartBitFast("zone"));

  for (auto part : kKeyParts) {
    CHECK_EQ_S(GetKeyPartBit(part), GetKeyPartBitFast(part));
  }
}

void TestParseTags() {
  FUNC_TIMER();
  OsmWayWrapper w;

  w = FillWayData("");
  CHECK_S(w.ptags.empty());

  w = FillWayData("blabla=1");
  CHECK_S(w.ptags.empty());

  w = FillWayData("maxspeed:blabla=10");
  CHECK_S(w.ptags.empty());

  {
    w = FillWayData("maxspeed:forward:lanes=50|60");
    CHECK_EQ_S(w.ptags.size(), 1u);
    CHECK_EQ_S(w.ptags.at(0).bits, GetBitMask(KEY_BIT_MAXSPEED, KEY_BIT_FORWARD,
                                              KEY_BIT_LANES_INNER));
    CHECK_EQ_S((int)w.ptags.at(0).val_st_idx, w.FindStringPos("50|60"));
  }

  {
    w = FillWayData("lanes:hgv=0");
    // Special case, first lanes is "normal", i.e. KEY_BIT_LANES
    CHECK_EQ_S(w.ptags.size(), 1u);
    CHECK_EQ_S(w.ptags.at(0).bits, GetBitMask(KEY_BIT_LANES, KEY_BIT_HGV));
  }

  {
    w = FillWayData("hgv:lanes=0");
    // Special case, non-first lanes is "inner", i.e. KEY_BIT_LANES_INNER
    CHECK_EQ_S(w.ptags.size(), 1u);
    CHECK_EQ_S(w.ptags.at(0).bits,
               GetBitMask(KEY_BIT_LANES_INNER, KEY_BIT_HGV));
  }

  {
    w = FillWayData("maxspeed:forward=20 :: maxspeed=10");
    CHECK_EQ_S(w.ptags.size(), 2u);
    // Should be sorted:
    CHECK_EQ_S(w.ptags.at(0).bits, GetBitMask(KEY_BIT_MAXSPEED));
    CHECK_EQ_S(w.ptags.at(1).bits,
               GetBitMask(KEY_BIT_MAXSPEED, KEY_BIT_FORWARD));
    CHECK_EQ_S((int)w.ptags.at(0).val_st_idx, w.FindStringPos("10"));
    CHECK_EQ_S((int)w.ptags.at(1).val_st_idx, w.FindStringPos("20"));
  }

  {
    w = FillWayData(
        "access:motorcar:lanes=1 :: "
        "access:forward=1 :: "
        "access:lanes=1 :: "
        "access:lanes:forward=1 :: "
        "access:motorcar=1 :: "
        "access:vehicle:lanes=1 :: "
        "maxspeed=1 :: "
        "access=1 :: "
        "access:motorcar:lanes:forward=1 :: "
        "access:vehicle=1 :: "
        "access:vehicle:forward=1");
    // We expect the following order, based on stable sort and priority sorting.
    //  0: maxspeed
    //  1: access
    //  2: access:forward
    //  3: access:lanes
    //  4: access:lanes:forward
    //  5: access:vehicle
    //  6: access:vehicle:forward
    //  7: access:vehicle:lanes
    //  8: access:motorcar
    //  9: access:motorcar:lanes
    // 10: access:motorcar:lanes:forward
    CHECK_EQ_S(w.ptags.size(), 11u);
    // Should be sorted:
    CHECK_EQ_S(w.ptags.at(0).bits, GetBitMask(KEY_BIT_MAXSPEED));
    CHECK_EQ_S(w.ptags.at(1).bits, GetBitMask(KEY_BIT_ACCESS));
    CHECK_EQ_S(w.ptags.at(2).bits, GetBitMask(KEY_BIT_ACCESS, KEY_BIT_FORWARD));
    CHECK_EQ_S(w.ptags.at(3).bits,
               GetBitMask(KEY_BIT_ACCESS, KEY_BIT_LANES_INNER));
    CHECK_EQ_S(
        w.ptags.at(4).bits,
        GetBitMask(KEY_BIT_ACCESS, KEY_BIT_LANES_INNER, KEY_BIT_FORWARD));
    CHECK_EQ_S(w.ptags.at(5).bits, GetBitMask(KEY_BIT_ACCESS, KEY_BIT_VEHICLE));
    CHECK_EQ_S(w.ptags.at(6).bits,
               GetBitMask(KEY_BIT_ACCESS, KEY_BIT_VEHICLE, KEY_BIT_FORWARD));
    CHECK_EQ_S(w.ptags.at(7).bits, GetBitMask(KEY_BIT_ACCESS, KEY_BIT_VEHICLE,
                                              KEY_BIT_LANES_INNER));
    CHECK_EQ_S(w.ptags.at(8).bits,
               GetBitMask(KEY_BIT_ACCESS, KEY_BIT_MOTORCAR));
    CHECK_EQ_S(w.ptags.at(9).bits, GetBitMask(KEY_BIT_ACCESS, KEY_BIT_MOTORCAR,
                                              KEY_BIT_LANES_INNER));
    CHECK_EQ_S(w.ptags.at(10).bits,
               GetBitMask(KEY_BIT_ACCESS, KEY_BIT_MOTORCAR, KEY_BIT_LANES_INNER,
                          KEY_BIT_FORWARD));
  }
}

void TestWayZones() {
  FUNC_TIMER();
  OsmWayWrapper w;
  build_graph::WayTaggedZones wr;

  w = FillWayData("");
  wr = build_graph::ExtractWayZones(*w.tagh, w.ptags);
  CHECK_EQ_S(wr.ncc, INVALID_NCC);
  CHECK_EQ_S(wr.et_forw, ET_ANY);
  CHECK_EQ_S(wr.et_backw, ET_ANY);
  CHECK_EQ_S(wr.im_forw, IM_NO);
  CHECK_EQ_S(wr.im_backw, IM_NO);

  w = FillWayData("maxspeed:source=FR:rural");
  wr = build_graph::ExtractWayZones(*w.tagh, w.ptags);
  CHECK_EQ_S(wr.ncc, NCC_FR);
  CHECK_EQ_S(wr.et_forw, ET_RURAL);
  CHECK_EQ_S(wr.et_backw, ET_RURAL);
  CHECK_EQ_S(wr.im_forw, IM_NO);
  CHECK_EQ_S(wr.im_backw, IM_NO);

  w = FillWayData("motorroad=yes");
  wr = build_graph::ExtractWayZones(*w.tagh, w.ptags);
  CHECK_EQ_S(wr.ncc, INVALID_NCC);
  CHECK_EQ_S(wr.et_forw, ET_ANY);
  CHECK_EQ_S(wr.et_backw, ET_ANY);
  CHECK_EQ_S(wr.im_forw, IM_YES);
  CHECK_EQ_S(wr.im_backw, IM_YES);

  w = FillWayData("motorroad:forward=yes");
  wr = build_graph::ExtractWayZones(*w.tagh, w.ptags);
  CHECK_EQ_S(wr.im_forw, IM_YES);
  CHECK_EQ_S(wr.im_backw, IM_NO);

  w = FillWayData("motorroad:backward=yes");
  wr = build_graph::ExtractWayZones(*w.tagh, w.ptags);
  CHECK_EQ_S(wr.im_forw, IM_NO);
  CHECK_EQ_S(wr.im_backw, IM_YES);

  w = FillWayData("maxspeed:source=??:urban");
  wr = build_graph::ExtractWayZones(*w.tagh, w.ptags);
  CHECK_EQ_S(wr.ncc, INVALID_NCC);

  for (std::string_view key :
       {"maxspeed", "maxspeed:source", "maxspeed:type", "source:maxspeed",
        "zone:maxspeed", "zone:traffic", "traffic:zone"}) {
    w = FillWayData(absl::StrCat(key, "=FR:urban"));
    wr = build_graph::ExtractWayZones(*w.tagh, w.ptags);
    CHECK_EQ_S(wr.ncc, NCC_FR);
    CHECK_EQ_S(wr.et_forw, ET_URBAN);
    CHECK_EQ_S(wr.et_backw, ET_URBAN);

    w = FillWayData(absl::StrCat(key, ":forward=FR:urban"));
    wr = build_graph::ExtractWayZones(*w.tagh, w.ptags);
    CHECK_EQ_S(wr.ncc, NCC_FR);
    CHECK_EQ_S(wr.et_forw, ET_URBAN);
    CHECK_EQ_S(wr.et_backw, ET_ANY);

    w = FillWayData(absl::StrCat(key, ":backward=FR:urban"));
    wr = build_graph::ExtractWayZones(*w.tagh, w.ptags);
    CHECK_EQ_S(wr.ncc, NCC_FR);
    CHECK_EQ_S(wr.et_forw, ET_ANY);
    CHECK_EQ_S(wr.et_backw, ET_URBAN);
  }
}

namespace {
void CheckCarMaxspeed(std::string_view tag_string, uint16_t exp_maxspeed_forw,
                      uint16_t exp_maxspeed_backw) {
  std::uint16_t speed_forw = 0;
  std::uint16_t speed_backw = 0;
  OsmWayWrapper w = FillWayData(tag_string);
  CarMaxspeedFromWay(*w.tagh, /*way_id=*/1, w.ptags, &speed_forw, &speed_backw);
  CHECK_EQ_S(speed_forw, exp_maxspeed_forw) << tag_string;
  CHECK_EQ_S(speed_backw, exp_maxspeed_backw) << tag_string;
}
}  // namespace

void TestCarMaxspeed() {
  FUNC_TIMER();

  CheckCarMaxspeed("", 0, 0);
  CheckCarMaxspeed("maxspeed:bicycle=20", 0, 0);
  CheckCarMaxspeed("maxspeed=50", 50, 50);
  CheckCarMaxspeed("maxspeed:both_ways=50", 50, 50);
  CheckCarMaxspeed("maxspeed:motorcar=50", 50, 50);
  CheckCarMaxspeed("maxspeed:motor_vehicle=50", 50, 50);
  CheckCarMaxspeed("maxspeed:vehicle=50", 50, 50);
  CheckCarMaxspeed("maxspeed:forward=50", 50, 0);
  CheckCarMaxspeed("maxspeed:forward:motorcar=50", 50, 0);
  CheckCarMaxspeed("maxspeed:backward=50", 0, 50);
  CheckCarMaxspeed("maxspeed:backward:motorcar=50", 0, 50);
  CheckCarMaxspeed("maxspeed:lanes=30|40", 40, 40);
  CheckCarMaxspeed("maxspeed:forward:lanes=30|40", 40, 0);
  CheckCarMaxspeed("maxspeed:backward:lanes=30|40", 0, 40);
}

namespace {
void CheckCarAccess(std::string_view tag_string, ACCESS exp_access_forw,
                    ACCESS exp_access_backw) {
  RoutingAttrs forw = {.access = ACC_NO}, backw = {.access = ACC_NO};
  OsmWayWrapper w = FillWayData(tag_string);
  CarAccess(*w.tagh, /*way_id=*/1, w.ptags, &forw, &backw);
  CHECK_EQ_S(forw.access, exp_access_forw) << tag_string;
  CHECK_EQ_S(backw.access, exp_access_backw) << tag_string;
}
}  // namespace

void TestCarAccess() {
  FUNC_TIMER();

  CheckCarAccess("", ACC_NO, ACC_NO);
  CheckCarAccess("access:bicycle=yes", ACC_NO, ACC_NO);
  CheckCarAccess("bicycle=yes", ACC_NO, ACC_NO);
  CheckCarAccess("access:motorcar=yes", ACC_YES, ACC_YES);
  CheckCarAccess("motorcar=yes", ACC_YES, ACC_YES);
  CheckCarAccess("motor_vehicle=yes", ACC_YES, ACC_YES);
  CheckCarAccess("vehicle=yes", ACC_YES, ACC_YES);
  CheckCarAccess("vehicle=yes :: motor_vehicle:forward=no", ACC_NO, ACC_YES);
  CheckCarAccess("vehicle:lanes:forward=no|yes :: access:lanes:backward=no",
                 ACC_YES, ACC_NO);
}

namespace {
void CheckCarRoadDirection(std::string_view tag_string, HIGHWAY_LABEL hw,
                           DIRECTION exp_direction) {
  OsmWayWrapper w = FillWayData(tag_string);
  DIRECTION dir = CarRoadDirection(*w.tagh, hw, /*way_id=*/1, w.ptags);
  CHECK_EQ_S(dir + 0, exp_direction + 0) << tag_string;
}
}  // namespace

void TestCarRoadDirection() {
  FUNC_TIMER();

  CheckCarRoadDirection("", HW_PRIMARY, DIR_BOTH);
  CheckCarRoadDirection("", HW_MOTORWAY, DIR_FORWARD);
  CheckCarRoadDirection("oneway=no", HW_MOTORWAY, DIR_BOTH);
  CheckCarRoadDirection("junction=roundabout", HW_PRIMARY, DIR_FORWARD);
  CheckCarRoadDirection("junction=roundabout :: oneway=no", HW_PRIMARY,
                        DIR_BOTH);
  CheckCarRoadDirection("oneway=yes", HW_PRIMARY, DIR_FORWARD);
  CheckCarRoadDirection("oneway=no", HW_PRIMARY, DIR_BOTH);
  CheckCarRoadDirection("oneway=-1", HW_PRIMARY, DIR_BACKWARD);
  // Unknown value is interpreted as "both ways blocked".
  CheckCarRoadDirection("oneway=blabla", HW_PRIMARY, DIR_MAX);
}

std::vector<HIGHWAY_LABEL> AllHws() {
  std::vector<HIGHWAY_LABEL> hws;
  for (HIGHWAY_LABEL hw = static_cast<HIGHWAY_LABEL>(0); hw < HW_MAX;
       hw = (HIGHWAY_LABEL)(hw + 1)) {
    hws.push_back(hw);
  }
  return hws;
}

std::vector<VEHICLE> AllVhs() {
  std::vector<VEHICLE> vhs;
  for (VEHICLE vh = static_cast<VEHICLE>(0); vh < VH_MAX;
       vh = (VEHICLE)(vh + 1)) {
    vhs.push_back(vh);
  }
  return vhs;
}

void LogRoutingAttrs(uint16_t cc_num, HIGHWAY_LABEL hw, VEHICLE vh,
                     ENVIRONMENT_TYPE et, IS_MOTORROAD im,
                     const PerCountryConfig& config) {
  auto val = config.GetDefault(cc_num, hw, vh, et, im);
  LOG_S(INFO) << absl::StrFormat(
      "%s hw=%-10s vh=%-7s et=%s im=%u %s", CountryNumToString(cc_num),
      HighwayLabelToString(hw).substr(0, 10), VehicleToString(vh).substr(0, 7),
      EnvironmentTypeToString(et), im, config.ConfigValueDebugString(val));
}

void TestRoutingConfig() {
  LOG_S(INFO) << "TestRoutingConfig() started";
  {
    PerCountryConfig config;
    CHECK_S(config.ApplyConfigLine("ALL:vh_motorized speed_max=10"));
    CHECK_S(config.ApplyConfigLine("CH:vh_motorized speed_max=50"));

    for (HIGHWAY_LABEL hw : AllHws()) {
      PerCountryConfig::ConfigValue cv;

      cv = config.GetDefault(NCC_CH, hw, VH_MOTOR_VEHICLE, ET_ANY, IM_NO);
      LOG_S(INFO) << absl::StrFormat("hw_%s:\n%s", HighwayLabelToString(hw),
                                     config.ConfigValueDebugString(cv));
      CHECK_EQ_S(cv.dflt.maxspeed, 50) << HighwayLabelToString(hw);

      cv = config.GetDefault(NCC_DE, hw, VH_MOTOR_VEHICLE, ET_ANY, IM_YES);
      LOG_S(INFO) << absl::StrFormat("hw_%s:\n%s", HighwayLabelToString(hw),
                                     config.ConfigValueDebugString(cv));
      CHECK_EQ_S(cv.dflt.maxspeed, 10) << HighwayLabelToString(hw);
    }
  }
  {
    PerCountryConfig config;
    config.ReadConfig("../config/routing.cfg");
    LOG_S(INFO) << "*** " << VehicleToString(VH_MOTOR_VEHICLE);
    for (HIGHWAY_LABEL hw : AllHws()) {
      LogRoutingAttrs(NCC_CH, hw, VH_MOTOR_VEHICLE, ET_URBAN, IM_NO, config);
      LogRoutingAttrs(NCC_CH, hw, VH_MOTOR_VEHICLE, ET_RURAL, IM_NO, config);
    }
  }
  LOG_S(INFO) << "TestRoutingConfig() finished";
}

void TestSimpleMemPool() {
  LOG_S(INFO) << "TestSimpleMemPool() started";
  SimpleMemPool p(10);
  std::uint8_t* ptr1 = p.AllocBytes(3);
  std::uint8_t* ptr2 = p.AllocBytes(7);
  std::uint8_t* ptr3 = p.AllocBytes(5);
  std::uint8_t* ptr4 = p.AllocBytes(6);
  CHECK_EQ_S(ptr2 - ptr1, 3);
  CHECK_NE_S(ptr3 - ptr2, 7);
  CHECK_NE_S(ptr4 - ptr3, 5);
  CHECK_EQ_S(p.MemConsumed(), 3 * (10 + 24));
  CHECK_EQ_S(p.MemAllocated(), 3 + 7 + 5 + 6);
  LOG_S(INFO) << "TestSimpleMemPool() finished";
}

void TestSimpleMemPoolAligned() {
  LOG_S(INFO) << "TestSimpleMemPoolAligned() started";
  SimpleMemPool p(10);
  std::uint8_t* ptr1 = p.AllocBytes(1);
  CHECK_EQ_S(((uint64_t)p.AllocBytes(1) & 7llu), 1);
  std::uint8_t* ptr2 = (std::uint8_t*)p.Alloc64BitAligned(1);
  CHECK_EQ_S(((uint64_t)ptr2 & 7), 0);
  CHECK_EQ_S((ptr2 - ptr1), 8);
  LOG_S(INFO) << "TestSimpleMemPoolAligned() finished";
}

void FindNode(const DataBlockTable& t, std::uint64_t id, std::int64_t lat,
              std::int64_t lon) {
  LOG_S(INFO) << absl::StrFormat("FindNode(%llu)", id);
  NodeBuilder::VNode node;
  CHECK_S(NodeBuilder::FindNode(t, id, &node));
  CHECK_EQ_S(node.id, id);
  CHECK_EQ_S(node.lat, lat);
  CHECK_EQ_S(node.lon, lon);
}

void TestNodeDataBlock() {
  LOG_S(INFO) << "TestNodeDataBlock() started";
  DataBlockTable t(10);
  NodeBuilder builder;
  builder.AddNode({5, 1, 10});
  builder.AddNode({300, 2, 100});
  builder.AddBlockToTable(&t);
  builder.AddNode({500, 3, 1000});
  builder.AddBlockToTable(&t);

  t.Sort();
  NodeBuilder::VNode node;
  CHECK_S(builder.FindNode(t, 5, &node));
  CHECK_S(builder.FindNode(t, 300, &node));
  CHECK_S(builder.FindNode(t, 500, &node));

  CHECK_S(!builder.FindNode(t, 4, &node));
  CHECK_S(!builder.FindNode(t, 6, &node));
  CHECK_S(!builder.FindNode(t, 299, &node));
  CHECK_S(!builder.FindNode(t, 301, &node));
  CHECK_S(!builder.FindNode(t, 499, &node));
  CHECK_S(!builder.FindNode(t, 501, &node));

  FindNode(t, 5, 1, 10);
  FindNode(t, 300, 2, 100);
  FindNode(t, 500, 3, 1000);

  LOG_S(INFO) << "TestNodeDataBlock() finished";
}

void TestNodeDataBlockReEncode() {
  LOG_S(INFO) << "TestNodeDataBlockReEncode() started";
  DataBlockTable from(100);
  constexpr unsigned int num_elements = 10;
  {
    NodeBuilder builder;
    for (unsigned int i = 1; i <= num_elements; ++i) {
      builder.AddNode({i, 0, 0});
      if (builder.pending_nodes(), 3) {
        builder.AddBlockToTable(&from);
      }
    }
    builder.AddBlockToTable(&from);
  }

  DataBlockTable to(100);
  {
    NodeBuilder builder;
    NodeBuilder::GlobalNodeIter iter(from);
    const NodeBuilder::VNode* node;
    while ((node = iter.Next()) != nullptr) {
      builder.AddNode(*node);
      if (builder.pending_nodes() == 2) {
        builder.AddBlockToTable(&to);
      }
    }
    builder.AddBlockToTable(&to);
  }
  LOG_S(INFO) << absl::StrFormat("%u %u", from.GetBlocks().size(),
                                 to.GetBlocks().size());
  CHECK_EQ_S(to.GetBlocks().size(), num_elements / 2);
  for (unsigned int i = 1; i <= num_elements; ++i) {
    NodeBuilder::VNode node;
    CHECK_S(NodeBuilder::FindNode(to, i, &node));
    CHECK_S(node.lat == 0 && node.lon == 0);
  }
  LOG_S(INFO) << "TestNodeDataBlockReEncode() finished";
}

void TestHugeBitset() {
  {
    HugeBitset bits;
    for (std::int64_t i = 0ull; i < 1000000; i = i + 30000) {
      CHECK_S(!bits.SetBit(i + 1013ull, false));
      CHECK_S(!bits.GetBit(i + 1013ull));
      CHECK_S(!bits.SetBit(i + 1013ull, true));
      CHECK_S(bits.SetBit(i + 1013ull, false));
      CHECK_S(!bits.GetBit(i + 1013ull));
      CHECK_S(!bits.SetBit(i + 1013ull, true));
      CHECK_S(bits.GetBit(i + 1013ull));
    }
  }
  {
    HugeBitset bits;
    CHECK_S(!bits.SetBit(3, true));
    CHECK_S(!bits.SetBit(33, true));
    CHECK_S(!bits.SetBit(333, true));
    CHECK_S(!bits.SetBit(3333, true));
    CHECK_S(!bits.SetBit(33333, true));
    CHECK_S(!bits.SetBit(333333, true));
    CHECK_S(!bits.SetBit(3333333, true));
    CHECK_S(!bits.SetBit(33333333, true));
    for (std::uint64_t i = bits.NextBit(0); i < bits.NumAllocatedBits();
         i = bits.NextBit(i + 1ull)) {
      LOG_S(INFO) << absl::StrFormat("Bit set: %llu", i);
    }
    CHECK_EQ_S(bits.NextBit(0), 3);
    CHECK_EQ_S(bits.NextBit(4), 33);
    CHECK_EQ_S(bits.NextBit(34), 333);
    CHECK_EQ_S(bits.NextBit(334), 3333);
    CHECK_EQ_S(bits.NextBit(3334), 33333);
    CHECK_EQ_S(bits.NextBit(33334), 333333);
    CHECK_EQ_S(bits.NextBit(333334), 3333333);
    CHECK_EQ_S(bits.NextBit(3333334), 33333333);
    CHECK_EQ_S(bits.NextBit(33333334), bits.NumAllocatedBits());
  }
}

void TestUInt(std::uint64_t val) {
  WriteBuff buff;
  std::uint64_t v;
  LOG_S(INFO) << absl::StrFormat("Test Varint %llu", val);
  std::uint32_t bytes = EncodeUInt(val, &buff);
  LOG_S(INFO) << absl::StrFormat("bytes for encoding: %d", bytes);
  CHECK_EQ_S(buff.used(), bytes);
  CHECK_S(bytes > 0 && bytes <= max_varint_bytes);
  CHECK_EQ_S(DecodeUInt(buff.base_ptr(), &v), bytes);
  CHECK_EQ_S(v, val);
}

void TestInt(std::int64_t val) {
  WriteBuff buff;
  std::int64_t v;
  LOG_S(INFO) << absl::StrFormat("Test Varint %lld", val);
  std::uint32_t bytes = EncodeInt(val, &buff);
  LOG_S(INFO) << absl::StrFormat("bytes for encoding: %d", bytes);
  CHECK_EQ_S(buff.used(), bytes);
  CHECK_S(bytes > 0 && bytes <= max_varint_bytes);
  CHECK_EQ_S(DecodeInt(buff.base_ptr(), &v), bytes);
  CHECK_EQ_S(v, val);
}

void TestInts() {
  LOG_S(INFO) << "*** Test UInts";
  TestUInt(0);
  TestUInt(1);
  TestUInt(127);
  TestUInt(128);
  TestUInt(129);
  TestUInt(65535);
  TestUInt(65536);
  TestUInt(18446744073709551615ull);

  LOG_S(INFO) << "*** Test Ints";
  TestInt(0);
  TestInt(1);
  TestInt(-1);
  TestInt(127);
  TestInt(128);
  TestInt(-127);
  TestInt(-128);
  TestInt(65535);
  TestInt(65536);
  TestInt(-65535);
  TestInt(-65536);
  TestInt(12345678901234);
  TestInt(9223372036854775807ll);
  TestInt(-9223372036854775807ll);
  TestInt(-9223372036854775807ll - 1);
}

void TestString(const std::string& val) {
  WriteBuff buff;
  LOG_S(INFO) << absl::StrFormat("Encode String <%s> length:%llu", val.c_str(),
                                 val.size());
  std::size_t bytes = EncodeString(val, &buff);
  LOG_S(INFO) << absl::StrFormat("  bytes from encoding: %d", bytes);
  CHECK_GT_S(bytes, 0);
  std::string_view decoded_str;
  LOG_S(INFO) << absl::StrFormat("  bytes from decoding: %d",
                                 DecodeString(buff.base_ptr(), &decoded_str));
  CHECK_EQ_S(DecodeString(buff.base_ptr(), &decoded_str), bytes);
  CHECK_EQ_S(decoded_str, val);
}

void TestStrings() {
  LOG_S(INFO) << "*** Test Strings";
  TestString("");
  TestString(std::string(1, '\0'));  // string of length 1 containing '\0'.
  TestString("a");
  TestString("1234567890");
}

void TestNodeList(const std::vector<std::uint64_t> ids1) {
  WriteBuff buff;

  LOG_S(INFO) << "****************** Encode nodes";
  for (std::size_t i = 0; i < ids1.size(); ++i) {
    LOG_S(INFO) << absl::StrFormat("  Id %d: %5llu", i, ids1.at(i));
  }

  int cnt1 = EncodeNodeIds(ids1, &buff);
  LOG_S(INFO) << "Binary Output:";
  for (int i = 0; i < cnt1 + 1; ++i) {
    LOG_S(INFO) << absl::StrFormat("  %03d:   %2x", i, buff.base_ptr()[i]);
  }

  std::vector<std::uint64_t> ids2;
  int cnt2 = DecodeNodeIds(buff.base_ptr(), ids1.size(), &ids2);
  LOG_S(INFO) << absl::StrFormat("Ids Output (cnt1:%d cnt2:%d)", cnt1, cnt2);
  CHECK_EQ_S(ids1.size(), ids2.size());
  for (std::size_t i = 0; i < ids2.size(); ++i) {
    LOG_S(INFO) << absl::StrFormat("  Id %d: %5llu", i, ids2.at(i));
  }

  CHECK_EQ_S(cnt1, cnt2);
  for (std::size_t i = 0; i < ids1.size(); ++i) {
    CHECK_EQ_S(ids1.at(i), ids2.at(i));
  }
}

void TestNodeLists() {
  LOG_S(INFO) << "TestNodeLists() started";
  TestNodeList({0});
  TestNodeList({1});
  TestNodeList({10000});
  TestNodeList({18446744073709551615ull});
  TestNodeList({0, 18446744073709551615ull});
  TestNodeList({18446744073709551615ull, 18446744073709551615ull});

  TestNodeList({200000000, 2});
  TestNodeList({1, 200000000});
  TestNodeList({1, 200000000, 400000000, 200000003, 199999999});
  LOG_S(INFO) << "TestNodeLists() finished";
}

void TestOneNodeIds(const std::vector<std::uint64_t>& ids) {
  WriteBuff buff;

  int cnt = EncodeNodeIds(ids, &buff);
  std::vector<std::uint64_t> ids2;
  int cnt2 = DecodeNodeIds(buff.base_ptr(), ids.size(), &ids2);
  CHECK_EQ_S(cnt, cnt2);
  CHECK_EQ_S(ids.size(), ids2.size());
  for (std::size_t i = 0; i < ids.size(); ++i) {
    CHECK_EQ_S(ids.at(i), ids2.at(i));
  }
}

void TestNodeids() {
  LOG_S(INFO) << "TestNodeids() started";
  TestOneNodeIds({0});
  TestOneNodeIds({1});
  TestOneNodeIds({10000});
  TestOneNodeIds({18446744073709551615ull});
  TestOneNodeIds({0, 18446744073709551615ull});
  TestOneNodeIds({18446744073709551615ull, 18446744073709551615ull});

  TestOneNodeIds({200000000, 2});
  TestOneNodeIds({1, 200000000});
  TestOneNodeIds({1, 200000000, 400000000, 200000003, 199999999});
  LOG_S(INFO) << "TestNodeids() finished";
}

void TestGraph() {
  LOG_S(INFO) << "TestGraph() started";
  Graph g;
  for (unsigned int i = 0; i < 5; ++i) {
    GWay way = {.id = i};
    g.ways.push_back(way);
    GNode node = {.node_id = i + 1};
    g.nodes.push_back(node);
  }
  for (unsigned int i = 0; i < 5; ++i) {
    CHECK_EQ_S(g.FindWayIndex(i), i);
  }
  CHECK_EQ_S(g.FindWayIndex(10), 5);
  for (unsigned int i = 0; i < 5; ++i) {
    CHECK_EQ_S(g.FindNodeIndex(i + 1), i);
  }
  CHECK_EQ_S(g.FindNodeIndex(10), 5);
  LOG_S(INFO) << "TestGraph() finished";
}

void TestLineClipping() {
  LOG_S(INFO) << "TestLineClipping() started";
  TwoPoint r = {10.0, 10.0, 20.0, 20.0};

  {
    TwoPoint line = {5.0, 5.0, 20.0, 15.0};
    CHECK_S(ClipLineCohenSutherland(r, &line));
    LOG_S(INFO) << absl::StrFormat("line: %f %f %f %f", line.x0, line.y0,
                                   line.x1, line.y1);
  }

  LOG_S(INFO) << "TestLineClipping() finished";
}

void TestFastPolygonContains() {
  LOG_S(INFO) << "TestFastPolygonContains() started";
  const int16_t country = 123;

  {
    //   *
    //  *
    // *
    FastCountryPolygons p;
    p.AddLine(0, 0, 100, 100, country);
    CHECK_EQ_S(p.CountIntersections(0, 50), 1);
    CHECK_EQ_S(p.CountIntersections(60, 50), 0);
    CHECK_EQ_S(p.CountIntersections(100, 50), 0);
    CHECK_EQ_S(p.CountIntersections(200, 50), 0);
    CHECK_EQ_S(p.CountIntersections(51, 50), 0);
    CHECK_EQ_S(p.CountIntersections(49, 50), 1);
    CHECK_EQ_S(p.CountIntersections(00, 0), 0);
    CHECK_EQ_S(p.CountIntersections(00, 100), 1);
  }

  {
    // *
    //  *
    //   *
    FastCountryPolygons p;
    p.AddLine(0, 100, 100, 0, country);
    CHECK_EQ_S(p.CountIntersections(0, 50), 1);
    CHECK_EQ_S(p.CountIntersections(60, 50), 0);
    CHECK_EQ_S(p.CountIntersections(100, 50), 0);
    CHECK_EQ_S(p.CountIntersections(200, 50), 0);
    CHECK_EQ_S(p.CountIntersections(51, 50), 0);
    CHECK_EQ_S(p.CountIntersections(49, 50), 1);
    CHECK_EQ_S(p.CountIntersections(0, 0), 0);
    CHECK_EQ_S(p.CountIntersections(0, 100), 1);
  }

  {
    //   *
    //  * *
    // *   *
    FastCountryPolygons p;
    p.AddLine(0, 0, 100, 100, country);
    p.AddLine(100, 100, 200, 0, country);
    CHECK_EQ_S(p.CountIntersections(200, 0), 0);
    CHECK_EQ_S(p.CountIntersections(0, 100), 2);
    CHECK_EQ_S(p.CountIntersections(0, 0), 0);
    CHECK_EQ_S(p.CountIntersections(100, 50), 1);
  }

  {
    // ***
    FastCountryPolygons p;
    p.AddLine(0, 0, 100, 0, country);
    CHECK_EQ_S(p.CountIntersections(-100, 0), 0);
    CHECK_EQ_S(p.CountIntersections(0, 0), 0);
    CHECK_EQ_S(p.CountIntersections(100, 0), 0);
    CHECK_EQ_S(p.CountIntersections(200, 0), 0);
  }

  {
    //  *
    //  *
    //  *
    FastCountryPolygons p;
    p.AddLine(100, 0, 100, 100, country);
    CHECK_EQ_S(p.CountIntersections(0, 0), 0);
    CHECK_EQ_S(p.CountIntersections(0, 100), 1);
  }

  {
    // Check that binary search works on the lines sorted by y-axis.
    // For this, insert lines in random order and expect 'p.PrepareData()' to
    // sort them properly.
    FastCountryPolygons p;
    const int y_ray = 5100;
    const int height = 100;
    int expected_intersections = 0;
    std::srand(1);  // Get always the same pseudo-random numbers.
    for (int i = 0; i < 10000; ++i) {
      const int y_val = rand() % 10000;
      if (y_val >= y_ray - height && y_val < y_ray) {
        expected_intersections++;
      }
      p.AddLine(100, y_val, 100, y_val + 100, country + i);
    }
    CHECK_GT_S(expected_intersections, 0);
    p.PrepareData();
    CHECK_EQ_S(p.CountIntersections(0, y_ray), expected_intersections);
  }

  {
    std::vector<FastCountryPolygons::Line> lines;
    lines.push_back({.x0 = 0,
                     .y0 = 0,
                     .x1 = 0,
                     .y1 = 0,
                     .country_num_1 = 1,
                     .country_num_2 = 0});
    lines.push_back({.x0 = 0,
                     .y0 = 0,
                     .x1 = 0,
                     .y1 = 0,
                     .country_num_1 = 2,
                     .country_num_2 = 0});
    FastCountryPolygons::MergeDupLines(&lines);
    CHECK_EQ_S(lines.size(), 1u);
    CHECK_EQ_S(lines.at(0).country_num_1, 1);
    CHECK_EQ_S(lines.at(0).country_num_2, 2);
  }

  LOG_S(INFO) << "TestFastPolygonContains() finished";
}

void TestTiledCountryLookup() {
  LOG_S(INFO) << "TestTiledCountryLookup() started";
  const int16_t country = 123;
  const int32_t kDegreeUnits = TiledCountryLookup::kDegreeUnits;
  // Tiling has step size 10 degrees.
  const int32_t tile_size = kDegreeUnits * 10;

  {
    FastCountryPolygons p;
    // Vertical line fully crossing tiles with x,y: (1,0) and (1,1).
    // tile (1,2) is partially crossed (it contains the end of the line).
    // This causes tiles (0,0) and (0,1) to be assigned to 'country'. Tile (0,2)
    // is partially assigned to 'country, and tile 0,3) has no country.
    p.AddLine(tile_size + 1, -1, tile_size + 1, 2 * tile_size + 1000, country);
    TiledCountryLookup tiler(p, tile_size);
    CHECK_EQ_S(tiler.GetCountryNum(1, 1), country);

    // CHECK_EQ_S(p.CountIntersections(0, 50), 1);
  }
  LOG_S(INFO) << "TestTiledCountryLookup() finished";
}

void TestUtil() {
  LOG_S(INFO) << "TestUtil() started";
  CHECK_EQ_S(WildCardToRegex("/aha/*.c?v"), R"(/aha/.*\.c.v)");
  CHECK_EQ_S(WildCardToRegex("/tmp/admin/??_*.csv"),
             R"(/tmp/admin/.._.*\.csv)");

  char two_letter_code[3];
  two_letter_code[2] = 0;
  for (char c0 = 'A'; c0 <= 'Z'; ++c0) {
    two_letter_code[0] = c0;
    for (char c1 = 'A'; c1 <= 'Z'; ++c1) {
      two_letter_code[1] = c1;
      auto num = TwoLetterCountryCodeToNum(two_letter_code);
      char converted[3];
      CountryNumToTwoLetter(num, converted);
      CHECK_EQ_S(std::string_view(two_letter_code),
                 std::string_view(converted));
    }
  }

  LOG_S(INFO) << "TestUtil() finished";
}

class TestArgli {
 public:
  static void Usage(const Argli& argli,
                    const std::vector<const char*> args = {}) {
    std::cerr << "Called with:" << std::endl;
    for (const char* arg : args) {
      std::cerr << " " << arg;
    }
    std::cerr << argli.Usage();
  }

  static Argli Test(const std::vector<Argli::ArgDef>& arg_defs,
                    std::vector<const char*> args,
                    std::string_view expected_error) {
    constexpr size_t kMaxArgc = 20;
    char* argv[kMaxArgc];
    const int argc = args.size() + 1;
    CHECK_LT_S(args.size(), kMaxArgc);
    char str[] = "TestOneArgli";
    argv[0] = str;  //"TestOneArgli";
    for (size_t i = 0; i < args.size(); ++i) {
      argv[i + 1] = (char*)args.at(i);
    }

    Argli argli;
    std::string errmsg = argli.InternalParse(argc, argv, arg_defs);
    CHECK_EQ_S(errmsg.empty(), expected_error.empty());
    if (!expected_error.empty()) {
      CHECK_S(absl::StrContains(errmsg, expected_error))
          << "\n<" << expected_error << "> not contained in <" << errmsg << ">";
    }
    return argli;
  }

  static void TestArgIsSet(Argli& argli, std::string_view name, bool expected) {
    if (argli.ArgIsSet(name) != expected) {
      Usage(argli);
    }
    CHECK_EQ_S(argli.ArgIsSet(name), expected);
  }

  static void TestGetString(Argli& argli, std::string_view name,
                            std::string_view expected) {
    if (argli.GetString(name) != expected) {
      Usage(argli);
    }
    CHECK_EQ_S(argli.GetString(name), expected);
  }

  static void TestGetInt(Argli& argli, std::string_view name,
                         int64_t expected) {
    if (argli.GetInt(name) != expected) {
      Usage(argli);
    }
    CHECK_EQ_S(argli.GetInt(name), expected);
  }

  static void TestGetBool(Argli& argli, std::string_view name, bool expected) {
    if (argli.GetBool(name) != expected) {
      Usage(argli);
    }
    CHECK_EQ_S(argli.GetBool(name), expected);
  }

  static void TestGetDouble(Argli& argli, std::string_view name,
                            double expected) {
    if (argli.GetDouble(name) != expected) {
      Usage(argli);
    }
    CHECK_EQ_S(argli.GetDouble(name), expected);
  }
};

void TestArglis() {
  LOG_S(INFO) << "TestArglis() started";

  {
    // =================== Basic test required attribute
    Argli a;
    std::vector<Argli::ArgDef> arg_defs = {
        {.name = "root", .type = "string", .required = true}};
    // Good:
    a = TestArgli::Test(arg_defs, {"--root="}, "root");
    TestArgli::TestGetString(a, "root", "");

    a = TestArgli::Test(arg_defs, {"-root="}, "root");
    TestArgli::TestGetString(a, "root", "");

    a = TestArgli::Test(arg_defs, {"--root=123"}, "");
    TestArgli::TestGetString(a, "root", "123");

    a = TestArgli::Test(arg_defs, {"-root=123"}, "");
    TestArgli::TestGetString(a, "root", "123");

    a = TestArgli::Test(arg_defs, {"-root", "123"}, "");
    TestArgli::TestGetString(a, "root", "123");

    // Bad:
    TestArgli::Test(arg_defs, {}, "root");
    TestArgli::Test(arg_defs, {"--root"}, "root");
    TestArgli::Test(arg_defs, {"-root"}, "root");
    TestArgli::Test(arg_defs, {"123", "--root"}, "123");
    TestArgli::Test(arg_defs, {"--other"}, "other");
    TestArgli::Test(arg_defs, {"-other"}, "other");
  }

  {
    // =================== Basic test bool attribute
    Argli a;
    std::vector<Argli::ArgDef> arg_defs = {
        {.name = "myflag", .type = "bool"},
        {.name = "pos", .type = "string", .positional = true}};

    // Good:
    a = TestArgli::Test(arg_defs, {}, "");
    TestArgli::TestArgIsSet(a, "myflag", false);
    TestArgli::TestArgIsSet(a, "pos", false);
    TestArgli::TestGetString(a, "myflag", "false");
    TestArgli::TestGetBool(a, "myflag", false);

    a = TestArgli::Test(arg_defs, {"--myflag"}, "");
    TestArgli::TestGetBool(a, "myflag", true);

    a = TestArgli::Test(arg_defs, {"--myflag", "bla"}, "");
    TestArgli::TestArgIsSet(a, "myflag", true);
    TestArgli::TestArgIsSet(a, "pos", true);
    TestArgli::TestGetBool(a, "myflag", true);
    TestArgli::TestGetString(a, "pos", "bla");

    a = TestArgli::Test(arg_defs, {"--myflag=true"}, "");
    TestArgli::TestGetBool(a, "myflag", true);
  }

  {
    // =================== Test different data types
    Argli a;
    std::vector<Argli::ArgDef> arg_defs = {
        {.name = "argstring", .type = "string", .dflt = "1"},
        {.name = "argint", .type = "int", .dflt = "2"},
        {.name = "argbool", .type = "bool", .dflt = "true"},
        {.name = "argdouble", .type = "double", .dflt = "4.0"}};

    // Good:
    a = TestArgli::Test(arg_defs, {}, "");
    TestArgli::TestArgIsSet(a, "argstring", false);
    TestArgli::TestArgIsSet(a, "argint", false);
    TestArgli::TestArgIsSet(a, "argbool", false);
    TestArgli::TestArgIsSet(a, "argdouble", false);
    TestArgli::TestGetString(a, "argstring", "1");
    TestArgli::TestGetString(a, "argint", "2");
    TestArgli::TestGetString(a, "argbool", "true");
    TestArgli::TestGetString(a, "argdouble", "4.0");
    TestArgli::TestGetInt(a, "argint", 2);
    TestArgli::TestGetBool(a, "argbool", true);
    TestArgli::TestGetDouble(a, "argdouble", 4.0);

    a = TestArgli::Test(
        arg_defs,
        {"--argstring=a", "--argint=10", "--argbool=false", "--argdouble=99.0"},
        "");
    TestArgli::TestArgIsSet(a, "argstring", true);
    TestArgli::TestArgIsSet(a, "argint", true);
    TestArgli::TestArgIsSet(a, "argbool", true);
    TestArgli::TestArgIsSet(a, "argdouble", true);
    TestArgli::TestGetString(a, "argstring", "a");
    TestArgli::TestGetString(a, "argint", "10");
    TestArgli::TestGetString(a, "argbool", "false");
    TestArgli::TestGetString(a, "argdouble", "99.0");
    TestArgli::TestGetInt(a, "argint", 10);
    TestArgli::TestGetBool(a, "argbool", false);
    TestArgli::TestGetDouble(a, "argdouble", 99.0);

    // Bad:
    a = TestArgli::Test(arg_defs, {"--other"}, "other");
    a = TestArgli::Test(arg_defs, {"--argint=noint"}, "convert");
    a = TestArgli::Test(arg_defs, {"--argbool=nobool"}, "convert");
    a = TestArgli::Test(arg_defs, {"--argdouble=nodouble"}, "convert");
  }

  {
    // =================== Test positional args
    Argli a;
    std::vector<Argli::ArgDef> arg_defs = {
        {.name = "pos1", .type = "string", .positional = true, .dflt = "a"},
        {.name = "normal", .type = "string"},
        {.name = "pos2", .type = "bool", .positional = true}};

    // Good:
    a = TestArgli::Test(arg_defs, {}, "");
    TestArgli::TestArgIsSet(a, "pos1", false);
    TestArgli::TestArgIsSet(a, "normal", false);
    TestArgli::TestArgIsSet(a, "pos2", false);
    TestArgli::TestGetString(a, "pos1", "a");
    TestArgli::TestGetString(a, "normal", "");
    TestArgli::TestGetBool(a, "pos2", false);
    a = TestArgli::Test(arg_defs, {"p1", "true", "-normal", "n"}, "");
    TestArgli::TestArgIsSet(a, "pos1", true);
    TestArgli::TestArgIsSet(a, "normal", true);
    TestArgli::TestArgIsSet(a, "pos2", true);
    TestArgli::TestGetString(a, "pos1", "p1");
    TestArgli::TestGetString(a, "normal", "n");
    TestArgli::TestGetBool(a, "pos2", true);
  }

  LOG_S(INFO) << "TestArglis() finished";
}

void TestCalculateDistance() {
  constexpr int32_t unit = 10000000;
  LOG_S(INFO) << "TestCalculateDistance() started";
  LOG_S(INFO) << "New:"
              << calculate_distance(-10 * unit, -20 * unit, 30 * unit,
                                    40 * unit);

  LOG_S(INFO) << "New:"
              << calculate_distance(-10 * unit, -20 * unit, 130 * unit,
                                    140 * unit);

  LOG_S(INFO) << "TestCalculateDistance() finished";
}

void TestDeDuperWithIdsInt() {
  DeDuperWithIds<int> dd;

  CHECK_EQ_S(dd.Add(0), 0);
  CHECK_EQ_S(dd.Add(1), 1);
  for (int i = 0; i < 5; ++i) {
    CHECK_EQ_S(dd.Add(2), 2);
  }

  CHECK_EQ_S(dd.GetObj(0), 0);
  CHECK_EQ_S(dd.GetEntryAt(0).id, 0);
  CHECK_EQ_S(dd.GetEntryAt(0).ref_count, 1);

  CHECK_EQ_S(dd.GetObj(1), 1);
  CHECK_EQ_S(dd.GetEntryAt(1).id, 1);
  CHECK_EQ_S(dd.GetEntryAt(1).ref_count, 1);

  CHECK_EQ_S(dd.GetObj(2), 2);
  CHECK_EQ_S(dd.GetEntryAt(2).id, 2);
  CHECK_EQ_S(dd.GetEntryAt(2).ref_count, 5);

  dd.SortByPopularity();
  std::vector<int> els = dd.GetObjVector();
  CHECK_EQ_S(els.size(), 3);
  CHECK_EQ_S(els.at(0), 2);
  CHECK_EQ_S(els.at(1), 0);
  CHECK_EQ_S(els.at(2), 1);

  std::vector<uint32_t> redir = dd.GetSortMapping();
  CHECK_EQ_S(redir.size(), 3);
  CHECK_EQ_S(redir.at(0), 1);
  CHECK_EQ_S(redir.at(1), 2);
  CHECK_EQ_S(redir.at(2), 0);
}

void TestDeDuperWithIdsString() {
  DeDuperWithIds<std::string> dd;

  for (int i = 0; i < 5; ++i) {
    CHECK_EQ_S(dd.Add("aaa"), 0);
  }
  CHECK_EQ_S(dd.Add("bbb"), 1);
  CHECK_EQ_S(dd.Add("ccc"), 2);

  CHECK_EQ_S(dd.GetObj(0), "aaa");
  CHECK_EQ_S(dd.GetObj(1), "bbb");
  CHECK_EQ_S(dd.GetObj(2), "ccc");
  CHECK_EQ_S(dd.num_unique(), 3);
  CHECK_EQ_S(dd.num_added(), 7);

  std::vector<std::string> els = dd.GetObjVector();
  CHECK_EQ_S(els.size(), 3);
  CHECK_EQ_S(els.at(0), "aaa");
  CHECK_EQ_S(els.at(1), "bbb");
  CHECK_EQ_S(els.at(2), "ccc");
}

namespace {
int64_t qdist(int64_t lat, int64_t lon, const GNode& n) {
  int64_t dlat = lat - n.lat;
  int64_t dlon = lon - n.lon;
  return dlat * dlat + dlon * dlon;
}
}  // namespace

void TestClosestPoint() {
  FUNC_TIMER();
  Graph g;
  // Coords (-1,-1) (0,0) (1,1) (2,2)
  g.nodes.push_back({.lat = 0, .lon = 0});
  g.nodes.push_back({.lat = 40'000'000, .lon = 20'000'000});
  g.nodes.push_back({.lat = 20'000'000, .lon = 10'000'000});
  g.nodes.push_back({.lat = -20'000'000, .lon = -10'000'000});

  auto idx = SortNodeIndexesByLon(g);
  CHECK_EQ_S(idx.size(), 4);
  CHECK_EQ_S(idx.at(0), 3);
  CHECK_EQ_S(idx.at(1), 0);
  CHECK_EQ_S(idx.at(2), 2);
  CHECK_EQ_S(idx.at(3), 1);

  CHECK_EQ_S(LowerBoundBinSearch(g, idx, -10'000'001), 0);
  CHECK_EQ_S(LowerBoundBinSearch(g, idx, -10'000'000), 0);
  CHECK_EQ_S(LowerBoundBinSearch(g, idx, -9'999'999), 1);

  CHECK_EQ_S(LowerBoundBinSearch(g, idx, -1), 1);
  CHECK_EQ_S(LowerBoundBinSearch(g, idx, 0), 1);
  CHECK_EQ_S(LowerBoundBinSearch(g, idx, 1), 2);

  CHECK_EQ_S(LowerBoundBinSearch(g, idx, 9'999'999), 2);
  CHECK_EQ_S(LowerBoundBinSearch(g, idx, 10'000'000), 2);
  CHECK_EQ_S(LowerBoundBinSearch(g, idx, 10'000'001), 3);

  CHECK_EQ_S(LowerBoundBinSearch(g, idx, 19'999'999), 3);
  CHECK_EQ_S(LowerBoundBinSearch(g, idx, 20'000'000), 3);
  CHECK_EQ_S(LowerBoundBinSearch(g, idx, 20'000'001), 4);

  // Note, we've found two (lat,lon) pairs that different results slow vs. fast
  // because the have the same qdist:
  // (-20689183,66378366)
  // (-3708155,-17583690)
  srand(1);
  for (int i = 0; i < 100'000; ++i) {
    // Random coordinates in the range [-10, +10) degrees.
    int64_t lat = (rand() % (2 * 90 * TEN_POW_7)) - 90 * TEN_POW_7;
    int64_t lon = (rand() % (2 * 180 * TEN_POW_7)) - 180 * TEN_POW_7;
    auto slow = FindClosestNodeSlow(g, lat, lon);
    auto fast = FindClosestNodeFast(g, idx, lat, lon);
    if (slow.dist != fast.dist) {
      const GNode& nslow = g.nodes.at(slow.node_pos);
      const GNode& nfast = g.nodes.at(fast.node_pos);
      int64_t slow_qdist = qdist(lat, lon, nslow);
      int64_t fast_qdist = qdist(lat, lon, nfast);
      LOG_S(INFO) << absl::StrFormat(
          "Searching for (%d,%d)\nslow (%d,%d) qdist:%d cm:%d\nfast (%d,%d) "
          "qdist:%d cm:%d",
          lat, lon, nslow.lat, nslow.lon, slow_qdist, slow.dist, nfast.lat,
          nfast.lon, fast_qdist, fast.dist);
    }
  }
}

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  if (argc != 1) {
    ABORT_S() << absl::StrFormat("usage: %s", argv[0]);
  }

  TestSimpleMemPool();
  TestSimpleMemPoolAligned();
  TestHugeBitset();
  TestInts();
  TestStrings();
  TestNodeLists();
  TestNodeids();
  TestNodeDataBlock();
  TestNodeDataBlockReEncode();
  TestGraph();
  TestLineClipping();
  TestFastPolygonContains();
  TestTiledCountryLookup();
  TestUtil();
  TestArglis();
  TestCalculateDistance();
  TestRoutingConfig();

  TestKeyPartBits();
  TestParseTags();
  TestWayZones();
  TestCarMaxspeed();
  TestCarAccess();
  TestCarRoadDirection();

  TestDeDuperWithIdsInt();
  TestDeDuperWithIdsString();
  TestClosestPoint();

  LOG_S(INFO)
      << "\n\033[1;32m*****************************\nTesting successfully "
         "finished\n*****************************\033[0m";
  return 0;
}
