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
  std::unique_ptr<ParsedTagInfo> pti;
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

// Parse tags in xml format and create an osm_way object from them. Adds
// 'num_nodes' nodes to the way, with ids 1, 2, .. num_nodes. The tags can be
// downloaded from the openstreetmap.org website.
// Each line in 'xml_tags' should have a format like <tag k="lanes" v="6"/>.
// Lines that start with '#' and empty lines are ignored.
//
// Example how to retrieve tags for way 526016665:
//   https://www.openstreetmap.org/api/0.6/way/526016665.
OsmWayWrapper FillWayData(std::string_view xml_tags, int num_nodes = 2) {
  OsmWayWrapper wr;
  wr.t.reset(new OSMPBF::StringTable);
  if (!xml_tags.empty()) {
    for (std::string_view tag :
         absl::StrSplit(xml_tags, "\n", absl::SkipEmpty())) {
      if (absl::StartsWith(tag, "#")) {
        continue;
      }
      CHECK_S(ConsumePrefixIf("<tag k=\"", &tag)) << "tag <" << tag << "> err";
      std::vector<std::string_view> kv =
          absl::StrSplit(tag, absl::MaxSplits("\" v=\"", 1));
      CHECK_EQ_S(kv.size(), 2) << "tag <" << tag << "> err";
      std::string_view key = kv.at(0);
      auto end_value = kv.at(1).find("\"/>");
      CHECK_S(end_value != std::string_view::npos) << "tag <" << tag << "> err";
      std::string_view value = kv.at(1).substr(0, end_value);
      LOG_S(INFO) << absl::StrFormat("Tag key=<%s> value=<%s>", key, value);

      wr.osm_way.mutable_keys()->Add(wr.FindOrAddString(key));
      wr.osm_way.mutable_vals()->Add(wr.FindOrAddString(value));
    }
  }
  wr.osm_way.set_id(1);
  for (int i = 0; i < num_nodes; ++i) {
    wr.osm_way.add_refs(1);  // Add a node with id + 1, i.e. 1,2,.,num_nodes.
  }
  wr.tagh.reset(new OSMTagHelper(*wr.t));
  wr.pti.reset(
      new ParsedTagInfo(*wr.tagh, ParseTags(*wr.tagh, wr.osm_way).tags()));
  return wr;
}

// Allocate a new tiler and return it. The caller has to free it.
// The tiler return 'country' for all coordinates.
TiledCountryLookup* InitCountryTiler(int16_t country) {
  const int32_t kDegreeUnits = TiledCountryLookup::kDegreeUnits;
  const int32_t tile_size = kDegreeUnits * 10;
  FastCountryPolygons p;
  // Create a tiler that always return NCC_CH. We add only one line, which is
  // enough to make it always think it's NCC_CH.
  p.AddLine(180 * kDegreeUnits, -90 * kDegreeUnits, 180 * kDegreeUnits,
            90 * kDegreeUnits, country);
  return new TiledCountryLookup(p, tile_size);
}

build_graph::GraphMetaData CreateMeta() {
  build_graph::GraphMetaData meta;

  meta.node_table.reset(new DataBlockTable(/*alloc_unit*/ 1024));
  meta.tiler.reset(InitCountryTiler(NCC_CH));
  meta.per_country_config.reset(new PerCountryConfig);
  meta.per_country_config->ReadConfig("../config/routing.cfg");

  return meta;
}

// Store dummy nodes with id starting at 1. Lat/lon get the same value as id.
void StoreNodes(int num_nodes, DataBlockTable* t) {
  NodeBuilder builder;
  for (int32_t i = 1; i <= num_nodes; ++i) {
    builder.AddNode({.id = (uint32_t)i, .lat = i, .lon = i});
  }
  builder.AddBlockToTable(t);
}

void TestWay1() {
  FUNC_TIMER();
  // Tags of way id=526016665 version=20, retrieved on 20241113.
  const char* WayData = R"(
# bicycle:lanes specifies *access*.
<tag k="bicycle:lanes:forward" v="no|yes|designated|no"/>

# cycleway:lanes specifies the existence and type of cycleway=* on a per-lane
# basis. See https://wiki.openstreetmap.org/wiki/Key:cycleway:lanes
# Note the difference to https://wiki.openstreetmap.org/wiki/Key:cycleway:lane
# which distinguishes between the different types of cycle lanes.
<tag k="cycleway:lanes:forward" v="||lane|"/>

<tag k="cycleway:left" v="lane"/>
<tag k="cycleway:left:lane" v="exclusive"/>
<tag k="cycleway:right" v="lane"/>
<tag k="cycleway:right:lane" v="advisory"/>
<tag k="embedded_rails" v="tram"/>
<tag k="foot" v="use_sidepath"/>
<tag k="highway" v="tertiary"/>
<tag k="lanes" v="6"/>
<tag k="lanes:backward" v="2"/>
<tag k="lanes:forward" v="4"/>
<tag k="lit" v="yes"/>
<tag k="maxspeed" v="50"/>
<tag k="name" v="Kasernenstrasse"/>
<tag k="oneway" v="no"/>
<tag k="overtaking" v="no"/>
<tag k="parking:condition:both" v="no_parking"/>
<tag k="sidewalk" v="separate"/>
<tag k="smoothness" v="good"/>
<tag k="surface" v="asphalt"/>
<tag k="turn:lanes:forward" v="|left|through|through"/>
<tag k="vehicle:lanes:backward" v="no|yes"/>
<tag k="vehicle:lanes:forward" v="no|yes|no|yes"/>
<tag k="width" v="20"/>
)";
  constexpr int num_nodes = 2;  // Our way has two nodes.
  build_graph::GraphMetaData meta = CreateMeta();
  StoreNodes(num_nodes, meta.node_table.get());
  DeDuperWithIds<WaySharedAttrs> deduper;
  OsmWayWrapper wr = FillWayData(WayData, num_nodes);
  std::mutex mut;

  ConsumeWayWorker(*wr.tagh, wr.osm_way, mut, &deduper, &meta, &meta.Stats());
  CHECK_EQ_S(meta.graph.ways.size(), 1);
  CHECK_EQ_S(deduper.num_added(), 1);
}

void TestWay2() {
  FUNC_TIMER();
  // Tags of way id=173455068 version=22, retrieved on 20250103.
  // Check that access:lanes:backward=no| does not cause backward access=no.
  const char* WayData = R"(
<tag k="access:lanes:backward" v="no|"/>
<tag k="bicycle:lanes:backward" v="designated|yes"/>
# <tag k="change:lanes:backward" v="no|no"/>
# <tag k="change:lanes:forward" v="not_left"/>
<tag k="cycleway:left" v="lane"/>
<tag k="cycleway:left:traffic_sign" v="DE:237"/>
<tag k="highway" v="primary"/>
<tag k="lanes" v="2"/>
<tag k="lanes:backward" v="1"/>
<tag k="lanes:forward" v="1"/>
<tag k="maxspeed" v="20"/>
# <tag k="name" v="Rheinbrückenstraße"/>
# <tag k="placement:backward" v="middle_of:1"/>
# <tag k="surface" v="asphalt"/>
# <tag k="width:lanes:backward" v="2|4"/>
# <tag k="width:lanes:forward" v="4"/>
)";
  constexpr int num_nodes = 2;  // Our way has two nodes.
  build_graph::GraphMetaData meta = CreateMeta();
  meta.opt.vt = VH_MOTORCAR;
  StoreNodes(num_nodes, meta.node_table.get());
  DeDuperWithIds<WaySharedAttrs> deduper;
  OsmWayWrapper wr = FillWayData(WayData, num_nodes);
  std::mutex mut;

  ConsumeWayWorker(*wr.tagh, wr.osm_way, mut, &deduper, &meta, &meta.Stats());
  CHECK_EQ_S(meta.graph.ways.size(), 1);
  CHECK_EQ_S(deduper.num_added(), 1);
  const WaySharedAttrs& wsa = deduper.GetObj(0);
  CHECK_S(WSAAnyRoutable(wsa, DIR_FORWARD));
  CHECK_S(WSAAnyRoutable(wsa, DIR_BACKWARD));
}

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  if (argc != 1) {
    ABORT_S() << absl::StrFormat("usage: %s", argv[0]);
  }

  TestWay1();
  TestWay2();

  LOG_S(INFO)
      << "\n\033[1;32m*****************************\nTesting successfully "
         "finished\n*****************************\033[0m";
  return 0;
}
