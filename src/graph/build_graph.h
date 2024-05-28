#pragma once

#include <map>
#include <mutex>

#include "base/huge_bitset.h"
#include "geometry/tiled_country_lookup.h"
#include "graph/data_block.h"
#include "graph/graph_def.h"
#include "graph/routing_attrs.h"
#include "graph/routing_config.h"
#include "osm/osm_helpers.h"

namespace build_graph {

struct WayTagStat {
  int64_t count = 0;
  int64_t example_way_id = 0;
};

struct HelperData {
  int64_t num_ways_selected = 0;
  int64_t num_ways_closed = 0;
  int64_t num_ways_deleted = 0;

  int64_t num_way_node_lookups = 0;
  int64_t num_way_node_lookups_found = 0;
  int64_t num_way_without_speed = 0;

  int64_t num_cross_country_edges = 0;
  int64_t num_turn_restriction_errors = 0;

  int64_t num_dead_end_nodes = 0;

  bool way_tag_stats = false;
  std::map<std::string, WayTagStat> way_tag_statmap;

  bool log_turn_restrictions = false;
};

struct MetaData {
  int n_threads = 16;
  // Assigns country codes to lon/lat positions. Used to assign countries to
  // ways.
  TiledCountryLookup* tiler = nullptr;

  // Default RoutingAttrs records percountry, highway type, rural/urban, ...
  PerCountryConfig per_country_config;

  // Nodes in-memory table. This contains all node coordinates loaded from pbf
  // file.
  DataBlockTable nodes;

  // Nodes that are referenced by a way used for routing.
  HugeBitset way_nodes_seen;
  // Nodes that are needed for routing, i.e. start/end of a way, crossings etc.
  HugeBitset way_nodes_needed;
  // Resulting graph data structure used for routing.
  Graph graph;

  // Turn restriction found in relations.
  // Currently not used.
  std::vector<TurnRestriction> turn_restrictions;

  HelperData hlp;
};

struct ParsedTag {
  // Bits of the key.
  uint64_t bits;
  // Index into the osm string table.
  uint32_t val_st_idx;
};

struct WayRural {
  uint16_t ncc = INVALID_NCC;
  ENVIRONMENT_TYPE et_forw = ET_ANY;
  ENVIRONMENT_TYPE et_backw = ET_ANY;
  IS_MOTORROAD im_forw = IM_NO;
  IS_MOTORROAD im_backw = IM_NO;
};

std::vector<ParsedTag> ParseTags(const OSMTagHelper& tagh,
                                 const OSMPBF::Way& osm_way);

void ConsumeWayStoreSeenNodesWorker(const OSMTagHelper& tagh,
                                    const OSMPBF::Way& osm_way, std::mutex& mut,
                                    HugeBitset* node_ids);

void ConsumeNodeBlob(const OSMTagHelper& tagh,
                     const OSMPBF::PrimitiveBlock& prim_block, std::mutex& mut,
                     const HugeBitset& touched_nodes_ids,
                     DataBlockTable* node_table);

// Extract country, rural/urban and motorroad information from way tags.
// This is needed to get the country specific config defaults for cwa given
// highway type.
// Handles the following keys:
//   maxspeed
//   maxspeed:source
//   maxspeed:type
//   source:maxspeed
//   zone:maxspeed
//   zone:traffic
//   traffic:zone
WayRural ExtractWayRural(const OSMTagHelper& tagh,
                         const std::vector<ParsedTag>& ptags);

void CarMaxspeed(const OSMTagHelper& tagh, std::int64_t way_id,
                 const std::vector<ParsedTag>& ptags, RoutingAttrs* ra_forw,
                 RoutingAttrs* ra_backw, bool logging_on = false);

void CarAccess(const OSMTagHelper& tagh, std::int64_t way_id,
               const std::vector<ParsedTag>& ptags, RoutingAttrs* ra_forw,
               RoutingAttrs* ra_backw, bool logging_on = false);

DIRECTION RoadDirection(const OSMTagHelper& tagh, HIGHWAY_LABEL hw,
                        int64_t way_id, const std::vector<ParsedTag>& ptags,
                        bool logging_on = false);

bool ComputeWayRoutingData(const MetaData& meta, const OSMTagHelper& tagh,
                           const OSMPBF::Way& osm_way, GWay* way);

void ConsumeWayWorker(const OSMTagHelper& tagh, const OSMPBF::Way& osm_way,
                      std::mutex& mut, MetaData* meta);

}  // namespace build_graph
