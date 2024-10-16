#pragma once

#include <map>
#include <mutex>

#include "base/deduper_with_ids.h"
#include "base/huge_bitset.h"
#include "geometry/tiled_country_lookup.h"
#include "graph/data_block.h"
#include "graph/graph_def.h"
#include "graph/routing_attrs.h"
#include "graph/routing_config.h"
#include "osm/osm_helpers.h"
#include "osm/parsed_tag.h"

namespace build_graph {

struct WayTagStat {
  int64_t count = 0;
  int64_t example_way_id = 0;
};

struct MetaStatsData {
  int64_t num_ways_with_highway_tag = 0;
  int64_t num_edges_with_highway_tag = 0;
  int64_t num_noderefs_with_highway_tag = 0;
  int64_t num_ways_closed = 0;
  int64_t num_ways_missing_nodes = 0;

  int64_t num_way_node_lookups = 0;
  int64_t num_way_node_lookups_found = 0;
  int64_t num_way_without_speed = 0;

  int64_t num_cross_country_edges = 0;
  int64_t num_turn_restriction_errors = 0;

  int64_t num_dead_end_nodes = 0;

  bool log_way_tag_stats = false;
  std::map<std::string, WayTagStat> way_tag_statmap;

  bool log_turn_restrictions = false;

  // TODO: may help improve multithreading:
  // void AddStats(const MetaStatsData& other);
};

struct MetaData {
  int n_threads = 6;
  // Assigns country codes to lon/lat positions. Used to assign countries to
  // nodes and ways.
  TiledCountryLookup* tiler = nullptr;

  // Default RoutingAttrs records per country, highway type, rural/urban, ...
  PerCountryConfig per_country_config;

  // Nodes in-memory table. This contains node coordinates loaded from pbf
  // file. All nodes in 'way_nodes_seen' are present.
  DataBlockTable nodes;

  // Nodes that are referenced by a way potentially used for routing. Currently
  // this is all ways that have a non-empty 'highway' tag.
  HugeBitset way_nodes_seen;
  // Nodes that are needed for routing, i.e. start/end of a way, crossings etc.
  HugeBitset way_nodes_needed;
  // Resulting graph data structure used for routing.
  Graph graph;

  // Turn restriction found in relations.
  // Currently not used.
  std::vector<TurnRestriction> turn_restrictions;

  MetaStatsData stats;
};

#if 0
struct ParsedTag {
  // A bit set representing the components in the parsed key.
  // See osm/key_bits.h.
  uint64_t bits;
  // String value, given as index into the osm string table.
  uint32_t val_st_idx;
};
#endif

// Way attributes country code, rural/urban and is_motorroad as extracted from
// tags. Note that country code is only used for error reporting, since we
// derive the country from lat/lon of the nodes.
struct WayTaggedZones {
  uint16_t ncc = INVALID_NCC;
  ENVIRONMENT_TYPE et_forw = ET_ANY;
  ENVIRONMENT_TYPE et_backw = ET_ANY;
  IS_MOTORROAD im_forw = IM_NO;
  IS_MOTORROAD im_backw = IM_NO;
};

// std::vector<ParsedTag> ParseTags(const OSMTagHelper& tagh,
//                                  const OSMPBF::Way& osm_way);

void ConsumeWayStoreSeenNodesWorker(const OSMTagHelper& tagh,
                                    const OSMPBF::Way& osm_way, std::mutex& mut,
                                    HugeBitset* node_ids, MetaStatsData* stats);

void ConsumeNodeBlob(const OSMTagHelper& tagh,
                     const OSMPBF::PrimitiveBlock& prim_block, std::mutex& mut,
                     const HugeBitset& touched_nodes_ids,
                     DataBlockTable* node_table);

// Extract country, rural/urban and motorroad information from way tags.
// This is needed to get the country specific config defaults for a given
// highway type.
// Handles the following keys:
//   maxspeed
//   maxspeed:source
//   maxspeed:type
//   source:maxspeed
//   zone:maxspeed
//   zone:traffic
//   traffic:zone
WayTaggedZones ExtractWayZones(const OSMTagHelper& tagh,
                               const std::vector<ParsedTag>& ptags);

// Extract the maxspeed from an osm way, in both directions. If there arre
// lanes, then the highest maxspeed is returned. If no maxspeed is set, then 0
// is returned.
// void CarMaxspeedFromWay(const OSMTagHelper& tagh, std::int64_t way_id,
//                         const std::vector<ParsedTag>& ptags,
//                         std::uint16_t* maxspeed_forw,
//                         std::uint16_t* maxspeed_backw, bool logging_on = false);

// void CarAccess(const OSMTagHelper& tagh, std::int64_t way_id,
//                const std::vector<ParsedTag>& ptags, RoutingAttrs* ra_forw,
//                RoutingAttrs* ra_backw, bool logging_on = false);

// DIRECTION CarRoadDirection(const OSMTagHelper& tagh, HIGHWAY_LABEL hw,
//                            int64_t way_id, const std::vector<ParsedTag>& ptags,
//                            bool logging_on = false);

// bool ComputeWayRoutingData(const MetaData& meta, const OSMTagHelper& tagh,
//                            const OSMPBF::Way& osm_way, GWay* way);

void ConsumeWayWorker(const OSMTagHelper& tagh, const OSMPBF::Way& osm_way,
                      std::mutex& mut, DeDuperWithIds<WaySharedAttrs>* deduper,
                      MetaData* meta);

}  // namespace build_graph
