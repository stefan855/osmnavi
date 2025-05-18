#pragma once

#include <map>
#include <memory>
#include <mutex>

#include "algos/components.h"
#include "algos/tarjan.h"
#include "base/deduper_with_ids.h"
#include "base/frequency_table.h"
#include "base/huge_bitset.h"
#include "geometry/tiled_country_lookup.h"
#include "graph/data_block.h"
#include "graph/graph_def.h"
#include "graph/routing_attrs.h"
#include "graph/routing_config.h"
#include "osm/osm_helpers.h"
#include "osm/parsed_tag.h"
#include "osm/turn_restriction.h"

namespace build_graph {

struct BuildGraphOptions {
  // Support the following vehicle types. Possible values are {VH_MOTORCAR,
  // VH_BICYCLE, VH_FOOT}. Note that currently only cars and partially bicycle
  // are supported.
  std::vector<VEHICLE> vehicle_types = {VH_MOTORCAR};
  // Path to input OSM pbf file. Must not be empty.
  std::string pbf;
  // File pattern for admin files (country polygons).
  std::string admin_filepattern = "../../data/admin/??_*.csv";
  std::string routing_config = "../config/routing.cfg";
  // Align clusters as much as possible with country borders.
  bool align_clusters_to_ncc = false;
  // Clusters are aligned to country border. This sometimes causes small
  // clusters containing little disconnected road segments, for instance when a
  // street meanders around the border. Setting this to true merge these tiny
  // clusters across the border, such that some clusters at the border contain a
  // few nodes across the border.
  bool merge_tiny_clusters = false;

  // Max number of threads to use for parallel processing.
  int n_threads = 8;

  // Development, Debugging...
  Verbosity verb_turn_restrictions = Verbosity::Brief;
  bool log_way_tag_stats = false;
  // Compute all cluster shortest paths a second time with A* and compare to the
  // results of single source Dijkstra. This is extremely time consuming.
  bool check_shortest_cluster_paths = false;
  bool keep_all_nodes = false;
};

struct BuildGraphStats {
  // Input (pbf)
  int64_t num_nodes_in_pbf = 0;
  int64_t num_ways_in_pbf = 0;
  int64_t num_relations_in_pbf = 0;
  int64_t num_ways_with_highway_tag = 0;
  int64_t num_edges_with_highway_tag = 0;
  int64_t num_noderefs_with_highway_tag = 0;
  int64_t num_ways_too_short = 0;
  int64_t num_ways_missing_nodes = 0;
  int64_t num_ways_dup_segments = 0;

  // Graph building
  int64_t num_turn_restriction_success = 0;
  int64_t max_turn_restriction_via_ways = 0;
  int64_t num_turn_restriction_error = 0;
  int64_t num_turn_restriction_error_connection = 0;

  // Graph
  int64_t num_ways_closed = 0;

  int64_t num_ways_no_maxspeed = 0;
  int64_t num_ways_diff_maxspeed = 0;
  int64_t num_ways_has_country = 0;
  int64_t num_ways_has_streetname = 0;
  int64_t num_ways_oneway_car = 0;
  // Count ways with restricted access in one direction and free access in the
  // other direction.
  int64_t num_ways_mixed_restricted_car = 0;

  int64_t num_cross_country_edges = 0;
  int64_t num_cross_country_restricted = 0;
  int64_t num_cross_cluster_edges = 0;
  int64_t num_cross_cluster_restricted = 0;
  // int64_t num_cross_cluster_restricted_dead_end = 0;

  int64_t num_nodes_in_cluster = 0;
  int64_t num_nodes_in_small_component = 0;
  int64_t num_nodes_no_country = 0;
  int64_t num_nodes_simple_tr_via = 0;
  int64_t num_edges_inverted = 0;
  int64_t num_edges_forward = 0;
  int64_t num_edges_forward_car_restr_unset = 0;
  int64_t num_edges_forward_car_restr_free = 0;
  int64_t num_edges_forward_car_restricted = 0;
  int64_t num_edges_forward_car_restricted2 = 0;
  int64_t num_edges_forward_car_strange = 0;
  int64_t num_edges_forward_car_forbidden = 0;
  int64_t num_edges_non_unique = 0;
  int64_t num_edges_at_simple_tr_via = 0;
  int64_t max_edges = 0;
  int64_t max_edges_out = 0;
  int64_t max_edges_inverted = 0;
  int64_t min_edge_length_cm = INF64;
  int64_t max_edge_length_cm = 0;
  int64_t sum_edge_length_cm = 0;

  // Tarjan algorithm
  int64_t num_dead_end_nodes = 0;

  FrequencyTable way_tag_stats;
};

struct GraphMetaData {
  BuildGraphOptions opt;

  // Assigns country codes to lon/lat positions. Used to assign countries to
  // nodes and ways.
  std::unique_ptr<TiledCountryLookup> tiler = nullptr;

  // Default RoutingAttrs records per country, highway type, rural/urban, ...
  std::unique_ptr<PerCountryConfig> per_country_config;

  // Nodes in-memory table. This contains node coordinates loaded from pbf
  // file. All nodes in 'way_nodes_seen' are present.
  std::unique_ptr<DataBlockTable> node_table;

  // Nodes that are referenced by a way potentially used for routing. Currently
  // this is all ways that have a non-empty 'highway' tag.
  std::unique_ptr<HugeBitset> way_nodes_seen;
  // Nodes that are needed for routing, i.e. start/end of a way, crossings etc.
  std::unique_ptr<HugeBitset> way_nodes_needed;
  // Resulting graph data structure used for routing.
  Graph graph;

  // Simple turn restrictions with a single via node. Only needed during
  // construction of the graph.
  std::vector<TurnRestriction> simple_turn_restrictions;

  BuildGraphStats stats;
};

// Read a pbf file from disk, build the road network and return it together with
// auxiliary data.
GraphMetaData BuildGraph(const BuildGraphOptions& opt);

// Mark all edges if a u-turn after travelling the edge is allowed or not.
void MarkUTurnAllowedEdges(Graph* g);

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

void ConsumeWayStoreSeenNodesWorker(const OSMTagHelper& tagh,
                                    const OSMPBF::Way& osm_way, std::mutex& mut,
                                    HugeBitset* node_ids,
                                    BuildGraphStats* stats);

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
//                         std::uint16_t* maxspeed_backw, bool logging_on =
//                         false);

// void CarAccess(const OSMTagHelper& tagh, std::int64_t way_id,
//                const std::vector<ParsedTag>& ptags, RoutingAttrs* ra_forw,
//                RoutingAttrs* ra_backw, bool logging_on = false);

// DIRECTION CarRoadDirection(const OSMTagHelper& tagh, HIGHWAY_LABEL hw,
//                            int64_t way_id, const std::vector<ParsedTag>&
//                            ptags, bool logging_on = false);

// bool ComputeWayRoutingData(const GraphMetaData& meta, const OSMTagHelper&
// tagh,
//                            const OSMPBF::Way& osm_way, GWay* way);

void ConsumeWayWorker(const OSMTagHelper& tagh, const OSMPBF::Way& osm_way,
                      std::mutex& mut, DeDuperWithIds<WaySharedAttrs>* deduper,
                      GraphMetaData* meta);

}  // namespace build_graph
