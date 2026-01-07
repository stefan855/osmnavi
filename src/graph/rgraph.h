#pragma once

#include <ranges>
#include <span>
#include <vector>

#include "base/deduper_with_ids.h"
#include "base/mmap_base.h"
#include "geometry/geometry.h"
#include "graph/graph_def.h"

/*
File Layout

VecHeader stands for (#num, offset)
VecData stands for a memory area containing an array of structs

ClusterBoundingRect = (bounding_rect)

FileHeader {
  version, date, ...
  VecHeader(ClusterBoundingRect)
  VecHeader(Cluster)
  Followed by VecData(ClusterBoundingRect)
  Followed by VecData(Cluster)
}

Cluster {
  uint32_t id
  uint64_t cluster_blob_start
  uint64_t cluster_blob_size

  // Cross cluster routing.
  VecHeader(InEdges)
  VecHeader(OutEdges)
  VecHeader(Distances)

  // Inside cluster routing.
  VecHeader(Nodes)
  VecHeader(Edges)
  VecHeader(WayWSAIdx)
  VecHeader(TurnCosts)
  VecHeader(ComplexTurnRestrictions)
  VecHeader(WaySharedAttrs)

  // Way description generation.
  VecHeader(WayStreetnameStartPos)
  VecHeader(NodeGroupedDeltaLatLon)
  VecHeader(EdgeGroupedDeltaShapeLatLon)

  // Debugging.
  VecHeader(NodeGroupedDeltaIds)
  VecHeader(WayGroupedDeltaIds)

}

*/

#if 0
// Check that we're on a little endian machine, which is true both for Intel and
// ARM. This is needed because we memory-map C++ POD (plain old data = "simple",
// C-style) structs into files and want to read them across platforms.
static_assert(std::endian::native == std::endian::little);

// Vector of elements of type 'T'. The actual data is stored in a blob starting
// at 'offset' and has 'num' elements.
template <typename T>
struct MMVec64 {
  uint64_t num;
  // Data starts at base_ptr + offset. Base pointer is the start of the memory
  // mapped file.
  uint64_t offset;

  std::span<const T> span(const uint8_t* base_ptr) const {
    return std::span<const T>((const T*)(base_ptr + offset), num);
  }

  const T& at(const uint8_t* base_ptr, size_t pos) const {
    CHECK_LT_S(pos, num);
    const T* data = (T*)(base_ptr + offset);
    return data[pos];
  }

  // The (global) offset to the first byte after the end of the vector.
  uint64_t end_offset() const { return offset + num * sizeof(T); }
};
CHECK_IS_POD(MMVec64<char>);
#endif

#if 0
// A vector that stores OSM ids. It favors low memory consumption over access
// speed.
// Each record in the vector stores kOSMIdsGroupSize delta encoded ids,
// except for the last record which might have less.
// The delta encoded ids are stored in a contiguous area of memory (see
// id_blob_start()) after the end of the regular vector.
// Memory Layout:
//   |MMVec64|num_ids|MMVec64-data|id-blob|
constexpr size_t kOSMIdsGroupSize = 64;
constexpr size_t kOSMIdsIdBits = 40;
constexpr size_t kOSMIdsBlobOffsetBits = 24;
struct MMGroupedOSMIds {
  struct IdGroup {
    uint64_t first_id : kOSMIdsIdBits;
    uint64_t blob_offset : kOSMIdsBlobOffsetBits;
  };

  MMVec64<IdGroup> mmgroups;

  // Number of Ids stored in total.
  uint32_t num_ids;

  uint64_t get_id(const uint8_t* base_ptr, uint32_t pos) const {
    size_t gidx = pos / kOSMIdsGroupSize;
    const IdGroup& group = mmgroups.at(base_ptr, gidx);
    uint64_t prev_id = group.first_id;
    uint32_t skip = pos % kOSMIdsGroupSize;
    uint32_t cnt = 0;
    const uint8_t* ptr = base_ptr + (mmgroups.end_offset() + group.blob_offset);
    while (skip-- > 0) {
      uint64_t id;
      cnt += PositiveDeltaDecodeUInt64(ptr + cnt, prev_id, &id);
      prev_id = id;
    }
    return prev_id;
  }

  // The blob that contains the delta encoded ids start at blob_start().
  // IdGroup.blob_offset has to be added to it.
  uint64_t id_blob_start() const { return mmgroups.end_offset(); }
};
CHECK_IS_POD(MMGroupedOSMIds);
#endif

struct MMBoundingRect {
  // Upper left and lower right corner of the cluster bounding rectangle;
  uint32_t lat0;
  uint32_t lon0;
  uint32_t lat1;
  uint32_t lon1;
};

constexpr uint64_t MMEdgeStartPosBits = 18;
struct MMNode {
  uint32_t edge_start_pos : MMEdgeStartPosBits;
  uint32_t border_node : 1;
  uint32_t dead_end : 1;
};

constexpr uint64_t MMTargetIdxBits = 18;
constexpr uint64_t MMWSAIdxBits = 10;
constexpr uint64_t MMTurnCostIdxBits = 14;
constexpr uint64_t MMWayIdxBits = 14;
struct MMEdge {
  uint64_t target_idx : MMTargetIdxBits;
  uint64_t wsa_idx : MMWSAIdxBits;
  uint64_t turn_cost_idx : MMTurnCostIdxBits;
  uint64_t way_idx : MMWayIdxBits;
  uint64_t dead_end : 1;
  uint64_t bridge : 1;
  uint64_t restricted : 1;
  uint64_t contra_way : 1;
  uint64_t has_shapes : 1;
  uint64_t complex_turn_restriction_trigger : 1;
};

constexpr uint64_t MMStreetnameStartPosBits = 20;
struct MMWay {
  //
};

struct MMInEdge {
  //
};

struct MMOutEdge {
  //
};

struct MMDistance {
  //
};

struct MMComplexTurnRestriction {
  //
};

struct MMLatLon {
  int32_t lat;
  int32_t lon;
};

struct MMCluster {
  uint32_t id;

  // *** Cross cluster routing.
  MMVec64<MMInEdge> in_edges;
  MMVec64<MMOutEdge> out_edges;

  // Distances computed using standard settings. The client might compute new
  // distances with different settings.
  MMVec64<MMDistance> distances;
#if 0
  // Latlons along each shortest way in 'distances'.
  // TODO: encoding format.
  shortest_way_latlongs
#endif

  // *** Inside cluster routing.
  MMVec64<MMNode> nodes;
  MMVec64<MMEdge> edges;
  MMVec64<MMWay> ways;
  MMCompressedUIntVec way_idx_to_streetname_idx;

  // Deduped shared attributes, referencded by index.
  MMVec64<WaySharedAttrs> way_shared_attrs;
  // Deduped turn cost arrays. Each array is referenced by the position of its
  // first entry in the vector and occupies one or more slots in the vector. The
  // length of the array is at position -1 and may be used for runtime checks.
  MMVec64<uint8_t> turn_cost_lists;
  // A simple vecctor of complex turn restrictions needed for routing.
  // TODO: Check that the number is small (< 50 or so). Otherwise, we might need
  // to speed up finding a match.
  MMVec64<MMComplexTurnRestriction> complex_turn_restrictions;

  // *** Way description generation.
#if 0
  NodeGroupedLatLon
  EdgeGroupedShapeLatLon
  street-name-blob (one long char array with 0-terminated streetnames.
#endif

  // *** Debugging.
  MMGroupedOSMIds grouped_osm_node_ids;
  MMGroupedOSMIds grouped_osm_way_ids;
};
CHECK_IS_POD(MMCluster);

struct MMFileHeader {
  uint64_t magic;
  uint64_t version;
  uint64_t file_size;

  MMVec64<MMBoundingRect> cluster_bounding_rects;
  MMVec64<MMCluster> clusters;
};
CHECK_IS_POD(MMFileHeader);

// ============================================================================

struct RNode {
  uint32_t edge_start_pos : 18;
  uint32_t border_node : 1;
  uint32_t dead_end : 1;
};
static_assert(std::is_standard_layout<RNode>::value);
static_assert(std::is_trivial<RNode>::value);

constexpr uint32_t RClusterMaxTurnCostBits = 14;
constexpr uint32_t RClusterMaxTurnCost = (1 << 14) - 1;

struct REdge {
  uint32_t way_idx : 10;
  uint32_t target_idx : 18;
  uint32_t dead_end : 1;
  uint32_t bridge : 1;
  uint32_t restricted : 1;
  uint32_t contra_way : 1;
  uint32_t has_shapes : 1;
  uint32_t complex_turn_restriction_trigger : 1;
  uint32_t turn_cost_idx : RClusterMaxTurnCostBits;
};
static_assert(std::is_standard_layout<REdge>::value);
static_assert(std::is_trivial<REdge>::value);

struct RLatLon {
  int32_t lat;
  int32_t lon;
};
static_assert(std::is_standard_layout<RLatLon>::value);
static_assert(std::is_trivial<RLatLon>::value);

struct RInEdge {
  uint32_t from_idx;
  uint32_t from_cluster_id;
  uint32_t target_idx;
};
static_assert(std::is_standard_layout<RInEdge>::value);
static_assert(std::is_trivial<RInEdge>::value);

struct ROutEdge {
  uint32_t from_idx;
  uint32_t target_idx;
  uint32_t target_cluster_id;
};
static_assert(std::is_standard_layout<ROutEdge>::value);
static_assert(std::is_trivial<ROutEdge>::value);

// A complex turn restriction.
struct RComplexTurnRestriction final {
  // List of edge indexes. Contains at least 3 entries.
  std::vector<uint32_t> path;
  // True if the last edge in the path is forbidden, false if the last edge is
  // mandatory.
  bool forbidden;

  uint32_t GetTriggerEdgeIdx() const {
    CHECK_GE_S(path.size(), 3);
    return path.front();
  }
};
static_assert(std::is_standard_layout<RComplexTurnRestriction>::value);
// static_assert(std::is_trivial<RComplexTurnRestriction>::value);

struct RCluster {
  uint32_t id;
  uint32_t num_border_nodes;
  uint32_t num_nodes;
  uint32_t num_deadend_nodes;
  // Forward edges between (non-dead-end) nodes in the cluster.
  uint32_t num_inner_edges;
  // Forward edges between nodes in the cluster with at least one node is in a
  // dead-end. Note that this includes bridges.
  uint32_t num_deadend_edges;
  uint32_t num_shape_nodes;

  // Bounding rectangle for the nodes in the cluster, including shape nodes.
  Rectangle<uint32_t> bounding_rect;

  // Dim #nodes
  std::vector<RNode> nodes;
  // Dim #edges
  std::vector<REdge> edges;
  // Dim #ways (edge.way_idx)
  std::vector<uint16_t> way_wsa_idx;
  // Dim #ways (edge.way_idx)
  std::vector<uint16_t> way_streetname_idx;

  // unique turn costs (edge.turn_cost_idx)
  std::vector<TurnCostData> turn_costs;
  // unique shared attrs (way_wsa_idx)
  std::vector<WaySharedAttrs> way_shared_attrs;
  // unique street names (way_streetname_idx)
  std::vector<std::string> way_streetnames;

  // All complex turn restrictions referenced by cluster.
  std::vector<RComplexTurnRestriction> complex_turn_restrictions;

  // Dim #nodes
  std::vector<int64_t> node_osm_ids;
  std::vector<RLatLon> node_latlon;

  // Dim #edges
  std::vector<RLatLon> edge_latlon;

  // Dim #ways (edge.way_idx)
  std::vector<int64_t> way_osm_ids;

  // Border edges of the cluster.
  std::vector<RInEdge> incoming_edges;
  std::vector<ROutEdge> outgoing_edges;

  // A vector of dimension |incoming_edges| * |outgoing_edges|.
  std::vector<std::vector<uint32_t>> edge_distances;

  void PrintClusterStats() const {
    uint32_t inner_with_shape = 0;
    uint32_t deadend_with_shape = 0;
    for (const REdge& e : edges) {
      if (e.has_shapes) {
        if (e.dead_end || e.bridge) {
          deadend_with_shape++;
        } else {
          inner_with_shape++;
        }
      }
    }
    LOG_S(INFO) << "*** Cluster " << id;
    LOG_S(INFO) << absl::StrFormat("num_border_nodes          %10u",
                                   num_border_nodes);
    LOG_S(INFO) << absl::StrFormat("num_nodes                 %10u", num_nodes);
    LOG_S(INFO) << absl::StrFormat("num_deadend_nodes         %10u",
                                   num_deadend_nodes);
    LOG_S(INFO) << absl::StrFormat("num_inner_edges           %10u",
                                   num_inner_edges);
    LOG_S(INFO) << absl::StrFormat("num_deadend_edges         %10u",
                                   num_deadend_edges);
    LOG_S(INFO) << absl::StrFormat("inner_with_shape          %10u",
                                   inner_with_shape);
    LOG_S(INFO) << absl::StrFormat("deadend_with_shape        %10u",
                                   deadend_with_shape);
    LOG_S(INFO) << absl::StrFormat("num_shape_nodes           %10u",
                                   num_shape_nodes);
    LOG_S(INFO) << absl::StrFormat("nodes.size()              %10llu",
                                   nodes.size());
    LOG_S(INFO) << absl::StrFormat("node_latlon.size()        %10llu",
                                   node_latlon.size());
    LOG_S(INFO) << absl::StrFormat("edges.size()              %10llu",
                                   edges.size());
    LOG_S(INFO) << absl::StrFormat("way_wsa_idx.size()        %10llu",
                                   way_wsa_idx.size());
    LOG_S(INFO) << absl::StrFormat("way_streetname_idx.size() %10llu",
                                   way_streetname_idx.size());
    LOG_S(INFO) << absl::StrFormat("turn_costs.size()         %10llu",
                                   turn_costs.size());
    LOG_S(INFO) << absl::StrFormat("way_shared_attrs.size()   %10llu",
                                   way_shared_attrs.size());
    LOG_S(INFO) << absl::StrFormat("way_streetnames.size()    %10llu",
                                   way_streetnames.size());
    LOG_S(INFO) << absl::StrFormat("node_osm_ids.size()       %10llu",
                                   node_osm_ids.size());
    LOG_S(INFO) << absl::StrFormat("way_osm_ids.size()        %10llu",
                                   way_osm_ids.size());
    LOG_S(INFO) << absl::StrFormat("incoming_edges.size()     %10llu",
                                   incoming_edges.size());
    LOG_S(INFO) << absl::StrFormat("outgoing_edges.size()     %10llu",
                                   outgoing_edges.size());
    LOG_S(INFO) << absl::StrFormat("edge_distances.size()     %10llu",
                                   edge_distances.size());
  }
};
static_assert(std::is_standard_layout<RCluster>::value);
// static_assert(std::is_trivial<RCluster>::value);

struct RGraph {
  std::vector<RCluster> clusters;
};
static_assert(std::is_standard_layout<RGraph>::value);
// static_assert(std::is_trivial<RGraph>::value);

namespace {
// For each cluster, connect the nodes in cluster_nodes->at(cluster_id).
inline void CollectClusterNodes(
    const Graph& g, std::vector<std::vector<uint32_t>>* nodes_per_cluster) {
  // Reserve the correct amount of memory in all node vectors.
  nodes_per_cluster->assign(g.clusters.size(), {});
  for (uint32_t cluster_id = 0; cluster_id < g.clusters.size(); ++cluster_id) {
    const GCluster& gc = g.clusters.at(cluster_id);
    nodes_per_cluster->reserve(gc.num_nodes + gc.num_deadend_nodes);
  }

  // Store nodes in vectors.
  for (uint32_t node_idx = 0; node_idx < g.nodes.size(); ++node_idx) {
    const GNode& n = g.nodes.at(node_idx);
    if (n.cluster_id != INVALID_CLUSTER_ID) {
      nodes_per_cluster->at(n.cluster_id).push_back(node_idx);
    }
  }

  // Sort the node vectors by "is border" then "is normal" then "is dead-end".
  // Within each category, sort by increasing idx, which laso sorts by
  // increasing OSM id.
  for (uint32_t cluster_id = 0; cluster_id < g.clusters.size(); ++cluster_id) {
    // Sort.
    std::vector<uint32_t>& nodes = nodes_per_cluster->at(cluster_id);
    std::sort(nodes.begin(), nodes.end(),
              [&g](const uint32_t a, const uint32_t b) {
                const GNode& na = g.nodes.at(a);
                const GNode& nb = g.nodes.at(b);
                if (na.cluster_border_node != nb.cluster_border_node) {
                  return na.cluster_border_node == 1;
                }
                if (na.dead_end != nb.dead_end) {
                  return na.dead_end == 0;
                }
                return a < b;
              });
  }
}

inline int rnode_edges_start(const RCluster& rc, uint32_t node_idx) {
  return rc.nodes.at(node_idx).edge_start_pos;
}

inline int rnode_edges_stop(const RCluster& rc, uint32_t node_idx) {
  return node_idx + 1 < rc.nodes.size()
             ? rc.nodes.at(node_idx + 1).edge_start_pos
             : rc.edges.size();
}

inline uint32_t rnode_num_edges(const RCluster& rc, uint32_t node_idx) {
  const uint32_t start = rnode_edges_start(rc, node_idx);
  const uint32_t stop = rnode_edges_stop(rc, node_idx);
  CHECK_GE_S(stop, start);
  return stop - start;
}

inline std::ranges::iota_view<int, int> rnode_edge_indices(const RCluster& rc,
                                                           uint32_t node_idx) {
#if 0
  LOG_S(INFO) << "AA node " << node_idx << " of total " << rc.nodes.size();
  LOG_S(INFO) << "AA Iterate from " << rnode_edges_start(rc, node_idx)
              << " to " << rnode_edges_stop(rc, node_idx);
#endif
  return std::views::iota(rnode_edges_start(rc, node_idx),
                          rnode_edges_stop(rc, node_idx));
}

inline std::span<const REdge> rnode_edges(const RCluster& rc,
                                          uint32_t node_idx) {
  const uint32_t start = rnode_edges_start(rc, node_idx);
  const uint32_t stop = rnode_edges_stop(rc, node_idx);
  return std::span<const REdge>(&(rc.edges[start]), stop - start);
}

inline std::span<REdge> rnode_edges(RCluster& rc, uint32_t node_idx) {
  const uint32_t start = rnode_edges_start(rc, node_idx);
  const uint32_t stop = rnode_edges_stop(rc, node_idx);
  return std::span<REdge>(&(rc.edges[start]), stop - start);
}

namespace {
// Add edges from g to the cluster and fill 'gedge_offset' and 'gedge_way_idx'.
void RClusterAddEdges(
    const Graph& g, const std::vector<uint32_t>& gnodes,
    const absl::flat_hash_map<uint32_t, uint32_t>& gnode_to_cnode, RCluster* rc,
    std::vector<uint16_t>* gedge_offset, std::vector<uint32_t>* gedge_way_idx) {
  // Edges.
  rc->edges.reserve(rc->num_inner_edges + rc->num_deadend_edges);
  uint32_t edge_start_pos = 0;
  // Offset of the original graph edge in the list of edges of the start node,
  // indexed by the edge index in rc->edges.
  gedge_offset->reserve(rc->num_inner_edges + rc->num_deadend_edges);
  gedge_way_idx->reserve(rc->num_inner_edges + rc->num_deadend_edges);
  for (uint32_t c_pos = 0; c_pos < gnodes.size(); ++c_pos) {
    uint32_t gn_idx = gnodes.at(c_pos);
    const GNode& n = g.nodes.at(gn_idx);
    CHECK_EQ_S(n.cluster_id, rc->id);
    rc->nodes.at(c_pos).edge_start_pos = edge_start_pos;
    for (const GEdge& e : gnode_forward_edges(g, gn_idx)) {
      const GNode& target = g.nodes.at(e.target_idx);
      if (target.cluster_id == rc->id) {
        // Add edge;
        rc->edges.push_back(
            {.way_idx = 0 /* Will be set later */,
             .target_idx = FindInMapOrFail(gnode_to_cnode, e.target_idx),
             .dead_end = e.dead_end,
             .bridge = e.bridge,
             .restricted = e.car_label != GEdge::LABEL_FREE,
             .contra_way = e.contra_way,
             .has_shapes = e.has_shapes,
             .complex_turn_restriction_trigger =
                 e.complex_turn_restriction_trigger,
             .turn_cost_idx = 0 /* Will be set later */});
        edge_start_pos++;
        gedge_offset->push_back(gnode_edge_offset(g, gn_idx, e));
        gedge_way_idx->push_back(e.way_idx);
#if 0
        rc->num_shape_nodes += g.GetGWayShadowNodes(
            g.ways.at(e.way_idx), e.contra_way ? e.target_idx : gn_idx,
            e.contra_way ? gn_idx : e.target_idx);
#endif
      }
    }
  }
  CHECK_EQ_S(rc->edges.size(), rc->num_inner_edges + rc->num_deadend_edges);
  CHECK_EQ_S(rc->edges.size(), gedge_offset->size());
  CHECK_EQ_S(rc->edges.size(), gedge_way_idx->size());
}

// Add turn costs to edges. Sets edge.turn_cost_idx and fills rc->turn_costs.
void RClusterAddTurnCosts(const Graph& g, const std::vector<uint32_t>& gnodes,
                          const std::vector<uint16_t>& gedge_offset,
                          RCluster* rc) {
  DeDuperWithIds<TurnCostData> deduper;
  for (uint32_t rn0_idx = 0; rn0_idx < rc->nodes.size(); ++rn0_idx) {
    for (uint32_t re0_idx : rnode_edge_indices(*rc, rn0_idx)) {
      TurnCostData tcd;
      const uint32_t rn1_idx = rc->edges.at(re0_idx).target_idx;
      for (uint32_t re1_idx : rnode_edge_indices(*rc, rn1_idx)) {
        // Now we have two edges representing a turn:
        //   (rn0_idx, re0_idx) and (rn1_idx, re1_idx).
        const uint32_t gn0_idx = gnodes.at(rn0_idx);
        const uint32_t ge0_off = gedge_offset.at(re0_idx);
        const uint32_t gn1_idx = gnodes.at(rn1_idx);
        const uint32_t ge1_off = gedge_offset.at(re1_idx);
        const N3Path n3p =
            N3Path::Create(g, {gn0_idx, ge0_off}, {gn1_idx, ge1_off});
        tcd.turn_costs.push_back(n3p.get_compressed_turn_cost_0to1(g));
      }
      rc->edges.at(re0_idx).turn_cost_idx = deduper.Add(tcd);
    }
  }
#if 0
  LOG_S(INFO) << "turn cost unique " << deduper.num_unique() << " of "
              << deduper.num_added();
#endif
  CHECK_LE_S(deduper.num_unique(), RClusterMaxTurnCost);
  rc->turn_costs = deduper.GetObjVector();
}

// The attributes of ways are added as individual vectors for every supported
// attribute,
//
// Attributes: wsa_idx, streetname_idx, osm_id.
void RClusterAddWayData(const Graph& g,
                        const std::vector<uint32_t>& gedge_way_idx,
                        RCluster* rc) {
  // Find the unique ways (given as way_idx).
  DeDuperWithIds<uint32_t> dd_way_idx;
  for (uint32_t e_idx = 0; e_idx < gedge_way_idx.size(); ++e_idx) {
    rc->edges.at(e_idx).way_idx = dd_way_idx.Add(gedge_way_idx.at(e_idx));
  }
  const std::vector<uint32_t> uniq_gway_idx = dd_way_idx.GetObjVector();
  dd_way_idx.clear();

  DeDuperWithIds<WaySharedAttrs> dd_way_shared_attrs;
  DeDuperWithIds<std::string> dd_way_streetnames;
  rc->way_wsa_idx.reserve(uniq_gway_idx.size());
  rc->way_streetname_idx.reserve(uniq_gway_idx.size());
  rc->way_osm_ids.reserve(uniq_gway_idx.size());
  for (uint32_t way_idx : uniq_gway_idx) {
    const GWay& w = g.ways.at(way_idx);
    uint32_t wsa_idx = dd_way_shared_attrs.Add(g.way_shared_attrs.at(w.wsa_id));
    rc->way_wsa_idx.push_back(wsa_idx);
    uint32_t streetname_idx =
        dd_way_streetnames.Add(g.streetnames.at(w.streetname_idx));
    rc->way_streetname_idx.push_back(streetname_idx);
    rc->way_osm_ids.push_back(w.id);
  }
  rc->way_shared_attrs = dd_way_shared_attrs.GetObjVector();
  rc->way_streetnames = dd_way_streetnames.GetObjVector();

#if 0
  {
    uint pos = 0;
    for (int64_t way_id : rc->way_osm_ids) {
      LOG_S(INFO) << "RCluster " << ++pos << ". uniq OSM way_id:" << way_id;
    }
  }
#endif
}

// Create an RCluster n 'rc'. rc->cluster_id has to be already set.
// 'gnodes' contains the graph node indices that are contained in the cluster.
inline void CreateRCluster(const Graph& g, const GCluster& gc,
                           const std::vector<uint32_t>& gnodes, RCluster* rc) {
  CHECK_S(rc->nodes.empty());
  rc->id = gc.cluster_id;
  rc->num_border_nodes = gc.num_border_nodes;
  rc->num_nodes = gc.num_nodes;
  rc->num_deadend_nodes = gc.num_deadend_nodes;
  rc->num_inner_edges = gc.num_inner_edges;
  rc->num_deadend_edges = gc.num_deadend_edges;
  rc->num_shape_nodes = 0;

  // Mapping table.
  absl::flat_hash_map<uint32_t, uint32_t> gnode_to_cnode;
  for (uint32_t pos = 0; pos < gnodes.size(); ++pos) {
    gnode_to_cnode[gnodes.at(pos)] = pos;
  }

  // Nodes.
  rc->nodes.reserve(gnodes.size());
  rc->node_osm_ids.reserve(gnodes.size());
  rc->node_latlon.reserve(gnodes.size());
  for (uint32_t idx : gnodes) {
    const GNode& n = g.nodes.at(idx);
    rc->nodes.push_back({.edge_start_pos = 0,
                         .border_node = n.cluster_border_node,
                         .dead_end = n.dead_end});
    rc->node_osm_ids.push_back(n.node_id);
    rc->node_latlon.push_back({.lat = n.lat, .lon = n.lon});
    /*
    LOG_S(INFO) << absl::StrFormat("Node %llu id=%lld latlon=(%d, %d)",
                                   rc->node_osm_ids.size(), n.node_id, n.lat,
                                   n.lon);
    */
  }
  CHECK_EQ_S(rc->nodes.size(), rc->num_nodes + rc->num_deadend_nodes);

  std::vector<uint16_t> gedge_offset;
  std::vector<uint32_t> gedge_way_idx;
  RClusterAddEdges(g, gnodes, gnode_to_cnode, rc, &gedge_offset,
                   &gedge_way_idx);

  RClusterAddWayData(g, gedge_way_idx, rc);
  RClusterAddTurnCosts(g, gnodes, gedge_offset, rc);

  // rc->PrintClusterStats();
}
}  // namespace

void CheckNodeVectors(
    const Graph& g,
    const std::vector<std::vector<uint32_t>>& nodes_per_cluster) {
  CHECK_EQ_S(g.clusters.size(), nodes_per_cluster.size());

  for (uint32_t cluster_id = 0; cluster_id < g.clusters.size(); ++cluster_id) {
    const GCluster& gc = g.clusters.at(cluster_id);
    CHECK_EQ_S(nodes_per_cluster.at(cluster_id).size(),
               gc.num_nodes + gc.num_deadend_nodes);
  }
}

}  // namespace

void CreateRClusters(const Graph& g) {
  FUNC_TIMER();
  RGraph rg;
  std::vector<std::vector<uint32_t>> nodes_per_cluster;

  CollectClusterNodes(g, &nodes_per_cluster);
  CheckNodeVectors(g, nodes_per_cluster);

  // Convert clusters.
  rg.clusters.reserve(g.clusters.size());
  for (uint32_t cluster_id = 0; cluster_id < g.clusters.size(); ++cluster_id) {
    rg.clusters.push_back({0});
    CreateRCluster(g, g.clusters.at(cluster_id),
                   nodes_per_cluster.at(cluster_id), &(rg.clusters.back()));
  }
}
