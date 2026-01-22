#pragma once

#include <ranges>
#include <span>
#include <vector>

#include "base/deduper_with_ids.h"
#include "base/mmap_base.h"
#include "geometry/geometry.h"
#include "graph/graph_def.h"

// Stores basic node data in an uint64_t.
struct MMNodeBits {
  bool border_node() const { return (data__ & 1ull) != 0; }
  bool dead_end() const { return (data__ & 2ull) != 0; }
  bool off_cluster_node() const { return (data__ & 4ull) != 0; }
  uint64_t edge_start_pos() const { return data__ >> 3ull; }

  // Set individual values.
  void set_border_node(bool val) { data__ = (data__ & ~1ull) | val; }
  void set_dead_end(bool val) { data__ = (data__ & ~2ull) | (val << 1ull); }
  void set_off_cluster_node(bool val) {
    data__ = (data__ & ~4ull) | (val << 2ull);
  }
  void set_edge_start_pos(uint64_t val) {
    CHECK_LT_S(val, 1ull << 61);
    data__ = (val << 3ull) | (data__ & 7ull);
  }

  // Data.
  uint64_t data__;
};
CHECK_IS_POD(MMNodeBits);
#define MM_NODE(x) ((const MMNodeBits)(x))
#define MM_NODE_RW(x) ((MMNodeBits&)(x))

// Stores basic edge data in an uint64_t. The data consists of several 1-bit and
// one unsigned multi-bit integer. The 1-bit values are stored in the lowest
// bits, the multi-bit value is stored in the bits just above the
// 1-bit values. Because of this layout, the uint64_t can be efficiently stored
// in a compressed vector.
struct MMEdgeBits {
  bool dead_end() const { return (data__ & 1ull) != 0; }
  bool bridge() const { return (data__ & 2ull) != 0; }
  bool restricted() const { return (data__ & 4ull) != 0; }
  bool contra_way() const { return (data__ & 8ull) != 0; }
  bool cluster_border_edge() const { return (data__ & 16ull) != 0; }
  bool complex_turn_restriction_trigger() const {
    return (data__ & 32ull) != 0;
  }
  uint64_t target_idx() const { return data__ >> 6ull; }

  // Set individual values.
  void set_dead_end(bool val) { data__ = (data__ & ~1ull) | val; }
  void set_bridge(bool val) { data__ = (data__ & ~2ull) | (val << 1ull); }
  void set_restricted(bool val) { data__ = (data__ & ~4ull) | (val << 2ull); }
  void set_contra_way(bool val) { data__ = (data__ & ~8ull) | (val << 3ull); }
  void set_cluster_border_edge(bool val) {
    data__ = (data__ & ~16ull) | (val << 4ull);
  }
  void set_complex_turn_restriction_trigger(bool val) {
    data__ = (data__ & ~32ull) | (val << 5ull);
  }
  void set_target_idx(uint64_t val) {
    CHECK_LT_S(val, 1ull << 58ull);
    data__ = (val << 6ull) | (data__ & 63ull);
  }

  // Data.
  uint64_t data__;
};
CHECK_IS_POD(MMEdgeBits);
#define MM_EDGE(x) ((const MMEdgeBits)(x))
#define MM_EDGE_RW(x) ((MMEdgeBits&)(x))

/*
constexpr uint32_t MMEdgeStartPosBits = 18;
struct MMNode {
  uint32_t edge_start_pos : MMEdgeStartPosBits;
  uint32_t border_node : 1;
  uint32_t dead_end : 1;
  uint32_t off_cluster_node : 1;
};
*/

/*
constexpr uint32_t MMTargetIdxBits = 18;
constexpr uint32_t MMTurnCostPosBits = 14;
struct MMEdge {
  uint32_t target_idx : MMTargetIdxBits;
  uint32_t turn_cost_pos : MMTurnCostPosBits;
  uint32_t dead_end : 1;
  uint32_t bridge : 1;
  uint32_t restricted : 1;
  uint32_t contra_way : 1;
  uint32_t cluster_border_edge : 1;
  uint32_t complex_turn_restriction_trigger : 1;
};
*/

struct MMInEdge {
  uint32_t from_cluster_id;  // The cluster id of the other cluster.
  uint32_t from_node_idx;    // in the curfrent cluster.
  uint32_t to_node_idx;
  uint32_t edge_idx;     // Index of this edge in the edge array.
  uint32_t in_edge_pos;  // Position in the list of in-edges of this cluster.
};

struct MMOutEdge {
  uint32_t from_node_idx;
  uint32_t to_node_idx;
  uint32_t edge_idx;
  uint32_t to_cluster_id;  // Id of the other cluster.
  uint32_t out_edge_pos;   // Position in the list of in-edges of this cluster.
};

struct MMComplexTurnRestriction {
  uint16_t forbidden : 1;
  uint16_t len;
  uint32_t first_leg_pos;
  uint32_t trigger_edge_idx;
};

struct MMLatLon {
  int32_t lat;
  int32_t lon;
};

// This is the memory mapped data structure that represents one cluster in the
// file.
struct MMCluster {
  // Nodes in 'nodes' are sorted by type, with the following layout:
  // border_nodes | off_cluster_nodes | inner_nodes | dead_end_nodes.
  uint16_t num_border_nodes;
  uint16_t num_off_cluster_nodes;
  uint32_t num_inner_nodes;
  uint32_t num_dead_end_nodes;

  // *** Cross cluster routing.
  MMVec64<MMInEdge> in_edges;
  MMVec64<MMOutEdge> out_edges;

  // Distances computed using standard settings. The client might compute new
  // distances with different settings.
  MMVec64<uint32_t> border_distances;

  // *** Inside cluster routing.
  // MMVec64<MMNode> nodes;
  // MMVec64<MMEdge> edges;
  MMCompressedUIntVec nodes;  // Cast to MMNodeBits to write or read.
  MMCompressedUIntVec edges;  // Cast to MMEdgeBits to write or read.
  MMCompressedUIntVec edge_to_distance;
  MMCompressedUIntVec edge_to_way_idx;
  MMCompressedUIntVec edge_to_turn_costs_pos;
  MMCompressedUIntVec way_idx_to_wsa_idx;

  // Deduped shared attributes, referencded by index.
  MMVec64<WaySharedAttrs> way_shared_attrs;
  MMTurnCostsTable turn_costs_table;
  // A vector of complex turn restrictions needed for routing. Sorted by
  // 'trigger_edge_idx'.
  MMVec64<MMComplexTurnRestriction> complex_turn_restrictions;
  // Array referenced by entries in complex_turn_restrictions.
  MMVec64<uint32_t> complex_turn_restriction_legs;

  // *** Way description generation.
  // A blob of all zero terminated strings concatenated together into one big
  // string. Streetnames are referenced by the start position in this array.
  MMVec64<MMLatLon> node_to_latlon;
  MMCompressedUIntVec way_to_streetname_pos;
  MMStringsTable streetnames_table;

  // *** Debugging.
  MMGroupedOSMIds grouped_node_to_osm_id;
  MMGroupedOSMIds grouped_way_to_osm_id;

  // Helper functions.
  uint32_t start_border_nodes() const { return 0; }
  uint32_t start_off_cluster_nodes() const { return num_border_nodes; }
  uint32_t start_inner_nodes() const {
    return num_border_nodes + num_off_cluster_nodes;
  }
  uint32_t start_dead_end_nodes() const {
    return num_border_nodes + num_off_cluster_nodes + num_inner_nodes;
  }

#if 0
  uint32_t edges_start_pos(uint32_t node_idx) {
    return MM_NODE(nodes.at(node_idx)).edge_start_pos();
  }
  uint32_t edges_stop_pos(uint32_t node_idx) {
    return (node_idx < nodes.size() - 1)
               ? MM_NODE(nodes.at(node_idx + 1)).edge_start_pos()
               : edges.size();
  }
#endif
};
CHECK_IS_POD(MMCluster);

struct MMClusterWrapper {
  const MMCluster& g;
  std::vector<uint32_t> edge_weights;
  const uint8_t* const base_ptr;
};

struct MMBoundingRect {
  // Upper left and lower right corner of the cluster bounding rectangle;
  uint32_t lat0;
  uint32_t lon0;
  uint32_t lat1;
  uint32_t lon1;
};

struct MMFileHeader {
  uint64_t magic;
  uint32_t version_major;
  uint32_t version_minor;
  uint64_t file_size;

  MMVec64<MMBoundingRect> cluster_bounding_rects;
  MMVec64<MMCluster> clusters;
};
CHECK_IS_POD(MMFileHeader);

namespace {
constexpr uint64_t kMagic = 7715514337782280064ull;
constexpr uint64_t kVersionMajor = 0;
constexpr uint64_t kVersionMinor = 1;

struct TmpComplexTR {
  bool forbidden;
  std::vector<uint32_t> path;
};

// Temporary data for a cluster, before it is written to the mmap file.
struct TmpClusterInfo {
  uint32_t cluster_id;

  // **************************************************************************
  // Auxiliary data.
  // **************************************************************************
  std::vector<int64_t> cnode_to_gnode;
  absl::flat_hash_map<uint32_t, uint32_t> gnode_to_cnode;

  absl::flat_hash_map<uint32_t, uint32_t> gway_to_cway;
  std::vector<uint32_t> cway_to_streetname_idx;
  std::vector<std::string> streetnames;

  // Offset of edges in graph, indexed by cedge index.
  std::vector<uint16_t> cedge_to_gedge_offset;
  // Way index of edges in graph, indexed by cedge index.
  std::vector<uint32_t> cedge_to_gway_idx;

  std::vector<uint32_t> cedge_to_turn_cost_idx;
  std::vector<TurnCostData> turn_costs;

  std::vector<TmpComplexTR> complex_tr;

  // **************************************************************************
  // Data prepared for memory mapping. Uses only cluster relative indexing.
  // **************************************************************************

  // Bounding rectangle for the nodes in the cluster, including shape nodes.
  MMBoundingRect mm_bounding_rect;

  std::vector<MMInEdge> mm_in_edges;
  std::vector<MMOutEdge> mm_out_edges;

  // Dim #nodes
  std::vector<uint64_t> mm_nodes;
  std::vector<int64_t> mm_node_to_osm_id;
  std::vector<MMLatLon> mm_node_latlon;

  // Dim #edges
  std::vector<uint64_t> mm_edges;  // Type MMEdgeBits.
  std::vector<uint32_t> mm_edge_to_distance;
  std::vector<uint32_t> mm_edge_to_way_idx;

  // Dim #ways
  std::vector<int64_t> mm_way_to_osm_id;
  std::vector<uint32_t> mm_way_to_wsa;

  // Other dims.
  std::vector<WaySharedAttrs> mm_way_shared_attrs;
};

// For each cluster, initialise TmpClusterInfo and collect the nodes belonging
// to the cluster in ci.cnode_to_gnode.
//
// This includes dead-ends, that is every dead-end belongs to exactly one
// cluster.
//
// This also includes border nodes of other clusters that are connected through
// incoming or outgoing edges to the cluster.
inline void CollectClusterNodes(const Graph& g,
                                std::vector<TmpClusterInfo>* cluster_infos) {
  // Prepare TmpClusterInfo for each cluster.
  cluster_infos->assign(g.clusters.size(), {0});
  for (uint32_t cluster_id = 0; cluster_id < g.clusters.size(); ++cluster_id) {
    const GCluster& gc = g.clusters.at(cluster_id);
    TmpClusterInfo* ci = &cluster_infos->at(cluster_id);
    ci->cluster_id = cluster_id;
    // +500 to accommodate the border nodes of connected clusters.
    ci->cnode_to_gnode.reserve(gc.num_nodes + gc.num_deadend_nodes + 500);
  }

  // Iterate over graph nodes and store node indices for each cluster.
  for (uint32_t node_idx = 0; node_idx < g.nodes.size(); ++node_idx) {
    const GNode& n = g.nodes.at(node_idx);
    if (n.cluster_id != INVALID_CLUSTER_ID) {
      cluster_infos->at(n.cluster_id).cnode_to_gnode.push_back(node_idx);
      if (n.cluster_border_node) {
        // Store connected nodes from other clusters.
        for (const GEdge& e : gnode_all_edges(g, node_idx)) {
          const GNode& target = g.nodes.at(e.target_idx);
          if (e.unique_target && target.cluster_id != n.cluster_id) {
            CHECK_NE_S(target.cluster_id, INVALID_CLUSTER_ID);
            // Add connected non-cluster node. This might create duplicate
            // entries, so we need to de-duplicate after sorting below.
            cluster_infos->at(n.cluster_id)
                .cnode_to_gnode.push_back(e.target_idx);
          }
        }
      }
    }
  }

  // Sort the node vectors into
  //   1. "border nodes"
  //   2. "off cluster nodes"
  //   3. "normal nodes"
  //   4. "dead-end nodes"
  // Within each category, nodes are sorted by both idx and osm id
  for (TmpClusterInfo& ci : *cluster_infos) {
    // Sort.
    std::sort(ci.cnode_to_gnode.begin(), ci.cnode_to_gnode.end(),
              [&g, &ci](const uint32_t a, const uint32_t b) {
                const GNode& na = g.nodes.at(a);
                const GNode& nb = g.nodes.at(b);
                if (na.cluster_border_node != nb.cluster_border_node) {
                  return na.cluster_border_node == 1;
                }
                if (na.cluster_id != nb.cluster_id) {
                  return na.cluster_id == ci.cluster_id;
                }
                if (na.dead_end != nb.dead_end) {
                  return na.dead_end == 0;
                }
                return a < b;  // Default sort: by idx (and by construction id).
              });
    // De-duplicate (only works when it is sorted).
    // See above why this is needed.
    auto last = std::unique(ci.cnode_to_gnode.begin(), ci.cnode_to_gnode.end());
    if (last != ci.cnode_to_gnode.end()) {
      ci.cnode_to_gnode.erase(last, ci.cnode_to_gnode.end());
    }
  }
}

// Add edges from g to the cluster and fill 'ci->cedge_to_gedge_offset' and
// 'ci->cedge_to_gway_idx'.
void ClusterAddEdges(const Graph& g, TmpClusterInfo* ci) {
  const GCluster& gcl = g.clusters.at(ci->cluster_id);
  const uint32_t expected_num_edges =
      gcl.num_inner_edges + gcl.num_deadend_edges + 500;
  // Edges.
  ci->mm_edges.reserve(expected_num_edges);
  uint32_t edge_start_pos = 0;
  ci->cedge_to_gedge_offset.reserve(expected_num_edges);
  ci->cedge_to_gway_idx.reserve(expected_num_edges);
  ci->mm_edge_to_distance.reserve(expected_num_edges);
  for (uint32_t c_pos = 0; c_pos < ci->cnode_to_gnode.size(); ++c_pos) {
    uint32_t gn_idx = ci->cnode_to_gnode.at(c_pos);
    const GNode& n = g.nodes.at(gn_idx);
    // CHECK_EQ_S(n.cluster_id, ci->cluster_id);
    MM_NODE_RW(ci->mm_nodes.at(c_pos)).set_edge_start_pos(edge_start_pos);
    CHECK_EQ_S(MM_NODE(ci->mm_nodes.at(c_pos)).edge_start_pos(),
               edge_start_pos);
    for (const GEdge& e : gnode_forward_edges(g, gn_idx)) {
      const GNode& target = g.nodes.at(e.target_idx);
      // We're interested in the edge if one of the two nodes is in-cluster.
      if (n.cluster_id == ci->cluster_id ||
          target.cluster_id == ci->cluster_id) {
        CHECK_EQ_S((n.cluster_id == ci->cluster_id) !=
                       (target.cluster_id == ci->cluster_id),
                   e.cluster_border_edge);
        // Add edge;
        uint32_t ctarget_idx =
            FindInMapOrFail(ci->gnode_to_cnode, e.target_idx);
        // CHECK_LT_S(ctarget_idx, 1u << MMTargetIdxBits);
        MMEdgeBits eb{0};
        eb.set_target_idx(ctarget_idx);
        eb.set_dead_end(e.dead_end);
        eb.set_bridge(e.bridge);
        eb.set_restricted(e.car_label != GEdge::LABEL_FREE);
        eb.set_contra_way(e.contra_way);
        eb.set_cluster_border_edge(e.cluster_border_edge);
        eb.set_complex_turn_restriction_trigger(
            e.complex_turn_restriction_trigger);
        ci->mm_edges.push_back(eb.data__);

        edge_start_pos++;
        ci->mm_edge_to_distance.push_back(e.distance_cm);
        ci->cedge_to_gedge_offset.push_back(gnode_edge_offset(g, gn_idx, e));
        ci->cedge_to_gway_idx.push_back(e.way_idx);
#if 0
        rc->num_shape_nodes += g.GetGWayShadowNodes(
            g.ways.at(e.way_idx), e.contra_way ? e.target_idx : gn_idx,
            e.contra_way ? gn_idx : e.target_idx);
#endif
      }
    }
  }
  // CHECK_LE_S(edge_start_pos, 1u << MMEdgeStartPosBits);
  // CHECK_EQ_S(rc->edges.size(), rc->num_inner_edges + rc->num_deadend_edges);
  CHECK_EQ_S(ci->mm_edges.size(), ci->cedge_to_gedge_offset.size());
  CHECK_EQ_S(ci->mm_edges.size(), ci->cedge_to_gway_idx.size());
}

void ClusterAddWayData(const Graph& g, TmpClusterInfo* ci) {
  // Determine the unique ways (given as gway_idx).
  DeDuperWithIds<uint32_t> dd_gway_idx;
  ci->mm_edge_to_way_idx.reserve(ci->cedge_to_gway_idx.size());
  for (uint32_t e_idx = 0; e_idx < ci->cedge_to_gway_idx.size(); ++e_idx) {
    ci->mm_edge_to_way_idx.push_back(
        dd_gway_idx.Add(ci->cedge_to_gway_idx.at(e_idx)));
  }

  const std::vector<uint32_t> uniq_gway_idx = dd_gway_idx.GetObjVector();
  CHECK_S(ci->gway_to_cway.empty());
  for (uint32_t cway_idx = 0; cway_idx < uniq_gway_idx.size(); ++cway_idx) {
    ci->gway_to_cway[uniq_gway_idx.at(cway_idx)] = cway_idx;
  }
  dd_gway_idx.clear();

  DeDuperWithIds<WaySharedAttrs> dd_way_shared_attrs;
  DeDuperWithIds<std::string> dd_way_streetnames;
  ci->mm_way_to_wsa.reserve(uniq_gway_idx.size());
  ci->mm_way_to_osm_id.reserve(uniq_gway_idx.size());
  ci->cway_to_streetname_idx.reserve(uniq_gway_idx.size());
  for (uint32_t way_idx : uniq_gway_idx) {
    const GWay& w = g.ways.at(way_idx);

    uint32_t wsa_idx = dd_way_shared_attrs.Add(g.way_shared_attrs.at(w.wsa_id));
    ci->mm_way_to_wsa.push_back(wsa_idx);

    uint32_t streetname_idx =
        dd_way_streetnames.Add(g.streetnames.at(w.streetname_idx));
    ci->cway_to_streetname_idx.push_back(streetname_idx);

    ci->mm_way_to_osm_id.push_back(w.id);
  }
  ci->mm_way_shared_attrs = dd_way_shared_attrs.GetObjVector();
  ci->streetnames = dd_way_streetnames.GetObjVector();
}

inline std::ranges::iota_view<uint32_t, uint32_t> cnode_edge_indices(
    const TmpClusterInfo& ci, uint32_t node_idx) {
  uint32_t start = MM_NODE(ci.mm_nodes.at(node_idx)).edge_start_pos();
  uint32_t stop = node_idx + 1 < ci.mm_nodes.size()
                      ? MM_NODE(ci.mm_nodes.at(node_idx + 1)).edge_start_pos()
                      : ci.mm_edges.size();
  // LOG_S(INFO) << "start:" << start << " stop:" << stop;
  return std::views::iota(start, stop);
}

// Add turn costs to edges. Sets edge.turn_cost_idx and fills ci->turn_costs.
void ClusterAddTurnCosts(const Graph& g, TmpClusterInfo* ci) {
  ci->cedge_to_turn_cost_idx.reserve(ci->mm_edges.size());
  DeDuperWithIds<TurnCostData> deduper;
  for (uint32_t cn0_idx = 0; cn0_idx < ci->mm_nodes.size(); ++cn0_idx) {
    for (uint32_t ce0_idx : cnode_edge_indices(*ci, cn0_idx)) {
      TurnCostData tcd;
      // const uint32_t cn1_idx = ci->mm_edges.at(ce0_idx).target_idx;
      const uint32_t cn1_idx = MM_EDGE(ci->mm_edges.at(ce0_idx)).target_idx();
      for (uint32_t ce1_idx : cnode_edge_indices(*ci, cn1_idx)) {
        // Now we have two edges representing a turn:
        //   (cn0_idx, ce0_idx) and (cn1_idx, ce1_idx).
        //  Get the turn cost for this turn that were computed in 'graph'.
        const uint32_t gn0_idx = ci->cnode_to_gnode.at(cn0_idx);
        const uint32_t ge0_off = ci->cedge_to_gedge_offset.at(ce0_idx);
        const uint32_t gn1_idx = ci->cnode_to_gnode.at(cn1_idx);
        const uint32_t ge1_off = ci->cedge_to_gedge_offset.at(ce1_idx);
        const N3Path n3p =
            N3Path::Create(g, {gn0_idx, ge0_off}, {gn1_idx, ge1_off});
        tcd.turn_costs.push_back(n3p.get_compressed_turn_cost_0to1(g));
      }
      // Check we're iterating all edges from 0 to size.
      CHECK_EQ_S(ci->cedge_to_turn_cost_idx.size(), ce0_idx);
      ci->cedge_to_turn_cost_idx.push_back(deduper.Add(tcd));
      // ci->mm_edges.at(ce0_idx).turn_cost_idx = deduper.Add(tcd);
    }
  }
  ci->turn_costs = deduper.GetObjVector();
}

TmpComplexTR ConvertToComplexTr(const Graph& g, const TmpClusterInfo& ci,
                                const TurnRestriction& tr) {
  TmpComplexTR res;
  res.forbidden = tr.forbidden;
  for (const TurnRestriction::TREdge& tr_edge : tr.path) {
    const auto iter_from = ci.gnode_to_cnode.find(tr_edge.from_node_idx);
    const auto iter_to = ci.gnode_to_cnode.find(tr_edge.to_node_idx);
    if (iter_from == ci.gnode_to_cnode.end() ||
        iter_to == ci.gnode_to_cnode.end()) {
      ABORT_S() << "Can't find from/to node";
    }
    uint32_t cfrom_idx = iter_from->second;
    uint32_t cto_idx = iter_to->second;

    /*
    const auto iter_way = ci.gway_to_cway.find(tr_edge.way_idx);
    if (iter_way == ci.gway_to_cway.end()) {
      ABORT_S() << "Can't find way";
    }
    uint32_t cway_idx = iter_way->second;
    */

    for (uint32_t cedge_idx : cnode_edge_indices(ci, cfrom_idx)) {
      if (MM_EDGE(ci.mm_edges.at(cedge_idx)).target_idx() == cto_idx) {
        if (ci.cedge_to_gway_idx.at(cedge_idx) == tr_edge.way_idx) {
          res.path.push_back(cedge_idx);
          break;
        }
      }
    }
  }
  CHECK_EQ_S(res.path.size(), tr.path.size());
  return res;
}

void ClusterAddComplexTRs(const Graph& g, TmpClusterInfo* ci) {
  for (uint32_t cfrom_idx = 0; cfrom_idx < ci->mm_nodes.size(); ++cfrom_idx) {
    for (uint32_t cedge_idx : cnode_edge_indices(*ci, cfrom_idx)) {
      const MMEdgeBits eb = MM_EDGE(ci->mm_edges.at(cedge_idx));
      if (eb.complex_turn_restriction_trigger()) {
        const uint32_t cto_idx = eb.target_idx();
        const uint32_t gfrom_idx = ci->cnode_to_gnode.at(cfrom_idx);
        const uint32_t gto_idx = ci->cnode_to_gnode.at(cto_idx);
        const uint32_t gway_idx = ci->cedge_to_gway_idx.at(cedge_idx);

        // Find triggering turn restrictions.
        TurnRestriction::TREdge key = {.from_node_idx = gfrom_idx,
                                       .way_idx = gway_idx,
                                       .to_node_idx = gto_idx};
        auto it = g.complex_turn_restriction_map.find(key);
        CHECK_S(it != g.complex_turn_restriction_map.end());
        uint32_t ctr_idx = it->second;
        do {
          const TurnRestriction& tr = g.complex_turn_restrictions.at(ctr_idx);
          if (tr.GetTriggerKey() != key) {
            // Check that we iterated at least once.
            CHECK_GT_S(ctr_idx, it->second);
            break;
          }
          // Consume the matching complex turn restriction.
          // active_ctrs->push_back({.ctr_idx = ctr_idx, .position = 0});

          ci->complex_tr.push_back(ConvertToComplexTr(g, *ci, tr));
          CHECK_EQ_S(cedge_idx, ci->complex_tr.back().path.front());
        } while (++ctr_idx < g.complex_turn_restrictions.size());
      }
    }
  }
  // Sort lexicographically by the content of the vector.
  std::sort(ci->complex_tr.begin(), ci->complex_tr.end(),
            [](const auto& a, const auto& b) { return a.path < b.path; });
}

void ClusterAddInEdges(const Graph& g, TmpClusterInfo* ci) {
  const GCluster& gcl = g.clusters.at(ci->cluster_id);
  for (const GCluster::EdgeDescriptor& gi : gcl.border_in_edges) {
    const GEdge& g_edge = g.edges.at(gi.g_edge_idx);
    const auto iter_from = ci->gnode_to_cnode.find(gi.g_from_idx);
    const auto iter_to = ci->gnode_to_cnode.find(g_edge.target_idx);
    if (iter_from == ci->gnode_to_cnode.end() ||
        iter_to == ci->gnode_to_cnode.end()) {
      ABORT_S() << "Can't find from/to node";
    }
    uint32_t cfrom_idx = iter_from->second;
    uint32_t cto_idx = iter_to->second;
    /*
    LOG_S(INFO) << absl::StrFormat("Incoming Edge from %u to %u", cfrom_idx,
                                   cto_idx);
    */

    bool found = false;
    for (uint32_t c_edge_idx : cnode_edge_indices(*ci, cfrom_idx)) {
      const uint32_t c_target_idx =
          MM_EDGE(ci->mm_edges.at(c_edge_idx)).target_idx();
      const uint32_t g_way_idx = ci->cedge_to_gway_idx.at(c_edge_idx);
      if (c_target_idx == cto_idx && g_edge.way_idx == g_way_idx) {
        found = true;
        ci->mm_in_edges.push_back(
            {.from_cluster_id = g.nodes.at(gi.g_from_idx).cluster_id,
             .from_node_idx = cfrom_idx,
             .to_node_idx = cto_idx,
             .edge_idx = c_edge_idx,
             .in_edge_pos = static_cast<uint32_t>(ci->mm_in_edges.size())});
        break;
      }
    }
    CHECK_S(found);  // Sanity check, by constriction it must be found.
  }
  CHECK_EQ_S(gcl.border_in_edges.size(), ci->mm_in_edges.size());
}

void ClusterAddOutEdges(const Graph& g, TmpClusterInfo* ci) {
  const GCluster& gcl = g.clusters.at(ci->cluster_id);
  for (const GCluster::EdgeDescriptor& gi : gcl.border_out_edges) {
    const GEdge& g_edge = g.edges.at(gi.g_edge_idx);
    const auto iter_from = ci->gnode_to_cnode.find(gi.g_from_idx);
    const auto iter_to = ci->gnode_to_cnode.find(g_edge.target_idx);
    if (iter_from == ci->gnode_to_cnode.end() ||
        iter_to == ci->gnode_to_cnode.end()) {
      ABORT_S() << "Can't find from/to node";
    }
    uint32_t cfrom_idx = iter_from->second;
    uint32_t cto_idx = iter_to->second;
    /*
    LOG_S(INFO) << absl::StrFormat("Outgoing Edge from %u to %u", cfrom_idx,
                                   cto_idx);
    */

    bool found = false;
    for (uint32_t c_edge_idx : cnode_edge_indices(*ci, cfrom_idx)) {
      const uint32_t c_target_idx =
          MM_EDGE(ci->mm_edges.at(c_edge_idx)).target_idx();
      const uint32_t g_way_idx = ci->cedge_to_gway_idx.at(c_edge_idx);
      if (c_target_idx == cto_idx && g_edge.way_idx == g_way_idx) {
        found = true;
        ci->mm_out_edges.push_back(
            {.from_node_idx = cfrom_idx,
             .to_node_idx = cto_idx,
             .edge_idx = c_edge_idx,
             .to_cluster_id = g.nodes.at(g_edge.target_idx).cluster_id,
             .out_edge_pos = static_cast<uint32_t>(ci->mm_out_edges.size())});
        break;
      }
    }
    CHECK_S(found);  // Sanity check, by constriction it must be found.
  }
  CHECK_EQ_S(gcl.border_out_edges.size(), ci->mm_out_edges.size());
}

void ClusterAddNodes(const Graph& g, TmpClusterInfo* ci) {
  CHECK_LT_S(ci->cnode_to_gnode.size(), std::numeric_limits<uint32_t>::max());
  CHECK_S(ci->gnode_to_cnode.empty());

  // Mapping table from cluster node idx to graph node idx.
  for (uint32_t pos = 0; pos < ci->cnode_to_gnode.size(); ++pos) {
    ci->gnode_to_cnode[ci->cnode_to_gnode.at(pos)] = pos;
  }

  ci->mm_nodes.reserve(ci->cnode_to_gnode.size());
  for (uint32_t idx : ci->cnode_to_gnode) {
    const GNode& n = g.nodes.at(idx);
    MMNodeBits nb = {0};
    nb.set_border_node(n.cluster_border_node);
    nb.set_dead_end(n.dead_end);
    nb.set_off_cluster_node(n.cluster_id != ci->cluster_id);
    // Off cluster nodes must be border nodes.
    CHECK_S(!nb.off_cluster_node() || nb.border_node());
    ci->mm_nodes.push_back(nb.data__);
    ci->mm_node_to_osm_id.push_back(n.node_id);
    ci->mm_node_latlon.push_back({.lat = n.lat, .lon = n.lon});
    // LOG_S(INFO) << "lat:" << n.lat << " lon:" << n.lon;
  }
}

void FillTmpClusterInfo(const Graph& g, TmpClusterInfo* ci) {
  ClusterAddNodes(g, ci);
  ClusterAddEdges(g, ci);
  ClusterAddWayData(g, ci);
  ClusterAddTurnCosts(g, ci);
  ClusterAddComplexTRs(g, ci);

  ClusterAddInEdges(g, ci);
  ClusterAddOutEdges(g, ci);
}

void ComputeClusterNodeNumbers(const TmpClusterInfo& ci, MMCluster* mmcluster) {
  mmcluster->num_border_nodes = 0;
  mmcluster->num_off_cluster_nodes = 0;
  mmcluster->num_inner_nodes = 0;
  mmcluster->num_dead_end_nodes = 0;
  for (size_t i = 0; i < ci.mm_nodes.size(); ++i) {
    if (MM_NODE(ci.mm_nodes.at(i)).off_cluster_node()) {
      mmcluster->num_off_cluster_nodes++;
      CHECK_LT_S(mmcluster->num_off_cluster_nodes, INFU16);
    } else if (MM_NODE(ci.mm_nodes.at(i)).border_node()) {
      mmcluster->num_border_nodes++;
      CHECK_LT_S(mmcluster->num_border_nodes, INFU16);
    } else if (MM_NODE(ci.mm_nodes.at(i)).dead_end()) {
      mmcluster->num_dead_end_nodes++;
    } else {
      mmcluster->num_inner_nodes++;
    }
  }
  // Verify.
  for (size_t i = 0; i < ci.mm_nodes.size(); ++i) {
    auto n = MM_NODE(ci.mm_nodes.at(i));
    if (i < mmcluster->start_off_cluster_nodes()) {
      CHECK_S(n.border_node() && !n.off_cluster_node() && !n.dead_end());
    } else if (i < mmcluster->start_inner_nodes()) {
      CHECK_S(n.border_node() && n.off_cluster_node() && !n.dead_end());
    } else if (i < mmcluster->start_dead_end_nodes()) {
      CHECK_S(!n.border_node() && !n.off_cluster_node() && !n.dead_end());
    } else {
      CHECK_LT_S(i, ci.mm_nodes.size());
      CHECK_S(!n.border_node() && !n.off_cluster_node() && n.dead_end());
    }
  }
}

void WriteMMCluster(const TmpClusterInfo& ci, MMCluster* mmcluster, int fd) {
  ComputeClusterNodeNumbers(ci, mmcluster);

  LOG_S(INFO) << absl::StrFormat(
      "Write cl:%u ic:%llu og:%llu n:%llu e:%llu w:%llu wsa:%llu tc:%llu "
      "ctrs:%llu",
      ci.cluster_id, ci.mm_in_edges.size(), ci.mm_out_edges.size(),
      ci.mm_nodes.size(), ci.mm_edges.size(), ci.gway_to_cway.size(),
      ci.mm_way_shared_attrs.size(), ci.turn_costs.size(),
      ci.complex_tr.size());
  LOG_S(INFO) << absl::StrFormat(
      "  nodes: border:%u off_cluster:%u inner:%u dead_end:%u",
      mmcluster->num_border_nodes, mmcluster->num_off_cluster_nodes,
      mmcluster->num_inner_nodes, mmcluster->num_dead_end_nodes);

  mmcluster->in_edges.WriteDataBlob("in_edges", fd, ci.mm_in_edges);
  mmcluster->out_edges.WriteDataBlob("out_edges", fd, ci.mm_out_edges);

  mmcluster->nodes.WriteDataBlob("nodes", fd, ci.mm_nodes);

  mmcluster->edges.WriteDataBlob("edges", fd, ci.mm_edges);

  mmcluster->edge_to_way_idx.WriteDataBlob("edge_to_way", fd,
                                           ci.mm_edge_to_way_idx);

  mmcluster->edge_to_distance.WriteDataBlob("edge_to_distance", fd,
                                            ci.mm_edge_to_distance);

  mmcluster->way_idx_to_wsa_idx.WriteDataBlob("way_to_wsa", fd,
                                              ci.mm_way_to_wsa);

  mmcluster->way_shared_attrs.WriteDataBlob("way_shared_attrs", fd,
                                            ci.mm_way_shared_attrs);

  mmcluster->node_to_latlon.WriteDataBlob("node_to_latlon", fd,
                                          ci.mm_node_latlon);

  mmcluster->grouped_node_to_osm_id.WriteDataBlob("node-id-groups", fd,
                                                  ci.mm_node_to_osm_id);

  mmcluster->grouped_way_to_osm_id.WriteDataBlob("way-id-groups", fd,
                                                 ci.mm_way_to_osm_id);

  {
    const std::vector<uint32_t> idx_to_pos =
        mmcluster->turn_costs_table.WriteDataBlob("turn_costs_table", fd,
                                                  ci.turn_costs);
    // 'idx' has to be replaced by position.
    std::vector<uint32_t> positions;
    positions.reserve(ci.cedge_to_turn_cost_idx.size());
    for (uint32_t idx : ci.cedge_to_turn_cost_idx) {
      positions.push_back(idx_to_pos.at(idx));
    }
    CHECK_EQ_S(positions.size(), ci.cedge_to_turn_cost_idx.size());
    mmcluster->edge_to_turn_costs_pos.WriteDataBlob("edge_to_turn_cost_pos", fd,
                                                    positions);
  }

  {
    const std::vector<uint32_t> idx_to_pos =
        mmcluster->streetnames_table.WriteDataBlob("streetnames_table", fd,
                                                   ci.streetnames);
    // 'idx' has to be replaced by positions.
    std::vector<uint32_t> positions;
    positions.reserve(ci.cway_to_streetname_idx.size());
    for (uint32_t idx : ci.cway_to_streetname_idx) {
      positions.push_back(idx_to_pos.at(idx));
    }
    CHECK_EQ_S(positions.size(), ci.cway_to_streetname_idx.size());
    mmcluster->way_to_streetname_pos.WriteDataBlob("way_to_streetname_pos", fd,
                                                   positions);
  }

  {
    std::vector<MMComplexTurnRestriction> ctrs;
    std::vector<uint32_t> legs;
    for (const TmpComplexTR& x : ci.complex_tr) {
      CHECK_LT_S(x.path.size(), std::numeric_limits<uint16_t>::max());
      ctrs.push_back({.forbidden = x.forbidden,
                      .len = static_cast<uint16_t>(x.path.size()),
                      .first_leg_pos = static_cast<uint32_t>(legs.size()),
                      .trigger_edge_idx = x.path.front()});
      legs.insert(legs.end(), x.path.begin(), x.path.end());
    }
    mmcluster->complex_turn_restrictions.WriteDataBlob("complex_trs", fd, ctrs);
    mmcluster->complex_turn_restriction_legs.WriteDataBlob("complex_trs_legs",
                                                           fd, legs);
  }
}

}  // namespace

// Convert the monolithic graph to a list of clusters and store them in a
// memory mapped file.
void WriteGraphToMMFile(const Graph& g, const std::string& path) {
  FUNC_TIMER();
  int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) FileAbortOnError("open");

  // Write zeroed data for headedr, bounding rects and clusters.
  MMFileHeader mmheader = {0};
  const uint32_t num_clusters = g.clusters.size();
  std::vector<MMBoundingRect> cluster_bounding_rects(num_clusters, {0});
  std::vector<MMCluster> clusters(num_clusters, {0});
  AppendData("file-headers", fd, (const uint8_t*)&mmheader, sizeof(mmheader));
  mmheader.cluster_bounding_rects.WriteDataBlob("bounding rects", fd,
                                                cluster_bounding_rects);
  mmheader.clusters.WriteDataBlob("clusters", fd, clusters);

  // Start fillimng data.
  mmheader.magic = kMagic;
  mmheader.version_major = kVersionMajor;
  mmheader.version_minor = kVersionMinor;
  mmheader.file_size = 0;

  std::vector<TmpClusterInfo> tmp_cluster_infos;
  CollectClusterNodes(g, &tmp_cluster_infos);
  CHECK_EQ_S(g.clusters.size(), tmp_cluster_infos.size());

  LogMemoryUsage();

  for (TmpClusterInfo& tci : tmp_cluster_infos) {
    FillTmpClusterInfo(g, &tci);
    WriteMMCluster(tci, &clusters.at(tci.cluster_id), fd);
    tci = {};  // Clear all data, release memory.
  }

  LogMemoryUsage();

  // Rewrite the header data and the two vectors belonging to the header.
  mmheader.file_size = GetFileSize(fd);
  WriteDataTo(fd, 0, (const uint8_t*)&mmheader, sizeof(mmheader));
  WriteDataTo(fd, mmheader.cluster_bounding_rects.offset(),
              (const uint8_t*)cluster_bounding_rects.data(),
              VectorDataSizeInBytes(cluster_bounding_rects));
  WriteDataTo(fd, mmheader.clusters.offset(), (const uint8_t*)clusters.data(),
              VectorDataSizeInBytes(clusters));
  CHECK_EQ_S(mmheader.file_size, GetFileSize(fd));

  if (::fsync(fd) != 0) FileAbortOnError("fsync");
  if (::close(fd) != 0) FileAbortOnError("close");
}
