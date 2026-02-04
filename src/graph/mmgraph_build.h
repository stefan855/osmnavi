#pragma once

#include <sys/mman.h>

#include <ranges>
#include <span>
#include <vector>

#include "base/deduper_with_ids.h"
#include "base/mmap_base.h"
#include "geometry/geometry.h"
#include "graph/graph_def.h"
#include "graph/mmgraph_def.h"

namespace {
struct TmpComplexTR {
  bool forbidden;
  uint32_t first_node_idx;
  std::vector<uint32_t> path;
};

// Temporary data for a cluster, before it is written to the mmap file.
struct TmpClusterInfo {
  uint32_t cluster_id;

  // **************************************************************************
  // Auxiliary data.
  // **************************************************************************
  std::vector<uint32_t> cnode_to_gnode;
  absl::flat_hash_map<uint32_t, uint32_t> gnode_to_cnode;

  absl::flat_hash_map<uint32_t, uint32_t> gway_to_cway;
  std::vector<uint32_t> cway_to_streetname_idx;
  std::vector<std::string> streetnames;

  // Offset of edges in graph, indexed by cluster edge index.
  std::vector<uint16_t> cedge_to_gedge_offset;
  // Way index of edges in graph, indexed by cluster edge index.
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
  std::vector<MMLatLon> mm_node_to_latlon;

  // Dim #edges
  std::vector<uint64_t> mm_edges;  // Type MMEdge.
  std::vector<uint32_t> mm_edge_to_distance;
  std::vector<uint32_t> mm_edge_to_way;

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
  uint32_t edge_start_idx = 0;
  ci->cedge_to_gedge_offset.reserve(expected_num_edges);
  ci->cedge_to_gway_idx.reserve(expected_num_edges);
  ci->mm_edge_to_distance.reserve(expected_num_edges);
  for (uint32_t c_pos = 0; c_pos < ci->cnode_to_gnode.size(); ++c_pos) {
    uint32_t gn_idx = ci->cnode_to_gnode.at(c_pos);
    const GNode& n = g.nodes.at(gn_idx);
    // CHECK_EQ_S(n.cluster_id, ci->cluster_id);
    MM_NODE_RW(ci->mm_nodes.at(c_pos)).set_edge_start_idx(edge_start_idx);
    CHECK_EQ_S(MM_NODE(ci->mm_nodes.at(c_pos)).edge_start_idx(),
               edge_start_idx);
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
        MMEdge eb{0};
        eb.set_target_idx(ctarget_idx);
        eb.set_dead_end(e.dead_end);
        eb.set_bridge(e.bridge);
        eb.set_restricted(e.car_label != GEdge::LABEL_FREE);
        eb.set_contra_way(e.contra_way);
        eb.set_cluster_border_edge(e.cluster_border_edge);
        eb.set_complex_turn_restriction_trigger(
            e.complex_turn_restriction_trigger);
        ci->mm_edges.push_back(eb.data__);

        edge_start_idx++;
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
  // CHECK_LE_S(edge_start_idx, 1u << MMEdgeStartPosBits);
  // CHECK_EQ_S(rc->edges.size(), rc->num_inner_edges + rc->num_deadend_edges);
  CHECK_EQ_S(ci->mm_edges.size(), ci->cedge_to_gedge_offset.size());
  CHECK_EQ_S(ci->mm_edges.size(), ci->cedge_to_gway_idx.size());
}

void ClusterAddWayData(const Graph& g, TmpClusterInfo* ci) {
  // Determine the unique ways (given as gway_idx).
  DeDuperWithIds<uint32_t> dd_gway_idx;
  ci->mm_edge_to_way.reserve(ci->cedge_to_gway_idx.size());
  for (uint32_t e_idx = 0; e_idx < ci->cedge_to_gway_idx.size(); ++e_idx) {
    ci->mm_edge_to_way.push_back(
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
  uint32_t start = MM_NODE(ci.mm_nodes.at(node_idx)).edge_start_idx();
  uint32_t stop = node_idx + 1 < ci.mm_nodes.size()
                      ? MM_NODE(ci.mm_nodes.at(node_idx + 1)).edge_start_idx()
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
  CHECK_S(!tr.path.empty());
  TmpComplexTR res;
  res.forbidden = tr.forbidden;
  res.first_node_idx = MAXU32;  // invalid.
  for (const TurnRestriction::TREdge& tr_edge : tr.path) {
    const auto iter_cfrom = ci.gnode_to_cnode.find(tr_edge.from_node_idx);
    const auto iter_cto = ci.gnode_to_cnode.find(tr_edge.to_node_idx);
    if (iter_cfrom == ci.gnode_to_cnode.end() ||
        iter_cto == ci.gnode_to_cnode.end()) {
      ABORT_S() << "Can't find from/to node";
    }
    uint32_t cfrom_idx = iter_cfrom->second;
    uint32_t cto_idx = iter_cto->second;

    // Locate edge at node.
    for (uint32_t cedge_idx : cnode_edge_indices(ci, cfrom_idx)) {
      if (MM_EDGE(ci.mm_edges.at(cedge_idx)).target_idx() == cto_idx) {
        if (ci.cedge_to_gway_idx.at(cedge_idx) == tr_edge.way_idx) {
          if (res.path.empty()) {
            res.first_node_idx = cfrom_idx;
          }
          res.path.push_back(cedge_idx);
          break;
        }
      }
    }
  }
  CHECK_EQ_S(res.path.size(), tr.path.size());
  CHECK_NE_S(res.first_node_idx, MAXU32);
  return res;
}

void ClusterAddComplexTRs(const Graph& g, TmpClusterInfo* ci) {
  for (uint32_t cfrom_idx = 0; cfrom_idx < ci->mm_nodes.size(); ++cfrom_idx) {
    for (uint32_t cedge_idx : cnode_edge_indices(*ci, cfrom_idx)) {
      const MMEdge eb = MM_EDGE(ci->mm_edges.at(cedge_idx));
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
    MMNode nb = {0};
    nb.set_border_node(n.cluster_border_node);
    nb.set_dead_end(n.dead_end);
    nb.set_off_cluster_node(n.cluster_id != ci->cluster_id);
    // Off cluster nodes must be border nodes.
    CHECK_S(!nb.off_cluster_node() || nb.border_node());
    ci->mm_nodes.push_back(nb.data__);
    ci->mm_node_to_osm_id.push_back(n.node_id);
    ci->mm_node_to_latlon.push_back({.lat = n.lat, .lon = n.lon});
    // LOG_S(INFO) << "lat:" << n.lat << " lon:" << n.lon;
  }

  ci->mm_bounding_rect = {0};
  if (!ci->mm_node_to_latlon.empty()) {
    ci->mm_bounding_rect.min = ci->mm_node_to_latlon.front();
    ci->mm_bounding_rect.max = ci->mm_node_to_latlon.front();
    for (const auto& x : ci->mm_node_to_latlon) {
      ci->mm_bounding_rect.min.lat =
          std::min(ci->mm_bounding_rect.min.lat, x.lat);
      ci->mm_bounding_rect.min.lon =
          std::min(ci->mm_bounding_rect.min.lon, x.lon);
      ci->mm_bounding_rect.max.lat =
          std::max(ci->mm_bounding_rect.max.lat, x.lat);
      ci->mm_bounding_rect.max.lon =
          std::max(ci->mm_bounding_rect.max.lon, x.lon);
    }
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

template <typename T1, typename T2>
void compare_check_vectors(const std::string& name, const T1& v1,
                           const T2& v2) {
  LOG_S(INFO) << "  Check " << name << ":" << v1.size();
  CHECK_EQ_S(v1.size(), v2.size());
  CHECK_EQ_S(v1.size(), v2.size());
  for (uint32_t i = 0; i < v1.size(); ++i) {
    CHECK_EQ_S(v1.at(i), v2.at(i));
  }
}

// check that a cnode is plausible:
//   - exists in g.
//   - is in expected cluster or a border node.
void CheckGNodePlausible(const Graph& g, const TmpClusterInfo& tci,
                         const MMCluster& mmc, uint32_t node_idx) {
  CHECK_LT_S(node_idx, mmc.nodes.size());
  int64_t osm_id = mmc.get_node_id(node_idx);
  size_t gnode_idx = g.FindNodeIndex(osm_id);
  CHECK_LT_S(gnode_idx, g.nodes.size()) << osm_id;
  const GNode& n = g.nodes.at(gnode_idx);
  CHECK_S(n.cluster_id == mmc.cluster_id || n.cluster_border_node)
      << mmc.cluster_id;

  MMLatLon latlon = mmc.node_to_latlon.at(node_idx);
  CHECK_EQ_S(latlon.lat, n.lat);
  CHECK_EQ_S(latlon.lon, n.lon);
}

void CheckGEdge(const Graph& g, const TmpClusterInfo& tci, const MMCluster& mmc,
                uint32_t cfrom_idx, uint32_t cedge_idx) {
  // LOG_S(INFO) << "AA " << cfrom_idx << ":" << cedge_idx;
  const MMEdge e(mmc.edges.at(cedge_idx));
  uint32_t cto_idx = e.target_idx();
  uint32_t cway_idx = mmc.edge_to_way.at(cedge_idx);

  int64_t from_id = tci.mm_node_to_osm_id.at(cfrom_idx);
  int64_t to_id = tci.mm_node_to_osm_id.at(cto_idx);
  int64_t way_id = tci.mm_way_to_osm_id.at(cway_idx);

  uint32_t gfrom_idx = tci.cnode_to_gnode.at(cfrom_idx);
  uint32_t gto_idx = tci.cnode_to_gnode.at(cto_idx);
  uint32_t gway_idx = g.FindWayIndex(way_id);
  CHECK_EQ_S(from_id, g.nodes.at(gfrom_idx).node_id);
  CHECK_EQ_S(to_id, g.nodes.at(gto_idx).node_id);
  CHECK_EQ_S(gway_idx, tci.cedge_to_gway_idx.at(cedge_idx));

  FullEdge fe = gnode_find_full_edge(g, gfrom_idx, gto_idx, gway_idx);
  auto cwsa = mmc.get_wsa(cway_idx);
  auto gwsa = GetWSA(g, gway_idx);
  CHECK_S(cwsa == gwsa);

  std::string_view cstreet_name = mmc.get_streetname(cway_idx);
  std::string gstreet_name =
      g.streetnames.at(g.ways.at(gway_idx).streetname_idx);
  CHECK_EQ_S(cstreet_name, gstreet_name);

  auto dist = mmc.edge_to_distance.at(cedge_idx);
  CHECK_EQ_S(dist, fe.gedge(g).distance_cm);
}

FullEdge find_full_gedge(const Graph& g, const TmpClusterInfo& tci,
                         const MMCluster& mmc, uint32_t cfrom_idx,
                         uint32_t cedge_idx) {
  const MMEdge e(mmc.edges.at(cedge_idx));
  uint32_t cto_idx = e.target_idx();
  uint32_t cway_idx = mmc.edge_to_way.at(cedge_idx);

  uint32_t gfrom_idx = tci.cnode_to_gnode.at(cfrom_idx);
  uint32_t gto_idx = tci.cnode_to_gnode.at(cto_idx);
  int64_t way_id = tci.mm_way_to_osm_id.at(cway_idx);
  uint32_t gway_idx = g.FindWayIndex(way_id);

  return gnode_find_full_edge(g, gfrom_idx, gto_idx, gway_idx);
}

void CheckTurnCosts(const Graph& g, const TmpClusterInfo& tci,
                    const MMCluster& mmc, uint32_t cfrom_idx,
                    uint32_t cedge_idx) {
  FullEdge fe1 = find_full_gedge(g, tci, mmc, cfrom_idx, cedge_idx);
  // Now iterate the edges at the target node and check the turn costs between
  // the two edges.
  uint32_t ctarget_idx = mmc.get_edge(cedge_idx).target_idx();
  std::span<const uint8_t> tcarr = mmc.get_turn_costs(cedge_idx);
  CHECK_EQ_S(tcarr.size(), mmc.get_num_edges(ctarget_idx));

  for (uint32_t edge2_idx : mmc.edge_indices(ctarget_idx)) {
    FullEdge fe2 = find_full_gedge(g, tci, mmc, ctarget_idx, edge2_idx);
    N3Path n3p = N3Path::Create(g, fe1, fe2);
    auto gturn_cost = n3p.get_compressed_turn_cost_0to1(g);

    CHECK_GE_S(edge2_idx, mmc.edge_start_idx(ctarget_idx));
    uint32_t off = edge2_idx - mmc.edge_start_idx(ctarget_idx);
    auto cturn_cost = tcarr[off];
    CHECK_EQ_S(gturn_cost, cturn_cost);
  }
}

void VerifyComplexTurnRestrictions(const Graph& g, const TmpClusterInfo& tci,
                                   const MMCluster& mmc) {
  LOG_S(INFO) << "  VerifyComplexTurnRestrictions:"
              << mmc.complex_turn_restrictions.size();
  // Count the triggered ctrs. Note that one edge can trigger multiple ctrs.
  uint32_t num_triggers = 0;
  for (uint32_t edge_idx = 0; edge_idx < mmc.edges.size(); ++edge_idx) {
    if (mmc.get_edge(edge_idx).complex_turn_restriction_trigger()) {
      uint32_t ctr_start_idx = mmc.find_complex_turn_restriction_idx(edge_idx);
      for (uint32_t i = ctr_start_idx; i < mmc.complex_turn_restrictions.size();
           ++i) {
        const MMComplexTurnRestriction& tr =
            mmc.complex_turn_restrictions.at(i);
        if (tr.trigger_edge_idx != edge_idx) {
          CHECK_GT_S(i, ctr_start_idx);  // At least one tr has been consumed.
          break;
        }
        std::span<const uint32_t> legs =
            mmc.get_complex_turn_restriction_legs(tr);
        CHECK_EQ_S(legs.size(), tr.num_legs);
        CHECK_EQ_S(tr.trigger_edge_idx, edge_idx);
        CHECK_EQ_S(tr.trigger_edge_idx, legs.front());
        CHECK_GE_S(edge_idx, mmc.edge_start_idx(tr.first_node_idx));
        CHECK_LT_S(edge_idx, mmc.edge_stop_idx(tr.first_node_idx));
        num_triggers++;

        // Check that each leg start at the target_idx of the previous leg.
        uint32_t head_node = tr.first_node_idx;
        for (auto leg_idx : legs) {
          CHECK_GE_S(leg_idx, mmc.edge_start_idx(head_node));
          CHECK_LT_S(leg_idx, mmc.edge_stop_idx(head_node));
          head_node = mmc.get_edge(leg_idx).target_idx();
        }
      }
    }
  }
  CHECK_EQ_S(num_triggers, mmc.complex_turn_restrictions.size());
}

void CheckMMGraph(const std::string& path, const Graph& g,
                  const std::vector<TmpClusterInfo>& tmp_cluster_infos) {
  FUNC_TIMER();
  int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC, 0644);
  if (fd < 0) FileAbortOnError("open");

  const uint64_t file_size = GetFileSize(fd);
  void* ptr = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
  if (ptr == MAP_FAILED) {
    perror("mmap");
    ::close(fd);
    ABORT_S();
  }
  ::close(fd);

  const MMGraph& mmheader = *((MMGraph*)ptr);
  CHECK_EQ_S(mmheader.magic, kMagic);
  CHECK_EQ_S(mmheader.version_major, kVersionMajor);
  CHECK_EQ_S(mmheader.version_minor, kVersionMinor);
  CHECK_EQ_S(mmheader.file_size, file_size);

  LOG_S(INFO) << "Check " << mmheader.sorted_bounding_rects.size()
              << " sorted bounding rects";
  CHECK_EQ_S(mmheader.sorted_bounding_rects.size(), g.clusters.size());
  for (uint32_t idx = 0; idx < mmheader.sorted_bounding_rects.size(); ++idx) {
    const MMClusterBoundingRect& cbr = mmheader.sorted_bounding_rects.at(idx);
    const MMCluster& mmc = mmheader.clusters.at(cbr.cluster_id);
    CHECK_EQ_S(cbr.bounding_rect.min.lat, mmc.bounding_rect.min.lat);
    CHECK_EQ_S(cbr.bounding_rect.min.lon, mmc.bounding_rect.min.lon);
    CHECK_EQ_S(cbr.bounding_rect.max.lat, mmc.bounding_rect.max.lat);
    CHECK_EQ_S(cbr.bounding_rect.max.lon, mmc.bounding_rect.max.lon);
    if (idx > 0) {
      // Check that it is sorted.
      CHECK_GE_S(
          cbr.bounding_rect.min.lon,
          mmheader.sorted_bounding_rects.at(idx - 1).bounding_rect.min.lon);
    }
  }

  LOG_S(INFO) << "Check " << mmheader.clusters.size() << " clusters";
  CHECK_EQ_S(mmheader.clusters.size(), g.clusters.size());
  for (uint32_t cluster_id = 0; cluster_id < mmheader.clusters.size();
       ++cluster_id) {
    LOG_S(INFO) << "Check cluster " << cluster_id;
    const TmpClusterInfo& tci = tmp_cluster_infos.at(cluster_id);
    const MMCluster& mmc = mmheader.clusters.at(tci.cluster_id);
    CHECK_EQ_S(tci.cluster_id, cluster_id);
    CHECK_EQ_S(mmc.cluster_id, cluster_id);
    LOG_S(INFO) << " num_border_nodes:" << mmc.num_border_nodes;
    LOG_S(INFO) << " num_off_cluster_nodes:" << mmc.num_off_cluster_nodes;
    LOG_S(INFO) << " num_inner_nodes:" << mmc.num_inner_nodes;
    LOG_S(INFO) << " num_dead_end_nodes:" << mmc.num_dead_end_nodes;

    // Check OSM ids first, because other checks use them.
    compare_check_vectors("grouped_node_to_osm_id", mmc.grouped_node_to_osm_id,
                          tci.mm_node_to_osm_id);
    CHECK_EQ_S(mmc.grouped_node_to_osm_id.size(), tci.mm_nodes.size());

    // way osm ids
    compare_check_vectors("grouped_way_to_osm_id", mmc.grouped_way_to_osm_id,
                          tci.mm_way_to_osm_id);
    CHECK_EQ_S(mmc.grouped_way_to_osm_id.size(), tci.mm_way_to_wsa.size());
    CHECK_EQ_S(mmc.grouped_way_to_osm_id.size(), tci.gway_to_cway.size());

    // in_edges
    LOG_S(INFO) << "  Check in_edges:" << mmc.in_edges.size();
    CHECK_EQ_S(mmc.in_edges.size(), tci.mm_in_edges.size());
    for (uint32_t i = 0; i < mmc.in_edges.size(); ++i) {
      CHECK_EQ_S(mmc.in_edges.at(i).from_cluster_id,
                 tci.mm_in_edges.at(i).from_cluster_id);
      CHECK_EQ_S(mmc.in_edges.at(i).from_node_idx,
                 tci.mm_in_edges.at(i).from_node_idx);
      CHECK_EQ_S(mmc.in_edges.at(i).to_node_idx,
                 tci.mm_in_edges.at(i).to_node_idx);
      CHECK_EQ_S(mmc.in_edges.at(i).edge_idx, tci.mm_in_edges.at(i).edge_idx);
      CHECK_EQ_S(mmc.in_edges.at(i).in_edge_pos,
                 tci.mm_in_edges.at(i).in_edge_pos);
    }

    // out_edges
    LOG_S(INFO) << "  Check out_edges:" << mmc.out_edges.size();
    CHECK_EQ_S(mmc.out_edges.size(), tci.mm_out_edges.size());
    for (uint32_t i = 0; i < mmc.out_edges.size(); ++i) {
      CHECK_EQ_S(mmc.out_edges.at(i).from_node_idx,
                 tci.mm_out_edges.at(i).from_node_idx);
      CHECK_EQ_S(mmc.out_edges.at(i).to_node_idx,
                 tci.mm_out_edges.at(i).to_node_idx);
      CHECK_EQ_S(mmc.out_edges.at(i).edge_idx, tci.mm_out_edges.at(i).edge_idx);
      CHECK_EQ_S(mmc.out_edges.at(i).to_cluster_id,
                 tci.mm_out_edges.at(i).to_cluster_id);
      CHECK_EQ_S(mmc.out_edges.at(i).out_edge_pos,
                 tci.mm_out_edges.at(i).out_edge_pos);
    }

    // nodes
    LOG_S(INFO) << "  Check nodes:" << mmc.nodes.size();
    CHECK_EQ_S(mmc.nodes.size(), tci.mm_nodes.size());
    for (uint32_t i = 0; i < mmc.nodes.size(); ++i) {
      CHECK_EQ_S(MM_NODE(mmc.nodes.at(i)).border_node(),
                 MM_NODE(tci.mm_nodes.at(i)).border_node());
      CHECK_EQ_S(MM_NODE(mmc.nodes.at(i)).dead_end(),
                 MM_NODE(tci.mm_nodes.at(i)).dead_end());
      CHECK_EQ_S(MM_NODE(mmc.nodes.at(i)).off_cluster_node(),
                 MM_NODE(tci.mm_nodes.at(i)).off_cluster_node());
      CHECK_EQ_S(MM_NODE(mmc.nodes.at(i)).edge_start_idx(),
                 MM_NODE(tci.mm_nodes.at(i)).edge_start_idx());
      CheckGNodePlausible(g, tci, mmc, i);
    }

    // edges
    LOG_S(INFO) << "  Check edges:" << mmc.edges.size();
    CHECK_EQ_S(mmc.edges.size(), tci.mm_edges.size());
    for (uint32_t i = 0; i < mmc.edges.size(); ++i) {
      CHECK_EQ_S(MM_EDGE(mmc.edges.at(i)).dead_end(),
                 MM_EDGE(tci.mm_edges.at(i)).dead_end());
      CHECK_EQ_S(MM_EDGE(mmc.edges.at(i)).bridge(),
                 MM_EDGE(tci.mm_edges.at(i)).bridge());
      CHECK_EQ_S(MM_EDGE(mmc.edges.at(i)).restricted(),
                 MM_EDGE(tci.mm_edges.at(i)).restricted());
      CHECK_EQ_S(MM_EDGE(mmc.edges.at(i)).contra_way(),
                 MM_EDGE(tci.mm_edges.at(i)).contra_way());
      CHECK_EQ_S(MM_EDGE(mmc.edges.at(i)).cluster_border_edge(),
                 MM_EDGE(tci.mm_edges.at(i)).cluster_border_edge());
      CHECK_EQ_S(
          MM_EDGE(mmc.edges.at(i)).complex_turn_restriction_trigger(),
          MM_EDGE(tci.mm_edges.at(i)).complex_turn_restriction_trigger());
      CHECK_EQ_S(MM_EDGE(mmc.edges.at(i)).target_idx(),
                 MM_EDGE(tci.mm_edges.at(i)).target_idx());
      CheckGNodePlausible(g, tci, mmc, MM_EDGE(mmc.edges.at(i)).target_idx());
    }

    // full edges
    LOG_S(INFO) << "  Check full edges:" << mmc.edges.size();
    for (uint32_t from_idx = 0; from_idx < mmc.nodes.size(); ++from_idx) {
      for (uint32_t edge_idx = mmc.edge_start_idx(from_idx);
           edge_idx < mmc.edge_stop_idx(from_idx); ++edge_idx) {
        CheckGEdge(g, tci, mmc, from_idx, edge_idx);
        CheckTurnCosts(g, tci, mmc, from_idx, edge_idx);
      }
    }

    compare_check_vectors("edge_to_distance", mmc.edge_to_distance,
                          tci.mm_edge_to_distance);

    // edge_to_way
    compare_check_vectors("edge_to_way", mmc.edge_to_way, tci.mm_edge_to_way);
    CHECK_EQ_S(mmc.edge_to_way.size(), tci.mm_edges.size());

    // way_to_wsa
    compare_check_vectors("way_to_wsa", mmc.way_to_wsa, tci.mm_way_to_wsa);
    CHECK_EQ_S(mmc.way_to_wsa.size(), tci.gway_to_cway.size());

    // way shared attrs
    LOG_S(INFO) << "  Check way_shared_attrs:" << mmc.way_shared_attrs.size();
    CHECK_EQ_S(mmc.way_shared_attrs.size(), tci.mm_way_shared_attrs.size());
    for (uint32_t i = 0; i < mmc.way_shared_attrs.size(); ++i) {
      CHECK_S(mmc.way_shared_attrs.at(i) == tci.mm_way_shared_attrs.at(i));
    }

    // This is a little bit complicated. Turn costs are handled almost
    // identical to streetnames, please check comment below.
    LOG_S(INFO) << "  Check turn costs of edges:"
                << mmc.edge_to_turn_costs_pos.size();
    CHECK_EQ_S(mmc.edge_to_turn_costs_pos.size(),
               tci.cedge_to_turn_cost_idx.size());
    for (uint32_t i = 0; i < mmc.edge_to_turn_costs_pos.size(); ++i) {
      std::span<const uint8_t> tcd1 =
          mmc.turn_costs_table.at(mmc.edge_to_turn_costs_pos.at(i));
      TurnCostData tcd2 = tci.turn_costs.at(tci.cedge_to_turn_cost_idx.at(i));
      CHECK_EQ_S(tcd1.size(), tcd2.turn_costs.size());
      for (uint32_t i = 0; i < tcd1.size(); ++i) {
        CHECK_EQ_S(tcd1[i], tcd2.turn_costs.at(i));
      }
    }

    // TODO: complex_turn_restrictions
    // TODO: complex_turn_restriction_legs
    // Check that all complex turn restrictions can be back-translated into
    // the g graph and that all the referenced nodes and edges exist in both
    // graphs.

    // node_to_latlon
    LOG_S(INFO) << "  Check node_to_latlon:" << mmc.node_to_latlon.size();
    CHECK_EQ_S(mmc.node_to_latlon.size(), tci.mm_node_to_latlon.size());
    for (uint32_t i = 0; i < mmc.node_to_latlon.size(); ++i) {
      const auto latlon1 = mmc.node_to_latlon.at(i);
      const auto latlon2 = tci.mm_node_to_latlon.at(i);
      CHECK_EQ_S(latlon1.lat, latlon2.lat);
      CHECK_EQ_S(latlon1.lon, latlon2.lon);
    }

    // This is a little bit complicated, because the incoming data has a
    // vector of indexes into a vector of strings, and the memory mapped data
    // has a vector of positions pointing into a table of chars. The strings in
    // the second table are 0-terminated.
    LOG_S(INFO) << "  Check streetnames of ways:"
                << mmc.way_to_streetname_pos.size();
    CHECK_EQ_S(mmc.way_to_streetname_pos.size(),
               tci.cway_to_streetname_idx.size());
    for (uint32_t i = 0; i < mmc.way_to_streetname_pos.size(); ++i) {
      const std::string s1(
          mmc.streetnames_table.at(mmc.way_to_streetname_pos.at(i)));
      const std::string s2 =
          tci.streetnames.at(tci.cway_to_streetname_idx.at(i));
      CHECK_EQ_S(s1, s2);
    }

    VerifyComplexTurnRestrictions(g, tci, mmc);
  }

  munmap((void*)ptr, file_size);
}

}  // namespace

void WriteMMCluster(const TmpClusterInfo& tci, MMCluster* mmcluster,
                    int64_t global_object_offset, int fd) {
  mmcluster->cluster_id = tci.cluster_id;
  ComputeClusterNodeNumbers(tci, mmcluster);
  mmcluster->bounding_rect = tci.mm_bounding_rect;

  LOG_S(INFO) << absl::StrFormat(
      "Write cl:%u ic:%llu og:%llu n:%llu e:%llu w:%llu wsa:%llu tc:%llu "
      "ctrs:%llu",
      tci.cluster_id, tci.mm_in_edges.size(), tci.mm_out_edges.size(),
      tci.mm_nodes.size(), tci.mm_edges.size(), tci.gway_to_cway.size(),
      tci.mm_way_shared_attrs.size(), tci.turn_costs.size(),
      tci.complex_tr.size());
  LOG_S(INFO) << absl::StrFormat(
      "  nodes: border:%u off_cluster:%u inner:%u dead_end:%u",
      mmcluster->num_border_nodes, mmcluster->num_off_cluster_nodes,
      mmcluster->num_inner_nodes, mmcluster->num_dead_end_nodes);

  mmcluster->in_edges.WriteDataBlob(
      "in_edges", global_object_offset + offsetof(MMCluster, in_edges), fd,
      tci.mm_in_edges);

  mmcluster->out_edges.WriteDataBlob(
      "out_edges", global_object_offset + offsetof(MMCluster, out_edges), fd,
      tci.mm_out_edges);

  mmcluster->nodes.WriteDataBlob(
      "nodes", global_object_offset + offsetof(MMCluster, nodes), fd,
      tci.mm_nodes);

  mmcluster->edges.WriteDataBlob(
      "edges", global_object_offset + offsetof(MMCluster, edges), fd,
      tci.mm_edges);

  mmcluster->edge_to_way.WriteDataBlob(
      "edge_to_way", global_object_offset + offsetof(MMCluster, edge_to_way),
      fd, tci.mm_edge_to_way);

  mmcluster->edge_to_distance.WriteDataBlob(
      "edge_to_distance",
      global_object_offset + offsetof(MMCluster, edge_to_distance), fd,
      tci.mm_edge_to_distance);

  mmcluster->way_to_wsa.WriteDataBlob(
      "way_to_wsa", global_object_offset + offsetof(MMCluster, way_to_wsa), fd,
      tci.mm_way_to_wsa);

  mmcluster->way_shared_attrs.WriteDataBlob(
      "way_shared_attrs",
      global_object_offset + offsetof(MMCluster, way_shared_attrs), fd,
      tci.mm_way_shared_attrs);

  mmcluster->node_to_latlon.WriteDataBlob(
      "node_to_latlon",
      global_object_offset + offsetof(MMCluster, node_to_latlon), fd,
      tci.mm_node_to_latlon);

  mmcluster->grouped_node_to_osm_id.WriteDataBlob(
      "node-id-groups",
      global_object_offset + offsetof(MMCluster, grouped_node_to_osm_id), fd,
      tci.mm_node_to_osm_id);

  mmcluster->grouped_way_to_osm_id.WriteDataBlob(
      "way-id-groups",
      global_object_offset + offsetof(MMCluster, grouped_way_to_osm_id), fd,
      tci.mm_way_to_osm_id);

  {
    const std::vector<uint32_t> idx_to_pos =
        mmcluster->turn_costs_table.WriteDataBlob(
            "turn_costs_table",
            global_object_offset + offsetof(MMCluster, turn_costs_table), fd,
            tci.turn_costs);
    // 'idx' has to be replaced by position.
    std::vector<uint32_t> positions;
    positions.reserve(tci.cedge_to_turn_cost_idx.size());
    for (uint32_t idx : tci.cedge_to_turn_cost_idx) {
      positions.push_back(idx_to_pos.at(idx));
    }
    CHECK_EQ_S(positions.size(), tci.cedge_to_turn_cost_idx.size());
    mmcluster->edge_to_turn_costs_pos.WriteDataBlob(
        "edge_to_turn_cost_pos",
        global_object_offset + offsetof(MMCluster, edge_to_turn_costs_pos), fd,
        positions);
  }

  {
    const std::vector<uint32_t> idx_to_pos =
        mmcluster->streetnames_table.WriteDataBlob(
            "streetnames_table",
            global_object_offset + offsetof(MMCluster, streetnames_table), fd,
            tci.streetnames);
    // 'idx' has to be replaced by positions.
    std::vector<uint32_t> positions;
    positions.reserve(tci.cway_to_streetname_idx.size());
    for (uint32_t idx : tci.cway_to_streetname_idx) {
      positions.push_back(idx_to_pos.at(idx));
    }
    CHECK_EQ_S(positions.size(), tci.cway_to_streetname_idx.size());
    mmcluster->way_to_streetname_pos.WriteDataBlob(
        "way_to_streetname_pos",
        global_object_offset + offsetof(MMCluster, way_to_streetname_pos), fd,
        positions);
  }

  {
    std::vector<MMComplexTurnRestriction> ctrs;
    std::vector<uint32_t> legs;
    for (const TmpComplexTR& x : tci.complex_tr) {
      CHECK_S(!x.path.empty());
      CHECK_LT_S(x.path.size(), std::numeric_limits<uint16_t>::max());
      ctrs.push_back({
          .trigger_edge_idx = x.path.front(),
          .first_node_idx = x.first_node_idx,
          .first_leg_pos = static_cast<uint32_t>(legs.size()),
          .num_legs = static_cast<uint16_t>(x.path.size()),
          .forbidden = x.forbidden,
      });
      const auto& ctr = ctrs.back();
      LOG_S(INFO) << absl::StrFormat(
          "CC0 Add complex turn restriction trigger:%u node:%u(id:%llu) "
          "first_leg:%u num_legs:%u forbidden:%u",
          ctr.trigger_edge_idx, ctr.first_node_idx,
          tci.mm_node_to_osm_id.at(ctr.first_node_idx), ctr.first_leg_pos,
          ctr.num_legs, ctr.forbidden);
      legs.insert(legs.end(), x.path.begin(), x.path.end());
      LOG_S(INFO) << absl::StrFormat("CC1 Add %llu legs new size %llu",
                                     x.path.size(), legs.size());
    }
    mmcluster->complex_turn_restrictions.WriteDataBlob(
        "complex_trs",
        global_object_offset + offsetof(MMCluster, complex_turn_restrictions),
        fd, ctrs);
    mmcluster->complex_turn_restriction_legs.WriteDataBlob(
        "complex_trs_legs",
        global_object_offset +
            offsetof(MMCluster, complex_turn_restriction_legs),
        fd, legs);
  }
}

// Convert the monolithic graph to a list of clusters and store them in a
// memory mapped file.
void WriteGraphToMMFile(const Graph& g, const std::string& path,
                        bool check_mmgraph = false) {
  FUNC_TIMER();
  int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) FileAbortOnError("open");

  // Write zeroed data for headedr, bounding rects and clusters.
  MMGraph mmheader = {0};
  const uint32_t num_clusters = g.clusters.size();
  std::vector<MMClusterBoundingRect> sorted_bounding_rects(num_clusters, {0});
  std::vector<MMCluster> clusters(num_clusters);
  AppendData("file-headers", fd, (const uint8_t*)&mmheader, sizeof(mmheader));
  const uint64_t boundig_rects_data_offset =
      mmheader.sorted_bounding_rects.WriteDataBlob(
          "bounding rects", offsetof(MMGraph, sorted_bounding_rects), fd,
          sorted_bounding_rects);
  const uint64_t clusters_data_offset = mmheader.clusters.WriteDataBlob(
      "clusters", offsetof(MMGraph, clusters), fd, clusters);

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
    WriteMMCluster(tci, &clusters.at(tci.cluster_id),
                   // global file offset of this MMCluster object.
                   clusters_data_offset + (tci.cluster_id * sizeof(MMCluster)),
                   fd);
    if (!check_mmgraph) {
      tci = {};  // Clear all data, release memory.
    }
    sorted_bounding_rects.at(tci.cluster_id) = {
        .cluster_id = tci.cluster_id, .bounding_rect = tci.mm_bounding_rect};
  }

  std::sort(sorted_bounding_rects.begin(), sorted_bounding_rects.end(),
            [](const MMClusterBoundingRect& a, const MMClusterBoundingRect& b) {
              if (a.bounding_rect.min.lon != b.bounding_rect.min.lon) {
                return a.bounding_rect.min.lon < b.bounding_rect.min.lon;
              }
              return a.cluster_id < b.cluster_id;
            });

  LogMemoryUsage();

  // Rewrite the header data and the two vectors belonging to the header.
  mmheader.file_size = GetFileSize(fd);
  WriteDataTo(fd, 0, (const uint8_t*)&mmheader, sizeof(mmheader));
  WriteDataTo(fd, boundig_rects_data_offset,
              (const uint8_t*)sorted_bounding_rects.data(),
              VectorDataSizeInBytes(sorted_bounding_rects));
  WriteDataTo(fd, clusters_data_offset, (const uint8_t*)clusters.data(),
              VectorDataSizeInBytes(clusters));
  CHECK_EQ_S(mmheader.file_size, GetFileSize(fd));

  if (::fsync(fd) != 0) FileAbortOnError("fsync");
  if (::close(fd) != 0) FileAbortOnError("close");

  if (check_mmgraph) {
    CheckMMGraph(path, g, tmp_cluster_infos);
  }
}
