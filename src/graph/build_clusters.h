#pragma once

#include <stdio.h>

#include <iostream>
#include <random>
#include <vector>

#include "absl/container/btree_map.h"
#include "absl/strings/str_format.h"
#include "algos/compact_dijkstra.h"
#include "algos/compact_edge_dijkstra.h"
#include "algos/louvain.h"
#include "algos/louvain_precluster.h"
#include "algos/router.h"
#include "base/thread_pool.h"
#include "graph/graph_def.h"
#include "logging/loguru.h"

namespace build_clusters {

namespace {

// Maps a node index in graph to a node index in the louvain graph.
using GNodeToLouvainIdx = absl::btree_map<uint32_t, uint32_t>;
using TGVec = std::vector<std::unique_ptr<louvain::LouvainGraph>>;

constexpr int64_t dfl_total_edge_weight = 5000000;

// Check if node is eligible for louvain clustering.
inline bool EligibleNodeForLouvain(const GNode& n) {
  return n.dead_end == 0 && n.large_component == 1;
}

// Create The first two levels of the Louvain graph and return them in a vector.
// The first level is a straight copy of the input. The second level has
// specific kinds of nodes(line nodes, restricted nodes, turn restriction nodes)
// clustered.
inline TGVec CreateInitalLouvainGraph(
    const Graph& graph, const GNodeToLouvainIdx& gidx_to_louvain_pos) {
  TGVec gvec;
  gvec.push_back(std::make_unique<louvain::LouvainGraph>());
  louvain::LouvainGraph* lg = gvec.back().get();
  // "Normal" nodes that should be preclustered such as nodes in restricted
  // areas or nodes that are part of a complex turn restriction.
  HugeBitset precluster_common_nodes;
  // Nodes that are on a line, which can be collapsed into one edge.
  HugeBitset precluster_line_nodes;

  // Add nodes from complex turn restrictions.
  for (const TurnRestriction& ctr : graph.complex_turn_restrictions) {
    for (const TurnRestriction::TREdge& tre : ctr.path) {
      const auto it1 = gidx_to_louvain_pos.find(tre.from_node_idx);
      if (it1 != gidx_to_louvain_pos.end()) {
        precluster_common_nodes.AddBit(it1->second);
      }
      const auto it2 = gidx_to_louvain_pos.find(tre.to_node_idx);
      if (it2 != gidx_to_louvain_pos.end()) {
        precluster_common_nodes.AddBit(it2->second);
      }
    }
  }

  for (auto [gnode_pos, louvain_pos] : gidx_to_louvain_pos) {
    lg->AddNode(louvain_pos, gnode_pos);
    // const GNode& n = graph.nodes.at(gnode_pos);
    // const bool n_has_node_tags = (graph.FindNodeTags(n.node_id) != nullptr);

    for (const GEdge& e : gnode_all_edges(graph, gnode_pos)) {
      const GNode& other = graph.nodes.at(e.target_idx);

      if (e.target_idx != gnode_pos && EligibleNodeForLouvain(other)) {
        auto it = gidx_to_louvain_pos.find(e.target_idx);
        CHECK_S(it != gidx_to_louvain_pos.end());

        if (e.unique_target) {
          lg->AddEdge(it->second, /*weight=*/1);
        }

        // The code below needs to run for all, also non-unique edges.
        if (e.car_label != GEdge::LABEL_FREE
            // TODO: Simple turn restrictions might not be needed when using
            // edge based routing also through clusters. If this is true, then
            // the code below really isn't needed anymore, otherwise we need to
            // inspect simple turn restrictions somehow and use them here. Also
            // node barrier data used to produce "fake" turn restrictions!
            //
            // n.simple_turn_restriction_via_node ||
            // other.simple_turn_restriction_via_node /* || n_has_node_tags ||
            // graph.FindNodeTags(other.node_id) != nullptr*/
        ) {
          precluster_common_nodes.AddBit(louvain_pos);
          precluster_common_nodes.AddBit(it->second);
        }
      }
    }
  }

  LOG_S(INFO) << absl::StrFormat("Louvain nodes: %12d", lg->nodes.size());
  LOG_S(INFO) << absl::StrFormat("Louvain edges: %12d", lg->edges.size());

  {
    // Create the first level with preclustering.
    // louvain::NodeLineRemover::ClusterLineNodes(lg);
    louvain::NodeLineRemover::CollectLineNodes(*lg, &precluster_line_nodes);
    LOG_S(INFO) << absl::StrFormat("Louvain precluster common: %12d",
                                   precluster_common_nodes.CountBits());
    LOG_S(INFO) << absl::StrFormat("Louvain precluster lines:      %12d",
                                   precluster_line_nodes.CountBits());
    precluster_line_nodes.AddFrom(precluster_common_nodes);
    LOG_S(INFO) << absl::StrFormat("Louvain precluster combined:   %12d",
                                   precluster_line_nodes.CountBits());
    louvain::PreclusterNodes(&precluster_line_nodes, lg);
    RemoveEmptyClusters(lg);
    gvec.push_back(std::make_unique<louvain::LouvainGraph>());

    // Create the second level ready for louvain clustering.
    CreateClusterGraph(*lg, gvec.back().get());
    lg = gvec.back().get();
    lg->SetTotalEdgeWeight(dfl_total_edge_weight);
  }

  LOG_S(INFO) << absl::StrFormat("Louvain nodes (after preclustering): %12d",
                                 lg->nodes.size());
  LOG_S(INFO) << absl::StrFormat("Louvain edges (after preclustering): %12d",
                                 lg->edges.size());

  return gvec;
}

// Repeatedly cluster the Louvain graph in gvec->back() and add the newly
// clustered graph at the end.
void MainLouvainLoop(TGVec* gvec) {
  louvain::LouvainGraph* lg = gvec->back().get();
  constexpr int MaxLevel = 20;
  for (int level = 0; level < MaxLevel; ++level) {
    lg->Validate();

    uint32_t prev_moves = INFU32;
    uint32_t prev_empty = 0;
    for (int step = 0; step < 40; ++step) {
      uint32_t moves = lg->Step();
      if (lg->empty_clusters_ <= prev_empty && moves >= prev_moves) {
        // stop when empty clusters start shrinking.
        break;
      }
      prev_moves = moves;
      prev_empty = lg->empty_clusters_;

      LOG_S(INFO) << absl::StrFormat(
          "Level %d Step %d moves:%u #clusters:%u empty:%u", level, step, moves,
          lg->clusters.size(), lg->empty_clusters_);
      if (moves == 0 || moves < lg->nodes.size() / 1000000) {
        if (step == 0) {
          level = MaxLevel - 1;  // Complete stop.
        }
        break;
      }
    }
    RemoveEmptyClusters(lg);
    if (level < MaxLevel - 1) {
      // Create a new level.
      gvec->push_back(std::make_unique<louvain::LouvainGraph>());
      CreateClusterGraph(*lg, gvec->back().get());
      lg = gvec->back().get();
      lg->SetTotalEdgeWeight(dfl_total_edge_weight);
    }
  }
}

// Fill g->clusters and assign cluster ids to nodes.
void AddClustersAndAssignClusterIds(
    const std::vector<std::unique_ptr<louvain::LouvainGraph>>& gvec, Graph* g) {
  FUNC_TIMER();
  const louvain::LouvainGraph& lg = *gvec.front();

  // Add new clusters to clusters vector.
  // CHECK_S(g->clusters.empty());

  // 'offset' was needed when clusters were done by country in increments.
  const size_t offset = g->clusters.size();
  const size_t num_new = gvec.back()->clusters.size();
  LOG_S(INFO) << "Add clusters " << offset << ".." << (offset + num_new - 1);

  g->clusters.resize(offset + num_new);
  CHECK_LT_S(g->clusters.size(), INVALID_CLUSTER_ID);
  for (uint32_t cluster_id = offset; cluster_id < g->clusters.size();
       ++cluster_id) {
    // g->clusters.at(cluster_id).ncc = INVALID_NCC;
    g->clusters.at(cluster_id).cluster_id = cluster_id;
  }

  // Store adjusted cluster_id in all clustered nodes.
  // The clustering is hierarchical, therefore we need the function
  // FindFinalCluster() which climbs all clustering from the bottom graph to the
  // top cluster graph to get the final cluster_id of a node.
  for (uint32_t node_pos = 0; node_pos < lg.nodes.size(); ++node_pos) {
    const louvain::LouvainNode& n = lg.nodes.at(node_pos);
    const uint32_t cluster_id = FindFinalCluster(gvec, node_pos);
    GNode* gn = &g->nodes.at(n.back_ref);
    gn->cluster_id = offset + cluster_id;
  }
}

}  // namespace

inline void ExecuteLouvain(int n_threads, Graph* graph) {
  FUNC_TIMER();

  // 'gidx_to_louvain_pos' contains a mapping from node indexes in graph.nodes
  // to the precomputed node positions in the louvain graph.
  //
  // The map is sorted by key in ascending order, and by construction, the
  // pointed to values are also sorted.
  //
  // Keys are only inserted for eligible nodes that have at least one edge to
  // another eligible node.
  //
  // Note that when iterating, the keys *and* the values will appear in
  // increasing order.
  //
  // TODO: Instead of a btree, a vector could be used. Binary search is needed
  // to find a node. It is not clear if this would be faster though, because
  // lookup in btrees is more cpu-cache friendly than binary search in a vector.

  for (const Graph::Component& comp : graph->large_components) {
    LOG_S(INFO) << "Cluster component with " << comp.nodes.size() << " entries";
    GNodeToLouvainIdx gidx_to_louvain_pos;
    // Iterate over all nodes in the component and update the node for
    // eligble nodes.
    // for (uint32_t gnode_idx = 0; gnode_idx < comp.nodes.size(); ++gnode_idx)
    // {
    for (uint32_t gnode_idx : comp.nodes) {
      const GNode& n = graph->nodes.at(gnode_idx);
      if (EligibleNodeForLouvain(n)) {
        for (const GEdge& e : gnode_all_edges(*graph, gnode_idx)) {
          // Check if this edge is in the louvain graph.
          if (e.unique_target && e.target_idx != gnode_idx &&
              EligibleNodeForLouvain(graph->nodes.at(e.target_idx))) {
            // Edge between two eligible nodes.
            // Node 'n' at position 'gnode_idx' is good to use.
            gidx_to_louvain_pos[gnode_idx] = 1;  // '1' will be changed below.
            break;
          }
        }
      }
    }

    // Assign increasing louvain positions 0..size-1.
    {
      uint32_t idx = 0;
      for (auto& it : gidx_to_louvain_pos) {
        it.second = idx++;
      }
    }

    TGVec gvec = CreateInitalLouvainGraph(*graph, gidx_to_louvain_pos);
    gidx_to_louvain_pos.clear();
    MainLouvainLoop(&gvec);
    // Create clusters and store cluster_id for each node.
    AddClustersAndAssignClusterIds(gvec, graph);
  }
}

// Assign cluster_id to the nodes in the subtree below 'start_node_idx'.
//
// Note that 'start_node_idx' has to be on the dead-end side of a bridge edge.
inline uint32_t AssignClusterIdsInDeadEnd(Graph* g, uint32_t start_node_idx,
                                          uint32_t cluster_id) {
  GCluster& cluster = g->clusters.at(cluster_id);
  GNode& start = g->nodes.at(start_node_idx);
  CHECK_S(start.dead_end);
  CHECK_EQ_S(start.cluster_id, INVALID_CLUSTER_ID);
  start.cluster_id = cluster_id;
  cluster.num_deadend_nodes++;
  std::vector<uint32_t> nodes({start_node_idx});
  uint32_t num_nodes = 1;
  size_t pos = 0;
  while (pos < nodes.size()) {
    uint32_t node_pos = nodes.at(pos++);
    for (GEdge& e : gnode_all_edges(*g, node_pos)) {
      if (e.bridge) {
        continue;
      }
      CHECK_S(e.dead_end) << GetGNodeIdSafe(*g, node_pos) << "->"
                          << GetGNodeIdSafe(*g, e.target_idx);
      GNode& target = g->nodes.at(e.target_idx);
      if (!e.unique_target || target.cluster_id == cluster_id) {
        continue;
      }
      CHECK_S(target.dead_end);
      CHECK_EQ_S(target.cluster_id, INVALID_CLUSTER_ID);
      target.cluster_id = cluster_id;
      cluster.num_deadend_nodes++;
      num_nodes++;
      nodes.push_back(e.target_idx);
    }
  }
  return num_nodes;
}

inline void AssignClusterIdsInAllDeadEnds(Graph* g) {
  FUNC_TIMER();
  for (uint32_t node_pos = 0; node_pos < g->nodes.size(); ++node_pos) {
    GNode& n = g->nodes.at(node_pos);
    for (GEdge& e : gnode_all_edges(*g, node_pos)) {
      if (!e.bridge) continue;
      const GNode& target = g->nodes.at(e.target_idx);
      if (n.dead_end) {  // 'n' on dead end side of bridge.
        if (target.cluster_id != INVALID_CLUSTER_ID &&
            n.cluster_id == INVALID_CLUSTER_ID) {
          AssignClusterIdsInDeadEnd(g, node_pos, target.cluster_id);
        }
      } else if (target.dead_end) {  // 'target' on dead end side of bridge.
        if (target.cluster_id == INVALID_CLUSTER_ID &&
            n.cluster_id != INVALID_CLUSTER_ID) {
          AssignClusterIdsInDeadEnd(g, e.target_idx, n.cluster_id);
        }
      }
    }
  }
}

inline void UpdateEdgesAndBorderNodes(Graph* g) {
  FUNC_TIMER();
  // Mark cross-cluster edges and count edge types.
  for (uint32_t node_pos = 0; node_pos < g->nodes.size(); ++node_pos) {
    GNode& n = g->nodes.at(node_pos);
    n.cluster_border_node = 0;
    if (n.cluster_id == INVALID_CLUSTER_ID) {
      continue;
    }
    GCluster& cluster = g->clusters.at(n.cluster_id);
    cluster.num_nodes++;

    for (GEdge& e : gnode_all_edges(*g, node_pos)) {
      GNode& other = g->nodes.at(e.target_idx);

      // By construction, any connection to an non-clustered node must be
      // through a bridge.
      // CHECK_EQ_S(other.cluster_id == INVALID_CLUSTER_ID, e.bridge !=
      // 0);
      if (e.bridge) {
        // Assign cluster id to all edges in dead end.
        ;  // do nothing, ignore dead-ends.
      } else if (n.cluster_id == other.cluster_id) {
        // Count inner edges only once (instead of twice). Self-edges are
        // not counted.
        if (n.node_id > other.node_id) {
          cluster.num_inner_edges++;
        }
        // CHECK_S(e.type == GEdge::TYPE_UNKNOWN ||
        //         e.type == GEdge::TYPE_CLUSTER_INNER)
        //     << e.type;
        // e.type = GEdge::TYPE_CLUSTER_INNER;
      } else {
        CHECK_NE_S(other.cluster_id, INVALID_CLUSTER_ID);
        cluster.num_outer_edges++;
        n.cluster_border_node = 1;
        other.cluster_border_node = 1;
        // CHECK_EQ_S(e.type, GEdge::TYPE_UNKNOWN);
        // e.type = GEdge::TYPE_CLUSTER_BORDER;
        e.cluster_border_edge = 1;
      }
    }
  }

  // Store and count border nodes in a *separate* loop. This avoids issues
  // when there are parallel edges.
  for (uint32_t node_pos = 0; node_pos < g->nodes.size(); ++node_pos) {
    GNode& n = g->nodes.at(node_pos);
    if (!n.cluster_border_node) {
      continue;
    }
    CHECK_NE_S(n.cluster_id, INVALID_CLUSTER_ID);
    if (n.cluster_id != INVALID_CLUSTER_ID) {
      GCluster& cluster = g->clusters.at(n.cluster_id);
      cluster.num_border_nodes++;
      cluster.border_nodes.push_back(node_pos);
    }
  }

  for (GCluster& cluster : g->clusters) {
    // By construction, we should not have empty clusters.
    CHECK_GT_S(cluster.num_nodes, 0) << cluster.cluster_id;
    // By construction, the node positions should be sorted.
    CHECK_S(std::is_sorted(cluster.border_nodes.begin(),
                           cluster.border_nodes.end()))
        << cluster.cluster_id;
    // std::sort(cluster.border_nodes.begin(),
    // cluster.border_nodes.end());
  }

  AssignClusterIdsInAllDeadEnds(g);
}

// Compute all shortest paths between border nodes in a cluster. The
// results are stored in 'cluster.distances'.
inline void ComputeShortestClusterPaths(const Graph& g,
                                        const RoutingMetric& metric, VEHICLE vt,
                                        GCluster* cluster) {
  CHECK_S(cluster->distances.empty());
  const size_t num_border_nodes = cluster->border_nodes.size();
  if (num_border_nodes == 0) {
    return;  // Nothing to compute, cluster is isolated.
  }

  // Construct a minimal graph with all necessary information.
  uint32_t num_nodes = 0;
  std::vector<CompactGraph::FullEdge> full_edges;
  absl::flat_hash_map<uint32_t, uint32_t> gnode_to_compact;
  CollectEdgesForCompactGraph(g, metric,
                              {.vt = vt,
                               .avoid_restricted_access_edges = true,
                               .restrict_to_cluster = true,
                               .restrict_cluster_id = cluster->cluster_id},
                              cluster->border_nodes,
                              /*undirected_expand=*/false, &num_nodes,
                              &full_edges, &gnode_to_compact);
  CompactGraph::SortAndCleanupEdges(&full_edges);
  const CompactGraph cg(num_nodes, full_edges);
  cg.LogStats();

  // Execute single source Dijkstra from every border node.
  for (size_t border_node = 0; border_node < num_border_nodes; ++border_node) {
    // Now store the distances for this border node.
    cluster->distances.emplace_back();
    const std::vector<compact_dijkstra::VisitedNode> vis =
        compact_dijkstra::SingleSourceDijkstra(cg, border_node);
    CHECK_EQ_S(vis.size(), num_nodes);
    for (size_t i = 0; i < num_border_nodes; ++i) {
      cluster->distances.back().push_back(vis.at(i).min_weight);
    }
  };
  CHECK_EQ_S(cluster->distances.size(), cluster->border_nodes.size());
}

namespace {

void SortBorderEdges(std::vector<GCluster::EdgeDescriptor>* edges) {
  std::sort(
      edges->begin(), edges->end(),
      [](const GCluster::EdgeDescriptor& a, const GCluster::EdgeDescriptor& b) {
        return a.g_edge_idx < b.g_edge_idx;
      });
  for (uint32_t i = 0; i < edges->size(); ++i) {
    edges->at(i).pos = i;
  }
}

// This finds and adds information about border incoming/outgoing edges,
// i.e. edges that connect 'cluster' with another cluster at border edges.
void AddBorderEdgeInformation(
    const Graph& g, const CompactGraph& cg,
    const absl::flat_hash_map<uint32_t, uint32_t>& gnode_to_compact,
    GCluster* cluster) {
  // const std::vector<uint32_t>& cg_edges_start = cg.edges_start();
  // const std::vector<CompactGraph::PartialEdge>& cg_edges =
  // cg.edges();

  // Iterate over all border nodes and collect information about
  // incoming/outgoing edges from/to other clusters.
  for (uint32_t cn_idx = 0; cn_idx < cluster->border_nodes.size(); ++cn_idx) {
    uint32_t gn_idx = cluster->border_nodes.at(cn_idx);
    const GNode& gn = g.nodes.at(gn_idx);
    CHECK_EQ_S(gn.cluster_id, cluster->cluster_id) << gn.node_id;

    // Iterate over edges arriving from another cluster to this border
    // node.
    for (const FullEdge& gfe : gnode_incoming_edges(g, gn_idx)) {
      CHECK_EQ_S(gfe.target_idx(g), gn_idx);
      uint32_t g_from_idx = gfe.start_idx();
      const GNode& gn_from = g.nodes.at(g_from_idx);
      if (gn_from.cluster_id != cluster->cluster_id &&
          gn_from.cluster_id != INVALID_CLUSTER_ID) {
        /*
        LOG_S(INFO) << absl::StrFormat("Edge %lld->%lld cluster %u->%u",
                                       gn_from.node_id, gn.node_id,
                                       gn_from.cluster_id, gn.cluster_id);
                                       */
        uint32_t c_from_idx = FindInMapOrFail(gnode_to_compact, g_from_idx);
        int64_t ce_idx = cg.FindEdge(c_from_idx, cn_idx, gfe.gedge(g).way_idx);
        CHECK_S(ce_idx >= 0);
        cluster->border_in_edges.push_back(
            {.g_from_idx = g_from_idx,
             .g_edge_idx = gfe.gedge_idx(g),
             .c_from_idx = c_from_idx,
             .c_edge_idx = (uint32_t)ce_idx,
             .pos = (uint32_t)cluster->border_in_edges.size()});
      }
    }

    // Iterate over edges leaving from this border node to another
    // cluster.
    for (uint32_t ge_idx = gn.edges_start_pos;
         ge_idx < gn.edges_start_pos + gn.num_forward_edges; ++ge_idx) {
      const GEdge& ge = g.edges.at(ge_idx);
      const GNode& gother = g.nodes.at(ge.target_idx);
      // Is this an edge connecting to another cluster?
      if (gother.cluster_id != cluster->cluster_id &&
          gother.cluster_id != INVALID_CLUSTER_ID) {
        uint32_t ct_idx = FindInMapOrFail(gnode_to_compact, ge.target_idx);
        int64_t ce_idx = cg.FindEdge(cn_idx, ct_idx, ge.way_idx);
        CHECK_S(ce_idx >= 0);
        cluster->border_out_edges.push_back(
            {.g_from_idx = gn_idx,
             .g_edge_idx = ge_idx,
             .c_from_idx = cn_idx,
             .c_edge_idx = (uint32_t)ce_idx,
             .pos = (uint32_t)cluster->border_out_edges.size()});
        // Edges between clusters should not trigger complex restrictions.
        CHECK_S(cg.edges().at(ce_idx).complex_tr_trigger == 0);
      }
    }
  }
  SortBorderEdges(&cluster->border_in_edges);
  SortBorderEdges(&cluster->border_out_edges);

  LOG_S(INFO) << absl::StrFormat(
      "Cluster %d has border nodes:%lld in edges:%lld out edges:%lld",
      cluster->cluster_id, cluster->border_nodes.size(),
      cluster->border_in_edges.size(), cluster->border_out_edges.size());
}
}  // namespace

// Compute all shortest paths between border edges in a cluster. The
// results are stored in 'cluster.edge_distances'.
inline void ComputeShortestClusterEdgePaths(Graph* g,
                                            const RoutingMetric& metric,
                                            VEHICLE vt, GCluster* cluster) {
  CHECK_S(cluster->edge_distances.empty());
  const size_t num_border_nodes = cluster->border_nodes.size();
  if (num_border_nodes == 0) {
    return;  // Nothing to compute, cluster is isolated.
  }

  // Construct a minimal graph with all necessary information.
  uint32_t num_nodes = 0;
  std::vector<CompactGraph::FullEdge> full_edges;
  absl::flat_hash_map<uint32_t, uint32_t> gnode_to_compact;
  CollectEdgesForCompactGraph(*g, metric,
                              {.vt = vt,
                               .avoid_restricted_access_edges = true,
                               .restrict_to_cluster = true,
                               .restrict_cluster_id = cluster->cluster_id},
                              cluster->border_nodes,
                              /*undirected_expand=*/false, &num_nodes,
                              &full_edges, &gnode_to_compact);
  CompactGraph::SortAndCleanupEdges(&full_edges);
  CompactGraph cg(num_nodes, full_edges);
  cg.AddComplexTurnRestrictions(g->complex_turn_restrictions, gnode_to_compact);
  cg.AddTurnCosts(*g, metric.IsTimeMetric(), gnode_to_compact);
  AddBorderEdgeInformation(*g, cg, gnode_to_compact, cluster);
  cg.LogStats();

  const std::vector<std::uint32_t> compact_to_graph =
      cg.InvertGraphToCompactNodeMap(gnode_to_compact);

  // Execute single source Dijkstra for every incoming border edge
  for (const auto& in_edge : cluster->border_in_edges) {
    // Now store the edge_distances for this border node.
    cluster->edge_distances.emplace_back();
    SingleSourceEdgeDijkstra router;
    int16_t off =
        (int16_t)(in_edge.c_edge_idx - cg.edges_start().at(in_edge.c_from_idx));
    router.Route(cg,
                 {.c_start_idx = in_edge.c_from_idx,
                  .c_edge_offset = off,
                  .edge_weight_fraction = 0.0},
                 {.handle_restricted_access = true});

    const std::vector<SingleSourceEdgeDijkstra::VisitedEdge>& vis =
        router.GetVisitedEdges();
    for (const auto& out_edge : cluster->border_out_edges) {
      // Get the best min_weight if there is more than one entry.
      cluster->edge_distances.back().push_back(
          vis.at(out_edge.c_edge_idx).min_weight);
    }

    {
      // Visit all shortest paths (ending at a border-out-edge) and label the
      // nodes as 'cluster_skeleton'.
      const std::vector<CompactGraph::PartialEdge>& edges = cg.edges();
      for (const auto& out_edge : cluster->border_out_edges) {
        // g->nodes.at(out_edge.g_from_idx).cluster_skeleton = 1;
        uint32_t e_idx = out_edge.c_edge_idx;
        if (vis.at(e_idx).min_weight == INFU32) {
          continue;
        }
        cluster->num_valid_paths += 1;
        cluster->sum_valid_path_nodes += 1;
        do {
          uint32_t base_e_idx = router.GetBaseIdx(cg.edges().size(), e_idx);
          uint32_t gnode_target_idx =
              compact_to_graph.at(edges.at(base_e_idx).to_c_idx);
          g->nodes.at(gnode_target_idx).cluster_skeleton = 1;
          e_idx = vis.at(e_idx).from_v_idx;
          cluster->sum_valid_path_nodes += 1;
        } while (e_idx != INFU32);
      }
    }
  }
  CHECK_EQ_S(cluster->edge_distances.size(), cluster->border_in_edges.size());
}

inline void CheckShortestClusterPaths(const Graph& g, int n_threads) {
  FUNC_TIMER();
  const RoutingMetricTime metric;
  ThreadPool pool;
  for (const GCluster& c : g.clusters) {
    pool.AddWork([&g, &c, &metric](int) {
      RoutingOptions opt;
      opt.use_astar_heuristic = true;
      opt.avoid_dead_end = true;
      opt.restrict_to_cluster = true;
      opt.restrict_cluster_id = c.cluster_id;
      for (uint32_t idx = 0; idx < c.num_border_nodes; ++idx) {
        for (uint32_t idx2 = 0; idx2 < c.num_border_nodes; ++idx2) {
          Router rt(g, /*verbosity=*/0);
          auto result = rt.Route(c.border_nodes.at(idx),
                                 c.border_nodes.at(idx2), metric, opt);

          if (c.distances.at(idx).at(idx2) != result.found_distance) {
            LOG_S(INFO) << absl::StrFormat(
                "Cluster:%u path from %u to %u differs -- stored:%u vs. "
                "computed:%u",
                c.cluster_id, idx, idx2, c.distances.at(idx).at(idx2),
                result.found_distance);
          }
          // CHECK(c.distances.at(idx).at(idx2), result.found_distance);
        }
      }
      LOG_S(INFO) << absl::StrFormat(
          "Checks paths in cluster:%u #border:%u finished", c.cluster_id,
          c.num_border_nodes);
    });
  }
  pool.Start(n_threads);
  pool.WaitAllFinished();
}

inline void PrintClusterInformation(const Graph& g) {
  std::vector<GCluster> stats(g.clusters.begin(), g.clusters.end());
  std::sort(stats.begin(), stats.end(), [](const auto& a, const auto& b) {
    // if (a.ncc != b.ncc) return a.ncc < b.ncc;
    double va = (100.0 * a.num_outer_edges) / std::max(a.num_inner_edges, 1u);
    double vb = (100.0 * b.num_outer_edges) / std::max(b.num_inner_edges, 1u);
    return va > vb;
  });
  uint64_t table_size = 0;
  double sum_nodes = 0;
  double sum_border_nodes = 0;
  double sum_in = 0;   // Within-cluster edges, each edge is counted twice.
  double sum_out = 0;  // Outgoing edges, each edge is counted twice.
  uint32_t max_nodes = 0;
  uint32_t max_border_nodes = 0;
  uint32_t max_deadend_nodes = 0;
  uint32_t max_tot_nodes = 0;
  uint32_t max_in = 0;
  uint32_t max_out = 0;
  uint32_t min_nodes = 1 << 31;
  uint32_t min_border_nodes = 1 << 31;
  uint32_t min_in = 1 << 31;
  uint32_t min_out = 1 << 31;
  uint64_t num_valid_paths = 0;
  uint64_t sum_valid_path_nodes = 0;
  uint64_t replacement_edges = 0;

  for (size_t i = 0; i < stats.size(); ++i) {
    const GCluster& rec = stats.at(i);

    table_size += (rec.num_outer_edges * rec.num_outer_edges);
    sum_nodes += rec.num_nodes;
    sum_border_nodes += rec.num_border_nodes;
    sum_in += rec.num_inner_edges;
    sum_out += rec.num_outer_edges;
    max_nodes = std::max(max_nodes, rec.num_nodes);
    max_border_nodes = std::max(max_border_nodes, rec.num_border_nodes);
    max_deadend_nodes = std::max(max_deadend_nodes, rec.num_deadend_nodes);
    max_tot_nodes =
        std::max(max_tot_nodes, rec.num_nodes + rec.num_deadend_nodes);
    max_in = std::max(max_in, rec.num_inner_edges);
    max_out = std::max(max_out, rec.num_outer_edges);
    min_nodes = std::min(min_nodes, rec.num_nodes);
    min_border_nodes = std::min(min_border_nodes, rec.num_border_nodes);
    min_in = std::min(min_in, rec.num_inner_edges);
    min_out = std::min(min_out, rec.num_outer_edges);
    num_valid_paths += rec.num_valid_paths;
    sum_valid_path_nodes += rec.sum_valid_path_nodes;

    replacement_edges +=
        (rec.num_border_nodes * (rec.num_border_nodes - 1)) / 2;

    LOG_S(INFO) << absl::StrFormat(
        "Rank:%5u Cluster %4u: Nodes:%5u Border:%5u Deadend:%5u Tot:%5u In:%5u "
        "Out:%5u Out/In:%2.2f%% #avg-p-nodes:%.2f ex-id:%lld",
        i, rec.cluster_id, rec.num_nodes, rec.num_border_nodes,
        rec.num_deadend_nodes, rec.num_nodes + rec.num_deadend_nodes,
        rec.num_inner_edges, rec.num_outer_edges,
        (100.0 * rec.num_outer_edges) / std::max(rec.num_inner_edges, 1u),
        static_cast<double>(rec.sum_valid_path_nodes) / rec.num_valid_paths,
        rec.border_nodes.empty() ? 0
                                 : g.nodes.at(rec.border_nodes.at(0)).node_id);
  }
  LOG_S(INFO) << absl::StrFormat(
      "Total border distance entries:%u, %.3f per node in graph", table_size,
      (table_size + 0.0) / g.nodes.size());
  LOG_S(INFO) << absl::StrFormat(
      "  Sum nodes:%.0f sum border:%.0f sum in-edges:%.0f sum "
      "out-edges:%.0f",
      sum_nodes, sum_border_nodes, sum_in, sum_out);
  LOG_S(INFO) << absl::StrFormat(
      "  Avg nodes:%.1f avg border:%.1f avg innner-edges:%.1f avg "
      "outer-edges:%.1f",
      sum_nodes / stats.size(), sum_border_nodes / stats.size(),
      sum_in / stats.size(), sum_out / stats.size());
  LOG_S(INFO) << absl::StrFormat(
      "  Max nodes:%u border:%u deadend:%u tot:%u max in-edges:%u max "
      "out-edges:%u",
      max_nodes, max_border_nodes, max_deadend_nodes, max_tot_nodes, max_in,
      max_out);
  LOG_S(INFO) << absl::StrFormat(
      "  Min nodes:%u min border:%u min in-edges:%u min out-edges:%u",
      min_nodes, min_border_nodes, min_in, min_out);
  LOG_S(INFO) << absl::StrFormat(
      "  Avg path nodes:%.2f",
      static_cast<double>(sum_valid_path_nodes) / num_valid_paths);

  // Cluster Graph Summary
  uint64_t total_edges = replacement_edges + (uint64_t)sum_out / 2;
  uint64_t total_nodes = (uint64_t)sum_border_nodes;
  LOG_S(INFO) << absl::StrFormat(
      "  Cluster-Graph: added edges:%llu total edges:%llu (%.1f%%)  "
      "total "
      "nodes:%llu (%.1f%%)",
      replacement_edges, total_edges,
      (100.0 * total_edges) / ((sum_in + sum_out) / 2), total_nodes,
      (100.0 * total_nodes) / sum_nodes);
}

// Go through clusters by increasing cluster id and assign each cluster
// the lowest color number that hasn't been used by a connected, previous
// cluster.
void AssignClusterColors(Graph* g) {
  uint32_t max_color_no = 0;
  std::srand(1);  // Get always the same pseudo-random numbers.

  // First cluster has color_no 0.
  for (uint32_t cluster_id = 1; cluster_id < g->clusters.size(); ++cluster_id) {
    // Determine the colors of neighbor clusters.
    std::vector<bool> neighbor_colors;
    GCluster* cluster = &(g->clusters.at(cluster_id));
    for (uint32_t n_idx : cluster->border_nodes) {
      for (const GEdge& e : gnode_all_edges(*g, n_idx)) {
        uint32_t other_cluster_id = g->nodes.at(e.target_idx).cluster_id;
        // Make sure we don't collide with clusters before the current
        // cluster.
        if (other_cluster_id != INVALID_CLUSTER_ID &&
            other_cluster_id < cluster_id) {
          uint32_t other_color_no = g->clusters.at(other_cluster_id).color_no;
          if (other_color_no >= neighbor_colors.size()) {
            neighbor_colors.resize(other_color_no + 1, false);
          }
          neighbor_colors.at(other_color_no) = true;
        }
      }
    }

    uint32_t free_color_no = neighbor_colors.size();
    // Find a random free color_no.
    uint64_t start = 1 + rand();
    for (uint64_t offset = 0; offset < neighbor_colors.size(); ++offset) {
      uint64_t color_no = (start + offset) % neighbor_colors.size();
      if (!neighbor_colors.at(color_no)) {
        free_color_no = color_no;
        break;
      }
    }

    // LOG_S(INFO) << absl::StrFormat("Assign cluster %u color %u",
    // cluster_id,
    //                                free_color_no);
    cluster->color_no = free_color_no;
    max_color_no = std::max(max_color_no, free_color_no);
  }

  LOG_S(INFO) << "AssignClusterColors() max-color_no:" << max_color_no;
  CHECK_LT_S(max_color_no, 16);
}

}  // namespace build_clusters
