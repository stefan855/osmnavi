#pragma once

#include <stdio.h>

#include <iostream>
#include <vector>

#include "algos/louvain.h"
#include "graph/graph_def.h"
#include "logging/loguru.h"

namespace build_graph {

// Store the cluster information in Graph g.
void StoreClusterInformation(
    const std::vector<std::unique_ptr<louvain::LouvainGraph>>& gvec, Graph* g) {
  FuncTimer timer("StoreClusterInformation()");
  const louvain::LouvainGraph& lg = *gvec.front();

  // Initialise clusters vector.
  g->clusters.resize(gvec.back()->clusters.size());
  for (uint32_t cluster_id = 0; cluster_id < g->clusters.size(); ++cluster_id) {
    g->clusters.at(cluster_id).cluster_id = cluster_id;
  }

  // Store cluster_id in all nodes.
  for (uint32_t node_pos = 0; node_pos < lg.nodes.size(); ++node_pos) {
    const louvain::LouvainNode& n = lg.nodes.at(node_pos);
    const uint32_t cluster_id = FindFinalCluster(gvec, node_pos);
    GNode* gn = &g->nodes.at(n.back_ref);
    gn->cluster_id = cluster_id;
  }

  // Mark border nodes.
  for (uint32_t node_pos = 0; node_pos < g->nodes.size(); ++node_pos) {
    GNode& n = g->nodes.at(node_pos);
    if (n.cluster_id == INVALID_CLUSTER_ID) {
      continue;
    }
    GCluster& cluster = g->clusters.at(n.cluster_id);
    cluster.num_nodes++;

    for (size_t edge_pos = 0; edge_pos < gnode_num_edges(n); ++edge_pos) {
      const GEdge& e = n.edges[edge_pos];
      if (!e.unique_other) continue;
      const GNode& other = g->nodes.at(e.other_node_idx);
      // By construction, any connection to an non-clustered node should be
      // through a bridge.
      CHECK_EQ_S(other.cluster_id == INVALID_CLUSTER_ID, e.bridge != 0);
      if (e.bridge) {
        cluster.num_bridges++;
      } else if (n.cluster_id == other.cluster_id) {
        cluster.num_inner_edges++;
      } else {
        CHECK_NE_S(other.cluster_id, INVALID_CLUSTER_ID);
        n.cluster_border_node = 1;
        cluster.num_border_nodes++;
        cluster.num_outer_edges++;
        cluster.border_nodes.push_back(node_pos);
      }
    }
  }

  for (GCluster& cluster : g->clusters) {
    // By construction, we should not have empty clusters.
    CHECK_GT_S(cluster.num_nodes, 0);
    std::sort(cluster.border_nodes.begin(), cluster.border_nodes.end());
  }
}

void PrintClusterInformation(
    const Graph& g,
    const std::vector<std::unique_ptr<louvain::LouvainGraph>>& gvec) {
  const louvain::LouvainGraph& lg = *gvec.front();
  std::vector<GCluster> stats(g.clusters.begin(), g.clusters.end());
  std::sort(stats.begin(), stats.end(), [](const auto& a, const auto& b) {
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
  uint32_t max_in = 0;
  uint32_t max_out = 0;
  uint32_t min_nodes = 1 << 31;
  uint32_t min_border_nodes = 1 << 31;
  uint32_t min_in = 1 << 31;
  uint32_t min_out = 1 << 31;
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
    max_in = std::max(max_in, rec.num_inner_edges);
    max_out = std::max(max_out, rec.num_outer_edges);
    min_nodes = std::min(min_nodes, rec.num_nodes);
    min_border_nodes = std::min(min_border_nodes, rec.num_border_nodes);
    min_in = std::min(min_in, rec.num_inner_edges);
    min_out = std::min(min_out, rec.num_outer_edges);

    replacement_edges +=
        (rec.num_border_nodes * (rec.num_border_nodes - 1)) / 2;

    LOG_S(INFO) << absl::StrFormat(
        "Rank:%5u Cluster %4u: Nodes:%5u Border:%5u In-Edges:%5u Out-Edges:%5u "
        "Out/In:%2.2f%%",
        i, rec.cluster_id, rec.num_nodes, rec.num_border_nodes,
        rec.num_inner_edges, rec.num_outer_edges,
        (100.0 * rec.num_outer_edges) / std::max(rec.num_inner_edges, 1u));
  }
  LOG_S(INFO) << absl::StrFormat("Table size %u, %.3f per node in graph",
                                 table_size,
                                 (table_size + 0.0) / lg.nodes.size());
  LOG_S(INFO) << absl::StrFormat(
      "  Sum nodes:%.0f sum border:%.0f sum in-edges:%.0f sum out-edges:%.0f",
      sum_nodes, sum_border_nodes, sum_in, sum_out);
  LOG_S(INFO) << absl::StrFormat(
      "  Avg nodes:%.1f avg border:%.1f avg in-edges:%.1f avg out-edges:%.1f",
      sum_nodes / stats.size(), sum_border_nodes / stats.size(),
      sum_in / stats.size(), sum_out / stats.size());
  LOG_S(INFO) << absl::StrFormat(
      "  Max nodes:%u max border:%u max in-edges:%u max out-edges:%u",
      max_nodes, max_border_nodes, max_in, max_out);
  LOG_S(INFO) << absl::StrFormat(
      "  Min nodes:%u min border:%u min in-edges:%u min out-edges:%u",
      min_nodes, min_border_nodes, min_in, min_out);

  // Cluster Graph Summary
  uint64_t total_edges = replacement_edges + (uint64_t)sum_out / 2;
  uint64_t total_nodes = (uint64_t)sum_border_nodes;
  LOG_S(INFO) << absl::StrFormat(
      "  Cluster-Graph: added edges:%llu total edges:%llu (%.1f%%)  total "
      "nodes:%llu (%.1f%%)",
      replacement_edges, total_edges,
      (100.0 * total_edges) / ((sum_in + sum_out) / 2), total_nodes,
      (100.0 * total_nodes) / sum_nodes);
}

void WriteLouvainGraph(const Graph& g, const std::string& filename) {
  LOG_S(INFO) << absl::StrFormat("Write louvain graph to %s", filename.c_str());
  std::ofstream myfile;
  myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);

  uint64_t count = 0;
  for (uint32_t node_pos = 0; node_pos < g.nodes.size(); ++node_pos) {
    const GNode& n0 = g.nodes.at(node_pos);
    if (n0.cluster_id == INVALID_CLUSTER_ID) {
      continue;
    }

    for (size_t edge_pos = 0; edge_pos < gnode_num_edges(n0); ++edge_pos) {
      const GEdge& e = n0.edges[edge_pos];
      if (e.bridge || !e.unique_other) continue;
      const GNode& n1 = g.nodes.at(e.other_node_idx);
      // Ignore half of the edges and nodes that are not in a cluster.
      if (e.other_node_idx <= node_pos || n1.cluster_id == INVALID_CLUSTER_ID) {
        continue;
      }

      std::string_view color = "mag";  // edges between clusters
      if (n0.cluster_id == n1.cluster_id) {
        // edge within cluster.
        constexpr int32_t kMaxColor = 16;
        static std::string_view colors[kMaxColor] = {
            "blue",  "green",  "red",    "black", "yel",   "violet",
            "olive", "lblue",  "dgreen", "dred",  "brown", "grey",
            "gblue", "orange", "lgreen", "pink",
        };
        color = colors[n0.cluster_id % kMaxColor];
      }

      myfile << absl::StrFormat("line,%s,%d,%d,%d,%d\n", color, n0.lat, n0.lon,
                                n1.lat, n1.lon);
      count++;
    }
  }

  myfile.close();
  LOG_S(INFO) << absl::StrFormat("Written %d lines to %s", count, filename);
}

}  // namespace build_graph
