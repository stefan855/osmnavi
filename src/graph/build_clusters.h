#pragma once

#include <stdio.h>

#include <iostream>
#include <vector>

#include "absl/container/btree_map.h"
#include "absl/strings/str_format.h"
#include "algos/louvain.h"
#include "base/thread_pool.h"
#include "graph/graph_def.h"
#include "logging/loguru.h"

namespace build_clusters {

namespace {

using TGraphToLouvain = absl::btree_map<uint32_t, uint32_t>;
using TGVec = std::vector<std::unique_ptr<louvain::LouvainGraph>>;

constexpr int64_t dfl_total_edge_weight = 5000000;

// Check if node is eligible for louvain clustering.
inline bool EligibleNodeForLouvain(const GNode& n) {
  return n.dead_end == 0 && n.large_component == 1;
}

// Check if node is eligible for louvain clustering and if it has country code
// 'ncc'.
inline bool EligibleNodeForLouvain(const GNode& n, std::uint16_t ncc) {
  return EligibleNodeForLouvain(n) && n.ncc == ncc;
}

// Create The first two levels of the Louvain graph and return them in a vector.
// The first level is a straight copy of the input. The second level has lines
// of nodes clustered.
inline TGVec CreateInitalLouvainGraph(const Graph& graph,
                                      const TGraphToLouvain& np_to_louvain_pos,
                                      uint16_t ncc) {
  TGVec gvec;
  gvec.push_back(std::make_unique<louvain::LouvainGraph>());
  louvain::LouvainGraph* g = gvec.back().get();

  for (auto [np, louvain_pos] : np_to_louvain_pos) {
    g->AddNode(louvain_pos, np);
    const GNode& n = graph.nodes.at(np);
    for (size_t ep = 0; ep < gnode_total_edges(n); ++ep) {
      const GEdge& e = n.edges[ep];
      const GNode& other = graph.nodes.at(e.other_node_idx);

      if (e.unique_other && e.other_node_idx != np &&
          EligibleNodeForLouvain(other, n.ncc)) {
        auto it = np_to_louvain_pos.find(e.other_node_idx);
        CHECK_S(it != np_to_louvain_pos.end()) << ncc << ":" << n.ncc;
        g->AddEdge(it->second, /*weight=*/n.ncc != other.ncc ? 1 : 1);
#if 0
        const GWay& way = graph.ways.at(e.way_idx);
        if (way.highway_label == HW_RESIDENTIAL ||
            way.highway_label == HW_LIVING_STREET ||
            way.highway_label == HW_SERVICE) {
          g->AddEdge(it->second, /*weight=*/1);
        } else {
          g->AddEdge(it->second, /*weight=*/1);
        }
#endif
      }
    }
    // Check that the new node has actually some edges.
    // CHECK_GT_S(g->nodes.back().num_edges, 0);
  }

  LOG_S(INFO) << absl::StrFormat("Louvain nodes: %12d", g->nodes.size());
  LOG_S(INFO) << absl::StrFormat("Louvain edges: %12d", g->edges.size());

  {
    louvain::NodeLineRemover::ClusterLineNodes(g);
    RemoveEmptyClusters(g);
    gvec.push_back(std::make_unique<louvain::LouvainGraph>());
    CreateClusterGraph(*g, gvec.back().get());
    g = gvec.back().get();
    g->SetTotalEdgeWeight(dfl_total_edge_weight);
  }

  LOG_S(INFO) << absl::StrFormat("Louvain nodes (line nodes removed): %12d",
                                 g->nodes.size());
  LOG_S(INFO) << absl::StrFormat("Louvain edges (line nodes removed): %12d",
                                 g->edges.size());

  return gvec;
}

void RunLouvainLevels(TGVec* gvec) {
  louvain::LouvainGraph* g = gvec->back().get();
  constexpr int MaxLevel = 20;
  for (int level = 0; level < MaxLevel; ++level) {
    g->Validate();

    uint32_t prev_moves = INFU32;
    uint32_t prev_empty = 0;
    for (int step = 0; step < 40; ++step) {
      uint32_t moves = g->Step();
      if (g->empty_clusters_ <= prev_empty && moves >= prev_moves) {
        // stop when empty clusters start shrinking.
        break;
      }
      prev_moves = moves;
      prev_empty = g->empty_clusters_;

      LOG_S(INFO) << absl::StrFormat(
          "Level %d Step %d moves:%u #clusters:%u empty:%u", level, step, moves,
          g->clusters.size(), g->empty_clusters_);
      if (moves == 0 || moves < g->nodes.size() / 1000000) {
        if (step == 0) {
          level = MaxLevel - 1;  // Complete stop.
        }
        break;
      }
    }
    RemoveEmptyClusters(g);
    if (level < MaxLevel - 1) {
      // Create a new level.
      gvec->push_back(std::make_unique<louvain::LouvainGraph>());
      CreateClusterGraph(*g, gvec->back().get());
      g = gvec->back().get();
      g->SetTotalEdgeWeight(dfl_total_edge_weight);
    }
  }
}

// Store the cluster information in Graph g.
void AddClustersAndClusterIds(
    std::uint16_t ncc,
    const std::vector<std::unique_ptr<louvain::LouvainGraph>>& gvec, Graph* g) {
  FUNC_TIMER();
  const louvain::LouvainGraph& lg = *gvec.front();

  // Add new clusters to clusters vector.
  const size_t offset = g->clusters.size();
  const size_t num_new = gvec.back()->clusters.size();

  g->clusters.resize(offset + num_new);
  for (uint32_t cluster_id = offset; cluster_id < g->clusters.size();
       ++cluster_id) {
    g->clusters.at(cluster_id).ncc = ncc;
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

// Experimental.
void ExecuteLouvainStages(int n_threads, Graph* graph) {
  FUNC_TIMER();

  // 'np_to_louvain_pos' contains a mapping from a node position in
  // graph.nodes to the precomputed node position in the louvain graph.
  // The map is sorted by key in ascending order, and by construction, the
  // pointed to values are also sorted.
  //
  // Keys are only inserted for eligible nodes that have at least one edge to
  // another eligible node.
  //
  // Note that when iterating, the keys *and* the values will appear in
  // increasing order.
  //
  // Instead of a btree a vector could be used. Binary search is used to find
  // a node. It is not clear if this would be faster though, because lookup in
  // btrees is more cpu-cache friendly than binary search in a vector.
  std::vector<TGraphToLouvain*> node_map(MAX_NCC, nullptr);
  // absl::btree_map<uint32_t, uint32_t> np_to_louvain_pos;
  for (uint32_t np = 0; np < graph->nodes.size(); ++np) {
    const GNode& n = graph->nodes.at(np);
    if (EligibleNodeForLouvain(n)) {
      for (size_t ep = 0; ep < gnode_total_edges(n); ++ep) {
        const GEdge& e = n.edges[ep];
        // Check if this edge is in the louvain graph.
        if (e.unique_other && e.other_node_idx != np &&
            EligibleNodeForLouvain(graph->nodes.at(e.other_node_idx))) {
          // Edge between two eligible nodes.
          // Node 'n' at position 'np' is good to use.
          TGraphToLouvain* np_to_louvain_pos = node_map.at(n.ncc);
          if (np_to_louvain_pos == nullptr) {
            np_to_louvain_pos = new TGraphToLouvain();
            node_map.at(n.ncc) = np_to_louvain_pos;
          }
          (*np_to_louvain_pos)[np] = np_to_louvain_pos->size();
          break;
        }
      }
    }
  }

  ThreadPool pool;
  for (size_t ncc = 0; ncc < node_map.size(); ++ncc) {
    TGraphToLouvain* np_to_louvain_pos = node_map.at(ncc);
    if (np_to_louvain_pos == nullptr) {
      continue;
    }
    node_map.at(ncc) = nullptr;
    pool.AddWork([ncc, graph, np_to_louvain_pos](int thread_idx) {
      TGVec gvec = CreateInitalLouvainGraph(*graph, *np_to_louvain_pos, ncc);
      delete np_to_louvain_pos;
      RunLouvainLevels(&gvec);
      AddClustersAndClusterIds(ncc, gvec, graph);
    });
  }
  pool.Start(n_threads);
  pool.WaitAllFinished();
}

void UpdateGraphClusterInformation(Graph* g) {
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

    for (size_t edge_pos = 0; edge_pos < gnode_total_edges(n); ++edge_pos) {
      GEdge& e = n.edges[edge_pos];
      GNode& other = g->nodes.at(e.other_node_idx);

      // By construction, any connection to an non-clustered node must be
      // through a bridge.
      // CHECK_EQ_S(other.cluster_id == INVALID_CLUSTER_ID, e.bridge != 0);
      if (e.bridge) {
        ;  // do nothing
      } else if (n.cluster_id == other.cluster_id) {
        // Count inner edges only once (instead of twice). Self-edges are not
        // counted.
        if (n.node_id > other.node_id) {
          cluster.num_inner_edges++;
        }
      } else {
        // TODO: if other node was not clustered, then it should be in other
        // country.
        if (other.cluster_id == INVALID_CLUSTER_ID) {
          CHECK_NE_S(n.ncc, other.ncc);
        }
        cluster.num_outer_edges++;
        n.cluster_border_node = 1;
        other.cluster_border_node = 1;
      }
    }
  }

  // Store and count border nodes in a *separate* loop. This avoids issues when
  // there are parallel edges.
  for (uint32_t node_pos = 0; node_pos < g->nodes.size(); ++node_pos) {
    GNode& n = g->nodes.at(node_pos);
    if (!n.cluster_border_node) {
      continue;
    }
    // CHECK_NE_S(n.cluster_id, INVALID_CLUSTER_ID);
    if (n.cluster_id != INVALID_CLUSTER_ID) {
      GCluster& cluster = g->clusters.at(n.cluster_id);
      cluster.num_border_nodes++;
      cluster.border_nodes.push_back(node_pos);
    }
  }

  for (GCluster& cluster : g->clusters) {
    // By construction, we should not have empty clusters.
    CHECK_GT_S(cluster.num_nodes, 0);
    std::sort(cluster.border_nodes.begin(), cluster.border_nodes.end());
  }
}

void PrintClusterInformation(const Graph& g) {
  std::vector<GCluster> stats(g.clusters.begin(), g.clusters.end());
  std::sort(stats.begin(), stats.end(), [](const auto& a, const auto& b) {
    if (a.ncc != b.ncc) return a.ncc < b.ncc;
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
        "%s Rank:%5u Cluster %4u: Nodes:%5u Border:%5u In:%5u Out:%5u  "
        "Out/In:%2.2f%% ex-id:%lld",
        CountryNumToString(rec.ncc), i, rec.cluster_id, rec.num_nodes,
        rec.num_border_nodes, rec.num_inner_edges, rec.num_outer_edges,
        (100.0 * rec.num_outer_edges) / std::max(rec.num_inner_edges, 1u),
        rec.border_nodes.empty() ? 0
                                 : g.nodes.at(rec.border_nodes.at(0)).node_id);
  }
  LOG_S(INFO) << absl::StrFormat(
      "Total border distance entries:%u, %.3f per node in graph", table_size,
      (table_size + 0.0) / g.nodes.size());
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

    for (size_t edge_pos = 0; edge_pos < gnode_total_edges(n0); ++edge_pos) {
      const GEdge& e = n0.edges[edge_pos];
      if (e.bridge || !e.unique_other) continue;
      const GNode& n1 = g.nodes.at(e.other_node_idx);
      // Ignore half of the edges and nodes that are not in a cluster.
      if (e.other_node_idx <= node_pos || n1.cluster_id == INVALID_CLUSTER_ID) {
        continue;
      }

      std::string_view color = "dpink";  // edges between clusters
      if (n0.cluster_id == n1.cluster_id) {
        // Edge within cluster.
        constexpr int32_t kMaxColor = 17;
        static std::string_view colors[kMaxColor] = {
            "blue",   "green",  "red",     "yel",    "violet", "olive",
            "lblue",  "dgreen", "dred",    "brown",  "grey",   "gblue",
            "orange", "lgreen", "greenbl", "lbrown", "pink",
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
}  // namespace build_clusters
