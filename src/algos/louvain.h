#pragma once

#include <vector>

#include "absl/container/btree_map.h"
#include "absl/strings/str_format.h"
#include "base/constants.h"
#include "base/huge_bitset.h"
#include "logging/loguru.h"

namespace louvain {

struct alignas(4) LouvainCluster {
  uint32_t w_inside_edges;
  uint32_t w_tot_edges;
  uint32_t num_nodes;
};

struct LouvainEdge {
  uint32_t other_node_pos;
  uint32_t weight;
};

constexpr std::uint32_t MAX_NUM_LOUVAIN_EDGES = (1 << 10) - 1;

struct alignas(4) LouvainNode {
  uint32_t cluster_pos;     // Cluster of node.
  uint32_t num_edges : 10;  // number of 'unique' edges.
  uint32_t edge_start;      // Pos of first edge in edges vector.
  uint32_t w_self;          // Sum of self edge weights.
  uint32_t w_tot;           // Sum of edge weights.
  uint32_t back_ref;        // Reference to source of this node.
};

// LouvainGraph represents one level of the hierarchical louvain clustering
// algorithm. The initial level is created from the graph that is to be
// clustered.
//
// At each step of the algorithm, the input graph is taken and the nodes are
// clustered into a (hopefully) smaller number of clusters. Then a new
// LouvainGraph is created, using the clusters that were built in the previous
// level as nodes. This new graph is then clustered again, until clustering
// stops producing less clusters (or some other criteria).
//
struct LouvainGraph {
  std::vector<LouvainCluster> clusters;
  std::vector<LouvainNode> nodes;
  std::vector<LouvainEdge> edges;
  double two_m_ = 0;
  uint32_t empty_clusters_ = 0;
  double resolution_ = 0.5;

  void SetTotalEdgeWeight(double m) { two_m_ = 2 * m; }

  std::string DebugStringCluster(uint32_t cluster_pos) {
    const LouvainCluster& c = clusters.at(cluster_pos);
    return absl::StrFormat("C%u w_in:%u w_tot:%u #:%u q:%f", cluster_pos,
                           c.w_inside_edges, c.w_tot_edges, c.num_nodes,
                           ClusterQ(cluster_pos));
  }

  void DebugPrint() {
    LOG_S(INFO) << "****************************";
    LOG_S(INFO) << "Graph with 2m=" << two_m_;
    LOG_S(INFO) << "Nodes";
    for (size_t i = 0; i < nodes.size(); ++i) {
      const LouvainNode& n = nodes.at(i);
      LOG_S(INFO) << absl::StrFormat("N%u #:%u C%u w_self:%u w_tot:%u q:%f", i,
                                     n.num_edges, n.cluster_pos, n.w_self,
                                     n.w_tot, NodeQ(i));
    }
    LOG_S(INFO) << "Clusters";
    for (size_t i = 0; i < clusters.size(); ++i) {
      const LouvainCluster& c = clusters.at(i);
      LOG_S(INFO) << absl::StrFormat("C%u #:%u w_in:%u w_tot:%u q:%f", i,
                                     c.num_nodes, c.w_inside_edges,
                                     c.w_tot_edges, ClusterQ(i));
    }
    LOG_S(INFO) << "****************************";
  }

  // Find an edge to node 'to_pos'. Returns position of edge in edge array or
  // INFU32 if not found.
  uint32_t FindEdgeInRange(uint32_t edge_start, uint32_t edge_stop,
                           uint32_t to_pos) {
    for (uint32_t t = edge_start; t < edge_stop; ++t) {
      if (to_pos == edges.at(t).other_node_pos) {
        return t;
      }
    }
    return INFU32;
  }

  // Find an edge to node 'to_pos'. Returns position of edge in edge array or
  // INFU32 if not found.
  uint32_t FindEdge(const LouvainNode& n, uint32_t to_pos) {
    return FindEdgeInRange(n.edge_start, n.edge_start + n.num_edges, to_pos);
  }

  // Check the louvain graph and fail if anything unexpected or wrong is found.
  // This is used to find errors or issues with data. Maybe it should be used
  // only in debug mode in the future.
  void Validate() {
    for (size_t i = 0; i < edges.size(); ++i) {
      // Edge target nodes exists?
      CHECK_LT_S(edges.at(i).other_node_pos, nodes.size());
      // Weight > 0?
      CHECK_GT_S(edges.at(i).weight, 0);
    }

    uint32_t prev_edges_end = 0;
    for (size_t i = 0; i < nodes.size(); ++i) {
      const LouvainNode& n = nodes.at(i);
      // Cluster exists?
      CHECK_LT_S(n.cluster_pos, clusters.size());
      // Cluster is not empty?
      CHECK_GT_S(clusters.at(n.cluster_pos).num_nodes, 0);
      // Edges exist and are consecutive from node to node?
      CHECK_EQ_S(n.edge_start, prev_edges_end);
      prev_edges_end = n.edge_start + n.num_edges;
      CHECK_LE_S(prev_edges_end, edges.size());
    }

    for (size_t i = 0; i < nodes.size(); ++i) {
      const LouvainNode& n = nodes.at(i);
      for (uint32_t p = n.edge_start; p < n.edge_start + n.num_edges; ++p) {
        const LouvainEdge& e = edges.at(p);
        const LouvainNode& other = nodes.at(e.other_node_pos);
        // Is there a dup?
        CHECK_S(FindEdgeInRange(p + 1, n.edge_start + n.num_edges,
                                e.other_node_pos) == INFU32);
        // Back-edge exists?
        CHECK_S(FindEdge(other, i) != INFU32);
      }
    }
    for (size_t i = 0; i < clusters.size(); ++i) {
      const LouvainCluster& c = clusters.at(i);
      CHECK_GE_S(c.w_tot_edges, c.w_inside_edges);
      if (c.num_nodes == 0) {
        CHECK_EQ_S(c.w_tot_edges, 0);
        CHECK_EQ_S(c.w_inside_edges, 0);
      }
    }
  }

  double TotalQuality() {
    double q = 0;
    for (const auto& c : clusters) {
      if (c.num_nodes > 0) {
        q += ClusterQ(c);
      }
    }
    return q;
  }

  uint32_t CountClusters() {
    uint32_t count = 0;
    for (const auto& c : clusters) {
      count += c.num_nodes > 0 ? 1 : 0;
    }
    return count;
  }

  // Add a new node. Nodes must be added in order 0, 1, 2, ...
  // back_ref points to the source for this node, i.e. the node position in the
  // original graph.
  // w_self is the weight of the node. This becomes non-zero for self-edges, for
  // instance when a cluster is contracted into a single node for the next
  // level.
  void AddNode(uint32_t pos, uint32_t back_ref = 0, uint32_t w_self = 0) {
    CHECK_EQ_S(pos, nodes.size());
    CHECK_EQ_S(nodes.size(), clusters.size());
    CHECK_LT_S(nodes.size(), INFU32 - 1);
    nodes.push_back({.cluster_pos = static_cast<uint32_t>(clusters.size()),
                     .num_edges = 0,
                     .edge_start = (uint32_t)edges.size(),
                     .w_self = w_self,
                     .w_tot = w_self,
                     .back_ref = back_ref});
    // Every node starts with his own cluster.
    clusters.push_back(
        {.w_inside_edges = w_self, .w_tot_edges = w_self, .num_nodes = 1});
    two_m_ += w_self;
  }

  void AddNodeAndEdges(uint32_t pos, std::vector<uint32_t> connected_nodes,
                       uint32_t weight = 1) {
    AddNode(pos);
    for (auto other_node : connected_nodes) {
      AddEdge(other_node, weight);
    }
  }

  // Add a new edge for the last added node.
  void AddEdge(uint32_t other_node_pos, uint32_t weight = 1) {
    CHECK_NE_S(other_node_pos + 1, nodes.size());  // No self edge.
    CHECK_GT_S(weight, 0);
    edges.push_back({.other_node_pos = other_node_pos, .weight = weight});
    CHECK_LT_S(nodes.back().num_edges, MAX_NUM_LOUVAIN_EDGES);
    nodes.back().num_edges += 1;
    nodes.back().w_tot += weight;
    clusters.at(nodes.back().cluster_pos).w_tot_edges += weight;
    two_m_ += weight;
  }

  uint32_t SumWeightsToNode(uint32_t node_pos, uint32_t other_node_pos) const {
    uint32_t sum = 0;
    const LouvainNode& n = nodes.at(node_pos);
    for (size_t i = 0; i < n.num_edges; ++i) {
      const LouvainEdge e = edges.at(n.edge_start + i);
      if (e.other_node_pos != other_node_pos) continue;
      sum += e.weight;
    }
    return sum;
  }

  uint32_t SumWeightsToCluster(uint32_t node_pos, uint32_t cluster_pos) const {
    uint32_t sum = 0;
    const LouvainNode& n = nodes.at(node_pos);
    for (size_t i = 0; i < n.num_edges; ++i) {
      const LouvainEdge e = edges.at(n.edge_start + i);
      if (nodes.at(e.other_node_pos).cluster_pos != cluster_pos) continue;
      sum += e.weight;
    }
    return sum;
  }

  // Basic modularity quality measure for a cluster.
  // in_w:  weight sum of edges within cluster, counted per node, i.e. 2x.
  // tot_w: weight sum of edges of all nodes in cluster, counted per node.
  inline double Modularity(uint32_t in_w, uint32_t tot_w) const {
    const double in_part = in_w / two_m_;
    const double tot_part = tot_w / two_m_;
    return in_part - resolution_ * tot_part * tot_part;
  }

  // Quality (aka as modularity) of a cluster C is defined as
  //   W-in/2m - (W-tot/2m)^2
  // W-in:  Sum of edge weights within cluster, counted per node, i.e. 2x.
  // W-tot: Sum of edge weights, counted per node, i.e. partially 2x.
  // m:     Sum of edge weights in graph.
  double ClusterQ(uint32_t cluster_pos) const {
    CHECK_LT_S(cluster_pos, clusters.size());
    const LouvainCluster& c = clusters.at(cluster_pos);
    return Modularity(c.w_inside_edges, c.w_tot_edges);
  }

  double ClusterQ(const LouvainCluster& c) const {
    return Modularity(c.w_inside_edges, c.w_tot_edges);
  }

  // Quality (aka as modularity) of an isolated node is defined as
  //   0 - (W-tot/2m)^2
  // W-tot: Sum of edge weights.
  // m:     Sum of edge weights in graph.
  double NodeQ(uint32_t node_pos) const {
    CHECK_LT_S(node_pos, nodes.size());
    const LouvainNode& n = nodes.at(node_pos);
    return Modularity(n.w_self, n.w_tot);
  }

  double NodeQ(const LouvainNode& n) const {
    return Modularity(n.w_self, n.w_tot);
  }

  // Cluster quality when a node is added are removed. For an added node, pass
  // positive w_in and w_tot, for a removed node, pass negative w_in and
  // w_tot.
  double ModifiedClusterQ(uint32_t cluster_pos, int32_t w_in,
                          int32_t w_tot) const {
    CHECK_LT_S(cluster_pos, clusters.size());
    const LouvainCluster& c = clusters.at(cluster_pos);
    return Modularity(static_cast<int32_t>(c.w_inside_edges) + w_in,
                      static_cast<int32_t>(c.w_tot_edges) + w_tot);
  }

  // Compute the delta when removing node 'node_pos' from its cluster.
  double DeltaQualityRemove(uint32_t node_pos) {
    const LouvainNode& n = nodes.at(node_pos);
    uint32_t node_w_in =
        2 * SumWeightsToCluster(node_pos, n.cluster_pos) + n.w_self;
    return ModifiedClusterQ(n.cluster_pos, -static_cast<int32_t>(node_w_in),
                            -static_cast<int32_t>(n.w_tot)) +
           NodeQ(node_pos) - ClusterQ(n.cluster_pos);
  }

  // Compute the delta when adding singular node node_pos to the cluster of
  // node 'new_node_pos'.
  double DeltaQualityAdd(uint32_t node_pos, uint32_t new_node_pos) const {
    CHECK_NE_S(node_pos, new_node_pos);
    const LouvainNode& n = nodes.at(node_pos);
    const LouvainNode& new_n = nodes.at(new_node_pos);
    uint32_t node_w_in =
        n.w_self + 2 * SumWeightsToCluster(node_pos, new_n.cluster_pos);
    return ModifiedClusterQ(new_n.cluster_pos, node_w_in, n.w_tot) -
           NodeQ(node_pos) - ClusterQ(new_n.cluster_pos);
  }

  // Move node 'node_pos' from its cluster and add it to cluster of node
  // 'other_node_pos'.
  void MoveToNewCluster(uint32_t node_pos, uint32_t other_node_pos) {
    LouvainNode& n = nodes.at(node_pos);
    LouvainNode& other_node = nodes.at(other_node_pos);
    if (n.cluster_pos == other_node.cluster_pos) return;
    // Remove from existing cluster.
    LouvainCluster& old_c = clusters.at(n.cluster_pos);
    old_c.w_inside_edges -= 2 * SumWeightsToCluster(node_pos, n.cluster_pos);
    old_c.w_inside_edges -= n.w_self;
    old_c.w_tot_edges -= n.w_tot;
    old_c.num_nodes -= 1;
    if (old_c.num_nodes == 0) empty_clusters_++;
    // Add n to new cluster.
    n.cluster_pos = other_node.cluster_pos;
    LouvainCluster& new_c = clusters.at(n.cluster_pos);
    new_c.w_inside_edges +=
        n.w_self + 2 * SumWeightsToCluster(node_pos, n.cluster_pos);
    new_c.w_tot_edges += n.w_tot;
    new_c.num_nodes += 1;
  }

  bool TryMove(uint32_t node_pos) {
    // Iterate over edges and try to add node to the cluster of the target
    // node. Store the best quality improvement together with the target node.
    // If in the end there is a positive quality improvement then apply it.
    // The quality delta is calculated in two steps:
    //   1) Remove node from current cluster -> delta_remove.
    //   2) Add node to new cluster -> delta_add.
    // delta = delta_remove + delta_add.
    LouvainNode& n = nodes.at(node_pos);
    uint32_t best_edge = edges.size();  // doesn't exist.
    double best_delta = 0.0;
    for (uint32_t i = n.edge_start; i < n.edge_start + n.num_edges; ++i) {
      const LouvainEdge& e = edges.at(i);
      const LouvainNode& other_node = nodes.at(e.other_node_pos);
      if (n.cluster_pos != other_node.cluster_pos) {
        // Detect target clusters that show up more than once.
        const double delta = DeltaQualityRemove(node_pos) +
                             DeltaQualityAdd(node_pos, e.other_node_pos);
        if (delta > best_delta) {
          best_delta = delta;
          best_edge = i;
        }
      }
    }
    if (best_delta > 0.000000001) {
      MoveToNewCluster(node_pos, edges.at(best_edge).other_node_pos);
      return true;
    }
    return false;
  }

  // Iterate once over the nodes array and return the number of movements that
  // were made.
  uint32_t Step() {
    uint32_t moves = 0;
    for (size_t node_pos = 0; node_pos < nodes.size(); ++node_pos) {
      moves += TryMove(node_pos) ? 1 : 0;
    }
    return moves;
  }
};

// Find the cluster of a node at the input level by climbing up the clusters
// level-by-level to the top level.
inline uint32_t FindFinalCluster(
    const std::vector<std::unique_ptr<LouvainGraph>>& gvec, uint32_t node_pos) {
  for (const auto& lg : gvec) {
    const LouvainNode& n = lg->nodes.at(node_pos);
    node_pos = n.cluster_pos;  // cluster_pos is node_pos one level up.
  }
  return node_pos;
}

// Remove empty clusters and adjust cluster_pos stored in nodes to reflect the
// new position.
// With this, the clusters on level X map 1:1 to nodes at level X+1.
inline void RemoveEmptyClusters(LouvainGraph* lg) {
  uint32_t write_pos = 0;
  std::vector<uint32_t> old_to_new(lg->clusters.size(), INFU32);
  for (size_t pos = 0; pos < lg->clusters.size(); ++pos) {
    if (lg->clusters.at(pos).num_nodes > 0) {
      old_to_new[pos] = write_pos;
      if (write_pos != pos) {
        CHECK_GT_S(pos, write_pos);
        lg->clusters.at(write_pos) = lg->clusters.at(pos);
      }
      write_pos += 1;
    }
  }
  if (write_pos < lg->clusters.size()) {
    lg->clusters.resize(write_pos);
    lg->clusters.shrink_to_fit();
  }
  // Fix cluster_pos in nodes.
  for (LouvainNode& n : lg->nodes) {
    CHECK_LT_S(old_to_new.at(n.cluster_pos), lg->clusters.size());
    n.cluster_pos = old_to_new.at(n.cluster_pos);
  }
}

// From a given Louvain graph with clusters, create a contracted graph with
// the input clusters as nodes. The new nodes have the same in-vector positions
// as the source clusters. Check-fails if there are empty clusters, i.e. call
// RemoveEmptyClusters() before calling this function.
inline void CreateClusterGraph(const LouvainGraph& input,
                               LouvainGraph* result) {
  LOG_S(INFO) << absl::StrFormat(
      "Cluster graph with nodes:%u edges:%u clusters:%u", input.nodes.size(),
      input.edges.size(), input.clusters.size());
  // BTreeKey is a pair of cluster positions, e.g. an edge in the cluster
  // graph we're going to build.
  using BTreeKey = std::pair<uint32_t, uint32_t>;
  // Maps a cluster edge to its aggregated weight.
  absl::btree_map<BTreeKey, uint32_t> cluster_edge_to_weight;

  // Fill 'cluster_edge_to_weight'.
  for (const auto& n0 : input.nodes) {
    for (uint32_t e_pos = n0.edge_start; e_pos < n0.edge_start + n0.num_edges;
         ++e_pos) {
      const LouvainEdge& e = input.edges.at(e_pos);
      const LouvainNode n1 = input.nodes.at(e.other_node_pos);
      if (n0.cluster_pos != n1.cluster_pos) {
        cluster_edge_to_weight[std::make_pair(n0.cluster_pos,
                                              n1.cluster_pos)] += e.weight;
      }
    }
  }

  auto map_iter = cluster_edge_to_weight.cbegin();
  // Build cluster graph using parallel iteration on clusters and
  // cluster_edge_to_weight.
  for (uint32_t cluster_pos = 0; cluster_pos < input.clusters.size();
       ++cluster_pos) {
    const LouvainCluster& c = input.clusters.at(cluster_pos);
    CHECK_GT_S(c.num_nodes, 0);
    result->AddNode(cluster_pos, /*back_ref=*/cluster_pos, c.w_inside_edges);
    if (map_iter != cluster_edge_to_weight.end()) {
      BTreeKey key = map_iter->first;
      CHECK_GE_S(key.first, cluster_pos) << "something is terribly wrong";
      while (key.first == cluster_pos) {
        if (key.second != key.first) {
          result->AddEdge(key.second, /*weight=*/map_iter->second);
        }
        ++map_iter;
        if (map_iter == cluster_edge_to_weight.end()) {
          break;
        }
        key = map_iter->first;
      }
    }
  }
  LOG_S(INFO) << absl::StrFormat("Cluster result nodes:%u edges:%u clusters:%u",
                                 result->nodes.size(), result->edges.size(),
                                 result->clusters.size());
}

}  // namespace louvain
