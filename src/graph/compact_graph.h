// Compute all shortest paths between border nodes in a cluster.

#pragma once

#include <vector>

#include "base/util.h"
#include "graph/graph_def.h"

// This is a compacted view on the nodes and edges of a graph, with a
// precomputed weight for every edge. The nodes in the graph have ids
// 0..num_nodes()-1.
class CompactDirectedGraph {
 public:
  struct FullEdge {
    uint32_t from_c_idx;
    uint32_t to_c_idx;
    uint32_t weight;
    // Edge has restricted car_label (LABEL_RESTRICTED or
    // LABEL_RESTRICTED_SECONDARY) set.
    uint8_t restricted_access : 1;
  };

  struct PartialEdge {
    uint32_t to_c_idx;
    uint32_t weight;
    // Edge has restricted car_label (LABEL_RESTRICTED or
    // LABEL_RESTRICTED_SECONDARY) set.
    uint8_t restricted_access : 1;
  };

  // Create a graph with the given number of nodes and edges in 'full_edges'.
  // 'full_edges' must be sorted non-decreasing by (from_c_idx, to_c_idx).
  //
  // Note: this file implements operator "<" for full edges, i.e. you can sort
  // the edges with std::sort(full_edges.begin(), full_edges.end());
  CompactDirectedGraph(uint32_t num_nodes,
                       const std::vector<FullEdge>& full_edges)
      : num_nodes_(num_nodes) {
    BuildGraph(full_edges);
  }

  // Return the number of nodes in the graph. Ids are 0..num_nodes()-1.
  // The ids 0..cluster.border_nodes.size()-1 represent the border nodes.
  uint32_t num_nodes() const { return num_nodes_; }

  // For every node, contains the start position of its edges in the edges()
  // vector. The last element does not correspond to a node, it has the value
  // edges().size().
  const std::vector<uint32_t>& edges_start() const { return edges_start_; }

  // Sorted edge vector contains all edges of all nodes. The edges of node k are
  // in positions edges_start()[k]..edges_start()[k+1] - 1.
  const std::vector<PartialEdge>& edges() const { return edges_; }

  // Return the position of the edge (from_node, to_node) in edges().
  // Returns -1 in case the edge does not exist.
  const int64_t FindEdge(uint32_t from_node, uint32_t to_node) const {
    for (size_t i = edges_start_.at(from_node);
         i < edges_start_.at(from_node + 1); ++i) {
      if (edges_.at(i).to_c_idx == to_node) {
        return i;
      }
    }
    return -1;
  }

  // Log stats about the graph.
  void LogStats() const {
    uint32_t min_weight = std::numeric_limits<uint32_t>::max();
    uint32_t max_weight = 0;
    uint32_t restricted_access = 0;
    for (const PartialEdge& e : edges_) {
      if (e.weight < min_weight) min_weight = e.weight;
      if (e.weight > max_weight) max_weight = e.weight;
      restricted_access += e.restricted_access;
    }
    LOG_S(INFO) << absl::StrFormat(
        "CompactGraph #nodes:%u #edges:%u restricted:%u mem:%u weight=[%u,%u]",
        edges_start_.size() - 1, edges_.size(), restricted_access,
        edges_start_.size() * sizeof(uint32_t) +
            edges_.size() * sizeof(PartialEdge) + sizeof(CompactDirectedGraph),
        min_weight, max_weight);
  }

 private:
  // Nodes are numbered [0..num_nodes). For each one, we want to store
  // the start of its edges in the edge array.
  // In the end we add one more element with value full_edges.size(), to allow
  // easy iteration.
  void BuildGraph(const std::vector<FullEdge>& full_edges) {
    CHECK_S(edges_start_.empty());
    edges_start_.reserve(num_nodes_ + 1);
    size_t current_start = 0;
    for (size_t c_idx = 0; c_idx < num_nodes_; ++c_idx) {
      edges_start_.push_back(current_start);
      // LOG_S(INFO) << absl::StrFormat("Cluster:%u node:%u edge_start:%u",
      //                                cluster.cluster_id, c_idx,
      //                                current_start);
      while (current_start < full_edges.size() &&
             full_edges.at(current_start).from_c_idx == c_idx) {
        current_start++;
      }
    }
    CHECK_EQ_S(current_start, full_edges.size());
    edges_start_.push_back(full_edges.size());  // Sentinel.

    edges_.reserve(full_edges.size());
    uint32_t full_restricted = 0;
    uint32_t partial_restricted = 0;
    for (const FullEdge& e : full_edges) {
      // LOG_S(INFO) << absl::StrFormat("Cluster:%u pos:%u from:%u to:%u w:%d",
      //                                cluster.cluster_id, edges_.size(),
      //                                e.from_c_idx, e.to_c_idx,
      //                                e.weight);
      edges_.push_back({e.to_c_idx, e.weight, e.restricted_access});
      full_restricted += e.restricted_access;
      partial_restricted += edges_.back().restricted_access;
    }
    CHECK_EQ_S(num_nodes_ + 1, edges_start_.size());
  }

  // Number of nodes in the graph.
  // The border nodes of the cluster are located at the beginning and have ids
  // 0..cluster.border_nodes.size()-1.
  const uint32_t num_nodes_;
  // Start position of the edges of edge node. Has num_nodes_+1 elements, the
  // last element is a sentinel. The edges of node i are in the range
  // [edges_start_[i], edges_start_[i+1]).
  std::vector<uint32_t> edges_start_;
  std::vector<PartialEdge> edges_;
};

inline bool operator<(const CompactDirectedGraph::FullEdge& a,
                      const CompactDirectedGraph::FullEdge& b) {
  return std::tie(a.from_c_idx, a.to_c_idx, a.restricted_access, a.weight) <
         std::tie(b.from_c_idx, b.to_c_idx, b.restricted_access, b.weight);
}
