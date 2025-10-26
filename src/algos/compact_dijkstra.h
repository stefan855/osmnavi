// Compute all shortest paths between a set of nodes, for instance between
// border nodes in a cluster, using a CompactGraph.

#pragma once

#include <algorithm>
#include <fstream>
#include <memory>
#include <queue>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "algos/routing_defs.h"
#include "algos/routing_metric.h"
#include "base/thread_pool.h"
#include "base/util.h"
#include "graph/compact_graph.h"
#include "graph/graph_def.h"

namespace compact_dijkstra {

// This exists once per 'node_idx'.
struct VisitedNode {
  std::uint32_t min_weight;       // The minimal weight seen so far.
  std::uint32_t done : 1;         // 1 <=> node has been finalized.
  std::uint32_t from_v_idx : 31;  // Parent node.
};

// This might exist multiple times for each node, when a node gets
// reinserted into the priority queue with a lower priority.
struct QueuedNode {
  std::uint32_t weight;
  std::uint32_t visited_node_idx;  // Index into visited_nodes vector.
};

struct MetricCmp {
  bool operator()(const QueuedNode& left, const QueuedNode& right) const {
    return left.weight > right.weight;
  }
};

// Execute node based single source Dijkstra (from start node to *all* nodes).
// Returns a vector with VisitedNode entries for every node in the graph.
//
// If 'spanning_tree_nodes' is not nullptr, then every node that is marked
// done is added to the spanning_tree_nodes vector. Therefore, every node in
// this vector is the descendant of some node at an earlier position in the
// same vector, except for the root node at position 0. For example, this is
// useful to visit all nodes and edges bottom up or top down.
inline std::vector<VisitedNode> SingleSourceDijkstra(
    const CompactGraph& cg, std::uint32_t start_idx,
    std::vector<uint32_t>* spanning_tree_nodes = nullptr) {
  if (spanning_tree_nodes != nullptr) {
    spanning_tree_nodes->reserve(cg.num_nodes());
  }
  CHECK_LT_S(cg.num_nodes(), 1 << 31) << "currently not supported";
  std::vector<VisitedNode> visited_nodes(
      cg.num_nodes(), {.min_weight = INFU32, .done = 0, .from_v_idx = INFU31});
  std::priority_queue<QueuedNode, std::vector<QueuedNode>, MetricCmp> pq;
  const std::vector<uint32_t>& edges_start = cg.edges_start();
  const std::vector<CompactGraph::PartialEdge>& edges = cg.edges();

  pq.emplace(0, start_idx);
  visited_nodes.at(start_idx).min_weight = 0;
  int count = 0;
  size_t max_queue_size = 0;

  while (!pq.empty()) {
    max_queue_size = std::max(max_queue_size, pq.size());
    ++count;
    // Remove the minimal node from the priority queue.
    const QueuedNode qnode = pq.top();
    pq.pop();
    VisitedNode& vnode = visited_nodes.at(qnode.visited_node_idx);
    if (vnode.done == 1) {
      continue;  // "old" entry in priority queue.
    }
    vnode.done = 1;
    if (spanning_tree_nodes != nullptr) {
      spanning_tree_nodes->push_back(qnode.visited_node_idx);
    }

    if (qnode.weight != vnode.min_weight) {
      ABORT_S() << absl::StrFormat(
          "qnode.weight:%u != vnode.min_weight:%u, vnode.done:%d", qnode.weight,
          vnode.min_weight, vnode.done);
    }

    // Search neighbours.
    for (size_t i = edges_start.at(qnode.visited_node_idx);
         i < edges_start.at(qnode.visited_node_idx + 1); ++i) {
      const CompactGraph::PartialEdge e = edges.at(i);
      const std::uint32_t new_weight = vnode.min_weight + e.weight;
      VisitedNode& other = visited_nodes.at(e.to_c_idx);
      if (!other.done && new_weight < other.min_weight) {
        other.min_weight = new_weight;
        other.from_v_idx = qnode.visited_node_idx;
        pq.emplace(new_weight, e.to_c_idx);
      }
    }
  }

  LOG_S(INFO) << absl::StrFormat("SingleSourceDijkstra: #vis:%llu #maxq:%llu",
                                 visited_nodes.size(), max_queue_size);
  return visited_nodes;
}

// Get the weight for each node, i.e. the length of the shortest way to this
// node from the vector of VisitedNodes. If the node was not reached then the
// weight is INFU32.
inline std::vector<uint32_t> GetNodeWeightsFromVisitedNodes(
    const std::vector<VisitedNode>& vis) {
  std::vector<uint32_t> weights;
  weights.reserve(vis.size());
  for (const VisitedNode& vn : vis) {
    weights.push_back(vn.min_weight);
  }
  return weights;
}

}  // namespace compact_dijkstra
