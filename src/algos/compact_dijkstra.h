// Compute all shortest paths between a set of nodes, for instance between
// border nodes in a cluster, using a CompactDirectedGraph.

#pragma once

#include <fstream>
#include <memory>
#include <queue>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "algos/dijkstra.h"
#include "algos/routing_defs.h"
#include "algos/routing_metric.h"
#include "base/thread_pool.h"
#include "base/util.h"
#include "graph/compact_graph.h"
#include "graph/graph_def.h"

namespace compact_dijkstra {

// Visit all reachable nodes using a BFS, starting from the nodes in
// 'start_nodes'. Assigns a fresh (and small) serial id=0..N-1 to each node. The
// start nodes are mapped to indices [0..start_node.size()-1].
//
// Returns the number of visited nodes in 'num_nodes' and the edges between
// visited nodes in 'full_edges'.
//
// If 'node_refs' is not nullptr, then it will contain the original node index
// in graph:nodes for every of the 'num_nodes' nodes which are indexed by
// [0..num_nodes-1]..
void CollectEdges(const Graph& g, const RoutingMetric& metric,
                  const RoutingOptions& opt,
                  std::vector<std::uint32_t>& start_nodes, uint32_t* num_nodes,
                  std::vector<CompactDirectedGraph::FullEdge>* full_edges,
                  std::vector<std::uint32_t>* node_refs = nullptr) {
  // Map from node index in g.nodes to the node index in the compact graph.
  absl::flat_hash_map<uint32_t, uint32_t> nodemap;
  // FIFO queue for bfs, containing node indices in g.nodes.
  std::queue<uint32_t> q;

  // Preallocate ids for all start nodes.
  for (uint32_t pos = 0; pos < start_nodes.size(); ++pos) {
    uint32_t node_idx = start_nodes.at(pos);
    nodemap[node_idx] = pos;
    q.push(node_idx);
  }

  // Do a bfs, limited to the nodes belonging to the cluster.
  uint32_t check_idx = 0;
  while (!q.empty()) {
    uint32_t node_idx = q.front();
    q.pop();
    const GNode& node = g.nodes.at(node_idx);
    CHECK_EQ_S(node.cluster_id, opt.cluster_id);
    // By construction, this number is strictly increasing, i.e. +1 in every
    // loop.
    uint32_t mini_idx = nodemap[node_idx];
    CHECK_EQ_S(mini_idx, check_idx);
    check_idx++;

    // Examine neighbours.
    for (size_t i = 0; i < node.num_edges_out; ++i) {
      const GEdge& edge = node.edges[i];
      const WaySharedAttrs& wsa = GetWSA(g, edge.way_idx);
      if (RoutingRejectEdge(g, opt, node, node_idx, edge, wsa,
                            EDGE_DIR(edge))) {
        continue;
      }

      // The other node is part of the same cluster, so use it.
      uint32_t other_idx;
      auto iter = nodemap.find(edge.other_node_idx);
      if (iter == nodemap.end()) {
        // The node hasn't been seen before. This means we need to allocate a
        // new id, and we need to enqueue the node because it hasn't been
        // handled yet.
        other_idx = nodemap.size();
        nodemap[edge.other_node_idx] = nodemap.size();
        q.push(edge.other_node_idx);
      } else {
        other_idx = iter->second;
      }

      full_edges->push_back(
          {mini_idx, other_idx,
           metric.Compute(wsa, opt.vt, EDGE_DIR(edge), edge)});
    }
  }
  // TODO: nodemap.size() is sometimes smaller than cluster.num_nodes.
  // Investigate why this happens. Maybe some nodes in the cluster are not
  // reachable when using the directed graph - clustering is done on the
  // undirected graph. CHECK_EQ_S(nodemap.size(), cluster.num_nodes);
  *num_nodes = nodemap.size();
  if (node_refs != nullptr) {
    node_refs->assign(*num_nodes, INF32);
    for (auto [graph_idx, compact_idx] : nodemap) {
      node_refs->at(compact_idx) = graph_idx;
    }
    for (size_t i = 0; i < *num_nodes; ++i) {
      CHECK_NE_S(node_refs->at(i), INF32);
    }
  }
}

// Sort the edges by ascending order (from_idx, to_idx, weight) and remove
// duplicates (from, to) keeping the one with the lowest weight.
void SortAndCleanupEdges(
    std::vector<CompactDirectedGraph::FullEdge>* full_edges) {
  std::sort(full_edges->begin(), full_edges->end());
  // Remove dups.
  auto last =
      std::unique(full_edges->begin(), full_edges->end(),
                  [](const CompactDirectedGraph::FullEdge& a,
                     const CompactDirectedGraph::FullEdge& b) {
                    return a.to_idx == b.to_idx && a.from_idx == b.from_idx;
                  });
  if (last != full_edges->end()) {
    full_edges->erase(last, full_edges->end());
  }
}

// This exists once per 'node_idx'.
struct VisitedNode {
  std::uint32_t min_weight;       // The minimal weight seen so far.
  std::uint32_t done : 1;         // 1 <=> node has been finalized.
  std::uint32_t from_v_idx : 31;  // 1 <=> node has been finalized.
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

// Execute single source Dijkstra (for a start node to *all* nodes).
// Returns the vector with VisitedNode entries for every node in the graph.
std::vector<VisitedNode> SingleSourceDijkstra(const CompactDirectedGraph& cg,
                                              std::uint32_t start_idx) {
  CHECK_LT_S(cg.num_nodes(), 1 << 31) << "currently not supported";
  std::vector<VisitedNode> visited_nodes(cg.num_nodes(), {INFU32, 0, 0});
  std::priority_queue<QueuedNode, std::vector<QueuedNode>, MetricCmp> pq;
  const std::vector<uint32_t>& edges_start = cg.edges_start();
  const std::vector<CompactDirectedGraph::PartialEdge>& edges = cg.edges();

  pq.emplace(0, start_idx);
  visited_nodes.at(start_idx).min_weight = 0;
  int count = 0;
  while (!pq.empty()) {
    ++count;
    // Remove the minimal node from the priority queue.
    const QueuedNode qnode = pq.top();
    pq.pop();
    VisitedNode& vnode = visited_nodes.at(qnode.visited_node_idx);
    if (vnode.done == 1) {
      continue;  // "old" entry in priority queue.
    }

    if (qnode.weight != vnode.min_weight) {
      ABORT_S() << absl::StrFormat(
          "qnode.weight:%u != vnode.min_weight:%u, vnode.done:%d", qnode.weight,
          vnode.min_weight, vnode.done);
    }
    vnode.done = 1;

    // Search neighbours.
    for (size_t i = edges_start.at(qnode.visited_node_idx);
         i < edges_start.at(qnode.visited_node_idx + 1); ++i) {
      const CompactDirectedGraph::PartialEdge e = edges.at(i);
      const std::uint32_t new_weight = vnode.min_weight + e.weight;
      VisitedNode& other = visited_nodes.at(e.to_idx);
      if (!other.done && new_weight < other.min_weight) {
        other.min_weight = new_weight;
        other.from_v_idx = qnode.visited_node_idx;
        pq.emplace(new_weight, e.to_idx);
      }
    }
  }

  return visited_nodes;
}

#if 0
std::vector<uint32_t> GetBorderRoutes(const CompactDirectedGraph& cg,
                                      std::uint32_t start_idx,
                                      std::uint32_t num_border_nodes) {
  CHECK_LT_S(cg.num_nodes(), 1 << 31);
  std::vector<VisitedNode> visited_nodes(cg.num_nodes(), {INFU32, 0, 0});
  std::priority_queue<QueuedNode, std::vector<QueuedNode>, MetricCmp> pq;
  const std::vector<uint32_t>& edges_start = cg.edges_start();
  const std::vector<CompactDirectedGraph::PartialEdge>& edges = cg.edges();

  pq.emplace(0, start_idx);
  visited_nodes.at(start_idx).min_weight = 0;
  int count = 0;
  while (!pq.empty()) {
    ++count;
    // Remove the minimal node from the priority queue.
    const QueuedNode qnode = pq.top();
    pq.pop();
    VisitedNode& vnode = visited_nodes.at(qnode.visited_node_idx);
    if (vnode.done == 1) {
      continue;  // "old" entry in priority queue.
    }

    if (qnode.weight != vnode.min_weight) {
      ABORT_S() << absl::StrFormat(
          "qnode.weight:%u != vnode.min_weight:%u, vnode.done:%d", qnode.weight,
          vnode.min_weight, vnode.done);
    }
    vnode.done = 1;

    // Search neighbours.
    for (size_t i = edges_start.at(qnode.visited_node_idx);
         i < edges_start.at(qnode.visited_node_idx + 1); ++i) {
      const CompactDirectedGraph::PartialEdge e = edges.at(i);
      const std::uint32_t new_weight = vnode.min_weight + e.weight;
      VisitedNode& other = visited_nodes.at(e.to_idx);
      if (!other.done && new_weight < other.min_weight) {
        other.min_weight = new_weight;
        other.from_v_idx = qnode.visited_node_idx;
        pq.emplace(new_weight, e.to_idx);
      }
    }
  }
  // LOG_S(INFO) << "Finished loops:" << count;

  std::vector<uint32_t> res;
  for (size_t i = 0; i < num_border_nodes; ++i) {
    res.push_back(visited_nodes.at(i).min_weight);
  }
  return res;
}
#endif

}  // namespace compact_dijkstra

// Compute all shortest paths between border nodes in a cluster. The results
// are stored in 'cluster.distances'.
void ComputeShortestClusterPaths(const Graph& g, const RoutingMetric& metric,
                                 VEHICLE vt, GCluster* cluster) {
  CHECK_S(cluster->distances.empty());
  const size_t num_border_nodes = cluster->border_nodes.size();
  if (num_border_nodes == 0) {
    return;  // Nothing to compute, cluster is isolated.
  }

  // Construct a minimal graph with all necessary information.
  uint32_t num_nodes = 0;
  std::vector<CompactDirectedGraph::FullEdge> full_edges;
  compact_dijkstra::CollectEdges(g, metric,
                                 {.vt = vt,
                                  .restrict_to_cluster = true,
                                  .cluster_id = cluster->cluster_id},
                                 cluster->border_nodes, &num_nodes,
                                 &full_edges);
  const auto prev_edges_size = full_edges.size();
  compact_dijkstra::SortAndCleanupEdges(&full_edges);
  const CompactDirectedGraph cg(num_nodes, full_edges);
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
