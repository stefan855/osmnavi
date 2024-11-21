// Compute all shortest paths between border nodes in a cluster.

#pragma once

#include <fstream>
#include <memory>
#include <queue>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "algos/dijkstra.h"
#include "algos/routing_metric.h"
#include "base/thread_pool.h"
#include "base/util.h"
#include "graph/compact_graph.h"
#include "graph/graph_def.h"

namespace cluster_all_paths {

// Visit all nodes in a cluster using a BFS, starting from the border nodes.
// Assigns a fresh (and small) serial id=0..N-1 to each node. The border nodes
// are mapped directly to indices 0..cluster.border_nodes.size()-1.
//
// Returns the number of nodes in the cluster in '*num_nodes' and the edges
// between cluster nodes, each with the value computed by 'metric' as its
// 'weight'.
void CollectClusterEdges(
    const Graph& g, const RoutingMetric& metric, VEHICLE vt,
    const GCluster& cluster, uint32_t* num_nodes,
    std::vector<CompactDirectedGraph::FullEdge>* full_edges) {
  // Map from node index in g.nodes to the node index in the mini graph.
  absl::flat_hash_map<uint32_t, uint32_t> nodemap;
  // FIFO queue for bfs, containing node indices in g.nodes.
  std::queue<uint32_t> q;

  const uint32_t wsa_idx_forw = RAinWSAIndex(vt, DIR_FORWARD);
  const uint32_t wsa_idx_backw = RAinWSAIndex(vt, DIR_BACKWARD);

  // Preallocate ids for all border nodes.
  for (uint32_t pos = 0; pos < cluster.border_nodes.size(); ++pos) {
    uint32_t node_idx = cluster.border_nodes.at(pos);
    nodemap[node_idx] = pos;
    q.push(node_idx);
  }

  // Do a bfs, limited to the nodes belonging to the cluster.
  uint32_t check_idx = 0;
  while (!q.empty()) {
    uint32_t node_idx = q.front();
    q.pop();
    const GNode& node = g.nodes.at(node_idx);
    CHECK_EQ_S(node.cluster_id, cluster.cluster_id);
    // By construction, this number is strictly increasing, i.e. +1 in every
    // loop.
    uint32_t mini_idx = nodemap[node_idx];
    CHECK_EQ_S(mini_idx, check_idx);
    check_idx++;

    // Examine neighbours.
    for (size_t i = 0; i < node.num_edges_out; ++i) {
      const GEdge& edge = node.edges[i];
      const GNode& other = g.nodes.at(edge.other_node_idx);
      if (other.cluster_id != cluster.cluster_id || edge.bridge ||
          edge.other_node_idx == node_idx) {
        continue;
      }

      // Check if vt is routable on this edge.
      const WaySharedAttrs& wsa = GetWSA(g, edge.way_idx);
      if (!RoutableAccess(GetRAFromWSA(wsa, vt,
                                       edge.contra_way == DIR_FORWARD
                                           ? DIR_FORWARD
                                           : DIR_BACKWARD)
                              .access)) {
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
          {mini_idx, other_idx, metric.Compute(wsa, vt, edge)});
    }
  }
  // TODO: nodemap.size() is sometimes smaller than cluster.num_nodes.
  // Investigate why this happens. Maybe some nodes in the cluster are not
  // reachable when using the directed graph - clustering is done on the
  // undirected graph. CHECK_EQ_S(nodemap.size(), cluster.num_nodes);
  *num_nodes = nodemap.size();
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

std::vector<uint32_t> GetBorderRoutes(const CompactDirectedGraph& cg,
                                      uint32_t num_border_nodes,
                                      std::uint32_t start_idx) {
  // LOG_S(INFO) << "Start routing from " << start_idx << " to "
  //             << num_border_nodes << " border nodes";

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
        // if (!other.done &&
        //     (other.min_weight == 0 || new_weight < other.min_weight)) {
        /*
        LOG_S(INFO) << "i=" << i << " idx=" << qnode.visited_node_idx
                    << " o_idx=" << e.to_idx << " new_m=" << new_weight
                    << " o_mm=" << other.min_weight;
                    */
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

}  // namespace cluster_all_paths

// Compute all shortest paths between border nodes in a cluster. The results
// are stored in 'cluster.distances'.
void ComputeShortestClusterPaths(const Graph& g, const RoutingMetric& metric,
                                 VEHICLE vt, GCluster* cluster) {
  if (cluster->border_nodes.empty()) {
    return;
  }

  // Construct a minimal graph with all necessary information.
  uint32_t num_nodes = 0;
  std::vector<CompactDirectedGraph::FullEdge> full_edges;
  cluster_all_paths::CollectClusterEdges(g, metric, vt, *cluster, &num_nodes,
                                         &full_edges);
  const auto prev_edges_size = full_edges.size();
  cluster_all_paths::SortAndCleanupEdges(&full_edges);
  LOG_S(INFO) << absl::StrFormat(
      "Cluster:%u removed %u dups from edges array size:%u",
      cluster->cluster_id, prev_edges_size - full_edges.size(),
      prev_edges_size);
  const CompactDirectedGraph cg(num_nodes, full_edges);
  cg.LogStats();
  for (size_t border_node = 0; border_node < cluster->border_nodes.size();
       ++border_node) {
    std::vector<uint32_t> dist = cluster_all_paths::GetBorderRoutes(
        cg, cluster->border_nodes.size(), border_node);
    CHECK_EQ_S(dist.size(), cluster->border_nodes.size());
    cluster->distances.push_back(dist);
    // cluster_all_paths::CompareRoutes(g, *cluster, metric, distances);
  };
  CHECK_EQ_S(cluster->distances.size(), cluster->border_nodes.size());
}
