// Compute all shortest paths between a set of nodes, for instance between
// border nodes in a cluster, using a CompactDirectedGraph.

#pragma once

#include <fstream>
#include <memory>
#include <queue>
#include <vector>

#include "absl/container/flat_hash_map.h"
// #include "algos/dijkstra.h"
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
// 'undirected_expand': If 'false', then only forward edges are followed, i.e.
// only nodes reachable by forward routing from one of the start nodes will be
// considered. If 'true', then all nodes that are connected through forward
// and/or backward routable edges are allowed.
//
// Returns the number of visited nodes in 'num_nodes' and the edges between
// visited nodes in 'full_edges'.
//
// If 'compact_nodemap' is not nullptr, then it will contain a mapping from
// indexes in graph.nodes to indexes in the compact graph [0..num_nodes-1].
inline void CollectEdgesForCompactGraph(
    const Graph& g, const RoutingMetric& metric, const RoutingOptions& opt,
    const std::vector<std::uint32_t>& start_nodes, bool undirected_expand,
    uint32_t* num_nodes,
    std::vector<CompactDirectedGraph::FullEdge>* full_edges,
    absl::flat_hash_map<uint32_t, uint32_t>* compact_nodemap = nullptr) {
  // Map from node index in g.nodes to the node index in the compact graph.
  absl::flat_hash_map<uint32_t, uint32_t> internal_nodemap;
  if (compact_nodemap == nullptr) {
    compact_nodemap = &internal_nodemap;
  } else {
    CHECK_S(compact_nodemap->empty());
  }
  // FIFO queue for bfs, containing node indices in g.nodes.
  std::queue<uint32_t> q;

  // Preallocate ids for all start nodes.
  for (uint32_t pos = 0; pos < start_nodes.size(); ++pos) {
    uint32_t node_idx = start_nodes.at(pos);
    (*compact_nodemap)[node_idx] = pos;
    q.push(node_idx);
  }

  // Do a BFS.
  uint32_t check_idx = 0;
  while (!q.empty()) {
    uint32_t node_idx = q.front();
    q.pop();
    const GNode& node = g.nodes.at(node_idx);
    // By construction, this number is strictly increasing, i.e. +1 in every
    // loop.
    uint32_t mini_idx = (*compact_nodemap)[node_idx];
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

      uint32_t other_mini_idx;
      auto iter = compact_nodemap->find(edge.other_node_idx);
      if (iter == compact_nodemap->end()) {
        // The node hasn't been seen before. This means we need to allocate a
        // new id, and we need to enqueue the node because it hasn't been
        // handled yet.
        other_mini_idx = compact_nodemap->size();
        (*compact_nodemap)[edge.other_node_idx] = compact_nodemap->size();
        q.push(edge.other_node_idx);
      } else {
        other_mini_idx = iter->second;
      }

      full_edges->push_back(
          {mini_idx, other_mini_idx,
           metric.Compute(wsa, opt.vt, EDGE_DIR(edge), edge)});
    }

    if (undirected_expand) {
      // Examine neighbours connected by backward edges and add them to the
      // queue if they haven't been seen yet.
      for (size_t i = node.num_edges_out; i < gnode_total_edges(node); ++i) {
        const GEdge& edge = node.edges[i];
        const WaySharedAttrs& wsa = GetWSA(g, edge.way_idx);
        if (RoutingRejectEdge(g, opt, node, node_idx, edge, wsa,
                              EDGE_INVERSE_DIR(edge))) {
          continue;
        }

        auto iter = compact_nodemap->find(edge.other_node_idx);
        if (iter == compact_nodemap->end()) {
          (*compact_nodemap)[edge.other_node_idx] = compact_nodemap->size();
          q.push(edge.other_node_idx);
        }
      }
    }
  }
  // TODO: compact_nodemap->size() is sometimes smaller than cluster.num_nodes.
  // Investigate why this happens. Maybe some nodes in the cluster are not
  // reachable when using the directed graph - clustering is done on the
  // undirected graph.
  *num_nodes = compact_nodemap->size();
  CHECK_LT_S(*num_nodes, 1 << 31) << "Not supported, Internal data has 31 bits";
}

inline std::vector<std::uint32_t> NodeMapToGraphNodeRefs(
    const absl::flat_hash_map<uint32_t, uint32_t>& compact_nodemap) {
  const uint32_t num_nodes = compact_nodemap.size();
  std::vector<std::uint32_t> node_refs;
  node_refs.assign(num_nodes, INFU32);
  for (auto [graph_idx, compact_idx] : compact_nodemap) {
    node_refs.at(compact_idx) = graph_idx;
  }
  for (size_t i = 0; i < num_nodes; ++i) {
    CHECK_NE_S(node_refs.at(i), INFU32);
  }
  return node_refs;
}

// Sort the edges by ascending order (from_idx, to_idx, weight) and remove
// duplicates (from, to) keeping the one with the lowest weight.
inline void SortAndCleanupEdges(
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

// Execute single source Dijkstra (from start node to *all* nodes).
// Returns a vector with VisitedNode entries for every node in the graph.
//
// If 'spanning_tree_nodes' is not nullptr, then every node that is marked done
// is added to the spanning_tree_nodes vector. Therefore, every node in this
// vector is the descendant of some node an earlier position in the same vector,
// except for the root node at position 0. For example, this is useful to visit
// all nodes and edges bottom up or top down.
inline std::vector<VisitedNode> SingleSourceDijkstra(
    const CompactDirectedGraph& cg, std::uint32_t start_idx,
    std::vector<uint32_t>* spanning_tree_nodes = nullptr) {
  CHECK_LT_S(cg.num_nodes(), 1 << 31) << "currently not supported";
  std::vector<VisitedNode> visited_nodes(
      cg.num_nodes(), {.min_weight = INFU32, .done = 0, .from_v_idx = 0});
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

}  // namespace compact_dijkstra
