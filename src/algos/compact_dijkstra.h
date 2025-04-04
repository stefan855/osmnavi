// Compute all shortest paths between a set of nodes, for instance between
// border nodes in a cluster, using a CompactDirectedGraph.

#pragma once

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

class SingleSourceEdgeDijkstra {
 public:
  // This exists once per 'edge'.
  struct VisitedEdge {
    std::uint32_t min_weight;  // The minimal weight seen so far.
    std::uint32_t from_idx;    // Previous edge.
    std::uint32_t next_secondary;
    uint32_t ctrs_id : 30;
    std::uint32_t in_target_restricted_access_area : 1;
    std::uint32_t done : 1;  // 1 <=> node has been finalized.
  };

  // If more than one edge-key occurs for an edge, then secondary edges are
  // added to the list rooted at 'VisitedEdge::next_secondary'.
  struct SecondaryVisitedEdge {
    VisitedEdge e;
    uint32_t orig_idx;  // v_idx of the root visited edge in vis_.
  };

  struct Options {
    bool store_spanning_tree_edges = false;
    bool handle_restricted_access = false;

    // Compact indexes of nodes that are in the restricted access area connected
    // to the start node. Is ignored when handle_restricted_access is false.
    // const absl::flat_hash_set<uint32_t> restricted_access_nodes;
  };

  SingleSourceEdgeDijkstra() { ; }

  const std::vector<VisitedEdge>& GetVisitedEdges() const { return vis_; }

  const std::vector<uint32_t>& GetSpanningTreeEdges() const {
    return spanning_tree_edges_;
  }

  // Execute edge based single source Dijkstra (from start node to *all*
  // reachable nodes).
  //
  // If 'opt.store_spanning_tree_edges' is true, then every edge that is marked
  // done is added in increasing min_metric order to the spanning_tree_edges
  // vector. Every edge in this vector is the descendant of some edge at an
  // earlier position in the same vector, except for a few edges leaving the
  // start node. For example, this is useful to visit all edges bottom up or top
  // down as they were visited during single source edge based Dijkstra.
  void Route(const CompactDirectedGraph& cg, std::uint32_t start_idx,
             const Options opt) {
    CHECK_LT_S(cg.edges().size(), 1 << 31) << "currently not supported";
    Clear();
    if (opt.store_spanning_tree_edges) {
      spanning_tree_edges_.reserve(cg.edges().size());
    }
    vis_.assign(cg.edges().size(), {.min_weight = INFU32,
                                    .from_idx = INFU32,
                                    .next_secondary = INFU32,
                                    .ctrs_id = INFU30,
                                    .in_target_restricted_access_area = 0,
                                    .done = 0});
    std::priority_queue<QueuedEdge, std::vector<QueuedEdge>, MetricCmpEdge> pq;

    const std::vector<uint32_t>& edges_start = cg.edges_start();
    const std::vector<CompactDirectedGraph::PartialEdge>& edges = cg.edges();
    const auto& compact_tr_map = cg.GetCompactSimpleTurnRestrictionMap();

    // Push edges of start node into queue.
    for (size_t i = edges_start.at(start_idx);
         i < edges_start.at(start_idx + 1); ++i) {
      vis_.at(i).min_weight = edges.at(i).weight;
      pq.emplace(edges.at(i).weight, i);
    }

    int count = 0;
    while (!pq.empty()) {
      ++count;
      // Remove the minimal node from the priority queue.
      const QueuedEdge qedge = pq.top();
      pq.pop();
      VisitedEdge& prev_edge = vis_.at(qedge.ve_idx);
      if (prev_edge.done == 1) {
        continue;  // "old" entry in priority queue.
      }
      CHECK_EQ_S(qedge.weight, prev_edge.min_weight);
      prev_edge.done = 1;
      if (opt.store_spanning_tree_edges) {
        spanning_tree_edges_.push_back(qedge.ve_idx);
      }

      const CompactDirectedGraph::PartialEdge& prev_cg_edge =
          edges.at(qedge.ve_idx);
      const uint32_t start = edges_start.at(prev_cg_edge.to_c_idx);
      const uint32_t num_edges =
          edges_start.at(prev_cg_edge.to_c_idx + 1) - start;

      uint32_t allowed_offset_bits = -1;  // Set all bits
      if (prev_cg_edge.simple_turn_restriction_trigger) {
        LOG_S(INFO) << "Search simple TR trigger!";
        auto iter = compact_tr_map.find(qedge.ve_idx);
        if (iter != compact_tr_map.end()) {
          allowed_offset_bits = iter->second;
          LOG_S(INFO) << "Found simple TR trigger!";
        }
      }

      for (uint32_t off = 0; off < num_edges; ++off) {
        const uint32_t i = start + off;
        const CompactDirectedGraph::PartialEdge& e = edges.at(i);

        if (!(allowed_offset_bits & (1u << off)) ||
            (opt.handle_restricted_access &&
             prev_edge.in_target_restricted_access_area &&
             !e.restricted_access)) {
          // At least one of these holds:
          // 1) Not all edges are allowed because of a TR
          // 2) We're in the target restricted access area, not allowed to
          // transition to a free edge.
          continue;
        }

        uint32_t new_weight = prev_edge.min_weight + e.weight;
        VisitedEdge& ve = vis_.at(i);
        if (!ve.done && new_weight < ve.min_weight) {
          ve.min_weight = new_weight;
          ve.from_idx = qedge.ve_idx;
          pq.emplace(new_weight, i);
          if (opt.handle_restricted_access) {
            ve.in_target_restricted_access_area =
                prev_edge.in_target_restricted_access_area |
                (!edges.at(qedge.ve_idx).restricted_access &&
                 e.restricted_access);
          }
        }
      }
    }
  }

  // For each node in compact graph cg, find the idx of the incoming edge with
  // the smallest min_weight. The returned vector has exactly cg.num_nodes()
  // entries. If a node was not reached by any edge, then the value is INFU32.
  //
  // This may be used to get the shortest path length to each node. For node
  // 'node_c_idx', this is vis_.at(min_edge.at(node_c_idx)).min_weight.
  //
  // It also may be used to compute the traffic on the shortest paths after
  // executing SSD, because each of the min edges is the end of a shortest path
  // and as such should be initialised with traffic '1'.
  std::vector<uint32_t> GetMinEdgesAtNodes(
      const CompactDirectedGraph& cg) const {
    std::vector<uint32_t> min_edges(cg.num_nodes(), INFU32);
    const std::vector<CompactDirectedGraph::PartialEdge>& edges = cg.edges();
    for (size_t i = 0; i < edges.size(); ++i) {
      uint32_t to_c_idx = edges.at(i).to_c_idx;
      if (min_edges.at(to_c_idx) == INFU32 ||
          vis_.at(i).min_weight < vis_.at(min_edges.at(to_c_idx)).min_weight) {
        min_edges.at(to_c_idx) = i;
      }
    }
    return min_edges;
  }

  std::vector<uint32_t> GetNodeWeightsFromVisitedEdges(
      const CompactDirectedGraph& cg, uint32_t start_idx) const {
    std::vector<uint32_t> w(cg.num_nodes(), INFU32);
    for (size_t i = 0; i < cg.edges().size(); ++i) {
      uint32_t to_c_idx = cg.edges().at(i).to_c_idx;
      w.at(to_c_idx) = std::min(w.at(to_c_idx), vis_.at(i).min_weight);
    }
    w.at(start_idx) = 0;
    return w;
  }

 private:
  // This might exist multiple times for each node, when a node gets
  // reinserted into the priority queue with a lower priority.
  struct QueuedEdge {
    std::uint32_t weight;
    std::uint32_t ve_idx;  // Index into visited_edges vector.
  };

  struct MetricCmpEdge {
    bool operator()(const QueuedEdge& left, const QueuedEdge& right) const {
      return left.weight > right.weight;
    }
  };

  void Clear() {
    vis_.clear();
    secondary_vis_.clear();
    spanning_tree_edges_.clear();
  }

  std::vector<VisitedEdge> vis_;
  std::vector<SecondaryVisitedEdge> secondary_vis_;
  std::vector<uint32_t> spanning_tree_edges_;
};

// ======================================================================
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//

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
// If 'spanning_tree_nodes' is not nullptr, then every node that is marked done
// is added to the spanning_tree_nodes vector. Therefore, every node in this
// vector is the descendant of some node at an earlier position in the same
// vector, except for the root node at position 0. For example, this is useful
// to visit all nodes and edges bottom up or top down.
inline std::vector<VisitedNode> SingleSourceDijkstra(
    const CompactDirectedGraph& cg, std::uint32_t start_idx,
    std::vector<uint32_t>* spanning_tree_nodes = nullptr) {
  if (spanning_tree_nodes != nullptr) {
    spanning_tree_nodes->reserve(cg.num_nodes());
  }
  CHECK_LT_S(cg.num_nodes(), 1 << 31) << "currently not supported";
  std::vector<VisitedNode> visited_nodes(
      cg.num_nodes(), {.min_weight = INFU32, .done = 0, .from_v_idx = INFU31});
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
      VisitedNode& other = visited_nodes.at(e.to_c_idx);
      if (!other.done && new_weight < other.min_weight) {
        other.min_weight = new_weight;
        other.from_v_idx = qnode.visited_node_idx;
        pq.emplace(new_weight, e.to_c_idx);
      }
    }
  }

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
