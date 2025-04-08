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
  struct VisitedEdge {
    std::uint32_t min_weight;  // The minimal weight seen so far.
    std::uint32_t from_idx;    // Previous edge entry.
    std::uint32_t ctrs_id : 30;
    std::uint32_t in_target_restricted_access_area : 1;
    std::uint32_t done : 1;  // 1 <=> node has been finalized.
    std::uint32_t next;  // if != INFU32 then the next entry with different key.
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
    const size_t cg_edges_size = cg.edges().size();
    CHECK_LT_S(cg_edges_size, 1 << 31) << "currently not supported";
    Clear();
    if (opt.store_spanning_tree_edges) {
      spanning_tree_edges_.reserve(cg_edges_size);
    }
    vis_.assign(cg_edges_size, {.min_weight = INFU32,
                                .from_idx = INFU32,
                                .ctrs_id = INFU30,
                                .in_target_restricted_access_area = 0,
                                .done = 0,
                                .next = INFU32});
    std::priority_queue<QueuedEdge, std::vector<QueuedEdge>, MetricCmpEdge> pq;

    const std::vector<uint32_t>& edges_start = cg.edges_start();
    const std::vector<CompactDirectedGraph::PartialEdge>& edges = cg.edges();
    const auto& compact_tr_map = cg.GetCompactSimpleTurnRestrictionMap();

    // Push edges of start node into queue.
    for (size_t i = edges_start.at(start_idx);
         i < edges_start.at(start_idx + 1); ++i) {
      uint32_t v_idx = FindOrAllocEdge(i,
                                       /*in_target_restricted_access_area=*/0,
                                       /*ctrs_id=*/INFU30);
      CHECK_EQ_S(i, v_idx);
      vis_.at(i).min_weight = edges.at(i).weight;
      pq.emplace(edges.at(i).weight, i);
    }

    int count = 0;
    while (!pq.empty()) {
      ++count;
      // Remove the minimal node from the priority queue.
      const QueuedEdge qedge = pq.top();
      pq.pop();
      VisitedEdge& prev_v = vis_.at(qedge.ve_idx);
      if (prev_v.done == 1) {
        continue;  // "old" entry in priority queue.
      }
      CHECK_EQ_S(qedge.weight, prev_v.min_weight);
      prev_v.done = 1;
      if (opt.store_spanning_tree_edges) {
        // TODO
        CHECK_S(false);
        spanning_tree_edges_.push_back(qedge.ve_idx);
      }
      const uint32_t v_prev_base_idx = GetBaseIdx(cg_edges_size, qedge.ve_idx);
      const CompactDirectedGraph::PartialEdge& prev_cg_edge =
          edges.at(v_prev_base_idx);
      const uint32_t start = edges_start.at(prev_cg_edge.to_c_idx);
      const uint32_t num_edges =
          edges_start.at(prev_cg_edge.to_c_idx + 1) - start;

      uint32_t allowed_offset_bits = -1;  // Set all bits
      if (prev_cg_edge.simple_turn_restriction_trigger) {
        LOG_S(INFO) << "Search simple TR trigger!";
        auto iter = compact_tr_map.find(v_prev_base_idx);
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
             prev_v.in_target_restricted_access_area && !e.restricted_access)) {
          // At least one of these holds:
          // 1) Not all edges are allowed because of a TR
          // 2) We're in the target restricted access area, not allowed to
          // transition to a free edge.
          continue;
        }

        const uint32_t new_weight = prev_v.min_weight + e.weight;
        const bool in_target_raa =
            opt.handle_restricted_access &&
            (prev_v.in_target_restricted_access_area |
             (!edges.at(v_prev_base_idx).restricted_access &&
              e.restricted_access));
        const uint32_t new_v_idx = FindOrAllocEdge(i, in_target_raa,
                                                   /*ctrs_id=*/INFU30);

        VisitedEdge& ve = vis_.at(new_v_idx);
        if (!ve.done && new_weight < ve.min_weight) {
          ve.min_weight = new_weight;
          ve.from_idx = qedge.ve_idx;
          pq.emplace(new_weight, new_v_idx);
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
  // executing SSD, because each of the min edges is the end of a shortest
  // path and as such should be initialised with traffic '1'.
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
    spanning_tree_edges_.clear();
  }

  inline uint32_t FindOrAllocEdge(uint32_t v_base_idx,
                                  bool in_target_restricted_access_area,
                                  uint32_t ctrs_id) {
    VisitedEdge& v_base = vis_.at(v_base_idx);
    // Free?
    if (v_base.next == INFU32) {
      v_base.in_target_restricted_access_area =
          in_target_restricted_access_area;
      v_base.ctrs_id = ctrs_id;
      v_base.next = v_base_idx;
      return v_base_idx;
    }
    // Is base element matching the key information?
    if (v_base.in_target_restricted_access_area ==
            in_target_restricted_access_area &&
        v_base.ctrs_id == ctrs_id) {
      return v_base_idx;
    }
    // Find matching element in list.
    uint32_t v_curr_idx = v_base.next;
    while (v_curr_idx != v_base_idx) {
      const VisitedEdge& v_curr = vis_.at(v_curr_idx);
      if (v_curr.in_target_restricted_access_area ==
              in_target_restricted_access_area &&
          v_curr.ctrs_id == ctrs_id) {
        return v_curr_idx;
      }
      v_curr_idx = v_curr.next;
    }
    // Allocate new element.
    v_curr_idx = vis_.size();
    vis_.push_back(
        {.min_weight = INFU32,
         .from_idx = INFU32,
         .ctrs_id = ctrs_id,
         .in_target_restricted_access_area = in_target_restricted_access_area,
         .done = 0,
         .next = v_base.next});
    v_base.next = v_curr_idx;
    return v_curr_idx;
  }

  inline uint32_t GetBaseIdx(size_t cg_edges_size, uint32_t v_idx) const {
    if (v_idx < cg_edges_size) return v_idx;
    while (vis_.at(v_idx).next >= cg_edges_size) {
      v_idx = vis_.at(v_idx).next;
    }
    return vis_.at(v_idx).next;
  }

#if 0
  std::optional<GEdgeKey> CreateNextEdgeKey(const Context& ctx,
                                            const PrevEdgeData prev,
                                            const GEdge& next_edge,
                                            bool next_in_target_area) {
    const bool active_key =
        (prev.v_idx != INFU32 && visited_edges_.at(prev.v_idx).key.GetType() ==
                                     GEdgeKey::TURN_RESTRICTION);

    if (!active_key && !next_edge.complex_turn_restriction_trigger) {
      // Common case, no turn restriction active, no turn restriction triggered.
      return GEdgeKey::CreateGraphEdge(g_, prev.other_idx, next_edge,
                                       next_in_target_area);
    }

    ActiveCtrs active_ctrs;
    if (active_key) {
      // We have an active config. Check if it triggers and if it forbids
      // next_edge.
      active_ctrs = ctr_deduper_.GetObj(
          visited_edges_.at(prev.v_idx).key.GetCtrConfigId());
      if (!ActiveCtrsAddNextEdge(g_, prev.other_idx, next_edge, &active_ctrs)) {
        // Forbidden turn!
        return std::nullopt;
      }
    }

    if (next_edge.complex_turn_restriction_trigger) {
      // Find new triggering turn restrictions.
      TurnRestriction::TREdge key = {.from_node_idx = prev.other_idx,
                                     .way_idx = next_edge.way_idx,
                                     .to_node_idx = next_edge.other_node_idx};
      auto it = g_.complex_turn_restriction_map.find(key);
      if (it != g_.complex_turn_restriction_map.end()) {
        uint32_t ctr_idx = it->second;
        do {
          const TurnRestriction& tr = g_.complex_turn_restrictions.at(ctr_idx);
          if (tr.GetTriggerKey() != key) {
            // Check that we iterated at least once.
            CHECK_NE_S(ctr_idx, it->second);
            break;
          }
          active_ctrs.push_back({.ctr_idx = ctr_idx, .position = 0});
        } while (++ctr_idx < g_.complex_turn_restrictions.size());
      }
    }

    if (active_ctrs.empty()) {
      // No active ctrs exist. Return normal edge key.
      return GEdgeKey::CreateGraphEdge(g_, prev.other_idx, next_edge,
                                       next_in_target_area);
    } else {
      uint32_t id = ctr_deduper_.Add(active_ctrs);
      return GEdgeKey::CreateTurnRestrictionEdge(g_, id, 0,
                                                 next_in_target_area);
    }
  }
#endif

  std::vector<VisitedEdge> vis_;
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
// If 'spanning_tree_nodes' is not nullptr, then every node that is marked
// done is added to the spanning_tree_nodes vector. Therefore, every node in
// this vector is the descendant of some node at an earlier position in the
// same vector, except for the root node at position 0. For example, this is
// useful to visit all nodes and edges bottom up or top down.
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
