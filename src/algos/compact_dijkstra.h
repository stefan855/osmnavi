// Compute all shortest paths between a set of nodes, for instance between
// border nodes in a cluster, using a CompactDirectedGraph.

#pragma once

#include <algorithm>
#include <fstream>
#include <memory>
#include <queue>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "algos/edge_key.h"
#include "algos/routing_defs.h"
#include "algos/routing_metric.h"
#include "base/thread_pool.h"
#include "base/util.h"
#include "graph/compact_graph.h"
#include "graph/graph_def.h"

#define DEBUG_PRINT 0

class SingleSourceEdgeDijkstra {
 public:
  struct VisitedEdge {
    std::uint32_t min_weight;  // The minimal weight seen so far.
    std::uint32_t from_v_idx;  // Previous edge entry.
    std::uint32_t active_ctr_id : 30;
    std::uint32_t in_target_restricted_access_area : 1;
    std::uint32_t done : 1;  // 1 <=> node has been finalized.
    // if next != INFU32 then the next entry with different key. The last entry
    // points to the first entry (i.e. a loop).
    std::uint32_t next;
  };

  struct Options {
    bool store_spanning_tree_edges = false;
    bool handle_restricted_access = false;
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
#if DEBUG_PRINT
    LOG_S(INFO) << absl::StrFormat("CompactDijkstra: Start routing at %u",
                                   start_idx);
#endif
    const size_t cg_edges_size = cg.edges().size();
    CHECK_LT_S(cg_edges_size, 1 << 31) << "currently not supported";
    Clear();
    if (opt.store_spanning_tree_edges) {
      spanning_tree_edges_.reserve(cg_edges_size);
    }
    vis_.assign(cg_edges_size, {.min_weight = INFU32,
                                .from_v_idx = INFU32,
                                .active_ctr_id = NO_ACTIVE_CTR_ID,
                                .in_target_restricted_access_area = 0,
                                .done = 0,
                                .next = INFU32});
    std::priority_queue<QueuedEdge, std::vector<QueuedEdge>, MetricCmpEdge> pq;

    const std::vector<uint32_t>& edges_start = cg.edges_start();
    const std::vector<CompactDirectedGraph::PartialEdge>& edges = cg.edges();
    const std::vector<TurnCostData>& turn_costs = cg.turn_costs();
    // const auto& compact_tr_map = cg.GetSimpleTRMap();
    ActiveCtrs active_ctrs;

    // Push edges of start node into queue.
    for (size_t i = edges_start.at(start_idx);
         i < edges_start.at(start_idx + 1); ++i) {
      const CompactDirectedGraph::PartialEdge& edge = edges.at(i);

      // Do the first edges trigger complex turn restrictions?
      if (edge.complex_tr_trigger) {
        AddTriggeringCTRs(cg, i, &active_ctrs);
        LOG_S(INFO) << "Initial edge is triggering CTR " << active_ctrs.size();
      }
#if DEBUG_PRINT
      LOG_S(INFO) << absl::StrFormat(
          "Add initial edge from_idx:%u to_idx:%u way_idx:%u #ctrs:%llu",
          start_idx, edge.to_c_idx, edge.way_idx, active_ctrs.size());
#endif

      uint32_t v_idx = FindOrAllocEdge(
          i, /*in_target_restricted_access_area=*/0, active_ctrs);
      CHECK_EQ_S(i, v_idx);
      vis_.at(i).min_weight = edge.weight;
      pq.emplace(edge.weight, i, start_idx);
    }

    int count = 0;
    while (!pq.empty()) {
      ++count;
      // Remove the minimal node from the priority queue.
      const QueuedEdge qedge = pq.top();
      pq.pop();
      // Make a copy, because the vector might see reallocations below, which
      // invalidates references.
      const VisitedEdge prev_v = vis_.at(qedge.ve_idx);
#if DEBUG_PRINT
      LOG_S(INFO) << absl::StrFormat(
          "prev_v(%u): minw:%u from_v_idx:%u active_ctr_id:%u in_target_ra:%u "
          "done:%u next:%u ",
          qedge.ve_idx, prev_v.min_weight, prev_v.from_v_idx,
          prev_v.active_ctr_id, prev_v.in_target_restricted_access_area,
          prev_v.done, prev_v.next);
#endif
      if (prev_v.done == 1) {
        continue;  // "old" entry in priority queue.
      }
      CHECK_EQ_S(qedge.weight, prev_v.min_weight);
      vis_.at(qedge.ve_idx).done = 1;
      if (opt.store_spanning_tree_edges) {
        // TODO
        CHECK_S(false);
        spanning_tree_edges_.push_back(qedge.ve_idx);
      }
      const uint32_t prev_v_base_idx = GetBaseIdx(cg_edges_size, qedge.ve_idx);
      const CompactDirectedGraph::PartialEdge& prev_cg_edge =
          edges.at(prev_v_base_idx);
      const uint32_t start = edges_start.at(prev_cg_edge.to_c_idx);
      const uint32_t num_edges =
          edges_start.at(prev_cg_edge.to_c_idx + 1) - start;

#if 0
      uint32_t allowed_offset_bits = -1;  // Set all bits
      if (prev_cg_edge.simple_tr_trigger) {
        // LOG_S(INFO) << "Search simple TR trigger!";
        auto iter = compact_tr_map.find(prev_v_base_idx);
        if (iter != compact_tr_map.end()) {
          allowed_offset_bits = iter->second;
          // LOG_S(INFO) << "Found simple TR trigger!";
        }
      }
#endif

      // LOG_S(INFO) << "XX:" << prev_cg_edge.turn_cost_idx;
      const TurnCostData& tcd = turn_costs.at(prev_cg_edge.turn_cost_idx);

      for (uint32_t off = 0; off < num_edges; ++off) {
        const uint32_t i = start + off;
        const CompactDirectedGraph::PartialEdge& e = edges.at(i);
#if DEBUG_PRINT
        LOG_S(INFO) << absl::StrFormat(
            "Examine edge from_idx:%u to_idx:%u way_idx:%u",
            prev_cg_edge.to_c_idx, e.to_c_idx, e.way_idx);
#endif
        // LOG_S(INFO) << "AA1";

#if 0
        if (!(allowed_offset_bits & (1u << off)) ||
            (opt.handle_restricted_access &&
             prev_v.in_target_restricted_access_area && !e.restricted_access)) {
          // At least one of these holds:
          // 1) Not all edges are allowed because of a TR
          // 2) We're in the target restricted access area, not allowed to
          // transition to a free edge.
          continue;
        }
#endif
        if ((tcd.turn_costs.at(off) == TURN_COST_INF_COMPRESSED) ||
            (opt.handle_restricted_access &&
             prev_v.in_target_restricted_access_area && !e.restricted_access)) {
          // LOG_S(INFO) << "XX2:" << (int)tcd.turn_costs.at(off);
          // At least one of these holds:
          // 1) Not all edges are allowed because of a TR
          // 2) We're in the target restricted access area, not allowed to
          // transition to a free edge.
          continue;
        }

        // LOG_S(INFO) << "AA2";
        // We need the start node of the prev edge, which we can get at the
        // prev-prev edge.
        if (!prev_cg_edge.uturn_allowed && e.to_c_idx == qedge.from_node_idx) {
          continue;
        }

        // LOG_S(INFO) << "AA3";
        // Create/Update the turn restriction edge key information for the
        // current edge.
        active_ctrs.clear();  // This is reused throughout the loop.
        if (!NextCtrKey(cg, prev_v, i, e.complex_tr_trigger, &active_ctrs)) {
#if DEBUG_PRINT
          LOG_S(INFO) << "Forbidden by TR";
#endif
          continue;
        }

        // LOG_S(INFO) << "AA4";
        const uint32_t new_weight =
            prev_v.min_weight + e.weight +
            decompress_turn_cost(tcd.turn_costs.at(off)) * 100;
        const bool in_target_raa =
            opt.handle_restricted_access &&
            (prev_v.in_target_restricted_access_area |
             (!edges.at(prev_v_base_idx).restricted_access &&
              e.restricted_access));

        const uint32_t new_v_idx =
            FindOrAllocEdge(i, in_target_raa, active_ctrs);
#if DEBUG_PRINT
        LOG_S(INFO) << absl::StrFormat(
            "Push edge from_idx:%u to_idx:%u way_idx:%u target_ra:%u #ctr:%llu",
            prev_cg_edge.to_c_idx, e.to_c_idx, e.way_idx, in_target_raa,
            active_ctrs.size());
#endif

        // LOG_S(INFO) << "AA5";
        VisitedEdge& ve = vis_.at(new_v_idx);
        if (!ve.done && new_weight < ve.min_weight) {
          ve.min_weight = new_weight;
          ve.from_v_idx = qedge.ve_idx;
          pq.emplace(new_weight, new_v_idx, prev_cg_edge.to_c_idx);
        }
        // LOG_S(INFO) << "AA6";
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
      // TODO: handle multiple visited edges per edge.
      if (min_edges.at(to_c_idx) == INFU32 ||
          vis_.at(i).min_weight < vis_.at(min_edges.at(to_c_idx)).min_weight) {
        min_edges.at(to_c_idx) = i;
      }
    }
    return min_edges;
  }

  // Construct the shortest path at target_node_idx. This is slow and should
  // only be used for small graphs in testing.
  // Returns the visited edge indexes on the path.
  std::vector<uint32_t> GetShortestPathToTargetSlow(
      const CompactDirectedGraph& cg, uint32_t target_node_idx) {
    // Find the minimal visited edge that ends at 'target_node_idx'.
    uint32_t found_min_weight = INFU32;
    uint32_t found_last_edge_idx = INFU32;
    for (uint32_t edge_idx = 0; edge_idx < cg.edges().size(); ++edge_idx) {
      if (cg.edges().at(edge_idx).to_c_idx == target_node_idx &&
          vis_.at(edge_idx).min_weight != INFU32) {
        uint32_t curr_idx = edge_idx;
        do {
          const VisitedEdge& ve = vis_.at(curr_idx);
          if (ve.min_weight < found_min_weight) {
            found_min_weight = ve.min_weight;
            found_last_edge_idx = curr_idx;
          }
          curr_idx = ve.next;
        } while (curr_idx != edge_idx);
      }
    }
    std::vector<uint32_t> v;
    if (found_last_edge_idx == INFU32) {
      return v;
    }

    // Now fill the edge indexes in backward direction.
    for (uint32_t idx = found_last_edge_idx; idx != INFU32;) {
      v.push_back(idx);
      idx = vis_.at(idx).from_v_idx;
    }
    // Reverse direction.
    std::reverse(v.begin(), v.end());
    return v;
  }

  // For each node, store the min_weight of the edge that reaches the node
  // first. If a node wasn't reached, store INFU32.
  std::vector<uint32_t> GetNodeWeightsFromVisitedEdges(
      const CompactDirectedGraph& cg, uint32_t start_idx) const {
    std::vector<uint32_t> w(cg.num_nodes(), INFU32);
    for (size_t i = 0; i < cg.edges().size(); ++i) {
      /*
      w.at(to_c_idx) = std::min(w.at(to_c_idx), vis_.at(i).min_weight);
      */
      uint32_t to_c_idx = cg.edges().at(i).to_c_idx;
      uint32_t curr_idx = i;
      if (vis_.at(curr_idx).min_weight != INFU32) {
        do {
          const VisitedEdge& ve = vis_.at(curr_idx);
          w.at(to_c_idx) = std::min(w.at(to_c_idx), ve.min_weight);
          curr_idx = ve.next;
        } while (curr_idx != i);
      }
    }
    w.at(start_idx) = 0;
    return w;
  }

  inline CompactDirectedGraph::PartialEdge GetEdge(
      const CompactDirectedGraph& cg, uint32_t v_idx) const {
    return cg.edges().at(GetBaseIdx(cg.edges().size(), v_idx));
  }

 private:
  static constexpr uint32_t NO_ACTIVE_CTR_ID = 0;
  // This might exist multiple times for each node, when a node gets
  // reinserted into the priority queue with a lower priority.
  struct QueuedEdge {
    std::uint32_t weight;
    std::uint32_t ve_idx;  // Index into visited_edges vector.
    std::uint32_t from_node_idx;
  };

  struct MetricCmpEdge {
    bool operator()(const QueuedEdge& left, const QueuedEdge& right) const {
      return left.weight > right.weight;
    }
  };

  void Clear() {
    vis_.clear();
    spanning_tree_edges_.clear();
    active_ctrs_vec_.clear();
    // Add element 0 which denotes "no active ctrs".
    active_ctrs_vec_.push_back({});
    CHECK_S(active_ctrs_vec_.at(NO_ACTIVE_CTR_ID).empty());
  }

  inline uint32_t FindOrAllocEdge(uint32_t v_base_idx,
                                  bool in_target_restricted_access_area,
                                  const ActiveCtrs& ctrs) {
    // LOG_S(INFO) << "BB1";
    VisitedEdge& v_base = vis_.at(v_base_idx);
    // LOG_S(INFO) << "BB2";
    // Slot unused?
    if (v_base.next == INFU32) {
      v_base.in_target_restricted_access_area =
          in_target_restricted_access_area;
      if (ctrs.empty()) {
        v_base.active_ctr_id = NO_ACTIVE_CTR_ID;
      } else {
        v_base.active_ctr_id = active_ctrs_vec_.size();
        active_ctrs_vec_.push_back(ctrs);
      }
      v_base.next = v_base_idx;  // loops back to itself.
      return v_base_idx;
    }
    // LOG_S(INFO) << "BB3";
#if 0
    // Is base element matching the key information?
    if (v_base.in_target_restricted_access_area ==
            in_target_restricted_access_area &&
        ((v_base.active_ctr_id == NO_ACTIVE_CTR_ID && ctrs.empty()) ||
         (v_base.active_ctr_id != NO_ACTIVE_CTR_ID &&
          active_ctrs_.at(v_base.active_ctr_id) == ctrs))) {
      return v_base_idx;
    }
    // Find matching element in list.
    uint32_t v_curr_idx = v_base.next;
    while (v_curr_idx != v_base_idx) {
      const VisitedEdge& v_curr = vis_.at(v_curr_idx);
      if (v_curr.in_target_restricted_access_area ==
              in_target_restricted_access_area &&
          ((v_curr.active_ctr_id == NO_ACTIVE_CTR_ID && ctrs.empty()) ||
         (v_curr.active_ctr_id != NO_ACTIVE_CTR_ID &&
          active_ctrs_.at(v_curr.active_ctr_id) == ctrs))) {
        return v_curr_idx;
      }
      v_curr_idx = v_curr.next;
    }
#endif

    // Find matching element in list.
    uint32_t v_curr_idx = v_base_idx;
    do {
      // LOG_S(INFO) << "BB4";
      const VisitedEdge& v_curr = vis_.at(v_curr_idx);
      if (v_curr.in_target_restricted_access_area ==
              in_target_restricted_access_area &&
          ((v_curr.active_ctr_id == NO_ACTIVE_CTR_ID && ctrs.empty()) ||
           (v_curr.active_ctr_id != NO_ACTIVE_CTR_ID &&
            active_ctrs_vec_.at(v_curr.active_ctr_id) == ctrs))) {
        return v_curr_idx;
      }
      v_curr_idx = v_curr.next;
    } while (v_curr_idx != v_base_idx);
    // LOG_S(INFO) << "BB5";

    // Allocate new element.
    v_curr_idx = vis_.size();
    uint32_t active_ctr_id = NO_ACTIVE_CTR_ID;
    if (!ctrs.empty()) {
      active_ctr_id = active_ctrs_vec_.size();
      active_ctrs_vec_.push_back(ctrs);
    }
    // LOG_S(INFO) << "BB6";
    // Note, that this might invalidate reference v_base.
    const uint32_t v_base_next_val = v_base.next;
    vis_.push_back(
        {.min_weight = INFU32,
         .from_v_idx = INFU32,
         .active_ctr_id = active_ctr_id,
         .in_target_restricted_access_area = in_target_restricted_access_area,
         .done = 0,
         .next = v_base_next_val});
    // Do not use v_base, pushing to vector might invalidated the reference.
    vis_.at(v_base_idx).next = v_curr_idx;
    // LOG_S(INFO) << "BB7";
    return v_curr_idx;
  }

  inline uint32_t GetBaseIdx(size_t cg_edges_size, uint32_t v_idx) const {
    if (v_idx < cg_edges_size) return v_idx;
    while (vis_.at(v_idx).next >= cg_edges_size) {
      v_idx = vis_.at(v_idx).next;
    }
    return vis_.at(v_idx).next;
  }

  void AddTriggeringCTRs(const CompactDirectedGraph& cg, uint32_t next_edge_idx,
                         ActiveCtrs* active_ctrs) {
    // Find new triggering turn restrictions.
    const auto it = cg.GetComplexTRMap().find(next_edge_idx);
    if (it != cg.GetComplexTRMap().end()) {
      uint32_t ctr_idx = it->second;
      do {
        const auto& ctr = cg.GetComplexTRS().at(ctr_idx);
        if (ctr.GetTriggerEdgeIdx() != next_edge_idx) {
          // Check that we iterated at least once.
          CHECK_NE_S(ctr_idx, it->second);
          break;
        }
        active_ctrs->push_back({.ctr_idx = ctr_idx, .position = 0});
      } while (++ctr_idx < cg.GetComplexTRS().size());
    }
  }

  // Given a previous and a new edge, compute the new active_ctr_id.
  // Return false if adding the new edge is forbidden, or true when there is a
  // new value in 'active_ctrs'.
  inline bool NextCtrKey(const CompactDirectedGraph& cg,
                         const VisitedEdge& prev_v,
                         const uint32_t next_edge_idx,
                         bool next_complex_trigger, ActiveCtrs* active_ctrs) {
    if (prev_v.active_ctr_id == 0 && !next_complex_trigger) {
      // Common case, no turn restriction active, no turn restriction triggered.
      active_ctrs->clear();
      return true;
    }

    if (prev_v.active_ctr_id > 0) {
      // We have active turn restrictions. Check if they forbid the next edge.
      // *active_ctrs = active_ctrs_vec_.at(prev_v.active_ctr_id);
      *active_ctrs = VECTOR_AT(active_ctrs_vec_, prev_v.active_ctr_id);
      // LOG_S(INFO) << "AA1: ActiveCtrsAddNextEdge() before call with #ctrs="
      //             << active_ctrs->size() << " edge idx=" << next_edge_idx;
      if (!ActiveCtrsAddNextEdge(cg, next_edge_idx, active_ctrs)) {
        // LOG_S(INFO) << "AA2: Can't add edge " << next_edge_idx;
        return false;
      }
      // LOG_S(INFO) << "AA3: ActiveCtrsAddNextEdge() after call with #ctrs="
      //             << active_ctrs->size();
    }

    if (next_complex_trigger) {
      AddTriggeringCTRs(cg, next_edge_idx, active_ctrs);
    }

    return true;

#if 0
    if (active_ctrs.empty()) {
      // No active ctrs exist.
      *active_ctr_id = NO_ACTIVE_CTR_ID;
      return true;
    } else {
      *active_ctr_id = active_ctrs_vec_.size();
      // TODO: more efficient way to copy or better move the content?
      active_ctrs_vec_.push_back(active_ctrs);
      return true;
    }
#endif
  }

  std::vector<VisitedEdge> vis_;
  std::vector<uint32_t> spanning_tree_edges_;
  std::vector<ActiveCtrs> active_ctrs_vec_;
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

// Use a compact graph instead of routing on the graph data structure directly.
inline RoutingResult RouteOnCompactGraph(const Graph& g, uint32_t g_start,
                                         uint32_t g_target,
                                         const RoutingMetric& metric,
                                         RoutingOptions opt,
                                         Verbosity verb = Verbosity::Brief) {
  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Run query on compact graph";
  }
  constexpr uint32_t CG_START = 0;
  constexpr uint32_t CG_TARGET = 1;
  uint32_t num_nodes;
  std::vector<CompactDirectedGraph::FullEdge> full_edges;
  absl::flat_hash_map<uint32_t, uint32_t> graph_to_compact_nodemap;
  // if (opt.avoid_dead_end) {
  //   opt.MayFillBridgeNodeId(g, g_target);
  // }
  CollectEdgesForCompactGraph(g, metric, opt, {g_start, g_target},
                              /*undirected_expand=*/true, &num_nodes,
                              &full_edges, &graph_to_compact_nodemap);
  CHECK_S(graph_to_compact_nodemap.contains(g_start));
  CHECK_S(graph_to_compact_nodemap.contains(g_target));
  CHECK_EQ_S(graph_to_compact_nodemap.find(g_start)->second, CG_START);
  CHECK_EQ_S(graph_to_compact_nodemap.find(g_target)->second, CG_TARGET);
  CompactDirectedGraph cg(num_nodes, full_edges);
  // cg.AddSimpleTurnRestrictions(g, g.simple_turn_restriction_map,
  //                              graph_to_compact_nodemap);
  cg.AddComplexTurnRestrictions(g.complex_turn_restrictions,
                                graph_to_compact_nodemap);
  cg.AddTurnCosts(g, metric.IsTimeMetric(), graph_to_compact_nodemap);

  // Now route!
  cg.LogStats();
  if (verb >= Verbosity::Debug) {
    cg.DebugPrint();
  }
  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Compact graph built. Start Routing";
  }
  SingleSourceEdgeDijkstra compact_router;
  compact_router.Route(cg, CG_START, {.handle_restricted_access = true});
  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Finished routing";
  }

  /*
  std::vector<uint32_t> w =
      compact_router.GetNodeWeightsFromVisitedEdges(cg, CG_START);
  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Result metric:" << w.at(CG_TARGET);
  }
  */

  const auto& vis = compact_router.GetVisitedEdges();
  RoutingResult res;
  res.route_v_idx = compact_router.GetShortestPathToTargetSlow(cg, CG_TARGET);
  res.found = !res.route_v_idx.empty();
  res.found_distance = res.route_v_idx.empty()
                           ? INFU32
                           : vis.at(res.route_v_idx.back()).min_weight;
  res.num_shortest_route_nodes =
      res.route_v_idx.empty() ? 0 : res.route_v_idx.size() + 1;
  res.num_visited = vis.size();

  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Result metric:" << res.found_distance;
  }

  // Print path.
  if (verb >= Verbosity::Verbose) {
    uint32_t from_idx = CG_START;
    uint32_t pos = 1;
    LOG_S(INFO) << "CompactDijkstra: Shortest path";
    for (uint32_t v_idx : res.route_v_idx) {
      const auto& ve = vis.at(v_idx);
      const auto& e = compact_router.GetEdge(cg, v_idx);
      LOG_S(INFO) << absl::StrFormat(
          "  %u. Edge %u to %u minw:%u ctrid:%llu target_ra:%u done:%u", pos++,
          from_idx, e.to_c_idx, ve.min_weight, ve.active_ctr_id,
          ve.in_target_restricted_access_area, ve.done);
      from_idx = e.to_c_idx;
    }
  }

  return res;
}

}  // namespace compact_dijkstra

struct CompactDijkstraRoutingData {
  const Graph& g;
  const absl::flat_hash_map<uint32_t, uint32_t> graph_to_compact_nodemap;
  const CompactDirectedGraph cg;
};

// Important: Unless opt.avoid_dead_end==false, all start/end nodes need to be
// provided in routing_nodes. This will load the individual dead-ends of each
// node in routing_nodes, but ignore all independent dead-ends.
inline CompactDijkstraRoutingData CreateCompactDijkstraRoutingData(
    const Graph& g, const std::vector<uint32_t> routing_nodes,
    const RoutingMetric& metric, RoutingOptions opt,
    Verbosity verb = Verbosity::Brief) {
  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Create compact routing data";
  }
  uint32_t num_nodes;
  std::vector<CompactDirectedGraph::FullEdge> full_edges;
  absl::flat_hash_map<uint32_t, uint32_t> graph_to_compact_nodemap;
  CollectEdgesForCompactGraph(g, metric, opt, routing_nodes,
                              /*undirected_expand=*/true, &num_nodes,
                              &full_edges, &graph_to_compact_nodemap);
  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Collected " << graph_to_compact_nodemap.size()
                << " nodes and " << full_edges.size() << " edges";
    ;
  }
  CompactDirectedGraph cg(num_nodes, full_edges);
  // cg.AddSimpleTurnRestrictions(g, g.simple_turn_restriction_map,
  //                              graph_to_compact_nodemap);
  cg.AddComplexTurnRestrictions(g.complex_turn_restrictions,
                                graph_to_compact_nodemap);
  cg.AddTurnCosts(g, metric.IsTimeMetric(), graph_to_compact_nodemap);

  cg.LogStats();
  if (verb >= Verbosity::Debug) {
    cg.DebugPrint();
  }
  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Compact routing data built (nodes:" << num_nodes
                << " edges:" << cg.edges().size() << ")";
  }
  return {
      .g = g, .graph_to_compact_nodemap = graph_to_compact_nodemap, .cg = cg};
}

// Use a compact graph instead of routing on the graph data structure directly.
inline RoutingResult RouteOnCompactGraph(const CompactDijkstraRoutingData data,
                                         uint32_t g_start, uint32_t g_target,
                                         Verbosity verb = Verbosity::Brief) {
  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Route using compact routing data";
    // LOG_S(INFO) << "AA g_start:" << g_start;
    // LOG_S(INFO) << "AA g_target:" << g_target;
    // LOG_S(INFO) << "AA map size:" << data.graph_to_compact_nodemap.size();
    // for (const auto& [key, val] : data.graph_to_compact_nodemap) {
    //   LOG_S(INFO) << "map entry:" << key << ":" << val;
    // }
  }

  CHECK_S(data.graph_to_compact_nodemap.contains(g_start));
  CHECK_S(data.graph_to_compact_nodemap.contains(g_target));
  const uint32_t cg_start_idx =
      data.graph_to_compact_nodemap.find(g_start)->second;
  const uint32_t cg_target_idx =
      data.graph_to_compact_nodemap.find(g_target)->second;

  SingleSourceEdgeDijkstra compact_router;
  compact_router.Route(data.cg, cg_start_idx,
                       {.handle_restricted_access = true});
  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Finished routing";
  }

  const auto& vis = compact_router.GetVisitedEdges();
  RoutingResult res;
  res.route_v_idx =
      compact_router.GetShortestPathToTargetSlow(data.cg, cg_target_idx);
  res.found = !res.route_v_idx.empty();
  res.found_distance = res.route_v_idx.empty()
                           ? INFU32
                           : vis.at(res.route_v_idx.back()).min_weight;
  res.num_shortest_route_nodes =
      res.route_v_idx.empty() ? 0 : res.route_v_idx.size() + 1;
  res.num_visited = vis.size();

  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Result metric:" << res.found_distance;
  }

  // Print path.
  if (verb >= Verbosity::Verbose) {
    uint32_t from_idx = cg_start_idx;
    uint32_t pos = 1;
    LOG_S(INFO) << "CompactDijkstra: Shortest path";
    for (uint32_t v_idx : res.route_v_idx) {
      const auto& ve = vis.at(v_idx);
      const auto& e = compact_router.GetEdge(data.cg, v_idx);
      LOG_S(INFO) << absl::StrFormat(
          "  %u. Edge %u to %u minw:%u ctrid:%llu target_ra:%u done:%u", pos++,
          from_idx, e.to_c_idx, ve.min_weight, ve.active_ctr_id,
          ve.in_target_restricted_access_area, ve.done);
      from_idx = e.to_c_idx;
    }
  }

  return res;
}
