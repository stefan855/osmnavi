// Compute all shortest paths between a set of nodes, for instance between
// border nodes in a cluster, using a CompactGraph.

#pragma once

#include <algorithm>
#include <fstream>
#include <memory>
#include <queue>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "algos/complex_turn_restriction.h"
#include "algos/routing_defs.h"
#include "algos/routing_metric.h"
#include "base/thread_pool.h"
#include "base/util.h"
#include "graph/compact_graph.h"
#include "graph/graph_def.h"

#define DEBUG_PRINT 0

class SingleSourceEdgeDijkstra {
 public:
  // Start routing with either an edge or a start n node.
  struct RoutingStart {
    uint32_t c_start_idx = INFU32;
    // If >= 0, then routing starts at the edge at the given offset, otherwise
    // at all edges of node 'c_node_idx'.
    int16_t c_edge_offset = -1;
    // Which fraction of the weight of the edge should be used for the metric?
    // If only turn costs but not the edge weight should be used then set to 0.
    // If for instance the vehicle starts at 33% of the edge, then set this to
    // 0.67. Default is using all weight, i.e. 1.0.
    float edge_weight_fraction = 1.0;
  };

  // The vector for visited edges is preallocated with cg.edges().size()
  // elements, so there is a standard ("base") slot for each edge of the graph.
  // But there might be more than one visited edge for a graph edge, because of
  // edge labelling (restricted areas, complex turn restrictions).
  //
  // Visited edges that can't be stored in the base position are added to the
  // end of the vector. The element in the base position maintains a linked list
  // containing all the added elements for the same base position, using the
  // attribute "next".
  struct VisitedEdge {
    std::uint32_t min_weight;  // The minimal weight seen so far.
    std::uint32_t from_v_idx;  // Previous edge entry.
    std::uint32_t active_ctr_id : 30;
    std::uint32_t in_target_restricted_access_area : 1;
    std::uint32_t done : 1;  // 1 <=> node has been finalized.
    // Is INFU32 if the slot is empty. Otherwise, it points to the next entry
    // for the same edge but with a different label. The last entry points to
    // the first entry (i.e. a loop). A list of edges has exactly one entry with
    // index < cg.edges.size(). This is the 'base' index, i.e. it points to the
    // edge in cg.edges(). See GetBaseIdx().
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
  void Route(const CompactGraph& cg, const RoutingStart& start,
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
    const std::vector<CompactGraph::PartialEdge>& edges = cg.edges();
    const std::vector<TurnCostData>& turn_costs = cg.turn_costs();
    ActiveCtrs active_ctrs;

    // ======================================
    // Push edge(s) of start node into queue.
    // ======================================
    for (size_t i = edges_start.at(start.c_start_idx);
         i < edges_start.at(start.c_start_idx + 1); ++i) {
      if (start.c_edge_offset >= 0 &&
          (int)(i - edges_start.at(start.c_start_idx)) != start.c_edge_offset) {
        continue;
      }

      const CompactGraph::PartialEdge& edge = edges.at(i);
      // Do the first edges trigger complex turn restrictions?
      if (edge.complex_tr_trigger) {
        AddTriggeringCTRs(cg, i, &active_ctrs);
      }
#if DEBUG_PRINT
      LOG_S(INFO) << absl::StrFormat(
          "Add initial edge from_idx:%u to_idx:%u way_idx:%u #ctrs:%llu",
          start_idx, edge.to_c_idx, edge.way_idx, active_ctrs.size());
#endif

      // TODO: restricted access area: should be 1 if the edge is in a
      // restricted area?
      uint32_t v_idx = FindOrAllocEdge(
          i, /*in_target_restricted_access_area=*/0, active_ctrs);
      CHECK_EQ_S(i, v_idx);
      vis_.at(i).min_weight = edge.weight * start.edge_weight_fraction;
      pq.emplace(vis_.at(i).min_weight, i, start.c_start_idx);
    }

    // =======================================
    // Loop until the priority queue is empty.
    // =======================================
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
      const CompactGraph::PartialEdge& prev_cg_edge = edges.at(prev_v_base_idx);
      const uint32_t start = edges_start.at(prev_cg_edge.to_c_idx);
      const uint32_t num_edges =
          edges_start.at(prev_cg_edge.to_c_idx + 1) - start;

      // LOG_S(INFO) << "XX:" << prev_cg_edge.turn_cost_idx;
      const TurnCostData& tcd = turn_costs.at(prev_cg_edge.turn_cost_idx);

      for (uint32_t off = 0; off < num_edges; ++off) {
        const uint32_t i = start + off;
        const CompactGraph::PartialEdge& e = edges.at(i);

#if DEBUG_PRINT
        LOG_S(INFO) << absl::StrFormat(
            "Examine edge from_idx:%u to_idx:%u way_idx:%u",
            prev_cg_edge.to_c_idx, e.to_c_idx, e.way_idx);
#endif

        /*
        LOG_S(INFO) << absl::StrFormat(
            "BB5 nodes:%u->%u->%u  cost:%d", qedge.from_node_idx,
            prev_cg_edge.to_c_idx, e.to_c_idx, (int)tcd.turn_costs.at(off));
            */

        if ((tcd.turn_costs.at(off) == TURN_COST_INFINITY_COMPRESSED) ||
            (opt.handle_restricted_access &&
             prev_v.in_target_restricted_access_area && !e.restricted_access)) {
          // At least one of these holds:
          // 1) Not all edges are allowed because of a TR
          // 2) We're in the target restricted access area, not allowed to
          // transition to a free edge.
          continue;
        }

#if 0
        // Obsolete because turn restrictions are now handled via turn costs.
        // Turn costs handle areas in a different way (u-turns are always 
        // allowed), so the code here will check fail for these.
        //
        // We need the start node of the prev edge, which we can get at the
        // prev-prev edge.
        if (!prev_cg_edge.uturn_allowed && e.to_c_idx == qedge.from_node_idx) {
          LOG_S(INFO) << absl::StrFormat(
              "UTURN-ISSUE nodes:%s -> %s -> %s  cost:%d",
              cg.DescribeCNodeIdx(qedge.from_node_idx),
              cg.DescribeCNodeIdx(prev_cg_edge.to_c_idx),
              cg.DescribeCNodeIdx(e.to_c_idx), (int)tcd.turn_costs.at(off));
            CHECK_S(0);
          }
          continue;
        }
#endif

        // Create/Update the turn restriction edge key information for the
        // current edge.
        active_ctrs.clear();  // This is reused throughout the loop.
        if (!UpdateActiveCtrs(cg, prev_v, i, e.complex_tr_trigger,
                              &active_ctrs)) {
#if DEBUG_PRINT
          LOG_S(INFO) << "Forbidden by TR";
#endif

          continue;
        }

        const uint32_t new_weight =
            prev_v.min_weight + decompress_turn_cost(tcd.turn_costs.at(off)) +
            e.weight;
        const bool in_target_raa =
            opt.handle_restricted_access &&
            (prev_v.in_target_restricted_access_area ||
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

        VisitedEdge& ve = vis_.at(new_v_idx);
        if (!ve.done && new_weight < ve.min_weight) {
          ve.min_weight = new_weight;
          ve.from_v_idx = qedge.ve_idx;
          pq.emplace(new_weight, new_v_idx, prev_cg_edge.to_c_idx);
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
  std::vector<uint32_t> GetMinEdgesAtNodes(const CompactGraph& cg) const {
    std::vector<uint32_t> min_edges(cg.num_nodes(), INFU32);
    const std::vector<CompactGraph::PartialEdge>& edges = cg.edges();
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
  std::vector<uint32_t> GetShortestPathToTargetSlow(const CompactGraph& cg,
                                                    uint32_t target_node_idx) {
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
      const CompactGraph& cg, uint32_t start_idx) const {
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

  inline CompactGraph::PartialEdge GetPartialEdge(const CompactGraph& cg,
                                                  uint32_t v_idx) const {
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

  // Find or allocate a visited edge element at 'v_base_index'. Each base index
  // might contain a list of visited edge, all for the same edge in the graph,
  // but with different labels. If edge+label is not found, then a new edge is
  // allocated at the end of the vector and added to the list of edges at this
  // specific base index.
  inline uint32_t FindOrAllocEdge(uint32_t v_base_idx,
                                  bool in_target_restricted_access_area,
                                  const ActiveCtrs& ctrs) {
    VisitedEdge& v_base = vis_.at(v_base_idx);
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

    // Find matching element in list.
    uint32_t v_curr_idx = v_base_idx;
    do {
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

    // Allocate new element.
    v_curr_idx = vis_.size();
    uint32_t active_ctr_id = NO_ACTIVE_CTR_ID;
    if (!ctrs.empty()) {
      active_ctr_id = active_ctrs_vec_.size();
      active_ctrs_vec_.push_back(ctrs);
    }
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
    return v_curr_idx;
  }

  inline uint32_t GetBaseIdx(size_t cg_edges_size, uint32_t v_idx) const {
    if (v_idx < cg_edges_size) return v_idx;
    while (vis_.at(v_idx).next >= cg_edges_size) {
      v_idx = vis_.at(v_idx).next;
    }
    return vis_.at(v_idx).next;
  }

  void AddTriggeringCTRs(const CompactGraph& cg, uint32_t next_edge_idx,
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
  inline bool UpdateActiveCtrs(const CompactGraph& cg,
                               const VisitedEdge& prev_v,
                               const uint32_t next_edge_idx,
                               bool next_complex_trigger,
                               ActiveCtrs* active_ctrs) {
    if (prev_v.active_ctr_id == 0 && !next_complex_trigger) {
      // Common case, no turn restriction active, no turn restriction triggered.
      active_ctrs->clear();
      return true;
    }

    if (prev_v.active_ctr_id > 0) {
      // We have active turn restrictions. Check if they forbid the next edge.
      // *active_ctrs = active_ctrs_vec_.at(prev_v.active_ctr_id);
      *active_ctrs = VECTOR_AT(active_ctrs_vec_, prev_v.active_ctr_id);
      if (!ActiveCtrsAddNextEdge(cg, next_edge_idx, active_ctrs)) {
        return false;
      }
    }

    if (next_complex_trigger) {
      AddTriggeringCTRs(cg, next_edge_idx, active_ctrs);
    }
    return true;
  }

  std::vector<VisitedEdge> vis_;
  std::vector<uint32_t> spanning_tree_edges_;
  std::vector<ActiveCtrs> active_ctrs_vec_;
};

struct CompactDijkstraRoutingData {
  const Graph& g;
  const CompactGraph cg;
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
  std::vector<CompactGraph::FullEdge> full_edges;
  absl::flat_hash_map<uint32_t, uint32_t> graph_to_compact_nodemap;
  CollectEdgesForCompactGraph(g, metric, opt, routing_nodes,
                              /*undirected_expand=*/true, &num_nodes,
                              &full_edges, &graph_to_compact_nodemap);

  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Collected " << graph_to_compact_nodemap.size()
                << " nodes and " << full_edges.size() << " edges";
    ;
  }

  CompactGraph cg(num_nodes, full_edges);
  // graph_to_compact_nodemap invalid after this call.
  cg.AddGNodeMap(g, std::move(graph_to_compact_nodemap),
                 /*create_inverted=*/true);

  cg.AddComplexTurnRestrictions(g.complex_turn_restrictions,
                                cg.graph_to_compact_map());
  cg.AddTurnCosts(g, metric.IsTimeMetric(), cg.graph_to_compact_map());

  cg.LogStats();
  if (verb >= Verbosity::Debug) {
    cg.DebugPrint();
  }
  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Compact routing data built (nodes:" << num_nodes
                << " edges:" << cg.edges().size() << ")";
  }
  return {.g = g, .cg = cg};
}

// Use a compact graph instead of routing on the graph data structure directly.
inline RoutingResult RouteOnCompactGraph(const CompactDijkstraRoutingData data,
                                         uint32_t g_start, uint32_t g_target,
                                         Verbosity verb = Verbosity::Brief) {
  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Route using compact routing data";
  }

  CHECK_S(data.cg.graph_to_compact_map().contains(g_start));
  CHECK_S(data.cg.graph_to_compact_map().contains(g_target));
  const uint32_t cg_start_idx =
      data.cg.graph_to_compact_map().find(g_start)->second;
  const uint32_t cg_target_idx =
      data.cg.graph_to_compact_map().find(g_target)->second;

  SingleSourceEdgeDijkstra compact_router;
  compact_router.Route(data.cg, {.c_start_idx = cg_start_idx},
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
      const SingleSourceEdgeDijkstra::VisitedEdge& ve = vis.at(v_idx);
      const CompactGraph::PartialEdge& e =
          compact_router.GetPartialEdge(data.cg, v_idx);
      LOG_S(INFO) << absl::StrFormat(
          "  %u. Edge %u to %u minw:%u ctrid:%llu target_ra:%u done:%u", pos++,
          from_idx, e.to_c_idx, ve.min_weight, ve.active_ctr_id,
          ve.in_target_restricted_access_area, ve.done);
      from_idx = e.to_c_idx;
    }
  }

  return res;
}
