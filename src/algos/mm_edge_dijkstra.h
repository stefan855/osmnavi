// Compute all shortest paths between a set of nodes, for instance between
// border nodes in a cluster, using a CompactGraph.

#pragma once

#include <algorithm>
#include <fstream>
#include <memory>
#include <queue>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "algos/mm_edge_dijkstra.h"
#include "algos/routing_defs.h"
#include "algos/routing_metric.h"
#include "base/thread_pool.h"
#include "base/util.h"

#define DEBUG_PRINT 0

class SingleSourceMMEdgeDijkstra {
 public:
  // Start routing with either an edge or a start n node.
  struct RoutingStart {
    uint32_t start_idx = INFU32;
    // If >= 0, then routing starts at the edge at the given offset, otherwise
    // at all edges of node 'start_idx'.
    int16_t edge_offset = -1;
    // Which fraction of the weight of the edge should be used for the metric?
    // If only turn costs but not the edge weight should be used then set to 0.
    // If for instance the vehicle starts at 33% of the edge, then set this to
    // 0.67. Default is using all weight, i.e. 1.0.
    float edge_weight_fraction = 1.0;
  };

  // The vector for visited edges is preallocated with g.edges().size()
  // elements, so there is a standard ("base") slot for each edge of the graph.
  // But there might be more than one visited edge for a graph edge, because of
  // edge labelling (restricted areas, complex turn restrictions).
  //
  // Visited edges that can't be stored in the base position are added to the
  // end of the vector. The element in the base position maintains a linked list
  // containing all the added elements for the same base position, using the
  // attribute "next".
  struct VisitedEdge {
    // The minimal weight seen so far. INFU32 if unused.
    std::uint32_t min_weight;
    // Previous edge entry. INFU32 if prevous entry does not exist.
    std::uint32_t from_v_idx;
    std::uint32_t active_ctr_id : 30;
    std::uint32_t in_target_restricted_access_area : 1;
    std::uint32_t done : 1;  // 1 <=> node has been finalized.
    // Is INFU32 if the slot is empty. Otherwise, it points to the next entry
    // for the same edge but with a different label. The last entry points to
    // the first entry (i.e. a loop). A list of edges has exactly one entry with
    // index < g.edges.size(). This is the 'base' index, i.e. it points to the
    // edge in g.edges(). See GetBaseIdx().
    std::uint32_t next;
  };

  struct Options {
    bool store_spanning_tree_edges = false;
    bool handle_restricted_access = false;
    bool include_dead_end = false;
  };

  SingleSourceMMEdgeDijkstra() { ; }

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
  void Route(const MMClusterWrapper& mcw, const RoutingStart& start,
             const Options opt) {
    const MMCluster& g = mcw.g;
#if DEBUG_PRINT
    LOG_S(INFO) << absl::StrFormat("CompactDijkstra: Start routing at %u",
                                   start_idx);
#endif

    const size_t edges_size = g.edges.size();
    CHECK_LT_S(edges_size, 1 << 31) << "currently not supported";
    Clear();
    if (opt.store_spanning_tree_edges) {
      spanning_tree_edges_.reserve(edges_size);
    }
    vis_.assign(edges_size, {.min_weight = INFU32,
                             .from_v_idx = INFU32,
                             .active_ctr_id = NO_ACTIVE_CTR_ID,
                             .in_target_restricted_access_area = 0,
                             .done = 0,
                             .next = INFU32});
    std::priority_queue<QueuedEdge, std::vector<QueuedEdge>, MetricCmpEdge> pq;
    const MMCompressedUIntVec& edges = g.edges;
    ActiveCtrs active_ctrs;

    // ======================================
    // Push edge(s) of start node into queue.
    // ======================================
    for (int64_t i : g.edge_indices(start.start_idx)) {
      if (start.edge_offset >= 0 &&
          (int64_t)(i - g.edge_start_idx(start.start_idx) !=
                    start.edge_offset)) {
        continue;
      }

      const MMEdge edge(edges.at(i));
      if (!opt.include_dead_end && (edge.bridge() || edge.dead_end())) {
        continue;
      }

      // Do the first edges trigger complex turn restrictions?
      if (edge.complex_turn_restriction_trigger()) {
        AddTriggeringCTRs(g, i, &active_ctrs);
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

      // We're just starting, so we should be at base index i.
      CHECK_EQ_S(i, v_idx);
      vis_.at(i).min_weight =
          mcw.edge_weights.at(i) * start.edge_weight_fraction;
      pq.emplace(vis_.at(i).min_weight, i, start.start_idx);
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
      LOG_S(INFO) < absl::StrFormat(
                        "prev_v(%u): minw:%u from_v_idx:%u active_ctr_id:%u "
                        "in_target_ra:%u "
                        "done:%u next:%u ",
                        qedge.ve_idx, prev_v.min_weight, prev_v.from_v_idx,
                        prev_v.active_ctr_id,
                        prev_v.in_target_restricted_access_area, prev_v.done,
                        prev_v.next);
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
      const uint32_t prev_v_base_idx = GetBaseIdx(edges_size, qedge.ve_idx);
      const MMEdge prev_edge(edges.at(prev_v_base_idx));

#if 0
      if (g.get_node_id(prev_edge.target_idx()) == 357301279) {
        LOG_S(INFO) << "reach target with metric " << prev_v.min_weight;
      }
#endif

      // const CompactGraph::PartialEdge& prev_edge =
      // edges.at(prev_v_base_idx);
      const uint32_t edge_start_idx = g.edge_start_idx(prev_edge.target_idx());
      // const uint32_t edge_start_idx = edges_start.at(prev_edge.to_c_idx);
      const uint32_t num_edges =
          g.edge_stop_idx(prev_edge.target_idx()) - edge_start_idx;

      const std::span<const uint8_t> turn_costs =
          g.get_turn_costs(prev_v_base_idx);
      CHECK_EQ_S(turn_costs.size(), num_edges);
      // const TurnCostData& tcd = turn_costs.at(prev_edge.turn_cost_idx);

      for (uint32_t off = 0; off < num_edges; ++off) {
        const uint32_t edge_idx = edge_start_idx + off;
        const MMEdge e(edges.at(edge_idx));

        if (!opt.include_dead_end && (e.bridge() || e.dead_end())) {
          continue;
        }

#if DEBUG_PRINT
        LOG_S(INFO) << absl::StrFormat(
            "Examine edge from_idx:%u to_idx:%u way_idx:%u", prev_edge.to_c_idx,
            e.to_c_idx, e.way_idx);
#endif

        /*
        LOG_S(INFO) << absl::StrFormat(
            "BB5 nodes:%u->%u->%u  cost:%d", qedge.from_node_idx,
            prev_edge.to_c_idx, e.to_c_idx, (int)tcd.turn_costs.at(off));
            */

        if ((turn_costs[off] == TURN_COST_INFINITY_COMPRESSED) ||
            (opt.handle_restricted_access &&
             prev_v.in_target_restricted_access_area && !e.restricted())) {
          // At least one of these holds:
          // 1) Not all edges are allowed because of a TR
          // 2) We're in the target restricted access area, not allowed to
          // transition to a free edge.
          continue;
        }

        // Create/Update the turn restriction edge key information for the
        // current edge.
        active_ctrs.clear();  // This is reused throughout the loop.
        if (!UpdateActiveCtrs(g, prev_v, edge_idx,
                              e.complex_turn_restriction_trigger(),
                              &active_ctrs)) {
#if DEBUG_PRINT
          LOG_S(INFO) << "Forbidden by TR";
#endif

          continue;
        }

        const uint32_t new_weight = prev_v.min_weight +
                                    decompress_turn_cost(turn_costs[off]) +
                                    mcw.edge_weights.at(edge_idx);
        const bool in_target_raa =
            opt.handle_restricted_access &&
            (prev_v.in_target_restricted_access_area ||
             (!prev_edge.restricted() && e.restricted()));

        const uint32_t new_v_idx =
            FindOrAllocEdge(edge_idx, in_target_raa, active_ctrs);

#if DEBUG_PRINT
        LOG_S(INFO) << absl::StrFormat(
            "Push edge from_idx:%u to_idx:%u way_idx:%u target_ra:%u #ctr:%llu",
            prev_edge.target_idx(), e.target_idx(), e.way_idx, in_target_raa,
            active_ctrs.size());
#endif

        VisitedEdge& ve = vis_.at(new_v_idx);
        if (!ve.done && new_weight < ve.min_weight) {
          ve.min_weight = new_weight;
          ve.from_v_idx = qedge.ve_idx;
          pq.emplace(new_weight, new_v_idx, prev_edge.target_idx());
        }
      }
    }
  }

  // Get the path finishing at edge 'v_edge_idx' after routing has run.
  // Returns an empty vector if no path exists, or a vector of edge indexes,
  // from in_edge to out_edge.
  std::vector<uint32_t> GetShortestPathFromTargetEdge(
      const MMCluster& g, uint32_t v_edge_idx) const {
    if (vis_.at(v_edge_idx).min_weight == INFU32) {
      return {};  // No path.
    }

    std::vector<uint32_t> v;
    for (uint32_t idx = v_edge_idx; idx != INFU32;) {
      v.push_back(GetGraphEdgeIdx(g, idx));
      idx = vis_.at(idx).from_v_idx;
    }
    // Reverse direction.
    std::reverse(v.begin(), v.end());
    return v;
  }

  // Construct the shortest path at target_node_idx. This is slow and should
  // only be used for small graphs in testing.
  // Returns the visited edge indexes on the path.
  std::vector<uint32_t> GetShortestPathToTargetSlow(
      const MMCluster& g, uint32_t target_node_idx) const {
    // Find the minimal visited edge that ends at 'target_node_idx'.
    uint32_t found_min_weight = INFU32;
    uint32_t found_last_edge_idx = INFU32;
    for (uint32_t edge_idx = 0; edge_idx < g.edges.size(); ++edge_idx) {
      if (MM_EDGE(g.edges.at(edge_idx)).target_idx() == target_node_idx &&
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

  inline uint32_t GetGraphEdgeIdx(const MMCluster& g, uint32_t v_idx) const {
    return GetBaseIdx(g.edges.size(), v_idx);
  }

  inline uint32_t GetBaseIdx(size_t edges_size, uint32_t v_idx) const {
    if (v_idx < edges_size) return v_idx;
    while (vis_.at(v_idx).next >= edges_size) {
      v_idx = vis_.at(v_idx).next;
    }
    return vis_.at(v_idx).next;
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

  static inline void AddTriggeringCTRs(const MMCluster& g,
                                       uint32_t next_edge_idx,
                                       ActiveCtrs* active_ctrs) {
    // Find new triggering turn restrictions.
    uint32_t first_ctr_idx = g.find_complex_turn_restriction_idx(next_edge_idx);
    for (uint32_t idx = first_ctr_idx; idx < g.complex_turn_restrictions.size();
         ++idx) {
      if (g.complex_turn_restrictions.at(idx).trigger_edge_idx !=
          next_edge_idx) {
        // Check that we iterated at least once.
        CHECK_NE_S(idx, first_ctr_idx);
        break;
      }
      active_ctrs->push_back({.ctr_idx = idx, .position = 0});
    }

#if 0
    const auto it = g.GetComplexTRMap().find(next_edge_idx);
    if (it != g.GetComplexTRMap().end()) {
      uint32_t ctr_idx = it->second;
      do {
        const auto& ctr = g.GetComplexTRS().at(ctr_idx);
        if (ctr.GetTriggerEdgeIdx() != next_edge_idx) {
          // Check that we iterated at least once.
          CHECK_NE_S(ctr_idx, it->second);
          break;
        }
        active_ctrs->push_back({.ctr_idx = ctr_idx, .position = 0});
      } while (++ctr_idx < g.GetComplexTRS().size());
    }
#endif
  }

  // Given a previous and a new edge, compute the new active_ctr_id.
  // Return false if adding the new edge is forbidden, or true when there is a
  // new value in 'active_ctrs'.
  inline bool UpdateActiveCtrs(const MMCluster& g, const VisitedEdge& prev_v,
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
      if (!ActiveCtrsAddNextEdge(g, next_edge_idx, active_ctrs)) {
        return false;
      }
    }

    if (next_complex_trigger) {
      AddTriggeringCTRs(g, next_edge_idx, active_ctrs);
    }
    return true;
  }

  std::vector<VisitedEdge> vis_;
  std::vector<uint32_t> spanning_tree_edges_;
  std::vector<ActiveCtrs> active_ctrs_vec_;
};

inline RoutingResult RouteOnMMGraph(const MMClusterWrapper& mcw,
                                    uint32_t start_idx, uint32_t target_idx,
                                    Verbosity verb = Verbosity::Brief) {
  const MMCluster& g = mcw.g;

  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Route using mmcluster routing data";
  }

  SingleSourceMMEdgeDijkstra router;
  router.Route(mcw, {.start_idx = start_idx},
               {.handle_restricted_access = true, .include_dead_end = true});
  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Finished routing";
  }

  const auto& vis = router.GetVisitedEdges();
  RoutingResult res;
  res.route_v_idx = router.GetShortestPathToTargetSlow(g, target_idx);
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
    uint32_t from_idx = start_idx;
    uint32_t pos = 1;
    LOG_S(INFO) << "CompactDijkstra: Shortest path";
    for (uint32_t v_idx : res.route_v_idx) {
      const SingleSourceMMEdgeDijkstra::VisitedEdge& ve = vis.at(v_idx);
      uint32_t edge_idx = router.GetGraphEdgeIdx(g, v_idx);
      const MMEdge e(g.edges.at(edge_idx));
      int64_t way_id = g.grouped_way_to_osm_id.at(g.edge_to_way.at(edge_idx));
      LOG_S(INFO) << absl::StrFormat(
          "  %u. Edge %lld to %lld way:%lld minw:%u ctrid:%llu target_ra:%u "
          "done:%u",
          pos++, g.get_node_id(from_idx), g.get_node_id(e.target_idx()), way_id,
          ve.min_weight, ve.active_ctr_id, ve.in_target_restricted_access_area,
          ve.done);
      from_idx = e.target_idx();
    }
  }

  return res;
}

struct MMClusterShortestPaths {
  std::vector<std::vector<std::uint32_t>> metrics;
};

uint32_t GetBestTurnCostFollowEdge(const MMCluster& mmc, uint32_t edge_idx) {
  std::span<const uint8_t> tc = mmc.get_turn_costs(edge_idx);
  if (tc.size() == 0) {
    return INFU32;
  }
  uint32_t minpos = 0;
  for (uint32_t pos = 1; pos < tc.size(); ++pos) {
    if (tc[pos] < tc[minpos]) minpos = pos;
  }
  return mmc.edge_start_idx(mmc.get_edge(edge_idx).target_idx()) + minpos;
}

// Given the edge 'prev_edge_idx', return edge_idx of the default following
// edge. Returns INFU32 if no edge available.
uint32_t GetBestWayIdxFollowEdge(const MMCluster& mmc, uint32_t prev_edge_idx) {
  // Use turn cost as proxy for "best continuation".
  std::span<const uint8_t> tc = mmc.get_turn_costs(prev_edge_idx);
  if (tc.size() == 0) {
    return INFU32;
  }

  const uint32_t prev_way_idx = mmc.edge_to_way.at(prev_edge_idx);
  uint32_t node_idx = mmc.get_edge(prev_edge_idx).target_idx();
  uint32_t start_edge_idx = mmc.edge_start_idx(node_idx);
  uint32_t found_off = INFU32;

  for (uint32_t offset : mmc.edge_offsets(node_idx)) {
    uint32_t edge_idx = start_edge_idx + offset;
    if (mmc.edge_to_way.at(edge_idx) == prev_way_idx) {
      CHECK_LT_S(offset, tc.size());
      // uturns often stay on the same way, so don't allow these..
      if (tc[offset] < TURN_COST_U_TURN_COMPRESSED &&
          (found_off == INFU32 || (tc[offset] < tc[found_off]))) {
        found_off = offset;
      }
    }
  }
  if (found_off == INFU32) {
    return INFU32;
  }
  return start_edge_idx + found_off;
}

// Given the edge 'prev_edge_idx', return edge_idx of the default following
// edge. Returns INFU32 if no edge available.
uint32_t GetBestStreetnameFollowEdge(const MMCluster& mmc,
                                     uint32_t prev_edge_idx) {
  // Use turn cost as proxy for "best continuation".
  std::span<const uint8_t> tc = mmc.get_turn_costs(prev_edge_idx);
  if (tc.size() == 0) {
    return INFU32;
  }

  const uint32_t prev_way_idx = mmc.edge_to_way.at(prev_edge_idx);
  std::string_view prev_streetname = mmc.get_streetname(prev_way_idx);
  if (prev_streetname.empty()) {
    return INFU32;
  }
  uint32_t node_idx = mmc.get_edge(prev_edge_idx).target_idx();
  uint32_t start_edge_idx = mmc.edge_start_idx(node_idx);
  uint32_t found_off = INFU32;

  for (uint32_t offset : mmc.edge_offsets(node_idx)) {
    uint32_t edge_idx = start_edge_idx + offset;
    uint32_t way_idx = mmc.edge_to_way.at(edge_idx);
    if (prev_streetname == mmc.get_streetname(way_idx)) {
      CHECK_LT_S(offset, tc.size());
      // uturns often stay on the same way, so don't allow these..
      if (tc[offset] < TURN_COST_U_TURN_COMPRESSED &&
          (found_off == INFU32 || tc[offset] < tc[found_off])) {
        found_off = offset;
      }
    }
  }
  if (found_off == INFU32) {
    return INFU32;
  }
  return start_edge_idx + found_off;
}

void AnalyzePath(const MMCluster& mmc, const std::vector<uint32_t>& path) {
  // Wrong, but will display non-lowest-cost, which is correct.
  uint32_t prev_edge_idx = path.at(0);
  uint32_t num_special = 0;

  for (uint32_t i = 1; i < path.size(); ++i) {
    uint32_t expand_node_idx = mmc.get_edge(prev_edge_idx).target_idx();
    // Based on the previous edge, compute the default next edge.
    // uint32_t p = 0;
    uint32_t predicted_edge = GetBestStreetnameFollowEdge(mmc, prev_edge_idx);
    if (predicted_edge == INFU32) {
      // p = 1;
      predicted_edge = GetBestWayIdxFollowEdge(mmc, prev_edge_idx);
    }
    if (predicted_edge == INFU32) {
      // p = 2;
      predicted_edge = GetBestTurnCostFollowEdge(mmc, prev_edge_idx);
    }
    // LOG_S(INFO) << "predicted edge:" << predicted_edge << " p:" << p;

    uint32_t edge_idx = path.at(i);
    uint32_t way_idx = mmc.edge_to_way.at(edge_idx);
    bool predicted_selected = (edge_idx == predicted_edge);
    num_special += !predicted_selected;
    LOG_S(INFO) << absl::StrFormat(
        "%3u Edge predicted:%s node:%llu->%llu way_id:%llu streetname:<%s>", i,
        predicted_selected ? "*" : "-", mmc.get_node_id(expand_node_idx),
        mmc.get_node_id(mmc.get_edge(edge_idx).target_idx()),
        mmc.get_way_to_way_id(way_idx), mmc.get_streetname(way_idx));
    prev_edge_idx = edge_idx;
  }
  LOG_S(INFO) << absl::StrFormat("  %u of %llu are special", num_special,
                                 path.size());
}

// Compute all shortest paths between border edges in a cluster.
inline MMClusterShortestPaths ComputeShortestMMClusterPaths(
    const MMClusterWrapper& mcw, const RoutingMetric& metric, VEHICLE vt) {
  const MMCluster& mmc = mcw.g;
  MMClusterShortestPaths res;

  // Execute single source Dijkstra for every incoming border edge
  for (const MMInEdge& in_edge : mmc.in_edges.span()) {
    // Now store the metrics for this border node.
    res.metrics.emplace_back();
    SingleSourceMMEdgeDijkstra router;
    int16_t off =
        (int16_t)(in_edge.edge_idx - mmc.edge_start_idx(in_edge.from_node_idx));
    router.Route(mcw,
                 {.start_idx = in_edge.from_node_idx,
                  .edge_offset = off,
                  .edge_weight_fraction = 0.0},
                 {.handle_restricted_access = true});

    const std::vector<SingleSourceMMEdgeDijkstra::VisitedEdge>& vis =
        router.GetVisitedEdges();
    for (const MMOutEdge& out_edge : mmc.out_edges.span()) {
      // Check that the out edge has only one label.
      if (vis.at(out_edge.edge_idx).next != INFU32) {
        CHECK_EQ_S(vis.at(out_edge.edge_idx).next, out_edge.edge_idx);
      }
      res.metrics.back().push_back(vis.at(out_edge.edge_idx).min_weight);

      const std::vector<uint32_t> path =
          router.GetShortestPathFromTargetEdge(mmc, out_edge.edge_idx);
      LOG_S(INFO) << "shortest cluster path len: " << path.size();
      if (path.size() > 0) {
        AnalyzePath(mmc, path);
      }
    }

#if 0
    {
      // Visit all shortest paths (ending at a border-out-edge) and label the
      // nodes as 'cluster_skeleton'.
      const std::vector<CompactGraph::PartialEdge>& edges = cg.edges();
      for (const auto& out_edge : cluster->border_out_edges) {
        // g->nodes.at(out_edge.g_from_idx).cluster_skeleton = 1;
        uint32_t e_idx = out_edge.tmp_c_edge_idx;
        if (vis.at(e_idx).min_weight == INFU32) {
          continue;
        }
        cluster->num_valid_paths += 1;
        cluster->sum_valid_path_nodes += 1;
        do {
          uint32_t base_e_idx = router.GetBaseIdx(cg.edges().size(), e_idx);
          uint32_t gnode_target_idx =
              compact_to_graph.at(edges.at(base_e_idx).to_c_idx);
          g->nodes.at(gnode_target_idx).cluster_skeleton = 1;
          e_idx = vis.at(e_idx).from_v_idx;
          cluster->sum_valid_path_nodes += 1;
        } while (e_idx != INFU32);
      }
    }
#endif
  }
  CHECK_EQ_S(res.metrics.size(), mmc.in_edges.size());
  return res;
}
