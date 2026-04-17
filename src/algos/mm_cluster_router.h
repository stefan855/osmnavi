// Compute all shortest paths between a set of nodes, for instance between
// border nodes in a cluster, using a CompactGraph.

#pragma once
#include <algorithm>
#include <fstream>
#include <memory>
#include <queue>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "algos/mm_router_defs.h"
#include "algos/routing_defs.h"
#include "algos/routing_metric.h"
#include "base/thread_pool.h"
#include "base/util.h"

#define DEBUG_PRINT 0

#if 0
// TODO: Template the routing algorithm, making some decisions compile time
// decisions.
constexpr uint64_t MCRouterDefault = 0;
constexpr uint64_t MCRouterSkipDeadEnds = 1;
constexpr uint64_t MCRouterSkipRestrictedEdges = 2;
constexpr uint64_t MCRouterSingleSourceNoTarget = 4;
constexpr uint64_t MCRouterUseAStar = 8;
#endif

class MMClusterRouter {
 public:
  // The vector for visited edges is preallocated with mc.edges().size()
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
    std::uint32_t active_ctr_id : 29;
    // True iff the edge is one of the target edges for routing.
    std::uint32_t target_edge : 1;
    std::uint32_t in_target_restricted_access_area : 1;
    std::uint32_t done : 1;  // 1 <=> node has been finalized.
    // Is INFU32 if the slot is empty. Otherwise, it points to the next entry
    // for the same edge but with a different label. The last entry points to
    // the first entry (i.e. a loop). A list of edges has exactly one entry with
    // index < num_base_edges_. This is the 'base' index, i.e. it points to the
    // edge in mc.edges(). See GetBaseIdx().
    std::uint32_t next;
  };

  struct Options {
    bool handle_restricted_access = false;
    bool include_dead_end = false;
  };

  MMClusterRouter(const MMClusterWrapper& mcw) : mc_(mcw.mc), mcw_(mcw) { ; }

  const std::vector<VisitedEdge>& GetVisitedEdges() const { return vis_; }

  // Execute edge based single source Dijkstra (from start edges to *all*
  // reachable edge).
  void Route(const MMGeoAnchor& start_anchor, const Options opt,
             const MMGeoAnchor& target_anchor = {}) {
    Clear();
    start_anchor_ = start_anchor;
    target_anchor_ = target_anchor;
    ActiveCtrs active_ctrs;
    num_base_edges_ =
        opt.include_dead_end ? mc_.edges.size() : mc_.num_non_dead_end_edges();
    CHECK_LE_S(num_base_edges_, mcw_.edge_weights.size());

#if DEBUG_PRINT
    LOG_S(INFO) << absl::StrFormat("CompactDijkstra: Start routing at %u",
                                   start_idx);
#endif

    CHECK_LT_S(num_base_edges_, 1 << 31) << "currently not supported";
    vis_.assign(num_base_edges_ + num_base_edges_ / 50,  // Add 2%.
                {.min_weight = INFU32,
                 .from_v_idx = INFU32,
                 .active_ctr_id = NO_ACTIVE_CTR_ID,
                 .target_edge = 0,
                 .in_target_restricted_access_area = 0,
                 .done = 0,
                 .next = INFU32});

    // ===================
    // Label target edges.
    // ===================
    for (const MMEdgePoint& ep : target_anchor_.edge_points) {
      vis_.at(ep.fe.edge_idx(mc_)).target_edge = 1;
    }

    // ======================================
    // Push edge(s) of start anchor into queue.
    // ======================================
    for (const MMEdgePoint& ep : start_anchor.edge_points) {
      const uint32_t edge_idx = ep.fe.edge_idx(mc_);
      const MMEdge edge(mc_.edges.at(edge_idx));

      if (!opt.include_dead_end && (edge.bridge() || edge.dead_end())) {
        continue;
      }

      // Do the first edges trigger complex turn restrictions?
      if (edge.complex_turn_restriction_trigger()) {
        AddTriggeringCTRs(mc_, edge_idx, &active_ctrs);
      }
#if DEBUG_PRINT
      LOG_S(INFO) << absl::StrFormat(
          "Add initial edge from_idx:%u to_idx:%u way_idx:%u #ctrs:%llu",
          start_idx, edge.to_c_idx, edge.way_idx, active_ctrs.size());
#endif

      // TODO: restricted access area: should be 1 if the edge is in a
      // restricted area?
      uint32_t v_idx = FindOrAllocEdge(
          edge_idx, /*in_target_restricted_access_area=*/0, active_ctrs);

      // We're just starting, so we should be at base index i.
      CHECK_EQ_S(edge_idx, v_idx);
      vis_.at(edge_idx).min_weight =
          mcw_.edge_weights.at(edge_idx) * ep.GetWeightFractionWhenStarting();
      pq_.emplace(vis_.at(edge_idx).min_weight, edge_idx, ep.fe.from_node_idx);
    }

    // =======================================
    // Loop until the priority queue is empty.
    // =======================================
    int count = 0;
    while (!pq_.empty()) {
      ++count;
      // Remove the minimal node from the priority queue.
      const QueuedEdge qedge = pq_.top();
      pq_.pop();
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
      if (prev_v.target_edge) {
        return;
      }

      const uint32_t prev_v_base_idx = GetBaseIdx(qedge.ve_idx);
      const MMEdge prev_edge(mc_.edges.at(prev_v_base_idx));

      const uint32_t edge_start_idx =
          mc_.edge_start_idx(prev_edge.target_idx());
      const uint32_t num_edges =
          mc_.edge_stop_idx(prev_edge.target_idx()) - edge_start_idx;

      const std::span<const uint8_t> turn_costs =
          mc_.get_turn_costs(prev_v_base_idx);
      CHECK_EQ_S(turn_costs.size(), num_edges);
      // const TurnCostData& tcd = turn_costs.at(prev_edge.turn_cost_idx);

      for (uint32_t off = 0; off < num_edges; ++off) {
        const uint32_t edge_idx = edge_start_idx + off;
        const MMEdge e(mc_.edges.at(edge_idx));

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
        if (!UpdateActiveCtrs(mc_, prev_v, edge_idx,
                              e.complex_turn_restriction_trigger(),
                              &active_ctrs)) {
#if DEBUG_PRINT
          LOG_S(INFO) << "Forbidden by TR";
#endif

          continue;
        }

        uint32_t new_weight;
        if (vis_.at(edge_idx).target_edge) {
          const int64_t pos = target_anchor_.FindPosByEdgeIdx(mc_, edge_idx);
          CHECK_GE_S(pos, 0);
          const auto fraction = target_anchor_.edge_points.at(pos).fraction;
          new_weight =
              prev_v.min_weight + decompress_turn_cost(turn_costs[off]) +
              static_cast<uint32_t>(mcw_.edge_weights.at(edge_idx) * fraction +
                                    0.5);
        } else {
          new_weight = prev_v.min_weight +
                       decompress_turn_cost(turn_costs[off]) +
                       mcw_.edge_weights.at(edge_idx);
        }

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
          pq_.emplace(new_weight, new_v_idx, prev_edge.target_idx());
        }
      }
    }
  }

  // Compute the resulting route.
  MMRoutingResult GetRoutingResult() const {
    MMRoutingResult res;
    res.route_v_idx = GetShortestPathFromTarget(target_anchor_);
    res.found = !res.route_v_idx.empty();
    res.found_distance = res.route_v_idx.empty()
                             ? INFU32
                             : vis_.at(res.route_v_idx.back()).min_weight;
    res.num_shortest_route_nodes =
        res.route_v_idx.empty() ? 0 : res.route_v_idx.size() + 1;
    res.num_visited = vis_.size();
    res.start_edge.from_node_idx = INFU32;
    res.target_edge.from_node_idx = INFU32;
    if (!res.route_v_idx.empty()) {
      // Find the full edges for the first and last edge in the found path.
      // This might be complicated, because we need the from_node_idx of the
      // first edge.
      {
        int64_t pos = start_anchor_.FindPosByEdgeIdx(
            mc_, GetGraphEdgeIdx(res.route_v_idx.front()));
        CHECK_GE_S(pos, 0);
        res.start_edge = start_anchor_.edge_points.at(pos).fe;
      }
      {
        int64_t pos = target_anchor_.FindPosByEdgeIdx(
            mc_, GetGraphEdgeIdx(res.route_v_idx.back()));
        CHECK_GE_S(pos, 0);
        res.target_edge = target_anchor_.edge_points.at(pos).fe;
      }
    }
    LOG_S(INFO) << "Result metric:" << res.found_distance;
    return res;
  }

  // Return the v_idx of the entry with the lowest min_metric.
  uint32_t GetLowestMetricIdxAt(const uint32_t v_idx) const {
    if (vis_.at(v_idx).next == INFU32) {
      return v_idx;
    }
    uint32_t best_i = v_idx;
    uint32_t i = vis_.at(best_i).next;
    while (i != v_idx) {
      if (vis_.at(i).min_weight < vis_.at(best_i).min_weight) {
        best_i = i;
      }
      i = vis_.at(i).next;
    }
    return best_i;
  }

  // Get the path finishing at edge 'v_edge_idx' after routing has run.
  // Returns an empty vector if no path exists, or a vector of edge indexes,
  // from in_edge to out_edge.
  std::vector<uint32_t> GetShortestPathFromTargetEdge(
      uint32_t v_edge_idx) const {
    if (vis_.at(v_edge_idx).min_weight == INFU32) {
      return {};  // No path.
    }

    std::vector<uint32_t> v;
    for (uint32_t idx = v_edge_idx; idx != INFU32;) {
      v.push_back(GetGraphEdgeIdx(idx));
      idx = vis_.at(idx).from_v_idx;
    }
    // Reverse direction.
    std::reverse(v.begin(), v.end());
    return v;
  }

  // Get the shortest path from all edges in 'target'.
  std::vector<uint32_t> GetShortestPathFromTarget(
      const MMGeoAnchor& target) const {
    if (target.edge_points.empty()) {
      return {};  // No path.
    }
    uint32_t best_idx =
        GetLowestMetricIdxAt(target.edge_points.at(0).fe.edge_idx(mc_));
    for (uint32_t i = 1; i < target.edge_points.size(); ++i) {
      uint32_t v_idx =
          GetLowestMetricIdxAt(target.edge_points.at(i).fe.edge_idx(mc_));
      if (vis_.at(v_idx).min_weight < vis_.at(best_idx).min_weight) {
        best_idx = v_idx;
      }
    }

    return GetShortestPathFromTargetEdge(best_idx);
  }

#if 0
  // Construct the shortest path at target_node_idx. This is slow and should
  // only be used for small graphs in testing.
  // Returns the visited edge indexes on the path.
  std::vector<uint32_t> GetShortestPathToTarget(const MMCluster& mc,
                                                uint32_t base_edge_idx,
                                                double fraction) const {
    // Search the edge index with minimal weight that corresponds to
    // 'target_edge_idx'. Because of labelling (complex tuern restrictions and
    // restricted areas)a there can be multiple entries for the same
    // 'target_edge_idx'.
    uint32_t found_min_weight = INFU32;
    uint32_t found_edge_idx = INFU32;
    if (vis_.at(base_edge_idx).min_weight != INFU32) {
      uint32_t curr_idx = base_edge_idx;
      do {
        const VisitedEdge& ve = vis_.at(curr_idx);
        if (ve.min_weight < found_min_weight) {
          found_min_weight = ve.min_weight;
          found_edge_idx = curr_idx;
        }
        curr_idx = ve.next;
      } while (curr_idx != base_edge_idx);
    }
    std::vector<uint32_t> v;
    if (found_edge_idx == INFU32) {
      return v;
    }

    // Now fill the edge indexes in backward direction.
    for (uint32_t idx = found_edge_idx; idx != INFU32;) {
      v.push_back(idx);
      idx = vis_.at(idx).from_v_idx;
    }
    // Reverse direction.
    std::reverse(v.begin(), v.end());
    return v;
  }
#endif

#if 0
  // Construct the shortest path at target_node_idx. This is slow and should
  // only be used for small graphs in testing.
  // Returns the visited edge indexes on the path.
  std::vector<uint32_t> GetShortestPathToTargetSlow(
      const MMCluster& mc, uint32_t target_node_idx) const {
    // Find the minimal visited edge that ends at 'target_node_idx'.
    uint32_t found_min_weight = INFU32;
    uint32_t found_last_edge_idx = INFU32;
    for (uint32_t edge_idx = 0; edge_idx < num_base_edges_; ++edge_idx) {
      if (MM_EDGE(mc.edges.at(edge_idx)).target_idx() == target_node_idx &&
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
#endif

  inline uint32_t GetGraphEdgeIdx(uint32_t v_idx) const {
    return GetBaseIdx(v_idx);
  }

  inline MMEdge GetGraphEdge(uint32_t v_idx) const {
    return mc_.get_edge(GetGraphEdgeIdx(v_idx));
  }

  inline uint32_t GetBaseIdx(uint32_t v_idx) const {
    if (v_idx < num_base_edges_) return v_idx;
    while (vis_.at(v_idx).next >= num_base_edges_) {
      v_idx = vis_.at(v_idx).next;
    }
    return vis_.at(v_idx).next;
  }

  const VisitedEdge& GetVEdge(uint32_t v_idx) const { return vis_.at(v_idx); }

#if 0
  inline const std::vector<ActiveCtrs>& GetActiveCtrVec() const {
    return active_ctrs_vec_;
  }
#endif

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
    active_ctrs_vec_.clear();
    // Add element 0 which denotes "no active ctrs".
    active_ctrs_vec_.push_back({});
    CHECK_S(active_ctrs_vec_.at(NO_ACTIVE_CTR_ID).empty());

    if (!pq_.empty()) {
      // No clear() function available, use swap trick.
      std::priority_queue<QueuedEdge, std::vector<QueuedEdge>, MetricCmpEdge>
          empty_queue;
      std::swap(pq_, empty_queue);
    }
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
         .target_edge = vis_.at(v_base_idx).target_edge,
         .in_target_restricted_access_area = in_target_restricted_access_area,
         .done = 0,
         .next = v_base_next_val});
    // Do not use v_base, pushing to vector might invalidated the reference.
    vis_.at(v_base_idx).next = v_curr_idx;
    return v_curr_idx;
  }

  static inline void AddTriggeringCTRs(const MMCluster& mc,
                                       uint32_t next_edge_idx,
                                       ActiveCtrs* active_ctrs) {
    // Find new triggering turn restrictions.
    uint32_t first_ctr_idx =
        mc.find_complex_turn_restriction_idx(next_edge_idx);
    for (uint32_t idx = first_ctr_idx;
         idx < mc.complex_turn_restrictions.size(); ++idx) {
      if (mc.complex_turn_restrictions.at(idx).trigger_edge_idx !=
          next_edge_idx) {
        // Check that we iterated at least once.
        CHECK_NE_S(idx, first_ctr_idx);
        break;
      }
      active_ctrs->push_back({.ctr_idx = idx, .position = 0});
    }

#if 0
    const auto it = mc.GetComplexTRMap().find(next_edge_idx);
    if (it != mc.GetComplexTRMap().end()) {
      uint32_t ctr_idx = it->second;
      do {
        const auto& ctr = mc.GetComplexTRS().at(ctr_idx);
        if (ctr.GetTriggerEdgeIdx() != next_edge_idx) {
          // Check that we iterated at least once.
          CHECK_NE_S(ctr_idx, it->second);
          break;
        }
        active_ctrs->push_back({.ctr_idx = ctr_idx, .position = 0});
      } while (++ctr_idx < mc.GetComplexTRS().size());
    }
#endif
  }

  // Given a previous and a new edge, compute the new active_ctr_id.
  // Return false if adding the new edge is forbidden, or true when there is a
  // new value in 'active_ctrs'.
  inline bool UpdateActiveCtrs(const MMCluster& mc, const VisitedEdge& prev_v,
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
      if (!ActiveCtrsAddNextEdge(mc, next_edge_idx, active_ctrs)) {
        return false;
      }
    }

    if (next_complex_trigger) {
      AddTriggeringCTRs(mc, next_edge_idx, active_ctrs);
    }
    return true;
  }

  // The number of edges to be considered. This has two possible values,
  // depending on if dead ends are excluded or not.
  uint32_t num_base_edges_;

  // Input.
  const MMCluster& mc_;
  const MMClusterWrapper& mcw_;
  MMGeoAnchor start_anchor_;
  MMGeoAnchor target_anchor_;

  std::priority_queue<QueuedEdge, std::vector<QueuedEdge>, MetricCmpEdge> pq_;
  std::vector<VisitedEdge> vis_;
  std::vector<ActiveCtrs> active_ctrs_vec_;
};

struct MMClusterShortestPaths {
  std::vector<std::vector<std::uint32_t>> metrics;
};

uint32_t GetBestTurnCostFollowEdge(const MMCluster& mc, uint32_t edge_idx) {
  std::span<const uint8_t> tc = mc.get_turn_costs(edge_idx);
  if (tc.size() == 0) {
    return INFU32;
  }
  uint32_t minpos = 0;
  for (uint32_t pos = 1; pos < tc.size(); ++pos) {
    if (tc[pos] < tc[minpos]) minpos = pos;
  }
  return mc.edge_start_idx(mc.get_edge(edge_idx).target_idx()) + minpos;
}

// Given the edge 'prev_edge_idx', return edge_idx of the default following
// edge. Returns INFU32 if no edge available.
uint32_t GetBestWayIdxFollowEdge(const MMCluster& mc, uint32_t prev_edge_idx) {
  // Use turn cost as proxy for "best continuation".
  std::span<const uint8_t> tc = mc.get_turn_costs(prev_edge_idx);
  if (tc.size() == 0) {
    return INFU32;
  }

  const uint32_t prev_way_idx = mc.edge_to_way.at(prev_edge_idx);
  uint32_t node_idx = mc.get_edge(prev_edge_idx).target_idx();
  uint32_t start_edge_idx = mc.edge_start_idx(node_idx);
  uint32_t found_off = INFU32;

  for (uint32_t offset : mc.edge_offsets(node_idx)) {
    uint32_t edge_idx = start_edge_idx + offset;
    if (mc.edge_to_way.at(edge_idx) == prev_way_idx) {
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
uint32_t GetBestStreetnameFollowEdge(const MMCluster& mc,
                                     uint32_t prev_edge_idx) {
  // Use turn cost as proxy for "best continuation".
  std::span<const uint8_t> tc = mc.get_turn_costs(prev_edge_idx);
  if (tc.size() == 0) {
    return INFU32;
  }

  const uint32_t prev_way_idx = mc.edge_to_way.at(prev_edge_idx);
  std::string_view prev_streetname = mc.get_streetname(prev_way_idx);
  if (prev_streetname.empty()) {
    return INFU32;
  }
  uint32_t node_idx = mc.get_edge(prev_edge_idx).target_idx();
  uint32_t start_edge_idx = mc.edge_start_idx(node_idx);
  uint32_t found_off = INFU32;

  for (uint32_t offset : mc.edge_offsets(node_idx)) {
    uint32_t edge_idx = start_edge_idx + offset;
    uint32_t way_idx = mc.edge_to_way.at(edge_idx);
    if (prev_streetname == mc.get_streetname(way_idx)) {
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

// Check how well edge prediction would work.
void AnalyzePath(const MMCluster& mc, const std::vector<uint32_t>& path) {
  // Wrong, but will display non-lowest-cost, which is correct.
  uint32_t prev_edge_idx = path.at(0);
  uint32_t num_special = 0;

  for (uint32_t i = 1; i < path.size(); ++i) {
    // uint32_t expand_node_idx = mc.get_edge(prev_edge_idx).target_idx();
    // Based on the previous edge, compute the default next edge.
    // uint32_t p = 0;
    uint32_t predicted_edge = GetBestStreetnameFollowEdge(mc, prev_edge_idx);
    if (predicted_edge == INFU32) {
      // p = 1;
      predicted_edge = GetBestWayIdxFollowEdge(mc, prev_edge_idx);
    }
    if (predicted_edge == INFU32) {
      // p = 2;
      predicted_edge = GetBestTurnCostFollowEdge(mc, prev_edge_idx);
    }
    // LOG_S(INFO) << "predicted edge:" << predicted_edge << " p:" << p;

    uint32_t edge_idx = path.at(i);
    // uint32_t way_idx = mc.edge_to_way.at(edge_idx);
    bool predicted_selected = (edge_idx == predicted_edge);
    num_special += !predicted_selected;
#if 0
    LOG_S(INFO) << absl::StrFormat(
        "%3u Edge predicted:%s node:%llu->%llu way_id:%llu streetname:<%s>", i,
        predicted_selected ? "*" : "-", mc.get_node_id(expand_node_idx),
        mc.get_node_id(mc.get_edge(edge_idx).target_idx()),
        mc.get_way_to_way_id(way_idx), mc.get_streetname(way_idx));
#endif
    prev_edge_idx = edge_idx;
  }
#if 0
  LOG_S(INFO) << absl::StrFormat("  %u of %llu are special", num_special,
                                 path.size());
#endif
}

// Compute all shortest paths between border edges in a cluster.
inline MMClusterShortestPaths ComputeShortestMMClusterPaths(
    const MMClusterWrapper& mcw, const RoutingMetric& metric, VEHICLE vt) {
  const MMCluster& mc = mcw.mc;
  MMClusterShortestPaths res;

  // Execute single source Dijkstra for every incoming border edge
  for (const MMInEdge& in_edge : mc.in_edges.span()) {
    // Now store the metrics for this border node.
    res.metrics.emplace_back();
    MMClusterRouter router(mcw);
    int16_t off =
        (int16_t)(in_edge.edge_idx - mc.edge_start_idx(in_edge.from_node_idx));

    MMGeoAnchor ga;
    ga.AddEdge(mc, 1.0, in_edge.from_node_idx, off);
    router.Route(ga, {.handle_restricted_access = true});

    const std::vector<MMClusterRouter::VisitedEdge>& vis =
        router.GetVisitedEdges();
    for (const MMOutEdge& out_edge : mc.out_edges.span()) {
      // Check that the out edge has only one label.
      if (vis.at(out_edge.edge_idx).next != INFU32) {
        CHECK_EQ_S(vis.at(out_edge.edge_idx).next, out_edge.edge_idx);
      }
      res.metrics.back().push_back(vis.at(out_edge.edge_idx).min_weight);

      const std::vector<uint32_t> path =
          router.GetShortestPathFromTargetEdge(out_edge.edge_idx);
      // LOG_S(INFO) << "shortest cluster path len: " << path.size();
      if (path.size() > 0) {
        AnalyzePath(mc, path);
      }
    }

#if 0
    {
      // Visit all shortest paths (ending at a border-out-edge) and label the
      // nodes as 'cluster_skeleton'.
      const std::vector<CompactGraph::PartialEdge>& edges = cg.edges();
      for (const auto& out_edge : cluster->border_out_edges) {
        // mc->nodes.at(out_edge.g_from_idx).cluster_skeleton = 1;
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
          mc->nodes.at(gnode_target_idx).cluster_skeleton = 1;
          e_idx = vis.at(e_idx).from_v_idx;
          cluster->sum_valid_path_nodes += 1;
        } while (e_idx != INFU32);
      }
    }
#endif
  }
  CHECK_EQ_S(res.metrics.size(), mc.in_edges.size());
  return res;
}

inline MMRoutingResult RouteOnMMCluster(const MMClusterWrapper& mcw,
                                        const MMGeoAnchor& start_anchor,
                                        const MMGeoAnchor& target_anchor,
                                        Verbosity verb = Verbosity::Brief) {
  const MMCluster& mc = mcw.mc;
  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Route using mmcluster routing data";
  }

  MMClusterRouter router(mcw);
  router.Route(start_anchor,
               {.handle_restricted_access = true, .include_dead_end = true},
               target_anchor);
  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Finished routing";
  }

  MMRoutingResult res = router.GetRoutingResult();
  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Result metric:" << res.found_distance;
  }

  // Print path.
  if (verb >= Verbosity::Verbose) {
    uint32_t from_idx = res.start_edge.from_node_idx;
    uint32_t pos = 1;
    const auto& vis = router.GetVisitedEdges();
    LOG_S(INFO) << "MMClusterRouter shortest path #edges:"
                << res.route_v_idx.size();
    for (uint32_t v_idx : res.route_v_idx) {
      const MMClusterRouter::VisitedEdge& ve = vis.at(v_idx);
      uint32_t edge_idx = router.GetGraphEdgeIdx(v_idx);
      const MMEdge e(mc.edges.at(edge_idx));
      int64_t way_id = mc.grouped_way_to_osm_id.at(mc.edge_to_way.at(edge_idx));
      LOG_S(INFO) << absl::StrFormat(
          "  %u. Edge %lld to %lld way:%lld minw:%u ctrid:%llu target_ra:%u "
          "done:%u",
          pos++, mc.get_node_id(from_idx), mc.get_node_id(e.target_idx()),
          way_id, ve.min_weight, ve.active_ctr_id,
          ve.in_target_restricted_access_area, ve.done);
      from_idx = e.target_idx();
    }
  }

  return res;
}

inline MMRoutingResult RouteOnMMClusterFromNodes(
    const MMClusterWrapper& mcw, uint32_t start_idx, uint32_t target_idx,
    Verbosity verb = Verbosity::Brief) {
  MMGeoAnchor start_anchor;
  start_anchor.AddStartNode(mcw.mc, start_idx);
  MMGeoAnchor target_anchor;
  target_anchor.AddTargetNode(mcw.mc, target_idx);
  return RouteOnMMCluster(mcw, start_anchor, target_anchor, verb);
}
