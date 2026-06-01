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
#include "base/util.h"

#define DEBUG_PRINT 0

class MMClusterRouter final {
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
    // The minimal metric seen so far. INFU32 if unused.
    std::uint32_t min_metric;
    // Previous edge entry. INFU32 if prevous entry does not exist.
    std::uint32_t from_v_idx;
    std::uint32_t active_ctr_id : 27;
    // True iff the edge is one of the target edges for routing.
    // This is statically set to 1 for all target edges, 0 for all other edges.
    std::uint32_t is_target_edge : 1;
    // True iff it should be ignored that this is a target edge. This is
    // set during routing when a pushed start edge is also a target edge and the
    // starting point is after the target point on the edge.
    // Background: In order to reach the target point, we have to re-enter the
    // edge from the beginning. Imagine a cycle of one-way edges and we start on
    // the same edge as we finish, but the start point is after the target
    // point. 'ignore_target_edge' needs to be part of the "key" of the edge,
    // because we want to travel the edge twice..
    std::uint32_t ignore_target_edge : 1;
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

  MMClusterRouter(const MMClusterWrapper& mcw, const Options& opt)
      : mc_(mcw.mc),
        mcw_(mcw),
        opt_(opt),
        outgoing_edge_idx_stop_(mc_.num_border_nodes < mc_.nodes.size()
                                    ? mc_.edge_start_idx(mc_.num_border_nodes)
                                    : mc_.edges.size()) {}

  const std::vector<VisitedEdge>& GetVisitedEdges() const { return vis_; }

 private:
  void LabelTargetEdges() {
    for (const MMEdgePoint& ep : target_anchor_.edge_points) {
      vis_.at(ep.fe.edge_idx(mc_)).is_target_edge = 1;
    }
  }

  void PushStartEdges(uint32_t start_metric_offset) {
    for (const MMEdgePoint& ep : start_anchor_.edge_points) {
      const uint32_t edge_idx = ep.fe.edge_idx(mc_);
      const MMEdge edge(mc_.edges.at(edge_idx));

      if (!opt_.include_dead_end && (edge.bridge() || edge.dead_end())) {
        continue;
      }

      // Do the first edges trigger complex turn restrictions?
      ActiveCtrs active_ctrs;
      if (edge.complex_turn_restriction_trigger()) {
        AddTriggeringCTRs(mc_, edge_idx, &active_ctrs);
      }
#if DEBUG_PRINT
      LOG_S(INFO) << absl::StrFormat(
          "Add initial edge from_idx:%u to_idx:%u way_idx:%u #ctrs:%llu",
          start_idx, edge.target_idx(), edge.way_idx, active_ctrs.size());
#endif

      // TODO: restricted access area: should be 1 if the edge is in a
      // restricted area?
      uint32_t v_idx = FindOrAllocEdge(
          edge_idx, /*in_target_restricted_access_area=*/0, active_ctrs);

      // We're just starting, so we should be at base index i.
      CHECK_EQ_S(edge_idx, v_idx);

      VisitedEdge& vis = vis_.at(edge_idx);

      // The fraction of the edge that we have to travel.
      float use_fraction = ep.GetFromFraction();

      if (vis.is_target_edge) {
        // Special case where the start edge is also a target edge.
        // We only can finish on this edge if start point <= target point.
        const uint32_t pos = target_anchor_.FindPosByEdgeIdx(mc_, edge_idx);
        CHECK_NE_S(pos, INFU32);
        const auto target_fraction =
            target_anchor_.edge_points.at(pos).to_fraction;
        if (target_fraction < ep.to_fraction) {
          vis.ignore_target_edge = true;
        } else {
          // Special case, we start and stop on the same edge.
          use_fraction = std::max(0.0f, target_fraction - ep.to_fraction);
        }
      }
      vis.min_metric =
          start_metric_offset +
          static_cast<uint32_t>(use_fraction * mcw_.edge_weights.at(edge_idx));
      pq_.emplace(vis.min_metric, edge_idx);
    }
  }

 public:
  // Return the minimum metric of edges in the queue.
  // Returns INFU32 if the queue is empty.
  uint32_t QueueMinMetric() {
    return !pq_.empty() ? pq_.top().min_metric : INFU32;
  }

  uint32_t QueueMinVIdx() { return !pq_.empty() ? pq_.top().ve_idx : INFU32; }

  uint32_t QueueSize() { return pq_.size(); }

  // Execute edge based single source Dijkstra (from start edges to *all*
  // reachable edge).
  void RouteInit(const MMGeoAnchor& start_anchor,
                 const MMGeoAnchor& target_anchor = {},
                 uint32_t start_metric_offset = 0) {
    Clear();
    start_anchor_ = start_anchor;
    target_anchor_ = target_anchor;
    num_base_edges_ =
        opt_.include_dead_end ? mc_.edges.size() : mc_.num_non_dead_end_edges();
    CHECK_LE_S(num_base_edges_, mcw_.edge_weights.size());

#if DEBUG_PRINT
    LOG_S(INFO) << absl::StrFormat("MMClusterRouter: Start routing at %u",
                                   start_idx);
#endif

    CHECK_LT_S(num_base_edges_, 1 << 31) << "currently not supported";
    vis_.assign(num_base_edges_ + num_base_edges_ / 50,  // Add 2%.
                {.min_metric = INFU32,
                 .from_v_idx = INFU32,
                 .active_ctr_id = NO_ACTIVE_CTR_ID,
                 .is_target_edge = 0,
                 .ignore_target_edge = 0,
                 .in_target_restricted_access_area = 0,
                 .done = 0,
                 .next = INFU32});

    LabelTargetEdges();
    PushStartEdges(start_metric_offset);
  }

  void AddIncomingEdge(const MMIncomingEdge& in_edge, uint32_t min_metric) {
    const MMEdge edge(mc_.edges.at(in_edge.edge_idx));
    CHECK_S(!edge.bridge() || edge.dead_end());
    CHECK_S(!edge.complex_turn_restriction_trigger());
    CHECK_S(!edge.restricted());

#if DEBUG_PRINT
    LOG_S(INFO) << "Add " << in_edge.DebugString();
#endif
    ActiveCtrs active_ctrs;  // empty.
    uint32_t v_idx = FindOrAllocEdge(
        in_edge.edge_idx, /*in_target_restricted_access_area=*/0, active_ctrs);
    VisitedEdge& vis = vis_.at(v_idx);
    if (vis.min_metric < min_metric || vis.done) {
      // The entry already exists, has a smaller target time.
      return;
    }
    vis.min_metric = min_metric;
    // Implicit linkage because this is an incoming edge and can't be reached
    // from within a cluster.
    // CHECK_EQ_S(vis.from_v_idx, INFU32);
    CHECK_EQ_S(vis.active_ctr_id, NO_ACTIVE_CTR_ID);
    CHECK_EQ_S(vis.in_target_restricted_access_area, 0);
    // This should go top in the queue.
    pq_.emplace(vis.min_metric, in_edge.edge_idx);
  }

  // Route one step and return the status of the router, i.e if we need to
  // continue routing or if we finished (successfully or not).
  inline MMClusterRouterStatus RouteOneStep() {
    // Remove the minimal node from the priority queue.
    if (pq_.empty()) {
      return {.finished = true, .found = false, .last_v_idx = INFU32};
    }
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
                      qedge.ve_idx, prev_v.min_metric, prev_v.from_v_idx,
                      prev_v.active_ctr_id,
                      prev_v.in_target_restricted_access_area, prev_v.done,
                      prev_v.next);
#endif

    if (prev_v.done == 1) {
      return {.finished = false,
              .found = false};  // "old" entry in priority queue.
    }
    CHECK_EQ_S(qedge.min_metric, prev_v.min_metric);
    vis_.at(qedge.ve_idx).done = 1;
    if (prev_v.is_target_edge && !prev_v.ignore_target_edge) {
      return {.finished = true, .found = true, .last_v_idx = qedge.ve_idx};
    }

    const uint32_t prev_v_base_idx = GetBaseIdx(qedge.ve_idx);
    const MMEdge prev_edge(mc_.edges.at(prev_v_base_idx));

    const uint32_t edge_start_idx = mc_.edge_start_idx(prev_edge.target_idx());
    const uint32_t num_edges =
        mc_.edge_stop_idx(prev_edge.target_idx()) - edge_start_idx;

    const std::span<const uint8_t> turn_costs =
        mc_.get_turn_costs(prev_v_base_idx);
    CHECK_EQ_S(turn_costs.size(), num_edges);
    // const TurnCostData& tcd = turn_costs.at(prev_edge.turn_cost_idx);

    for (uint32_t off = 0; off < num_edges; ++off) {
      const uint32_t edge_idx = edge_start_idx + off;
      const MMEdge e(mc_.edges.at(edge_idx));

      if (!opt_.include_dead_end && (e.bridge() || e.dead_end())) {
        continue;
      }

#if DEBUG_PRINT
      LOG_S(INFO) << absl::StrFormat(
          "Examine edge from_idx:%u to_idx:%u way_idx:%u",
          prev_edge.target_idx(), e.target_idx(), e.way_idx);
#endif

      if ((turn_costs[off] == TURN_COST_INFINITY_COMPRESSED) ||
          (opt_.handle_restricted_access &&
           prev_v.in_target_restricted_access_area && !e.restricted())) {
        // At least one of these holds:
        // 1) Not all edges are allowed because of a TR
        // 2) We're in the target restricted access area, not allowed to
        // transition to a free edge.
        continue;
      }

      // Create/Update the turn restriction edge key information for the
      // current edge.
      // ActiveCtrs active_ctrs;
      active_ctrs_.clear();
      if (!UpdateActiveCtrs(mc_, prev_v, edge_idx,
                            e.complex_turn_restriction_trigger(),
                            &active_ctrs_)) {
#if DEBUG_PRINT
        LOG_S(INFO) << "Forbidden by TR";
#endif

        continue;
      }

      uint32_t new_metric;
      if (vis_.at(edge_idx).is_target_edge) {
        const uint32_t pos = target_anchor_.FindPosByEdgeIdx(mc_, edge_idx);
        CHECK_NE_S(pos, INFU32);
        const auto fraction = target_anchor_.edge_points.at(pos).to_fraction;
        new_metric = prev_v.min_metric + decompress_turn_cost(turn_costs[off]) +
                     static_cast<uint32_t>(
                         mcw_.edge_weights.at(edge_idx) * fraction + 0.5);
      } else {
        new_metric = prev_v.min_metric + decompress_turn_cost(turn_costs[off]) +
                     mcw_.edge_weights.at(edge_idx);
      }

      const bool in_target_raa = opt_.handle_restricted_access &&
                                 (prev_v.in_target_restricted_access_area ||
                                  (!prev_edge.restricted() && e.restricted()));

      const uint32_t new_v_idx =
          FindOrAllocEdge(edge_idx, in_target_raa, active_ctrs_);

#if DEBUG_PRINT
      LOG_S(INFO) << absl::StrFormat(
          "Push edge from_idx:%u to_idx:%u way_idx:%u target_ra:%u #ctr:%llu",
          prev_edge.target_idx(), e.target_idx(), e.way_idx, in_target_raa,
          active_ctrs_.size());
#endif

      VisitedEdge& ve = vis_.at(new_v_idx);
      if (!ve.done && new_metric < ve.min_metric) {
        ve.min_metric = new_metric;
        ve.from_v_idx = qedge.ve_idx;
        pq_.emplace(new_metric, new_v_idx /*, prev_edge.target_idx()*/);
      }
    }
    return {.finished = false, .found = false};
  }

  // Execute edge based Dijkstra.
  MMClusterRouterStatus Route(const MMGeoAnchor& start_anchor,
                              const MMGeoAnchor& target_anchor = {},
                              uint32_t start_metric_offset = 0) {
    RouteInit(start_anchor, target_anchor, start_metric_offset);

    // ====================================================================
    // Loop until the priority queue is empty or a target edge was reached.
    // ====================================================================
    MMClusterRouterStatus st;
    do {
      st = RouteOneStep();
    } while (!st.finished);
    return st;
  }

  // Compute the resulting route.
  MMRoutingResult GetRoutingResult(const uint32_t last_v_idx) const {
    MMRoutingResult res;

    // Array of v_idx from start to end.
    const std::vector<uint32_t> v_arr = GetForwardPath(last_v_idx);
    CHECK_S(!v_arr.empty());

    // Fill start edge.
    {
      // The first edge must be either a start edge or an incoming edge.
      const uint32_t graph_edge_idx = GetGraphEdgeIdx(v_arr.at(0));
      const uint32_t start_edge_pos =
          start_anchor_.FindPosByEdgeIdx(mc_, graph_edge_idx);
      res.start_is_anchor = (start_edge_pos != INFU32);
      if (res.start_is_anchor) {
        // We have a start edge!
        res.start = start_anchor_.edge_points.at(start_edge_pos);
      } else {
        // Is this an incoming edge?
        uint32_t in_edge_pos = mc_.find_incoming_edge_pos(graph_edge_idx);
        CHECK_NE_S(in_edge_pos, INFU32);  // TODO: Can this occur?
        const MMIncomingEdge& in_edge = mc_.in_edges.at(in_edge_pos);
        res.start.fe = MMFullEdge::CreateWithEdgeIdx(mc_, in_edge.from_node_idx,
                                                     graph_edge_idx);
        // This edge is a copy of an outgoing edge in another router,
        // therefore it should not count against the metric, i.e. we skip it
        // 100%.
        res.start.distance_cm = 0;
        res.start.to_fraction = 1.0;
      }
    }

    // Fill path information.
    res.full_edges.reserve(v_arr.size());
    res.min_metrics.reserve(v_arr.size());
    res.edge_weights.reserve(v_arr.size());
    {
      uint32_t prev_metric = 0;  // TODO: wrong when starting on in_edge.
      for (uint32_t pos = 0; pos < v_arr.size(); ++pos) {
        const uint32_t v_idx = v_arr.at(pos);
        if (pos == 0) {
          res.full_edges.push_back(res.start.fe);
          CHECK_EQ_S(res.start.fe.edge_idx(mc_), GetGraphEdgeIdx(v_idx));
        } else {
          uint32_t from_node_idx = res.full_edges.back().target_idx(mc_);
          res.full_edges.push_back(MMFullEdge::CreateWithEdgeIdx(
              mc_, from_node_idx, GetGraphEdgeIdx(v_idx)));
        }
        const MMClusterRouter::VisitedEdge& ve = GetVEdge(v_idx);
        res.min_metrics.push_back(ve.min_metric);
        res.edge_weights.push_back(ve.min_metric - prev_metric);
        prev_metric = ve.min_metric;
        /*
        LOG_S(INFO) << "Fill path edge "
                    << res.full_edges.back().DebugString(mc_)
                    << " metric:" << res.edge_weights.back();
                    */
      }
    }

    // Fill target edge.
    {
      const uint32_t graph_edge_idx = GetGraphEdgeIdx(v_arr.back());
      const uint32_t target_edge_pos =
          target_anchor_.FindPosByEdgeIdx(mc_, graph_edge_idx);
      res.target_is_anchor = (target_edge_pos != INFU32);
      if (res.target_is_anchor) {
        // We have a target edge!
        res.target = target_anchor_.edge_points.at(target_edge_pos);
        CHECK_EQ_S(res.target.fe.edge_idx(mc_),
                   res.full_edges.back().edge_idx(mc_));
      } else {
        res.target.fe = res.full_edges.back();
        res.target.distance_cm =
            mc_.edge_to_distance.at(res.target.fe.edge_idx(mc_));
        res.target.to_fraction = 1.0;
      }
      LOG_S(INFO) << "target_is_anchor=" << res.target_is_anchor << " "
                  << res.target.DebugString(mc_, 0, 0);
    }

    res.final_metric = GetVEdge(last_v_idx).min_metric;
    return res;
  }

  // Get the path finishing at edge 'v_edge_idx' after routing has run.
  // Returns an empty vector if no path exists, or a vector of edge indexes,
  // from in_edge to out_edge.
  std::vector<uint32_t> GetForwardPath(uint32_t v_edge_idx) const {
    if (vis_.at(v_edge_idx).min_metric == INFU32) {
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

#if 0
  // Return the v_idx of the entry with the lowest min_metric.
  uint32_t GetLowestMetricIdxAt(const uint32_t base_v_idx) const {
    if (vis_.at(base_v_idx).next == INFU32) {
      return base_v_idx;
    }
    uint32_t best_i = base_v_idx;
    uint32_t i = vis_.at(best_i).next;
    while (i != base_v_idx) {
      if (vis_.at(i).min_metric < vis_.at(best_i).min_metric) {
        best_i = i;
      }
      i = vis_.at(i).next;
    }
    return best_i;
  }

  // Get the shortest path finishing at an edge in 'target'.
  // Returns an empty path if there is no shortest path ending at 'target'.
  std::vector<uint32_t> GetShortestPathToGeoAnchor(
      const MMGeoAnchor& target) const {
    if (target.edge_points.empty()) {
      return {};  // No path.
    }
    uint32_t best_idx =
        GetLowestMetricIdxAt(target.edge_points.at(0).fe.edge_idx(mc_));
    for (uint32_t i = 1; i < target.edge_points.size(); ++i) {
      uint32_t v_idx =
          GetLowestMetricIdxAt(target.edge_points.at(i).fe.edge_idx(mc_));
      if (vis_.at(v_idx).min_metric < vis_.at(best_idx).min_metric) {
        best_idx = v_idx;
      }
    }

    return GetForwardPath(best_idx);
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

  // Return true if the edge at 'v_idx' was used as start edge.
  inline bool IsStartEdge(uint32_t v_idx) const {
    return start_anchor_.FindPosByEdgeIdx(mc_, GetBaseIdx(v_idx)) != INFU32;
  }

  const VisitedEdge& GetVEdge(uint32_t v_idx) const { return vis_.at(v_idx); }

  inline bool IsOutgoingEdge(uint32_t v_idx) {
    uint32_t base_idx = GetBaseIdx(v_idx);
    return base_idx < outgoing_edge_idx_stop_ &&
           mc_.get_node(mc_.get_edge(base_idx).target_idx()).off_cluster_node();
  }

 private:
  static constexpr uint32_t NO_ACTIVE_CTR_ID = 0;
  // This might exist multiple times for each node, when a node gets
  // reinserted into the priority queue with a lower priority.
  struct QueuedEdge {
    std::uint32_t min_metric;
    std::uint32_t ve_idx;  // Index into visited_edges vector.
  };

  struct MetricCmpEdge {
    bool operator()(const QueuedEdge& left, const QueuedEdge& right) const {
      return left.min_metric > right.min_metric;
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

  // Find or allocate a visited edge element at 'v_base_index'. Each base
  // index might contain a list of visited edge, all for the same edge in the
  // graph, but with different labels. If edge+label is not found, then a new
  // edge is allocated at the end of the vector and added to the list of edges
  // at this specific base index.
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
      if (v_curr.ignore_target_edge) {
        // Never return an edge with 'ignore_target_edge' set.
        continue;
      }
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
    const uint32_t v_base_next_val = v_base.next;
    // Note: this potentially invalidates reference 'v_base' above, so don't use
    // it afterwards.
    vis_.push_back(
        {.min_metric = INFU32,
         .from_v_idx = INFU32,
         .active_ctr_id = active_ctr_id,
         .is_target_edge = vis_.at(v_base_idx).is_target_edge,
         .ignore_target_edge = 0,
         .in_target_restricted_access_area = in_target_restricted_access_area,
         .done = 0,
         .next = v_base_next_val});
    // Do not use v_base, pushing to vector above  might have invalidated the
    // reference.
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
  }

  // Given a previous and a new edge, compute the new active_ctr_id.
  // Return false if adding the new edge is forbidden, or true when there is a
  // new value in 'active_ctrs'.
  // Note that 'active_ctrs' has to be empty when calling this function.
  inline bool UpdateActiveCtrs(const MMCluster& mc, const VisitedEdge& prev_v,
                               const uint32_t next_edge_idx,
                               bool next_complex_trigger,
                               ActiveCtrs* active_ctrs) {
    if (prev_v.active_ctr_id == 0 && !next_complex_trigger) {
      // Common case, no turn restriction active, no turn restriction
      // triggered.
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
  const Options opt_;
  MMGeoAnchor start_anchor_;
  MMGeoAnchor target_anchor_;
  // First edge index *not* belonging to an outgoing edge.
  // Outgoing edges are at [0..outgoing_edge_idx_stop).
  const uint32_t outgoing_edge_idx_stop_;

  std::priority_queue<QueuedEdge, std::vector<QueuedEdge>, MetricCmpEdge> pq_;
  std::vector<VisitedEdge> vis_;
  std::vector<ActiveCtrs> active_ctrs_vec_;
  ActiveCtrs active_ctrs_;  // Reused so we don't need to reallocate.
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
  for (const MMIncomingEdge& in_edge : mc.in_edges.span()) {
    // Now store the metrics for this border node.
    res.metrics.emplace_back();
    MMClusterRouter router(
        mcw, {.handle_restricted_access = true, .include_dead_end = false});
    int16_t off =
        (int16_t)(in_edge.edge_idx - mc.edge_start_idx(in_edge.from_node_idx));

    MMGeoAnchor ga;
    ga.AddEdge(mc, 1.0, in_edge.from_node_idx, off);
    router.Route(ga);

    const std::vector<MMClusterRouter::VisitedEdge>& vis =
        router.GetVisitedEdges();
    for (const MMOutgoingEdge& out_edge : mc.out_edges.span()) {
      // Check that the out edge has only one label.
      if (vis.at(out_edge.edge_idx).next != INFU32) {
        CHECK_EQ_S(vis.at(out_edge.edge_idx).next, out_edge.edge_idx);
      }
      res.metrics.back().push_back(vis.at(out_edge.edge_idx).min_metric);

      const std::vector<uint32_t> path =
          router.GetForwardPath(out_edge.edge_idx);
      // LOG_S(INFO) << "shortest cluster path len: " << path.size();
      if (path.size() > 0) {
        AnalyzePath(mc, path);
      }
    }
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

  MMClusterRouter router(
      mcw, {.handle_restricted_access = true, .include_dead_end = true});
  MMClusterRouterStatus status = router.Route(start_anchor, target_anchor);
  if (verb >= Verbosity::Brief) {
    LOG_S(INFO) << "Finished routing";
  }

  MMRoutingResult res;

  if (status.found) {
    res = router.GetRoutingResult(status.last_v_idx);
    if (verb >= Verbosity::Brief) {
      LOG_S(INFO) << "Result metric:" << res.final_metric;
    }

    // Print path.
    if (verb >= Verbosity::Verbose) {
      LOG_S(INFO) << "MMClusterRouter shortest path #edges:"
                  << res.full_edges.size();
      uint32_t metric = 0;
      for (uint32_t i = 0; i < res.full_edges.size(); ++i) {
        const MMFullEdge& fe = res.full_edges.at(i);
        metric += res.edge_weights.at(i);
        LOG_S(INFO) << (i + 1) << ". " << fe.DebugString(mc)
                    << " metric:" << metric;
      }
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
