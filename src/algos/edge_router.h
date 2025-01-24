#pragma once

#include <fstream>
#include <queue>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "algos/edge_key.h"
#include "algos/routing_defs.h"
#include "algos/routing_metric.h"
#include "algos/tarjan.h"
#include "base/util.h"
#include "geometry/distance.h"
#include "graph/graph_def.h"

// A router that can run in Dijkstra or in AStar mode, and may use precomputed
// routes from clusters (see 'hybrid').
//
// The router is edge based, i.e. it can handle turn costs.
//
// In AStar mode, the router uses a heuristic to estimate a lower bound (see
// 'heuristic_to_target') that is needed to travel from the current node to the
// target.
class EdgeRouter {
 public:
  // This exists once for every edge.
  struct VisitedEdge {
    GEdgeKey key;
    std::uint32_t done : 1;            // 1 <=> edge has been finalized.
    std::uint32_t shortest_route : 1;  // 1 <=> edge is part of shortest route.
    std::uint32_t restricted : 1;
    std::uint32_t prev_v_idx;  // Predecessor in visited_edges vector.
    std::uint32_t min_metric;  // The minimal metric seen so far.
    std::uint32_t heuristic_to_target : 30;
  };

  // This might exist multiple times for each 'v_idx', when a node gets
  // reinserted into the priority queue with a lower priority.
  struct QueuedEdge {
    std::uint32_t metric;
    std::uint32_t v_idx;  // Index into visited_edges vector.
  };

  EdgeRouter(const Graph& g, int verbosity = 1)
      : g_(g),
        pq_(&MetricCmp),
        target_visited_index_(INFU32),
        verbosity_(verbosity) {
    Clear();
  }

  std::string AlgoName(const RoutingOptions& opt) {
    return opt.use_astar_heuristic ? "e_astar" : "e_dijkstra";
  }

  std::string Name(const RoutingMetric& metric, const RoutingOptions& opt) {
    return absl::StrFormat("%s-%s-%s%s", AlgoName(opt), metric.Name(),
                           opt.backward_search ? "backward" : "forward",
                           opt.hybrid.on ? "-hybrid" : "");
  }

  RoutingResult Route(std::uint32_t start_idx, std::uint32_t target_idx,
                      const RoutingMetric& metric, const RoutingOptions& opt) {
    if (verbosity_ > 0) {
      LOG_S(INFO) << absl::StrFormat(
          "Start routing from %u to %u (%s)", g_.nodes.at(start_idx).node_id,
          g_.nodes.at(target_idx).node_id, Name(metric, opt));
    }
    Clear();
    if (start_idx == target_idx) {
      return {.found = true,
              .found_distance = 0,
              .num_visited_nodes = 0,
              .num_shortest_route_nodes = 0};
    }

    Context ctx = {.opt = opt,
                   .metric = metric,
                   .target_lat = g_.nodes.at(target_idx).lat,
                   .target_lon = g_.nodes.at(target_idx).lon};
    ctx.target_restricted.InitialiseTransitionNodes(g_, start_idx, target_idx);
    if (ctx.target_restricted.transition_nodes.contains(start_idx)) {
      // Special case, start and target area are the same.
    }

    if (verbosity_ > 0) {
      const auto& tr = ctx.target_restricted;
      LOG_S(INFO) << absl::StrFormat(
          "Restricted target area: active:%d trans-nodes:%d rect min:(%d,%d) "
          "max:(%d,%d)",
          tr.active, tr.transition_nodes.size(), tr.bounding_rect.minp.x,
          tr.bounding_rect.minp.y, tr.bounding_rect.maxp.x,
          tr.bounding_rect.maxp.y);
    }

    // Add all outgoing edges of the start node to the queue.
    ExpandNeighboursForward(ctx, {.other_idx = start_idx,
                                  .metric = 0,
                                  .v_idx = INFU32,
                                  .restricted = true,
                                  .in_target_area = false});

    // Store the best edge seen so far that points to target_idx.
    uint32_t target_best_v_idx = INFU32;
    uint32_t target_best_metric = INFU32;
    while (!pq_.empty()) {
      const QueuedEdge qe = pq_.top();
      pq_.pop();
      VisitedEdge& ve = visited_edges_.at(qe.v_idx);
      if (target_best_v_idx != INFU32 && ve.min_metric >= target_best_metric) {
        // We have found a shortest way.
        break;
      }
      if (ve.done == 1) {
        continue;  // "old" entry in priority queue.
      }
      ve.done = 1;
      if (verbosity_ >= 3) {
        LOG_S(INFO) << absl::StrFormat(
            "POP edge:%u->%u(m:%d) type:%c %s done:0->1",
            ve.key.FromNode(g_).node_id, ve.key.ToNode(g_).node_id, qe.metric,
            ve.key.IsClusterEdge()                              ? 'C'
            : ve.key.GetEdge(g_).car_label == GEdge::LABEL_FREE ? 'F'
                                                                : 'R',
            ve.key.GetBit() ? "in_target_area" : "in_free_area");
      }

      const uint32_t other_node_idx = ve.key.GetToIdx(g_);
      if (other_node_idx == target_idx) {
        // This edge ends at the target node. Remember it if it is the fastest.
        if (ve.min_metric < target_best_metric) {
          target_best_metric = ve.min_metric;
          target_best_v_idx = qe.v_idx;
        }
      }

      if (!ctx.opt.backward_search) {
        ExpandNeighboursForward(ctx, {.other_idx = other_node_idx,
                                      .metric = ve.min_metric,
                                      .v_idx = qe.v_idx,
                                      .restricted = ve.restricted != 0,
                                      .in_target_area = ve.key.GetBit()});
      } else {
        CHECK_S(false);
        // ExpandNeighboursBackward(qedge, ctx, vedge);
      }
    }

    // Shortest route found?
    if (target_best_v_idx != INFU32) {
      RoutingResult result = {
          .found = true,
          .found_distance = target_best_metric,
          .num_visited_nodes = (uint32_t)visited_edges_.size(),
          .num_shortest_route_nodes = 1};

      auto current_idx = target_best_v_idx;
      while (current_idx != INFU32) {
        result.num_shortest_route_nodes++;
        VisitedEdge& vtmp = visited_edges_.at(current_idx);
        vtmp.shortest_route = 1;
        current_idx = vtmp.prev_v_idx;
        if (verbosity_ >= 2) {
          const GNode& n = vtmp.key.ToNode(g_);

          LOG_S(INFO) << absl::StrFormat(
              "Shortest route: metric:%10u type:%c node:%u cluster:%u",
              vtmp.min_metric,
              vtmp.key.IsClusterEdge()                              ? 'C'
              : vtmp.key.GetEdge(g_).car_label == GEdge::LABEL_FREE ? 'F'
                                                                    : 'R',
              n.node_id, n.cluster_id);
        }
      }
      if (verbosity_ >= 2) {
        const GNode& n = g_.nodes.at(start_idx);
        LOG_S(INFO) << absl::StrFormat(
            "Shortest route: metric:%10u restr:- node:%u cluster:%u", 0,
            n.node_id, n.cluster_id);
      }
      if (verbosity_ > 0) {
        LOG_S(INFO) << absl::StrFormat(
            "Route found, visited edges:%u metric:%u", visited_edges_.size(),
            result.found_distance);
      }
      return result;
    } else {
      if (verbosity_ > 0) {
        LOG_S(INFO) << "Route not found";
      }
      return {.num_visited_nodes = (uint32_t)visited_edges_.size()};
    }
  }

  void SaveSpanningTreeSegments(const std::string& filename) {
    if (verbosity_ > 0) {
      LOG_S(INFO) << "Write route to " << filename;
    }
    std::ofstream myfile;
    myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);
    // Draw the shortest route in red over the spanning tree in black.
    for (int output_shortest = 0; output_shortest <= 1; output_shortest++) {
      for (const VisitedEdge& ve : visited_edges_) {
        if (ve.shortest_route == (output_shortest > 0)) {
          const GNode a = ve.key.FromNode(g_);
          const GNode b = ve.key.ToNode(g_);
          myfile << absl::StrFormat("line,%s,%d,%d,%d,%d\n",
                                    ve.shortest_route ? "red" : "black", a.lat,
                                    a.lon, b.lat, b.lon);
        }
      }
    }
    myfile.close();
  }

 private:
  struct Context {
    const RoutingOptions opt;
    const RoutingMetric& metric;
    int32_t target_lat;
    int32_t target_lon;
    DestinationArea target_restricted;
  };

  //
  struct PrevEdgeData {
    const uint32_t other_idx;
    const uint32_t metric;
    const uint32_t v_idx;
    const bool restricted;
    const bool in_target_area;
  };

  static bool MetricCmp(const QueuedEdge& left, const QueuedEdge& right) {
    return left.metric > right.metric;
  }

  void Clear() {
    visited_edges_.clear();
    edgekey_to_vedge_idx_.clear();
    CHECK_S(pq_.empty());  // No clear() method, should be empty anyways.
    target_visited_index_ = INFU32;
  }

  std::uint32_t FindOrAddVisitedEdge(GEdgeKey key, bool use_astar_heuristic) {
    // Prevent doing two lookups by following
    // https://stackoverflow.com/questions/1409454.
    const auto iter = edgekey_to_vedge_idx_.insert(
        EdgeIdMap::value_type(key.HashKey(g_), visited_edges_.size()));
    if (iter.second) {
      // Key didn't exist and was inserted, so add it to visited_edges_ too.
      visited_edges_.push_back(
          {.key = key,
           .done = 0,
           .shortest_route = 0,
           .restricted = 0,
           .prev_v_idx = INFU32,
           .min_metric = INFU32,
           .heuristic_to_target = use_astar_heuristic ? INFU30 : 0});
    }
    return iter.first->second;
  }

  // Compute the metric to get from 'node' to the target node, on a
  // hypothetical straight street which allows maximum speed.
  static inline std::uint32_t ComputeHeuristicToTarget(const GNode& node,
                                                       const Context& ctx) {
    // TODO: use country specific maxspeed.
    // TODO: support mexspeed for different vehicles.
    // SOLUTION: Should use per (country, vehicle) maxspeed. If target
    // is in other country then use mix.
    static const RoutingAttrs g_ra = {.access = ACC_YES, .maxspeed = 120};
    static const WaySharedAttrs g_wsa = {.ra =
                                             g_ra};  // Sets .ra[0] and .ra[1]!
    return ctx.metric.Compute(
        g_wsa, ctx.opt.vt, DIR_FORWARD,
        {.distance_cm = static_cast<uint64_t>(
             1.00 * calculate_distance(node.lat, node.lon, ctx.target_lat,
                                       ctx.target_lon)),
         .contra_way = 0});
  }

  // Expand the forward edges.
  void ExpandNeighboursForward(const Context& ctx, const PrevEdgeData prev) {
    const GNode& expansion_node = g_.nodes.at(prev.other_idx);
    // The node with 'uturn_node_idx' represents a u-turn, which is forbidden in
    // general.
    // TODO: Handle u-turns better, specifically (1) allowed u-turns and (2)
    // when forward and backward lanes of a road or on different ways.
    const uint32_t uturn_node_idx =
        prev.v_idx == INFU32 ? INFU32
                             : visited_edges_.at(prev.v_idx).key.GetFromIdx();

    for (const GEdge& curr_ge : gnode_forward_edges(g_, prev.other_idx)) {
      if (verbosity_ >= 3) {
        LOG_S(INFO) << absl::StrFormat(
            "CONSIDER edge:%u->%u curr:%s %s prev:%s", expansion_node.node_id,
            g_.nodes.at(curr_ge.other_node_idx).node_id,
            curr_ge.car_label != GEdge::LABEL_FREE ? "restricted" : "free",
            prev.in_target_area ? "in_target_area" : "in_free_area",
            prev.restricted ? "restricted" : "free");
      }
      if (curr_ge.other_node_idx == uturn_node_idx) {
        continue;
      }

      const WaySharedAttrs& wsa = GetWSA(g_, curr_ge.way_idx);
      if (RoutingRejectEdge(g_, ctx.opt, expansion_node, prev.other_idx,
                            curr_ge, wsa, EDGE_DIR(curr_ge))) {
        continue;
      }

      bool curr_in_target_area = prev.in_target_area;
      if (prev.restricted != (curr_ge.car_label != GEdge::LABEL_FREE)) {
        if (!CheckAreaTransition(ctx, prev, curr_ge, &curr_in_target_area)) {
          continue;
        }
      }

      std::uint32_t v_idx = FindOrAddVisitedEdge(
          GEdgeKey::Create(g_, prev.other_idx, curr_ge, curr_in_target_area),
          ctx.opt.use_astar_heuristic);
      VisitedEdge& ve = visited_edges_.at(v_idx);
      if (verbosity_ >= 3) {
        LOG_S(INFO) << absl::StrFormat(
            "VISITED  edge:%u->%u(m:%d) done:%d", ve.key.FromNode(g_).node_id,
            ve.key.ToNode(g_).node_id, ve.min_metric, ve.done);
      }
      if (ve.done) {
        continue;
      }

      std::uint32_t new_metric =
          prev.metric +
          ctx.metric.Compute(wsa, ctx.opt.vt, EDGE_DIR(curr_ge), curr_ge);

      if (verbosity_ >= 3) {
        LOG_S(INFO) << absl::StrFormat(
            "%-8s edge:%u->%u(m:%d) new_metric:%d",
            new_metric < ve.min_metric ? "PUSH" : "DONTPUSH",
            ve.key.FromNode(g_).node_id, ve.key.ToNode(g_).node_id,
            ve.min_metric, new_metric);
      }

      if (new_metric < ve.min_metric) {
        ve.min_metric = new_metric;
        ve.prev_v_idx = prev.v_idx;
        ve.restricted = (curr_ge.car_label != GEdge::LABEL_FREE);

        // Compute heuristic distance from new node to target.
        if (ve.heuristic_to_target == INFU30) {
          ve.heuristic_to_target = ComputeHeuristicToTarget(
              g_.nodes.at(curr_ge.other_node_idx), ctx);
          CHECK_LT_S(ve.heuristic_to_target, INFU30);
        }
        pq_.emplace(new_metric + ve.heuristic_to_target, v_idx);
      }
    }

    if (ctx.opt.hybrid.on && expansion_node.cluster_border_node &&
        expansion_node.cluster_id != ctx.opt.hybrid.start_cluster_id &&
        expansion_node.cluster_id != ctx.opt.hybrid.target_cluster_id) {
      if (prev.v_idx != INFU32 &&
          visited_edges_.at(prev.v_idx).key.IsClusterEdge()) {
        // We just traversed a cluster, don't enter the same cluster again.
        return;
      }

      // Expand the 'virtual' links within the cluster, using the precomputed
      // distances.
      const GCluster& cluster = g_.clusters.at(expansion_node.cluster_id);
      const std::vector<std::uint32_t>& v_dist =
          cluster.GetBorderNodeDistances(prev.other_idx);

      for (size_t i = 0; i < cluster.border_nodes.size(); ++i) {
        uint32_t other_idx = cluster.border_nodes.at(i);
        if (other_idx == prev.other_idx) {
          // Self link, so ignore.
          continue;
        }

        const std::uint32_t dist = v_dist.at(i);
        if (dist == INFU32) continue;  // node is not reachable.
        CHECK_LT_S(dist, INFU30);      // Not plausible if so big.

        // TODO: check for potential overflow.
        std::uint32_t v_idx = FindOrAddVisitedEdge(
            GEdgeKey::CreateClusterEdge(g_, prev.other_idx, i, /*bit=*/false),
            ctx.opt.use_astar_heuristic);

        VisitedEdge& vedge = visited_edges_.at(v_idx);
        std::uint32_t new_metric = prev.metric + dist;

        if (verbosity_ >= 3) {
          LOG_S(INFO) << absl::StrFormat(
              "CLUSTER %5u from:%u to:%u done:%d new-metric:%d "
              "old-metric:%d",
              expansion_node.cluster_id, expansion_node.node_id,
              vedge.key.ToNode(g_).node_id, vedge.done, new_metric,
              vedge.min_metric);
        }

        if (!vedge.done && new_metric < vedge.min_metric) {
          vedge.min_metric = new_metric;
          vedge.prev_v_idx = prev.v_idx;
          // vedge.restricted is already false.
          CHECK_S(!vedge.restricted);

          // Compute heuristic distance from new node to target.
          if (vedge.heuristic_to_target == INFU30) {
            vedge.heuristic_to_target =
                ComputeHeuristicToTarget(vedge.key.ToNode(g_), ctx);
            CHECK_LT_S(vedge.heuristic_to_target, INFU30);
          }
          pq_.emplace(new_metric + vedge.heuristic_to_target, v_idx);
        }
      }
    }
  }

#if 0
  // Expand the backward edges (for traveling backwards).
  // Note: It is important to pass vedge by value, because a call to
  // FindOrAddVisitedEdge() may invalidate a reference.
  void ExpandNeighboursBackward(const QueuedEdge& qedge, const Context& ctx,
                                const VisitedEdge vedge) {
    const GNode& node = g_.nodes.at(vedge.node_idx);

    for (const GEdge& edge : gnode_all_edges(g_, vedge.node_idx)) {
      // Skip edges that are forward only.
      if (!edge.inverted /*i < node.num_edges_out*/ && !edge.both_directions) {
        continue;
      }

      const WaySharedAttrs& wsa = GetWSA(g_, edge.way_idx);
      if (RoutingRejectEdge(g_, ctx.opt, node, vedge.node_idx, edge, wsa,
                            EDGE_INVERSE_DIR(edge))) {
        continue;
      }

      std::uint32_t v_idx = FindOrAddVisitedEdge(edge.other_node_idx,
                                                 ctx.opt.use_astar_heuristic);
      VisitedEdge& vother = visited_edges_.at(v_idx);
      if (vother.done) {
        continue;
      }
      std::uint32_t new_metric =
          vedge.min_metric +
          ctx.metric.Compute(wsa, ctx.opt.vt, EDGE_INVERSE_DIR(edge), edge);
      if (new_metric < vother.min_metric) {
        vother.min_metric = new_metric;
        vother.prev_v_idx = qedge.visited_node_idx;

        // Compute heuristic distance from new node to target.
        if (vother.heuristic_to_target == INFU30) {
          const uint32_t h =
              ComputeHeuristicToTarget(g_.nodes.at(edge.other_node_idx), ctx);
          CHECK_LT_S(h, INFU30);
          vother.heuristic_to_target = h;
        }
        pq_.emplace(new_metric + vother.heuristic_to_target, v_idx);
      }
    }
  }
#endif

  // Checks if a transition of area is happening, such as when driving into
  // roads with access=destination. Allowed transitions are
  // "start-restricted"->"free" and "free"->"destination restricted". Returns
  // false if a transition is happening that is not allowed, and true in all
  // other cases (i.e. no transition and allowed transitions). Sets
  // curr_in_target_area to true if the corresponding bit in the edge kit should
  // be set. This only happens in the special case when "start-restricted" and
  // "target-restricted" are identical.
  bool CheckAreaTransition(const Context& ctx, const PrevEdgeData& prev,
                           const GEdge& curr_ge, bool* curr_in_target_area) {
    *curr_in_target_area = prev.in_target_area;
    const bool curr_restricted = curr_ge.car_label != GEdge::LABEL_FREE;
    const DestinationArea& d_area = ctx.target_restricted;

    if (!d_area.start_equal_target) {
      // Normal case, the start and target areas *not* the same.
      const GNode& from_node = g_.nodes.at(prev.other_idx);
      const bool is_target_transition_node =
          d_area.active &&
          d_area.bounding_rect.Contains({from_node.lat, from_node.lon}) &&
          d_area.transition_nodes.contains(prev.other_idx);
      if (prev.restricted == is_target_transition_node) {
        // This catches two transition cases:
        //   restricted->free: forbidden when leaving target restricted
        //   area.
        //   free->restricted: forbidden when not moving into target
        //   restricted area.
        if (verbosity_ >= 3) {
          int64_t prev_node_id = -1;
          if (prev.v_idx != INFU32) {
            prev_node_id =
                visited_edges_.at(prev.v_idx).key.FromNode(g_).node_id;
          }
          LOG_S(INFO) << absl::StrFormat(
              "RESTRICTED: transition (%lld->) %lld->%lld forbidden "
              "(edge "
              "%s)",
              prev_node_id, from_node.node_id,
              g_.nodes.at(curr_ge.other_node_idx).node_id,
              curr_restricted ? "restricted" : "free");
        }
        return false;
      }
    } else {
      // Special case, the start and target areas are the same and
      // non-empty.
      if (prev.in_target_area) {
        CHECK_S(prev.restricted);
        CHECK_S(!curr_restricted);
        // If we're in the target area, then it is not allowed to leave it
        // again.
        return false;
      } else if (!curr_restricted) {
        // OK, we're leaving the start area.
      } else {
        CHECK_S(!prev.in_target_area);
        CHECK_S(!prev.restricted);
        CHECK_S(curr_restricted);
        if (!d_area.transition_nodes.contains(prev.other_idx)) {
          // We're not allowed to enter another restricted area.
          return false;
        }
        // We're entering the target area, set bit in edge keys.
        *curr_in_target_area = true;
        return true;
      }
    }
    return true;
  }

  const Graph& g_;
  std::vector<VisitedEdge> visited_edges_;

  typedef absl::flat_hash_map<uint64_t, uint32_t> EdgeIdMap;
  EdgeIdMap edgekey_to_vedge_idx_;

  std::priority_queue<QueuedEdge, std::vector<QueuedEdge>, decltype(&MetricCmp)>
      pq_;
  std::uint32_t target_visited_index_;
  const int verbosity_;
};
