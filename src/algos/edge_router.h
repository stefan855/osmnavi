#pragma once

#include <fstream>
#include <queue>
#include <vector>

#include "absl/container/flat_hash_map.h"
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
    std::uint32_t prev_v_idx;          // Predecessor in visited_edges vector.
    std::uint32_t min_metric;          // The minimal metric seen so far.
    std::uint32_t done : 1;            // 1 <=> edge has been finalized.
    std::uint32_t shortest_route : 1;  // 1 <=> edge is part of shortest route.
    std::uint32_t restricted : 1;
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
    ctx.target_restricted.InitialiseTransitionNodes(g_, target_idx);
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
    ExpandNeighboursForward(ctx, start_idx, /*prev_metric=*/0,
                            /*prev_v_idx=*/INFU32,
                            /*prev_restricted==*/true);

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
        LOG_S(INFO) << absl::StrFormat("POP edge:%u->%u(m:%d) done:0->1",
                                       ve.key.FromNode(g_).node_id,
                                       ve.key.ToNode(g_).node_id, qe.metric);
      }

      const uint32_t other_node_idx = ve.key.GetEdge(g_).other_node_idx;
      if (other_node_idx == target_idx) {
        // This edge ends at the target node. Remember it if it is the fastest.
        if (ve.min_metric < target_best_metric) {
          target_best_metric = ve.min_metric;
          target_best_v_idx = qe.v_idx;
        }
      }

      if (!ctx.opt.backward_search) {
        ExpandNeighboursForward(ctx, other_node_idx, ve.min_metric, qe.v_idx,
                                ve.restricted);
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
              "Shortest route: metric:%10u restr:%c node:%u cluster:%u",
              vtmp.min_metric,
              vtmp.key.GetEdge(g_).car_label == GEdge::LABEL_FREE ? 'N' : 'Y',
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
            "Route found, visited nodes:%u metric:%u", visited_edges_.size(),
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
    RestrictedArea target_restricted;
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
        EdgeIdMap::value_type(key, visited_edges_.size()));
    if (iter.second) {
      // Key didn't exist and was inserted, so add it to visited_edges_ too.
      visited_edges_.push_back(
          {.key = key,
           .prev_v_idx = INFU32,
           .min_metric = INFU32,
           .done = 0,
           .shortest_route = 0,
           .heuristic_to_target = use_astar_heuristic ? INFU30 : 0});
    }
    return iter.first->second;
  }

  // Compute the metric to get from 'node' to the target node, on a hypothetical
  // straight street which allows maximum speed.
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
  void ExpandNeighboursForward(const Context& ctx, uint32_t start_idx,
                               uint32_t prev_metric, uint32_t prev_v_idx,
                               bool prev_restricted) {
    const GNode& start_node = g_.nodes.at(start_idx);

    for (const GEdge& curr_ge : gnode_forward_edges(g_, start_idx)) {
      if (verbosity_ >= 3) {
        LOG_S(INFO) << absl::StrFormat(
            "CONSIDER edge:%u->%u", start_node.node_id,
            g_.nodes.at(curr_ge.other_node_idx).node_id);
      }

      const WaySharedAttrs& wsa = GetWSA(g_, curr_ge.way_idx);
      if (RoutingRejectEdge(g_, ctx.opt, start_node, start_idx, curr_ge, wsa,
                            EDGE_DIR(curr_ge))) {
        continue;
      }

      const bool curr_restricted = curr_ge.car_label != GEdge::LABEL_FREE;
      if (prev_restricted != curr_restricted) {
        const RestrictedArea& r_area = ctx.target_restricted;
        bool is_target_transition_node =
            r_area.active &&
            r_area.bounding_rect.Contains({start_node.lat, start_node.lon}) &&
            r_area.transition_nodes.contains(start_idx);
        if (prev_restricted == is_target_transition_node) {
          // This catches two transition cases:
          //   restricted->free: forbidden when leaving target restricted area.
          //   free->restricted: forbidden when not into target restricted area.
          if (verbosity_ >= 3) {
            int64_t prev_node_id = -1;
            if (prev_v_idx != INFU32) {
              prev_node_id =
                  visited_edges_.at(prev_v_idx).key.FromNode(g_).node_id;
            }
            LOG_S(INFO) << absl::StrFormat(
                "RESTRICTED: transition (%lld->) %u->%u forbidden allowed "
                "(edge %s)",
                prev_node_id, start_node.node_id,
                g_.nodes.at(curr_ge.other_node_idx).node_id,
                curr_restricted ? "restricted" : "free");
          }
          continue;
        }
#if 0
        if (prev_restricted) {
          // We're allowed to leave any restricted area, except for the
          // restricted area at the target.
          if (is_target_transition_node) {
            continue;
          }
        } else {
          // We're entering a restricted area. This is only allowed at the
          // restricted area at the target.
          if (!is_target_transition_node) {
            continue;
          }
        }
#endif
      }

      std::uint32_t v_idx =
          FindOrAddVisitedEdge(GEdgeKey::Create(g_, start_idx, curr_ge),
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
          prev_metric +
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
        ve.prev_v_idx = prev_v_idx;
        ve.restricted = curr_restricted;

        // Compute heuristic distance from new node to target.
        if (ve.heuristic_to_target == INFU30) {
          ve.heuristic_to_target = ComputeHeuristicToTarget(
              g_.nodes.at(curr_ge.other_node_idx), ctx);
          CHECK_LT_S(ve.heuristic_to_target, INFU30);
        }
        pq_.emplace(new_metric + ve.heuristic_to_target, v_idx);
      }
    }

#if 0
    if (ctx.opt.hybrid.on && node.cluster_border_node &&
        node.cluster_id != ctx.opt.hybrid.start_cluster_id &&
        node.cluster_id != ctx.opt.hybrid.target_cluster_id) {
      {
        // Check if we just traversed this cluster in the shortest path to this
        // node. A cluster can not be traversed more than once in the shortest
        // path!
        const GNode& parent_node =
            g_.nodes.at(visited_edges_.at(vedge.prev_v_idx).node_idx);
        if (parent_node.cluster_id == node.cluster_id) {
          return;
        }
      }

      // Expand the 'virtual' links within the cluster, using the precomputed
      // distances.
      const GCluster& cluster = g_.clusters.at(node.cluster_id);
      const std::vector<std::uint32_t>& v_dist =
          cluster.GetBorderNodeDistances(vedge.node_idx);

      for (size_t i = 0; i < cluster.border_nodes.size(); ++i) {
        uint32_t other_idx = cluster.border_nodes.at(i);
        if (other_idx == vedge.node_idx) continue;
        const std::uint32_t dist = v_dist.at(i);
        if (dist == INFU32) continue;  // node is not reachable.
        CHECK_LT_S(dist, INFU30);      // This shouldn't be so big.
        // TODO: check for potential overflow.
        const std::uint32_t v_other_idx =
            FindOrAddVisitedEdge(other_idx, ctx.opt.use_astar_heuristic);
        VisitedEdge& vother = visited_edges_.at(v_other_idx);
        std::uint32_t new_metric = vedge.min_metric + dist;

        if (verbosity_ >= 3) {
          LOG_S(INFO) << absl::StrFormat(
              "CLUSTER %5u from:%u(m:%d) to:%u done:%d new-metric:%d "
              "old-metric:%d",
              node.cluster_id, node.node_id, qedge.metric,
              g_.nodes.at(other_idx).node_id, vother.done, new_metric,
              vother.min_metric);
        }

        if (!vother.done && new_metric < vother.min_metric) {
          vother.min_metric = new_metric;
          vother.prev_v_idx = qedge.visited_node_idx;

          // Compute heuristic distance from new node to target.
          if (vother.heuristic_to_target == INFU30) {
            const uint32_t h =
                ComputeHeuristicToTarget(g_.nodes.at(other_idx), ctx);
            CHECK_LT_S(h, INFU30);
            vother.heuristic_to_target = h;
          }
          pq_.emplace(new_metric + vother.heuristic_to_target, v_other_idx);
        }
      }
    }
#endif
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

  const Graph& g_;
  std::vector<VisitedEdge> visited_edges_;

  typedef absl::flat_hash_map<GEdgeKey, uint32_t> EdgeIdMap;
  EdgeIdMap edgekey_to_vedge_idx_;

  std::priority_queue<QueuedEdge, std::vector<QueuedEdge>, decltype(&MetricCmp)>
      pq_;
  std::uint32_t target_visited_index_;
  const int verbosity_;
};
