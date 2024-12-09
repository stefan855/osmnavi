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

class AStarRouter {
 public:
  // This exists once per 'node_idx'.
  struct VisitedNode {
    std::uint32_t node_idx;             // Index into global node vector.
    std::uint32_t min_metric;           // The minimal metric seen so far.
    std::uint32_t heuristic_to_target;  // Estimated heuristic metric to target.
    std::uint32_t from_v_idx : 30;      // Predecessor in visited_nodes vector.
    std::uint32_t done : 1;             // 1 <=> node has been finalized.
    std::uint32_t shortest_route : 1;   // 1 <=> node is part of shortest route.
  };

  // This might exist multiple times for each 'node_idx', when a node gets
  // reinserted into the priority queue with a lower priority.
  struct QueuedNode {
    std::uint32_t metric;
    std::uint32_t visited_node_idx;  // Index into visited_nodes vector.
  };

  AStarRouter(const Graph& g, bool verbose = true)
      : g_(g),
        pq_(&MetricCmp),
        target_visited_node_index_(INFU32),
        verbose_(verbose) {
    Clear();
  }

  RoutingResult Route(std::uint32_t start_idx, std::uint32_t target_idx,
                      const RoutingMetric& metric, const RoutingOptions& opt) {
    if (verbose_) {
      LOG_S(INFO) << "Start routing from " << g_.nodes.at(start_idx).node_id
                  << " to " << g_.nodes.at(target_idx).node_id << " (A*, "
                  << metric.Name() << ")"
                  << (opt.backward_search ? " backward search"
                                          : " forward search");
    }
    Clear();
    const std::int32_t target_lat = g_.nodes.at(target_idx).lat;
    const std::int32_t target_lon = g_.nodes.at(target_idx).lon;

    std::uint32_t start_v_idx = FindOrAddVisitedNode(start_idx);
    CHECK_EQ_S(start_v_idx, 0);
    CHECK_EQ_S(visited_nodes_.at(0).from_v_idx, INF30);

    visited_nodes_.front().min_metric = 0;

    RoutingResult result;
    pq_.emplace(0, start_v_idx);
    while (!pq_.empty()) {
      const QueuedNode qnode = pq_.top();
      pq_.pop();
      VisitedNode& vnode = visited_nodes_.at(qnode.visited_node_idx);
      if (vnode.done == 1) {
        continue;  // "old" entry in priority queue.
      }
      vnode.done = 1;

      // shortest route found?
      if (vnode.node_idx == target_idx) {
        target_visited_node_index_ = qnode.visited_node_idx;
        if (verbose_) {
          LOG_S(INFO) << absl::StrFormat(
              "Route found, visited nodes:%u metric:%u", visited_nodes_.size(),
              vnode.min_metric);
        }

        // Mark nodes on shortest route.
        auto current_idx = target_visited_node_index_;
        while (current_idx != INF30) {
          visited_nodes_.at(current_idx).shortest_route = 1;
          current_idx = visited_nodes_.at(current_idx).from_v_idx;
        }
        result.found = true;
        result.found_distance = vnode.min_metric;
        return result;
      }

      if (!opt.backward_search) {
        ExpandNeighboursForward(qnode, metric, opt, vnode.node_idx,
                                vnode.min_metric, target_lat, target_lon);
      } else {
        ExpandNeighboursBackward(qnode, metric, opt, vnode.node_idx,
                                 vnode.min_metric, target_lat, target_lon);
      }
    }
    if (verbose_) {
      LOG_S(INFO) << "Route not found";
    }
    return result;
  }

  void SaveSpanningTreeSegments(const std::string& filename) {
    if (verbose_) {
      LOG_S(INFO) << "Write route to " << filename;
    }
    std::ofstream myfile;
    myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);
    for (const VisitedNode& n : visited_nodes_) {
      if (n.done && n.from_v_idx != INF30) {
        const VisitedNode& from = visited_nodes_.at(n.from_v_idx);
        const GNode& sn = g_.nodes.at(n.node_idx);
        const GNode& sfrom = g_.nodes.at(from.node_idx);
        bool shortest = from.shortest_route && n.shortest_route;
        myfile << absl::StrFormat("line,%s,%d,%d,%d,%d\n",
                                  shortest ? "red" : "black", sfrom.lat,
                                  sfrom.lon, sn.lat, sn.lon);
      }
    }
    myfile.close();
  }

 private:
  static constexpr std::uint32_t INF32 = 1 << 31;
  static constexpr std::uint32_t INF30 = 1 << 29;

  static bool MetricCmp(const QueuedNode& left, const QueuedNode& right) {
    return left.metric > right.metric;
  }

  void Clear() {
    visited_nodes_.clear();
    node_to_vnode_idx_.clear();
    CHECK_S(pq_.empty());  // No clear() method, should be empty anyways.
    target_visited_node_index_ = INFU32;
  }

  std::uint32_t FindOrAddVisitedNode(std::uint32_t node_idx) {
    // Prevent doing two lookups by following
    // https://stackoverflow.com/questions/1409454.
    const auto iter = node_to_vnode_idx_.insert(
        NodeIdMap::value_type(node_idx, visited_nodes_.size()));
    if (iter.second) {
      // Key didn't exist and was inserted, so add it to visited_nodes_ too.
      visited_nodes_.emplace_back(node_idx, INF32, INF32, INF30, 0, 0);
    }
    return iter.first->second;
  }

  // Compute the metric to get from 'node' to the target node, on a hypothetical
  // straight street which allows maximum speed.
  static inline std::uint32_t ComputeHeuristicToTarget(
      const GNode& node, std::int32_t target_lat, std::int32_t target_lon,
      const RoutingMetric& metric, const RoutingOptions& opt) {
    // TODO: use country specific maxspeed.
    // TODO: support mexspeed for different vehicles.
    // SOLUTION: Should use per (country, vehicle) maxspeed. If target
    // is in other country then use mix.
    static const RoutingAttrs g_ra = {.access = ACC_YES, .maxspeed = 120};
    static const WaySharedAttrs g_wsa = {.ra =
                                             g_ra};  // Sets .ra[0] and .ra[1]!
    return metric.Compute(
        g_wsa, opt.vt, DIR_FORWARD,
        {.distance_cm = static_cast<uint64_t>(
             1.00 *
             calculate_distance(node.lat, node.lon, target_lat, target_lon)),
         .contra_way = 0});
  }

  // Expand the forward edges.
  void ExpandNeighboursForward(const QueuedNode& qnode,
                               const RoutingMetric& metric,
                               const RoutingOptions& opt,
                               std::uint32_t node_idx, std::uint32_t min_metric,
                               std::int32_t target_lat,
                               std::int32_t target_lon) {
    const GNode& node = g_.nodes.at(node_idx);
    for (size_t i = 0; i < node.num_edges_out; ++i) {
      const GEdge& edge = node.edges[i];
      const WaySharedAttrs& wsa = GetWSA(g_, edge.way_idx);
      if (RoutingRejectEdge(g_, opt, node, node_idx, edge, wsa,
                            EDGE_DIR(edge))) {
        continue;
      }

      std::uint32_t v_idx = FindOrAddVisitedNode(edge.other_node_idx);
      VisitedNode& vother = visited_nodes_.at(v_idx);
      if (vother.done) {
        continue;
      }
      std::uint32_t new_metric =
          min_metric + metric.Compute(wsa, opt.vt, EDGE_DIR(edge), edge);
      if (new_metric < vother.min_metric) {
        vother.min_metric = new_metric;
        vother.from_v_idx = qnode.visited_node_idx;

        // Compute heuristic distance from new node to target.
        if (vother.heuristic_to_target == INF32) {
          vother.heuristic_to_target =
              ComputeHeuristicToTarget(g_.nodes.at(edge.other_node_idx),
                                       target_lat, target_lon, metric, opt);
        }
        pq_.emplace(new_metric + vother.heuristic_to_target, v_idx);
      }
    }
  }

  // Expand the backward edges (for traveling backwards).
  void ExpandNeighboursBackward(const QueuedNode& qnode,
                                const RoutingMetric& metric,
                                const RoutingOptions& opt,
                                std::uint32_t node_idx,
                                std::uint32_t min_metric,
                                std::int32_t target_lat,
                                std::int32_t target_lon) {
    const GNode& node = g_.nodes.at(node_idx);
    for (size_t i = 0; i < gnode_total_edges(node); ++i) {
      const GEdge& edge = node.edges[i];
      // Skip edges that are forward only.
      if (i < node.num_edges_out && !edge.both_directions) {
        continue;
      }

      const WaySharedAttrs& wsa = GetWSA(g_, edge.way_idx);
      if (RoutingRejectEdge(g_, opt, node, node_idx, edge, wsa,
                            EDGE_INVERSE_DIR(edge))) {
        continue;
      }

      std::uint32_t v_idx = FindOrAddVisitedNode(edge.other_node_idx);
      VisitedNode& vother = visited_nodes_.at(v_idx);
      if (vother.done) {
        continue;
      }
      std::uint32_t new_metric =
          min_metric +
          metric.Compute(wsa, opt.vt, EDGE_INVERSE_DIR(edge), edge);
      if (new_metric < vother.min_metric) {
        vother.min_metric = new_metric;
        vother.from_v_idx = qnode.visited_node_idx;

        // Compute heuristic distance from new node to target.
        if (vother.heuristic_to_target == INF32) {
          vother.heuristic_to_target =
              ComputeHeuristicToTarget(g_.nodes.at(edge.other_node_idx),
                                       target_lat, target_lon, metric, opt);
        }
        pq_.emplace(new_metric + vother.heuristic_to_target, v_idx);
      }
    }
  }

  const Graph& g_;
  std::deque<VisitedNode> visited_nodes_;

  typedef absl::flat_hash_map<uint32_t, uint32_t> NodeIdMap;
  NodeIdMap node_to_vnode_idx_;
  // absl::flat_hash_map<std::uint32_t, std::uint32_t> node_to_vnode_idx_;

  std::priority_queue<QueuedNode, std::vector<QueuedNode>, decltype(&MetricCmp)>
      pq_;
  std::uint32_t target_visited_node_index_;
  const bool verbose_;
};
