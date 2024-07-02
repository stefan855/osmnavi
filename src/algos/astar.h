#pragma once

#include <fstream>
#include <queue>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "algos/routing_metric.h"
#include "base/util.h"
#include "geometry/distance.h"
#include "graph/graph_def.h"

class AStarRouter {
 public:
  // This exists once per 'node_idx'.
  struct VisitedNode {
    std::uint32_t node_idx;            // Index into global node vector.
    std::uint32_t min_metric;          // The minimal metric seen so far.
    std::uint32_t heuristic_distance;  // estimated distance to target.
    std::uint32_t from_v_idx : 30;     // index into visited_nodes vector.
    std::uint32_t done : 1;            // 1 <=> node has been finalized.
    std::uint32_t shortest_route : 1;  // 1 <=> node is part of shortest route.
  };

  // This might exist multiple times for each 'node_idx', when a node gets
  // reinserted into the priority queue with a lower priority.
  struct QueuedNode {
    std::uint32_t metric;
    std::uint32_t visited_node_idx;  // Index into visited_nodes vector.
  };

  AStarRouter(const Graph& g)
      : g_(g), pq_(&MetricCmp), target_visited_node_index_(INF32) {}

  bool Route(std::uint32_t start_idx, std::uint32_t target_idx,
             const RoutingMetric& metric, bool avoid_dead_end = true) {
    LOG_S(INFO) << "Start routing from " << start_idx << " to " << target_idx
                << " (A*, " << metric.Name() << ")";

    const double target_lat = g_.nodes.at(target_idx).lat;
    const double target_lon = g_.nodes.at(target_idx).lon;

    std::uint32_t start_v_idx = FindOrAddVisitedNode(start_idx);
    assert(start_v_idx == 0);
    assert(visited_nodes_.at(0).from_v_idx == INF30);

    visited_nodes_.front().min_metric = 0;

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
        LOG_S(INFO) << absl::StrFormat(
            "Route found, visited nodes:%u metric:%u", visited_nodes_.size(),
            vnode.min_metric);

        // Mark nodes on shortest route.
        auto current_idx = target_visited_node_index_;
        while (current_idx != INF30) {
          visited_nodes_.at(current_idx).shortest_route = 1;
          current_idx = visited_nodes_.at(current_idx).from_v_idx;
        }
        return true;
      }

      // Search neighbours.
      const GNode& node = g_.nodes.at(vnode.node_idx);
      const std::uint32_t min_metric = vnode.min_metric;
      // TODO: Do not use 'vnode' after this line, because
      // FindOrAddVisitedNode() in the loop below might invalidate it.
      for (size_t i = 0; i < node.num_edges_out; ++i) {
        const GEdge& edge = node.edges[i];
        if (avoid_dead_end && edge.bridge && !node.dead_end) {
          // Node is in the non-dead-end side of the bridge, so ignore edge and
          // do not enter the dead end.
          continue;
        }
        std::uint32_t v_idx = FindOrAddVisitedNode(edge.other_node_idx);
        VisitedNode& vother = visited_nodes_.at(v_idx);
        if (vother.done) {
          continue;
        }
        std::uint32_t new_metric =
            min_metric + metric.Compute(g_.ways.at(edge.way_idx), edge);
        if (new_metric < vother.min_metric) {
          vother.min_metric = new_metric;
          vother.from_v_idx = qnode.visited_node_idx;
          if (vother.heuristic_distance == INF32) {
            // TODO: use country specific maxspeed.
            static const RoutingAttrs g_ri = {.access = ACC_YES,
                                              .maxspeed = 120};
            static const GWay g_way = {.ri = g_ri};
            const GNode& other_node = g_.nodes.at(edge.other_node_idx);
            vother.heuristic_distance = metric.Compute(
                g_way,
                {.distance_cm = static_cast<uint64_t>(
                     1.05 * calculate_distance(other_node.lat /* / 10000000.0*/,
                                               other_node.lon /* / 10000000.0*/,
                                               target_lat, target_lon)),
                 .contra_way = 0});
          }
          pq_.emplace(new_metric + vother.heuristic_distance, v_idx);
        }
      }
    }
    LOG_S(INFO) << "Route not found";
    return false;
  }

  void SaveSpanningTreeSegments(const std::string& filename) {
    LOG_S(INFO) << "Write route to " << filename;
    std::ofstream myfile;
    myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);
    for (const VisitedNode& n : visited_nodes_) {
      if (n.done && n.from_v_idx != INF30) {
        const VisitedNode& from = visited_nodes_.at(n.from_v_idx);
        const GNode& sn = g_.nodes.at(n.node_idx);
        const GNode& sfrom = g_.nodes.at(from.node_idx);
        bool shortest = from.shortest_route && n.shortest_route;
        myfile << absl::StrFormat("line,%s,%d,%d,%d,%d\n",
                                  shortest ? "mag" : "black", sfrom.lat,
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

  std::uint32_t FindOrAddVisitedNode(std::uint32_t node_idx) {
    auto iter = node_to_vnode_idx_.find(node_idx);
    if (iter != node_to_vnode_idx_.end()) {
      return iter->second;
    }
    visited_nodes_.emplace_back(node_idx, INF32, INF32, INF30, 0, 0);
    node_to_vnode_idx_[node_idx] = visited_nodes_.size() - 1;
    // Compute distance heuristic to target node.
    return visited_nodes_.size() - 1;
  }

  const Graph& g_;
  std::vector<VisitedNode> visited_nodes_;
  absl::flat_hash_map<std::uint32_t, std::uint32_t> node_to_vnode_idx_;
  std::priority_queue<QueuedNode, std::vector<QueuedNode>, decltype(&MetricCmp)>
      pq_;
  std::uint32_t target_visited_node_index_;
};
