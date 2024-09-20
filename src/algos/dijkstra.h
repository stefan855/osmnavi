#pragma once

#include <fstream>
#include <queue>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "algos/routing_metric.h"
#include "base/constants.h"
#include "base/util.h"
#include "graph/graph_def.h"

class DijkstraRouter {
 public:
  // This exists once per 'node_idx'.
  struct VisitedNode {
    std::uint32_t node_idx;            // Index into global node vector.
    std::uint32_t from_v_idx;          // Predecessor in visited_nodes vector.
    std::uint32_t min_metric;          // The minimal metric seen so far.
    std::uint32_t done : 1;            // 1 <=> node has been finalized.
    std::uint32_t shortest_route : 1;  // 1 <=> node is part of shortest route.
  };

  // This might exist multiple times for each 'node_idx', when a node gets
  // reinserted into the priority queue with a lower priority.
  struct QueuedNode {
    std::uint32_t metric;
    std::uint32_t visited_node_idx;  // Index into visited_nodes vector.
  };

  struct Filter {
    bool avoid_dead_end;
    bool restrict_to_cluster;
    std::uint32_t cluster_id;
  };
  static constexpr Filter standard_filter = {.avoid_dead_end = true,
                                             .restrict_to_cluster = false,
                                             .cluster_id = INFU32};

  struct Result {
    bool found = false;
    // If a route was found, the distance from start to target node.
    uint32_t found_distance = INFU32;
    /*
    // If a route was found, the internal visited index of the target node.
    uint32_t found_target_visited_node_index = INFU32;
    */
  };

  DijkstraRouter(const Graph& g, bool verbose = true)
      : g_(g), pq_(&MetricCmp), verbose_(verbose) {
    Clear();
  }

  Result Route(std::uint32_t start_idx, std::uint32_t target_idx,
               const RoutingMetric& metric,
               const Filter filter = standard_filter) {
    if (verbose_) {
      LOG_S(INFO) << "Start routing from " << start_idx << " to " << target_idx
                  << " (Dijkstra, " << metric.Name() << ")";
    }
    Clear();
    std::uint32_t start_v_idx = FindOrAddVisitedNode(start_idx);
    CHECK_EQ_S(start_v_idx, 0);
    CHECK_EQ_S(visited_nodes_.at(0).from_v_idx, INF);

    visited_nodes_.front().min_metric = 0;

    Result result;
    pq_.emplace(0, start_v_idx);
    while (!pq_.empty()) {
      // Remove the minimal node from the priority queue.
      const QueuedNode qnode = pq_.top();
      pq_.pop();
      VisitedNode& vnode = visited_nodes_.at(qnode.visited_node_idx);
      if (vnode.done == 1) {
        continue;  // "old" entry in priority queue.
      }

      if (qnode.metric != vnode.min_metric) {
        ABORT_S() << absl::StrFormat(
            "qnode.metric:%u != vnode.min_metric:%u, vnode.done:%d",
            qnode.metric, vnode.min_metric, vnode.done);
      }
      vnode.done = 1;

      // Shortest route found?
      if (vnode.node_idx == target_idx) {
        target_visited_node_index_ = qnode.visited_node_idx;
        if (verbose_) {
          LOG_S(INFO) << absl::StrFormat(
              "Route found, visited nodes:%u metric:%u", visited_nodes_.size(),
              vnode.min_metric);
        }

        // Mark nodes on shortest route.
        auto current_idx = target_visited_node_index_;
        while (current_idx != INF) {
          visited_nodes_.at(current_idx).shortest_route = 1;
          current_idx = visited_nodes_.at(current_idx).from_v_idx;
        }
        result.found = true;
        result.found_distance = vnode.min_metric;
        return result;
      }

      // Search neighbours.
      const GNode& node = g_.nodes.at(vnode.node_idx);
      const std::uint32_t min_metric = vnode.min_metric;
      // TODO: Do not use 'vnode' after this line, because
      // FindOrAddVisitedNode() in the loop below might invalidate it.
      for (size_t i = 0; i < node.num_edges_out; ++i) {
        const GEdge& edge = node.edges[i];
        // Even if we want to avoid dead ends, still allow to leave a dead end
        // but not *entering* one. This way, the start node can reside in a dead
        // end and routing still works. To achieve this behavior, we allow to
        // pass a bridge only when leaving a dead end.
        if (filter.avoid_dead_end && edge.bridge && !node.dead_end) {
          continue;
        }
        if (filter.restrict_to_cluster &&
            g_.nodes.at(edge.other_node_idx).cluster_id != filter.cluster_id) {
          continue;
        }
        std::uint32_t v_idx = FindOrAddVisitedNode(edge.other_node_idx);
        VisitedNode& vother = visited_nodes_.at(v_idx);
        std::uint32_t new_metric =
            min_metric + metric.Compute(g_.ways.at(edge.way_idx), edge);
        if (!vother.done && new_metric < vother.min_metric) {
          vother.min_metric = new_metric;
          vother.from_v_idx = qnode.visited_node_idx;
          pq_.emplace(new_metric, v_idx);
        }
      }
    }
    if (verbose_) {
      LOG_S(INFO) << "Route not found";
    }
    return result;
  }

  std::uint32_t GetFoundDistance() const {
    CHECK_NE_S(target_visited_node_index_, INFU32);
    return visited_nodes_.at(target_visited_node_index_).min_metric;
  }

  void SaveSpanningTreeSegments(const std::string& filename) {
    if (verbose_) {
      LOG_S(INFO) << "Write route to " << filename;
    }
    std::ofstream myfile;
    myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);
    for (const VisitedNode& n : visited_nodes_) {
      if (n.done && n.from_v_idx != INF) {
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
  static constexpr std::uint32_t INF = 1 << 31;

  static bool MetricCmp(const QueuedNode& left, const QueuedNode& right) {
    return left.metric > right.metric;
  }

  std::uint32_t FindOrAddVisitedNode(std::uint32_t node_idx) {
    auto iter = node_to_vnode_idx_.find(node_idx);
    if (iter != node_to_vnode_idx_.end()) {
      return iter->second;
    }
    visited_nodes_.emplace_back(node_idx, INF, INF, 0, 0);
    node_to_vnode_idx_[node_idx] = visited_nodes_.size() - 1;
    return visited_nodes_.size() - 1;
  }

  void Clear() {
    visited_nodes_.clear();
    node_to_vnode_idx_.clear();
    CHECK_S(pq_.empty());  // No clear() method, should be empty anyways.
    target_visited_node_index_ = INFU32;
  }

  const Graph& g_;
  std::vector<VisitedNode> visited_nodes_;
  absl::flat_hash_map<uint32_t, uint32_t> node_to_vnode_idx_;
  std::priority_queue<QueuedNode, std::vector<QueuedNode>, decltype(&MetricCmp)>
      pq_;
  std::uint32_t target_visited_node_index_;
  const bool verbose_;
};
