#pragma once

#include <deque>
#include <fstream>
#include <queue>
#include <vector>

#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "algos/routing_defs.h"
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

  // verbosity = 0 (quiet), 1 (few), 2 (detailed)
  DijkstraRouter(const Graph& g, bool verbosity = 1)
      : g_(g), pq_(&MetricCmp), verbosity_(verbosity) {
    Clear();
  }

  std::string AlgoName() {
    return "dijkstra";
  }

  std::string Name(const RoutingMetric& metric, const RoutingOptions& opt) {
    return absl::StrFormat("Dijkstra, %s, %s%s", metric.Name(),
                           opt.backward_search ? "backward" : "forward ",
                           opt.hybrid.on ? ", hybrid" : "");
  }

  RoutingResult Route(std::uint32_t start_idx, std::uint32_t target_idx,
                      const RoutingMetric& metric, const RoutingOptions& opt) {
    if (verbosity_ > 0) {
      LOG_S(INFO) << absl::StrFormat(
          "Start routing from %u to %u (Dijkstra%s, %s, %s)", start_idx,
          target_idx, opt.hybrid.on ? "-hybrid" : "", metric.Name(),
          opt.backward_search ? "backward" : "forward");
    }
    Clear();
    std::uint32_t start_v_idx = FindOrAddVisitedNode(start_idx);
    CHECK_EQ_S(start_v_idx, 0);
    CHECK_EQ_S(visited_nodes_.at(0).from_v_idx, INFU32);

    visited_nodes_.front().min_metric = 0;

    RoutingResult result;
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
      if (verbosity_ > 1) {
        LOG_S(INFO) << absl::StrFormat("POP node:%u(m:%d) done:0->1",
                                       g_.nodes.at(vnode.node_idx).node_id,
                                       qnode.metric);
      }

      // Shortest route found?
      if (vnode.node_idx == target_idx) {
        target_visited_node_index_ = qnode.visited_node_idx;
        // Mark nodes on shortest route.
        auto current_idx = target_visited_node_index_;
        while (current_idx != INFU32) {
          VisitedNode& vnode = visited_nodes_.at(current_idx);
          vnode.shortest_route = 1;
          current_idx = visited_nodes_.at(current_idx).from_v_idx;
          if (verbosity_ > 1) {
            const GNode& n = g_.nodes.at(vnode.node_idx);
            LOG_S(INFO) << absl::StrFormat(
                "Shortest route: metric:%12u node:%u cluster:%u",
                vnode.min_metric, n.node_id, n.cluster_id);
          }
        }
        result.found = true;
        result.found_distance = vnode.min_metric;
        result.num_visited_nodes = visited_nodes_.size();
        if (verbosity_ > 0) {
          LOG_S(INFO) << absl::StrFormat(
              "Route found, visited nodes:%u metric:%u", visited_nodes_.size(),
              vnode.min_metric);
        }

        return result;
      }

      if (!opt.backward_search) {
        ExpandNeighboursForward(qnode, metric, opt, vnode.node_idx);
      } else {
        ExpandNeighboursBackward(qnode, metric, opt, vnode.node_idx);
      }
    }
    if (verbosity_ > 0) {
      LOG_S(INFO) << "Route not found";
    }
    return result;
  }

  void SaveSpanningTreeSegments(const std::string& filename) {
    if (verbosity_ > 0) {
      LOG_S(INFO) << "Write route to " << filename;
    }
    std::ofstream myfile;
    myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);
    // Draw the shortest route in red over the spanning tree in black.
    for (int output_shortest = 0; output_shortest <= 1; output_shortest++) {
      for (const VisitedNode& n : visited_nodes_) {
        if (n.done && n.from_v_idx != INFU32) {
          const VisitedNode& from = visited_nodes_.at(n.from_v_idx);
          const GNode& sn = g_.nodes.at(n.node_idx);
          const GNode& sfrom = g_.nodes.at(from.node_idx);
          bool shortest = from.shortest_route && n.shortest_route;
          if (shortest == (output_shortest > 0)) {
            myfile << absl::StrFormat("line,%s,%d,%d,%d,%d\n",
                                      shortest ? "red" : "black", sfrom.lat,
                                      sfrom.lon, sn.lat, sn.lon);
          }
        }
      }
    }
    myfile.close();
  }

 private:
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
      visited_nodes_.emplace_back(node_idx, INFU32, INFU32, 0, 0);
    }
    return iter.first->second;
  }

  void ExpandNeighboursForward(const QueuedNode& qnode,
                               const RoutingMetric& metric,
                               const RoutingOptions& opt,
                               std::uint32_t node_idx) {
    const GNode& node = g_.nodes.at(node_idx);
    for (size_t i = 0; i < node.num_edges_out; ++i) {
      const GEdge& edge = node.edges[i];
      const WaySharedAttrs& wsa = GetWSA(g_, edge.way_idx);
      if (RoutingRejectEdge(g_, opt, node, node_idx, edge, wsa,
                            EDGE_DIR(edge))) {
        continue;
      }

      std::uint32_t v_other_idx = FindOrAddVisitedNode(edge.other_node_idx);
      VisitedNode& vother = visited_nodes_.at(v_other_idx);
      if (vother.done) {
        continue;
      }

      std::uint32_t new_metric =
          qnode.metric + metric.Compute(wsa, opt.vt, EDGE_DIR(edge), edge);
      if (verbosity_ > 1) {
        LOG_S(INFO) << absl::StrFormat(
            "NORMAL        Examine from:%u(m:%d) to:%u done:%d new-metric:%d "
            "old-metric:%d",
            node.node_id, qnode.metric,
            g_.nodes.at(edge.other_node_idx).node_id, vother.done, new_metric,
            vother.min_metric);
      }

      if (new_metric < vother.min_metric) {
        vother.min_metric = new_metric;
        vother.from_v_idx = qnode.visited_node_idx;
        pq_.emplace(new_metric, v_other_idx);
      }
    }

    if (opt.hybrid.on && node.cluster_border_node &&
        node.cluster_id != opt.hybrid.start_cluster_id &&
        node.cluster_id != opt.hybrid.target_cluster_id) {
      // Expand the 'virtual' links within the cluster, using the precomputed
      // distances.
      const GCluster& cluster = g_.clusters.at(node.cluster_id);
      const std::vector<std::uint32_t>& v_dist =
          cluster.GetBorderNodeDistances(node_idx);

      for (size_t i = 0; i < cluster.border_nodes.size(); ++i) {
        uint32_t other_idx = cluster.border_nodes.at(i);
        if (other_idx == node_idx) continue;
        const std::uint32_t dist = v_dist.at(i);
        if (dist == INFU32) continue;  // node is not reachable.
        CHECK_LT_S(dist, INFU30);      // This shouldn't be so big.
        // TODO: check for potential overflow.
        std::uint32_t new_metric = qnode.metric + dist;
        const std::uint32_t v_other_idx = FindOrAddVisitedNode(other_idx);
        VisitedNode& vother = visited_nodes_.at(v_other_idx);

        if (verbosity_ > 1) {
          LOG_S(INFO) << absl::StrFormat(
              "CLUSTER %5u from:%u(m:%d) to:%u done:%d new-metric:%d "
              "old-metric:%d",
              node.cluster_id, node.node_id, qnode.metric,
              g_.nodes.at(other_idx).node_id, vother.done, new_metric,
              vother.min_metric);
        }

        if (!vother.done && new_metric < vother.min_metric) {
          // node.node_id == 104271819 || node.node_id == 1479539537
          vother.min_metric = new_metric;
          vother.from_v_idx = qnode.visited_node_idx;
          pq_.emplace(new_metric, v_other_idx);
        }
      }
    }
  }

  void ExpandNeighboursBackward(const QueuedNode& qnode,
                                const RoutingMetric& metric,
                                const RoutingOptions& opt,
                                std::uint32_t node_idx) {
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
          qnode.metric +
          metric.Compute(wsa, opt.vt, EDGE_INVERSE_DIR(edge), edge);
      if (new_metric < vother.min_metric) {
        vother.min_metric = new_metric;
        vother.from_v_idx = qnode.visited_node_idx;
        pq_.emplace(new_metric, v_idx);
      }
    }
  }

  const Graph& g_;
  std::deque<VisitedNode> visited_nodes_;
  typedef absl::flat_hash_map<uint32_t, uint32_t> NodeIdMap;
  NodeIdMap node_to_vnode_idx_;
  std::priority_queue<QueuedNode, std::vector<QueuedNode>, decltype(&MetricCmp)>
      pq_;
  std::uint32_t target_visited_node_index_;
  const int verbosity_;
};
