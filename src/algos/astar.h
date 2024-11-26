#pragma once

#include <fstream>
#include <queue>
#include <vector>

#include "absl/container/flat_hash_map.h"
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

  struct Filter {
    VEHICLE vt;
    // Avoid travelling *into* a dead-end over a bridge. Note that the other
    // direction (from dead-end over a bridge) is always allowed. This way, the
    // start node can be in a dead end.
    bool avoid_dead_end;
    bool restrict_to_cluster;
    bool inverse_search;
    // OSM node id of a node that is on the non-dead-end side of a bridge.
    // Setting this node allows to travel the bridge from the non-dead-end part
    // of the network.
    std::int64_t allow_bridge_node_id;
    std::uint32_t cluster_id;

    // If the target node is in a dead-end, fills 'allow_bridge_node_id' with
    // the node_id of the bridge leading to node 'target_idx'. Does nothing if
    // the target node is not in a dead-end.
    void MayFillBridgeNodeId(const Graph& g, std::uint32_t target_idx) {
      if (!g.nodes.at(target_idx).dead_end) {
        return;
      }
      std::uint32_t bridge_node_idx;
      FindBridge(g, target_idx, nullptr, &bridge_node_idx);
      LOG_S(INFO) << "Bridge node " << g.nodes.at(bridge_node_idx).node_id;
      allow_bridge_node_id = g.nodes.at(bridge_node_idx).node_id;
    }
  };
  static constexpr Filter standard_filter = {.vt = VH_MOTOR_VEHICLE,
                                             .avoid_dead_end = true,
                                             .restrict_to_cluster = false,
                                             .inverse_search = false,
                                             .allow_bridge_node_id = -1,
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

  AStarRouter(const Graph& g, bool verbose = true)
      : g_(g),
        pq_(&MetricCmp),
        target_visited_node_index_(INFU32),
        verbose_(verbose) {
    Clear();
  }

  Result Route(std::uint32_t start_idx, std::uint32_t target_idx,
               const RoutingMetric& metric,
               const Filter filter = standard_filter) {
    if (verbose_) {
      LOG_S(INFO) << "Start routing from " << g_.nodes.at(start_idx).node_id
                  << " to " << g_.nodes.at(target_idx).node_id << " (A*, "
                  << metric.Name() << ")"
                  << (filter.inverse_search ? " backward search"
                                            : " forward search");
    }
    Clear();
    const std::int32_t target_lat = g_.nodes.at(target_idx).lat;
    const std::int32_t target_lon = g_.nodes.at(target_idx).lon;

    std::uint32_t start_v_idx = FindOrAddVisitedNode(start_idx);
    CHECK_EQ_S(start_v_idx, 0);
    CHECK_EQ_S(visited_nodes_.at(0).from_v_idx, INF30);

    visited_nodes_.front().min_metric = 0;

    Result result;
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

      if (!filter.inverse_search) {
        ExpandNeighbours(qnode, metric, filter, g_.nodes.at(vnode.node_idx),
                         vnode.min_metric, target_lat, target_lon);
      } else {
        InverseExpandNeighbours(qnode, metric, filter,
                                g_.nodes.at(vnode.node_idx), vnode.min_metric,
                                target_lat, target_lon);
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

#if 0
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
#endif

  void Clear() {
    visited_nodes_.clear();
    node_to_vnode_idx_.clear();
    CHECK_S(pq_.empty());  // No clear() method, should be empty anyways.
    target_visited_node_index_ = INFU32;
  }

  void ExpandNeighbours(const QueuedNode& qnode, const RoutingMetric& metric,
                        const Filter& filter, const GNode& node,
                        std::uint32_t min_metric, std::int32_t target_lat,
                        std::int32_t target_lon) {
    for (size_t i = 0; i < node.num_edges_out; ++i) {
      const GEdge& edge = node.edges[i];
      if (filter.avoid_dead_end && edge.bridge && !node.dead_end &&
          node.node_id != filter.allow_bridge_node_id) {
        // Node is in the non-dead-end side of the bridge, so ignore edge and
        // do not enter the dead end.
        continue;
      }
      if (filter.restrict_to_cluster &&
          g_.nodes.at(edge.other_node_idx).cluster_id != filter.cluster_id) {
        continue;
      }

      const WaySharedAttrs& wsa = GetWSA(g_, edge.way_idx);
      // Is edge routable for the given vehicle type in the filter?
      if (!RoutableAccess(
              GetRAFromWSA(wsa, filter.vt, EDGE_DIR(edge)).access)) {
        continue;
      }

      std::uint32_t v_idx = FindOrAddVisitedNode(edge.other_node_idx);
      VisitedNode& vother = visited_nodes_.at(v_idx);
      if (vother.done) {
        continue;
      }
      std::uint32_t new_metric =
          min_metric + metric.Compute(wsa, filter.vt, EDGE_DIR(edge), edge);
      if (new_metric < vother.min_metric) {
        vother.min_metric = new_metric;
        vother.from_v_idx = qnode.visited_node_idx;

        // Compute heuristic distance from new node to target.
        if (vother.heuristic_to_target == INF32) {
          // TODO: use country specific maxspeed.
          // TODO: support mexspeed for different vehicles.
          // SOLUTION: Should use per (country, vehicle) maxspeed. If target
          // is in other countru then use mix.
          static const RoutingAttrs g_ra = {.access = ACC_YES, .maxspeed = 120};
          static const WaySharedAttrs g_wsa = {
              .ra = g_ra};  // Sets .ra[0] and .ra[1]!
          const GNode& other_node = g_.nodes.at(edge.other_node_idx);
          vother.heuristic_to_target = metric.Compute(
              g_wsa, filter.vt, DIR_FORWARD,
              {.distance_cm = static_cast<uint64_t>(
                   1.00 * calculate_distance(other_node.lat /* / 10000000.0*/,
                                             other_node.lon /* / 10000000.0*/,
                                             target_lat, target_lon)),
               .contra_way = 0});
        }
        pq_.emplace(new_metric + vother.heuristic_to_target, v_idx);
      }
    }
  }

  void InverseExpandNeighbours(const QueuedNode& qnode,
                               const RoutingMetric& metric,
                               const Filter& filter, const GNode& node,
                               std::uint32_t min_metric,
                               std::int32_t target_lat,
                               std::int32_t target_lon) {
    for (size_t i = 0; i < gnode_total_edges(node); ++i) {
      const GEdge& edge = node.edges[i];
      // Skip edges that are forward only.
      if (i < node.num_edges_out && !edge.both_directions) {
        continue;
      }

      if (edge.bridge && filter.avoid_dead_end && !node.dead_end) {
        // Node is in the non-dead-end side of the bridge, so ignore edge and
        // do not enter the dead end.
        continue;
      }
      if (filter.restrict_to_cluster &&
          g_.nodes.at(edge.other_node_idx).cluster_id != filter.cluster_id) {
        continue;
      }

      const WaySharedAttrs& wsa = GetWSA(g_, edge.way_idx);
      // Is edge routable for the given vehicle type in the filter?
      if (!RoutableAccess(
              GetRAFromWSA(wsa, filter.vt, EDGE_INVERSE_DIR(edge)).access)) {
        continue;
      }

      std::uint32_t v_idx = FindOrAddVisitedNode(edge.other_node_idx);
      VisitedNode& vother = visited_nodes_.at(v_idx);
      if (vother.done) {
        continue;
      }
      std::uint32_t new_metric =
          min_metric +
          metric.Compute(wsa, filter.vt, EDGE_INVERSE_DIR(edge), edge);
      if (new_metric < vother.min_metric) {
        vother.min_metric = new_metric;
        vother.from_v_idx = qnode.visited_node_idx;

        // Compute heuristic distance from new node to target.
        if (vother.heuristic_to_target == INF32) {
          // TODO: use country specific maxspeed.
          // TODO: support mexspeed for different vehicles.
          // SOLUTION: Should use per (country, vehicle) maxspeed. If target
          // is in other countru then use mix.
          static const RoutingAttrs g_ra = {.access = ACC_YES, .maxspeed = 120};
          static const WaySharedAttrs g_wsa = {
              .ra = g_ra};  // Sets .ra[0] and .ra[1]!
          const GNode& other_node = g_.nodes.at(edge.other_node_idx);
          vother.heuristic_to_target = metric.Compute(
              g_wsa, filter.vt, DIR_FORWARD,
              {.distance_cm = static_cast<uint64_t>(
                   1.00 * calculate_distance(other_node.lat /* / 10000000.0*/,
                                             other_node.lon /* / 10000000.0*/,
                                             target_lat, target_lon)),
               .contra_way = 0});
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
