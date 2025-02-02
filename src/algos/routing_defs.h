#pragma once

#include "absl/container/flat_hash_set.h"
#include "algos/restricted_access_edges.h"
#include "algos/tarjan.h"
#include "base/util.h"
#include "geometry/geometry.h"
#include "graph/graph_def.h"

// Return the cluster of a node. If the nodes is in a dead end, then the bridge
// is found and the cluster id of the node on the other side of the bridge is
// returned.
inline uint32_t FindClusterOfNode(const Graph& g, std::uint32_t node_idx) {
  const GNode& n = g.nodes.at(node_idx);
  if (!n.dead_end) {
    CHECK_NE_S(n.cluster_id, INVALID_CLUSTER_ID) << n.node_id;
    return n.cluster_id;
  }
  std::uint32_t cluster_node_idx;
  FindBridge(g, node_idx, nullptr, &cluster_node_idx);
  const GNode& cluster_node = g.nodes.at(cluster_node_idx);
  CHECK_NE_S(cluster_node.cluster_id, INVALID_CLUSTER_ID);
  return cluster_node.cluster_id;
}

struct RoutingOptions {
  VEHICLE vt = VH_MOTOR_VEHICLE;
  // Avoid travelling *into* a dead-end over a bridge. Note that the other
  // direction (from dead-end over a bridge) is always allowed. This way, the
  // start node can be in a dead end.
  bool avoid_dead_end = true;
  bool avoid_restricted_access_edges = false;
  bool restrict_to_cluster = false;
  // Search the shortest way forward (false) or backward (true) mode.
  bool backward_search = false;
  // If to use AStar heuristic.
  bool use_astar_heuristic = false;
  // Node index (in graph.nodes) of a node that is on the non-dead-end side of a
  // bridge. Setting this node allows to travel the bridge from the non-dead-end
  // part of the network.
  std::uint32_t allow_bridge_node_idx = INFU32;
  std::uint32_t restrict_cluster_id = INFU32;

  struct HybridOptions {
    bool on = false;
    std::uint32_t start_idx = INFU32;
    std::uint32_t target_idx = INFU32;
    std::uint32_t start_cluster_id = INFU32;
    std::uint32_t target_cluster_id = INFU32;
  };
  HybridOptions hybrid;

  // If the target node is in a dead-end, fills 'allow_bridge_node_id' with
  // the node_id of the bridge leading to node 'target_idx'. Does nothing if
  // the target node is not in a dead-end.
  void MayFillBridgeNodeId(const Graph& g, std::uint32_t target_idx) {
    if (!g.nodes.at(target_idx).dead_end) {
      return;
    }
    std::uint32_t bridge_node_idx;
    FindBridge(g, target_idx, nullptr, &bridge_node_idx);
    // LOG_S(INFO) << "Bridge node " << g.nodes.at(bridge_node_idx).node_id;
    allow_bridge_node_idx = bridge_node_idx;
  }

  // Sets the options in 'hybrid' and 'allow_bridge_node_idx' if the target node
  // is in a dead end.
  void SetHybridOptions(const Graph& g, std::uint32_t start_idx,
                        std::uint32_t target_idx) {
    hybrid = {.on = true,
              .start_idx = start_idx,
              .target_idx = target_idx,
              .start_cluster_id = FindClusterOfNode(g, start_idx),
              .target_cluster_id = FindClusterOfNode(g, target_idx)};
    MayFillBridgeNodeId(g, target_idx);
    CHECK_S(!restrict_to_cluster);
  }

 private:
};

// Helper data for routers that want to protect a restricted-access area
// ("access=destination" etc.) from being used wrongly during routing.
struct RestrictedAccessArea {
  bool active = false;
  // This is true iff both start and target node are in a non-empty destination
  // area and the two areas are the same. In this case, a special handling for
  // edge keys is needed in Dijkstra.
  bool start_equal_target = false;
  Rectangle<int32_t> bounding_rect = {0, 0, 0, 0};
  absl::flat_hash_set<uint32_t> transition_nodes;

  void InitialiseTransitionNodes(const Graph& g, uint32_t start_idx,
                                 uint32_t target_idx) {
    transition_nodes = GetRestrictedAccessTransitionNodes(g, target_idx);
    active = transition_nodes.size() > 0;
    start_equal_target = false;
    if (active) {
      auto start_trans = GetRestrictedAccessTransitionNodes(g, start_idx);
      start_equal_target = (start_trans.size() == transition_nodes.size() &&
                            transition_nodes.contains(*start_trans.begin()));
    }
    if (active) {
      // Compute bounding rectangle of all transition nodes.
      const GNode& n1 = g.nodes.at(target_idx);
      bounding_rect.Set({n1.lat, n1.lon});
      for (uint32_t idx : transition_nodes) {
        const GNode& n2 = g.nodes.at(idx);
        bounding_rect.ExtendBound({n2.lat, n2.lon});
      }
    }
  }
};

struct RoutingResult {
  bool found = false;
  // If a route was found, the distance from start to target node.
  uint32_t found_distance = 0;
  uint32_t num_visited = 0;  // edges or nodes, depending on router type.
  uint32_t num_shortest_route_nodes = 0;
  std::vector<uint32_t> route_v_idx;  // Filled iff found == true.
};

// Check if the routing options 'opt' make us reject to follow 'edge'.
inline bool RoutingRejectEdge(const Graph& g, const RoutingOptions& opt,
                              const GNode& node, std::uint32_t node_idx,
                              const GEdge& edge, const WaySharedAttrs& wsa,
                              DIRECTION edge_direction) {
  if (edge.bridge && opt.avoid_dead_end && !node.dead_end &&
      node_idx != opt.allow_bridge_node_idx) {
    // Node is in the non-dead-end side of the bridge, so ignore edge and
    // do not enter the dead end.
    return true;
  }

  if (opt.avoid_restricted_access_edges && opt.vt == VH_MOTOR_VEHICLE &&
      edge.car_label != GEdge::LABEL_FREE) {
    return true;
  }

  const GNode& other = g.nodes.at(edge.other_node_idx);
  if (opt.restrict_to_cluster && other.cluster_id != opt.restrict_cluster_id) {
    return true;
  }

  // Hybrid search.
  // If we are in a dead-end (or entering one), then we must be in start or
  // target cluster. That's ok in all cases.
  if (opt.hybrid.on && !node.dead_end && !other.dead_end) {
    bool to_start_or_end = (other.cluster_id == opt.hybrid.start_cluster_id) ||
                           (other.cluster_id == opt.hybrid.target_cluster_id);
    // Always ok if the edge goes to the start or to the end cluster.
    if (!to_start_or_end) {
      // A connection to outside start/target clusters must be - by construction
      // - from a border node (in any cluster).
      CHECK_S(node.cluster_border_node) << node.node_id;
      // Outside connections must be between border nodes in different clusters.
      // Same cluster is not ok because this would be an inner edge for this
      // cluster, even if it connects border nodes.
      if (!other.cluster_border_node || node.cluster_id == other.cluster_id) {
        return true;
      }
    }
  }

  // Is edge routable in 'opt' for the given vehicle type?
  return !RoutableAccess(GetRAFromWSA(wsa, opt.vt, edge_direction).access);
}
