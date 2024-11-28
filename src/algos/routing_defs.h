#pragma once

#include "algos/tarjan.h"
#include "base/util.h"
#include "graph/graph_def.h"

struct RoutingOptions {
  VEHICLE vt = VH_MOTOR_VEHICLE;
  // Avoid travelling *into* a dead-end over a bridge. Note that the other
  // direction (from dead-end over a bridge) is always allowed. This way, the
  // start node can be in a dead end.
  bool avoid_dead_end = true;
  bool restrict_to_cluster = false;
  // Search the shortest way inforward (false) or backward (true) mode.
  bool backward_search = false;
  // Node index (in graph.nodes) of a node that is on the non-dead-end side of a
  // bridge. Setting this node allows to travel the bridge from the non-dead-end
  // part of the network.
  std::uint32_t allow_bridge_node_idx = INFU32;
  std::uint32_t cluster_id = INFU32;

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
    allow_bridge_node_idx = bridge_node_idx;
  }
};

struct RoutingResult {
  bool found = false;
  // If a route was found, the distance from start to target node.
  uint32_t found_distance = INFU32;
  /*
  // If a route was found, the internal visited index of the target node.
  uint32_t found_target_visited_node_index = INFU32;
  */
};

// Handle the common reasons why we reject to follow an edge.
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
  if (opt.restrict_to_cluster &&
      g.nodes.at(edge.other_node_idx).cluster_id != opt.cluster_id) {
    return true;
  }
  // Is edge routable for the given vehicle type in the opt?
  return !RoutableAccess(GetRAFromWSA(wsa, opt.vt, edge_direction).access);
}
