#pragma once

#include <deque>
#include <queue>
#include <vector>

#include "base/util.h"
#include "graph/graph_def.h"

// Find bridges in undirected graph.
class Tarjan {
 public:
  struct BridgeInfo {
    // Node in the non-dead-end part of the graph.
    int32_t from_node_idx;
    // Node in the dead-end part of the graph.
    int32_t to_node_idx;
    // Size of DFS-induced subtree below to_node.
    uint32_t subtree_size;
  };
  Tarjan(const Graph& g) : g_(g) {}

#if 0
  // Recursive Version of Tarjan bridge finding algorithm. Not used because
  // recursion causes stack memory issues on large graphs.
  void DFS(std::int32_t parent, std::int32_t current, std::int32_t time,
           std::vector<std::int32_t>* visno, std::vector<std::int32_t>* low) {
    visno->at(current) = time;
    low->at(current) = time;
    ++time;
    for (std::int32_t v : GetAdjacent(g_.nodes.at(current))) {
      if (visno->at(v) == -1) {
        DFS(current, v, time, visno, low);
        // Return from recursion.
        low->at(current) = std::min(low->at(current), low->at(v));
        if (low->at(v) > visno->at(current)) {
          const GNode& n1 = g_.nodes.at(current);
          const GNode& n2 = g_.nodes.at(v);
          LOG_S(INFO) <<
            absl::StrFormat("bridge %lld - %lld", n1.node_id, n2.node_id);
          LOG_S(INFO) << absl::StrFormat("[[%f,%f],[%f,%f]],",
              n1.lon / 10000000.0, n1.lat / 10000000.0,
              n2.lon / 10000000.0, n2.lat / 10000000.0);
        }
      } else if (v != parent) {
        // Node is visited but not parent.
        low->at(current) = std::min(low->at(current), visno->at(v));
      }
    }
  }
#endif

  // Iterative version of DFS, with explicit stack management in a vector.
  // This is necessary, because recursion exhausts the normal program stack on
  // large graphs. This implements the recursive Version above.
  void DFSIterative(Graph::Component comp, std::vector<std::int32_t>* visno,
                    std::vector<std::int32_t>* low,
                    std::vector<BridgeInfo>* bridges) {
    struct StackEntry {
      std::int32_t parent;
      std::int32_t node;
      // Offset of the edge within node, i.e. starting with 0.
      std::uint32_t next_edge_offset;
      // Number of nodes in the spanning tree below 'node'.
      std::uint32_t tree_size;
    };
    std::int32_t time = 0;

    std::vector<StackEntry> stack;
    stack.emplace_back(/*parent=*/-1, /*node=*/comp.start_node,
                       /*next_edge_offset=*/0);

    while (!stack.empty()) {
      StackEntry& e = stack.back();

      // === INIT
      if (e.next_edge_offset == 0) {
        visno->at(e.node) = time;
        low->at(e.node) = time;
      }

      std::uint32_t child;
      if (GetNextNeighbour(e.node,
                           /*edge_offset=*/&e.next_edge_offset,
                           /*neighbour=*/&child)) {
        // HANDLE CHILD.
        if (visno->at(child) == -1) {
          // DOWN INTO RECURSION.
          time++;
          e.tree_size++;
          stack.emplace_back(/*parent=*/e.node, /*node=*/child,
                             /*next_edge_offset=*/0);
        } else if ((uint32_t)e.parent != child) {
          // NODE ALREADY VISITED.
          low->at(e.node) = std::min(low->at(e.node), visno->at(child));
        }
      } else {
        // UP FROM RECURSION.
        if (e.parent >= 0) {
          stack.at(stack.size() - 2).tree_size += e.tree_size;
          low->at(e.parent) = std::min(low->at(e.parent), low->at(e.node));
          if (low->at(e.node) > visno->at(e.parent)) {
            // The edge "parent <=> node" is a bridge.
            if (bridges != nullptr) {
              // Arrange such that the tree below 'to_node_idx' is smaller.
              if (e.tree_size < comp.size / 2) {
                CHECK_LE_S(e.tree_size, comp.size);
                bridges->push_back({.from_node_idx = e.parent,
                                    .to_node_idx = e.node,
                                    .subtree_size = e.tree_size});
              } else {
                // This can happen when DFS started in a dead-end.
                bridges->push_back({.from_node_idx = e.node,
                                    .to_node_idx = e.parent,
                                    .subtree_size = comp.size - e.tree_size});
              }
            }
          }
        } else {
          // LOG_S(INFO) << absl::StrFormat("finish at root tree below %d",
          // e.tree_size);
        }
        stack.pop_back();
      }
    }
  }

  std::uint32_t FindBridges(Graph::Component comp,
                            std::vector<BridgeInfo>* bridges = nullptr) {
    std::vector<std::int32_t> visno(g_.nodes.size(), -1);
    std::vector<std::int32_t> low(g_.nodes.size(), -1);
    LOG_S(INFO) << absl::StrFormat(
        "Tarjan.FindBridges() Component start node %lld (idx:%u) size %u",
        GetGNodeIdSafe(g_, comp.start_node), comp.start_node, comp.size);
    CHECK_GT_S(comp.size, 0u);
    DFSIterative(comp, &visno, &low, bridges);
    return 0;
  }

 private:
  // Beginning with n.edge[*edge_offset], find the first edge that has
  // 'unique_target' set and return the connected node in 'neighbour'. Returns
  // false if no more edges with 'unique_target' exist.
  //
  // This is used to iterate over the undirected neighbours of a node.
  bool GetNextNeighbour(uint32_t node_idx, std::uint32_t* edge_offset,
                        std::uint32_t* neighbour) {
    const GNode& n = g_.nodes.at(node_idx);
    size_t current = n.edges_start_pos + (*edge_offset);
    size_t stop = gnode_edges_stop(g_, node_idx);

    while (current < stop) {
      if (g_.edges.at(current).unique_target) {
        *neighbour = g_.edges.at(current).target_idx;
        (*edge_offset) = current + 1 - n.edges_start_pos;
        return true;
      }
      current++;
    }
    return false;
  }
#if 0
  bool GetNextNeighbour(const GNode& n, std::uint32_t* edge_pos,
                        std::uint32_t* neighbour) {
    const uint32_t num_edges = gnode_total_edges(n);
    while ((*edge_pos) < num_edges) {
      if (n.edges[*edge_pos].unique_target) {
        *neighbour = n.edges[*edge_pos].target_idx;
        (*edge_pos)++;
        return true;
      }
      (*edge_pos)++;
    }
    return false;
  }
#endif

  const Graph& g_;
  std::vector<std::int32_t> tmp_node_list_;
};

// Mark the bridge (from_idx, to_idx) as bridge, but only when node 'from_idx'
// is not marked as 'dead-end'. This happens when a larger dead-end contains a
// smaller dead-end.
inline void MarkBridgeEdge(Graph& g, uint32_t from_idx, uint32_t to_idx) {
  GNode& from = g.nodes.at(from_idx);
  if (from.dead_end) return;
  for (GEdge& e : gnode_all_edges(g, from_idx)) {
    // for (size_t edge_pos = 0; edge_pos < gnode_total_edges(from); ++edge_pos)
    // { GEdge& e = from.edges[edge_pos];
    if (e.target_idx == to_idx) {
      // e.bridge = 1;  // TODO: remove
      CHECK_EQ_S((int)e.type, (int)GEdge::TYPE_UNKNOWN);
      e.type = GEdge::TYPE_DEADEND_BRIDGE;
    }
  }
}

// Mark the nodes in the subtree below 'start_node_idx' as 'dead-end' and make
// sure for dead-end each node there is at least one edge marked 'to_bridge'.
//
// Note that 'start_node_idx' has to be on the dead-end side of a bridge edge.
// Requires that bridge edges at 'start_node_idx' are already marked.
inline uint32_t MarkDeadEndNodes(Graph& g, uint32_t start_node_idx,
                                 uint32_t expected_size) {
  GNode& start = g.nodes.at(start_node_idx);
  if (start.dead_end) {
    // There was a bridge with a larger subtree that contained this bridge, so
    // we ignore this start_node.
    return 0;
  }
  std::vector<uint32_t> nodes;
  nodes.push_back(start_node_idx);
  start.dead_end = 1;
  uint32_t num_nodes = 1;
  size_t pos = 0;
  while (pos < nodes.size()) {
    uint32_t node_pos = nodes.at(pos++);
    // GNode& n = g.nodes.at(node_pos);
    for (GEdge& e : gnode_all_edges(g, node_pos)) {
      // for (size_t edge_pos = 0; edge_pos < gnode_total_edges(n); ++edge_pos)
      // { GEdge& e = n.edges[edge_pos];
      
      // if (e.bridge || !e.unique_target) continue;
      if (e.is_deadend_bridge()) continue;
      CHECK_EQ_S((int)e.type, (int)GEdge::TYPE_UNKNOWN);
      e.type = GEdge::TYPE_DEADEND_INNER;
      if (!e.unique_target) continue;

      GNode& other = g.nodes.at(e.target_idx);
      if (!other.dead_end) {
        other.dead_end = 1;
        num_nodes++;
        nodes.push_back(e.target_idx);
        // Mark to_bridge edges on the other node.
        bool has_to_bridge = false;
        for (GEdge& e : gnode_all_edges(g, e.target_idx)) {
          // for (size_t o_pos = 0; o_pos < gnode_total_edges(other); ++o_pos) {
          // GEdge& e = other.edges[o_pos];
          if (e.target_idx == node_pos) {
            e.to_bridge = true;
            has_to_bridge = true;
          }
        }
        // Every dead-end node needs at least one edge with to_bridge==1.
        CHECK_S(has_to_bridge) << start.node_id << " " << other.node_id;
      }
    }
  }
  return num_nodes;
}

// Given a dead-end node at 'start_node_idx', find the bridge belonging to the
// node. Check fails if the node is not in a dead-end or if the bridge can't be
// found.
//
// As a result, node1_idx is the node on the dead-end side of the bridge and
// node2_idx on the non-dead-end side of the bridge.
inline void FindBridge(const Graph& g, const uint32_t start_node_idx,
                       uint32_t* node1_idx, uint32_t* node2_idx) {
  // Stop after too many iterations, looks like a program error.
  int32_t count = 10000000;
  uint32_t pos = start_node_idx;
  while (count-- > 0) {
    const GNode& n = g.nodes.at(pos);
    CHECK_S(n.dead_end);
    for (const GEdge& e : gnode_all_edges(g, pos)) {
      // for (size_t i = 0; i < gnode_total_edges(n); ++i) {
      // const GEdge& e = n.edges[i];
      if (e.to_bridge) {
        pos = e.target_idx;
        break;
      } else if (e.is_deadend_bridge()) {
        // Found the bridge.
        if (node1_idx != nullptr) *node1_idx = pos;
        if (node2_idx != nullptr) *node2_idx = e.target_idx;
        CHECK_S(!g.nodes.at(e.target_idx).dead_end)
            << g.nodes.at(e.target_idx).node_id;
        return;
      }
    }
  }
  ABORT_S() << "FindBridge " << g.nodes.at(start_node_idx).node_id << " "
            << count;
}

inline int64_t ApplyTarjan(Graph& g) {
  FUNC_TIMER();
  int64_t num_dead_end_nodes = 0;
  if (g.large_components.empty()) {
    ABORT_S() << "g.large_components is empty";
  }
  Tarjan t(g);
  std::vector<Tarjan::BridgeInfo> bridges;
  for (const Graph::Component& comp : g.large_components) {
    bridges.clear();
    t.FindBridges(comp, &bridges);
    // Sort bridges in descending order of subtrees. If a dead-end contains
    // other, smaller dead-ends, then only the dead-end with the largest subtree
    // is actually used.
    std::sort(bridges.begin(), bridges.end(),
              [](const Tarjan::BridgeInfo& a, const Tarjan::BridgeInfo& b) {
                return a.subtree_size > b.subtree_size;
              });
    for (const Tarjan::BridgeInfo& bridge : bridges) {
      MarkBridgeEdge(g, bridge.from_node_idx, bridge.to_node_idx);
      MarkBridgeEdge(g, bridge.to_node_idx, bridge.from_node_idx);
      num_dead_end_nodes +=
          MarkDeadEndNodes(g, bridge.to_node_idx, bridge.subtree_size);
    }
  }
  LOG_S(INFO) << absl::StrFormat("Graph has %lld (%.2f%%) dead end nodes.",
                                 num_dead_end_nodes,
                                 (100.0 * num_dead_end_nodes) / g.nodes.size());
  return num_dead_end_nodes;
}
