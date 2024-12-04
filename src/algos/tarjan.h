#pragma once

#include <deque>
#include <queue>
#include <vector>

#include "base/util.h"
#include "graph/graph_def.h"

// Mark the bridge (from_idx, to_idx) as bridge, but only when node 'from_idx'
// is not marked as 'dead-end'. This happens when a larger dead-end contains a
// smaller dead-end.
inline void MarkBridgeEdge(Graph& g, uint32_t from_idx, uint32_t to_idx) {
  GNode& from = g.nodes.at(from_idx);
  if (from.dead_end) return;
  for (size_t edge_pos = 0; edge_pos < gnode_total_edges(from); ++edge_pos) {
    GEdge& e = from.edges[edge_pos];
    if (e.other_node_idx == to_idx) {
      e.bridge = 1;
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
    GNode& n = g.nodes.at(node_pos);
    for (size_t edge_pos = 0; edge_pos < gnode_total_edges(n); ++edge_pos) {
      GEdge& e = n.edges[edge_pos];
      if (e.bridge || !e.unique_other) continue;
      GNode& other = g.nodes.at(e.other_node_idx);
      if (!other.dead_end) {
        other.dead_end = 1;
        num_nodes++;
        nodes.push_back(e.other_node_idx);
        // Mark to_bridge edges on the other node.
        bool has_to_bridge = false;
        for (size_t o_pos = 0; o_pos < gnode_total_edges(other); ++o_pos) {
          GEdge& e = other.edges[o_pos];
          if (e.other_node_idx == node_pos) {
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
void FindBridge(const Graph& g, const uint32_t start_node_idx,
                uint32_t* node1_idx, uint32_t* node2_idx) {
  // Stop after too many iterations, looks like a program error.
  int32_t count = 10000000;
  uint32_t pos = start_node_idx;
  while (count-- > 0) {
    const GNode& n = g.nodes.at(pos);
    CHECK_S(n.dead_end);
    for (size_t i = 0; i < gnode_total_edges(n); ++i) {
      const GEdge& e = n.edges[i];
      if (e.to_bridge) {
        pos = e.other_node_idx;
        break;
      } else if (e.bridge) {
        // Found the bridge.
        if (node1_idx != nullptr) *node1_idx = pos;
        if (node2_idx != nullptr) *node2_idx = e.other_node_idx;
        CHECK_S(!g.nodes.at(e.other_node_idx).dead_end)
            << g.nodes.at(e.other_node_idx).node_id;
        return;
      }
    }
  }
  ABORT_S() << "FindBridge " << g.nodes.at(start_node_idx).node_id << " "
            << count;
}

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
      std::uint32_t next_edge_pos;
      // Number of nodes in the spanning tree below 'node'.
      std::uint32_t tree_size;
    };
    std::int32_t time = 0;

    std::vector<StackEntry> stack;
    stack.emplace_back(/*parent=*/-1, /*node=*/comp.start_node,
                       /*next_edge_pos=*/0);

    while (!stack.empty()) {
      StackEntry& e = stack.back();

      // === INIT
      if (e.next_edge_pos == 0) {
        visno->at(e.node) = time;
        low->at(e.node) = time;
      }

      std::uint32_t child;
      if (GetNextNeighbour(g_.nodes.at(e.node),
                           /*edge_pos=*/&e.next_edge_pos,
                           /*neighbour=*/&child)) {
        // HANDLE CHILD.
        if (visno->at(child) == -1) {
          // DOWN INTO RECURSION.
          time++;
          e.tree_size++;
          stack.emplace_back(/*parent=*/e.node, /*node=*/child,
                             /*next_edge_pos=*/0);
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
        "Tarjan.FindBridges() Component start node %u size %u", comp.start_node,
        comp.size);
    CHECK_GT_S(comp.size, 0u);
    DFSIterative(comp, &visno, &low, bridges);
    return 0;
  }

 private:
  // Beginning with n.edge[*edge_pos], find the first edge that has
  // 'unique_other' set and return the connected node in 'neighbour'. Returns
  // false if no more edges with 'unique_other' exist.
  //
  // This is used to iterate over the undirected neighbours of a node.
  bool GetNextNeighbour(const GNode& n, std::uint32_t* edge_pos,
                        std::uint32_t* neighbour) {
    const uint32_t num_edges = gnode_total_edges(n);
    while ((*edge_pos) < num_edges) {
      if (n.edges[*edge_pos].unique_other) {
        *neighbour = n.edges[*edge_pos].other_node_idx;
        (*edge_pos)++;
        return true;
      }
      (*edge_pos)++;
    }
    return false;
  }

  const Graph& g_;
  std::vector<std::int32_t> tmp_node_list_;
};
