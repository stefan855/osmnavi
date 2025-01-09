#pragma once

/*
 * Some parts of the road network should not be separated into different
 * clusters. Pre-clustering runs before the real Louvain clustering begins and
 * puts nodes that should stay together into the same cluster.
 *
 * Different cases are handled:
 *
 * 1) Lines of nodes. This is a list of consecutive nodes connected through
 * forward and backward edges only, i.e. they have no other connections into the
 * road network except for start and end node. These lines of nodes are
 * collapsed into one cluster each. This reduces the size of the network and
 * speeds up clustering.
 *
 * 2) Restricted edges are edges that have access=destination or a similar
 * restriction. Edges between clusters should never be restricted because this
 * doesn't work when routing across clusters. At first look, it seems that this
 * problem can be solved by just removing restricted edges from the graph
 * before it is clustered, because restricted edges are only travelled within
 * the start- and the end-cluster and never between clusters. But with this it
 * isn't guaranteed that restricted sub-networks are fully contained within a
 * cluster, which is needed for the start- and the end-cluster.
 *
 * 3) TODO: Turn restrictions. Turn restrictions establish relations between
 * incoming and outgoing edges at crossings. All the edges making up a turn
 * restriction should be contained within the same cluster, so simplify routing
 * across clusters.
 */

#include <vector>

#include "absl/container/btree_map.h"
#include "absl/strings/str_format.h"
#include "algos/louvain.h"
#include "base/constants.h"
#include "base/huge_bitset.h"
#include "logging/loguru.h"

namespace louvain {

// NodeLineRemover removes superfluous nodes from a Louvain input graph.
// 'superfluous' in this context is a node that serves as a transit node on the
// way between two other nodes but has no other connections. Such a node has two
// neighbours connected to the node (using two edges, i.e. the node has degree
// 2), and one can remove the node from the graph and connect the two neighbour
// nodes directly instead, without changing the navigational connectivity. There
// are three cases:
// * A circle of line nodes not connected to anything else. These circles are
// left untouched.
// * A circle of line nodes connected on both ends to the same node with higher
// degree, such as streets that end in a turnaround. The nodes in the turnaround
// are clustered with the border node and weights are preserved.
// * A line of nodes that have two different border nodes with degree != 2. The
// line nodes between the two border nodes are clustered with one of the two end
// nodes, and the weights of the edges are ignored.
//
// TODO: the struct NodeLineRemover only is needed for scoping. Either use a
// real struct with data or use a different naming scheme.
struct NodeLineRemover {
  struct NodeLine {
    std::vector<uint32_t> vnodes;
    bool isolated_circle;
    bool connected_circle;
  };

  // Given two connected line nodes 'prev_node' and 'this_node', return the next
  // node after 'this_node', without returning to 'prev_node' if possible.
  //
  // A line in this context is a series of nodes that are connected in forward
  // and backward direction and nothing else, i.e. the same as a doubly linked
  // list. This means, all nodes in the line have exactly two edges.
  //
  // The line ends when a node with degree != 2 is reached, on both ends of the
  // line. The one exception is when there is a circle, in which case the line
  // is closed and there are no end nodes.
  static uint32_t NextLineNode(const LouvainGraph& lg, uint32_t prev_node,
                               uint32_t this_node) {
    const LouvainNode& n = lg.nodes.at(this_node);
    CHECK_EQ_S(n.num_edges, 2);
    uint32_t result = INFU32;
    if (lg.edges.at(n.edge_start).other_node_pos != prev_node) {
      result = lg.edges.at(n.edge_start).other_node_pos;
    } else {
      result = lg.edges.at(n.edge_start + 1).other_node_pos;
    }
    CHECK_NE_S(result, this_node);
    return result;
  }

  static void ExtendNodeLine(const LouvainGraph& lg, uint32_t node_pos,
                             std::vector<uint32_t>* vnodes,
                             bool* isolated_circle) {
    // Extend the line in the first direction given by first edge at 'node_pos'.
    while (true) {
      uint32_t next_pos =
          NextLineNode(lg, vnodes->empty() ? INFU32 : vnodes->back(), node_pos);
      CHECK_NE_S(next_pos, node_pos);  // No self links allowed.

      vnodes->push_back(node_pos);

      if (lg.nodes.at(next_pos).num_edges != 2) {
        // Found stop node, store it and stop iteration.
        vnodes->push_back(next_pos);
        return;
      } else if (next_pos == vnodes->front()) {
        CHECK_GT_S(vnodes->size(), 1u);  // No self links allowed.
        // Closed circle that is not connected to the rest of the graph.
        *isolated_circle = true;
        return;
      }
      // Valid line node.
      node_pos = next_pos;
    }
  }

  // Return a line of nodes, containing 'node_pos'. Returns true if a line
  // was found going through 'node_pos', false if not.
  static bool GetLineOfNodes(const LouvainGraph& lg, uint32_t node_pos,
                             NodeLine* res) {
    if (lg.nodes.at(node_pos).num_edges != 2) {
      return false;
    }
    res->vnodes.clear();
    res->isolated_circle = false;
    res->connected_circle = false;

    // Extend the line in the first direction given by first edge at 'node_pos'.
    ExtendNodeLine(lg, node_pos, &res->vnodes, &res->isolated_circle);

    // Special case, we found an isolated circle. In this case, vnodes
    // contains only nodes with two edges, i.e. no start/stop nodes.
    if (res->isolated_circle) {
      CHECK_GE_S(res->vnodes.size(), 2u);
      return true;
    }

    // Reverse direction such that the original node_pos is last.
    std::reverse(res->vnodes.begin(), res->vnodes.end());
    CHECK_EQ_S(res->vnodes.back(), node_pos);
    // Remove the last node, such that we can reuse the loop function.
    res->vnodes.pop_back();

    // Extend the line in the second direction given by second edge at
    // 'node_pos'.
    ExtendNodeLine(lg, node_pos, &res->vnodes, &res->isolated_circle);
    // This should be found in the first call to ExtendNodeLine().
    CHECK_S(!res->isolated_circle);
    res->connected_circle = (res->vnodes.front() == res->vnodes.back());
    return true;
  }

  // Collect all line nodes that should be clustered in 'precluster_nodes'.
  // Cluster_nodes stores the node positions in lg.nodes.
  static void CollectLineNodes(const LouvainGraph& lg,
                               HugeBitset* precluster_nodes) {
    uint32_t count_line_nodes = 0;
    uint32_t count_isolated_circle_nodes = 0;
    uint32_t count_connected_circle_nodes = 0;

    for (size_t node_pos = 0; node_pos < lg.nodes.size(); ++node_pos) {
      if (precluster_nodes->GetBit(node_pos)) {
        continue;
      }

      NodeLine nl;
      if (GetLineOfNodes(lg, node_pos, &nl)) {
        CHECK_S(!nl.vnodes.empty());
        if (nl.isolated_circle) {
          count_isolated_circle_nodes++;
        } else if (nl.connected_circle) {
          CHECK_GT_S(nl.vnodes.size(), 2);
          count_connected_circle_nodes += nl.vnodes.size() - 2;
          for (size_t i = 1; i < nl.vnodes.size() - 1; ++i) {
            const uint32_t node_pos = nl.vnodes.at(i);
            precluster_nodes->AddBit(node_pos);
          }
        } else {
          CHECK_GT_S(nl.vnodes.size(), 2);
          count_line_nodes += nl.vnodes.size() - 2;
          for (size_t i = 1; i < nl.vnodes.size() - 1; ++i) {
            const uint32_t node_pos = nl.vnodes.at(i);
            precluster_nodes->AddBit(node_pos);
          }
        }
      }
    }
    LOG_S(INFO) << "ClusterLineNodes() count_line_nodes:                "
                << count_line_nodes;
    LOG_S(INFO) << "ClusterLineNodes() count_connected_circle_nodes:    "
                << count_connected_circle_nodes;
    LOG_S(INFO) << "ClusterLineNodes() count_isolated_circle_nodes: "
                << count_isolated_circle_nodes;
  }

#if 0
  static void ClusterLineNodesOld(LouvainGraph* lg) {
    uint32_t count_line_nodes = 0;
    uint32_t count_isolated_circle_nodes = 0;
    uint32_t count_connected_circle_nodes = 0;
    HugeBitset done_nodes;

    for (size_t node_pos = 0; node_pos < lg->nodes.size(); ++node_pos) {
      if (done_nodes.GetBit(node_pos)) {
        continue;
      }

      NodeLine nl;
      if (GetLineOfNodes(*lg, node_pos, &nl)) {
        CHECK_S(!nl.vnodes.empty());
        if (nl.isolated_circle) {
          count_isolated_circle_nodes++;
        } else if (nl.connected_circle) {
          CHECK_GT_S(nl.vnodes.size(), 2);
          count_connected_circle_nodes += nl.vnodes.size() - 2;
          // Properly move these nodes to the cluster of the first border node.
          for (size_t i = 1; i < nl.vnodes.size() - 1; ++i) {
            const uint32_t node_pos = nl.vnodes.at(i);
            done_nodes.SetBit(node_pos, true);
            lg->MoveToNewCluster(node_pos, nl.vnodes.front());
          }
        } else {
          CHECK_GT_S(nl.vnodes.size(), 2);
          count_line_nodes += nl.vnodes.size() - 2;
          // Move all line nodes to the cluster of the first border node.
          // This is a "hack", because we update the graph structure only as
          // much as is needed to obtain a properly clustered graph at the next
          // level.
#if 0
          uint32_t new_cluster_pos =
              lg->nodes.at(nl.vnodes.front()).cluster_pos;
#endif
          for (size_t i = 1; i < nl.vnodes.size() - 1; ++i) {
            const uint32_t node_pos = nl.vnodes.at(i);
            done_nodes.SetBit(node_pos, true);

            lg->MoveToNewCluster(node_pos, nl.vnodes.front());
#if 0
            LouvainNode& n = lg->nodes.at(node_pos);
            LouvainCluster& old_cluster = lg->clusters.at(n.cluster_pos);
            CHECK_EQ_S(old_cluster.num_nodes, 1);
            old_cluster.num_nodes = 0;  // mark as 'deleted'.
            lg->empty_clusters_++;
            old_cluster.w_inside_edges = 0;
            old_cluster.w_tot_edges = 0;
            n.cluster_pos = new_cluster_pos;
            LouvainCluster& new_cluster = lg->clusters.at(n.cluster_pos);
            CHECK_GE_S(new_cluster.num_nodes, 1);
            new_cluster.num_nodes++;
            // Weights in the new cluster are unchanged because we want to
            // ignore these nodes within the cluster.
#endif
          }
        }
      }
    }
    LOG_S(INFO) << "ClusterLineNodes() count_line_nodes:                "
                << count_line_nodes;
    LOG_S(INFO) << "ClusterLineNodes() count_connected_circle_nodes:    "
                << count_connected_circle_nodes;
    LOG_S(INFO) << "ClusterLineNodes() count_isolated_circle_nodes: "
                << count_isolated_circle_nodes;
    LOG_S(INFO) << "ClusterLineNodes() empty clusters: " << lg->empty_clusters_;
  }
#endif
};

// Clusters the nodes in precluster_nodes, one cluster for each connected set.
void PreclusterNodes(HugeBitset* precluster_nodes, LouvainGraph* lg) {
  std::vector<std::uint32_t> queue;
  for (size_t start_pos = 0; start_pos < lg->nodes.size(); ++start_pos) {
    if (!precluster_nodes->GetBit(start_pos)) {
      continue;
    }

    // All connected nodes that should be preclustered are moved the cluster
    // of 'start'.
    CHECK_S(queue.empty());
    queue.push_back(start_pos);
    precluster_nodes->RemoveBit(start_pos);
    const LouvainNode& start = lg->nodes.at(start_pos);

    while (!queue.empty()) {
      const uint32_t idx = queue.back();
      queue.pop_back();
      LouvainNode& n = lg->nodes.at(idx);
      if (n.cluster_pos != start.cluster_pos) {
        lg->MoveToNewCluster(idx, start_pos);
      }
      // Visit the connected nodes.
      for (uint32_t ep = n.edge_start; ep < n.edge_start + n.num_edges; ++ep) {
        const LouvainEdge& e = lg->edges.at(ep);
        if (precluster_nodes->GetBit(e.other_node_pos)) {
          queue.push_back(e.other_node_pos);
          precluster_nodes->RemoveBit(e.other_node_pos);
        }
      }
    }
  }
  CHECK_EQ_S(precluster_nodes->CountBits(), 0);
}

}  // namespace louvain
