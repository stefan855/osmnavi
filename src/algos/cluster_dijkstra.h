// Compute all shortest paths between border nodes in a cluster.

#pragma once

#include <fstream>
#include <queue>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "algos/routing_metric.h"
#include "base/util.h"
#include "graph/graph_def.h"

namespace {

// This is a compacted view on the nodes and edges of a cluster, with
// precomputed distance metric for every edge.
// The nodes in the mini graph have ids 0..num_nodes()-1.
class ClusterMiniGraph {
 public:
  struct MiniEdge {
    uint32_t to_mini_idx;
    int32_t metric;
  };

  ClusterMiniGraph(const Graph& g, const RoutingMetric& metric,
                   const GCluster& cluster) {
    BuildMiniGraph(g, metric, cluster);
  }

  // Return the number of nodes in the mini graph. Ids are 0..num_nodes()-1.
  // The ids 0..cluster.border_nodes.size()-1 represent the border nodes.
  uint32_t num_nodes() const { return num_nodes_; }

  // For every node, contains the start position of its edges in the edges()
  // vector. The last element does not correspond to a node, it has the value
  // edges().size().
  const std::vector<uint32_t>& edges_start() const { return edges_start_; }

  // Sorted edge vector contains all edges of all nodes. The edges of node k are
  // in positions edges_start()[k]..edges_start()[k+1] - 1.
  const std::vector<MiniEdge>& edges() const { return edges_; }

 private:
  struct TmpEdge {
    uint32_t from_mini_idx;
    uint32_t to_mini_idx;
    int32_t metric;
  };

  static void SortAndCleanupEdges(std::vector<TmpEdge>* tmp_edges) {
    std::sort(tmp_edges->begin(), tmp_edges->end(),
              [](const TmpEdge& a, const TmpEdge& b) {
                if (a.from_mini_idx != b.from_mini_idx) {
                  return a.from_mini_idx < b.from_mini_idx;
                }
                if (a.to_mini_idx != b.to_mini_idx) {
                  return a.to_mini_idx < b.to_mini_idx;
                }
                return a.metric < b.metric;
              });
    // Remove dups.
    auto last = std::unique(tmp_edges->begin(), tmp_edges->end(),
                            [](const TmpEdge& a, const TmpEdge& b) {
                              return a.to_mini_idx == b.to_mini_idx &&
                                     a.from_mini_idx == b.from_mini_idx;
                            });
    if (last != tmp_edges->end()) {
      LOG_S(INFO) << "Removing dups from edges array: "
                  << tmp_edges->end() - last;
      tmp_edges->erase(last, tmp_edges->end());
    }
  }

  // Visit all nodes in a cluster using a BFS, starting from the border nodes.
  // Assigns a serial id=0..N-1 to each node. The border nodes of the cluster
  // get ids 0..cluster.border_nodes.size()-1.
  // Returns the number of nodes in the cluster (=N) and the edges between
  // cluster nodes, each with the distance value computed by 'metric'.
  static void ClusterBFS(const Graph& g, const RoutingMetric& metric,
                         const GCluster& cluster, uint32_t* num_nodes,
                         std::vector<TmpEdge>* tmp_edges) {
    // Map from node index in g.nodes to the node index in the mini graph.
    absl::flat_hash_map<uint32_t, uint32_t> nodemap;
    // FIFO queue for bfs, containing node indices in g.nodes.
    std::queue<uint32_t> q;

    for (uint32_t pos = 0; pos < cluster.border_nodes.size(); ++pos) {
      uint32_t node_idx = cluster.border_nodes.at(pos);
      nodemap[node_idx] = pos;
      q.push(node_idx);
    }

    // Do a bfs, limited to the nodes belonging to the cluster.
    uint32_t check_mini_idx = 0;
    while (!q.empty()) {
      uint32_t node_idx = q.front();
      q.pop();
      const GNode& node = g.nodes.at(node_idx);
      CHECK_EQ_S(node.cluster_id, cluster.cluster_id);
      // By construction, this number is strictly increasing, i.e. +1 in every
      // loop.
      uint32_t mini_idx = nodemap[node_idx];
      CHECK_EQ_S(mini_idx, check_mini_idx);
      check_mini_idx++;

      // Examine neighbours.
      for (size_t i = 0; i < node.num_edges_out; ++i) {
        const GEdge& edge = node.edges[i];
        if (edge.bridge || edge.other_node_idx == node_idx) {
          continue;
        }
        const GNode& other = g.nodes.at(edge.other_node_idx);
        if (other.cluster_id != cluster.cluster_id) {
          // ignore node outside cluster.
          continue;
        }

        // The other node is part of the same cluster, so use it.
        uint32_t other_mini_idx;
        auto iter = nodemap.find(edge.other_node_idx);
        if (iter == nodemap.end()) {
          other_mini_idx = nodemap.size();
          nodemap[edge.other_node_idx] = nodemap.size();
          q.push(edge.other_node_idx);
        } else {
          other_mini_idx = iter->second;
        }

        tmp_edges->push_back({mini_idx, other_mini_idx,
                              metric.Compute(g.ways.at(edge.way_idx), edge)});
      }
    }
    // TODO: nodemap.size() is sometimes smaller than cluster.num_nodes.
    // Investigate why this happens. Maybe some nodes in the cluster are not
    // reachable when using the directed graph - clustering is done on the
    // undirected graph. CHECK_EQ_S(nodemap.size(), cluster.num_nodes);
    *num_nodes = nodemap.size();
  }

  void BuildMiniGraph(const Graph& g, const RoutingMetric& metric,
                      const GCluster& cluster) {
    num_nodes_ = 0;
    std::vector<TmpEdge> tmp_edges;
    ClusterBFS(g, metric, cluster, &num_nodes_, &tmp_edges);
    SortAndCleanupEdges(&tmp_edges);

    // Nodes are numbered [0..num_nodes). For each one, we want to store
    // the start of its edges in the edge array.
    // In the end we add one more element with value tmp_edges.size(), to allow
    // easy iteration.
    size_t current_start = 0;
    for (size_t mini_idx = 0; mini_idx < num_nodes_; ++mini_idx) {
      edges_start_.push_back(current_start);
      // LOG_S(INFO) << absl::StrFormat("Cluster:%u node:%u edge_start:%u",
      //                                cluster.cluster_id, mini_idx,
      //                                current_start);
      while (current_start < tmp_edges.size() &&
             tmp_edges.at(current_start).from_mini_idx == mini_idx) {
        current_start++;
      }
    }
    CHECK_EQ_S(current_start, tmp_edges.size());
    edges_start_.push_back(tmp_edges.size());

    edges_.reserve(tmp_edges.size());
    for (const TmpEdge& e : tmp_edges) {
      // LOG_S(INFO) << absl::StrFormat("Cluster:%u pos:%u from:%u to:%u w:%d",
      //                                cluster.cluster_id, edges_.size(),
      //                                e.from_mini_idx, e.to_mini_idx,
      //                                e.metric);
      edges_.push_back({e.to_mini_idx, e.metric});
    }
    LOG_S(INFO) << absl::StrFormat("Cluster:%u #nodes:%u #edges:%u mem:%u",
                                   cluster.cluster_id, edges_start_.size() - 1,
                                   edges_.size(),
                                   edges_start_.size() * sizeof(uint32_t) +
                                       edges_.size() * sizeof(MiniEdge));
    CHECK_EQ_S(num_nodes_ + 1, edges_start_.size());
  }

  uint32_t num_nodes_;
  std::vector<uint32_t> edges_start_;
  std::vector<MiniEdge> edges_;
};

}  // namespace

// Compute all shortest paths between border nodes in a cluster. The results
// are stored in 'cluster'.
void ComputeShortestClusterPaths(const Graph& g, const RoutingMetric& metric,
                                 GCluster* cluster) {
  ClusterMiniGraph mg(g, metric, *cluster);
}

#if 0
class DijkstraRouter {
 public:
  // This exists once per 'node_idx'.
  struct VisitedNode {
    std::uint32_t node_idx;            // Index into global node vector.
    std::uint32_t from_v_idx;          // index into visited_nodes vector.
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

  DijkstraRouter(const Graph& g)
      : g_(g), pq_(&MetricCmp), target_visited_node_index_(INF) {}

  bool Route(std::uint32_t start_idx, std::uint32_t target_idx,
             const RoutingMetric& metric, bool avoid_dead_end = true) {
    LOG_S(INFO) << "Start routing from " << start_idx << " to " << target_idx
                << " (Dijkstra, " << metric.Name() << ")";
    std::uint32_t start_v_idx = FindOrAddVisitedNode(start_idx);
    CHECK_EQ_S(start_v_idx, 0);
    CHECK_EQ_S(visited_nodes_.at(0).from_v_idx, INF);

    visited_nodes_.front().min_metric = 0;

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
        LOG_S(INFO) << absl::StrFormat(
            "Route found, visited nodes:%u metric:%u", visited_nodes_.size(),
            vnode.min_metric);

        // Mark nodes on shortest route.
        auto current_idx = target_visited_node_index_;
        while (current_idx != INF) {
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
        // Even if we want to avoid dead ends, still allow to leave a dead end
        // but not entering one. This way, the start node can reside in a dead
        // end and routing still works. To achieve this behavior, we allow to
        // pass a bridge only when leaving a dead end.
        if (avoid_dead_end && edge.bridge && !node.dead_end) {
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
    LOG_S(INFO) << "Route not found";
    return false;
  }

  void SaveSpanningTreeSegments(const std::string& filename) {
    LOG_S(INFO) << "Write route to " << filename;
    std::ofstream myfile;
    myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);
    for (const VisitedNode& n : visited_nodes_) {
      if (n.done && n.from_v_idx != INF) {
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

  const Graph& g_;
  std::vector<VisitedNode> visited_nodes_;
  absl::flat_hash_map<uint32_t, uint32_t> node_to_vnode_idx_;
  std::priority_queue<QueuedNode, std::vector<QueuedNode>, decltype(&MetricCmp)>
      pq_;
  std::uint32_t target_visited_node_index_;
};
#endif
