#pragma once

/*
 * When forcing clusters into borders (such as country borders), it
 * happens that besides the normal large clusters we also get a large number of
 * tiny clusters with few nodes only. One way this happens is when a street
 * follows the border line and crosses it multiple times in both directions.
 * This will create tiny clusters that are "across" the border but are not
 * connected to anything on the other side.
 *
 * We want to get rid of these tiny clusters. Why?
 *
 * Besides increasing the number of clusters enormously, these tiny clusters
 * also significantly increase the number of border nodes. Both things cause
 * significant overhead when computing shortest routes within clusters and
 * across clusters.
 *
 * This file implements an algorithm that merges the tiny clusters with
 * the best large clusters they are connected to. This will cause some clusters
 * to contain a few nodes from the other side of the border, i.e. it violates
 * the condition to not cross border within a cluster. There is not other way
 * out, so we have to live with it.
 *
 * The main idea of the algorithm is to merge tiny clusters into connected large
 * clusters one by one. The merging is prioritized by the country code of the
 * target large cluster. This way, all tiny clusters connected to several
 * countries consistently go to the country with the smallest country
 * code (be it A or B). The merging algorithm maintains a vector of ClusterData
 * with updated information for each cluster, especially if a cluster has been
 * deleted and merged.
 *
 * Note: The current implementation supports only country border based
 * clustering, but it easily can be extended to other types of borders.
 */

#include <queue>
#include <unordered_map>
#include <vector>

#include "absl/container/btree_map.h"
#include "absl/strings/str_format.h"
#include "base/constants.h"
#include "base/huge_bitset.h"
#include "logging/loguru.h"

namespace build_clusters {

constexpr uint32_t kTinyClusterThreshold = 50;

// Border edge of a tiny cluster.
struct CrossEdge {
  const GNode* large;  // node in large cluster.
  const GNode* tiny;   // node in tiny cluster.
};

struct ClusterData {
  // True iff the cluster is "tiny". This means it has less than
  // kTinyClusterThreshold nodes and all external edges from border nodes have
  // edge.cross_country set to true.
  bool tiny = false;
  // When a tiny cluster is merged, then 'deleted' is set to true and the new
  // cluster id is stored in 'replace_id' below.
  bool deleted = false;
  // When a tiny cluster has been deleted, then 'replace_id' contains the new
  // cluster_id of the nodes in the original tiny cluster.
  uint32_t replace_id = INFU32;
};

struct WorkData {
  struct CompareCrossEdge {
    // The element with the smallest (large->ncc, large->node_id) tuple is
    // first. node_id is used to make the algorithm deterministic.
    bool operator()(const CrossEdge& left, const CrossEdge& right) {
      if (left.large->ncc != right.large->ncc) {
        return left.large->ncc > right.large->ncc;
      }
      return left.large->node_id > right.large->node_id;
    }
  };

  const Graph& g;
  // Same size as graph.clusters.
  std::vector<ClusterData> cluster_data;
  // Tiny cluster cross-edges (edges that connect to other clusters) that need
  // to be looked at. Main sort order is by the country code of the large
  // clusters, such that all tiny clusters between country A and B consistently
  // go to the country with the smaller country code. Note that some entries
  // might become invalid when tiny clusters are merged and deleted.
  std::priority_queue<CrossEdge, std::vector<CrossEdge>, CompareCrossEdge>
      work_queue;
};

namespace {
// Return the cluster id, taking into account that the original cluster may have
// been merged.
inline uint32_t GetRedirectedClusterId(const WorkData& wd, const GNode& n) {
  // const ClusterData& cd = wd.cluster_data.at(n.cluster_id);
  const ClusterData& cd = ATR(wd.cluster_data, n.cluster_id);
  return cd.deleted ? cd.replace_id : n.cluster_id;
}

// Return the cluster id, taking into account that the original cluster may have
// been merged.
inline uint32_t GetRedirectedClusterId(const WorkData& wd, uint32_t node_idx) {
  return GetRedirectedClusterId(wd, wd.g.nodes.at(node_idx));
}

void CheckCrossEdges(const WorkData& wd, std::uint32_t cluster_id) {
  const GCluster& c = wd.g.clusters.at(cluster_id);
  for (uint32_t node_idx : c.border_nodes) {
    const GNode& n = wd.g.nodes.at(node_idx);
    uint32_t r_cluster_id = GetRedirectedClusterId(wd, n);
    for (size_t edge_pos = 0; edge_pos < gnode_num_edges(n); ++edge_pos) {
      const GEdge& e = n.edges[edge_pos];
      if (!e.unique_other || e.bridge) {
        continue;
      }
      bool same_cluster =
          GetRedirectedClusterId(wd, e.other_node_idx) == r_cluster_id;
      if (!same_cluster) {
        const GNode& other = wd.g.nodes.at(e.other_node_idx);
        CHECK_S(e.cross_country == 1) << absl::StrFormat(
            "edge from %u to %u should be cross_country (ncc %u vs. %u)",
            n.node_id, other.node_id, n.ncc, other.ncc);
      }
    }
  }
}

bool ComputeIsTinyCluster(const WorkData& wd, std::uint32_t cluster_id) {
  const GCluster& c = wd.g.clusters.at(cluster_id);
  if (c.num_nodes >= kTinyClusterThreshold) {
    return false;
  }
  // Check that all cross-cluster edges are also cross-country.
  // If this check fails it isn't necessarily an error, but with the current
  // clustering we will not get tiny clusters unless they can't grow because of
  // a country border.
  CheckCrossEdges(wd, cluster_id);
  return true;
}

// Add an edge to the work queue, but only if the edge crosses country border
// and connects a large to a tiny cluster.
// Note that edges within a cluster or otherwise not eligible edges are not
// added.
void MayAddEdgeToWorkQueue(const GEdge& e, const GNode& n1, const GNode& n2,
                           WorkData* wd) {
  if (!e.unique_other || e.bridge || n1.node_id == n2.node_id) {
    return;
  }
  const uint32_t cid1 = GetRedirectedClusterId(*wd, n1);
  const uint32_t cid2 = GetRedirectedClusterId(*wd, n2);
  if (cid1 == cid2) {
    // Edge inside cluster, ignore.
    return;
  }
  const ClusterData& cd1 = wd->cluster_data.at(cid1);
  const ClusterData& cd2 = wd->cluster_data.at(cid2);

  if (cd1.tiny != cd2.tiny) {
    // We expect only cross country edges to appear here. It isn't necessarily
    // an error if this is not cross country, but we have to rethink the
    // algorithm.
    CHECK_NE_S(n1.ncc, n2.ncc);
    CHECK_S(e.cross_country) << n1.node_id << ":" << n2.node_id;
    // Add with large cluster's node first.
    if (!cd1.tiny) {
      wd->work_queue.emplace(&n1, &n2);
    } else {
      CHECK_S(!cd2.tiny);
      wd->work_queue.emplace(&n2, &n1);
    }
  }
}

void AddClusterBorderEdges(const GCluster& c, WorkData* wd) {
  for (uint32_t node_idx : c.border_nodes) {
    const GNode& n = wd->g.nodes.at(node_idx);
    for (size_t edge_pos = 0; edge_pos < gnode_num_edges(n); ++edge_pos) {
      const GEdge& e = n.edges[edge_pos];
      MayAddEdgeToWorkQueue(e, n, wd->g.nodes.at(e.other_node_idx), wd);
    }
  }
}

// Merge tiny cluster (if possible) and add new candidate edges to the work
// queue.
void MayMergeCluster(const CrossEdge& ce, WorkData* wd) {
  ClusterData& tiny_cd = wd->cluster_data.at(ce.tiny->cluster_id);
  // Check if ce is still valid.
  if (tiny_cd.deleted) {
    // Already merged.
    return;
  }
  CHECK_EQ_S(tiny_cd.replace_id, INFU32);
  const GCluster& large_cl =
      wd->g.clusters.at(GetRedirectedClusterId(*wd, *ce.large));
  // large clusters are never deleted.
  CHECK_S(!wd->cluster_data.at(large_cl.cluster_id).deleted);

  // Update information in cluster data to reflect the merge and the deletion.
  tiny_cd.deleted = true;
  tiny_cd.replace_id = large_cl.cluster_id;

  // Check connecting edges of the just merged tiny cluster. If they connect to
  // a third cluster which is tiny and not deleted, then add the edge to the
  // work queue.
  for (uint32_t node_idx :
       wd->g.clusters.at(ce.tiny->cluster_id).border_nodes) {
    const GNode& n = wd->g.nodes.at(node_idx);
    // Check that the node was part of tiny and is now in large.
    CHECK_EQ_S(n.cluster_id, ce.tiny->cluster_id);
    CHECK_EQ_S(GetRedirectedClusterId(*wd, n), large_cl.cluster_id);

    // Iterate edges and add eligible ones to work queue.
    for (size_t edge_pos = 0; edge_pos < gnode_num_edges(n); ++edge_pos) {
      const GEdge& e = n.edges[edge_pos];
      if (!e.unique_other || e.bridge) {
        continue;
      }
      const GNode& other = wd->g.nodes.at(e.other_node_idx);
      // if (other.cluster_id == INVALID_CLUSTER_ID) {
      //   continue;
      // }
      // We know that 'n' has been moved to the large cluster.
      // The 'other' node should be in a tiny cluster that is not deleted.
      ClusterData& other_cd = wd->cluster_data.at(other.cluster_id);
      if (other_cd.tiny && !other_cd.deleted) {
        wd->work_queue.emplace(&n, &other);
      }
    }
  }
}

// Remove deleted clusters from graph and adjust all cluster_ids in
// graph->nodes.
void RenumberClusters(const WorkData& wd, Graph* graph) {
  // Maps old to new cluster id. There are two changes to cluster ids.
  // 1) Deleted (merged) clusters are removed from the list of clusters,
  // shifting all subsequent cluster_ids by -1.
  // 2) All nodes in merged clusters get the new id of the merged-into cluster.
  std::vector<uint32_t> old_to_new;
  old_to_new.resize(graph->clusters.size(), INVALID_CLUSTER_ID);
  // Vector with the new clusters, which replace the existing clusters.
  std::vector<GCluster> new_clusters;

  // Create new gclusters. Takes into account deleted clusters and updates
  // the old_to_new-map accordingly.
  for (size_t orig_id = 0; orig_id < wd.cluster_data.size(); ++orig_id) {
    if (!wd.cluster_data.at(orig_id).deleted) {
      uint32_t new_id = new_clusters.size();
      new_clusters.push_back(
          {.ncc = graph->clusters.at(orig_id).ncc, .cluster_id = new_id});
      old_to_new.at(orig_id) = new_id;
    }
  }
  // For deleted clusters, add the mapping to the new cluster.
  for (size_t orig_id = 0; orig_id < wd.cluster_data.size(); ++orig_id) {
    if (wd.cluster_data.at(orig_id).deleted) {
      uint32_t replace_id = wd.cluster_data.at(orig_id).replace_id;
      CHECK_NE_S(old_to_new.at(replace_id), INVALID_CLUSTER_ID) << replace_id;
      old_to_new.at(orig_id) = old_to_new.at(replace_id);
    }
  }
  // No replace all cluster_ids in the graph.
  for (GNode& n : graph->nodes) {
    if (n.cluster_id != INVALID_CLUSTER_ID) {
      n.cluster_id = old_to_new.at(n.cluster_id);
    }
  }
  // Replace the existing, now obsolete clusters, with the new clusters. Note
  // that the caller has to update the cluster information (including border
  // nodes).
  graph->clusters = new_clusters;
}

}  // namespace

void MergeTinyClusters(Graph* graph) {
  FUNC_TIMER();
  WorkData wd = {.g = *graph};
  wd.cluster_data.resize(graph->clusters.size());

  // Mark which clusters are tiny and which not.
  uint32_t num_tiny = 0;
  for (size_t cluster_id = 0; cluster_id < wd.cluster_data.size();
       ++cluster_id) {
    if (ComputeIsTinyCluster(wd, cluster_id)) {
      wd.cluster_data.at(cluster_id).tiny = true;
      ++num_tiny;
    }
    LOG_S(INFO) << absl::StrFormat("Cluster %u tiny:%d nodes:%u", cluster_id,
                                   wd.cluster_data.at(cluster_id).tiny,
                                   graph->clusters.at(cluster_id).num_nodes);
  }
  LOG_S(INFO) << "Tiny clusters: " << num_tiny << " of "
              << wd.cluster_data.size();
  // Fill the work queue with edges that connect large with tiny clusters.
  for (const GCluster& c : graph->clusters) {
    // Only add outgoing edges from tiny clusters, otherwise every edge is
    // added twice.
    if (wd.cluster_data.at(c.cluster_id).tiny) {
      AddClusterBorderEdges(c, &wd);
    }
  }

  int count = 0;
  while (!wd.work_queue.empty()) {
    // Remove the minimal node from the priority queue.
    const CrossEdge ce = wd.work_queue.top();
    wd.work_queue.pop();
    MayMergeCluster(ce, &wd);
    count++;
  }
  LOG_S(INFO) << "Call to MayMergeCluster():" << count;

  RenumberClusters(wd, graph);
}

}  // namespace build_clusters
