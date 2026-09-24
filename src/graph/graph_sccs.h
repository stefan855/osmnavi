#pragma once

#include "absl/strings/str_format.h"
#include "algos/tarjan_scc.h"
#include "base/util.h"
#include "graph/graph_def.h"
#include "graph/graph_def_utils.h"

namespace {

// Create a graph at cluster level to be used as input to Tarjan SCC. Because
// we have turn restrictions, we need to make the edges of the cluster graph
// the "primary" objects, i.e. the nodes, for Tarjan SCC. Edges in Tarjan SCC
// are actually edge-to-edge paths in the cluster graph.
//
// Incoming cluster edges are interpreted as "nodes" for Tarjan. Every
// outgoing edge is converted to the corresponding incoming edge, so all nodes
// are incoming edges.
//
// Each valid path through a cluster consists of incoming edge -> outgoing
// edge. This is converted to incoming-to-incoming-edge and interpreted as
// "edge" for Tarjan SCC.
struct MyThinGraph : ThinGraph {
  MyThinGraph(const Graph& g) : g_(g) {
    // Build the vector of "nodes" for tarjan SCC, in our case incoming edges
    // of the clusters.
    uint32_t num = 0;
    for (const GCluster& c : g_.clusters) {
      num += c.border_in_edges.size();
    }

    idx_to_edge_key_.reserve(num);
    for (const GCluster& c : g_.clusters) {
      for (const GCluster::EdgeDescriptor& d : c.border_in_edges) {
        uint32_t edge_key = create_edge_key(c.cluster_id, d.pos);
        edge_key_to_idx_[edge_key] = idx_to_edge_key_.size();
        idx_to_edge_key_.push_back(edge_key);
      }
    }
    LOG_S(INFO) << "Created Thingraph nodes:" << idx_to_edge_key_.size();
    LOG_S(INFO) << "Created Thingraph map:" << edge_key_to_idx_.size();

    // =======================================================================

    // Check outgoing edge reachability within clusters.
    for (const GCluster& c : g_.clusters) {
      for (const GCluster::EdgeDescriptor& out : c.border_out_edges) {
        // Check if it can be reached from any incoming edge.
        uint32_t cnt = 0;
        for (uint32_t in_pos = 0; in_pos < c.border_in_edges.size(); ++in_pos) {
          cnt += c.edge_distances.at(in_pos).at(out.pos) != INFU32;
        }
        if (cnt < 2) {
          const uint32_t to_node_idx = g_.edges.at(out.g_edge_idx).target_idx;
          const GCluster& target_c =
              g_.clusters.at(g_.nodes.at(to_node_idx).cluster_id);
          const uint32_t converted_in_pos =
              target_c.FindIncomingEdgePos(out.g_edge_idx);

          LOG_S(INFO) << absl::StrFormat(
              "Outgoing edge (%u,%u) (as in:(%u,%u)) reachability:%u",
              c.cluster_id, out.pos, target_c.cluster_id, converted_in_pos,
              cnt);
        }
      }
    }

    // =======================================================================

    // We have the nodes. Now add the edges.
    uint32_t count = 0;
    for (const GCluster& c : g_.clusters) {
      for (const GCluster::EdgeDescriptor& in : c.border_in_edges) {
        const std::vector<std::uint32_t>& dist = c.GetEdgeOutDistances(in.pos);
        for (const GCluster::EdgeDescriptor& out : c.border_out_edges) {
          if (dist.at(out.pos) != INFU32) {
            // Find the to-cluster of the outgoing edge.
            const uint32_t to_node_idx = g_.edges.at(out.g_edge_idx).target_idx;
            const GCluster& target_c =
                g_.clusters.at(g_.nodes.at(to_node_idx).cluster_id);
            const uint32_t converted_in_pos =
                target_c.FindIncomingEdgePos(out.g_edge_idx);
            CHECK_LT_S(converted_in_pos, target_c.border_in_edges.size());
            // Add this edge from the in edge in the current cluster to the
            // target in_edge in the target cluster.
            AddEdge(get_tarjan_node_idx(create_edge_key(c.cluster_id, in.pos)),
                    get_tarjan_node_idx(create_edge_key(target_c.cluster_id,
                                                        converted_in_pos)));
            /*
            LOG_S(INFO) << absl::StrFormat(
                "Add edge #%u (%u,%u) -> (%u,%u)", count, c.cluster_id, in.pos,
                target_c.cluster_id, converted_in_pos);
            */
            LOG_S(INFO) << absl::StrFormat(
                "Add edge #%u |%s| ---> |%s|", count,
                EdgeKeyDebugStr(create_edge_key(c.cluster_id, in.pos)),
                EdgeKeyDebugStr(
                    create_edge_key(target_c.cluster_id, converted_in_pos)));
            count++;
          }
        }
      }
    }
    LOG_S(INFO) << "Created Thingraph edges:" << count;

    AddSentinel();
  }

  std::string EdgeKeyDebugStr(uint32_t edge_key) const {
    const GCluster c = g_.clusters.at(cluster_id_from_edge_key(edge_key));
    const uint32_t pos = edge_pos_from_edge_key(edge_key);
    const GCluster::EdgeDescriptor& ed = c.border_in_edges.at(pos);
    const GEdge& e = g_.edges.at(ed.g_edge_idx);
    return absl::StrFormat("key(%u,%u) %lu->%ld way:%ld", c.cluster_id, pos,
                           GetGNodeIdSafe(g_, ed.g_from_idx),
                           GetGNodeIdSafe(g_, e.target_idx),
                           GetGWayIdSafe(g_, e.way_idx));
  }

  virtual std::string DebugStr(uint32_t tarjan_node_idx) const {
    return EdgeKeyDebugStr(idx_to_edge_key_.at(tarjan_node_idx));
    /*
    const uint32_t edge_key = idx_to_edge_key_.at(tarjan_node_idx);
    const GCluster c = g_.clusters.at(cluster_id_from_edge_key(edge_key));
    const uint32_t pos = edge_pos_from_edge_key(edge_key);
    const GCluster::EdgeDescriptor& ed = c.border_in_edges.at(pos);
    const GEdge& e = g_.edges.at(ed.g_edge_idx);
    return absl::StrFormat("key(%u,%u) %lu->%ld way:%ld", c.cluster_id, pos,
                           GetGNodeIdSafe(g_, ed.g_from_idx),
                           GetGNodeIdSafe(g_, e.target_idx),
                           GetGWayIdSafe(g_, e.way_idx));
                           */
  }

  uint32_t get_tarjan_node_idx(uint32_t edge_key) {
    return FindInMapOrFail(edge_key_to_idx_, edge_key);
  }

 private:
  const Graph& g_;
  std::vector<uint32_t> idx_to_edge_key_;
  absl::flat_hash_map<uint32_t, uint32_t> edge_key_to_idx_;

  static constexpr uint32_t CLUSTER_ID_SHIFT = 32 - NUM_CLUSTER_BITS;
  static_assert(NUM_CLUSTER_BITS < 32 && CLUSTER_ID_SHIFT >= 10);

  // Encode a cluster id and the position of an in-/outgoing edge of the
  // cluster into a 32bit value.
  static inline uint32_t create_edge_key(uint32_t cluster_id,
                                         uint32_t edge_pos) {
    CHECK_LT_S(edge_pos, 1u << CLUSTER_ID_SHIFT);
    return (cluster_id << CLUSTER_ID_SHIFT) + edge_pos;
  }
  static inline uint32_t cluster_id_from_edge_key(uint32_t edge_key) {
    return edge_key >> CLUSTER_ID_SHIFT;
  }
  static inline uint32_t edge_pos_from_edge_key(uint32_t edge_key) {
    return edge_key & ((1u << CLUSTER_ID_SHIFT) - 1);
  }
};

}  // namespace

// TODO: Experimental, doesn't do anything reasonable yet.
//
// Determine the strongly connected components in the cluster level graph. Every
// incoming/outgoing edge in the cluster graph is assigned a unique component
// number (scc_no) in this process.
void ComputeClusterGraphSCCs(const Graph& g) {
  FUNC_TIMER();

  MyThinGraph tg(g);
  TarjanSCC tarjan(tg);
  tarjan.DFS(TarjanSCC::Iterative);

  // CHECK_S(0);
}
