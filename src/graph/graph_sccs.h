#pragma once

#include "absl/strings/str_format.h"
#include "algos/tarjan_scc.h"
#include "base/util.h"
#include "graph/graph_def.h"
#include "graph/graph_def_utils.h"

namespace {
// ThinGraph for the edge graph in component 'comp.
struct FullGraphThinGraph : ThinGraph {
 private:
  const Graph& g_;
  // Map from ThinGraph-node-index to Graph-edge-index.
  std::vector<uint32_t> tarjan_nodes_;
  // Inverse mapping from above.
  absl::flat_hash_map<uint32_t, uint32_t> gidx_to_tarjan_;

 public:
  FullGraphThinGraph(const Graph& g, const Graph::Component& comp) : g_(g) {
    // Run through all nodes in component and fill mapping data.
    for (uint32_t gnode_idx : comp.nodes_sorted) {
      for (const GEdge& e : gnode_forward_edges(g_, gnode_idx)) {
        if (!e.unique_target || e.target_idx == gnode_idx) {
          continue;
        }
        uint32_t gedge_idx = gnode_edge_idx(g_, e);
        gidx_to_tarjan_[gedge_idx] = tarjan_nodes_.size();
        tarjan_nodes_.push_back(gedge_idx);
      }
    }

    // Add data to the thin graph.
    for (uint32_t t_idx = 0; t_idx < tarjan_nodes_.size(); ++t_idx) {
      uint32_t gedge_idx1 = tarjan_nodes_.at(t_idx);
      uint32_t t_idx1 = FindInMapOrFail(gidx_to_tarjan_, gedge_idx1);
      const GEdge& e1 = g_.edges.at(gedge_idx1);
      const GNode& target = g_.nodes.at(e1.target_idx);

      // Now iterate the forward edges at the target node and check turn costs
      // if the can be accessed.
      const TurnCostData& turn_costs = g.turn_costs.at(e1.turn_cost_idx);
      CHECK_EQ_S(turn_costs.turn_costs.size(), target.num_forward_edges);
      for (uint32_t off = 0; off < target.num_forward_edges; ++off) {
        uint32_t gedge_idx2 = target.edges_start_pos + off;
        const GEdge& e2 = g_.edges.at(gedge_idx2);
        if (!e2.unique_target || e2.target_idx == e1.target_idx) {
          continue;
        }
        // TODO: determine allowed turns, this way all turns are enabled.
        if (turn_costs.turn_costs.at(off) != TURN_COST_INFINITY_COMPRESSED) {
          uint32_t t_idx2 = FindInMapOrFail(gidx_to_tarjan_, gedge_idx2);
          AddEdge(t_idx1, t_idx2, /*verbose=*/false);
        }
      }
    }
    AddSentinel();

    LOG_S(INFO) << absl::StrFormat("Created Thingraph #nodes:%lu #edges:%lu",
                                   starts.size() - 1, targets.size());
    CHECK_EQ_S(starts.size(), tarjan_nodes_.size() + 1u);
  }

  std::string GEdgeDebugStr(uint32_t gedge_idx) const {
    return debug_str(g_, g_.edges.at(gedge_idx),
                     g_.FindStartIdxByEdgeIdxSlowish(gedge_idx));
  }

  virtual std::string DebugStr(uint32_t tarjan_node_idx) const {
    return GEdgeDebugStr(tarjan_nodes_.at(tarjan_node_idx));
  }

  uint32_t tarjan_node_to_gedge_idx(uint32_t tarjan_node_idx) const {
    return tarjan_nodes_.at(tarjan_node_idx);
  }
};

}  // namespace

// TODO: Experimental, doesn't do anything reasonable yet.
//
// Determine the strongly connected components in the cluster level graph. Every
// incoming/outgoing edge in the cluster graph is assigned a unique component
// number (scc_no) in this process.
void ComputeFullGraphSCCs(const Graph& g,
                          std::vector<bool>* edge_to_isolated_scc) {
  FUNC_TIMER();

  // 'edge_colors' assigns a color to each edge in the graph.
  // 0: edge wasn't assigned to an SCC.
  // 1: edge was in the largest SCC.
  // 2+: edge was not in the largest SCC.
  CHECK_S(edge_to_isolated_scc->empty());
  edge_to_isolated_scc->assign(g.edges.size(), false);

  for (const auto& comp : g.large_components) {
    FullGraphThinGraph tg(g, comp);
    TarjanSCC tarjan(tg);
    tarjan.DFS(TarjanSCC::Iterative);
    const std::vector<TarjanSCC::SCC>& sccs = tarjan.GetSCCs();

    size_t max_size = 0;
    for (const TarjanSCC::SCC& scc : sccs) {
      max_size = std::max(max_size, scc.nodes.size());
    }

    for (const TarjanSCC::SCC& scc : sccs) {
      bool isolated = (scc.nodes.size() == max_size) ? false : true;
      for (uint32_t k : scc.nodes) {
        // k is the internal node idx of TarjanSCC, so convert it.
        edge_to_isolated_scc->at(tg.tarjan_node_to_gedge_idx(k)) = isolated;
      }
    }
  }
}
