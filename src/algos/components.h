#pragma once

#include <deque>
#include <queue>
#include <vector>

#include "base/util.h"
#include "graph/graph_def.h"

// Find large components in undirected graph.
class ComponentAnalyzer {
 public:
  static constexpr uint32_t kLargeComponentMinSize = 2000;

  ComponentAnalyzer(const Graph& g) : g_(g), label_(g.nodes.size(), INFU32) {}

  std::vector<Graph::Component> FindLargeComponents() {
    FUNC_TIMER();
    label_.resize(g_.nodes.size(), INFU32);
    std::vector<Graph::Component> comps;

    uint32_t num_components = 0;
    for (std::uint32_t n = 0; n < label_.size(); ++n) {
      if (label_.at(n) == INFU32) {
        num_components++;
        uint32_t size = LabelComponent(n);
        CHECK_GT_S(size, 0);
        comps.push_back({.start_node = n, .size = size});
      }
    }

    // Sort and then keep first and large components only.
    std::sort(comps.begin(), comps.end(),
              [](const Graph::Component& a, const Graph::Component& b) {
                return a.size > b.size;
              });

    LOG_S(INFO) << absl::StrFormat(
        "%llu components, large  components (>= %u nodes) are kept",
        comps.size(), kLargeComponentMinSize);
    size_t pos = 0;
    size_t cum_comp_size = 0;
    for (const auto& comp : comps) {
      cum_comp_size += comp.size;
      LOG_S(INFO) << absl::StrFormat(
          "Component %5llu start:%lld size:%u (%.2f%% of %u, cum:%.2f%%)",
          pos++, GetGNodeIdSafe(g_, comp.start_node), comp.size,
          (100.0 * comp.size) / g_.nodes.size(), g_.nodes.size(),
          (100.0 * cum_comp_size) / g_.nodes.size());
    }

    for (size_t pos = 1; pos < comps.size(); ++pos) {
      if (comps.at(pos).size < kLargeComponentMinSize) {
        comps.erase(comps.begin() + pos, comps.end());
        break;
      }
    }

    return comps;
  }

  static void MarkLargeComponents(Graph* g) {
    FUNC_TIMER();
    CHECK_S(!g->large_components.empty());
    std::vector<uint32_t> nodes;
    for (const Graph::Component& comp : g->large_components) {
      nodes.clear();
      GNode& start = g->nodes.at(comp.start_node);
      if (start.large_component) {
        CHECK_S(false);
        return;
      }
      start.large_component = 1;
      nodes.push_back(comp.start_node);
      while (!nodes.empty()) {
        // TODO: pop from front? Might be more efficient.
        /*
        GNode& n = g->nodes.at(nodes.front());
        nodes.pop_front();
        */
        const uint32_t node_idx = nodes.back();
        nodes.pop_back();
        for (const GEdge& e : gnode_all_edges(*g, node_idx)) {
          // for (size_t edge_pos = 0; edge_pos < gnode_total_edges(n);
          // ++edge_pos) { GEdge& e = n.edges[edge_pos];
          if (!e.unique_target) continue;
          GNode& other = g->nodes.at(e.target_idx);
          if (other.large_component) continue;
          other.large_component = 1;
          nodes.push_back(e.target_idx);
        }
      }
    }
  }

 private:
  // Label all nodes in component reachable from node 'start_idx'. The label is
  // 'start_idx'.
  uint32_t LabelComponent(std::uint32_t start_idx) {
    std::vector<std::uint32_t> queue;
    queue.push_back(start_idx);
    label_.at(start_idx) = start_idx;
    uint32_t count = 0;

    while (!queue.empty()) {
      const uint32_t node_idx = queue.back();
      queue.pop_back();
      count++;
      for (const GEdge& e : gnode_all_edges(g_, node_idx)) {
        if (e.unique_target && label_.at(e.target_idx) == INFU32) {
          queue.push_back(e.target_idx);
          label_.at(e.target_idx) = start_idx;
        }
      }
    }
    return count;
  }

  const Graph& g_;
  // Same size as g.nodes. All nodes of a component have the same label, which
  // is the lowest node index of any node in the component.
  std::vector<std::uint32_t> label_;
};
