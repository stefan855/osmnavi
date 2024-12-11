#pragma once

#include <deque>
#include <queue>
#include <vector>

#include "base/util.h"
#include "graph/graph_def.h"

// Find large components in undirected graph.
class ComponentAnalyzer {
 public:
  ComponentAnalyzer(const Graph& g) : g_(g) {}

  std::vector<Graph::Component> FindLargeComponents() {
    FUNC_TIMER();
    label_.resize(g_.nodes.size(), INFU32);
    std::map<uint32_t, uint32_t> comp_size_to_count;
    std::map<uint32_t, uint32_t> comp_size_to_start_node;
    std::vector<Graph::Component> comps;

    uint32_t num_components = 0;
    for (std::uint32_t n = 0; n < label_.size(); ++n) {
      if (label_.at(n) == INFU32) {
        num_components++;
        uint32_t size = LabelComponent(n);
        if (size >= 0) {
          comps.push_back({.start_node = n, .size = size});
        }
        comp_size_to_count[size] += 1;
        comp_size_to_start_node[size] = n;
      }
    }

    // Sort and then keep first and large components only.
    std::sort(comps.begin(), comps.end(),
              [](const Graph::Component& a, const Graph::Component& b) {
                return a.size > b.size;
              });
    for (size_t pos = 1; pos < comps.size(); ++pos) {
      if (comps.at(pos).size < Graph::kLargeComponentMinSize) {
        comps.erase(comps.begin() + pos, comps.end());
        break;
      }
    }

    for (const auto& comp : comps) {
      LOG_S(INFO) << absl::StrFormat(
          "Keep component start:%u size:%u (%.2f%% of %u)", comp.start_node,
          comp.size, (100.0 * comp.size) / g_.nodes.size(), g_.nodes.size());
    }
    return comps;
  }

  static void MarkLargeComponents(Graph* g) {
    FUNC_TIMER();
    CHECK_S(!g->large_components.empty());
    std::deque<uint32_t> nodes;
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
        GNode& n = g->nodes.at(nodes.front());
        nodes.pop_front();
        for (size_t edge_pos = 0; edge_pos < gnode_total_edges(n); ++edge_pos) {
          GEdge& e = n.edges[edge_pos];
          if (!e.unique_other) continue;
          GNode& other = g->nodes.at(e.other_node_idx);
          if (other.large_component) continue;
          other.large_component = 1;
          nodes.push_back(e.other_node_idx);
        }
      }
    }
  }

 private:
  uint32_t LabelComponent(std::uint32_t start_idx) {
    std::vector<std::uint32_t> queue;
    queue.push_back(start_idx);
    label_.at(start_idx) = start_idx;
    uint32_t count = 0;

    while (!queue.empty()) {
      const uint32_t node_idx = queue.back();
      queue.pop_back();
      count++;
      const GNode& n = g_.nodes.at(node_idx);
      for (uint32_t pos = 0; pos < gnode_total_edges(n); ++pos) {
        const uint32_t other_idx = n.edges[pos].other_node_idx;
        if (n.edges[pos].unique_other && label_.at(other_idx) == INFU32) {
          queue.push_back(other_idx);
          label_.at(other_idx) = start_idx;
        }
      }
    }
    return count;
  }

  const Graph& g_;
  std::vector<std::uint32_t> label_;
};
