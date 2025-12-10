#pragma once

#include <deque>
#include <queue>
#include <vector>

#include "base/util.h"
#include "graph/graph_def.h"

namespace components {

namespace {
// Label all nodes in component reachable from node 'start_idx'. Visited nodes
// are set to true in 'visited' and comp->nodes.
void LabelComponent(const Graph& g, uint32_t start_idx, HugeBitset* visited,
                    Graph::Component* comp) {
  CHECK_S(!visited->GetBit(start_idx));
  CHECK_S(comp->nodes.empty());
  visited->AddBit(start_idx);
  comp->nodes.push_back(start_idx);

  size_t pos = 0;
  while (pos < comp->nodes.size()) {
    const uint32_t node_idx = comp->nodes.at(pos);
    pos += 1;
    for (const GEdge& e : gnode_all_edges(g, node_idx)) {
      if (e.unique_target && !visited->GetBit(e.target_idx)) {
        comp->nodes.push_back(e.target_idx);
        visited->AddBit(e.target_idx);
      }
    }
  }
}
void PrintStats(const Graph& g, const std::vector<Graph::Component>& comps,
                size_t min_size) {
  LOG_S(INFO) << absl::StrFormat(
      "%llu components, large  components (>= %llu nodes) are kept",
      comps.size(), min_size);

  size_t pos = 0;
  size_t cum_comp_size = 0;
  for (const auto& comp : comps) {
    const size_t size = comp.nodes.size();
    cum_comp_size += size;
    LOG_S(INFO) << absl::StrFormat(
        "Component %5llu start:%lld size:%u (%.2f%% of %u, cum:%.2f%%)", pos++,
        GetGNodeIdSafe(g, comp.nodes.front()), size,
        (100.0 * size) / g.nodes.size(), g.nodes.size(),
        (100.0 * cum_comp_size) / g.nodes.size());
  }
}
}  // namespace

inline std::vector<Graph::Component> FindComponents(const Graph& g,
                                                    size_t min_size) {
  FUNC_TIMER();
  HugeBitset visited;
  std::vector<Graph::Component> comps;

  for (std::uint32_t node_idx = 0; node_idx < g.nodes.size(); ++node_idx) {
    if (!visited.GetBit(node_idx)) {
      comps.push_back({});
      Graph::Component* comp = &(comps.back());
      LabelComponent(g, node_idx, &visited, comp);
      CHECK_GT_S(comp->nodes.size(), 0);
      CHECK_EQ_S(comp->nodes.size(), comp->nodes.size());
    }
  }

  // Sort and then keep first and large components only.
  std::sort(comps.begin(), comps.end(),
            [](const Graph::Component& a, const Graph::Component& b) {
              return a.nodes.size() > b.nodes.size();
            });

  PrintStats(g, comps, min_size);

  // Remove small components from result.
  for (size_t pos = 1; pos < comps.size(); ++pos) {
    if (comps.at(pos).nodes.size() < min_size) {
      comps.erase(comps.begin() + pos, comps.end());
      break;
    }
  }

  return comps;
}

inline void MarkLargeComponents(const std::vector<Graph::Component>& comps,
                                Graph* g) {
  FUNC_TIMER();
  for (const Graph::Component& comp : comps) {
    for (uint32_t node_idx : comp.nodes) {
      CHECK_S(!g->nodes.at(node_idx).large_component);
      g->nodes.at(node_idx).large_component = 1;
    }
  }
}
}  // namespace components
