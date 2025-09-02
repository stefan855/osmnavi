#pragma once

#include "base/util.h"
#include "graph/graph_def.h"
#include "logging/loguru.h"

void DeterminePriorityRoads(Graph* g) {
  FUNC_TIMER();
  for (const NodeTags& nt : graph->node_tags_sorted) {
    if (meta->way_nodes_seen->GetBit(nt.node_id)) {
      meta->way_nodes_needed->SetBit(nt.node_id, true);
    }
  }
}
