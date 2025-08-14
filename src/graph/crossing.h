#pragma once

#include "base/util.h"
#include "graph/graph_def.h"
#include "logging/loguru.h"

void DeterminePriorityRoads(Graph* g) {
  FUNC_TIMER();
  for (const NodeAttribute& na : graph->node_attrs_sorted) {
    if (meta->way_nodes_seen->GetBit(na.node_id)) {
      meta->way_nodes_needed->SetBit(na.node_id, true);
    }
  }
}
