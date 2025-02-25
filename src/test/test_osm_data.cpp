#include <osmpbf/osmpbf.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include <memory>

#include "base/argli.h"
#include "base/util.h"
#include "graph/build_graph.h"
#include "graph/graph_def.h"
#include "osm/osm_helpers.h"
#include "test/test_utils.h"

void TestTRSimple() {
  FUNC_TIMER();
  OsmWrapper w;
  OSMPBF::Relation rel;

  w.AddKeyVal("type", "restriction", &rel);
  w.AddMember("from", "way", 10, &rel);
  w.AddMember("to", "way", 11, &rel);
  w.AddMember("via", "node", 1, &rel);

  CHECK_EQ_S(w.tagh.GetValue(rel, "type"), "restriction");

  LOG_S(INFO) << rel.DebugString();
}

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  if (argc != 1) {
    ABORT_S() << absl::StrFormat("usage: %s", argv[0]);
  }

  TestTRSimple();

  LOG_S(INFO)
      << "\n\033[1;32m*****************************\nTesting successfully "
         "finished\n*****************************\033[0m";
  return 0;
}
