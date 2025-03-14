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
#include "osm/turn_restriction.h"
#include "test/test_utils.h"

void TestSimpleTurnRestrictionParsing() {
  FUNC_TIMER();
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node indexes.
  enum : uint32_t { Way0 = 0, Way1, Way2, Way3 };
  Graph g = CreateStandardTurnRestrictionGraph(true);
  OsmWrapper w;

  {
    OSMPBF::Relation rel =
        CreateTRRelation(g, &w, /*id=*/1, Way0, D, Way2, "no_right_turn");
    TRResult res;
    ParseTurnRestriction(g, w.tagh, rel, Verbosity::Trace, &res);
    CHECK_EQ_S(res.num_success, 1);
    CHECK_EQ_S(res.num_error, 0);
    CHECK_EQ_S(res.trs.size(), 1);
    const TurnRestriction& tr = res.trs.front();
    CHECK_EQ_S(tr.relation_id, 1);
    CHECK_EQ_S(tr.from_way_id, g.ways.at(Way0).id);
    CHECK_EQ_S(tr.via_ids.size(), 1);
    CHECK_S(tr.via_is_node);
    CHECK_EQ_S(tr.via_ids.at(0), g.nodes.at(D).node_id);
    CHECK_EQ_S(tr.to_way_id, g.ways.at(Way2).id);
    CHECK_S(tr.forbidden);
    CHECK_S(tr.direction == TurnDirection::RightTurn);
    CHECK_EQ_S(tr.node_path.size(), 2);
    CHECK_EQ_S(tr.node_path.front().from_node_idx, B);
    CHECK_EQ_S(tr.node_path.front().way_idx, Way0);
    CHECK_EQ_S(tr.node_path.front().to_node_idx, D);
    CHECK_EQ_S(tr.node_path.back().from_node_idx, D);
    CHECK_EQ_S(tr.node_path.back().to_node_idx, E);
    CHECK_EQ_S(tr.node_path.back().way_idx, Way2);
  }
  {
    OSMPBF::Relation rel =
        CreateTRRelation(g, &w, /*id=*/1, Way0, D, Way2, "only_right_turn");
    TRResult res;
    ParseTurnRestriction(g, w.tagh, rel, Verbosity::Trace, &res);
    CHECK_EQ_S(res.num_success, 1);
    CHECK_EQ_S(res.num_error, 0);
    CHECK_EQ_S(res.trs.size(), 1);
    const TurnRestriction& tr = res.trs.front();
    CHECK_EQ_S(tr.relation_id, 1);
    CHECK_EQ_S(tr.from_way_id, g.ways.at(Way0).id);
    CHECK_EQ_S(tr.via_ids.size(), 1);
    CHECK_S(tr.via_is_node);
    CHECK_EQ_S(tr.via_ids.at(0), g.nodes.at(D).node_id);
    CHECK_EQ_S(tr.to_way_id, g.ways.at(Way2).id);
    CHECK_S(!tr.forbidden);
    CHECK_S(tr.direction == TurnDirection::RightTurn);
    CHECK_EQ_S(tr.node_path.size(), 2);
    /*
        CHECK_EQ_S(tr.from_node_idx, B);
        CHECK_EQ_S(tr.from_way_idx, Way0);
        CHECK_EQ_S(tr.first_via_node_idx, D);
        CHECK_EQ_S(tr.last_via_node_idx, D);
        CHECK_EQ_S(tr.to_node_idx, E);
        CHECK_EQ_S(tr.to_way_idx, Way2);
    */
    CHECK_EQ_S(tr.node_path.front().from_node_idx, B);
    CHECK_EQ_S(tr.node_path.front().way_idx, Way0);
    CHECK_EQ_S(tr.node_path.front().to_node_idx, D);
    CHECK_EQ_S(tr.node_path.back().from_node_idx, D);
    CHECK_EQ_S(tr.node_path.back().to_node_idx, E);
    CHECK_EQ_S(tr.node_path.back().way_idx, Way2);
  }
  {
    OSMPBF::Relation rel =
        CreateTRRelation(g, &w, /*id=*/1, Way0, D, Way2, "no_exit");
    // no_exit can have multiple to-ways.
    w.AddMember("to", "way", g.ways.at(Way3).id, &rel);
    TRResult res;
    ParseTurnRestriction(g, w.tagh, rel, Verbosity::Trace, &res);
    CHECK_EQ_S(res.num_success, 2);
    CHECK_EQ_S(res.num_error, 0);
    CHECK_EQ_S(res.trs.size(), 2);
    const TurnRestriction& tr1 = res.trs.front();
    CHECK_EQ_S(tr1.node_path.size(), 2);
    CHECK_EQ_S(tr1.node_path.back().to_node_idx, E);
    CHECK_EQ_S(tr1.node_path.back().way_idx, Way2);
    CHECK_S(tr1.forbidden);
    CHECK_S(tr1.direction == TurnDirection::NoExit);
    const TurnRestriction& tr2 = res.trs.back();
    CHECK_EQ_S(tr2.node_path.size(), 2);
    CHECK_EQ_S(tr2.node_path.back().to_node_idx, F);
    CHECK_EQ_S(tr2.node_path.back().way_idx, Way3);
    CHECK_S(tr2.forbidden);
    CHECK_S(tr2.direction == TurnDirection::NoExit);
  }
  {
    OSMPBF::Relation rel =
        CreateTRRelation(g, &w, /*id=*/1, Way0, D, Way2, "no_entry");
    // no_exit can have multiple to-ways.
    w.AddMember("from", "way", g.ways.at(Way1).id, &rel);
    TRResult res;
    ParseTurnRestriction(g, w.tagh, rel, Verbosity::Trace, &res);
    CHECK_EQ_S(res.num_success, 2);
    CHECK_EQ_S(res.num_error, 0);
    CHECK_EQ_S(res.trs.size(), 2);
    const TurnRestriction& tr1 = res.trs.front();
    CHECK_EQ_S(tr1.node_path.size(), 2);
    CHECK_EQ_S(tr1.node_path.front().from_node_idx, B);
    CHECK_EQ_S(tr1.node_path.front().way_idx, Way0);
    CHECK_S(tr1.forbidden);
    CHECK_S(tr1.direction == TurnDirection::NoEntry);
    const TurnRestriction& tr2 = res.trs.back();
    CHECK_EQ_S(tr2.node_path.size(), 2);
    CHECK_EQ_S(tr2.node_path.front().from_node_idx, C);
    CHECK_EQ_S(tr2.node_path.front().way_idx, Way1);
    CHECK_S(tr2.forbidden);
    CHECK_S(tr2.direction == TurnDirection::NoEntry);
  }
  {
    OSMPBF::Relation rel =
        CreateTRRelation(g, &w, /*id=*/1, Way0, D, Way2, "no_blabla");
    TRResult res;
    ParseTurnRestriction(g, w.tagh, rel, Verbosity::Trace, &res);
    CHECK_EQ_S(res.num_success, 0);
    CHECK_EQ_S(res.num_error, 1);
    CHECK_EQ_S(res.trs.size(), 0);
  }
}

void TestCondensed() {
  FUNC_TIMER();
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node indexes.
  enum : uint32_t { Way0 = 0, Way1, Way2, Way3 };
  Graph g = CreateStandardTurnRestrictionGraph(true);
  OsmWrapper w;

  {
    OSMPBF::Relation rel =
        CreateTRRelation(g, &w, /*id=*/1, Way0, D, Way2, "no_right_turn");

    TRResult res;
    ParseTurnRestriction(g, w.tagh, rel, Verbosity::Trace, &res);
    CondensedTurnRestrictionMap m =
        ComputeCondensedTurnRestrictions(g, Verbosity::Trace, res.trs);
    CHECK_EQ_S(m.size(), 1);
    const auto& key = m.begin()->first;
    const auto& data = m.begin()->second;
    LOG_S(INFO) << "Condensed TR: "
                << CondensedTurnRestrictionDebugString(g, key, data);
    std::vector<uint64_t> expected = {'C', 'F'};
    CHECK_S(CondensedToNodeIds(g, key, data) == expected);
  }
  {
    OSMPBF::Relation rel =
        CreateTRRelation(g, &w, /*id=*/1, Way0, D, Way2, "only_right_turn");

    TRResult res;
    ParseTurnRestriction(g, w.tagh, rel, Verbosity::Trace, &res);
    CondensedTurnRestrictionMap m =
        ComputeCondensedTurnRestrictions(g, Verbosity::Trace, res.trs);
    CHECK_EQ_S(m.size(), 1);
    const auto& key = m.begin()->first;
    const auto& data = m.begin()->second;
    LOG_S(INFO) << "Condensed TR: "
                << CondensedTurnRestrictionDebugString(g, key, data);
    std::vector<uint64_t> expected = {'E'};
    CHECK_S(CondensedToNodeIds(g, key, data) == expected);
  }
  {
    OSMPBF::Relation rel =
        CreateTRRelation(g, &w, /*id=*/1, Way0, D, Way2, "no_exit");
    w.AddMember("to", "way", g.ways.at(Way3).id, &rel);

    TRResult res;
    ParseTurnRestriction(g, w.tagh, rel, Verbosity::Trace, &res);
    CHECK_EQ_S(res.trs.size(), 2);
    CondensedTurnRestrictionMap m =
        ComputeCondensedTurnRestrictions(g, Verbosity::Trace, res.trs);
    CHECK_EQ_S(m.size(), 1);
    const auto& key = m.begin()->first;
    const auto& data = m.begin()->second;
    LOG_S(INFO) << "Condensed TR: "
                << CondensedTurnRestrictionDebugString(g, key, data);
    std::vector<uint64_t> expected = {'C'};
    CHECK_S(CondensedToNodeIds(g, key, data) == expected);
  }
  {
    OSMPBF::Relation rel =
        CreateTRRelation(g, &w, /*id=*/1, Way0, D, Way2, "no_entry");
    w.AddMember("from", "way", g.ways.at(Way1).id, &rel);

    TRResult res;
    ParseTurnRestriction(g, w.tagh, rel, Verbosity::Trace, &res);
    CHECK_EQ_S(res.trs.size(), 2);
    CondensedTurnRestrictionMap m =
        ComputeCondensedTurnRestrictions(g, Verbosity::Trace, res.trs);
    CHECK_EQ_S(m.size(), 2);
    const CondensedTurnRestrictionKey key1 = {B, Way0, D};
    const CondensedTurnRestrictionKey key2 = {C, Way1, D};
    CHECK_S(m.contains(key1));
    CHECK_S(m.contains(key2));
    const auto& data1 = m[key1];
    const auto& data2 = m[key2];
    LOG_S(INFO) << "Condensed TR1: "
                << CondensedTurnRestrictionDebugString(g, key1, data1);
    std::vector<uint64_t> expected1 = {'C', 'F'};
    CHECK_S(CondensedToNodeIds(g, key1, data1) == expected1);
    LOG_S(INFO) << "Condensed TR2: "
                << CondensedTurnRestrictionDebugString(g, key2, data2);
    std::vector<uint64_t> expected2 = {'B', 'F'};
    CHECK_S(CondensedToNodeIds(g, key2, data2) == expected2);
  }
}

void TestTRSimple() {
  FUNC_TIMER();
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node indexes.
  enum : uint32_t { Way0 = 0, Way1, Way2, Way3 };
  Graph g = CreateStandardTurnRestrictionGraph(true);

  OsmWrapper w;
  OSMPBF::Relation rel =
      CreateTRRelation(g, &w, /*id=*/1, Way0, D, Way2, "no_right_turn");

  TRResult res;
  ParseTurnRestriction(g, w.tagh, rel, Verbosity::Trace, &res);
  g.condensed_turn_restriction_map =
      ComputeCondensedTurnRestrictions(g, Verbosity::Trace, res.trs);
  CHECK_EQ_S(g.condensed_turn_restriction_map.size(), 1);

  for (const auto& [key, data] : g.condensed_turn_restriction_map) {
    LOG_S(INFO) << "Condensed TR: "
                << CondensedTurnRestrictionDebugString(g, key, data);
    std::vector<uint64_t> expected = {'C', 'F'};
    CHECK_S(CondensedToNodeIds(g, key, data) == expected);
  }
}

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  if (argc != 1) {
    ABORT_S() << absl::StrFormat("usage: %s", argv[0]);
  }

  TestSimpleTurnRestrictionParsing();
  TestCondensed();
  TestTRSimple();

  LOG_S(INFO)
      << "\n\033[1;32m*****************************\nTesting successfully "
         "finished\n*****************************\033[0m";
  return 0;
}
