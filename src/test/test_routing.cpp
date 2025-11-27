#include <osmpbf/osmpbf.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include "algos/compact_dijkstra.h"
#include "algos/compact_edge_dijkstra.h"
#include "algos/edge_router3.h"
#include "algos/edge_routing_label3.h"
#include "algos/router.h"
#include "algos/tarjan.h"
#include "base/country_code.h"
#include "base/util.h"
#include "graph/build_clusters.h"
#include "graph/compact_graph.h"
#include "graph/graph_def.h"
#include "graph/routing_attrs.h"
#include "osm/osm_helpers.h"
#include "test/test_utils.h"

RoutingResult TestCompactEdgeRouter(const Graph& g, uint32_t g_start,
                                    uint32_t g_target,
                                    const RoutingMetric& metric,
                                    const RoutingOptions& opt) {
  g.DebugPrint();
  // Providing all nodes as start nodes causes the nodes indices to be the same
  // in g and cg, which helps in debugging.
  std::vector<uint32_t> nodes;
  for (uint32_t i = 0; i < g.nodes.size(); ++i) {
    nodes.push_back(i);
  }
  const CompactDijkstraRoutingData data = CreateCompactDijkstraRoutingData(
      g, /*{g_start, g_target}*/ nodes, metric, opt, Verbosity::Brief);
  return RouteOnCompactGraph(data, g_start, g_target, Verbosity::Brief);
}

// Create a compacted graph from 'g' which has the same node ordering and the
// same number of edges.
CompactGraph CreateCompactGraph(const Graph& g) {
  std::vector<CompactGraph::FullEdge> full_edges;
  for (uint32_t from_idx = 0; from_idx < g.nodes.size(); ++from_idx) {
    for (const GEdge& e : gnode_forward_edges(g, from_idx)) {
      full_edges.push_back(
          {.from_c_idx = from_idx,
           .to_c_idx = e.target_idx,
           .weight = (uint32_t)e.distance_cm,
           .restricted_access = (e.car_label != GEdge::LABEL_FREE)});
    }
  }
  CompactGraph::SortAndCleanupEdges(&full_edges);
  CompactGraph cg(g.nodes.size(), full_edges);
  CHECK_EQ_S(cg.num_nodes(), g.nodes.size());
  CHECK_EQ_S(cg.edges().size(), g.edges.size());
  return cg;
}

uint32_t ExecuteSingleSourceDijkstra(const CompactGraph& cg,
                                     uint32_t start_c_idx,
                                     uint32_t target_c_idx) {
  std::vector<compact_dijkstra::VisitedNode> vis =
      compact_dijkstra::SingleSourceDijkstra(cg, start_c_idx);
  LOG_S(INFO) << absl::StrFormat("CompactGraph route from %u to %u metric %u",
                                 start_c_idx, target_c_idx,
                                 vis.at(target_c_idx).min_weight);
  return vis.at(target_c_idx).min_weight;
}

/*
 * This graph contains 4 nodes in three clusters 0:[a], 1:[b,c] and 2:[d].
 * Test cluster construction and routing for edge based routers.
 *
 *   [a] ----- [b] ----- [c] ----- [d]
 *         1         2         4
 */
Graph CreateEdgeClusterGraph(bool both_dirs) {
  enum : uint32_t { A = 0, B, C, D };  // Node names.
  Graph g;
  AddDefaultWSA(g);
  AddWay(g, /*way_idx=*/0);

  AddCluster(g, 0, {});
  AddCluster(g, 1, {});
  AddCluster(g, 2, {});

  AddNode(g, A, /*cluster_id=*/0);
  AddNode(g, B, /*cluster_id=*/1);
  AddNode(g, C, /*cluster_id=*/1);
  AddNode(g, D, /*cluster_id=*/2);

  std::vector<TEdge> edges;
  AddEdge(A, B, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(B, C, 2000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(C, D, 4000, GEdge::LABEL_FREE, both_dirs, &edges);
  StoreEdges(edges, &g);

  build_clusters::UpdateEdgesAndBorderNodes(&g);
  for (GCluster& cl : g.clusters) {
    build_clusters::ComputeShortestClusterPaths(g, RoutingMetricDistance(),
                                                VH_MOTORCAR, &cl);
    build_clusters::ComputeShortestClusterEdgePaths(g, RoutingMetricDistance(),
                                                    VH_MOTORCAR, &cl);
  }

  return g;
}

void TestEdgeRoutingLabel3() {
  FUNC_TIMER();
  /*
   * This graph contains 4 nodes in three clusters 0:[a], 1:[b,c] and 2:[d].
   * Test cluster construction and routing for edge based routers.
   *
   *   [a] ----- [b] ----- [c] ----- [d]
   *         1         2         4
   */
  enum : uint32_t { A = 0, B, C, D };  // Node names.
  Graph g = CreateEdgeClusterGraph(/*both_dirs=*/true);

  {
    CTRList list;
    const FullEdge fe_bc = gnode_find_full_edge(g, B, C, /*way_idx=*/0);
    const EdgeRoutingLabel3 bc =
        EdgeRoutingLabel3::CreateGraphEdgeLabel(g, B, fe_bc.gedge(g), 1);
    CHECK_S(bc.GetType() == EdgeRoutingLabel3::GRAPH);
    CHECK_EQ_S(bc.GetFromIdx(g, list), B);
    CHECK_EQ_S(bc.GetOffset(), fe_bc.offset());
    CHECK_EQ_S(bc.GetBit(), 1);
    CHECK_EQ_S(bc.FromNode(g, list).node_id, 'A' + B);
    CHECK_EQ_S(bc.ToNode(g, list).node_id, 'A' + C);
    CHECK_EQ_S(bc.GetEdgeIdx(g, list), fe_bc.gedge_idx(g));
    CHECK_EQ_S(bc.GetEdge(g, list).target_idx, C);

    const FullEdge fe_cb = gnode_find_full_edge(g, C, B, /*way_idx=*/0);
    const EdgeRoutingLabel3 cb =
        EdgeRoutingLabel3::CreateGraphEdgeLabel(g, C, fe_cb.gedge(g), 0);
    CHECK_NE_S(bc.UInt64Key(g, list), cb.UInt64Key(g, list));
  }

  {
    CTRList list;
    const GCluster cl = g.clusters.at(1);
    CHECK_EQ_S(cl.border_in_edges.size(), 2);
    CHECK_EQ_S(cl.border_out_edges.size(), 2);
    for (uint32_t inp = 0; inp < cl.border_in_edges.size(); ++inp) {
      for (uint32_t outp = 0; outp < cl.border_out_edges.size(); ++outp) {
        const auto& ed_in = cl.border_in_edges.at(inp);
        const auto& ed_out = cl.border_out_edges.at(outp);
        EdgeRoutingLabel3 lbl = EdgeRoutingLabel3::CreateClusterEdgeLabel(
            g, ed_in.g_edge_idx, /*offset=*/outp, /*bit=*/0);
        CHECK_S(lbl.GetType() == EdgeRoutingLabel3::CLUSTER);
        CHECK_S(lbl.IsClusterEdge());
        CHECK_EQ_S(lbl.GetFromIdx(g, list),
                   g.edges.at(ed_in.g_edge_idx).target_idx);
        CHECK_EQ_S(lbl.GetInEdgeIdx(), ed_in.g_edge_idx);
        CHECK_EQ_S(lbl.GetIncomingEdgeDescriptor(g).pos, ed_in.pos);
        CHECK_EQ_S(lbl.GetOutgoingEdgeDescriptor(g).pos, ed_out.pos);
        CHECK_EQ_S(lbl.GetClusterId(g), cl.cluster_id);
        CHECK_EQ_S(lbl.GetOffset(), outp);
        CHECK_EQ_S(lbl.GetBit(), 0);
        CHECK_EQ_S(lbl.GetToIdx(g, list),
                   g.edges.at(ed_out.g_edge_idx).target_idx);
        // uint64_t key should be different  for the exit edge and for a normal
        // graph edge.
        const EdgeRoutingLabel3 graph_lbl =
            EdgeRoutingLabel3::CreateGraphEdgeLabel(
                g, ed_out.g_from_idx, g.edges.at(ed_out.g_edge_idx), 0);
        CHECK_NE_S(lbl.UInt64Key(g, list), graph_lbl.UInt64Key(g, list));
      }
    }
  }

  {
    uint32_t ab_edge_idx = gnode_find_edge_idx(g, A, B, /*way_idx=*/0);
    TurnRestriction tr;
    // Turn restriction with edge ab in the leg 3.
    tr.path.push_back({});
    tr.path.push_back({});
    tr.path.push_back({});
    tr.path.push_back({.from_node_idx = A,
                       .way_idx = 0,
                       .to_node_idx = B,
                       .edge_idx = ab_edge_idx});
    // Store the turn restriction as position 1.
    g.complex_turn_restrictions.push_back(TurnRestriction());
    g.complex_turn_restrictions.push_back(tr);
    // Create a CTR list with the active CTR at position 2.
    CTRList list = {{}, {}, {{.ctr_idx = 1, .position = 3}}};
    // Create label that points to the active CTR created above.
    const EdgeRoutingLabel3 lbl =
        EdgeRoutingLabel3::CreateCTREdgeLabel(g, 2, /*bit=*/1);

    CHECK_S(lbl.GetType() == EdgeRoutingLabel3::COMPLEX_TURN_RESTRICTION);
    CHECK_S(!lbl.IsClusterEdge());
    CHECK_EQ_S(lbl.GetFromIdx(g, list), A);
    CHECK_EQ_S(lbl.GetOffset(), 0);
    CHECK_EQ_S(lbl.GetBit(), 1);
    CHECK_EQ_S(lbl.GetToIdx(g, list), B);
    CHECK_EQ_S(lbl.FromNode(g, list).node_id, 'A' + A);
    CHECK_EQ_S(lbl.ToNode(g, list).node_id, 'A' + B);
    CHECK_EQ_S(lbl.GetEdgeIdx(g, list), ab_edge_idx);
    CHECK_EQ_S(lbl.GetEdge(g, list).target_idx, B);
  }
}

void TestCTRDeDuper() {
  FUNC_TIMER();

  CTRDeDuper ctr_deduper;
  ActiveCtrs active_ctrs;
  active_ctrs.emplace_back(1, 2);
  CHECK_EQ_S(ctr_deduper.Add(active_ctrs), 0);
  CHECK_EQ_S(ctr_deduper.Add(active_ctrs), 0);
  active_ctrs.emplace_back(1, 3);
  CHECK_EQ_S(ctr_deduper.Add(active_ctrs), 1);
  CHECK_EQ_S(ctr_deduper.Add(active_ctrs), 1);
}

/*
 * A Graph with two dead-ends a,b and e,f.
 * Use to check the routing from/to dead-ends works as expected.
 *
 *                             [g]
 *                             /  \
 *                            /    \
 *                           / 1  1 \
 *                          /        \
 *     [a] ----- [b] ----- [c] ----- [d] ----- [e] ----- [f]
 *           1         1         1         1         1
 */
Graph CreateGraphWithDeadEnds(bool both_dirs) {
  enum : uint32_t { A = 0, B, C, D, E, F, G };  // Node names.
  Graph g;
  AddDefaultWSA(g);
  AddWay(g, /*way_idx=*/0);

  AddNode(g, A);
  AddNode(g, B);
  AddNode(g, C);
  AddNode(g, D);
  AddNode(g, E);
  AddNode(g, F);
  AddNode(g, G);

  std::vector<TEdge> edges;
  AddEdge(A, B, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(B, C, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(C, D, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(D, E, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(E, F, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(C, G, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(G, D, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  StoreEdges(edges, &g);

  g.large_components.push_back({.start_node = A, .size = 7});
  ApplyTarjan(g);
  return g;
}

void TestRouteDeadEnds() {
  FUNC_TIMER();
  enum : uint32_t { A = 0, B, C, D, E, F, G };  // Node names.
  const Graph g = CreateGraphWithDeadEnds(/*both_dirs=*/true);
  {
    EdgeRouter3 router(g, 3);
    RoutingOptions opt;
    opt.MayFillBridgeNodeId(g, F);
    auto res = router.Route(A, F, RoutingMetricDistance(), opt);
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 5000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 6);
  }
  {
    CHECK_EQ_S(5000, TestCompactEdgeRouter(g, A, F, RoutingMetricDistance(),
                                           {.avoid_dead_end = false})
                         .found_distance);
  }
  {
    CHECK_EQ_S(3000, TestCompactEdgeRouter(g, B, E, RoutingMetricDistance(),
                                           {.avoid_dead_end = false})
                         .found_distance);
  }
  {
    CHECK_EQ_S(5000, TestCompactEdgeRouter(g, F, A, RoutingMetricDistance(),
                                           {.avoid_dead_end = false})
                         .found_distance);
  }
  {
    CHECK_EQ_S(3000, TestCompactEdgeRouter(g, E, B, RoutingMetricDistance(),
                                           {.avoid_dead_end = false})
                         .found_distance);
  }
}

/*
 * A typical graph with restricted areas. The restricted edge [a][b] should be
 * bypassed through [e] because this it is shorter.
 * This should be handled correctly by all routers, because the shortest way
 * when ignoring restricted-access is the correct way also when considering
 * restricted-access.
 *
 *          [e]                [f]
 *         /   \               /  \
 *        /     \             /    \
 *       / 1   1 \           / 1  1 \
 *      /         \         /        \
 *     [a] ----- [b] ----- [c] ----- [d]
 *           4r        1         1r
 */
Graph CreateGraphWithRestrictedSimple(bool both_dirs) {
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node names.
  Graph g;
  AddDefaultWSA(g);
  AddWay(g, /*way_idx=*/0);

  AddNode(g, A);
  AddNode(g, B);
  AddNode(g, C);
  AddNode(g, D);
  AddNode(g, E);
  AddNode(g, F);

  std::vector<TEdge> edges;
  AddEdge(A, B, 4000, GEdge::LABEL_RESTRICTED, both_dirs, &edges);
  AddEdge(A, E, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(E, B, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(B, C, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(C, D, 1000, GEdge::LABEL_RESTRICTED, both_dirs, &edges);
  AddEdge(C, F, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(F, D, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  StoreEdges(edges, &g);

  return g;
}

void TestRouteRestrictedSimple() {
  FUNC_TIMER();
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node names.
  const Graph g = CreateGraphWithRestrictedSimple(/*both_dirs=*/true);
  {
    EdgeRouter3 router(g, 3);
    auto res = router.Route(A, D, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 4000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 5);
  }
  {
    CHECK_EQ_S(4000, TestCompactEdgeRouter(g, A, D, RoutingMetricDistance(),
                                           RoutingOptions())
                         .found_distance);
  }
  {
    EdgeRouter3 router(g, 3);
    auto res = router.Route(D, A, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 4000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 5);
  }
  {
    Router router(g, 3);
    auto res = router.Route(A, D, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 4000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 5);
  }
  {
    Router router(g, 3);
    auto res = router.Route(D, A, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 4000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 5);
  }
}

/*
 * A graph with restricted areas. Edge labels denote the length, 'r' means that
 * the edge is restricted.
 *
 *          [g]       [h]
 *         /   \     /   \
 *        /     \   /     \
 *       / 1   1 \ / 5   5 \
 *      /         V         \
 *     [a] ----- [b] ----- [c] ----- [d] ----- [e] ----- [f] ----- [i]
 *           5r        1r        1         1         1r        1
 */
Graph CreateGraphWithRestricted(bool both_dirs) {
  enum : uint32_t { A = 0, B, C, D, E, F, G, H, I };  // Node names.
  Graph g;
  AddDefaultWSA(g);
  AddWay(g, /*way_idx=*/0);

  AddNode(g, A);
  AddNode(g, B);
  AddNode(g, C);
  AddNode(g, D);
  AddNode(g, E);
  AddNode(g, F);
  AddNode(g, G);
  AddNode(g, H);
  AddNode(g, I);

  std::vector<TEdge> edges;
  AddEdge(A, G, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(A, B, 5000, GEdge::LABEL_RESTRICTED, both_dirs, &edges);
  AddEdge(B, H, 5000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(B, C, 1000, GEdge::LABEL_RESTRICTED, both_dirs, &edges);
  AddEdge(C, D, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(D, E, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(E, F, 1000, GEdge::LABEL_RESTRICTED, both_dirs, &edges);
  AddEdge(G, B, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(H, C, 5000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(F, I, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  StoreEdges(edges, &g);

  return g;
}

void TestRouteRestricted() {
  FUNC_TIMER();
  enum : uint32_t { A = 0, B, C, D, E, F, G, H, I };  // Node names.
  const Graph g = CreateGraphWithRestricted(/*both_dirs=*/true);
  {
    EdgeRouter3 router(g, 3);
    auto res = router.Route(A, D, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 7000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 4);
    CHECK_EQ_S(7000, TestCompactEdgeRouter(g, A, D, RoutingMetricDistance(),
                                           RoutingOptions())
                         .found_distance);
  }
  {
    EdgeRouter3 router(g, 3);
    auto res = router.Route(A, E, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 8000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 5);
    CHECK_EQ_S(8000, TestCompactEdgeRouter(g, A, E, RoutingMetricDistance(),
                                           RoutingOptions())
                         .found_distance);
  }
  {
    EdgeRouter3 router(g, 3);
    auto res = router.Route(A, F, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 9000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 6);
    CHECK_EQ_S(9000, TestCompactEdgeRouter(g, A, F, RoutingMetricDistance(),
                                           RoutingOptions())
                         .found_distance);
  }
  {
    EdgeRouter3 router(g, 3);
    auto res = router.Route(A, I, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(!res.found);
    CHECK_EQ_S(INFU32, TestCompactEdgeRouter(g, A, I, RoutingMetricDistance(),
                                             RoutingOptions())
                           .found_distance);
  }

  {
    EdgeRouter3 router(g, 3);
    auto res = router.Route(D, A, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 7000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 4);
    CHECK_EQ_S(7000, TestCompactEdgeRouter(g, D, A, RoutingMetricDistance(),
                                           RoutingOptions())
                         .found_distance);
  }
  {
    EdgeRouter3 router(g, 3);
    auto res = router.Route(E, A, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 8000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 5);
    CHECK_EQ_S(8000, TestCompactEdgeRouter(g, E, A, RoutingMetricDistance(),
                                           RoutingOptions())
                         .found_distance);
  }
  {
    EdgeRouter3 router(g, 3);
    auto res = router.Route(F, A, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 9000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 6);
    CHECK_EQ_S(9000, TestCompactEdgeRouter(g, F, A, RoutingMetricDistance(),
                                           RoutingOptions())
                         .found_distance);
  }
  {
    EdgeRouter3 router(g, 3);
    auto res = router.Route(I, A, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(!res.found);
    CHECK_EQ_S(INFU32, TestCompactEdgeRouter(g, I, A, RoutingMetricDistance(),
                                             RoutingOptions())
                           .found_distance);
  }
}

/*
 * When routing from [a] to [f], this network can't be handled with standard
 * edge based Dijkstra, because start and end nodes are in the same restricted
 * area. There are two detours (through [g] and [h]) into the free network,
 * which both make the path shorter. But only the detour through [h] should
 * be taken for a valid shortest path. If both detours are taken, then the path
 * becomes invalid because it is not allowed to transit through the restricted
 * edge [c][d].
 *
 *                  [g]                 [h]
 *                 /   \               /   \
 *                /     \             /     \
 *               / 1   1 \           / 1   1 \
 *              /         \         /         \
 *   [a] ----- [b] ----- [c] ----- [d] ----- [e] ----- [f]
 *        1r         4r        1r        8r        1r
 */
Graph CreateRestrictedGraphHard(bool both_dirs) {
  enum : uint32_t { A = 0, B, C, D, E, F, G, H };  // Node names.
  Graph g;
  AddDefaultWSA(g);
  AddWay(g, /*way_idx=*/0);

  AddNode(g, A);
  AddNode(g, B);
  AddNode(g, C);
  AddNode(g, D);
  AddNode(g, E);
  AddNode(g, F);
  AddNode(g, G);
  AddNode(g, H);

  std::vector<TEdge> edges;
  AddEdge(A, B, 1000, GEdge::LABEL_RESTRICTED, both_dirs, &edges);
  AddEdge(B, C, 4000, GEdge::LABEL_RESTRICTED, both_dirs, &edges);
  AddEdge(B, G, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(G, C, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(C, D, 1000, GEdge::LABEL_RESTRICTED, both_dirs, &edges);
  AddEdge(D, E, 8000, GEdge::LABEL_RESTRICTED, both_dirs, &edges);
  AddEdge(D, H, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(H, E, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(E, F, 1000, GEdge::LABEL_RESTRICTED, both_dirs, &edges);
  StoreEdges(edges, &g);

  return g;
}

void TestRouteRestrictedHard() {
  FUNC_TIMER();
  enum : uint32_t { A = 0, B, C, D, E, F, G, H };  // Node names.
  const Graph g = CreateRestrictedGraphHard(/*both_dirs=*/true);

  // Basic edge based routing without multiple lebales per edge doesn't allow
  // detours into the free area if start and end nodes are in the same
  // restricted area. So currently they only find the path that stays in the
  // restricted area.
  {
    EdgeRouter3 router(g, 3);
    auto res = router.Route(A, F, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 9000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 7);
    CHECK_EQ_S(9000, TestCompactEdgeRouter(g, A, F, RoutingMetricDistance(),
                                           RoutingOptions())
                         .found_distance);
  }
  {
    EdgeRouter3 router(g, 3);
    auto res = router.Route(F, A, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 9000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 7);
    CHECK_EQ_S(9000, TestCompactEdgeRouter(g, F, A, RoutingMetricDistance(),
                                           RoutingOptions())
                         .found_distance);
  }
}

/*
 * This graph contains 6 nodes in three clusters 0:[a,b], 1:[c,d] and 2:[e,f].
 * The best route from a to d is a->b->e->f->c->d and crosses two cluster
 * borders b->e and f->c.
 *
 *             [e] ----- [f]
 *              |    1    |
 *              |         |
 *              | 1     1 |
 *              |         |
 *   [a] ----- [b] ----- [c] ----- [d]
 *         1         4         1
 */
Graph CreateClusterGraph(bool both_dirs) {
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node names.
  Graph g;
  AddDefaultWSA(g);
  AddWay(g, /*way_idx=*/0);

  /*
  AddCluster(g, 0, {.num_nodes = 2, .border_nodes = {B}, .distances = {{0}}});
  AddCluster(g, 1, {.num_nodes = 2, .border_nodes = {C}, .distances = {{0}}});
  AddCluster(g, 2,
             {.num_nodes = 2,
              .border_nodes = {E, F},
              .distances = {{0, 1000}, {1000, 0}}});
              */
  AddCluster(g, 0, {});
  AddCluster(g, 1, {});
  AddCluster(g, 2, {});

  AddNode(g, A, /*cluster_id=*/0);
  AddNode(g, B, /*cluster_id=*/0);
  AddNode(g, C, /*cluster_id=*/1);
  AddNode(g, D, /*cluster_id=*/1);
  AddNode(g, E, /*cluster_id=*/2);
  AddNode(g, F, /*cluster_id=*/2);

  std::vector<TEdge> edges;
  AddEdge(A, B, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(B, C, 4000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(B, E, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(C, D, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(E, F, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(F, C, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  StoreEdges(edges, &g);

  build_clusters::UpdateEdgesAndBorderNodes(&g);
  for (GCluster& cl : g.clusters) {
    build_clusters::ComputeShortestClusterPaths(g, RoutingMetricDistance(),
                                                VH_MOTORCAR, &cl);
    build_clusters::ComputeShortestClusterEdgePaths(g, RoutingMetricDistance(),
                                                    VH_MOTORCAR, &cl);
  }

  return g;
}

void TestClusterRoute() {
  FUNC_TIMER();
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node names.
  const Graph g = CreateClusterGraph(/*both_dirs=*/true);

  {
    EdgeRouter3 router(g, 3);
    auto res = router.Route(A, D, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 5000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 6);
  }
  {
    EdgeRouter3 router(g, 3);
    RoutingOptions opt;
    opt.SetHybridOptions(g, A, D);
    auto res = router.Route(A, D, RoutingMetricDistance(), opt);
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 5000);
    // Cluster edges cover the internal connection and the outgoing edge. Hence
    // there is one edge less.
    CHECK_EQ_S(res.num_shortest_route_nodes, 5);
  }
}

/*
 * This graph contains 6 nodes in three clusters 0:[a,e], 1:[b,c,f] and 2:[d].
 * The best route from a to d is a->e->f->c->d. The router will first see
 * a->b->c, but this will be replaced with a->e->f->c. Since the last edges of
 * these two routes are in the same cluster, f->c will replace b->c.
 *
 *   [e] ----- [f]
 *    |    1       \
 *    |             \
 *    | 1            \ 1
 *    |               \
 *   [a] ----- [b] --- [c] ----- [d]
 *         1        5        1
 */
Graph CreateClusterGraphDoubleEdge(bool both_dirs) {
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node names.
  Graph g;
  AddDefaultWSA(g);
  AddWay(g, /*way_idx=*/0);

  /*
  AddCluster(g, 0,
             {.num_nodes = 2,
              .border_nodes = {A, E},
              .distances = {{0, 1000}, {1000, 0}}});
  AddCluster(
      g, 1,
      {.num_nodes = 3,
       .border_nodes = {B, C, F},
       .distances = {{0, 5000, INFU32}, {5000, 0, 1000}, {INFU32, 1000, 0}}});
  AddCluster(g, 2, {.num_nodes = 1, .border_nodes = {D}, .distances = {{0}}});
  */
  AddCluster(g, 0, {});
  AddCluster(g, 1, {});
  AddCluster(g, 2, {});

  AddNode(g, A, /*cluster_id=*/0);
  AddNode(g, B, /*cluster_id=*/1);
  AddNode(g, C, /*cluster_id=*/1);
  AddNode(g, D, /*cluster_id=*/2);
  AddNode(g, E, /*cluster_id=*/0);
  AddNode(g, F, /*cluster_id=*/1);

  std::vector<TEdge> edges;
  AddEdge(A, B, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(A, E, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(B, C, 5000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(C, D, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(E, F, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  AddEdge(F, C, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  StoreEdges(edges, &g);

  build_clusters::UpdateEdgesAndBorderNodes(&g);
  for (GCluster& cl : g.clusters) {
    build_clusters::ComputeShortestClusterPaths(g, RoutingMetricDistance(),
                                                VH_MOTORCAR, &cl);
    build_clusters::ComputeShortestClusterEdgePaths(g, RoutingMetricDistance(),
                                                    VH_MOTORCAR, &cl);
  }

  return g;
}

void TestClusterRouteDoubleEdge() {
  FUNC_TIMER();
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node names.
  const Graph g = CreateClusterGraphDoubleEdge(/*both_dirs=*/true);

  {
    EdgeRouter3 router(g, 3);
    auto res = router.Route(A, D, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 4000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 5);
  }
  {
    EdgeRouter3 router(g, 3);
    RoutingOptions opt;
    opt.SetHybridOptions(g, A, D);
    auto res = router.Route(A, D, RoutingMetricDistance(), opt);
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 4000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 4);

    // Now check the individual parts of the route...
    CHECK_EQ_S(res.route_v_idx.size(), 3);

    EdgeRouter3::VisitedEdge ve = router.GetVEdge(res.route_v_idx.at(0));
    CHECK_EQ_S(ve.key.GetFromIdx(g, router.ctr_list_), A);
    CHECK_EQ_S(ve.key.GetToIdx(g, router.ctr_list_), E);
    CHECK_S(!ve.key.IsClusterEdge());

    ve = router.GetVEdge(res.route_v_idx.at(1));
    CHECK_EQ_S(ve.key.GetFromIdx(g, router.ctr_list_), E);
    CHECK_EQ_S(ve.key.GetToIdx(g, router.ctr_list_), F);
    CHECK_S(!ve.key.IsClusterEdge());

    ve = router.GetVEdge(res.route_v_idx.at(2));
    CHECK_EQ_S(ve.key.GetFromIdx(g, router.ctr_list_), F);
    CHECK_EQ_S(ve.key.GetToIdx(g, router.ctr_list_), D);
    CHECK_S(ve.key.IsClusterEdge());

    /*
    ve = router.GetVEdge(res.route_v_idx.at(3));
    CHECK_EQ_S(ve.key.GetFromIdx(g, router.ctr_list_), C);
    CHECK_EQ_S(ve.key.GetToIdx(g, router.ctr_list_), D);
    CHECK_S(!ve.key.IsClusterEdge());
    */
  }
}

void TestRouteSimpleTurnRestriction() {
  FUNC_TIMER();
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node indexes.
  enum : uint32_t { Way0 = 0, Way1, Way2, Way3 };
  Graph g = CreateStandardTurnRestrictionGraph(true);

  {
    // Test shortest way without turn restriction.
    EdgeRouter3 router(g, 3);
    auto res = router.Route(B, E, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 2000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 3);
  }
  {
    CHECK_EQ_S(2000, TestCompactEdgeRouter(g, B, E, RoutingMetricDistance(),
                                           RoutingOptions())
                         .found_distance);
  }

  // Add turn restriction to graph.
  OsmWrapper w;
  OSMPBF::Relation rel =
      CreateSimpleTRRelation(g, &w, /*id=*/1, Way0, D, Way2, "no_right_turn");
  TRResult res;
  ParseTurnRestriction(g, w.tagh, rel, Verbosity::Trace, &res);

#if 0
  g.simple_turn_restriction_map =
      ComputeSimpleTurnRestrictionMap(g, Verbosity::Trace, res.trs);
  CHECK_EQ_S(g.simple_turn_restriction_map.size(), 1);
  MarkSimpleViaNodes(&g);
#endif
  AddTurnCostsForTests(res.trs, VH_MOTORCAR, &g);

  {
    // Test shortest way with turn restriction.
    EdgeRouter3 router(g, 3);
    auto res = router.Route(B, E, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);

    // This checks that a simple turn restriction is working.
    CHECK_EQ_S(res.found_distance, 4000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 5);
    CHECK_S(router.GetShortestPathNodeIndexes(res) ==
            std::vector<uint32_t>({B, A, C, D, E}));
  }
  {
    // Now check that running on the compact graph yields the same results.
    CHECK_EQ_S(4000, TestCompactEdgeRouter(g, B, E, RoutingMetricDistance(),
                                           RoutingOptions())
                         .found_distance);
  }
}

/*
 * Most simple graph to test a complex turn restriction. Find the shortest way
 * from [a] to [d].
 *
 * If w0-w1-w2 is forbidden, it is not possible.
 *
 *    [a] ------- [b] ------- [c] ------- [d]
 *          1w0         1w1          1w2
 *
 */
inline Graph CreateBasicComplexTurnRestrictionGraph(bool both_dirs) {
  enum : uint32_t { A = 0, B, C, D };  // Node indexes.
  enum : uint32_t { Way0 = 0, Way1, Way2, Way3 };
  Graph g;
  AddDefaultWSA(g);

  AddNode(g, A);
  AddNode(g, B);
  AddNode(g, C);
  AddNode(g, D);

  AddWay(g, /*way_idx=*/Way0, HW_TERTIARY, /*wsa_id=*/0, {A, B});
  AddWay(g, /*way_idx=*/Way1, HW_TERTIARY, /*wsa_id=*/0, {B, C});
  AddWay(g, /*way_idx=*/Way2, HW_TERTIARY, /*wsa_id=*/0, {C, D});

  std::vector<TEdge> edges;
  AddEdge(A, B, 1000, GEdge::LABEL_FREE, Way0, both_dirs, &edges);
  AddEdge(B, C, 1000, GEdge::LABEL_FREE, Way1, both_dirs, &edges);
  AddEdge(C, D, 1000, GEdge::LABEL_FREE, Way2, both_dirs, &edges);
  StoreEdges(edges, &g);

  return g;
}

void TestRouteBasicComplexTurnRestriction() {
  FUNC_TIMER();
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node indexes.
  enum : uint32_t { Way0 = 0, Way1, Way2, Way3 };
  Graph g = CreateBasicComplexTurnRestrictionGraph(true);

  {
    // Test shortest way without turn restriction.
    EdgeRouter3 router(g, 3);
    auto res = router.Route(A, D, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 3000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 4);
    CHECK_S(router.GetShortestPathNodeIndexes(res) ==
            std::vector<uint32_t>({A, B, C, D}));
    CHECK_EQ_S(3000, TestCompactEdgeRouter(g, A, D, RoutingMetricDistance(),
                                           RoutingOptions())
                         .found_distance);
  }

  // Add turn restriction to graph.
  OsmWrapper w;
  OSMPBF::Relation rel = CreateComplexTRRelation(
      g, &w, /*id=*/1, {Way0, Way1, Way2}, "no_straight_on");
  TRResult res;
  ParseTurnRestriction(g, w.tagh, rel, Verbosity::Trace, &res);
  g.complex_turn_restrictions = res.trs;
  CHECK_EQ_S(g.complex_turn_restrictions.size(), 1);
  g.complex_turn_restriction_map =
      ComputeTurnRestrictionMapToFirst(g.complex_turn_restrictions);
  CHECK_EQ_S(g.complex_turn_restriction_map.size(), 1);
  MarkComplexTriggerEdges(&g);

  {
    // Test shortest way with turn restriction.
    EdgeRouter3 router(g, 3);
    auto res = router.Route(A, D, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(!res.found);
    CHECK_EQ_S(INFU32, TestCompactEdgeRouter(g, A, D, RoutingMetricDistance(),
                                             RoutingOptions())
                           .found_distance);
  }
}

/*
 * Find the shortest way from [a] to [d].
 *
 * Given the forbidden turn w0-w1-w2, it is not possible to go [a][b][c][d], so
 * one needs to go [a][b][c][f][d] instead
 *
 *
 *                [e]               [f]
 *                 |               /   \
 *            1-w3 |         5-w4 /     \ 5-w4
 *                 |             /       \
 *                 |            /         \
 *    [a] ------- [b] ------- [c] ------- [d]
 *          1-w0        1-w1        1-w2
 *
 *       Notation: '5-w4': Labels an edge of length 5 and on way 4.
 */
inline Graph CreateComplexTurnRestrictionGraph(bool both_dirs) {
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node indexes.
  enum : uint32_t { Way0 = 0, Way1, Way2, Way3, Way4 };
  Graph g;
  AddDefaultWSA(g);

  AddNode(g, A);
  AddNode(g, B);
  AddNode(g, C);
  AddNode(g, D);
  AddNode(g, E);
  AddNode(g, F);

  AddWay(g, /*way_idx=*/Way0, HW_TERTIARY, /*wsa_id=*/0, {A, B});
  AddWay(g, /*way_idx=*/Way1, HW_TERTIARY, /*wsa_id=*/0, {B, C});
  AddWay(g, /*way_idx=*/Way2, HW_TERTIARY, /*wsa_id=*/0, {C, D});
  AddWay(g, /*way_idx=*/Way3, HW_TERTIARY, /*wsa_id=*/0, {E, B});
  AddWay(g, /*way_idx=*/Way4, HW_TERTIARY, /*wsa_id=*/0, {C, F, D});

  std::vector<TEdge> edges;
  AddEdge(A, B, 1000, GEdge::LABEL_FREE, Way0, both_dirs, &edges);
  AddEdge(B, C, 1000, GEdge::LABEL_FREE, Way1, both_dirs, &edges);
  AddEdge(C, D, 1000, GEdge::LABEL_FREE, Way2, both_dirs, &edges);
  AddEdge(E, B, 1000, GEdge::LABEL_FREE, Way3, both_dirs, &edges);
  AddEdge(C, F, 5000, GEdge::LABEL_FREE, Way4, both_dirs, &edges);
  AddEdge(F, D, 5000, GEdge::LABEL_FREE, Way4, both_dirs, &edges);
  StoreEdges(edges, &g);

  return g;
}

void TestRouteComplexTurnRestrictionNegative() {
  FUNC_TIMER();
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node indexes.
  enum : uint32_t { Way0 = 0, Way1, Way2, Way3, Way4 };
  Graph g = CreateComplexTurnRestrictionGraph(true);

  {
    // Test shortest way without turn restriction.
    EdgeRouter3 router(g, 3);
    auto res = router.Route(A, D, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 3000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 4);
    CHECK_S(router.GetShortestPathNodeIndexes(res) ==
            std::vector<uint32_t>({A, B, C, D}));
  }
  {
    // Test shortest way without turn restriction.
    EdgeRouter3 router(g, 3);
    auto res = router.Route(E, D, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 3000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 4);
    CHECK_S(router.GetShortestPathNodeIndexes(res) ==
            std::vector<uint32_t>({E, B, C, D}));
  }

  // Add turn restriction to graph.
  OsmWrapper w;
  OSMPBF::Relation rel = CreateComplexTRRelation(
      g, &w, /*id=*/1, {Way0, Way1, Way2}, "no_straight_on");
  TRResult res;
  ParseTurnRestriction(g, w.tagh, rel, Verbosity::Trace, &res);
  g.complex_turn_restrictions = res.trs;
  CHECK_EQ_S(g.complex_turn_restrictions.size(), 1);
  g.complex_turn_restriction_map =
      ComputeTurnRestrictionMapToFirst(g.complex_turn_restrictions);
  CHECK_EQ_S(g.complex_turn_restriction_map.size(), 1);
  MarkComplexTriggerEdges(&g);

  {
    // Test shortest way with turn restriction.
    EdgeRouter3 router(g, 3);
    auto res = router.Route(A, D, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.num_shortest_route_nodes, 6);
    CHECK_S(router.GetShortestPathNodeIndexes(res) ==
            std::vector<uint32_t>({A, B, E, B, C, D}));
    CHECK_EQ_S(res.found_distance, 5000);
    CHECK_EQ_S(5000, TestCompactEdgeRouter(g, A, D, RoutingMetricDistance(),
                                           RoutingOptions())
                         .found_distance);
  }
  {
    // Test route without initial segment.
    EdgeRouter3 router(g, 3);
    auto res = router.Route(E, D, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 3000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 4);
    CHECK_S(router.GetShortestPathNodeIndexes(res) ==
            std::vector<uint32_t>({E, B, C, D}));
    CHECK_EQ_S(3000, TestCompactEdgeRouter(g, E, D, RoutingMetricDistance(),
                                           RoutingOptions())
                         .found_distance);
  }
}

void TestRouteComplexTurnRestrictionPositive() {
  FUNC_TIMER();
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node indexes.
  enum : uint32_t { Way0 = 0, Way1, Way2, Way3, Way4 };
  Graph g = CreateComplexTurnRestrictionGraph(true);

  // Add turn restriction to graph.
  OsmWrapper w;
  OSMPBF::Relation rel = CreateComplexTRRelation(
      g, &w, /*id=*/1, {Way0, Way1, Way4}, "only_straight_on");
  TRResult res;
  ParseTurnRestriction(g, w.tagh, rel, Verbosity::Trace, &res);
  g.complex_turn_restrictions = res.trs;
  CHECK_EQ_S(g.complex_turn_restrictions.size(), 1);
  g.complex_turn_restriction_map =
      ComputeTurnRestrictionMapToFirst(g.complex_turn_restrictions);
  CHECK_EQ_S(g.complex_turn_restriction_map.size(), 1);
  MarkComplexTriggerEdges(&g);

  {
    // Test shortest way with turn restriction.
    EdgeRouter3 router(g, 3);
    auto res = router.Route(A, D, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.num_shortest_route_nodes, 6);
    CHECK_EQ_S(res.found_distance, 5000);
    CHECK_S(router.GetShortestPathNodeIndexes(res) ==
            std::vector<uint32_t>({A, B, E, B, C, D}));
    CHECK_EQ_S(5000, TestCompactEdgeRouter(g, A, D, RoutingMetricDistance(),
                                           RoutingOptions())
                         .found_distance);
  }
}

/*
 * Find the shortest way from [a] to [f].
 *
 *    [a] ------- [b] ------- [c] ------- [d] ------- [e] ------- [f]
 *          1w0         1w1          1w2        1w3         1w4
 *
 */
inline Graph CreateOverlappingTurnRestrictionsGraph(bool both_dirs) {
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node indexes.
  enum : uint32_t { Way0 = 0, Way1, Way2, Way3, Way4 };
  Graph g;
  AddDefaultWSA(g);

  AddNode(g, A);
  AddNode(g, B);
  AddNode(g, C);
  AddNode(g, D);
  AddNode(g, E);
  AddNode(g, F);

  AddWay(g, /*way_idx=*/Way0, HW_TERTIARY, /*wsa_id=*/0, {A, B});
  AddWay(g, /*way_idx=*/Way1, HW_TERTIARY, /*wsa_id=*/0, {B, C});
  AddWay(g, /*way_idx=*/Way2, HW_TERTIARY, /*wsa_id=*/0, {C, D});
  AddWay(g, /*way_idx=*/Way3, HW_TERTIARY, /*wsa_id=*/0, {D, E});
  AddWay(g, /*way_idx=*/Way4, HW_TERTIARY, /*wsa_id=*/0, {E, F});

  std::vector<TEdge> edges;
  AddEdge(A, B, 1000, GEdge::LABEL_FREE, Way0, both_dirs, &edges);
  AddEdge(B, C, 1000, GEdge::LABEL_FREE, Way1, both_dirs, &edges);
  AddEdge(C, D, 1000, GEdge::LABEL_FREE, Way2, both_dirs, &edges);
  AddEdge(D, E, 1000, GEdge::LABEL_FREE, Way3, both_dirs, &edges);
  AddEdge(E, F, 1000, GEdge::LABEL_FREE, Way4, both_dirs, &edges);
  StoreEdges(edges, &g);

  return g;
}

void TestRouteOverlappingTurnRestrictions() {
  FUNC_TIMER();
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node indexes.
  enum : uint32_t { Way0 = 0, Way1, Way2, Way3, Way4 };
  Graph g = CreateOverlappingTurnRestrictionsGraph(true);

  {
    // Test shortest way without turn restrictions.
    EdgeRouter3 router(g, 3);
    auto res = router.Route(A, F, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 5000);
  }

  // Add multiple turn restrictions to graph.
  // The longest turn restriction forbids the last segment. The other turn
  // restrictions start later and allow the last leg.
  TRResult res;
  OsmWrapper w;
  OSMPBF::Relation rel1 = CreateComplexTRRelation(
      g, &w, /*id=*/1, {Way0, Way1, Way2, Way3, Way4}, "no_straight_on");
  ParseTurnRestriction(g, w.tagh, rel1, Verbosity::Trace, &res);
  OSMPBF::Relation rel2 = CreateComplexTRRelation(
      g, &w, /*id=*/2, {Way1, Way2, Way3, Way4}, "only_straight_on");
  ParseTurnRestriction(g, w.tagh, rel2, Verbosity::Trace, &res);
  OSMPBF::Relation rel3 = CreateComplexTRRelation(
      g, &w, /*id=*/3, {Way2, Way3, Way4}, "only_straight_on");
  ParseTurnRestriction(g, w.tagh, rel3, Verbosity::Trace, &res);
  CHECK_EQ_S(res.max_success_via_ways, 3);
  g.complex_turn_restrictions = res.trs;
  CHECK_EQ_S(g.complex_turn_restrictions.size(), 3);
  g.complex_turn_restriction_map =
      ComputeTurnRestrictionMapToFirst(g.complex_turn_restrictions);
  CHECK_EQ_S(g.complex_turn_restriction_map.size(), 3);
  MarkComplexTriggerEdges(&g);

  {
    // Test shortest way with turn restriction.
    EdgeRouter3 router(g, 3);
    auto res = router.Route(A, F, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(!res.found);
    CHECK_EQ_S(INFU32, TestCompactEdgeRouter(g, A, F, RoutingMetricDistance(),
                                             RoutingOptions())
                           .found_distance);
  }
  {
    // Test route without initial segment.
    EdgeRouter3 router(g, 3);
    auto res = router.Route(B, F, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.num_shortest_route_nodes, 5);
    CHECK_S(router.GetShortestPathNodeIndexes(res) ==
            std::vector<uint32_t>({B, C, D, E, F}));
    CHECK_EQ_S(4000, TestCompactEdgeRouter(g, B, F, RoutingMetricDistance(),
                                           RoutingOptions())
                         .found_distance);
  }
}

uint32_t ExecuteRouter(const Graph& g, uint32_t from, uint32_t to,
                       std::string_view optstr) {
  RoutingOptions opt;
  if (optstr.contains("hybrid")) {
    opt.SetHybridOptions(g, from, to);
  }
  RoutingResult res;
  if (optstr.contains("edge")) {
    EdgeRouter3 router(g, 0);
    res = router.Route(from, to, RoutingMetricDistance(), opt);
  } else {
    Router router(g, 0);
    res = router.Route(from, to, RoutingMetricDistance(), opt);
  }
  if (res.found) {
    return res.found_distance;
  }
  return INFU32;
}

// Compare different shortest path algorithms against each other.
void CompareShortestPaths(const Graph& g, bool test_compact_edge) {
  const CompactGraph cg = CreateCompactGraph(g);

  // The mapping of graph to compact indexes is the identity. So just create
  // them in a simple loop.
  absl::flat_hash_map<uint32_t, uint32_t> graph_to_compact_nodemap;
  std::vector<std::uint32_t> compact_to_graph_nodemap;
  for (uint32_t i = 0; i < cg.num_nodes(); ++i) {
    graph_to_compact_nodemap[i] = i;
    compact_to_graph_nodemap.push_back(i);
  }

  for (uint32_t start = 0; start < cg.num_nodes(); ++start) {
    std::vector<compact_dijkstra::VisitedNode> vis_nssd =
        compact_dijkstra::SingleSourceDijkstra(cg, start);
    std::vector<uint32_t> w1 =
        compact_dijkstra::GetNodeWeightsFromVisitedNodes(vis_nssd);

    SingleSourceEdgeDijkstra edge_router;
    edge_router.Route(
        cg, {.c_start_idx = start},
        {.store_spanning_tree_edges = false,
         .handle_restricted_access = true,
         /*.restricted_access_nodes = edge_router.ComputeRestrictedAccessNodes(
             g, graph_to_compact_nodemap, compact_to_graph_nodemap, start)*/});
    std::vector<uint32_t> w2 =
        edge_router.GetNodeWeightsFromVisitedEdges(cg, start);

    // w1: SingleSourceDijkstra based on nodes, no restricted access handling.
    // w2: SingleSourceDijkstra based on edges, with restricted access handling.
    CHECK_EQ_S(w1.size(), w2.size());
    for (uint32_t i = 0; i < cg.num_nodes(); ++i) {
      LOG_S(INFO) << absl::StrFormat("Route %c to %c SSD-w1:%u E-SSD:w2:%u",
                                     65 + start, 65 + i, w1.at(i), w2.at(i));
      CHECK_EQ_S(w1.at(i), ExecuteRouter(g, start, i, ""));
      CHECK_EQ_S(w1.at(i), ExecuteRouter(g, start, i, "hybrid"));
      if (test_compact_edge) {
        // CompactDijkstra has simplified restricted-access handling, so it may
        // yield different results.
        CHECK_EQ_S(w2.at(i), ExecuteRouter(g, start, i, "edge"));
        CHECK_EQ_S(w2.at(i), ExecuteRouter(g, start, i, "edge,hybrid"));
      }
    }
  }
}

void TestCompareShortestPaths() {
  FUNC_TIMER();
  {
    LOG_S(INFO) << "Test CreateGraphWithRestrictedSimple()";
    const Graph g = CreateGraphWithRestrictedSimple(/*both_dirs=*/true);
    CompareShortestPaths(g, /*test_compact_edge=*/true);
  }
  {
    LOG_S(INFO) << "Test CreateGraphWithRestricted()";
    const Graph g = CreateGraphWithRestricted(/*both_dirs=*/true);
    CompareShortestPaths(g, /*test_compact_edge=*/false);
  }
  {
    LOG_S(INFO) << "Test CreateRestrictedGraphHard()";
    const Graph g = CreateRestrictedGraphHard(/*both_dirs=*/true);
    CompareShortestPaths(g, /*test_compact_edge=*/false);
  }
  {
    LOG_S(INFO) << "Test CreateClusterGraph()";
    const Graph g = CreateClusterGraph(/*both_dirs=*/true);
    CompareShortestPaths(g, /*test_compact_edge=*/true);
  }
  {
    LOG_S(INFO) << "Test CreateClusterGraphDoubleEdge()";
    const Graph g = CreateClusterGraphDoubleEdge(/*both_dirs=*/true);
    CompareShortestPaths(g, /*test_compact_edge=*/true);
  }
}

void TestBitFunctions() {
  FUNC_TIMER();
  CHECK_EQ_S(sizeof(int), 4);
  CHECK_EQ_S(sizeof(long), 8);
  CHECK_EQ_S(sizeof(long long), 8);
  // __builtin_ctz(0) is undefined.
  // CHECK_EQ_S(__builtin_ctz(0), 32);
  CHECK_EQ_S(__builtin_ctz(1u << 0), 0);
  CHECK_EQ_S(__builtin_ctz(1u << 30), 30);
  CHECK_EQ_S(__builtin_ctz(1u << 31), 31);
  CHECK_EQ_S(__builtin_ctzll(1llu << 60), 60);
  CHECK_EQ_S(__builtin_ctzll(1llu << 63), 63);
}

// Get compressed turn costs for a path of len 2. If the costs can't be found,
// -1 is returned.
int64_t GetComprTurnCost(const Graph& g, uint32_t node0_idx, uint32_t node1_idx,
                         uint32_t node2_idx) {
  N3Path pd = FindN3Path(g, node0_idx, node1_idx, node2_idx);
  if (pd.valid) {
    return pd.get_compressed_turn_cost_0to1(g);
  } else {
    return -1;
  }
}

void TestTurnCosts_UTurns() {
  FUNC_TIMER();
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node indexes.
  enum : uint32_t { Way0 = 0, Way1, Way2, Way3, Way4 };
  Graph g = CreateComplexTurnRestrictionGraph(true);

  // Check all u-turn related turn costs in the following graph.
  // For every edge we check all possible u-turns.
  /*
   *                [e]               [f]
   *                 |               /   \
   *             1w3 |          5w4 /     \ 5w4
   *                 |             /       \
   *                 |            /         \
   *    [a] ------- [b] ------- [c] ------- [d]
   *          1w0         1w1          1w2
   */

  constexpr uint32_t ALLOWED = compress_turn_cost(TURN_COST_U_TURN);
  constexpr uint32_t FORBIDDEN = TURN_COST_INFINITY_COMPRESSED;

  // Check non-existing path.
  CHECK_EQ_S(GetComprTurnCost(g, A, B, D), -1);

  // A: u-turn allowed.
  CHECK_EQ_S(GetComprTurnCost(g, B, A, B), ALLOWED);
  // B: u-turns forbidden.
  CHECK_EQ_S(GetComprTurnCost(g, A, B, A), FORBIDDEN);
  CHECK_EQ_S(GetComprTurnCost(g, C, B, C), FORBIDDEN);
  CHECK_EQ_S(GetComprTurnCost(g, E, B, E), FORBIDDEN);
  // C: u-turn: forbidden.
  CHECK_EQ_S(GetComprTurnCost(g, B, C, B), FORBIDDEN);
  CHECK_EQ_S(GetComprTurnCost(g, D, C, D), FORBIDDEN);
  CHECK_EQ_S(GetComprTurnCost(g, F, C, F), FORBIDDEN);
  // D: u-turn: forbidden.
  CHECK_EQ_S(GetComprTurnCost(g, C, D, C), FORBIDDEN);
  CHECK_EQ_S(GetComprTurnCost(g, F, D, F), FORBIDDEN);
  // E: u-turn: allowed.
  CHECK_EQ_S(GetComprTurnCost(g, B, E, B), ALLOWED);
  // F: u-turn: forbidden.
  CHECK_EQ_S(GetComprTurnCost(g, C, F, C), FORBIDDEN);
  CHECK_EQ_S(GetComprTurnCost(g, D, F, D), FORBIDDEN);
}

void TestTurnCosts_Angles() {
  FUNC_TIMER();
  enum : uint32_t { A = 0, B, C };  // Node indexes.
  constexpr bool both_dirs = true;

  Graph g;
  AddDefaultWSA(g);
  constexpr uint32_t Way0 = 0;
  AddWay(g, Way0);

  AddNode(g, A);
  AddNode(g, B);
  AddNode(g, C);

  std::vector<TEdge> edges;
  AddEdge(A, B, 1000, GEdge::LABEL_FREE, Way0, both_dirs, &edges);
  AddEdge(B, C, 1000, GEdge::LABEL_FREE, Way0, both_dirs, &edges);
  StoreEdges(edges, &g);

  SetNodeCoords(g, A, 0.0, 0.0);
  SetNodeCoords(g, B, 0.0, 0.1);

  SetNodeCoords(g, C, 0.0, 0.2);  // 180 degrees, i.e. no curve.
  RecomputeDistancesForTesting(&g);
  AddTurnCostsForTests({}, VH_MOTORCAR, &g);
  CHECK_EQ_S(FindN3Path(g, A, B, C).get_compressed_turn_cost_0to1(g),
             compress_turn_cost(0));

  SetNodeCoords(g, C, 0.1, 0.2);  // 135 degrees
  RecomputeDistancesForTesting(&g);
  AddTurnCostsForTests({}, VH_MOTORCAR, &g);
  CHECK_EQ_S(FindN3Path(g, A, B, C).get_compressed_turn_cost_0to1(g),
             compress_turn_cost(500));

  SetNodeCoords(g, C, 0.1, 0.1);  // 90 degrees
  RecomputeDistancesForTesting(&g);
  AddTurnCostsForTests({}, VH_MOTORCAR, &g);
  CHECK_EQ_S(FindN3Path(g, A, B, C).get_compressed_turn_cost_0to1(g),
             compress_turn_cost(2000));

  SetNodeCoords(g, C, 0.1, 0.0);  // 45 degrees
  RecomputeDistancesForTesting(&g);
  AddTurnCostsForTests({}, VH_MOTORCAR, &g);
  CHECK_EQ_S(FindN3Path(g, A, B, C).get_compressed_turn_cost_0to1(g),
             compress_turn_cost(4000));

  SetNodeCoords(g, C, 0.0, 0.0);  // 0 degrees, i.e. u-turn
  RecomputeDistancesForTesting(&g);
  AddTurnCostsForTests({}, VH_MOTORCAR, &g);
  CHECK_EQ_S(FindN3Path(g, A, B, C).get_compressed_turn_cost_0to1(g),
             compress_turn_cost(TURN_COST_U_TURN));

  SetNodeCoords(g, C, -0.1, 0.0);  // 45 degrees
  RecomputeDistancesForTesting(&g);
  AddTurnCostsForTests({}, VH_MOTORCAR, &g);
  CHECK_EQ_S(FindN3Path(g, A, B, C).get_compressed_turn_cost_0to1(g),
             compress_turn_cost(4000));

  SetNodeCoords(g, C, -0.1, 0.1);  // 90 degrees
  RecomputeDistancesForTesting(&g);
  AddTurnCostsForTests({}, VH_MOTORCAR, &g);
  CHECK_EQ_S(FindN3Path(g, A, B, C).get_compressed_turn_cost_0to1(g),
             compress_turn_cost(2000));

  SetNodeCoords(g, C, -0.1, 0.2);  // 135 degrees
  RecomputeDistancesForTesting(&g);
  AddTurnCostsForTests({}, VH_MOTORCAR, &g);
  CHECK_EQ_S(FindN3Path(g, A, B, C).get_compressed_turn_cost_0to1(g),
             compress_turn_cost(500));
}

void TestTurnCosts_Angles2() {
  FUNC_TIMER();

  // Maximal curve velocity for arc_length=100m, angle=90 degrees.
  CHECK_BETWEEN(MaxCurveVelocity(100 * 100, 90), 63.5, 63.7);
  // Maximal curve velocity for arc_length=10m, angle=90 degrees.
  CHECK_BETWEEN(MaxCurveVelocity(10 * 100, 90), 19.8, 20.3);
  // Maximal curve velocity for arc_length=4m, angle=180, i.e. u-turn.
  CHECK_BETWEEN(MaxCurveVelocity(4 * 100, 180), 8.9, 9.1);
}

void TestTurnCosts_SpeedChange() {
  FUNC_TIMER();
  // DistanceForSpeedChange(VH_MOTORCAR, 40, 60)
}

void TestCountryBitset() {
  FUNC_TIMER();
  {
    CountryBitset cb;
    for (uint32_t i = 0; i < MAX_NCC; ++i) {
      CHECK_EQ_S(cb.GetBit(i), false) << i;
      cb.SetBit(i);
      CHECK_EQ_S(cb.GetBit(i), true) << i;
      cb.SetBit(i, false);
      CHECK_EQ_S(cb.GetBit(i), false) << i;
    }
  }
  {
    // Test copy construction.
    CountryBitset cb;
    cb.SetBit(NCC_CH);
    CountryBitset cb2 = cb;
    CHECK_EQ_S(cb2.GetBit(NCC_CH), true);
    CHECK_EQ_S(cb2.GetBit(NCC_DE), false);
  }
  {
    CountryBitset cb("../config/left_traffic_countries.cfg");
    CHECK_EQ_S(cb.GetBit(NCC_CH), false);
    CHECK_EQ_S(cb.GetBit(NCC_DE), false);
    CHECK_EQ_S(cb.GetBit(NCC_AT), false);
    CHECK_EQ_S(cb.GetBit(NCC_FR), false);
    CHECK_EQ_S(cb.GetBit(NCC_LI), false);

    CHECK_EQ_S(cb.GetBit(TwoLetterCountryCodeToNum("AU")), true);
    CHECK_EQ_S(cb.GetBit(TwoLetterCountryCodeToNum("GB")), true);
    CHECK_EQ_S(cb.GetBit(TwoLetterCountryCodeToNum("NG")), true);
    CHECK_EQ_S(cb.GetBit(TwoLetterCountryCodeToNum("US")), false);
    CHECK_EQ_S(cb.GetBit(TwoLetterCountryCodeToNum("ZA")), true);
    CHECK_EQ_S(cb.GetBit(TwoLetterCountryCodeToNum("ZW")), true);
  }
}

void TestEdgeClusterRoute() {
  FUNC_TIMER();
  enum : uint32_t { A = 0, B, C, D };  // Node names.
  Graph g = CreateEdgeClusterGraph(/*both_dirs=*/true);

  GCluster& cl0 = g.clusters.at(0);
  GCluster& cl1 = g.clusters.at(1);
  // GCluster& cl2 = g.clusters.at(2);

  CHECK_EQ_S(cl0.border_in_edges.size(), 1);
  CHECK_EQ_S(cl0.border_out_edges.size(), 1);
  CHECK_EQ_S(cl1.border_in_edges.size(), 2);
  CHECK_EQ_S(cl1.border_out_edges.size(), 2);

  // Now check the distance from B to C. For this, we need an incoming edge at B
  // and an outgoing edge at C,
  const GCluster::EdgeDescriptor* ed_in_ab =
      FindEdgeDesc(g, cl1.border_in_edges, A, B);
  const GCluster::EdgeDescriptor* ed_out_cd =
      FindEdgeDesc(g, cl1.border_out_edges, C, D);
  CHECK_S(ed_in_ab != nullptr);
  CHECK_S(ed_out_cd != nullptr);

  CHECK_LT_S((size_t)ed_in_ab->pos, cl1.edge_distances.size());
  auto dist = cl1.edge_distances.at(ed_in_ab->pos);
  CHECK_LT_S((size_t)ed_out_cd->pos, dist.size());
  uint32_t metric = dist.at(ed_out_cd->pos);
  // The full way found through the cluster is A->B->C->D. A and D are not part
  // of cluster 1, but the are needed for the edges A->B and C->D. The metric
  // returned does not count the incoming edge, but it does count the outgoing
  // edge.
  CHECK_EQ_S(metric, 6000);
}

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  if (argc != 1) {
    ABORT_S() << absl::StrFormat("usage: %s", argv[0]);
  }

  TestEdgeRoutingLabel3();
  TestRouteDeadEnds();
  TestRouteRestrictedSimple();
  TestRouteRestricted();
  TestRouteRestrictedHard();
  TestClusterRoute();
  TestClusterRouteDoubleEdge();
  TestCompareShortestPaths();
  TestBitFunctions();
  TestRouteSimpleTurnRestriction();
  TestCTRDeDuper();
  TestRouteBasicComplexTurnRestriction();
  TestRouteComplexTurnRestrictionNegative();
  TestRouteComplexTurnRestrictionPositive();
  TestRouteOverlappingTurnRestrictions();

  TestTurnCosts_UTurns();
  TestTurnCosts_Angles();
  TestTurnCosts_Angles2();
  TestTurnCosts_SpeedChange();

  TestCountryBitset();

  TestEdgeClusterRoute();

  LOG_S(INFO)
      << "\n\033[1;32m*****************************\nTesting successfully "
         "finished\n*****************************\033[0m";
  return 0;
}
