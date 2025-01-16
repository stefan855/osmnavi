#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include "algos/edge_router.h"
#include "base/country_code.h"
#include "base/util.h"
#include "graph/graph_def.h"
#include "graph/routing_attrs.h"
#include "osm/osm_helpers.h"

void AddNode(Graph& g, uint32_t idx) {
  CHECK_EQ_S(idx, g.nodes.size());
  g.nodes.push_back({.node_id = 100 + idx,
                     .large_component = 1,
                     .cluster_id = 0,
                     .cluster_border_node = 0,
                     .edges_start_pos = 0,
                     .dead_end = 0,
                     .ncc = NCC_CH,
                     .lat = 100 + (int32_t)idx,
                     .lon = 100 + (int32_t)idx});
}

using TEdge = std::tuple<uint32_t, uint32_t, uint32_t, GEdge::RESTRICTION>;
void AddEdge(uint32_t from, uint32_t to, uint32_t w, GEdge::RESTRICTION label,
             bool both_dirs, std::vector<TEdge>* edges) {
  edges->push_back({from, to, w, label});
  if (both_dirs) {
    edges->push_back({to, from, w, label});
  }

  /*
  g.edges.push_back({.other_node_idx = to,
                     .way_idx = 0,
                     .distance_cm = w,
                     .unique_other = 1,
                     .bridge = 0,
                     .to_bridge = 0,
                     .contra_way = 0,
                     .cross_country = 0,
                     .inverted = 0,
                     .both_directions = 0,
                     .car_label = label,
                     .car_label_strange = 0});
   */
}

void StoreEdges(std::vector<TEdge> edges, Graph* g) {
  std::sort(edges.begin(), edges.end());
  for (const TEdge& e : edges) {
    g->nodes.at(std::get<0>(e)).edges_start_pos += 1;  // Hack: use as counter.
    g->edges.push_back({.other_node_idx = std::get<1>(e),
                        .way_idx = 0,
                        .distance_cm = std::get<2>(e),
                        .unique_other = 1,
                        .bridge = 0,
                        .to_bridge = 0,
                        .contra_way = 0,
                        .cross_country = 0,
                        .inverted = 0,
                        .both_directions = 0,
                        .car_label = std::get<3>(e),
                        .car_label_strange = 0});
  }
  uint32_t curr_pos = 0;
  for (GNode& n : g->nodes) {
    uint32_t count = n.edges_start_pos;
    n.edges_start_pos = curr_pos;
    curr_pos += count;
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
 *     [a] ----- [b] ----- [c] ----- [d] ----- [e] ----- [f] ----- [x]
 *           5r        1r        1         1         1r        1
 */
Graph CreateGraph1(bool both_dirs) {
  enum : uint32_t { A = 0, B, C, D, E, F, G, H, X };  // Node names.
  Graph g;
  const RoutingAttrs ra_entry = {.dir = 1, .access = ACC_YES, .maxspeed = 50};
  g.way_shared_attrs.push_back({.ra = ra_entry});
  g.ways.push_back({.highway_label = HW_TERTIARY,
                    .uniform_country = 1,
                    .ncc = NCC_CH,
                    .wsa_id = 0});
  AddNode(g, A);
  AddNode(g, B);
  AddNode(g, C);
  AddNode(g, D);
  AddNode(g, E);
  AddNode(g, F);
  AddNode(g, G);
  AddNode(g, H);
  AddNode(g, X);

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
  AddEdge(F, X, 1000, GEdge::LABEL_FREE, both_dirs, &edges);
  StoreEdges(edges, &g);

  return g;
}

void TestRouteRestricted() {
  enum : uint32_t { A = 0, B, C, D, E, F, G, H, X };  // Node names.
  Graph g = CreateGraph1(/*both_dirs=*/true);
  {
    EdgeRouter router(g, 3);
    auto res = router.Route(A, D, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 7000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 4);
  }
  {
    EdgeRouter router(g, 3);
    auto res = router.Route(A, E, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 8000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 5);
  }
  {
    EdgeRouter router(g, 3);
    auto res = router.Route(A, F, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 9000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 6);
  }
  {
    EdgeRouter router(g, 3);
    auto res = router.Route(A, X, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(!res.found);
  }

  {
    EdgeRouter router(g, 3);
    auto res = router.Route(D, A, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 7000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 4);
  }
  {
    EdgeRouter router(g, 3);
    auto res = router.Route(E, A, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 8000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 5);
  }
  {
    EdgeRouter router(g, 3);
    auto res = router.Route(F, A, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 9000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 6);
  }
  {
    EdgeRouter router(g, 3);
    auto res = router.Route(X, A, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(!res.found);
  }
}
int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  if (argc != 1) {
    ABORT_S() << absl::StrFormat("usage: %s", argv[0]);
  }

  TestRouteRestricted();

  LOG_S(INFO)
      << "\n\033[1;32m*****************************\nTesting successfully "
         "finished\n*****************************\033[0m";
  return 0;
}
