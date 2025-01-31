#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include "algos/compact_dijkstra.h"
#include "algos/edge_key.h"
#include "algos/edge_router.h"
#include "algos/router.h"
#include "base/country_code.h"
#include "base/util.h"
#include "graph/compact_graph.h"
#include "graph/graph_def.h"
#include "graph/routing_attrs.h"
#include "osm/osm_helpers.h"

// Create a compacted graph from 'g' which has the same node ordering and the
// same number of edges.
CompactDirectedGraph CreateCompactGraph(const Graph& g) {
  std::vector<CompactDirectedGraph::FullEdge> full_edges;
  for (uint32_t from_idx = 0; from_idx < g.nodes.size(); ++from_idx) {
    for (const GEdge& e : gnode_forward_edges(g, from_idx)) {
      full_edges.push_back({.from_c_idx = from_idx,
                            .to_c_idx = e.other_node_idx,
                            .weight = (uint32_t)e.distance_cm,
                            .restricted = (e.car_label != GEdge::LABEL_FREE)});
    }
  }
  compact_dijkstra::SortAndCleanupEdges(&full_edges);
  CompactDirectedGraph cg(g.nodes.size(), full_edges);
  CHECK_EQ_S(cg.num_nodes(), g.nodes.size());
  CHECK_EQ_S(cg.edges().size(), g.edges.size());
  return cg;
}

uint32_t ExecuteSingleSourceDijkstra(const CompactDirectedGraph& cg,
                                     uint32_t start_c_idx,
                                     uint32_t target_c_idx) {
  std::vector<compact_dijkstra::VisitedNode> vis =
      compact_dijkstra::SingleSourceDijkstra(cg, start_c_idx);
  LOG_S(INFO) << absl::StrFormat("CompactGraph route from %u to %u metric %u",
                                 start_c_idx, target_c_idx,
                                 vis.at(target_c_idx).min_weight);
  return vis.at(target_c_idx).min_weight;
}

uint32_t ExecuteSingleSourceEdgeDijkstra(const CompactDirectedGraph& cg,
                                         uint32_t start_c_idx,
                                         uint32_t target_c_idx) {
  std::vector<compact_dijkstra::VisitedEdge> vis =
      compact_dijkstra::SingleSourceEdgeDijkstra(cg, start_c_idx);
  /*
  LOG_S(INFO) << absl::StrFormat("CompactGraph route from %u to %u metric %u",
                                 start_c_idx, target_c_idx,
                                 vis.at(target_c_idx).min_weight);
  return vis.at(target_c_idx).min_weight;
  */
  return 0;
}

void TestGEdgeKey() {
  FUNC_TIMER();
  Graph g;
  g.nodes.push_back({.node_id = 100, .cluster_id = 0, .edges_start_pos = 0});
  g.nodes.push_back({.node_id = 101, .cluster_id = 0, .edges_start_pos = 2});
  g.nodes.push_back({.node_id = 102, .cluster_id = 0, .edges_start_pos = 4});
  g.nodes.push_back({.node_id = 103, .cluster_id = 0, .edges_start_pos = 4});
  g.clusters.push_back({.border_nodes = {2, 3}});

  // Test Cluster Edge.
  GEdgeKey cl_edge1 =
      GEdgeKey::CreateClusterEdge(g, /*from_idx=*/0, /*offset=*/1, 0);
  GEdgeKey cl_edge2 =
      GEdgeKey::CreateClusterEdge(g, /*from_idx=*/2, /*offset=*/1, 0);
  CHECK_EQ_S(cl_edge1.GetToIdx(g), 3);
  CHECK_EQ_S(cl_edge1.ToNode(g).node_id, 103);
  // Cluster edges hash key encodes cluster_id + offset + bits, i.e. the
  // from_idx is replaced with cluster_id. Therefore, the two edges must have
  // the same hash key.
  CHECK_EQ_S(cl_edge1.HashKey(g), cl_edge2.HashKey(g));

  // Dummy edges of node 100.
  g.edges.push_back({});
  g.edges.push_back({});
  // Edges of node 101.
  g.edges.push_back({.other_node_idx = 2});
  g.edges.push_back({.other_node_idx = 3});

  GEdgeKey a = GEdgeKey::Create(g, 1, g.edges.at(2), 1);
  GEdgeKey b = GEdgeKey::Create(g, 1, g.edges.at(3), 0);
  CHECK_EQ_S(a.GetFromIdx(), 1);
  CHECK_EQ_S(b.GetFromIdx(), 1);
  CHECK_EQ_S(a.GetOffset(), 0);
  CHECK_EQ_S(b.GetOffset(), 1);
  CHECK_EQ_S(a.GetBit(), 1);
  CHECK_EQ_S(b.GetBit(), 0);

  CHECK_EQ_S(a.GetEdge(g).other_node_idx, 2);
  CHECK_EQ_S(b.GetEdge(g).other_node_idx, 3);

  CHECK_EQ_S(a.FromNode(g).node_id, 101);
  CHECK_EQ_S(b.FromNode(g).node_id, 101);

  CHECK_EQ_S(a.ToNode(g).node_id, 102);
  CHECK_EQ_S(b.ToNode(g).node_id, 103);

  CHECK_S(a.HashKey(g) != b.HashKey(g));
  GEdgeKey c = GEdgeKey::Create(g, 1, g.edges.at(2), 1);
  CHECK_S(a.HashKey(g) == c.HashKey(g));
}

void AddCluster(Graph& g, uint32_t cluster_id, GCluster c) {
  CHECK_EQ_S(cluster_id, g.clusters.size());
  g.clusters.push_back(c);
}

void AddNode(Graph& g, uint32_t idx, uint32_t cluster_id = 0) {
  CHECK_EQ_S(idx, g.nodes.size());
  g.nodes.push_back({.node_id = 100 + idx,
                     .large_component = 1,
                     .cluster_id = cluster_id,
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
}

void StoreEdges(std::vector<TEdge> edges, Graph* g) {
  std::sort(edges.begin(), edges.end());
  for (const TEdge& e : edges) {
    const uint32_t from_idx = std::get<0>(e);
    const uint32_t to_idx = std::get<1>(e);
    g->nodes.at(from_idx).edges_start_pos += 1;  // Hack: use as counter.
    g->edges.push_back({.other_node_idx = to_idx,
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
    // If this edge crosses clusters, then mark the nodes accordingly.
    if (g->nodes.at(from_idx).cluster_id != g->nodes.at(to_idx).cluster_id) {
      g->nodes.at(from_idx).cluster_border_node = 1;
      g->nodes.at(to_idx).cluster_border_node = 1;
    }
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
Graph CreateGraphWithRestricted(bool both_dirs) {
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
  const Graph g = CreateGraphWithRestricted(/*both_dirs=*/true);
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

uint32_t ExecuteRouter(const Graph& g, uint32_t from, uint32_t to,
                       std::string_view optstr) {
  RoutingOptions opt;
  if (optstr.contains("hybrid")) {
    opt.SetHybridOptions(g, from, to);
  }
  RoutingResult res;
  if (optstr.contains("edge")) {
    EdgeRouter router(g, 0);
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

void TestRouteRestrictedHard() {
  enum : uint32_t { A = 0, B, C, D, E, F, G, H };  // Node names.
  const Graph g = CreateRestrictedGraphHard(/*both_dirs=*/true);

  // Current edge based routing doesn't allow detours into the free area if
  // start and end nodes are in the same restricted area. So currently they only
  // find the path that stays in the restricted area.
  {
    EdgeRouter router(g, 3);
    auto res = router.Route(A, F, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 9000);  // TODO: Should be 9000.
    CHECK_EQ_S(res.num_shortest_route_nodes, 7);
  }
  {
    EdgeRouter router(g, 3);
    auto res = router.Route(F, A, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 9000);  // TODO: Should be 9000.
    CHECK_EQ_S(res.num_shortest_route_nodes, 7);
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
  const RoutingAttrs ra_entry = {.dir = 1, .access = ACC_YES, .maxspeed = 50};
  g.way_shared_attrs.push_back({.ra = ra_entry});
  g.ways.push_back({.highway_label = HW_TERTIARY,
                    .uniform_country = 1,
                    .ncc = NCC_CH,
                    .wsa_id = 0});
  AddCluster(g, 0, {.num_nodes = 2, .border_nodes = {B}, .distances = {{0}}});
  AddCluster(g, 1, {.num_nodes = 2, .border_nodes = {C}, .distances = {{0}}});
  AddCluster(g, 2,
             {.num_nodes = 2,
              .border_nodes = {E, F},
              .distances = {{0, 1000}, {1000, 0}}});

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

  return g;
}

void TestClusterRoute() {
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node names.
  const Graph g = CreateClusterGraph(/*both_dirs=*/true);

  {
    EdgeRouter router(g, 3);
    auto res = router.Route(A, D, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 5000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 6);
  }
  {
    EdgeRouter router(g, 3);
    RoutingOptions opt;
    opt.SetHybridOptions(g, A, D);
    auto res = router.Route(A, D, RoutingMetricDistance(), opt);
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 5000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 6);
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
  const RoutingAttrs ra_entry = {.dir = 1, .access = ACC_YES, .maxspeed = 50};
  g.way_shared_attrs.push_back({.ra = ra_entry});
  g.ways.push_back({.highway_label = HW_TERTIARY,
                    .uniform_country = 1,
                    .ncc = NCC_CH,
                    .wsa_id = 0});
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

  return g;
}

void TestClusterRouteDoubleEdge() {
  enum : uint32_t { A = 0, B, C, D, E, F };  // Node names.
  const Graph g = CreateClusterGraphDoubleEdge(/*both_dirs=*/true);

  {
    EdgeRouter router(g, 3);
    auto res = router.Route(A, D, RoutingMetricDistance(), RoutingOptions());
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 4000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 5);
  }
  {
    CompactDirectedGraph cg = CreateCompactGraph(g);
    uint32_t m = ExecuteSingleSourceDijkstra(cg, A, D);
    CHECK_EQ_S(m, 4000);
    ExecuteSingleSourceEdgeDijkstra(cg, A, D);
  }
  {
    EdgeRouter router(g, 3);
    RoutingOptions opt;
    opt.SetHybridOptions(g, A, D);
    auto res = router.Route(A, D, RoutingMetricDistance(), opt);
    CHECK_S(res.found);
    CHECK_EQ_S(res.found_distance, 4000);
    CHECK_EQ_S(res.num_shortest_route_nodes, 5);

    // Now check the individual parts of the route...
    CHECK_EQ_S(res.route_v_idx.size(), 4);

    EdgeRouter::VisitedEdge ve = router.GetVEdge(res.route_v_idx.at(0));
    CHECK_EQ_S(ve.key.GetFromIdx(), A);
    CHECK_EQ_S(ve.key.GetToIdx(g), E);
    CHECK_S(!ve.key.IsClusterEdge());

    ve = router.GetVEdge(res.route_v_idx.at(1));
    CHECK_EQ_S(ve.key.GetFromIdx(), E);
    CHECK_EQ_S(ve.key.GetToIdx(g), F);
    CHECK_S(!ve.key.IsClusterEdge());

    ve = router.GetVEdge(res.route_v_idx.at(2));
    CHECK_EQ_S(ve.key.GetFromIdx(), F);
    CHECK_EQ_S(ve.key.GetToIdx(g), C);
    CHECK_S(ve.key.IsClusterEdge());

    ve = router.GetVEdge(res.route_v_idx.at(3));
    CHECK_EQ_S(ve.key.GetFromIdx(), C);
    CHECK_EQ_S(ve.key.GetToIdx(g), D);
    CHECK_S(!ve.key.IsClusterEdge());
  }
}

// Compare different shortest path algorithms against each other, using all
// the networks defined in this file.
void CompareShortestPaths(const Graph& g) {
  const CompactDirectedGraph cg = CreateCompactGraph(g);

  for (uint32_t start = 0; start < cg.num_nodes(); ++start) {
    std::vector<compact_dijkstra::VisitedNode> vis_nssd =
        compact_dijkstra::SingleSourceDijkstra(cg, start);
    std::vector<uint32_t> w1 =
        compact_dijkstra::GetNodeWeightsFromVisitedNodes(vis_nssd);

    std::vector<compact_dijkstra::VisitedEdge> vis_essd =
        compact_dijkstra::SingleSourceEdgeDijkstra(cg, start);
    std::vector<uint32_t> w2 =
        compact_dijkstra::GetNodeWeightsFromVisitedEdges(cg, start, vis_essd);

    CHECK_EQ_S(w1.size(), w2.size());
    for (uint32_t i = 0; i < cg.num_nodes(); ++i) {
      LOG_S(INFO) << absl::StrFormat("Route %c to %c", 65+start, 65+i);
      CHECK_EQ_S(w1.at(i), w2.at(i)) << i;
      CHECK_EQ_S(w1.at(i), ExecuteRouter(g, start, i, "")) << i;
      CHECK_EQ_S(w1.at(i), ExecuteRouter(g, start, i, "hybrid")) << i;
      // CHECK_EQ_S(w1.at(i), ExecuteRouter(g, start, i, "edge")) << i;
      // CHECK_EQ_S(w1.at(i), ExecuteRouter(g, start, i, "edge,hybrid")) << i;
    }
  }
}

void TestCompareShortestPaths() {
  {
    LOG_S(INFO) << "Test CreateGraphWithRestricted()";
    const Graph g = CreateGraphWithRestricted(/*both_dirs=*/true);
    CompareShortestPaths(g);
  }
  {
    LOG_S(INFO) << "Test CreateRestrictedGraphHard()";
    const Graph g = CreateRestrictedGraphHard(/*both_dirs=*/true);
    CompareShortestPaths(g);
  }
  {
    LOG_S(INFO) << "Test CreateClusterGraph()";
    const Graph g = CreateClusterGraph(/*both_dirs=*/true);
    CompareShortestPaths(g);
  }
  {
    LOG_S(INFO) << "Test CreateClusterGraphDoubleEdge()";
    const Graph g = CreateClusterGraphDoubleEdge(/*both_dirs=*/true);
    CompareShortestPaths(g);
  }
}

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  if (argc != 1) {
    ABORT_S() << absl::StrFormat("usage: %s", argv[0]);
  }

  TestGEdgeKey();
  TestRouteRestricted();
  TestRouteRestrictedHard();
  TestClusterRoute();
  TestClusterRouteDoubleEdge();
  TestCompareShortestPaths();

  LOG_S(INFO)
      << "\n\033[1;32m*****************************\nTesting successfully "
         "finished\n*****************************\033[0m";
  return 0;
}
