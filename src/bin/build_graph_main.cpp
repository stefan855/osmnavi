#include <osmpbf/osmpbf.h>
#include <stdarg.h>
#include <stdio.h>

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <span>
#include <vector>

#include "absl/strings/str_format.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "algos/astar.h"
#include "algos/compact_dijkstra.h"
#include "algos/dijkstra.h"
#include "base/argli.h"
#include "base/huge_bitset.h"
#include "base/thread_pool.h"
#include "base/util.h"
#include "graph/build_graph.h"
#include "graph/graph_def.h"
#include "graph/routing_attrs.h"
#include "osm/osm_helpers.h"

void PrintStructSizes() {
  LOG_S(INFO) << "----------- Struct Sizes ---------";
  LOG_S(INFO) << absl::StrFormat("Dijkstra: sizeof(VisitedNode): %4u",
                                 sizeof(DijkstraRouter::VisitedNode));
  LOG_S(INFO) << absl::StrFormat("Dijkstra: sizeof(QueuedNode):  %4u",
                                 sizeof(DijkstraRouter::QueuedNode));
  LOG_S(INFO) << absl::StrFormat("AStar: sizeof(VisitedNode):    %4u",
                                 sizeof(AStarRouter::VisitedNode));
  LOG_S(INFO) << absl::StrFormat("AStar: sizeof(QueuedNode):     %4u",
                                 sizeof(AStarRouter::QueuedNode));
  LOG_S(INFO) << absl::StrFormat("sizeof(GNode):                 %4u",
                                 sizeof(GNode));
  LOG_S(INFO) << absl::StrFormat("sizeof(GEdge):                 %4u",
                                 sizeof(GEdge));
  LOG_S(INFO) << absl::StrFormat("sizeof(GWay):                  %4u",
                                 sizeof(GWay));
  LOG_S(INFO) << absl::StrFormat("sizeof(RoutingAttrs):          %4u",
                                 sizeof(RoutingAttrs));
  LOG_S(INFO) << absl::StrFormat("sizeof(WayTaggedZones):        %4u",
                                 sizeof(build_graph::WayTaggedZones));
  LOG_S(INFO) << absl::StrFormat("sizeof(pair<int32_t,int32_t>): %4u",
                                 sizeof(std::pair<int32_t, int32_t>));
}

#if 0
std::uint32_t RandomNodeIdx(const Graph& g) {
  std::uint32_t pos = INFU32;
  do {
    pos = rand() % g.nodes.size();
  } while (!g.nodes.at(pos).large_component);
  return pos;
}

void RandomShortestPaths(const Graph& g) {
  FUNC_TIMER();
  std::srand(1);  // Get always the same pseudo-random numbers.
  ThreadPool pool;
  for (int i = 0; i < 10000; ++i) {
    uint32_t start = RandomNodeIdx(g);
    uint32_t target = RandomNodeIdx(g);
    pool.AddWork([&g, start, target](int thread_idx) {
      AStarRouter router(g);
      RoutingOptions opt;
      opt.MayFillBridgeNodeId(g, target);
      router.Route(start, target, RoutingMetricTime(), opt);
    });
  }
  pool.Start(23);
  pool.WaitAllFinished();
}
#endif

void TestRoutes(const Graph& g) {
  if (true) {
    // Usterstrasse, Pfäffikon ZH  (not a dead end)
    std::uint32_t start_idx = g.FindNodeIndex(49973500);
    // Bremgartenstrasse, Bern
    std::uint32_t target_idx = g.FindNodeIndex(805904068);
    LOG_S(INFO) << "Route from Pfäffikon ZH (49973500) to Bern (805904068)";
    if (start_idx >= g.nodes.size() || target_idx >= g.nodes.size()) {
      LOG_S(INFO) << absl::StrFormat(
          "failed to find end points of Pfäffikon/Bern start:%d target:%d",
          start_idx, target_idx);
    } else {
      {
        DijkstraRouter router(g);
        RoutingOptions opt;
        auto result =
            router.Route(start_idx, target_idx, RoutingMetricDistance(), opt);
        if (result.found) {
          router.SaveSpanningTreeSegments("/tmp/route_dist.csv");
        } else {
          LOG_S(INFO) << "failed to find dijkstra route!";
        }
      }
      {
        DijkstraRouter router(g);
        RoutingOptions opt;
        auto result =
            router.Route(start_idx, target_idx, RoutingMetricTime(), opt);
        if (result.found) {
          router.SaveSpanningTreeSegments("/tmp/route_time.csv");
        } else {
          LOG_S(INFO) << "failed to find dijkstra route!";
        }
      }
      {
        DijkstraRouter router(g);
        RoutingOptions opt;
        opt.backward_search = true;
        auto result =
            router.Route(target_idx, start_idx, RoutingMetricDistance(), opt);
        if (!result.found) {
          LOG_S(INFO) << "failed to find dijkstra route!";
        }
      }
      {
        DijkstraRouter router(g);
        RoutingOptions opt;
        opt.backward_search = true;
        auto result =
            router.Route(target_idx, start_idx, RoutingMetricTime(), opt);
        if (!result.found) {
          LOG_S(INFO) << "failed to find dijkstra route!";
        }
      }
      {
        AStarRouter router(g);
        RoutingOptions opt;
        auto result =
            router.Route(start_idx, target_idx, RoutingMetricDistance(), opt);
        if (result.found) {
          router.SaveSpanningTreeSegments("/tmp/astar_route_dist.csv");
        } else {
          LOG_S(INFO) << "failed to find astar route!";
        }
      }
      {
        AStarRouter router(g);
        RoutingOptions opt;
        auto result =
            router.Route(start_idx, target_idx, RoutingMetricTime(), opt);
        if (result.found) {
          router.SaveSpanningTreeSegments("/tmp/astar_route_time.csv");
        } else {
          LOG_S(INFO) << "failed to find astar route!";
        }
      }

      {
        AStarRouter router(g);
        RoutingOptions opt;
        opt.backward_search = true;
        auto result =
            router.Route(target_idx, start_idx, RoutingMetricDistance(), opt);
        if (!result.found) {
          LOG_S(INFO) << "failed to find astar route!";
        }
      }
      {
        AStarRouter router(g);
        RoutingOptions opt;
        opt.backward_search = true;
        auto result =
            router.Route(target_idx, start_idx, RoutingMetricTime(), opt);
        if (!result.found) {
          LOG_S(INFO) << "failed to find astar route!";
        }
      }
    }
  }
  if (true) {
    // Ackerstrasse Uster (deadend)
    std::uint32_t start_idx = g.FindNodeIndex(3108534441);
    // Mürtschenweg Weesen (deadend)
    std::uint32_t target_idx = g.FindNodeIndex(3019156898);
    LOG_S(INFO) << "=======================================================";
    LOG_S(INFO) << "Route from Uster ZH (3108534441) to Weesen (3019156898)";
    if (start_idx >= g.nodes.size() || target_idx >= g.nodes.size()) {
      LOG_S(INFO) << absl::StrFormat("failed to find end points", start_idx,
                                     target_idx);
    } else {
      {
        DijkstraRouter router(g);
        RoutingOptions opt;
        opt.MayFillBridgeNodeId(g, target_idx);
        auto result =
            router.Route(start_idx, target_idx, RoutingMetricTime(), opt);
        if (result.found) {
          // router.SaveSpanningTreeSegments("/tmp/route_time.csv");
        }
      }
      {
        DijkstraRouter router(g);
        RoutingOptions opt;
        opt.MayFillBridgeNodeId(g, start_idx);
        opt.backward_search = true;
        auto result =
            router.Route(target_idx, start_idx, RoutingMetricTime(), opt);
        if (result.found) {
          router.SaveSpanningTreeSegments("/tmp/route_time.csv");
        }
      }
      {
        AStarRouter router(g);
        RoutingOptions opt;
        opt.MayFillBridgeNodeId(g, target_idx);
        auto result =
            router.Route(start_idx, target_idx, RoutingMetricTime(), opt);
        if (result.found) {
          // router.SaveSpanningTreeSegments("/tmp/astar_route_time.csv");
        }
      }
      {
        AStarRouter router(g);
        RoutingOptions opt;
        opt.backward_search = true;
        opt.MayFillBridgeNodeId(g, start_idx);
        auto result =
            router.Route(target_idx, start_idx, RoutingMetricTime(), opt);
        if (result.found) {
          router.SaveSpanningTreeSegments("/tmp/astar_route_time.csv");
        }
      }
    }
  }
}

void WriteGraphToCSV(const Graph& g, VEHICLE vt, const std::string& filename) {
  FuncTimer timer(absl::StrFormat("Write graph to %s", filename.c_str()),
                  __FILE__, __LINE__);
  std::ofstream myfile;
  myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);

  size_t count = 0;
  std::string color;
  for (const GNode& n : g.nodes) {
    for (const GEdge& e : std::span(n.edges, n.num_edges_out)) {
      if (!e.unique_other) continue;
      const GNode& other = g.nodes.at(e.other_node_idx);
      const GWay& w = g.ways.at(e.way_idx);
      if (!RoutableForward(g, w, vt) && !RoutableBackward(g, w, vt)) {
        continue;
      }
      /*
      if (n.node_id == other.node_id) {
        LOG_S(INFO) << absl::StrFormat("Node %lld length %fm way %lld name %s",
      n.node_id, e.distance_cm / 100.0, w.id, w.streetname != nullptr ?
      w.streetname : "n/a");
      }
      */
      if (RoutableForward(g, w, vt) && RoutableBackward(g, w, vt) &&
          n.lat > other.lat) {
        // Edges that have both directions will show up twice when iterating, so
        // ignore one of the two edges for this case.
        continue;
      }
      if (!n.large_component) {
        color = "mag";
      } else {
        bool has_maxspeed =
            GetRAFromWSA(g, e.way_idx, vt,
                         e.contra_way == 0 ? DIR_FORWARD : DIR_BACKWARD)
                .maxspeed > 0;
        if (has_maxspeed) {
          color = "blue";
        } else {
          color = "black";
        }
        if (e.bridge) {
          color = "red";
        } else if (n.dead_end && other.dead_end) {
          color = "green";
        }
      }
      myfile << absl::StrFormat("line,%s,%d,%d,%d,%d\n", color.c_str(), n.lat,
                                n.lon, other.lat, other.lon);
      count++;
    }
  }
  myfile.close();
  LOG_S(INFO) << absl::StrFormat("Written %u lines", count);
}

void WriteCrossCountryEdges(const build_graph::GraphMetaData& meta,
                            const std::string& filename) {
  LOG_S(INFO) << absl::StrFormat("Write cross country_edges to %s",
                                 filename.c_str());
  std::ofstream myfile;
  myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);

  uint64_t count = 0;
  for (const GWay& way : meta.graph.ways) {
    if (way.uniform_country == 1) {
      continue;
    }
    const std::vector<size_t> node_idx =
        meta.graph.GetGWayNodeIndexes(*(meta.way_nodes_needed), way);
    for (size_t pos = 0; pos < node_idx.size() - 1; ++pos) {
      const GNode& n1 = meta.graph.nodes.at(node_idx.at(pos));
      const GNode& n2 = meta.graph.nodes.at(node_idx.at(pos + 1));
      if (n1.ncc != n2.ncc) {
        count++;
        myfile << absl::StrFormat("line,black,%d,%d,%d,%d\n", n1.lat, n1.lon,
                                  n2.lat, n2.lon);
      }
    }
  }

  myfile.close();
  LOG_S(INFO) << absl::StrFormat("Written %d lines to %s", count, filename);
}

void WriteLouvainGraph(const Graph& g, const std::string& filename) {
  LOG_S(INFO) << absl::StrFormat("Write louvain graph to %s", filename.c_str());
  std::ofstream myfile;
  myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);

  uint64_t count = 0;
  for (uint32_t node_pos = 0; node_pos < g.nodes.size(); ++node_pos) {
    const GNode& n0 = g.nodes.at(node_pos);
    if (n0.cluster_id == INVALID_CLUSTER_ID) {
      continue;
    }

    for (size_t edge_pos = 0; edge_pos < gnode_total_edges(n0); ++edge_pos) {
      const GEdge& e = n0.edges[edge_pos];
      if (e.bridge || !e.unique_other) continue;
      const GNode& n1 = g.nodes.at(e.other_node_idx);
      // Ignore half of the edges and nodes that are not in a cluster.
      if (e.other_node_idx <= node_pos || n1.cluster_id == INVALID_CLUSTER_ID) {
        continue;
      }

      std::string_view color = "dpink";  // edges between clusters
      if (n0.cluster_id == n1.cluster_id) {
        // Edge within cluster.
        constexpr int32_t kMaxColor = 17;
        static std::string_view colors[kMaxColor] = {
            "blue",   "green",  "red",     "yel",    "violet", "olive",
            "lblue",  "dgreen", "dred",    "brown",  "grey",   "gblue",
            "orange", "lgreen", "greenbl", "lbrown", "pink",
        };
        color = colors[n0.cluster_id % kMaxColor];
      }

      myfile << absl::StrFormat("line,%s,%d,%d,%d,%d\n", color, n0.lat, n0.lon,
                                n1.lat, n1.lon);
      count++;
    }
  }

  myfile.close();
  LOG_S(INFO) << absl::StrFormat("Written %d lines to %s", count, filename);
}

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  FUNC_TIMER();

  build_graph::BuildGraphOptions opt;
  Argli argli(
      argc, argv,
      {
          {.name = "pbf",
           .type = "string",
           .positional = true,
           .required = true,
           .desc = "Input OSM pbf file (such as planet file)."},

          {.name = "admin_filepattern",
           .type = "string",
           .dflt = opt.admin_filepattern,
           .desc = "Location of country boundary files."},

          {.name = "routing_config",
           .type = "string",
           .dflt = opt.routing_config,
           .desc = "Location of routing config file."},

          {.name = "merge_tiny_clusters",
           .type = "bool",
           .dflt = opt.merge_tiny_clusters ? "true" : "false",
           .desc = "Merge tiny clusters at country borders."},

          {.name = "n_threads",
           .type = "int",
           .dflt = absl::StrCat(opt.n_threads),
           .desc = " Max . number of threads to use for parallel processing."},

          {.name = "log_way_tag_stats",
           .type = "bool",
           .desc = "Collect and print the way-tag-stats found in the data, "
                   "sorted by decreasing frequency."},

          {.name = "log_turn_restrictions",
           .type = "bool",
           .desc = "Log information about turn restrictions."},

          {.name = "check_shortest_cluster_paths",
           .type = "bool",
           .desc = "Compute all cluster shortest paths a second time with A* "
                   "and compare to the results of single source Dijkstra. This "
                   "is extremely time consuming."},
      });

  // TODO: Pass vehicle types from command line.
  opt.vehicle_types = {VH_MOTOR_VEHICLE};
  opt.pbf_filename = argli.GetString("pbf");
  opt.admin_filepattern = argli.GetString("admin_filepattern");
  opt.routing_config = argli.GetString("routing_config");
  opt.merge_tiny_clusters = argli.GetBool("merge_tiny_clusters");
  opt.n_threads = argli.GetInt("n_threads");
  opt.log_turn_restrictions = argli.GetBool("log_turn_restrictions");
  opt.log_way_tag_stats = argli.GetBool("log_way_tag_stats");
  opt.check_shortest_cluster_paths =
      argli.GetBool("check_shortest_cluster_paths");

  build_graph::GraphMetaData meta = build_graph::BuildGraph(opt);

  TestRoutes(meta.graph);
  PrintStructSizes();

  WriteGraphToCSV(meta.graph, VH_MOTOR_VEHICLE, "/tmp/graph_motor_vehicle.csv");
  WriteGraphToCSV(meta.graph, VH_BICYCLE, "/tmp/graph_bicycle.csv");
  WriteLouvainGraph(meta.graph, "/tmp/louvain.csv");
  WriteCrossCountryEdges(meta, "/tmp/cross.csv");

#if 0
  // RandomShortestPaths(meta.graph);

  LOG_S(INFO)
      << "Find node closest to 46,956106, 7,423648. Node 805904068 is good";
  int64_t found_id = 0;
  int64_t min_dist = INF64;
  constexpr int64_t lat = 469561060;
  constexpr int64_t lon = 74236480;
  for (const GNode& n : meta.graph.nodes) {
    int64_t dlat = lat - n.lat;
    int64_t dlon = lon - n.lon;
    int64_t dist = dlat * dlat + dlon * dlon;
    if (dist < min_dist) {
      min_dist = dist;
      found_id = n.node_id;
    }
  }
  LOG_S(INFO) << "found node " << found_id << " square-dist " << min_dist;
#endif

  LOG_S(INFO) << "Finished.";
  return 0;
}
