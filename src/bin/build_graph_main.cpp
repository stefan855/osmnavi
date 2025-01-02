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
#include "algos/compact_dijkstra.h"
#include "algos/router.h"
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
                                 sizeof(Router::VisitedNode));
  LOG_S(INFO) << absl::StrFormat("Dijkstra: sizeof(QueuedNode):  %4u",
                                 sizeof(Router::QueuedNode));
  LOG_S(INFO) << absl::StrFormat("sizeof(GNode):                 %4u",
                                 sizeof(GNode));
  LOG_S(INFO) << absl::StrFormat("sizeof(GEdge):                 %4u",
                                 sizeof(GEdge));
  // LOG_S(INFO) << absl::StrFormat("sizeof(GEdgeKey):              %4u",
  //                                sizeof(GEdgeKey));
  LOG_S(INFO) << absl::StrFormat("sizeof(GWay):                  %4u",
                                 sizeof(GWay));
  LOG_S(INFO) << absl::StrFormat("sizeof(RoutingAttrs):          %4u",
                                 sizeof(RoutingAttrs));
  LOG_S(INFO) << absl::StrFormat("sizeof(WayTaggedZones):        %4u",
                                 sizeof(build_graph::WayTaggedZones));
  LOG_S(INFO) << absl::StrFormat("sizeof(pair<int32_t,int32_t>): %4u",
                                 sizeof(std::pair<int32_t, int32_t>));
}

void DoOneRoute(const Graph& g, std::uint32_t start_idx,
                std::uint32_t target_idx, bool astar,
                const RoutingMetric& metric, bool backward, bool hybrid,
                std::string_view csv_prefix) {
  RoutingOptions opt;
  opt.use_astar_heuristic = astar;
  if (backward) {
    std::swap(start_idx, target_idx);
    opt.backward_search = true;
  }
  if (hybrid) {
    opt.SetHybridOptions(g, start_idx, target_idx);
  } else {
    opt.MayFillBridgeNodeId(g, target_idx);
  }

  const absl::Time start = absl::Now();
  Router router(g, 0);
  auto res = router.Route(start_idx, target_idx, metric, opt);
  const double elapsed = ToDoubleSeconds(absl::Now() - start);

  if (!csv_prefix.empty()) {
    router.SaveSpanningTreeSegments(absl::StrFormat(
        "/tmp/%s_%s_%s%s.csv", csv_prefix, router.AlgoName(opt).substr(0, 5),
        backward ? "backward" : "forward", hybrid ? "_hybrid" : ""));
  }

  LOG_S(INFO) << absl::StrFormat(
      "Metric:%u %s secs:%6.3f #nodes:%5u visits:%10u %s", res.found_distance,
      res.found ? "SUC" : "ERR", elapsed, res.num_shortest_route_nodes,
      res.num_visited_nodes, router.Name(metric, opt));
}

void TestRoute(const Graph& g, int64_t start_node_id, int64_t target_node_id,
               const std::string& start_name, const std::string& target_name,
               std::string_view csv_prefix = "") {
  std::uint32_t start_idx = g.FindNodeIndex(start_node_id);
  std::uint32_t target_idx = g.FindNodeIndex(target_node_id);
  LOG_S(INFO) << absl::StrFormat("**** Route from %s(%u) to %s(%u)", start_name,
                                 start_node_id, target_name, target_node_id);
  if (start_idx >= g.nodes.size() || target_idx >= g.nodes.size()) {
    LOG_S(INFO) << "Can't find endpoints";
    return;
  }

  DoOneRoute(g, start_idx, target_idx, /*astar=*/false, RoutingMetricTime(),
             /*backward=*/false, /*hybrid=*/false, csv_prefix);
  DoOneRoute(g, start_idx, target_idx, /*astar=*/false, RoutingMetricTime(),
             /*backward=*/true, /*hybrid=*/false, csv_prefix);
  DoOneRoute(g, start_idx, target_idx, /*astar=*/false, RoutingMetricTime(),
             /*backward=*/false, /*hybrid=*/true, csv_prefix);

  DoOneRoute(g, start_idx, target_idx, /*astar=*/true, RoutingMetricTime(),
             /*backward=*/false, /*hybrid=*/false, csv_prefix);
  DoOneRoute(g, start_idx, target_idx, /*astar=*/true, RoutingMetricTime(),
             /*backward=*/true, /*hybrid=*/false, csv_prefix);
  DoOneRoute(g, start_idx, target_idx, /*astar=*/true, RoutingMetricTime(),
             /*backward=*/false, /*hybrid=*/true, csv_prefix);
}

void WriteGraphToCSV(const Graph& g, VEHICLE vt, const std::string& filename) {
  FuncTimer timer(absl::StrFormat("Write graph to %s", filename.c_str()),
                  __FILE__, __LINE__);
  std::ofstream myfile;
  myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);

  size_t count = 0;
  std::string color;
  for (uint32_t node_idx = 0; node_idx < g.nodes.size(); ++node_idx) {
    const GNode& n = g.nodes.at(node_idx);
    // for (const GNode& n : g.nodes) {
    for (const GEdge& e : gnode_forward_edges(g, node_idx)) {
      // for (const GEdge& e : std::span(n.edges, n.num_edges_out)) {
      if (!e.unique_other) continue;
      const GNode& other = g.nodes.at(e.other_node_idx);
      const GWay& w = g.ways.at(e.way_idx);
      if (!RoutableForward(g, w, vt) && !RoutableBackward(g, w, vt)) {
        continue;
      }
      /*
      if (n.node_id == other.node_id) {
        LOG_S(INFO) << absl::StrFormat("Node %lld length %fm way %lld name
      %s", n.node_id, e.distance_cm / 100.0, w.id, w.streetname != nullptr ?
      w.streetname : "n/a");
      }
      */
      if (RoutableForward(g, w, vt) && RoutableBackward(g, w, vt) &&
          n.lat > other.lat) {
        // Edges that have both directions will show up twice when iterating,
        // so ignore one of the two edges for this case.
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

void WriteRestrictedRoadsToCSV(const Graph& g, VEHICLE vt,
                               const std::string& filename) {
  FuncTimer timer(
      absl::StrFormat("Write restricted roads to %s", filename.c_str()),
      __FILE__, __LINE__);
  std::ofstream myfile;
  myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);

  size_t count = 0;
  std::string color;
  for (uint32_t node_idx = 0; node_idx < g.nodes.size(); ++node_idx) {
    const GNode& n = g.nodes.at(node_idx);
    // for (const GNode& n : g.nodes) {
    for (const GEdge& e : gnode_forward_edges(g, node_idx)) {
      // for (const GEdge& e : std::span(n.edges, n.num_edges_out)) {
      if (!e.unique_other) continue;
      const GWay& w = g.ways.at(e.way_idx);
      const auto& wsa = GetWSA(g, w);
      const auto& ra_forw = GetRAFromWSA(wsa, vt, EDGE_DIR(e));
      const auto& ra_backw = GetRAFromWSA(wsa, vt, EDGE_INVERSE_DIR(e));

      const bool restricted =
          RestrictedAccess(ra_forw.access) || RestrictedAccess(ra_backw.access);
      const bool service = (w.highway_label == HW_SERVICE);
      const bool residential = (w.highway_label == HW_RESIDENTIAL);
      const bool unclassified = (w.highway_label == HW_UNCLASSIFIED);

      if (!restricted && !service && !residential && !unclassified) {
        continue;
      }

      if (restricted) {
        color = "green";
      } else if (service) {
        color = "pink";
      } else if (residential) {
        color = "dpink";
      } else if (unclassified) {
        color = "violet";
      } else {
        CHECK_S(false);
      }
      const GNode& other = g.nodes.at(e.other_node_idx);
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

    for (const GEdge& e : gnode_all_edges(g, node_pos)) {
      // for (size_t edge_pos = 0; edge_pos < gnode_total_edges(n0); ++edge_pos)
      // { const GEdge& e = n0.edges[edge_pos];
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
          {.name = "keep_all_nodes",
           .type = "bool",
           .desc = "Keep all nodes instead of pruning nodes that are needed "
                   "for geometry only."},
      });

  // TODO: Pass vehicle types from command line.
  opt.vehicle_types = {VH_MOTOR_VEHICLE};
  opt.pbf = argli.GetString("pbf");
  opt.admin_filepattern = argli.GetString("admin_filepattern");
  opt.routing_config = argli.GetString("routing_config");
  opt.merge_tiny_clusters = argli.GetBool("merge_tiny_clusters");
  opt.n_threads = argli.GetInt("n_threads");
  opt.log_turn_restrictions = argli.GetBool("log_turn_restrictions");
  opt.log_way_tag_stats = argli.GetBool("log_way_tag_stats");
  opt.check_shortest_cluster_paths =
      argli.GetBool("check_shortest_cluster_paths");
  opt.keep_all_nodes = argli.GetBool("keep_all_nodes");

  build_graph::GraphMetaData meta = build_graph::BuildGraph(opt);
  const Graph& g = meta.graph;

  PrintStructSizes();

  WriteGraphToCSV(g, VH_MOTOR_VEHICLE, "/tmp/graph_motor_vehicle.csv");
  WriteGraphToCSV(g, VH_BICYCLE, "/tmp/graph_bicycle.csv");
  WriteLouvainGraph(g, "/tmp/louvain.csv");
  WriteCrossCountryEdges(meta, "/tmp/cross.csv");
  WriteRestrictedRoadsToCSV(g, VH_MOTOR_VEHICLE, "/tmp/experimental1.csv");

  // TestRoutes(g);
  TestRoute(g, 49973500, 805904068, "Pfäffikon ZH", "Bern",
            /*csv_prefix=*/"pb");
  TestRoute(g, 805904068, 49973500, "Bern", "Pfäffikon ZH",
            /*csv_prefix=*/"bp");

  TestRoute(g, 3108534441, 3019156898, "Uster ZH", "Weesen",
            /*csv_prefix=*/"uw");
  TestRoute(g, 3019156898, 3108534441, "Weesen", "Uster ZH",
            /*csv_prefix=*/"wu");

  TestRoute(g, 26895904, 300327675, "Augsburg", "Stralsund",
            /*csv_prefix=*/"as");
  TestRoute(g, 300327675, 26895904, "Stralsund", "Augsburg",
            /*csv_prefix=*/"sa");

  TestRoute(g, 1131001345, 899297768, "Lissabon", "Nordkapp-Norwegen",
            /*csv_prefix=*/"ln");
  TestRoute(g, 899297768, 1131001345, "Nordkapp-Norwegen", "Lissabon",
            /*csv_prefix=*/"");

  LOG_S(INFO) << "Finished.";
  return 0;
}
