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
#include "algos/edge_router.h"
#include "algos/edge_router2.h"
#include "algos/router.h"
#include "base/argli.h"
#include "base/huge_bitset.h"
#include "base/thread_pool.h"
#include "base/util.h"
#include "geometry/closest_node.h"
#include "graph/build_graph.h"
#include "graph/graph_def.h"
#include "graph/routing_attrs.h"
#include "osm/osm_helpers.h"

void PrintDebugInfoForNode(const Graph& g, int64_t node_id) {
  LOG_S(INFO) << "\n ******************** Data for node " << node_id;
  for (const auto& [key, data] : g.simple_turn_restriction_map) {
    if (GetGNodeIdSafe(g, key.from_node_idx) == node_id ||
        GetGNodeIdSafe(g, key.to_node_idx) == node_id) {
      LOG_S(INFO) << absl::StrFormat("STR key:(%lld -> %lld  way:%lld)",
                                     GetGNodeIdSafe(g, key.from_node_idx),
                                     GetGNodeIdSafe(g, key.to_node_idx),
                                     GetGWayIdSafe(g, key.way_idx));
      const GNode& n = g.nodes.at(key.to_node_idx);
      for (uint32_t offset = 0;
           offset < gnode_num_all_edges(g, key.to_node_idx); ++offset) {
        if (data.allowed_edge_bits & (1u << offset)) {
          const GEdge& e = g.edges.at(n.edges_start_pos + offset);
          LOG_S(INFO) << absl::StrFormat(
              "  SELECTED EDGE %lld -> %lld way:%lld %s", n.node_id,
              GetGNodeIdSafe(g, e.target_idx), GetGWayIdSafe(g, e.way_idx),
              e.inverted ? "inv" : "out");
        }
      }
    }
  }
}

void PrintStructSizes() {
  LOG_S(INFO) << "----------- Struct Sizes ---------";
  LOG_S(INFO) << absl::StrFormat("Dijkstra: sizeof(VisitedNode):  %4u",
                                 sizeof(Router::VisitedNode));
  LOG_S(INFO) << absl::StrFormat("Dijkstra: sizeof(QueuedNode):   %4u",
                                 sizeof(Router::QueuedNode));
  LOG_S(INFO) << absl::StrFormat("sizeof(GNode):                  %4u",
                                 sizeof(GNode));
  LOG_S(INFO) << absl::StrFormat("sizeof(GEdge):                  %4u",
                                 sizeof(GEdge));
  LOG_S(INFO) << absl::StrFormat("sizeof(GEdgeKey):               %4u",
                                 sizeof(GEdgeKey));
  LOG_S(INFO) << absl::StrFormat("sizeof(EdgeRouter::VisitedEdge):%4u",
                                 sizeof(EdgeRouter::VisitedEdge));
  LOG_S(INFO) << absl::StrFormat("sizeof(GWay):                   %4u",
                                 sizeof(GWay));
  LOG_S(INFO) << absl::StrFormat("sizeof(NodeTags):               %4u",
                                 sizeof(NodeTags));
  LOG_S(INFO) << absl::StrFormat("sizeof(FullEdge):               %4u",
                                 sizeof(FullEdge));
  LOG_S(INFO) << absl::StrFormat("sizeof(RoutingAttrs):           %4u",
                                 sizeof(RoutingAttrs));
  LOG_S(INFO) << absl::StrFormat("sizeof(WayTaggedZones):         %4u",
                                 sizeof(build_graph::WayTaggedZones));
  LOG_S(INFO) << absl::StrFormat("sizeof(pair<int32_t,int32_t>):  %4u",
                                 sizeof(std::pair<int32_t, int32_t>));
  LOG_S(INFO) << absl::StrFormat("sizeof(CTRPosition):            %4u",
                                 sizeof(CTRPosition));
  LOG_S(INFO) << absl::StrFormat("sizeof(TurnCostData):           %4u",
                                 sizeof(TurnCostData));
  LOG_S(INFO) << absl::StrFormat("sizeof(KeySet):                 %4u",
                                 sizeof(KeySet));
}

template <typename RouterT>
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
  RouterT router(g, 0);
  auto res = router.Route(start_idx, target_idx, metric, opt);
  const double elapsed = ToDoubleSeconds(absl::Now() - start);

  if (!csv_prefix.empty()) {
    router.SaveSpanningTreeSegments(absl::StrFormat(
        "/tmp/%s_%s_%s%s.csv", csv_prefix, router.AlgoName(opt).substr(0, 5),
        backward ? "backward" : "forward", hybrid ? "_hybrid" : ""));
  }

  uint32_t num_len_gt_1 = 0;
  uint32_t max_len = 0;
  if constexpr (std::is_same_v<RouterT, EdgeRouter> ||
                std::is_same_v<RouterT, EdgeRouter2>) {
    router.TurnRestrictionSpecialStats(&num_len_gt_1, &max_len);
  }

  LOG_S(INFO) << absl::StrFormat(
      "Metr:%u %s secs:%6.3f #n:%5u vis:%9u cTR:%4u(%3.0f%% max:%u lgt1:%u) "
      "%s",
      res.found_distance, res.found ? "SUC" : "ERR", elapsed,
      res.num_shortest_route_nodes, res.num_visited,
      res.num_complex_turn_restriction_keys,
      res.complex_turn_restriction_keys_reduction_factor * 100.0, max_len,
      num_len_gt_1, router.Name(metric, opt));
}

void DoCompactRoute(const CompactDijkstraRoutingData& comp_data,
                    uint32_t g_start, uint32_t g_target,
                    std::string_view csv_prefix) {
  const absl::Time start = absl::Now();
  auto res =
      RouteOnCompactGraph(comp_data, g_start, g_target, Verbosity::Quiet);
  const double elapsed = ToDoubleSeconds(absl::Now() - start);

  /*
  if (!csv_prefix.empty()) {
    router.SaveSpanningTreeSegments(absl::StrFormat(
        "/tmp/%s_%s_%s%s.csv", csv_prefix, router.AlgoName(opt).substr(0, 5),
        backward ? "backward" : "forward", hybrid ? "_hybrid" : ""));
  }
  */

  uint32_t num_len_gt_1 = 0;
  uint32_t max_len = 0;

  LOG_S(INFO) << absl::StrFormat(
      "Metr:%u %s secs:%6.3f #n:%5u vis:%9u cTR:%4u(%3.0f%% max:%u lgt1:%u) "
      "%s",
      res.found_distance, res.found ? "SUC" : "ERR", elapsed,
      res.num_shortest_route_nodes, res.num_visited,
      res.num_complex_turn_restriction_keys,
      res.complex_turn_restriction_keys_reduction_factor * 100.0, max_len,
      num_len_gt_1, "compact edge dijkstra ");
}

void TestRoute(const Graph& g, const CompactDijkstraRoutingData& comp_data,
               int64_t start_node_id, int64_t target_node_id,
               std::string_view start_name, std::string_view target_name,
               std::string_view csv_prefix = "") {
  std::uint32_t start_idx = g.FindNodeIndex(start_node_id);
  std::uint32_t target_idx = g.FindNodeIndex(target_node_id);
  LOG_S(INFO) << absl::StrFormat(
      "\n********************** Route from %s(%u) to %s(%u)", start_name,
      start_node_id, target_name, target_node_id);
  if (start_idx >= g.nodes.size() || target_idx >= g.nodes.size()) {
    LOG_S(INFO) << "Can't find endpoints";
    return;
  }

  if (!g.nodes.at(start_idx).large_component ||
      !g.nodes.at(target_idx).large_component) {
    LOG_S(INFO) << "Start- or endpoint is not in large component. Can't route.";
    return;
  }

  DoOneRoute<Router>(g, start_idx, target_idx, /*astar=*/false,
                     RoutingMetricTime(),
                     /*backward=*/false, /*hybrid=*/false, csv_prefix);
  DoOneRoute<EdgeRouter>(g, start_idx, target_idx, /*astar=*/false,
                         RoutingMetricTime(),
                         /*backward=*/false, /*hybrid=*/false, csv_prefix);
  DoOneRoute<EdgeRouter2>(g, start_idx, target_idx, /*astar=*/false,
                          RoutingMetricTime(),
                          /*backward=*/false, /*hybrid=*/false, csv_prefix);

  DoCompactRoute(comp_data, start_idx, target_idx, csv_prefix);
  // RouteOnCompactGraph(comp_data, start_idx, target_idx, Verbosity::Brief);

  DoOneRoute<Router>(g, start_idx, target_idx, /*astar=*/false,
                     RoutingMetricTime(),
                     /*backward=*/true, /*hybrid=*/false, csv_prefix);
  DoOneRoute<Router>(g, start_idx, target_idx, /*astar=*/false,
                     RoutingMetricTime(),
                     /*backward=*/false, /*hybrid=*/true, csv_prefix);

  DoOneRoute<EdgeRouter>(g, start_idx, target_idx, /*astar=*/false,
                         RoutingMetricTime(),
                         /*backward=*/false, /*hybrid=*/true, csv_prefix);

  DoOneRoute<Router>(g, start_idx, target_idx, /*astar=*/true,
                     RoutingMetricTime(),
                     /*backward=*/false, /*hybrid=*/false, csv_prefix);
  DoOneRoute<EdgeRouter>(g, start_idx, target_idx, /*astar=*/true,
                         RoutingMetricTime(),
                         /*backward=*/false, /*hybrid=*/false, csv_prefix);

  DoOneRoute<EdgeRouter2>(g, start_idx, target_idx, /*astar=*/true,
                          RoutingMetricTime(),
                          /*backward=*/false, /*hybrid=*/false, csv_prefix);

  DoOneRoute<Router>(g, start_idx, target_idx, /*astar=*/true,
                     RoutingMetricTime(),
                     /*backward=*/true, /*hybrid=*/false, csv_prefix);
  DoOneRoute<Router>(g, start_idx, target_idx, /*astar=*/true,
                     RoutingMetricTime(),
                     /*backward=*/false, /*hybrid=*/true, csv_prefix);

  DoOneRoute<EdgeRouter>(g, start_idx, target_idx, /*astar=*/true,
                         RoutingMetricTime(),
                         /*backward=*/false, /*hybrid=*/true, csv_prefix);
}

#if 0
void TestRouteLatLong(const Graph& g, double start_lat, double start_lon,
                      double target_lat, double target_lon,
                      std::string_view csv_prefix = "") {
  ClosestNodeResult start =
      FindClosestNodeSlow(g, std::llround(TEN_POW_7 * start_lat),
                          std::llround(TEN_POW_7 * start_lon));
  ClosestNodeResult target =
      FindClosestNodeSlow(g, std::llround(TEN_POW_7 * target_lat),
                          std::llround(TEN_POW_7 * target_lon));

  if (start.dist > 500 * 100 || target.dist > 500 * 100) {
    LOG_S(INFO) << absl::StrFormat(
        "Start-node %lld: %.2fm and/or end-node: %lld: %.2fm too far away",
        g.nodes.at(start.node_pos).node_id, start.dist / 100.0,
        g.nodes.at(target.node_pos).node_id, target.dist / 100.0);
    return;
  }

  const GNode& sn = g.nodes.at(start.node_pos);
  const GNode& tn = g.nodes.at(target.node_pos);

  TestRoute(g, sn.node_id, tn.node_id, "start=", "target=", csv_prefix);
}
#endif

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
      if (!e.unique_target) continue;
      const GNode& other = g.nodes.at(e.target_idx);
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

// Write edges with a specific label to file.
void WriteLabeledEdges(const Graph& g, GEdge::RESTRICTION label, bool strange,
                       const std::string& color, const std::string& filename) {
  FuncTimer timer(
      absl::StrFormat("Write labeled edges to %s", filename.c_str()), __FILE__,
      __LINE__);
  std::ofstream myfile;
  myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);

  size_t count = 0;
  for (uint32_t node_idx = 0; node_idx < g.nodes.size(); ++node_idx) {
    const GNode& n = g.nodes.at(node_idx);
    for (const GEdge& e : gnode_forward_edges(g, node_idx)) {
      if (label != GEdge::LABEL_UNSET && e.car_label != label) continue;
      if (e.car_label_strange != strange) continue;

      const GNode& other = g.nodes.at(e.target_idx);
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
      if (!e.unique_target) continue;
      const GWay& w = g.ways.at(e.way_idx);
      const auto& wsa = GetWSA(g, w);
      const auto& ra_forw = GetRAFromWSA(wsa, vt, EDGE_DIR(e));
      const auto& ra_backw = GetRAFromWSA(wsa, vt, EDGE_INVERSE_DIR(e));

      const bool restricted =
          RestrictedAccess(ra_forw.access) || RestrictedAccess(ra_backw.access);
      const bool strange = e.car_label_strange;
      const bool service = (w.highway_label == HW_SERVICE);
      const bool residential = (w.highway_label == HW_RESIDENTIAL);
      const bool unclassified = (w.highway_label == HW_UNCLASSIFIED);

      if (!restricted && !strange && !service && !residential &&
          !unclassified) {
        continue;
      }

      if (restricted) {
        color = "green";
      } else if (strange) {
        color = "black";
      } else if (service) {
        color = "pink";
      } else if (residential) {
        color = "dpink";
      } else if (unclassified) {
        color = "violet";
      } else {
        CHECK_S(false);
      }
      const GNode& other = g.nodes.at(e.target_idx);
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
    const std::vector<uint32_t> node_idx =
        meta.graph.GetGWayNodeIndexes(*(meta.way_nodes_needed), way);
    for (size_t pos = 0; pos < node_idx.size() - 1; ++pos) {
      const GNode& n1 = meta.graph.nodes.at(node_idx.at(pos));
      const GNode& n2 = meta.graph.nodes.at(node_idx.at(pos + 1));
      if (n1.ncc != n2.ncc) {
        count++;
        myfile << absl::StrFormat("line,black,%d,%d,%d,%d\n", n1.lat, n1.lon,
                                  n2.lat, n2.lon);
        // LOG_S(INFO) << absl::StrFormat("cross country %lld -> %lld",
        //                                n1.node_id, n2.node_id);
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
      if (e.bridge || !e.unique_target) continue;
      const GNode& n1 = g.nodes.at(e.target_idx);
      // Ignore half of the edges and nodes that are not in a cluster.
      if (e.target_idx <= node_pos || n1.cluster_id == INVALID_CLUSTER_ID) {
        continue;
      }

      std::string_view color = "dpink";  // edges between clusters
      if (n0.cluster_id == n1.cluster_id) {
        // Edge within cluster.
        constexpr int32_t kMaxColor = 17;
        static std::string_view colors[kMaxColor] = {
            "blue",     //
            "dgreen",   //
            "orange",   //
            "yel",      //
            "brown",    //
            "violet",   //
            "lblue",    //
            "dred",     //
            "green",    //
            "red",      //
            "grey",     //
            "gblue",    //
            "lgreen",   //
            "greenbl",  //
            "lbrown",   //
            "pink",     //
            "olive",    //
        };
        color = colors[g.clusters.at(n0.cluster_id).color_no % kMaxColor];
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
      {{.name = "pbf",
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

       {.name = "left_traffic_config",
        .type = "string",
        .dflt = opt.left_traffic_config,
        .desc = "Location of left traffic countries config file."},

       {.name = "align_clusters_to_ncc",
        .type = "bool",
        .dflt = opt.align_clusters_to_ncc ? "true" : "false",
        .desc = "Align cluster borders and country border. If set to false "
                "then merge_tiny_clusters is set to false too."},

       {.name = "merge_tiny_clusters",
        .type = "bool",
        .dflt = opt.merge_tiny_clusters ? "true" : "false",
        .desc = "Merge tiny clusters at country borders."},

       {.name = "n_threads",
        .type = "int",
        .dflt = "16",
        .desc = "Max . number of threads to use for parallel processing."},

       {.name = "log_way_tag_stats",
        .type = "bool",
        .desc = "Collect and print the way-tag-stats found in the data, "
                "sorted by decreasing frequency."},

       {.name = "verb_turn_restrictions",
        .type = "string",
        .dflt = "brief",
        .desc = "Verbosity level for turn restrictions reading"},

       {.name = "check_shortest_cluster_paths",
        .type = "bool",
        .desc = "Compute all cluster shortest paths a second time with A* "
                "and compare to the results of single source Dijkstra. This "
                "is extremely time consuming."},

       {.name = "keep_all_nodes",
        .type = "bool",
        .desc = "Keep all nodes instead of pruning nodes that are needed "
                "for geometry only."},

       {.name = "do_routes",
        .type = "bool",
        .desc = "Run a few test routes and output data."},

       {.name = "debug_node",
        .type = "int",
        .desc = "Print debug information for this node_id."}});

  // TODO: Pass vehicle type from command line.
  opt.vt = VH_MOTORCAR;
  opt.pbf = argli.GetString("pbf");
  opt.admin_filepattern = argli.GetString("admin_filepattern");
  opt.routing_config = argli.GetString("routing_config");
  opt.left_traffic_config = argli.GetString("left_traffic_config");
  opt.align_clusters_to_ncc = argli.GetBool("align_clusters_to_ncc");
  opt.merge_tiny_clusters =
      opt.align_clusters_to_ncc && argli.GetBool("merge_tiny_clusters");
  opt.n_threads = argli.GetInt("n_threads");
  opt.verb_turn_restrictions =
      ParseVerbosityFlag(argli.GetString("verb_turn_restrictions"));
  opt.log_way_tag_stats = argli.GetBool("log_way_tag_stats");
  opt.check_shortest_cluster_paths =
      argli.GetBool("check_shortest_cluster_paths");
  opt.keep_all_nodes = argli.GetBool("keep_all_nodes");

  build_graph::GraphMetaData meta = build_graph::BuildGraph(opt);
  const Graph& g = meta.graph;

  PrintStructSizes();

  {
    // Write files in parallel.
    ThreadPool pool;
    pool.AddWork([&g](int thread_idx) {
      WriteGraphToCSV(g, VH_MOTORCAR, "/tmp/graph_motorcar.csv");
    });

    pool.AddWork([&g](int thread_idx) {
      WriteGraphToCSV(g, VH_BICYCLE, "/tmp/graph_bicycle.csv");
    });
    pool.AddWork(
        [&g](int thread_idx) { WriteLouvainGraph(g, "/tmp/louvain.csv"); });
    pool.AddWork([&meta](int thread_idx) {
      WriteCrossCountryEdges(meta, "/tmp/cross.csv");
    });
    pool.AddWork([&g](int thread_idx) {
      WriteRestrictedRoadsToCSV(g, VH_MOTORCAR, "/tmp/experimental1.csv");
    });
    pool.AddWork([&g](int thread_idx) {
      WriteLabeledEdges(g, GEdge::LABEL_RESTRICTED, false, "mag",
                        "/tmp/experimental2.csv");
    });
    pool.AddWork([&g](int thread_idx) {
      WriteLabeledEdges(g, GEdge::LABEL_RESTRICTED_SECONDARY, false, "red",
                        "/tmp/experimental3.csv");
    });
    pool.AddWork([&g](int thread_idx) {
      WriteLabeledEdges(g, GEdge::LABEL_UNSET, true, "black",
                        "/tmp/experimental4.csv");
    });
    pool.Start(std::min(meta.opt.n_threads, std::min(8, opt.n_threads)));
    pool.WaitAllFinished();
#if 0
    WriteGraphToCSV(g, VH_MOTORCAR, "/tmp/graph_motorcar.csv");
    WriteGraphToCSV(g, VH_BICYCLE, "/tmp/graph_bicycle.csv");
    WriteLouvainGraph(g, "/tmp/louvain.csv");
    WriteCrossCountryEdges(meta, "/tmp/cross.csv");
    WriteRestrictedRoadsToCSV(g, VH_MOTORCAR, "/tmp/experimental1.csv");
    WriteLabeledEdges(g, GEdge::LABEL_RESTRICTED, false, "mag",
                      "/tmp/experimental2.csv");
    WriteLabeledEdges(g, GEdge::LABEL_RESTRICTED_SECONDARY, false, "red",
                      "/tmp/experimental3.csv");
    WriteLabeledEdges(g, GEdge::LABEL_UNSET, true, "black",
                      "/tmp/experimental4.csv");
#endif
  }

  if (argli.GetBool("do_routes")) {
    struct RoutingDef {
      int64_t start_id;
      int64_t target_id;
      std::string_view from_name;
      std::string_view to_name;
      std::string_view file_prefix;
    };

    std::vector<RoutingDef> defs = {
        {49973500, 805904068, "Pfäffikon ZH", "Bern", "pb"},
        {805904068, 49973500, "Bern", "Pfäffikon ZH", "bp"},
        {3108534441, 3019156898, "Uster", "Weesen", "uw"},
        {3019156898, 3108534441, "Weesen", "Uster", "wu"},
        {26895904, 300327675, "Augsburg", "Stralsund", "as"},
        {300327675, 26895904, "Stralsund", "Augsburg", "sa"},
        {1131001345, 899297768, "Lissabon", "Nordkapp", "ln"},
        {899297768, 1131001345, "Nordkapp", "Lissabon", ""},
        {654083753, 3582774151, "Ulisbach SG", "Ricken SG", "ur"},
        {32511837, 3582774151, "Ulisbach restricted SG", "Ricken SG", "ur2"},
    };
    std::vector<uint32_t> routing_nodes;
    for (const auto& def : defs) {
      std::uint32_t idx = g.FindNodeIndex(def.start_id);
      if (idx < g.nodes.size()) routing_nodes.push_back(idx);
      idx = g.FindNodeIndex(def.target_id);
      if (idx < g.nodes.size()) routing_nodes.push_back(idx);
    }
    if (routing_nodes.size() >= 2) {
      const CompactDijkstraRoutingData comp_data =
          CreateCompactDijkstraRoutingData(
              g, routing_nodes, RoutingMetricTime(), RoutingOptions());

      for (const auto& def : defs) {
        TestRoute(g, comp_data, def.start_id, def.target_id, def.from_name,
                  def.to_name, def.file_prefix);
      }
    }
  }

  LOG_S(INFO) << "Finished.";
  if (argli.ArgIsSet("debug_node")) {
    PrintDebugInfoForNode(meta.graph, argli.GetInt("debug_node"));
  }
  return 0;
}
