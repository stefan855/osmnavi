#include <osmpbf/osmpbf.h>

#include <filesystem>
#include <map>

#include "absl/strings/str_format.h"
#include "algos/compact_dijkstra.h"
#include "algos/routing_metric.h"
#include "base/argli.h"
#include "base/thread_pool.h"
#include "base/util.h"
#include "geometry/closest_node.h"
#include "geometry/distance.h"
#include "graph/build_graph.h"
#include "osm/poi.h"
#include "osm/read_osm_pbf.h"

namespace {

struct GraphData {
  const Graph& g;
  const CompactDirectedGraph& cg;
  const absl::flat_hash_map<uint32_t, uint32_t>& graph_to_compact_nodemap;
  const std::vector<std::uint32_t>& compact_to_graph_nodemap;
};

// For each POI, find the closest node in the road network.
void MatchPOIsToRoads(const Graph& g, int n_threads, bool check_slow,
                      std::vector<pois::POI>* pois) {
  FUNC_TIMER();
  const size_t StepSize = check_slow ? 500 : 5000;
  const auto idx = SortNodeIndexesByLon(g);
  ThreadPool pool;
  for (size_t start = 0; start < pois->size(); start += StepSize) {
    size_t len = std::min(StepSize, pois->size() - start);
    pool.AddWork([&g, &idx, check_slow, pois, start, len](int thread_idx) {
      LOG_S(INFO) << absl::StrFormat(
          "Locate navigation nodes for POIs [%u..%u) of total %u", start,
          start + len, pois->size());
      for (size_t i = start; i < start + len; ++i) {
        pois::POI& poi = pois->at(i);
        auto res = FindClosestNodeFast(g, idx, poi.lat, poi.lon);
        poi.routing_node_idx = res.node_pos;
        poi.routing_node_dist = res.dist;

        if (check_slow) {
          auto res2 = FindClosestNodeSlow(g, poi.lat, poi.lon);
          if (res.dist != res2.dist) {
            const GNode& fast = g.nodes.at(res.node_pos);
            const GNode& slow = g.nodes.at(res2.node_pos);
            LOG_S(INFO) << absl::StrFormat(
                "%d DIFF for POI %c %d lat:%d lon:%d fast id:%d dist:%d slow "
                "id:%d dist:%d",
                i, poi.obj_type, poi.id, poi.lat, poi.lon, fast.node_id,
                res.dist, slow.node_id, res2.dist);
          }
        }
      }
    });
  }
  pool.Start(n_threads);
  pool.WaitAllFinished();
}

const GEdge* FindEdgeBetweenNodes(const Graph& g, const GNode& n1,
                                  uint32_t n2_idx, VEHICLE vt) {
  uint32_t best_maxspeed = 0;
  const GEdge* best_edge = nullptr;

  uint32_t n1_idx = &n1 - &g.nodes[0];
  for (const GEdge& edge : gnode_forward_edges(g, n1_idx)) {
    // for (size_t i = 0; i < n1.num_edges_out; ++i) {
    // const GEdge& edge = n1.edges[i];
    if (edge.other_node_idx != n2_idx) {
      continue;
    }
    const WaySharedAttrs& wsa = GetWSA(g, edge.way_idx);
    const RoutingAttrs& ra = GetRAFromWSA(wsa, vt, EDGE_DIR(edge));
    if (RoutableAccess(ra.access) && ra.maxspeed > best_maxspeed) {
      best_maxspeed = ra.maxspeed;
      best_edge = &edge;
    }
  }
  return best_edge;
}

// Write: way_id, start_lon, start_lat, end_lon, end_lat, visits
void WriteCSV(const GraphData& gd, const std::vector<int64_t>& edge_traffic,
              std::string_view export_file) {
  const std::vector<uint32_t>& edges_start = gd.cg.edges_start();
  const std::vector<CompactDirectedGraph::PartialEdge>& edges = gd.cg.edges();
  int64_t count = 0;

  std::ofstream myfile;
  const std::string filename =
      export_file.empty() ? "/tmp/traffic.csv" : std::string(export_file);
  myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);
  if (!export_file.empty()) {
    myfile << "way_id,start_lat,start_lon,end_lat,end_lon,visits,deadend\n";
  }

  int64_t max_traffic = 0;
  for (int64_t val : edge_traffic) {
    max_traffic = std::max(max_traffic, val);
  }
  const char* colors[4] = {"red25", "red50", "red75", "red"};
  int64_t traffic_div = std::max<int64_t>(1, (max_traffic + 9999) / 10000);

  for (size_t from_node = 0; from_node < gd.cg.num_nodes(); ++from_node) {
    for (size_t i = edges_start.at(from_node);
         i < edges_start.at(from_node + 1); ++i) {
      if (edge_traffic.at(i) == 0) {
        continue;
      }
      count++;
      const GNode& n1 =
          gd.g.nodes.at(gd.compact_to_graph_nodemap.at(from_node));
      uint32_t n2_idx = gd.compact_to_graph_nodemap.at(edges.at(i).to_c_idx);
      const GEdge* edge =
          FindEdgeBetweenNodes(gd.g, n1, n2_idx, VH_MOTOR_VEHICLE);
      const GNode& n2 = gd.g.nodes.at(n2_idx);
      CHECK_NE_S(edge, nullptr);
      int64_t way_id = gd.g.ways.at(edge->way_idx).id;
      // Check if the edge is fully in a dead end or a bridge.
      bool dead_end = n1.dead_end || n2.dead_end;
      if (!export_file.empty()) {
        myfile << absl::StrFormat("%lld,%d,%d,%d,%d,%lld,%d\n", way_id, n1.lat,
                                  n1.lon, n2.lat, n2.lon, edge_traffic.at(i),
                                  dead_end);
      } else {
        int div = edge_traffic.at(i) / traffic_div;
        int idx = div < 1 ? 0 : div < 10 ? 1 : div < 100 ? 2 : 3;
        CHECK_LT_S(idx, 4);
        myfile << absl::StrFormat("line,%s,%d,%d,%d,%d\n", colors[idx], n1.lat,
                                  n1.lon, n2.lat, n2.lon);
      }
    }
  }
  myfile.close();
  LOG_S(INFO) << absl::StrFormat("Written %d lines to %s", count, filename);
}

// Runs single source Dijkstra for every POI and adds up the traffic for every
// edge in the spanning tree.
void SingleSourceDijkstraWorker(const CompactDirectedGraph& cg,
                                uint32_t poi_c_idx,
                                std::vector<int64_t>* edge_traffic) {
  std::vector<uint32_t> spanning_tree_nodes;
  std::vector<compact_dijkstra::VisitedNode> vis =
      compact_dijkstra::SingleSourceDijkstra(cg, poi_c_idx,
                                             &spanning_tree_nodes);
#if 0
  LOG_S(INFO) << absl::StrFormat(
      "SingleSourceDijkstra: %u of %u nodes where visited.",
      spanning_tree_nodes.size(), vis.size());
#endif

  // Initialize traffic through every node to 1.
  std::vector<uint32_t> traffic(vis.size(), 1);
  // Iterate through spanning tree nodes bottom up.
  for (size_t i = spanning_tree_nodes.size() - 1; i > 0; --i) {
    uint32_t child_idx = spanning_tree_nodes.at(i);
    uint32_t parent_idx = vis.at(child_idx).from_v_idx;
    // Emit undirected edge (child_idx, parent_idx, traffic[child_idx])
    int64_t e_idx = cg.FindEdge(parent_idx, child_idx);
    CHECK_GE_S(e_idx, 0);
    edge_traffic->at(e_idx) += traffic.at(child_idx);
    // Add traffic[child_idx] to the traffic[parent_idx].
    traffic.at(parent_idx) += traffic.at(child_idx);
  }
}

#if 0
absl::flat_hash_set<uint32_t> ComputeRestrictedAccessTransitionNodes(
    const GraphData& gd, uint32_t poi_c_idx) {
  const absl::flat_hash_set<uint32_t> g_tns =
      GetRestrictedAccessTransitionNodes(
          gd.g, gd.compact_to_graph_nodemap.at(poi_c_idx));
  absl::flat_hash_set<uint32_t> res;
  for (uint32_t g_idx : g_tns) {
    const auto iter = gd.graph_to_compact_nodemap.find(g_idx);
    CHECK_S(iter != gd.graph_to_compact_nodemap.end());
    res.insert(iter->second);
  }
  return res;
}
#endif

// Runs single source Dijkstra for every POI and adds up the traffic for every
// edge in the spanning tree.
void SingleSourceEdgeDijkstraWorker(const GraphData& gd, uint32_t poi_c_idx,
                                    std::vector<int64_t>* thread_edge_traffic) {
  SingleSourceEdgeDijkstra router;
#if 0
  absl::flat_hash_set<uint32_t> restricted_access_nodes =
      router.ComputeRestrictedAccessNodes(gd.g, gd.graph_to_compact_nodemap,
                                          gd.compact_to_graph_nodemap,
                                          poi_c_idx);
#endif
  router.Route(
      gd.cg, poi_c_idx,
      {.store_spanning_tree_edges = true, .handle_restricted_access = true,
       /* .restricted_access_nodes = restricted_access_nodes */});
  const std::vector<uint32_t>& spanning_tree_edges =
      router.GetSpanningTreeEdges();
  std::vector<uint32_t> min_edges = router.GetMinEdgesAtNodes(gd.cg);
  const auto& vis = router.GetVisitedEdges();
  std::vector<int64_t> edge_traffic(gd.cg.edges().size(), 0);

  CHECK_EQ_S(edge_traffic.size(), vis.size());

  // min_edges contains - for every node - the index of the edge that arrives at
  // this node with the minimum metric, i.e. this is the last edge (or leaf
  // edge) on the shortest way to this node. Therefore, the traffic on this edge
  // is initialised as '1'.
  for (uint32_t e_idx : min_edges) {
    if (e_idx != INFU32) {
      edge_traffic.at(e_idx) = 1;
    }
  }

  // Now we iterate in backward direction over the edges as they were marked
  // done, which is in backward direction of *all* shortest ways. We propagate
  // und sum traffic to predecessor edges.
  for (int64_t i = spanning_tree_edges.size() - 1; i >= 0; --i) {
    CHECK_LT_S(i, spanning_tree_edges.size());
    uint32_t e_idx = spanning_tree_edges.at(i);
    CHECK_LT_S(e_idx, edge_traffic.size());
    CHECK_LT_S(e_idx, vis.size());
    if (edge_traffic.at(e_idx) > 0 && vis.at(e_idx).from_idx != INFU31) {
      uint32_t prev_e_idx = vis.at(e_idx).from_idx;
      CHECK_LT_S(prev_e_idx, edge_traffic.size());
      // Add the traffic accumulated at the current edge to its predecessor
      // edge.
      edge_traffic.at(prev_e_idx) += edge_traffic.at(e_idx);
    }
  }

  // Now all edges have the accumulated traffic going through them fort this run
  // of SSD. Add the traffic to the traffic collected per thread.
  CHECK_EQ_S(edge_traffic.size(), thread_edge_traffic->size());
  // TODO: Check if the compiler uses SIMD instructions for that. If not, do it
  // manually.
  for (size_t i = 0; i < edge_traffic.size(); ++i) {
    (*thread_edge_traffic)[i] += edge_traffic[i];
  }
}

// Sum up the edge traffic that was collected in parallel in multiple threads.
// Return a vector with one number for each edge.
std::vector<int64_t> SumUpEdgeTraffic(
    const std::vector<std::vector<int64_t>>& threaded_edge_traffic) {
  std::vector<int64_t> edge_traffic;
  size_t num_edges = threaded_edge_traffic.at(0).size();
  for (size_t i = 0; i < num_edges; ++i) {
    int64_t sum = 0;
    for (const auto& v : threaded_edge_traffic) {
      sum += v.at(i);
    }
    edge_traffic.push_back(sum);
  }
  return edge_traffic;
}

std::vector<int64_t> RunCompactGraphRandomTraffic(
    const GraphData& gd, const std::vector<pois::POI>& pois, int n_threads,
    bool use_edge_dijkstra) {
  // n_threads = 1;
  ThreadPool pool;
  // Contains for each thread a vector with cg.edges().size() traffic counts,
  // one for every edge in the compact graph.
  std::vector<std::vector<int64_t>> threaded_edge_traffic(n_threads);
  for (std::vector<int64_t>& edge_traffic : threaded_edge_traffic) {
    edge_traffic.assign(gd.cg.edges().size(), 0);
  }
  uint32_t count = 0;
  for (size_t i = 0; i < pois.size(); ++i) {
    const pois::POI& p = pois.at(i);
    if (p.routing_node_dist < 10000) {  // < 100m.
      const auto iter = gd.graph_to_compact_nodemap.find(p.routing_node_idx);
      if (iter == gd.graph_to_compact_nodemap.end()) continue;
      count++;

      pool.AddWork([&gd, poi_c_idx = iter->second, i, total = pois.size(),
                    &threaded_edge_traffic, use_edge_dijkstra](int thread_idx) {
        LOG_S(INFO) << absl::StrFormat(
            "SingleSource%sDijkstra for POI %u (%u total)",
            use_edge_dijkstra ? "Edge" : "", i, total);
        if (use_edge_dijkstra) {
          SingleSourceEdgeDijkstraWorker(gd, poi_c_idx,
                                         &threaded_edge_traffic.at(thread_idx));
        } else {
          SingleSourceDijkstraWorker(gd.cg, poi_c_idx,
                                     &threaded_edge_traffic.at(thread_idx));
        }
      });
    }
  }
  pool.Start(n_threads);
  pool.WaitAllFinished();

  LOG_S(INFO) << absl::StrFormat(
      "Executed SingleSourceDijkstra for %u of total %u POIS", count,
      pois.size());

  return SumUpEdgeTraffic(threaded_edge_traffic);
}

void CollectRandomTraffic(const Graph& g, int n_threads,
                          const std::vector<pois::POI>& pois,
                          bool use_edge_dijkstra,
                          std::string_view export_file) {
  FUNC_TIMER();
  const RoutingMetricTime metric;
  uint32_t num_nodes = 0;
  CHECK_S(!g.large_components.empty());
  const std::vector<std::uint32_t>& start_nodes = {
      g.large_components.front().start_node};
  std::vector<CompactDirectedGraph::FullEdge> full_edges;
  // Maps node index in graph.nodes to node index in compact graph.
  absl::flat_hash_map<uint32_t, uint32_t> graph_to_compact_nodemap;

  // Create a compact graph for the largest component in the graph.
  compact_dijkstra::CollectEdgesForCompactGraph(
      g, metric,
      {
          .vt = VH_MOTOR_VEHICLE,
          .avoid_dead_end = false,
          .avoid_restricted_access_edges = false,
          .restrict_to_cluster = false,
      },
      start_nodes, /*undirected_expand=*/true, &num_nodes, &full_edges,
      &graph_to_compact_nodemap);

  const std::vector<std::uint32_t> compact_to_graph_nodemap =
      compact_dijkstra::InvertGraphToCompactNodeMap(graph_to_compact_nodemap);
  compact_dijkstra::SortAndCleanupEdges(&full_edges);
  const CompactDirectedGraph cg(num_nodes, full_edges);
  cg.LogStats();

  const GraphData gd = {.g = g,
                        .cg = cg,
                        .graph_to_compact_nodemap = graph_to_compact_nodemap,
                        .compact_to_graph_nodemap = compact_to_graph_nodemap};
  const std::vector<int64_t> edge_traffic =
      RunCompactGraphRandomTraffic(gd, pois, n_threads, use_edge_dijkstra);

  WriteCSV(gd, edge_traffic, export_file);
}

}  // namespace

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  FUNC_TIMER();

  const Argli argli(
      argc, argv,
      {
          {.name = "pbf",
           .type = "string",
           .positional = true,
           .required = true,
           .desc = "Input OSM pbf file (such as planet file)."},
          {.name = "max_pois",
           .type = "int",
           .dflt = "10000",
           .desc = "Maximal number of POIs to create traffic for."},
          {.name = "n_threads",
           .type = "int",
           .dflt = "10",
           .desc = "Number of threads to use"},
          {.name = "use_edge_dijkstra",
           .type = "bool",
           .desc = "Use normal (false) or edge based (true) Dijkstra."},
          {.name = "check_slow",
           .type = "bool",
           .desc = "Check ClosestPoint algorithm against slow variant."},
          {.name = "export_file",
           .type = "string",
           .desc = "Write output in export format to file <export_file>."},
      });

  const std::string pbf = argli.GetString("pbf");
  const uint64_t max_pois = argli.GetInt("max_pois");
  const int n_threads = argli.GetInt("n_threads");
  const bool use_edge_dijkstra = argli.GetBool("use_edge_dijkstra");
  const bool check_slow = argli.GetBool("check_slow");
  const std::string export_file = argli.GetString("export_file");
  CHECK_GT_S(max_pois, 0);

  // Read POIs.
  std::vector<pois::POI> pois = pois::ReadPBF(pbf, n_threads);
  const size_t orig_poi_size = pois.size();
  LOG_S(INFO) << absl::StrFormat("Use %u of %u pois", max_pois, pois.size());
  if (max_pois < pois.size()) {
    pois.resize(max_pois);
  }

  // Read Road Network.
  build_graph::BuildGraphOptions opt = {.pbf = pbf, .n_threads = n_threads};
  build_graph::GraphMetaData meta = build_graph::BuildGraph(opt);
  const Graph& g = meta.graph;

  MatchPOIsToRoads(g, n_threads, check_slow, &pois);

  // Execute random queries and collect traffic.
  CollectRandomTraffic(g, n_threads, pois, use_edge_dijkstra, export_file);

  LOG_S(INFO) << absl::StrFormat("Collected traffic for %llu of %lld POIs",
                                 std::min(max_pois, orig_poi_size),
                                 orig_poi_size);
  LOG_S(INFO) << "Finished.";
  return 0;
}
