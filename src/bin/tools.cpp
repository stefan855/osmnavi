#include <osmpbf/osmpbf.h>

#include <algorithm>
#include <filesystem>
#include <map>
#include <random>

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
// For each POI, find the closest node in the road network.
void MatchPOIsToRoads(const Graph& g, int n_threads, bool check_slow,
                      pois::CollectedData* data) {
  FUNC_TIMER();
  const size_t StepSize = check_slow ? 500 : 5000;
  const auto idx = SortNodeIndexesByLon(g);
  ThreadPool pool;
  for (size_t start = 0; start < data->pois.size(); start += StepSize) {
    size_t len = std::min(StepSize, data->pois.size() - start);
    pool.AddWork([&g, &idx, check_slow, data, start, len](int thread_idx) {
      LOG_S(INFO) << absl::StrFormat(
          "Locate navigation nodes for POIs [%u..%u) of total %u", start,
          start + len, data->pois.size());
      for (size_t i = start; i < start + len; ++i) {
        pois::POI& poi = data->pois.at(i);
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

namespace {

const GEdge* FindEdgeBetweenNodes(const Graph& g, const GNode& n1,
                                  uint32_t n2_idx, VEHICLE vt) {
  uint32_t best_maxspeed = 0;
  const GEdge* best_edge = nullptr;
  for (size_t i = 0; i < n1.num_edges_out; ++i) {
    const GEdge& edge = n1.edges[i];
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
void WriteCSV(const Graph& g, const CompactDirectedGraph cg,
              const std::vector<std::uint32_t>& graph_node_refs,
              const std::vector<int64_t>& edge_traffic,
              std::string_view export_file) {
  const std::vector<uint32_t>& edges_start = cg.edges_start();
  const std::vector<CompactDirectedGraph::PartialEdge>& edges = cg.edges();
  int64_t count = 0;

  std::ofstream myfile;
  const std::string filename =
      export_file.empty() ? "/tmp/traffic.csv" : std::string(export_file);
  myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);
  if (!export_file.empty()) {
    myfile << "way_id,start_lon,start_lat,end_lon,end_lat,visits,deadend\n";
  }

  for (size_t from_node = 0; from_node < cg.num_nodes(); ++from_node) {
    for (size_t i = edges_start.at(from_node);
         i < edges_start.at(from_node + 1); ++i) {
      if (edge_traffic.at(i) == 0) {
        continue;
      }
      count++;
      const GNode& n1 = g.nodes.at(graph_node_refs.at(from_node));
      uint32_t n2_idx = graph_node_refs.at(edges.at(i).to_idx);
      const GEdge* edge = FindEdgeBetweenNodes(g, n1, n2_idx, VH_MOTOR_VEHICLE);
      const GNode& n2 = g.nodes.at(n2_idx);
      CHECK_NE_S(edge, nullptr);
      int64_t way_id = g.ways.at(edge->way_idx).id;
      // Check if the edge is fully in a dead end or a bridge.
      bool dead_end = n1.dead_end || n2.dead_end;
      if (!export_file.empty()) {
        myfile << absl::StrFormat("%lld,%d,%d,%d,%d,%lld,%d\n", way_id, n1.lat,
                                  n1.lon, n2.lat, n2.lon, edge_traffic.at(i),
                                  dead_end);
      } else {
        myfile << absl::StrFormat("line,%s,%d,%d,%d,%d\n",
                                  dead_end ? "grey" : "red", n1.lat, n1.lon,
                                  n2.lat, n2.lon);
      }
    }
  }
  myfile.close();
  LOG_S(INFO) << absl::StrFormat("Written %d lines to %s", count, filename);
}

// Runs single source Dijkstra for every POI and adds up the traffic for every
// edge in the spanning tree.
void SingleSourceDijkstraWorker(const Graph& g, const CompactDirectedGraph& cg,
                                uint32_t poi_compact_idx,
                                std::vector<int64_t>* edge_traffic) {
  std::vector<uint32_t> spanning_tree_nodes;
  std::vector<compact_dijkstra::VisitedNode> vis =
      compact_dijkstra::SingleSourceDijkstra(cg, poi_compact_idx,
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
    const Graph& g, const CompactDirectedGraph& cg,
    const absl::flat_hash_map<uint32_t, uint32_t>& compact_nodemap,
    const pois::CollectedData& data, int n_threads) {
  ThreadPool pool;
  // Contains for each thread a vector with cg.edges().size() traffic counts,
  // one for every edge in the compact graph.
  std::vector<std::vector<int64_t>> threaded_edge_traffic(n_threads);
  for (std::vector<int64_t>& edge_traffic : threaded_edge_traffic) {
    edge_traffic.assign(cg.edges().size(), 0);
  }
  uint32_t count = 0;
  for (size_t i = 0; i < data.pois.size(); ++i) {
    const pois::POI& p = data.pois.at(i);
    if (p.routing_node_dist < 10000) {  // < 100m.
      const auto iter = compact_nodemap.find(p.routing_node_idx);
      if (iter == compact_nodemap.end()) continue;
      count++;

      pool.AddWork([&g, &cg, poi_compact_idx = iter->second, i,
                    total = data.pois.size(),
                    &threaded_edge_traffic](int thread_idx) {
        LOG_S(INFO) << absl::StrFormat(
            "SingleSourceDijkstra for POI %u (%u total)", i, total);
        SingleSourceDijkstraWorker(g, cg, poi_compact_idx,
                                   &threaded_edge_traffic.at(thread_idx));
      });
    }
  }
  pool.Start(n_threads);
  pool.WaitAllFinished();

  LOG_S(INFO) << absl::StrFormat(
      "Executed SingleSourceDijkstra for %u of total %u POIS", count,
      data.pois.size());

  return SumUpEdgeTraffic(threaded_edge_traffic);
}

}  // namespace

void CollectRandomTraffic(const Graph& g, int n_threads,
                          const pois::CollectedData& data,
                          std::string_view export_file) {
  FUNC_TIMER();
  const RoutingMetricTime metric;
  uint32_t num_nodes = 0;
  CHECK_S(!g.large_components.empty());
  const std::vector<std::uint32_t>& start_nodes = {
      g.large_components.front().start_node};
  std::vector<CompactDirectedGraph::FullEdge> full_edges;
  std::vector<std::uint32_t> graph_node_refs;
  // Maps node index in graph.nodes to node index in compact graph.
  absl::flat_hash_map<uint32_t, uint32_t> compact_nodemap;

  // Create a compact graph for the largest component in the graph.
  compact_dijkstra::CollectEdgesForCompactGraph(
      g, metric,
      {
          .vt = VH_MOTOR_VEHICLE,
          .avoid_dead_end = false,
          .restrict_to_cluster = false,
      },
      start_nodes, /*undirected_expand=*/true, &num_nodes, &full_edges,
      &compact_nodemap);
  graph_node_refs = compact_dijkstra::NodeMapToGraphNodeRefs(compact_nodemap);
  compact_dijkstra::SortAndCleanupEdges(&full_edges);
  const CompactDirectedGraph cg(num_nodes, full_edges);
  cg.LogStats();

  const std::vector<int64_t> edge_traffic =
      RunCompactGraphRandomTraffic(g, cg, compact_nodemap, data, n_threads);

  WriteCSV(g, cg, graph_node_refs, edge_traffic, export_file);
}

}  // namespace

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  FUNC_TIMER();

  Argli argli(
      argc, argv,
      {
          {.name = "command",
           .type = "string",
           .positional = true,
           .required = true,
           .desc = "Command to execute, possible values are \"pois\""},
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
          {.name = "check_slow",
           .type = "bool",
           .desc = "Check ClosestPoint algorithm against slow variant."},
          {.name = "export_file",
           .type = "string",
           .desc = "Write output in export format to file <export_file>."},
      });

  const std::string command = argli.GetString("command");
  const std::string pbf = argli.GetString("pbf");
  const int64_t max_pois = argli.GetInt("max_pois");
  const int n_threads = argli.GetInt("n_threads");
  const int check_slow = argli.GetBool("check_slow");
  const std::string export_file = argli.GetString("export_file");
  CHECK_GT_S(max_pois, 0);
  CHECK_EQ_S(command, "pois");

  // Read POIs.
  pois::CollectedData data;
  pois::ReadPBF(pbf, n_threads, &data);

  int64_t pois_size = data.pois.size();
  if (max_pois < pois_size) {
    // Pseudo random number generator with a constant seed, to be reproducible.
    std::mt19937 myrandom(1);
    // Shuffle the POIs so we can run over a subset of size max_pois without
    // introducing a bias.
    LOG_S(INFO) << "Randomizing+shrinking " << pois_size << " POIs to "
                << max_pois;
    std::shuffle(data.pois.begin(), data.pois.end(), myrandom);
    data.pois.resize(max_pois);
  }

  // Read Road Network.
  build_graph::BuildGraphOptions opt = {.pbf = pbf, .n_threads = n_threads};
  build_graph::GraphMetaData meta = build_graph::BuildGraph(opt);
  const Graph& g = meta.graph;

  MatchPOIsToRoads(g, n_threads, check_slow, &data);

  // Execute random queries and collect traffic.
  CollectRandomTraffic(g, n_threads, data, export_file);

  LOG_S(INFO) << absl::StrFormat("Collected traffic for %llu of %lld POIs",
                                 std::min(max_pois, pois_size), pois_size);
  LOG_S(INFO) << "Finished.";
  return 0;
}
