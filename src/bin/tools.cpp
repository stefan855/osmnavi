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
// For each PPOI, find the closest node in the road network.
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

#if 0
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
#endif

namespace {
void WriteCSVs(const Graph& g, const CompactDirectedGraph cg,
               std::vector<std::uint32_t>& graph_node_refs,
               std::vector<std::vector<int64_t>>& v_edge_traffic) {
  const std::vector<uint32_t>& edges_start = cg.edges_start();
  const std::vector<CompactDirectedGraph::PartialEdge>& edges = cg.edges();
  int64_t count = 0;

  std::ofstream myfile;
  const std::string filename = "/tmp/experimental1.csv";
  myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);

  for (size_t from_node = 0; from_node < cg.num_nodes(); ++from_node) {
    for (size_t i = edges_start.at(from_node);
         i < edges_start.at(from_node + 1); ++i) {
      int64_t traffic = 0;
      for (const std::vector<int64_t>& et : v_edge_traffic) {
        traffic += et.at(i);
      }
      if (traffic > 100'000) {
        count++;
        const GNode& n1 = g.nodes.at(graph_node_refs.at(from_node));
        const GNode& n2 = g.nodes.at(graph_node_refs.at(edges.at(i).to_idx));
        myfile << absl::StrFormat("line,red,%d,%d,%d,%d\n", n1.lat, n1.lon,
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
}  // namespace

void CollectRandomTraffic(const Graph& g, int n_threads,
                          pois::CollectedData* data) {
  FUNC_TIMER();
  const RoutingMetricTime metric;
  uint32_t num_nodes = 0;
  CHECK_S(!g.large_components.empty());
  const std::vector<std::uint32_t>& start_nodes = {
      g.large_components.front().start_node};
  std::vector<CompactDirectedGraph::FullEdge> full_edges;
  std::vector<std::uint32_t> graph_node_refs;
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

  ThreadPool pool;
  // Contains for each thread a vector with cg.edges().size() traffic counts,
  // one for every edge in the compact graph.
  std::vector<std::vector<int64_t>> v_edge_traffic(n_threads);
  for (std::vector<int64_t>& edge_traffic : v_edge_traffic) {
    edge_traffic.assign(cg.edges().size(), 0);
  }
  uint32_t count = 0;
  for (size_t i = 0; i < data->pois.size(); ++i) {
    pois::POI& p = data->pois.at(i);
    if (p.routing_node_dist < 10000) {
      const auto iter = compact_nodemap.find(p.routing_node_idx);
      if (iter == compact_nodemap.end()) continue;
      count++;

      pool.AddWork([&g, &cg, poi_compact_idx = iter->second, i,
                    total = data->pois.size(),
                    &v_edge_traffic](int thread_idx) {
        LOG_S(INFO) << absl::StrFormat(
            "SingleSourceDijkstra for POI %u (%u total)", i, total);
        SingleSourceDijkstraWorker(g, cg, poi_compact_idx,
                                   &v_edge_traffic.at(thread_idx));
      });
    }
  }
  pool.Start(n_threads);
  pool.WaitAllFinished();

  WriteCSVs(g, cg, graph_node_refs, v_edge_traffic);

  LOG_S(INFO) << absl::StrFormat(
      "Executed SingleSourceDijkstra for %u of total %u POIS", count,
      data->pois.size());
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
      });

  const std::string command = argli.GetString("command");
  const std::string pbf = argli.GetString("pbf");
  const int max_pois = argli.GetInt("max_pois");
  const int n_threads = argli.GetInt("n_threads");
  const int check_slow = argli.GetBool("check_slow");
  CHECK_GT_S(max_pois, 0);
  CHECK_EQ_S(command, "pois");

  // Read POIs.
  pois::CollectedData data;
  pois::ReadPBF(pbf, n_threads, &data);

  if ((size_t)max_pois < data.pois.size()) {
    // Pseudo random number generator with a constant seed, to be reproducible.
    std::mt19937 myrandom(1);
    // Shuffle the POIs so we can run over a subset of size max_pois without
    // introducing a bias.
    std::shuffle(data.pois.begin(), data.pois.end(), myrandom);
    data.pois.resize(max_pois);
  }

  // Read Road Network.
  build_graph::BuildGraphOptions opt = {.pbf = pbf, .n_threads = n_threads};
  build_graph::GraphMetaData meta = build_graph::BuildGraph(opt);
  const Graph& g = meta.graph;

  MatchPOIsToRoads(g, n_threads, check_slow, &data);

  // Execute random queries and collect traffic.
  CollectRandomTraffic(g, n_threads, &data);

  /*
  for (size_t i = 0; i < data.pois.size(); ++i) {
    const pois::POI& p = data.pois.at(i);
    LOG_S(INFO) << absl::StrFormat("%10u. %c %10d lat:%d lon:%d #n:%d %s %s", i,
                                   p.obj_type, p.id, p.lat, p.lon, p.num_points,
                                   p.type, p.name);
  }
  */

  LOG_S(INFO) << "Finished.";
  return 0;
}
