#include <osmpbf/osmpbf.h>
#include <stdarg.h>
#include <stdio.h>

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <span>
#include <vector>

#include "absl/container/btree_map.h"
#include "absl/strings/str_format.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "algos/astar.h"
#include "algos/cluster_dijkstra.h"
#include "algos/components.h"
#include "algos/dijkstra.h"
#include "algos/louvain.h"
#include "algos/merge_tiny_clusters.h"
#include "algos/tarjan.h"
#include "base/argli.h"
#include "base/country_code.h"
#include "base/deduper_with_ids.h"
#include "base/huge_bitset.h"
#include "base/thread_pool.h"
#include "base/util.h"
#include "base/varbyte.h"
#include "geometry/distance.h"
#include "geometry/polygon.h"
#include "graph/build_clusters.h"
#include "graph/build_graph.h"
#include "graph/data_block.h"
#include "graph/graph_def.h"
#include "graph/routing_attrs.h"
#include "osm/admin_boundary.h"
#include "osm/id_chain.h"
#include "osm/osm_helpers.h"
#include "osm/read_osm_pbf.h"

namespace build_graph {

void AddEdge(Graph& g, const size_t start_idx, const size_t other_idx,
             const bool out, const bool contra_way, const bool both_directions,
             const size_t way_idx, const std::uint64_t distance_cm) {
  GNode& n = g.nodes.at(start_idx);
  const GNode& other = g.nodes.at(other_idx);
  GEdge* edges = out ? n.edges : n.edges + n.num_edges_out;
  size_t num_edges = out ? n.num_edges_out : n.num_edges_inverted;
  for (size_t pos = 0; pos < num_edges; ++pos) {
    // Unused edges are marked with 'other_node_idx == INFU32'.
    if (edges[pos].other_node_idx == INFU32) {
      assert(other_idx != INFU32);
      edges[pos].other_node_idx = other_idx;
      edges[pos].way_idx = way_idx;
      edges[pos].distance_cm = distance_cm;
      edges[pos].unique_other = 0;
      edges[pos].bridge = 0;
      edges[pos].to_bridge = 0;
      edges[pos].contra_way = contra_way ? 1 : 0;
      edges[pos].both_directions = both_directions ? 1 : 0;
      edges[pos].cross_country = n.ncc != other.ncc;
      return;
    }
  }
  // Should not happen because we preallocated needed space.
  ABORT_S() << absl::StrFormat(
      "Cannot store edge at node:%lld start_idx:%u num_edges:%u other_idx:%u "
      "out:%d way_idx:%u dist:%llu",
      n.node_id, start_idx, num_edges, other_idx, out, way_idx, distance_cm);
}

void MarkUniqueOther(GNode* n) {
  GEdge* edges = n->edges;
  for (size_t i = 0; i < gnode_total_edges(*n); ++i) {
    size_t k = 0;
    while (k < i) {
      if (edges[i].other_node_idx == edges[k].other_node_idx) {
        break;
      }
      k++;
    }
    edges[i].unique_other = (i == k);
  }
}

void FindLargeComponents(Graph* g) {
  ComponentAnalyzer a(*g);
  g->large_components = a.FindLargeComponents();
  ComponentAnalyzer::MarkLargeComponents(g);
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

void ApplyTarjan(Graph& g, MetaData* meta) {
  FUNC_TIMER();
  if (g.large_components.empty()) {
    ABORT_S() << "g.large_components is empty";
  }
  Tarjan t(g);
  std::vector<Tarjan::BridgeInfo> bridges;
  for (const Graph::Component& comp : g.large_components) {
    bridges.clear();
    t.FindBridges(comp, &bridges);
    // Sort bridges in descending order of subtrees. If a dead-end contains
    // other, smaller dead-ends, then only the dead-end with the largest subtree
    // is actually used.
    std::sort(bridges.begin(), bridges.end(),
              [](const Tarjan::BridgeInfo& a, const Tarjan::BridgeInfo& b) {
                return a.subtree_size > b.subtree_size;
              });
    for (const Tarjan::BridgeInfo& bridge : bridges) {
      MarkBridgeEdge(g, bridge.from_node_idx, bridge.to_node_idx);
      MarkBridgeEdge(g, bridge.to_node_idx, bridge.from_node_idx);
      meta->stats.num_dead_end_nodes +=
          MarkDeadEndNodes(g, bridge.to_node_idx, bridge.subtree_size);
    }
  }
  LOG_S(INFO) << absl::StrFormat(
      "Graph has %u (%.2f%%) dead end nodes.", meta->stats.num_dead_end_nodes,
      (100.0 * meta->stats.num_dead_end_nodes) / g.nodes.size());
}

void TestRoute(const Graph& g) {
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
        auto result =
            router.Route(start_idx, target_idx, RoutingMetricDistance());
        if (result.found) {
          router.SaveSpanningTreeSegments("/tmp/route_dist.csv");
        } else {
          LOG_S(INFO) << "failed to find dijkstra route!";
        }
      }
      {
        DijkstraRouter router(g);
        auto result = router.Route(start_idx, target_idx, RoutingMetricTime());
        if (result.found) {
          router.SaveSpanningTreeSegments("/tmp/route_time.csv");
        } else {
          LOG_S(INFO) << "failed to find dijkstra route!";
        }
      }
      {
        DijkstraRouter router(g);
        auto result = router.Route(start_idx, target_idx, RoutingMetricTime());
        if (result.found) {
          router.SaveSpanningTreeSegments("/tmp/route_time.csv");
        } else {
          LOG_S(INFO) << "failed to find dijkstra route!";
        }
      }
      {
        DijkstraRouter router(g);
        DijkstraRouter::Filter filt = DijkstraRouter::standard_filter;
        filt.inverse_search = true;
        auto result =
            router.Route(target_idx, start_idx, RoutingMetricDistance(), filt);
        if (!result.found) {
          LOG_S(INFO) << "failed to find dijkstra route!";
        }
      }
      {
        DijkstraRouter router(g);
        DijkstraRouter::Filter filt = DijkstraRouter::standard_filter;
        filt.inverse_search = true;
        auto result =
            router.Route(target_idx, start_idx, RoutingMetricTime(), filt);
        if (!result.found) {
          LOG_S(INFO) << "failed to find dijkstra route!";
        }
      }
      {
        AStarRouter router(g);
        auto result =
            router.Route(start_idx, target_idx, RoutingMetricDistance());
        if (result.found) {
          router.SaveSpanningTreeSegments("/tmp/astar_route_dist.csv");
        } else {
          LOG_S(INFO) << "failed to find astar route!";
        }
      }
      {
        AStarRouter router(g);
        auto result = router.Route(start_idx, target_idx, RoutingMetricTime());
        if (result.found) {
          router.SaveSpanningTreeSegments("/tmp/astar_route_time.csv");
        } else {
          LOG_S(INFO) << "failed to find astar route!";
        }
      }

      {
        AStarRouter router(g);
        AStarRouter::Filter filt = AStarRouter::standard_filter;
        filt.inverse_search = true;
        auto result =
            router.Route(target_idx, start_idx, RoutingMetricDistance(), filt);
        if (!result.found) {
          LOG_S(INFO) << "failed to find astar route!";
        }
      }
      {
        AStarRouter router(g);
        AStarRouter::Filter filt = AStarRouter::standard_filter;
        filt.inverse_search = true;
        auto result =
            router.Route(target_idx, start_idx, RoutingMetricTime(), filt);
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
        AStarRouter router(g);

        AStarRouter::Filter filt = AStarRouter::standard_filter;
        filt.MayFillBridgeNodeId(g, target_idx);
        auto result =
            router.Route(start_idx, target_idx, RoutingMetricTime(), filt);
        if (result.found) {
          router.SaveSpanningTreeSegments("/tmp/astar_route_time.csv");
        }
      }
    }
  }
}

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
      AStarRouter::Filter filt = AStarRouter::standard_filter;
      filt.MayFillBridgeNodeId(g, target);
      auto result = router.Route(start, target, RoutingMetricTime(), filt);
    });
  }
  pool.Start(23);
  pool.WaitAllFinished();
}

namespace {
void LogMissingWay(std::string_view type, int64_t way_id, int64_t rel_id) {
  LOG_S(INFO) << absl::StrFormat("TR: '%s' way %lld in rel %lld not found",
                                 type, way_id, rel_id);
}

bool ConnectTurnRestriction(const MetaData& meta, TurnRestriction* tr) {
  IdChain chain;
  IdChain::IdPair from;
  if (!chain.CreateIdPair(meta.graph, tr->from_way_id, &from)) {
    if (meta.stats.log_turn_restrictions) {
      LogMissingWay("from", tr->from_way_id, tr->relation_id);
    }
    return false;
  }
  chain.AddIdPair(from);
  if (tr->via_is_node) {
    chain.AddIdPair({.id1 = tr->via_ids.at(0), .id2 = tr->via_ids.at(0)});
  } else {
    for (int64_t id : tr->via_ids) {
      IdChain::IdPair via;
      if (!chain.CreateIdPair(meta.graph, id, &via)) {
        if (meta.stats.log_turn_restrictions) {
          LogMissingWay("via", id, tr->relation_id);
        }
        return false;
      }
      chain.AddIdPair(via);
    }
  }

  IdChain::IdPair to;
  if (!chain.CreateIdPair(meta.graph, tr->to_way_id, &to)) {
    if (meta.stats.log_turn_restrictions) {
      LogMissingWay("to", tr->to_way_id, tr->relation_id);
    }
    return false;
  }
  chain.AddIdPair(to);
  bool success = chain.success();
  if (meta.stats.log_turn_restrictions) {
    LOG_S(INFO) << absl::StrFormat("TR: %u %s: %s match in relation %lld",
                                   chain.get_chain().size(),
                                   success ? "success" : "error",
                                   chain.GetChainCodeString(), tr->relation_id);
  }
  return success;
}
}  // namespace

void ConsumeRelation(const OSMTagHelper& tagh, const OSMPBF::Relation& osm_rel,
                     std::mutex& mut, MetaData* meta) {
  // TODO: Handle turn restrictions (currently we're only reading them).
  TurnRestriction tr;
  ResType rt = ParseTurnRestriction(tagh, osm_rel,
                                    meta->stats.log_turn_restrictions, &tr);
  if (rt == ResType::Ignore) return;

  if (rt == ResType::Success && ConnectTurnRestriction(*meta, &tr)) {
    std::unique_lock<std::mutex> l(mut);
    meta->turn_restrictions.push_back(tr);
  } else {
    std::unique_lock<std::mutex> l(mut);
    meta->stats.num_turn_restriction_errors++;
  }
}

namespace {

// Read the ways that might useful for routing, remember the nodes ids touched
// by these ways, then read the node coordinates and store them in 'nodes'.
void LoadNodeCoordinates(OsmPbfReader* reader, DataBlockTable* node_table,
                         MetaStatsData* stats) {
  HugeBitset touched_nodes_ids;
  // Read ways and remember the touched nodes in 'touched_nodes_ids'.
  reader->ReadWays([&touched_nodes_ids, stats](const OSMTagHelper& tagh,
                                               const OSMPBF::Way& way,
                                               std::mutex& mut) {
    ConsumeWayStoreSeenNodesWorker(tagh, way, mut, &touched_nodes_ids, stats);
  });

  // Read all the node coordinates for nodes in 'touched_nodes_ids'.
  reader->ReadBlobs(
      OsmPbfReader::ContentNodes,
      [&touched_nodes_ids, node_table](const OSMTagHelper& tagh,
                                       const OSMPBF::PrimitiveBlock& prim_block,
                                       std::mutex& mut) {
        ConsumeNodeBlob(tagh, prim_block, mut, touched_nodes_ids, node_table);
      });
  // Make node table searchable, so we can look up lat/lon by node_id.
  node_table->Sort();
}

}  // namespace

void WriteCrossCountryEdges(MetaData* meta, const std::string& filename) {
  LOG_S(INFO) << absl::StrFormat("Write cross country_edges to %s",
                                 filename.c_str());
  std::ofstream myfile;
  myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);

  meta->stats.num_cross_country_edges = 0;
  for (const GWay& way : meta->graph.ways) {
    if (way.uniform_country == 1) {
      continue;
    }
    const std::vector<size_t> node_idx =
        meta->graph.GetGWayNodeIndexes(*(meta->way_nodes_needed), way);
    for (size_t pos = 0; pos < node_idx.size() - 1; ++pos) {
      const GNode& n1 = meta->graph.nodes.at(node_idx.at(pos));
      const GNode& n2 = meta->graph.nodes.at(node_idx.at(pos + 1));
      if (n1.ncc != n2.ncc) {
        meta->stats.num_cross_country_edges++;
        myfile << absl::StrFormat("line,black,%d,%d,%d,%d\n", n1.lat, n1.lon,
                                  n2.lat, n2.lon);
      }
    }
  }

  myfile.close();
  LOG_S(INFO) << absl::StrFormat("Written %d lines to %s",
                                 meta->stats.num_cross_country_edges, filename);
}

void SortGWays(MetaData* meta) {
  FUNC_TIMER();
  // Sort by ascending way_id.
  std::sort(meta->graph.ways.begin(), meta->graph.ways.end(),
            [](const GWay& a, const GWay& b) { return a.id < b.id; });
}

void AllocateGNodes(MetaData* meta) {
  FUNC_TIMER();
  meta->graph.nodes.reserve(meta->way_nodes_needed->CountBits());
  NodeBuilder::GlobalNodeIter iter(*meta->node_table);
  const NodeBuilder::VNode* node;
  while ((node = iter.Next()) != nullptr) {
    if (meta->way_nodes_needed->GetBit(node->id)) {
      GNode snode;
      snode.node_id = node->id;
      snode.lat = node->lat;
      snode.lon = node->lon;
      snode.num_edges_inverted = 0;
      snode.num_edges_out = 0;
      snode.dead_end = 0;
      snode.large_component = 0;
      meta->graph.nodes.push_back(snode);
    }
  }
}

void SetCountryInGNodes(MetaData* meta) {
  FUNC_TIMER();
  // TODO: run with thread pool.
  for (GNode& n : meta->graph.nodes) {
    n.ncc = meta->tiler->GetCountryNum(n.lon, n.lat);
  }
}

void ComputeEdgeCountsWorker(size_t start_pos, size_t stop_pos, MetaData* meta,
                             std::mutex& mut) {
  Graph& graph = meta->graph;

  for (size_t way_idx = start_pos; way_idx < stop_pos; ++way_idx) {
    const GWay& way = graph.ways.at(way_idx);
    const WaySharedAttrs& wsa = GetWSA(graph, way);
    std::vector<size_t> node_idx =
        meta->graph.GetGWayNodeIndexes(*(meta->way_nodes_needed), way);
    {
      // Update edge counts.
      std::unique_lock<std::mutex> l(mut);
      int64_t prev_idx = -1;
      for (const size_t idx : node_idx) {
        if (prev_idx >= 0) {
          // TODO: self edges?
          // CHECK_NE_S(idx1, idx2) << way.id;

          if (WSAAnyRoutable(wsa, DIR_FORWARD) &&
              WSAAnyRoutable(wsa, DIR_BACKWARD)) {
            graph.nodes.at(prev_idx).num_edges_out++;
            graph.nodes.at(idx).num_edges_out++;
          } else if (WSAAnyRoutable(wsa, DIR_FORWARD)) {
            graph.nodes.at(prev_idx).num_edges_out++;
            graph.nodes.at(idx).num_edges_inverted++;
          } else {
            CHECK_S(WSAAnyRoutable(wsa, DIR_BACKWARD)) << way.id;
            graph.nodes.at(prev_idx).num_edges_inverted++;
            graph.nodes.at(idx).num_edges_out++;
          }
#if 0
          if (RoutableForward(graph, way, VH_MOTOR_VEHICLE) &&
              RoutableBackward(graph, way, VH_MOTOR_VEHICLE)) {
            graph.nodes.at(prev_idx).num_edges_out++;
            graph.nodes.at(idx).num_edges_out++;
          } else if (RoutableForward(graph, way, VH_MOTOR_VEHICLE)) {
            graph.nodes.at(prev_idx).num_edges_out++;
            graph.nodes.at(idx).num_edges_inverted++;
          } else {
            CHECK_S(RoutableBackward(graph, way, VH_MOTOR_VEHICLE)) << way.id;
            graph.nodes.at(prev_idx).num_edges_inverted++;
            graph.nodes.at(idx).num_edges_out++;
          }
#endif
        }
        prev_idx = idx;
      }
    }
  };
}

void ComputeEdgeCounts(MetaData* meta) {
  FUNC_TIMER();
  std::mutex mut;
  const size_t unit_length = 25000;
  ThreadPool pool;
  for (size_t start_pos = 0; start_pos < meta->graph.ways.size();
       start_pos += unit_length) {
    pool.AddWork([meta, &mut, start_pos, unit_length](int thread_idx) {
      const size_t stop_pos =
          std::min(start_pos + unit_length, meta->graph.ways.size());
      ComputeEdgeCountsWorker(start_pos, stop_pos, meta, mut);
    });
  }
  pool.Start(meta->n_threads);
  pool.WaitAllFinished();
}

void AllocateEdgeArrays(MetaData* meta) {
  FUNC_TIMER();
  Graph& graph = meta->graph;
  for (GNode& n : meta->graph.nodes) {
    const std::uint32_t num_edges = gnode_total_edges(n);
    CHECK_GT_S(num_edges, 0u);
    if (num_edges > 0) {
      n.edges = (GEdge*)meta->graph.aligned_pool_.AllocBytes(num_edges *
                                                             sizeof(GEdge));
      for (size_t k = 0; k < num_edges; ++k) {
        n.edges[k].other_node_idx = INFU32;
      }
    }
  }
}

void PopulateEdgeArraysWorker(size_t start_pos, size_t stop_pos, MetaData* meta,
                              std::mutex& mut) {
  Graph& graph = meta->graph;
  std::vector<uint64_t> ids;
  std::vector<size_t> node_idx;
  std::vector<uint64_t> dist_sums;

  // Compute distances between the nodes of the way and store
  for (size_t way_idx = start_pos; way_idx < stop_pos; ++way_idx) {
    const GWay& way = graph.ways.at(way_idx);
    ids.clear();
    node_idx.clear();
    dist_sums.clear();
    // Decode node_ids.
    std::uint64_t num_nodes;
    std::uint8_t* ptr = way.node_ids;
    ptr += DecodeUInt(ptr, &num_nodes);
    ptr += DecodeNodeIds(ptr, num_nodes, &ids);

    // Compute distances sum from start and store in distance vector.
    // Non-existing nodes add 0 to the distance.
    NodeBuilder::VNode prev_node = {.id = 0, .lat = 0, .lon = 0};
    int64_t sum = 0;
    for (const uint64_t id : ids) {
      if (meta->way_nodes_seen->GetBit(id)) {
        NodeBuilder::VNode node;
        if (!NodeBuilder::FindNode(*meta->node_table, id, &node)) {
          // Should not happen, all 'seen' nodes should exist.
          ABORT_S() << absl::StrFormat("Way:%llu has missing node %llu", way.id,
                                       id);
        }
        // Sum up distance so far.
        if (prev_node.id != 0) {
          sum += calculate_distance(prev_node.lat, prev_node.lon, node.lat,
                                    node.lon);
        }
        prev_node = node;
      }
      dist_sums.push_back(sum);
      if (meta->way_nodes_needed->GetBit(id)) {
        std::size_t idx = graph.FindNodeIndex(id);
        CHECK_S(idx >= 0 && idx < graph.nodes.size());
        node_idx.push_back(idx);
      } else {
        node_idx.push_back(std::numeric_limits<size_t>::max());
      }
    }
    CHECK_EQ_S(ids.size(), dist_sums.size());
    CHECK_EQ_S(ids.size(), node_idx.size());

    // Go through 'needed' nodes (skip the others) and output edges.
    {
      const WaySharedAttrs& wsa = GetWSA(graph, way);
      std::unique_lock<std::mutex> l(mut);
      int last_pos = -1;
      for (size_t pos = 0; pos < ids.size(); ++pos) {
        uint64_t id = ids.at(pos);
        if (meta->way_nodes_needed->GetBit(id)) {
          if (last_pos >= 0) {
            // Emit edge.
            std::size_t idx1 = node_idx.at(last_pos);
            std::size_t idx2 = node_idx.at(pos);
            uint64_t distance_cm = dist_sums.at(pos) - dist_sums.at(last_pos);
            // Store edges with the summed up distance.

            if (WSAAnyRoutable(wsa, DIR_FORWARD) &&
                WSAAnyRoutable(wsa, DIR_BACKWARD)) {
              AddEdge(graph, idx1, idx2, /*out=*/true, /*contra_way=*/false,
                      /*both_directions=*/true, way_idx, distance_cm);
              AddEdge(graph, idx2, idx1, /*out=*/true, /*contra_way=*/true,
                      /*both_directions=*/true, way_idx, distance_cm);
            } else if (WSAAnyRoutable(wsa, DIR_FORWARD)) {
              AddEdge(graph, idx1, idx2, /*out=*/true, /*contra_way=*/false,
                      /*both_directions=*/false, way_idx, distance_cm);
              AddEdge(graph, idx2, idx1, /*out=*/false, /*contra_way=*/true,
                      /*both_directions=*/false, way_idx, distance_cm);
            } else {
              CHECK_S(WSAAnyRoutable(wsa, DIR_BACKWARD)) << way.id;
              AddEdge(graph, idx2, idx1, /*out=*/true, /*contra_way=*/true,
                      /*both_directions=*/false, way_idx, distance_cm);
              AddEdge(graph, idx1, idx2, /*out=*/false, /*contra_way=*/false,
                      /*both_directions=*/false, way_idx, distance_cm);
            }
          }
          last_pos = pos;
        }
      }
    }
  }
}

void PopulateEdgeArrays(MetaData* meta) {
  FUNC_TIMER();
  std::mutex mut;
  const size_t unit_length = 25000;
  ThreadPool pool;
  for (size_t start_pos = 0; start_pos < meta->graph.ways.size();
       start_pos += unit_length) {
    pool.AddWork([meta, &mut, start_pos, unit_length](int thread_idx) {
      const size_t stop_pos =
          std::min(start_pos + unit_length, meta->graph.ways.size());
      PopulateEdgeArraysWorker(start_pos, stop_pos, meta, mut);
    });
  }
  pool.Start(meta->n_threads);
  pool.WaitAllFinished();
}

void MarkUniqueEdges(MetaData* meta) {
  FUNC_TIMER();
  for (GNode& n : meta->graph.nodes) {
    MarkUniqueOther(&n);
  }
}

#if 0
void CheckShortestClusterPaths(int n_threads, const Graph& g,
                               const RoutingMetric& metric) {
  FUNC_TIMER();
  ThreadPool pool;
  for (const GCluster& c : g.clusters) {
    pool.AddWork([&g, &c, &metric](int) {
      LOG_S(INFO) << absl::StrFormat("Checks paths in cluster:%u #border:%u",
                                     c.cluster_id, c.num_border_nodes);
      AStarRouter::Filter filter = {.avoid_dead_end = true,
                                    .restrict_to_cluster = true,
                                    .cluster_id = c.cluster_id};
      for (uint32_t idx = 0; idx < c.num_border_nodes; ++idx) {
        for (uint32_t idx2 = 0; idx2 < c.num_border_nodes; ++idx2) {
          // DijkstraRouter rt(g, /*verbose=*/false);
          AStarRouter rt(g, /*verbose=*/false);
          auto result = rt.Route(c.border_nodes.at(idx),
                                 c.border_nodes.at(idx2), metric, filter);

          if (c.distances.at(idx).at(idx2) != result.found_distance) {
            LOG_S(INFO) << absl::StrFormat(
                "Cluster:%u path from %u to %u differs -- stored:%u vs. "
                "computed:%u",
                c.cluster_id, idx, idx2, c.distances.at(idx).at(idx2),
                result.found_distance);
            // Debug special case when AStar route is not equal to the Dijkstra
            // route.
            if (c.cluster_id == 36) {
              rt.SaveSpanningTreeSegments("/tmp/experimental1.csv");
              // Run the same using Dijkstra, to compare.
              DijkstraRouter rt2(g, /*verbose=*/false);
              auto result2 = rt2.Route(c.border_nodes.at(idx),
                                       c.border_nodes.at(idx2), metric,
                                       {.avoid_dead_end = true,
                                        .restrict_to_cluster = true,
                                        .cluster_id = c.cluster_id});
              CHECK_EQ_S(c.distances.at(idx).at(idx2), result2.found_distance);
              rt2.SaveSpanningTreeSegments("/tmp/experimental2.csv");
            } else {
              rt.SaveSpanningTreeSegments("/tmp/experimental3.csv");
            }
          }
          // CHECK(c.distances.at(idx).at(idx2), result.found_distance);
        }
      }
    });
  }
  pool.Start(n_threads);
  pool.WaitAllFinished();
}
#endif

void ComputeShortestPathsInAllClusters(MetaData* meta) {
  FUNC_TIMER();
  RoutingMetricTime metric;
  if (!meta->graph.clusters.empty()) {
    ThreadPool pool;
    for (GCluster& cluster : meta->graph.clusters) {
      pool.AddWork([meta, &metric, &cluster](int) {
        // TODO: support the other vehicle types.
        ComputeShortestClusterPaths(meta->graph, metric, VH_MOTOR_VEHICLE,
                                    &cluster);
      });
    }
    // Faster with few threads only.
    pool.Start(std::min(5, meta->n_threads));
    pool.WaitAllFinished();
  }
}

void ExecuteLouvain(const bool merge_tiny_clusters, MetaData* meta) {
  FUNC_TIMER();
  build_clusters::ExecuteLouvainStages(meta->n_threads, &meta->graph);
  build_clusters::UpdateGraphClusterInformation(&meta->graph);

  if (merge_tiny_clusters) {
    build_clusters::MergeTinyClusters(&(meta->graph));
    build_clusters::UpdateGraphClusterInformation(&meta->graph);
  }

  // build_clusters::StoreClusterInformation(gvec, &meta->graph);
  build_clusters::PrintClusterInformation(meta->graph);
  build_clusters::WriteLouvainGraph(meta->graph, "/tmp/louvain.csv");

  ComputeShortestPathsInAllClusters(meta);

#if 0
  // Check if astar and dijkstra find the same shortest paths.
  // CheckShortestClusterPaths(meta->n_threads, meta->graph, metric);
#endif
}

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
                                 sizeof(WayTaggedZones));
  LOG_S(INFO) << absl::StrFormat("sizeof(pair<int32_t,int32_t>): %4u",
                                 sizeof(std::pair<int32_t, int32_t>));
}

void PrintStats(const OsmPbfReader& reader, const MetaData& meta) {
  const Graph& graph = meta.graph;
  LOG_S(INFO) << "=========== Pbf Stats ============";
  LOG_S(INFO) << absl::StrFormat(
      "Nodes:              %12lld",
      reader.CountEntries(OsmPbfReader::ContentNodes));
  LOG_S(INFO) << absl::StrFormat(
      "Ways:               %12lld",
      reader.CountEntries(OsmPbfReader::ContentWays));
  LOG_S(INFO) << absl::StrFormat(
      "Relations:          %12lld",
      reader.CountEntries(OsmPbfReader::ContentRelations));

  LOG_S(INFO) << "========= Various Stats ==========";
  LOG_S(INFO) << absl::StrFormat("Num var-nodes:      %12lld",
                                 meta.node_table->total_records());
  LOG_S(INFO) << absl::StrFormat(
      "  bytes/var-node:   %12.2f",
      static_cast<double>(meta.node_table->mem_allocated()) /
          meta.node_table->total_records());
  LOG_S(INFO) << absl::StrFormat("Num turn restrictions:%10lld",
                                 meta.turn_restrictions.size());
  LOG_S(INFO) << absl::StrFormat("Num turn restr errors:%10lld",
                                 meta.stats.num_turn_restriction_errors);
  LOG_S(INFO) << absl::StrFormat("Ways with highw tag:%12lld",
                                 meta.stats.num_ways_with_highway_tag);
  LOG_S(INFO) << absl::StrFormat("Edges with highw tag:%11lld",
                                 meta.stats.num_edges_with_highway_tag);
  LOG_S(INFO) << absl::StrFormat("Noderefs with hw tag:%11lld",
                                 meta.stats.num_noderefs_with_highway_tag);

  LOG_S(INFO) << "========= Graph Stats ============";

  std::int64_t way_bytes = graph.ways.size() * sizeof(GWay);
  std::int64_t way_added_bytes = graph.unaligned_pool_.MemAllocated();
  LOG_S(INFO) << absl::StrFormat("Ways selected:      %12lld",
                                 graph.ways.size());

  LOG_S(INFO) << absl::StrFormat("  Nodes \"seen\":     %12lld",
                                 meta.way_nodes_seen->CountBits());

  LOG_S(INFO) << absl::StrFormat("  Nodes \"needed\":   %12lld",
                                 meta.way_nodes_needed->CountBits());

  uint64_t num_no_maxspeed = 0;
  uint64_t num_diff_maxspeed = 0;
  uint64_t num_has_country = 0;
  uint64_t num_has_streetname = 0;
  uint64_t num_oneway_car = 0;
  for (const GWay& w : graph.ways) {
    const WaySharedAttrs& wsa = GetWSA(graph, w);
    const RoutingAttrs ra_forw =
        GetRAFromWSA(wsa, VH_MOTOR_VEHICLE, DIR_FORWARD);
    const RoutingAttrs ra_backw =
        GetRAFromWSA(wsa, VH_MOTOR_VEHICLE, DIR_BACKWARD);

    if (RoutableAccess(ra_forw.access) && RoutableAccess(ra_backw.access) &&
        ra_forw.maxspeed != ra_backw.maxspeed) {
      num_diff_maxspeed += 1;
    }
    if ((RoutableAccess(ra_forw.access) && ra_forw.maxspeed == 0) ||
        (RoutableAccess(ra_backw.access) && ra_backw.maxspeed == 0)) {
      num_no_maxspeed += 1;
    }
    num_has_country += w.uniform_country;
    num_has_streetname += w.streetname == nullptr ? 0 : 1;
    num_oneway_car +=
        (wsa.ra[0].access == ACC_NO) != (wsa.ra[1].access == ACC_NO);
  }

  LOG_S(INFO) << absl::StrFormat("  No maxspeed:      %12lld", num_no_maxspeed);
  LOG_S(INFO) << absl::StrFormat("  Diff maxspeed/dir:%12lld",
                                 num_diff_maxspeed);
  LOG_S(INFO) << absl::StrFormat("  Has country:      %12lld", num_has_country);
  LOG_S(INFO) << absl::StrFormat("  Has no country:   %12lld",
                                 graph.ways.size() - num_has_country);
  LOG_S(INFO) << absl::StrFormat("  Has streetname:   %12lld",
                                 num_has_streetname);
  LOG_S(INFO) << absl::StrFormat("  Oneway for cars:  %12lld", num_oneway_car);
  LOG_S(INFO) << absl::StrFormat("  Closed ways:      %12lld",
                                 meta.stats.num_ways_closed);
  LOG_S(INFO) << absl::StrFormat("  Missing-nodes ways:%11lld",
                                 meta.stats.num_ways_missing_nodes);
  LOG_S(INFO) << absl::StrFormat("  Bytes per way     %12.2f",
                                 (double)way_bytes / graph.ways.size());
  LOG_S(INFO) << absl::StrFormat("  Added per way     %12.2f",
                                 (double)way_added_bytes / graph.ways.size());
  LOG_S(INFO) << absl::StrFormat("  Total Bytes:      %12lld",
                                 way_bytes + way_added_bytes);

  size_t num_edges_inverted = 0;
  size_t num_edges_out = 0;
  size_t num_non_unique_edges = 0;
  size_t num_no_country = 0;
  size_t max_edges_inverted = 0;
  size_t max_edges_out = 0;
  size_t node_in_cluster = 0;
  size_t node_in_small_component = 0;
  size_t min_edge_length_cm = INFU64;
  size_t max_edge_length_cm = 0;
  uint64_t sum_edge_length_cm = 0;
  for (size_t node_idx = 0; node_idx < graph.nodes.size(); ++node_idx) {
    const GNode& n = graph.nodes.at(node_idx);
    num_edges_inverted += n.num_edges_inverted;
    num_edges_out += n.num_edges_out;
    max_edges_inverted =
        std::max((uint64_t)n.num_edges_inverted, max_edges_inverted);
    max_edges_out = std::max((uint64_t)n.num_edges_out, max_edges_out);
    if (n.cluster_id != INVALID_CLUSTER_ID) {
      node_in_cluster++;
    }
    if (n.large_component == 0) {
      node_in_small_component++;
    }
    for (size_t edge_pos = 0; edge_pos < gnode_total_edges(n); ++edge_pos) {
      const GEdge& edge = n.edges[edge_pos];
      if (!edge.unique_other) {
        num_non_unique_edges++;
      }
      if (edge_pos < n.num_edges_out && edge.other_node_idx != node_idx) {
        sum_edge_length_cm += edge.distance_cm;
        if (edge.distance_cm == 0) {
          LOG_S(INFO) << "Edge with length 0 from " << n.node_id << " to "
                      << graph.nodes.at(edge.other_node_idx).node_id;
        }
        if (edge.distance_cm < min_edge_length_cm) {
          min_edge_length_cm = edge.distance_cm;
        }
        if (edge.distance_cm > max_edge_length_cm) {
          max_edge_length_cm = edge.distance_cm;
        }
      }
    }
    num_no_country += (n.ncc == INVALID_NCC ? 1 : 0);
  }

  LOG_S(INFO) << absl::StrFormat("Needed nodes:       %12lld",
                                 graph.nodes.size());
  LOG_S(INFO) << absl::StrFormat("  Num edges out:    %12lld", num_edges_out);
  LOG_S(INFO) << absl::StrFormat("  Num edges inverted: %10lld",
                                 num_edges_inverted);
  LOG_S(INFO) << absl::StrFormat("  Num non-unique:   %12lld",
                                 num_non_unique_edges);

  LOG_S(INFO) << absl::StrFormat("  Min edge length:  %12lld",
                                 min_edge_length_cm);
  LOG_S(INFO) << absl::StrFormat("  Max edge length:  %12lld",
                                 max_edge_length_cm);
  LOG_S(INFO) << absl::StrFormat("  Avg edge length   %12.0f",
                                 (double)sum_edge_length_cm / num_edges_out);

  LOG_S(INFO) << absl::StrFormat(
      "  Num edges/node:   %12.2f",
      static_cast<double>(num_edges_inverted + num_edges_out) /
          graph.nodes.size());
  LOG_S(INFO) << absl::StrFormat("  Max edges out:    %12lld", max_edges_out);
  LOG_S(INFO) << absl::StrFormat("  Max edges inverted: %10lld",
                                 max_edges_inverted);
  LOG_S(INFO) << absl::StrFormat("  Cross country edges:%10lld",
                                 meta.stats.num_cross_country_edges);
  LOG_S(INFO) << absl::StrFormat("  Num no country:   %12lld", num_no_country);
  LOG_S(INFO) << absl::StrFormat("  Deadend nodes:    %12lld",
                                 meta.stats.num_dead_end_nodes);
  LOG_S(INFO) << absl::StrFormat("  Node in cluster:  %12lld", node_in_cluster);
  LOG_S(INFO) << absl::StrFormat("  Node in small comp:%11lld",
                                 node_in_small_component);

  std::int64_t node_bytes = graph.nodes.size() * sizeof(GNode);
  std::int64_t node_added_bytes = graph.aligned_pool_.MemAllocated();
  LOG_S(INFO) << absl::StrFormat("  Bytes per node    %12.2f",
                                 (double)node_bytes / graph.nodes.size());
  LOG_S(INFO) << absl::StrFormat("  Added per node    %12.2f",
                                 (double)node_added_bytes / graph.nodes.size());
  LOG_S(INFO) << absl::StrFormat("  Total Bytes:      %12lld",
                                 node_bytes + node_added_bytes);

  LOG_S(INFO) << "========= Memory Stats ===========";
  LOG_S(INFO) << absl::StrFormat("Varnode memory:     %12.2f MB",
                                 meta.node_table->mem_allocated() / 1000000.0);
  LOG_S(INFO) << absl::StrFormat("Node graph memory:  %12.2f MB",
                                 (node_bytes + node_added_bytes) / 1000000.0);
  LOG_S(INFO) << absl::StrFormat("Way graph memory:   %12.2f MB",
                                 (way_bytes + way_added_bytes) / 1000000.0);
  LOG_S(INFO) << absl::StrFormat(
      "Total graph memory: %12.2f MB",
      (node_bytes + node_added_bytes + way_bytes + way_added_bytes) /
          1000000.0);
}

void PrintWayTagStats(const MetaData& meta) {
  const FrequencyTable& ft = meta.stats.way_tag_stats;
  const std::vector<FrequencyTable::Entry> v = ft.GetSortedElements();
  for (size_t i = 0; i < v.size(); ++i) {
    const FrequencyTable::Entry& e = v.at(i);
    char cc[3] = "--";  // Way not found, can easily happen because many ways
                        // are not stored in the graph.
    {
      const GWay* way = meta.graph.FindWay(e.example_id);
      if (way != nullptr) {
        strcpy(cc, ">1");
        if (way->uniform_country) {
          CountryNumToTwoLetter(way->ncc, cc);
        }
      }
    }

    RAW_LOG_F(INFO, "%7lu %10ld id:%10ld %s %s", i, e.ref_count, e.example_id,
              cc, std::string(e.key).c_str());
  }
  LOG_S(INFO) << absl::StrFormat(
      "Number of different ways tag configurations: %ld, total %ld (%.2f%%)",
      ft.TotalUnique(), ft.Total(), (100.0 * ft.TotalUnique()) / ft.Total());
}

void DoIt(const Argli& argli) {
  const std::string in_bpf = argli.GetString("pbf");
  const std::string admin_pattern = argli.GetString("admin_pattern");
  const std::string routing_config = argli.GetString("routing_config");
  const bool merge_tiny_clusters = argli.GetBool("merge_tiny_clusters");

  MetaData meta;
  meta.way_nodes_seen.reset(new HugeBitset);
  meta.way_nodes_needed.reset(new HugeBitset);
  meta.node_table.reset(new DataBlockTable);
  meta.stats.log_way_tag_stats = argli.GetBool("log_way_tag_stats");
  meta.stats.log_turn_restrictions = argli.GetBool("log_turn_restrictions");
  meta.n_threads = 8;

  // Reading is fastest with 7 threads on my hardware.
  OsmPbfReader reader(in_bpf, std::min(7, meta.n_threads));

  // Combine a few setup operations that can be done in parallel.
  {
    ThreadPool pool;
    // Read file structure of pbf file.
    pool.AddWork([&reader](int thread_idx) { reader.ReadFileStructure(); });
    // Read country polygons and initialise tiler.
    pool.AddWork([&meta, &admin_pattern](int thread_idx) {
      meta.tiler.reset(new TiledCountryLookup(
          admin_pattern,
          /*tile_size=*/TiledCountryLookup::kDegreeUnits / 5));
    });
    // Read routing config.
    pool.AddWork([&meta, &routing_config](int) {
      meta.per_country_config.reset(new PerCountryConfig);
      meta.per_country_config->ReadConfig(routing_config);
    });
    pool.Start(std::min(meta.n_threads, 3));
    pool.WaitAllFinished();
  }

  LoadNodeCoordinates(&reader, meta.node_table.get(), &meta.stats);

  {
    DeDuperWithIds<WaySharedAttrs> deduper;
    reader.ReadWays([&deduper, &meta](const OSMTagHelper& tagh,
                                      const OSMPBF::Way& way, std::mutex& mut) {
      ConsumeWayWorker(tagh, way, mut, &deduper, &meta);
    });
    {
      // Sort and build the vector with shared way attributes.
      deduper.SortByPopularity();
      meta.graph.way_shared_attrs = deduper.GetObjVector();
      const std::vector<uint32_t> mapping = deduper.GetSortMapping();
      for (GWay& w : meta.graph.ways) {
        w.wsa_id = mapping.at(w.wsa_id);
#if 0
        CHECK_S(memcmp(w.ra, meta.graph.way_shared_attrs.at(w.wsa_id).ra,
                       sizeof(w.ra)) == 0)
            << w.id;
#endif
      }
      LOG_S(INFO) << absl::StrFormat(
          "Shared way attributes de-duping %u -> %u (%.2f%%)",
          deduper.num_added(), deduper.num_unique(),
          (100.0 * deduper.num_unique()) / std::max(1u, deduper.num_added()));
    }
  }
  SortGWays(&meta);

  // TurnRestriction currently not used.
  reader.ReadRelations(
      [&meta](const OSMTagHelper& tagh, const OSMPBF::Relation& osm_rel,
              std::mutex& mut) { ConsumeRelation(tagh, osm_rel, mut, &meta); });

  AllocateGNodes(&meta);
  SetCountryInGNodes(&meta);

  // Now we know exactly which ways we have and which nodes are needed.
  // Creade edges.
  ComputeEdgeCounts(&meta);
  AllocateEdgeArrays(&meta);
  PopulateEdgeArrays(&meta);
  MarkUniqueEdges(&meta);

  // =========================================================================

  {
    NodeBuilder::VNode n;
    bool found = NodeBuilder::FindNode(*meta.node_table, 207718684, &n);
    LOG_S(INFO) << "Search node 207718684 expect lat:473492652 lon:87012469";
    LOG_S(INFO) << absl::StrFormat("Find node   %llu        lat:%llu lon:%llu",
                                   207718684, found ? n.lat : 0,
                                   found ? n.lon : 0);
  }

  FindLargeComponents(&meta.graph);
  ApplyTarjan(meta.graph, &meta);

  TestRoute(meta.graph);
  // RandomShortestPaths(meta.graph);

  ExecuteLouvain(merge_tiny_clusters, &meta);

  // Output.
  if (meta.stats.log_way_tag_stats) {
    PrintWayTagStats(meta);
  }
  WriteGraphToCSV(meta.graph, VH_MOTOR_VEHICLE, "/tmp/graph_motor_vehicle.csv");
  WriteGraphToCSV(meta.graph, VH_BICYCLE, "/tmp/graph_bicycle.csv");
  WriteCrossCountryEdges(&meta, "/tmp/cross.csv");
  PrintStructSizes();
  PrintStats(reader, meta);

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
}

}  // namespace build_graph

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  FUNC_TIMER();

  Argli argli(
      argc, argv,
      {
          {.name = "pbf",
           .type = "string",
           .positional = true,
           .required = true,
           .desc = "Input OSM pbf file (such as planet file)."},

          {.name = "admin_pattern",
           .type = "string",
           .dflt = "../../data/admin/??_*.csv",
           .desc = "Location of country boundary files."},

          {.name = "routing_config",
           .type = "string",
           .dflt = "../config/routing.cfg",
           .desc = "Location of routing config file."},

          {.name = "merge_tiny_clusters",
           .type = "bool",
           .dflt = "true",
           .desc = "Merge tiny clusters at country borders."},

          {.name = "log_way_tag_stats",
           .type = "bool",
           .desc = "Collect and print the way-tag-stats found in the data, "
                   "sorted by decreasing frequency."},

          {.name = "log_turn_restrictions",
           .type = "bool",
           .desc = "Log information about turn restrictions."},
      });

  build_graph::DoIt(argli);

  LOG_S(INFO) << "Finished.";
  return 0;
}
