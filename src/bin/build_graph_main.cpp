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
#include "algos/components.h"
#include "algos/dijkstra.h"
#include "algos/louvain.h"
#include "algos/tarjan.h"
#include "base/argli.h"
#include "base/country_code.h"
#include "base/huge_bitset.h"
#include "base/thread_pool.h"
#include "base/util.h"
#include "base/varbyte.h"
#include "geometry/distance.h"
#include "geometry/polygon.h"
#include "graph/build_graph.h"
#include "graph/data_block.h"
#include "graph/graph_def.h"
#include "graph/routing_attrs.h"
#include "osm-util/admin_boundary.h"
#include "osm-util/osm_helpers.h"
#include "osm-util/read_osm_pbf.h"

namespace build_graph {

void AddEdge(Graph& g, const size_t start_idx, const size_t other_idx,
             const bool out, const bool contra_way, const size_t way_idx,
             const std::uint64_t distance_cm) {
  GNode& n = g.nodes.at(start_idx);
  GEdge* edges = out ? n.edges : n.edges + n.num_edges_out;
  size_t num_edges = out ? n.num_edges_out : n.num_edges_in;
  for (size_t pos = 0; pos < num_edges; ++pos) {
    // Unused edges are marked with 'other_node_idx == INFU32'.
    if (edges[pos].other_node_idx == INFU32) {
      assert(other_idx != INFU32);
      edges[pos].other_node_idx = other_idx;
      edges[pos].way_idx = way_idx;
      edges[pos].distance_cm = distance_cm;
      edges[pos].unique_other = 0;
      edges[pos].bridge = 0;
      edges[pos].contra_way = contra_way ? 1 : 0;
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
  for (size_t i = 0; i < n->num_edges_out + n->num_edges_in; ++i) {
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

void WriteGraphToCSV(const Graph& g, const std::string& filename) {
  FuncTimer timer(absl::StrFormat("Write graph to %s", filename.c_str()));
  std::ofstream myfile;
  myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);

  size_t count = 0;
  std::string color;
  for (const GNode& n : g.nodes) {
    for (const GEdge& e : std::span(n.edges, n.num_edges_out)) {
      if (!e.unique_other) continue;
      const GNode& other = g.nodes.at(e.other_node_idx);
      const GWay& w = g.ways.at(e.way_idx);
      /*
      if (n.node_id == other.node_id) {
        LOG_S(INFO) << absl::StrFormat("Node %lld length %fm way %lld name %s",
      n.node_id, e.distance_cm / 100.0, w.id, w.streetname != nullptr ?
      w.streetname : "n/a");
      }
      */
      if (RoutableForward(w) && RoutableBackward(w) && n.lat > other.lat) {
        // Edges that have both directions will show up twice when iterating, so
        // ignore one of the two edges for this case.
        continue;
      }
      if (!n.large_component) {
        color = "mag";
      } else {
        bool has_maxspeed = g.ways.at(e.way_idx).ri[e.contra_way].maxspeed > 0;
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
  FuncTimer timer("ApplyTarjan()");
  if (g.large_components.empty()) {
    ABORT_S() << "g.large_components is empty";
  }
  Tarjan t(g);
  std::vector<Tarjan::BridgeInfo> bridges;
  for (const Graph::Component& comp : g.large_components) {
    bridges.clear();
    t.FindBridges(comp, &bridges);
    // Sort bridges in descending order of subtrees.
    std::sort(bridges.begin(), bridges.end(),
              [](const Tarjan::BridgeInfo& a, const Tarjan::BridgeInfo& b) {
                return a.subtree_size > b.subtree_size;
              });
    for (const Tarjan::BridgeInfo& bridge : bridges) {
      MarkBridgeEdge(g, bridge.from_node_idx, bridge.to_node_idx);
      MarkBridgeEdge(g, bridge.to_node_idx, bridge.from_node_idx);
      meta->hlp.num_dead_end_nodes +=
          MarkDeadEndNodes(g, bridge.to_node_idx, bridge.subtree_size);
    }
  }
  LOG_S(INFO) << absl::StrFormat(
      "Graph has %u (%.2f%%) dead end nodes.", meta->hlp.num_dead_end_nodes,
      (100.0 * meta->hlp.num_dead_end_nodes) / g.nodes.size());
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
        if (router.Route(start_idx, target_idx, RoutingMetricDistance())) {
          router.SaveSpanningTreeSegments("/tmp/route_dist.csv");
        } else {
          LOG_S(INFO) << "failed to find dijkstra route!";
        }
      }
      {
        DijkstraRouter router(g);
        if (router.Route(start_idx, target_idx, RoutingMetricTime())) {
          router.SaveSpanningTreeSegments("/tmp/route_time.csv");
        } else {
          LOG_S(INFO) << "failed to find dijkstra route!";
        }
      }
      {
        AStarRouter router(g);
        if (router.Route(start_idx, target_idx, RoutingMetricDistance())) {
          router.SaveSpanningTreeSegments("/tmp/astar_route_dist.csv");
        } else {
          LOG_S(INFO) << "failed to find astar route!";
        }
      }
      {
        AStarRouter router(g);
        if (router.Route(start_idx, target_idx, RoutingMetricTime())) {
          router.SaveSpanningTreeSegments("/tmp/astar_route_time.csv");
        } else {
          LOG_S(INFO) << "failed to find astar route!";
        }
      }
    }
  }
}

namespace {
struct IdPair {
  enum Code { Free, Normal, Reversed, Error };
  int64_t id1;
  int64_t id2;
  Code code = Free;
  void SetCode(Code new_code) {
    CHECK_NE_S(new_code, Free);
    code = new_code;
    if (new_code == Reversed) {
      std::swap(id1, id2);
    }
  }
};

void GetFrontAndBackNodeId(const GWay& w, int64_t* front_id, int64_t* back_id) {
  std::vector<uint64_t> ids = Graph::GetGWayNodeIds(w);
  CHECK_GE_S(ids.size(), 2);
  *front_id = ids.front();
  *back_id = ids.back();
}

bool CreateIdPair(const Graph& g, int64_t way_id, IdPair* pair) {
  size_t idx = g.FindWayIndex(way_id);
  if (idx >= g.ways.size()) {
    return false;
  }
  const GWay& way = g.ways.at(idx);
  GetFrontAndBackNodeId(way, &pair->id1, &pair->id2);
  return true;
}

// Find out if pair can be chained to predecessor_id.
// Return IdPair::Normal or IdPair::Reversed if pair can be connected, or
// IdPair::Error if it can't be connected.
IdPair::Code TryAddIdPair(int64_t predecessor_id, const IdPair& pair) {
  CHECK_EQ_S(pair.code, IdPair::Free);
  if (pair.id1 == predecessor_id) {
    return IdPair::Normal;
  } else if (pair.id2 == predecessor_id) {
    return IdPair::Reversed;
  }
  return IdPair::Error;
}

// Add a new pair to an existing chain. Return true if the new element could be
// added without error, false if there was an error.
bool AddIdPairToChain(const IdPair& p, std::vector<IdPair>* chain) {
  if (chain->empty() || chain->back().code == IdPair::Error) {
    chain->push_back(p);  // Leave in state 'Free'.
    return true;
  }
  IdPair::Code code = TryAddIdPair(chain->back().id2, p);
  if (chain->back().code == IdPair::Free) {
    chain->back().SetCode(IdPair::Normal);
    if (code == IdPair::Error) {
      code = TryAddIdPair(chain->back().id1, p);
      if (code != IdPair::Error) {
        chain->back().SetCode(IdPair::Reversed);
      }
    }
  }
  chain->push_back(p);
  chain->back().SetCode(code);
  return code != IdPair::Error;
}

bool FinalizeChain(std::vector<IdPair>* chain) {
  if (!chain->empty() && chain->back().code == IdPair::Free) {
    chain->back().SetCode(IdPair::Normal);
  }
  bool res = true;
  for (const IdPair& p : *chain) {
    CHECK_NE_S(p.code, IdPair::Free);
    res = res && (p.code != IdPair::Error);
  }
  return res;
}

std::string GetChainCodeString(const std::vector<IdPair>& chain) {
  std::string res;
  for (const IdPair& p : chain) {
    CHECK_NE_S(p.code, IdPair::Free);
    absl::StrAppend(&res, p.code == IdPair::Error    ? "X"
                          : p.code == IdPair::Normal ? "N"
                                                     : "R");
  }
  return res;
}

void LogMissingWay(std::string_view type, int64_t way_id, int64_t rel_id) {
  LOG_S(INFO) << absl::StrFormat("TR: '%s' way %lld in rel %lld not found",
                                 type, way_id, rel_id);
}

bool ConnectTurnRestriction(const MetaData& meta, TurnRestriction* tr) {
  IdPair from;
  if (!CreateIdPair(meta.graph, tr->from_way_id, &from)) {
    if (meta.hlp.log_turn_restrictions) {
      LogMissingWay("from", tr->from_way_id, tr->relation_id);
    }
    return false;
  }
  std::vector<IdPair> chain;
  AddIdPairToChain(from, &chain);
  if (tr->via_is_node) {
    AddIdPairToChain({.id1 = tr->via_ids.at(0), .id2 = tr->via_ids.at(0)},
                     &chain);
  } else {
    for (int64_t id : tr->via_ids) {
      IdPair via;
      if (!CreateIdPair(meta.graph, id, &via)) {
        if (meta.hlp.log_turn_restrictions) {
          LogMissingWay("via", id, tr->relation_id);
        }
        return false;
      }
      AddIdPairToChain(via, &chain);
    }
  }

  IdPair to;
  if (!CreateIdPair(meta.graph, tr->to_way_id, &to)) {
    if (meta.hlp.log_turn_restrictions) {
      LogMissingWay("to", tr->to_way_id, tr->relation_id);
    }
    return false;
  }
  AddIdPairToChain(to, &chain);
  bool res = FinalizeChain(&chain);
  if (meta.hlp.log_turn_restrictions) {
    LOG_S(INFO) << absl::StrFormat("TR: %u %s: %s match in relation %lld",
                                   chain.size(), res ? "success" : "error",
                                   GetChainCodeString(chain), tr->relation_id);
  }
  return res;
}
}  // namespace

void ConsumeRelation(const OSMTagHelper& tagh, const OSMPBF::Relation& osm_rel,
                     std::mutex& mut, MetaData* meta) {
  TurnRestriction tr;
  ResType rt = ParseTurnRestriction(tagh, osm_rel, &tr);
  if (rt == ResType::Success) {
    ConnectTurnRestriction(*meta, &tr);
    std::unique_lock<std::mutex> l(mut);
    meta->turn_restrictions.push_back(tr);
  } else if (rt == ResType::Error) {
    std::unique_lock<std::mutex> l(mut);
    meta->hlp.num_turn_restriction_errors++;
  }
}

namespace {

// Read the ways that might useful for routing, remember the nodes ids touched
// by these ways, then read the node coordinates and store them in 'nodes'.
void LoadNodeCoordinates(OsmPbfReader* reader, DataBlockTable* node_table) {
  HugeBitset touched_nodes_ids;
  // Read ways and remember the touched nodes in 'touched_nodes_ids'.
  reader->ReadWays([&touched_nodes_ids](const OSMTagHelper& tagh,
                                        const OSMPBF::Way& way,
                                        std::mutex& mut) {
    ConsumeWayStoreSeenNodesWorker(tagh, way, mut, &touched_nodes_ids);
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

  meta->hlp.num_cross_country_edges = 0;
  for (const GWay& way : meta->graph.ways) {
    if (way.uniform_country == 1) {
      continue;
    }
    const std::vector<size_t> node_idx =
        meta->graph.GetGWayNodeIndexes(meta->way_nodes_needed, way);
    for (size_t pos = 0; pos < node_idx.size() - 1; ++pos) {
      const GNode& n1 = meta->graph.nodes.at(node_idx.at(pos));
      const GNode& n2 = meta->graph.nodes.at(node_idx.at(pos + 1));
      uint16_t ncc1 = meta->tiler->GetCountryNum(n1.lon, n1.lat);
      uint16_t ncc2 = meta->tiler->GetCountryNum(n2.lon, n2.lat);
      if (ncc1 != ncc2) {
        meta->hlp.num_cross_country_edges++;
        myfile << absl::StrFormat("line,black,%d,%d,%d,%d\n", n1.lat, n1.lon,
                                  n2.lat, n2.lon);
      }
    }
  }

  myfile.close();
  LOG_S(INFO) << absl::StrFormat("Written %d lines to %s",
                                 meta->hlp.num_cross_country_edges, filename);
}

void SortGWays(MetaData* meta) {
  FuncTimer timer("Sort gways");
  // Sort by ascending way_id.
  std::sort(meta->graph.ways.begin(), meta->graph.ways.end(),
            [](const GWay& a, const GWay& b) { return a.id < b.id; });
}

void AllocateGNodes(MetaData* meta) {
  FuncTimer timer("AllocateGNodes");
  meta->graph.nodes.reserve(meta->way_nodes_needed.CountBits());
  NodeBuilder::GlobalNodeIter iter(meta->nodes);
  const NodeBuilder::VNode* node;
  while ((node = iter.Next()) != nullptr) {
    if (meta->way_nodes_needed.GetBit(node->id)) {
      GNode snode;
      snode.node_id = node->id;
      snode.lat = node->lat;
      snode.lon = node->lon;
      snode.num_edges_in = 0;
      snode.num_edges_out = 0;
      snode.dead_end = 0;
      snode.large_component = 0;
      meta->graph.nodes.push_back(snode);
    }
  }
}

void ComputeEdgeCountsWorker(size_t start_pos, size_t stop_pos, MetaData* meta,
                             std::mutex& mut) {
  Graph& graph = meta->graph;

  for (size_t way_idx = start_pos; way_idx < stop_pos; ++way_idx) {
    const GWay& way = graph.ways.at(way_idx);
    std::vector<size_t> node_idx =
        meta->graph.GetGWayNodeIndexes(meta->way_nodes_needed, way);
    {
      // Update edge counts.
      std::unique_lock<std::mutex> l(mut);
      int64_t prev_idx = -1;
      for (const size_t idx : node_idx) {
        if (prev_idx >= 0) {
          // TODO: self edges?
          // CHECK_NE_S(idx1, idx2) << way.id;

          if (RoutableForward(way) && RoutableBackward(way)) {
            graph.nodes.at(prev_idx).num_edges_out++;
            graph.nodes.at(idx).num_edges_out++;
          } else if (RoutableForward(way)) {
            graph.nodes.at(prev_idx).num_edges_out++;
            graph.nodes.at(idx).num_edges_in++;
          } else {
            CHECK_S(RoutableBackward(way)) << way.id;
            graph.nodes.at(prev_idx).num_edges_in++;
            graph.nodes.at(idx).num_edges_out++;
          }
        }
        prev_idx = idx;
      }
    }
  };
}

void ComputeEdgeCounts(MetaData* meta) {
  FuncTimer timer("Compute edge counts");
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
  FuncTimer timer("Allocate edge arrays");
  Graph& graph = meta->graph;
  for (GNode& n : meta->graph.nodes) {
    const std::uint32_t num_edges = snode_num_edges(n);
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
      if (meta->way_nodes_seen.GetBit(id)) {
        NodeBuilder::VNode node;
        if (!NodeBuilder::FindNode(meta->nodes, id, &node)) {
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
      if (meta->way_nodes_needed.GetBit(id)) {
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
      std::unique_lock<std::mutex> l(mut);
      int last_pos = -1;
      for (size_t pos = 0; pos < ids.size(); ++pos) {
        uint64_t id = ids.at(pos);
        if (meta->way_nodes_needed.GetBit(id)) {
          if (last_pos >= 0) {
            // Emit edge.
            std::size_t idx1 = node_idx.at(last_pos);
            std::size_t idx2 = node_idx.at(pos);
            uint64_t distance_cm = dist_sums.at(pos) - dist_sums.at(last_pos);
            // Store edges with the summed up distance.

            if (RoutableForward(way) && RoutableBackward(way)) {
              AddEdge(graph, idx1, idx2, /*out=*/true, /*contra_way=*/false,
                      way_idx, distance_cm);
              AddEdge(graph, idx2, idx1, /*out=*/true, /*contra_way=*/true,
                      way_idx, distance_cm);
            } else if (RoutableForward(way)) {
              AddEdge(graph, idx1, idx2, /*out=*/true, /*contra_way=*/false,
                      way_idx, distance_cm);
              AddEdge(graph, idx2, idx1, /*out=*/false, /*contra_way=*/false,
                      way_idx, distance_cm);
            } else {
              CHECK_S(RoutableBackward(way)) << way.id;
              AddEdge(graph, idx2, idx1, /*out=*/true, /*contra_way=*/true,
                      way_idx, distance_cm);
              AddEdge(graph, idx1, idx2, /*out=*/false, /*contra_way=*/true,
                      way_idx, distance_cm);
            }
          }
          last_pos = pos;
        }
      }
    }
  }
}

void PopulateEdgeArrays(MetaData* meta) {
  FuncTimer timer("PopulateEdgeArrays()");
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
  FuncTimer timer("Mark unique edges");
  for (GNode& n : meta->graph.nodes) {
    MarkUniqueOther(&n);
  }
}

namespace {
bool SelectNodeForLouvain(const MetaData& meta, const GNode& n) {
  return n.dead_end == 0 && n.large_component == 1;
  // && meta.tiler->GetCountryNum(n.lon, n.lat) == NCC_DE;
}

void AnalizeLouvainGraph(
    const Graph& g,
    const std::vector<std::unique_ptr<louvain::LouvainGraph>>& gvec) {
  const louvain::LouvainGraph& lg = *gvec.front();
  struct ClusterStats {
    uint32_t cluster_pos = 0;
    uint32_t num_nodes = 0;
    uint32_t num_border_nodes = 0;
    uint32_t num_in_edges = 0;
    uint32_t num_out_edges = 0;
  };
  std::vector<ClusterStats> stats(gvec.back()->clusters.size());

  for (uint32_t node_pos = 0; node_pos < lg.nodes.size(); ++node_pos) {
    const louvain::LouvainNode& n0 = lg.nodes.at(node_pos);
    uint32_t cluster_0 = FindFinalCluster(gvec, node_pos);
    ClusterStats& rec = stats.at(cluster_0);
    rec.cluster_pos = cluster_0;
    rec.num_nodes++;

    bool is_border_node = false;
    for (uint32_t p = n0.edge_start; p < n0.edge_start + n0.num_edges; ++p) {
      const louvain::LouvainEdge& e = lg.edges.at(p);
      const louvain::LouvainNode& n1 = lg.nodes.at(e.other_node_pos);
      uint32_t cluster_1 = FindFinalCluster(gvec, e.other_node_pos);
      if (cluster_0 == cluster_1) {
        rec.num_in_edges++;
      } else {
        rec.num_out_edges++;
        is_border_node = true;
      }
    }
    if (is_border_node) {
      rec.num_border_nodes++;
    }
  }

  std::sort(stats.begin(), stats.end(), [](const auto& a, const auto& b) {
    double va = (100.0 * a.num_out_edges) / std::max(a.num_in_edges, 1u);
    double vb = (100.0 * b.num_out_edges) / std::max(b.num_in_edges, 1u);
    return va > vb;
  });

  uint64_t table_size = 0;
  double sum_nodes = 0;
  double sum_border_nodes = 0;
  double sum_in = 0;   // Within-cluster edges, each edge is counted twice.
  double sum_out = 0;  // Outgoing edges, each edge is counted twice.
  uint32_t max_nodes = 0;
  uint32_t max_border_nodes = 0;
  uint32_t max_in = 0;
  uint32_t max_out = 0;
  uint64_t replacement_edges = 0;
  for (size_t i = 0; i < stats.size(); ++i) {
    const ClusterStats& rec = stats.at(i);

    table_size += (rec.num_out_edges * rec.num_out_edges);
    sum_nodes += rec.num_nodes;
    sum_border_nodes += rec.num_border_nodes;
    sum_in += rec.num_in_edges;
    sum_out += rec.num_out_edges;
    max_nodes = std::max(max_nodes, rec.num_nodes);
    max_border_nodes = std::max(max_border_nodes, rec.num_border_nodes);
    max_in = std::max(max_in, rec.num_in_edges);
    max_out = std::max(max_out, rec.num_out_edges);

    replacement_edges +=
        (rec.num_border_nodes * (rec.num_border_nodes - 1)) / 2;

    LOG_S(INFO) << absl::StrFormat(
        "Rank:%5u Cluster %4u: Nodes:%5u Border:%5u In-Edges:%5u Out-Edges:%5u "
        "Out/In:%2.2f%%",
        i, rec.cluster_pos, rec.num_nodes, rec.num_border_nodes,
        rec.num_in_edges, rec.num_out_edges,
        (100.0 * rec.num_out_edges) / std::max(rec.num_in_edges, 1u));
  }
  LOG_S(INFO) << absl::StrFormat("Table size %u, %.3f per node in graph",
                                 table_size,
                                 (table_size + 0.0) / lg.nodes.size());
  LOG_S(INFO) << absl::StrFormat(
      "  Sum nodes:%.0f sum border:%.0f sum in-edges:%.0f sum out-edges:%.0f",
      sum_nodes, sum_border_nodes, sum_in, sum_out);
  LOG_S(INFO) << absl::StrFormat(
      "  Avg nodes:%.1f avg border:%.1f avg in-edges:%.1f avg out-edges:%.1f",
      sum_nodes / stats.size(), sum_border_nodes / stats.size(),
      sum_in / stats.size(), sum_out / stats.size());
  LOG_S(INFO) << absl::StrFormat(
      "  Max nodes:%u max border:%u max in-edges:%u max out-edges:%u",
      max_nodes, max_border_nodes, max_in, max_out);

  // Cluster Graph Summary
  uint64_t total_edges = replacement_edges + (uint64_t)sum_out / 2;
  uint64_t total_nodes = (uint64_t)sum_border_nodes;
  LOG_S(INFO) << absl::StrFormat(
      "  Cluster-Graph: added edges:%llu total edges:%llu (%.1f%%)  total "
      "nodes:%llu (%.1f%%)",
      replacement_edges, total_edges,
      (100.0 * total_edges) / ((sum_in + sum_out) / 2), total_nodes,
      (100.0 * total_nodes) / sum_nodes);
}

void WriteLouvainGraph(
    const Graph& g,
    const std::vector<std::unique_ptr<louvain::LouvainGraph>>& gvec,
    const std::string& filename) {
  LOG_S(INFO) << absl::StrFormat("Write louvain graph to %s", filename.c_str());
  std::ofstream myfile;
  myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);

  const louvain::LouvainGraph& lg = *gvec.front();
  uint64_t count = 0;
  for (uint32_t node_pos = 0; node_pos < lg.nodes.size(); ++node_pos) {
    const louvain::LouvainNode& n0 = lg.nodes.at(node_pos);
    uint32_t cluster_0 = FindFinalCluster(gvec, node_pos);

    for (uint32_t p = n0.edge_start; p < n0.edge_start + n0.num_edges; ++p) {
      const louvain::LouvainEdge& e = lg.edges.at(p);
      const louvain::LouvainNode& n1 = lg.nodes.at(e.other_node_pos);
      uint32_t cluster_1 = FindFinalCluster(gvec, e.other_node_pos);
      std::string_view color = "mag";  // edges between clusters
      if (cluster_0 == cluster_1) {
        // edge within cluster.
        constexpr int32_t kMaxColor = 16;
        static std::string_view colors[kMaxColor] = {
            "blue",  "green",  "red",    "black", "yel",   "violet",
            "olive", "lblue",  "dgreen", "dred",  "brown", "grey",
            "gblue", "orange", "lgreen", "pink",
        };

        color = colors[cluster_0 % kMaxColor];
      }

      if (&n0 >= &n1) continue;  // Ignore half of the edges.
      const GNode& gn0 = g.nodes.at(n0.back_ref);
      const GNode& gn1 = g.nodes.at(n1.back_ref);
      myfile << absl::StrFormat("line,%s,%d,%d,%d,%d\n", color, gn0.lat,
                                gn0.lon, gn1.lat, gn1.lon);
      count++;
    }
  }

  myfile.close();
  LOG_S(INFO) << absl::StrFormat("Written %d lines to %s", count, filename);
}

}  // namespace

// Experimental.
void ExecuteLouvain(MetaData* meta) {
  FuncTimer timer("ExecuteLouvain()");
  constexpr double dfl_total_edge_weight = 5000000;

  // Nodes that are eligible because they are in country, not in dead end, etc.
  HugeBitset eligible_nodes;
  for (uint32_t np = 0; np < meta->graph.nodes.size(); ++np) {
    if (SelectNodeForLouvain(*meta, meta->graph.nodes.at(np))) {
      eligible_nodes.SetBit(np, true);
    }
  }

  // 'np_to_louvain_pos' contains a mapping from a node position in
  // meta->graph.nodes to the precomputed node position in the louvain graph.
  // The map is sorted by key and ascending order, and by construction, the
  // pointed to values are also sorted.
  //
  // Keys are only inserted  for eligible nodes that have at least one edge to
  // another eligible node.
  //
  // Note that when iterating, the keys *and* the values will appear in
  // increasing order.
  absl::btree_map<uint32_t, uint32_t> np_to_louvain_pos;
  for (uint32_t np = 0; np < meta->graph.nodes.size(); ++np) {
    if (eligible_nodes.GetBit(np)) {
      const GNode& n = meta->graph.nodes.at(np);
      for (size_t ep = 0; ep < n.num_edges_out + n.num_edges_in; ++ep) {
        const GEdge& e = n.edges[ep];
        if (e.unique_other && e.other_node_idx != np &&
            eligible_nodes.GetBit(e.other_node_idx)) {
          // Edge between two eligible nodes.
          // Node 'n' at position 'np' is good to use.
          np_to_louvain_pos[np] = np_to_louvain_pos.size();
          break;
        }
      }
    }
  }

  std::vector<std::unique_ptr<louvain::LouvainGraph>> gvec;
  gvec.push_back(std::make_unique<louvain::LouvainGraph>());
  louvain::LouvainGraph* g = gvec.back().get();

  // Create initial Louvain graph.
  for (auto [np, louvain_pos] : np_to_louvain_pos) {
    g->AddNode(louvain_pos, np);
    const GNode& n = meta->graph.nodes.at(np);
    for (size_t ep = 0; ep < n.num_edges_out + n.num_edges_in; ++ep) {
      const GEdge& e = n.edges[ep];
      if (e.unique_other && e.other_node_idx != np &&
          eligible_nodes.GetBit(e.other_node_idx)) {
        auto it = np_to_louvain_pos.find(e.other_node_idx);
        CHECK_S(it != np_to_louvain_pos.end());
        const GWay& way = meta->graph.ways.at(e.way_idx);
        if (way.highway_label == HW_RESIDENTIAL ||
            way.highway_label == HW_LIVING_STREET ||
            way.highway_label == HW_SERVICE) {
          g->AddEdge(it->second, /*weight=*/1);
        } else {
          g->AddEdge(it->second, /*weight=*/1);
        }
      }
    }
    CHECK_GT_S(g->nodes.back().num_edges, 0);
  }

  LOG_S(INFO) << absl::StrFormat("Louvain nodes: %12d", g->nodes.size());
  LOG_S(INFO) << absl::StrFormat("Louvain edges: %12d", g->edges.size());

  {
    louvain::NodeLineRemover::ClusterLineNodes(g);
    RemoveEmptyClusters(g);
    gvec.push_back(std::make_unique<louvain::LouvainGraph>());
    CreateClusterGraph(*g, gvec.back().get());
    g = gvec.back().get();
    g->SetTotalEdgeWeight(dfl_total_edge_weight);
  }

  LOG_S(INFO) << absl::StrFormat("Louvain nodes (no line nodes): %12d",
                                 g->nodes.size());
  LOG_S(INFO) << absl::StrFormat("Louvain edges (no line nodes): %12d",
                                 g->edges.size());

  constexpr int MaxLevel = 20;
  for (int level = 0; level < MaxLevel; ++level) {
    g->Validate();

    uint32_t prev_moves = INFU32;
    uint32_t prev_empty = 0;
    for (int step = 0; step < 40; ++step) {
      uint32_t moves = g->Step();
      if (g->empty_clusters_ <= prev_empty && moves >= prev_moves) {
        break;
      }
      prev_moves = moves;
      prev_empty = g->empty_clusters_;

      LOG_S(INFO) << absl::StrFormat(
          "Level %d Step %d moves:%u #clusters:%u empty:%u", level, step, moves,
          g->clusters.size(), g->empty_clusters_);
      if (moves == 0) {
        if (step == 0) {
          level = MaxLevel - 1;  // Complete stop.
        }
        break;
      }
    }
    RemoveEmptyClusters(g);
    if (level < MaxLevel - 1) {
      // Create a new level.
      gvec.push_back(std::make_unique<louvain::LouvainGraph>());
      CreateClusterGraph(*g, gvec.back().get());
      g = gvec.back().get();
      g->SetTotalEdgeWeight(dfl_total_edge_weight);
    }
  }

  WriteLouvainGraph(meta->graph, gvec, "/tmp/louvain.csv");
  AnalizeLouvainGraph(meta->graph, gvec);
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
  LOG_S(INFO) << absl::StrFormat("sizeof(WayRural):              %4u",
                                 sizeof(WayRural));
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
                                 meta.nodes.total_records());
  LOG_S(INFO) << absl::StrFormat(
      "  bytes/var-node:   %12.2f",
      static_cast<double>(meta.nodes.mem_allocated()) /
          meta.nodes.total_records());
  LOG_S(INFO) << absl::StrFormat("Num turn restricts: %12lld",
                                 meta.turn_restrictions.size());
  LOG_S(INFO) << absl::StrFormat("Num turn restr errors:%10lld",
                                 meta.hlp.num_turn_restriction_errors);

  LOG_S(INFO) << "========= Graph Stats ============";
  size_t num_edges_in = 0;
  size_t num_edges_out = 0;
  size_t max_edges_in = 0;
  size_t max_edges_out = 0;
  for (const GNode& n : graph.nodes) {
    num_edges_in += n.num_edges_in;
    num_edges_out += n.num_edges_out;
    max_edges_in = std::max((uint64_t)n.num_edges_in, max_edges_in);
    max_edges_out = std::max((uint64_t)n.num_edges_out, max_edges_out);
  }

  uint64_t num_diff_maxspeed = 0;
  uint64_t num_has_country = 0;
  uint64_t num_has_streetname = 0;
  uint64_t num_oneway = 0;
  for (const GWay& w : graph.ways) {
    if (RoutableForward(w) && RoutableBackward(w) &&
        w.ri[0].maxspeed != w.ri[1].maxspeed) {
      num_diff_maxspeed += 1;
    }
    num_has_country += w.uniform_country;
    num_has_streetname += w.streetname == nullptr ? 0 : 1;
    num_oneway += w.dir_dontuse == DIR_BOTH ? 0 : 1;
  }

  LOG_S(INFO) << absl::StrFormat("Num nodes:          %12lld",
                                 graph.nodes.size());
  LOG_S(INFO) << absl::StrFormat("  Num edges out:    %12lld", num_edges_out);
  LOG_S(INFO) << absl::StrFormat("  Num edges in:     %12lld", num_edges_in);
  LOG_S(INFO) << absl::StrFormat(
      "  Num edges/node   %12.2f",
      static_cast<double>(num_edges_in + num_edges_out) / graph.nodes.size());
  LOG_S(INFO) << absl::StrFormat("  Max edges out:    %12lld", max_edges_out);
  LOG_S(INFO) << absl::StrFormat("  Max edges in:     %12lld", max_edges_in);
  LOG_S(INFO) << absl::StrFormat("  Cross country edges:%10lld",
                                 meta.hlp.num_cross_country_edges);

  LOG_S(INFO) << absl::StrFormat("  Deadend nodes:    %12lld",
                                 meta.hlp.num_dead_end_nodes);

  std::int64_t node_bytes = graph.nodes.size() * sizeof(GNode);
  std::int64_t node_added_bytes = graph.aligned_pool_.MemAllocated();
  LOG_S(INFO) << absl::StrFormat("  Bytes per node    %12.2f",
                                 (double)node_bytes / graph.nodes.size());
  LOG_S(INFO) << absl::StrFormat("  Added per node    %12.2f",
                                 (double)node_added_bytes / graph.nodes.size());

  std::int64_t way_bytes = graph.ways.size() * sizeof(GWay);
  std::int64_t way_added_bytes = graph.unaligned_pool_.MemAllocated();
  LOG_S(INFO) << absl::StrFormat("Num ways:          %12lld",
                                 graph.ways.size());

  LOG_S(INFO) << absl::StrFormat("  Way nodes:       %12lld",
                                 meta.way_nodes_seen.CountBits());

  LOG_S(INFO) << absl::StrFormat("  Routing nodes:   %12lld",
                                 meta.way_nodes_needed.CountBits());

  LOG_S(INFO) << absl::StrFormat("  Diff maxspeed/dir:%11lld",
                                 num_diff_maxspeed);
  LOG_S(INFO) << absl::StrFormat("  Has country:     %12lld", num_has_country);
  LOG_S(INFO) << absl::StrFormat("  Has streetname:  %12lld",
                                 num_has_streetname);
  LOG_S(INFO) << absl::StrFormat("  Oneway:          %12lld", num_oneway);
  LOG_S(INFO) << absl::StrFormat("  Closed ways:     %12lld",
                                 meta.hlp.num_ways_closed);
  LOG_S(INFO) << absl::StrFormat("  Deleted ways:    %12lld",
                                 meta.hlp.num_ways_deleted);
  LOG_S(INFO) << absl::StrFormat("  Bytes per way    %12.2f",
                                 (double)way_bytes / graph.ways.size());
  LOG_S(INFO) << absl::StrFormat("  Added per way    %12.2f",
                                 (double)way_added_bytes / graph.ways.size());
  LOG_S(INFO) << "========= Memory Stats ===========";
  LOG_S(INFO) << absl::StrFormat("Varnode memory:    %12.2f Gib",
                                 meta.nodes.mem_allocated() / 1000000000.0);
  LOG_S(INFO) << absl::StrFormat(
      "Node graph memory: %12.2f Gib",
      (node_bytes + node_added_bytes) / 1000000000.0);
  LOG_S(INFO) << absl::StrFormat("Way graph memory:  %12.2f Gib",
                                 (way_bytes + way_added_bytes) / 1000000000.0);
  LOG_S(INFO) << absl::StrFormat(
      "Total graph memory:%12.2f Gib",
      (node_bytes + node_added_bytes + way_bytes + way_added_bytes) /
          1000000000.0);
}

void PrintWayTagStats(const MetaData& meta) {
  int64_t total_count = 0;
  std::vector<std::pair<WayTagStat, std::string_view>> v;
  for (auto const& [key, val] : meta.hlp.way_tag_statmap) {
    v.emplace_back(val, key);
    total_count += val.count;
  }
  std::sort(v.begin(), v.end(), [](const auto& a, const auto& b) {
    return a.first.count > b.first.count;
  });
  for (size_t i = 0; i < v.size(); ++i) {
    const WayTagStat& stat = v.at(i).first;
    char cc[3] = "nw";
    {
      size_t idx = meta.graph.FindWayIndex(stat.example_way_id);
      if (idx < meta.graph.ways.size()) {
        strcpy(cc, "--");
        const GWay& way = meta.graph.ways.at(idx);
        if (way.uniform_country) {
          CountryNumToTwoLetter(way.ncc, cc);
        }
      }
    }

    RAW_LOG_F(INFO, "%7lu %10ld id:%10ld %s %s", i, stat.count,
              stat.example_way_id, cc, std::string(v.at(i).second).c_str());
  }
  LOG_S(INFO) << absl::StrFormat(
      "Number of different ways tag configurations: %ld, total %ld (%.2f%%)",
      meta.hlp.way_tag_statmap.size(), total_count,
      (100.0 * meta.hlp.way_tag_statmap.size()) / total_count);
}

void DoIt(const Argli& argli) {
  const std::string in_bpf = argli.GetString("pbf");
  const std::string admin_pattern = argli.GetString("admin_pattern");
  const std::string routing_config = argli.GetString("routing_config");

  MetaData meta;
  meta.hlp.way_tag_stats = argli.GetBool("way_tag_stats");
  meta.hlp.log_turn_restrictions = argli.GetBool("log_turn_restrictions");

  OsmPbfReader reader(in_bpf, meta.n_threads);

  // Combine a few setup operations that can be done in parallel.
  {
    ThreadPool pool;
    // Read file structure of pbf file.
    pool.AddWork([&reader](int thread_idx) { reader.ReadFileStructure(); });
    // Read country polygons and initialise tiler.
    pool.AddWork([&meta, &admin_pattern](int thread_idx) {
      meta.tiler = new TiledCountryLookup(
          admin_pattern,
          /*tile_size=*/TiledCountryLookup::kDegreeUnits / 5);
    });
    // Read routing config.
    pool.AddWork([&meta, &routing_config](int) {
      meta.per_country_config.ReadConfig(routing_config);
    });
    pool.Start(std::min(meta.n_threads, 3));
    pool.WaitAllFinished();
  }

  LoadNodeCoordinates(&reader, &meta.nodes);

  reader.ReadWays(
      [&meta](const OSMTagHelper& tagh, const OSMPBF::Way& way,
              std::mutex& mut) { ConsumeWayWorker(tagh, way, mut, &meta); });
  SortGWays(&meta);

  /*
  // TurnRestriction currently not used.
  reader.ReadRelations(
      [&meta](const OSMTagHelper& tagh, const OSMPBF::Relation& osm_rel,
              std::mutex& mut) { ConsumeRelation(tagh, osm_rel, mut, &meta); });
  */

  AllocateGNodes(&meta);

  // Now we know exactly which ways we have and which nodes are needed.
  // Creade edges.
  ComputeEdgeCounts(&meta);
  AllocateEdgeArrays(&meta);
  PopulateEdgeArrays(&meta);
  MarkUniqueEdges(&meta);

  // =========================================================================

  {
    NodeBuilder::VNode n;
    bool found = NodeBuilder::FindNode(meta.nodes, 207718684, &n);
    LOG_S(INFO) << "Search node 207718684 expect lat:473492652 lon:87012469";
    LOG_S(INFO) << absl::StrFormat("Find node   %llu        lat:%llu lon:%llu",
                                   207718684, found ? n.lat : 0,
                                   found ? n.lon : 0);
  }

  FindLargeComponents(&meta.graph);
  ApplyTarjan(meta.graph, &meta);
  TestRoute(meta.graph);
  ExecuteLouvain(&meta);

  // Output.
  if (meta.hlp.way_tag_stats) {
    PrintWayTagStats(meta);
  }
  WriteGraphToCSV(meta.graph, "/tmp/graph.csv");
  WriteCrossCountryEdges(&meta, "/tmp/cross.csv");
  PrintStructSizes();
  PrintStats(reader, meta);
}

}  // namespace build_graph

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  FuncTimer timer("main()");

  // absl::ParseCommandLine(argc, argv);
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
          {.name = "way_tag_stats",
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
