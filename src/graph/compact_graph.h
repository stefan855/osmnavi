#pragma once

#include <vector>

#include "absl/container/flat_hash_map.h"
#include "algos/routing_defs.h"
#include "algos/routing_metric.h"
#include "base/deduper_with_ids.h"
#include "base/util.h"
#include "graph/graph_def.h"
#include "graph/turn_costs.h"
#include "osm/turn_restriction_defs.h"

// This is a compact, i.e. memory efficient view on the nodes and edges of a
// graph, with precomputed weights for every edge. The nodes in the graph have
// ids 0..num_nodes()-1.
class CompactDirectedGraph {
 public:
  // Edges that are used as input to build the compact graph.
  struct FullEdge {
    uint32_t from_c_idx;
    uint32_t to_c_idx;
    uint32_t weight;
    uint32_t way_idx : WAY_IDX_BITS;  // 31 bits.
    uint32_t restricted_access : 1;
    uint32_t uturn_allowed : 1;
  };

  // Edge type used for edges of the compact graph.
  struct PartialEdge {
    uint32_t to_c_idx;
    uint32_t weight;
    uint32_t way_idx : WAY_IDX_BITS;  // 31 bits.
    // Edge has restricted car_label (LABEL_RESTRICTED or
    // LABEL_RESTRICTED_SECONDARY) set.
    uint32_t restricted_access : 1;
    // If a u-turn (i.e. return from to_c_idx to the origin of this edge) is
    // allowed or not.
    uint32_t uturn_allowed : 1;
    // '1' iff this edge a trigger for a simple turn restriction.
    uint32_t simple_tr_trigger : 1;
    // '1' iff this edge a trigger for a complex turn restriction.
    uint32_t complex_tr_trigger : 1;

    // Index into turn_costs_ vector.
    std::uint32_t turn_cost_idx : MAX_TURN_COST_IDX_BITS;
  };

  // A complex turn restriction.
  struct ComplexTurnRestriction final {
    // List of edge indexes. Contains at least 3 entries.
    std::vector<uint32_t> path;
    // True if the last edge in the path is forbidden, false if the last edge is
    // mandatory.
    bool forbidden;

    uint32_t GetTriggerEdgeIdx() const {
      CHECK_GE_S(path.size(), 3);
      return path.front();
    }
  };

  // Create a graph with the given number of nodes and edges in 'full_edges'.
  // 'full_edges' must be sorted non-decreasing by (from_c_idx, to_c_idx,
  // way_idx, restricted_access), see SortAndCleanupEdges().
  //
  // Note: this file implements operator "<" for full edges, i.e. you can sort
  // the edges with std::sort(full_edges.begin(), full_edges.end());
  CompactDirectedGraph(uint32_t num_nodes,
                       const std::vector<FullEdge>& full_edges)
      : num_nodes_(num_nodes) {
    turn_costs_.push_back(TurnCostData(32, TURN_COST_ZERO_COMPRESSED));
    BuildGraph(full_edges);
  }

  // Return the number of nodes in the graph. Ids are 0..num_nodes()-1.
  // The ids 0..cluster.border_nodes.size()-1 represent the border nodes.
  uint32_t num_nodes() const { return num_nodes_; }

  // For every node, contains the start position of its edges in the edges()
  // vector. The last element does not correspond to a node, it has the value
  // edges().size().
  const std::vector<uint32_t>& edges_start() const { return edges_start_; }

  // Sorted edge vector contains all edges of all nodes. The edges of node k are
  // in positions edges_start()[k]..edges_start()[k+1] - 1.
  const std::vector<PartialEdge>& edges() const { return edges_; }

  const std::vector<TurnCostData>& turn_costs() const { return turn_costs_; }

  // Return the position of the edge (from_node, to_node) in edges().
  // Returns -1 in case the edge does not exist.
  const int64_t FindEdge(uint32_t from_node, uint32_t to_node,
                         int64_t way_idx = -1) const {
    for (size_t i = edges_start_.at(from_node);
         i < edges_start_.at(from_node + 1); ++i) {
      if (edges_.at(i).to_c_idx == to_node &&
          (way_idx < 0 || edges_.at(i).way_idx == way_idx)) {
        return i;
      }
    }
    return -1;
  }

  // Sort the edges by ascending order (from_c_idx, to_c_idx, way_idx,
  // restricted,
  // weight) and remove duplicates (from, to, way_idx, restricted) keeping the
  // one with the lowest weight.
  static void SortAndCleanupEdges(
      std::vector<CompactDirectedGraph::FullEdge>* full_edges) {
    std::sort(full_edges->begin(), full_edges->end());
    // Remove dups.
    auto last = std::unique(full_edges->begin(), full_edges->end(),
                            [](const CompactDirectedGraph::FullEdge& a,
                               const CompactDirectedGraph::FullEdge& b) {
                              return a.to_c_idx == b.to_c_idx &&
                                     a.from_c_idx == b.from_c_idx &&
                                     a.way_idx == b.way_idx &&
                                     a.restricted_access == b.restricted_access;
                            });
    if (last != full_edges->end()) {
      full_edges->erase(last, full_edges->end());
    }
  }

  // Given a compact node map (mapping graph.nodes indices to compact graph node
  // indices [0..num_nodes-1]), return a vector with the inverse map.
  static std::vector<std::uint32_t> InvertGraphToCompactNodeMap(
      const absl::flat_hash_map<uint32_t, uint32_t>& graph_to_compact_nodemap) {
    const uint32_t num_nodes = graph_to_compact_nodemap.size();
    std::vector<std::uint32_t> node_refs;
    node_refs.assign(num_nodes, INFU32);
    for (auto [graph_idx, compact_idx] : graph_to_compact_nodemap) {
      node_refs.at(compact_idx) = graph_idx;
    }
    // Sanity check.
    for (size_t i = 0; i < num_nodes; ++i) {
      CHECK_NE_S(node_refs.at(i), INFU32);
    }
    return node_refs;
  }

 private:
  // Compute turn costs for edge 'in_ce'.
  TurnCostData ConvertTurnCostsAtEdge(
      const Graph& g, const PartialEdge& in_ce, const GEdge& in_ge,
      const std::vector<std::uint32_t>& compact_to_graph) const {
    const TurnCostData& g_tcd = g.turn_costs.at(in_ge.turn_cost_idx);
    CHECK_EQ_S(g.nodes.at(in_ge.target_idx).num_forward_edges,
               g_tcd.turn_costs.size());

    const uint32_t c_start = edges_start_.at(in_ce.to_c_idx);
    const uint32_t c_stop = edges_start_.at(in_ce.to_c_idx + 1);
    // TurnCostData tcd{{c_stop - c_start, TURN_COST_ZERO_COMPRESSED}};
    TurnCostData tcd(c_stop - c_start, TURN_COST_ZERO_COMPRESSED);

    for (uint32_t ce_idx = c_start; ce_idx < c_stop; ++ce_idx) {
      // Find corresponding edge in graph 'g'.
      const PartialEdge& out_ce = edges_.at(ce_idx);
      uint32_t g_off = gnode_find_forward_edge_offset(
          g, in_ge.target_idx, compact_to_graph.at(out_ce.to_c_idx),
          out_ce.way_idx);
      // Copy turn cost for this one edge.
      tcd.turn_costs.at(ce_idx - c_start) = g_tcd.turn_costs.at(g_off);
    }
    return tcd;
  }
#if 0
  void ConvertTurnCostsAtEdge(
      const Graph& g, const PartialEdge& in_ce, const GEdge& in_ge,
      const std::vector<std::uint32_t>& compact_to_graph,
      TurnCostData* tcd) const {
    const TurnCostData& g_tcd = g.turn_costs.at(in_ge.turn_cost_idx);
    CHECK_EQ_S(g.nodes.at(in_ge.target_idx).num_forward_edges,
               g_tcd.turn_costs.size());

    const uint32_t c_start = edges_start_.at(in_ce.to_c_idx);
    const uint32_t c_stop = edges_start_.at(in_ce.to_c_idx + 1);
    tcd->turn_costs.assign(c_stop - c_start, TURN_COST_ZERO_COMPRESSED);

    for (uint32_t ce_idx = c_start; ce_idx < c_stop; ++ce_idx) {
      // Find corresponding edge in graph 'g'.
      const PartialEdge& out_ce = edges_.at(ce_idx);
      uint32_t g_off = gnode_find_forward_edge_offset(
          g, in_ge.target_idx, compact_to_graph.at(out_ce.to_c_idx),
          out_ce.way_idx);
      // Copy turn cost for this one edge.
      tcd->turn_costs.at(ce_idx - c_start) = g_tcd.turn_costs.at(g_off);
    }
  }
#endif

 public:
  // Add the turn costs that are stored with every outgoing edge in 'g' to the
  // compact graph.
  void AddTurnCosts(
      const Graph& g, bool is_time_metric,
      const absl::flat_hash_map<uint32_t, uint32_t>& graph_to_compact_nodemap) {
    FUNC_TIMER();

    CHECK_EQ_S(graph_to_compact_nodemap.size(), num_nodes_);
    const std::vector<std::uint32_t> compact_to_graph =
        InvertGraphToCompactNodeMap(graph_to_compact_nodemap);

    DeDuperWithIds<TurnCostData> deduper;
    // TurnCostData tcd;
    for (uint32_t cn_idx = 0; cn_idx < num_nodes_; cn_idx++) {
      for (uint32_t ce_idx = edges_start_.at(cn_idx);
           ce_idx < edges_start_.at(cn_idx + 1); ce_idx++) {
        // Compute turn costs for 'c_edge' arriving at its target node.
        PartialEdge& in_ce = edges_.at(ce_idx);
        const GEdge& in_ge =
            gnode_find_edge(g, compact_to_graph.at(cn_idx),
                            compact_to_graph.at(in_ce.to_c_idx), in_ce.way_idx);
        TurnCostData tcd = ConvertTurnCostsAtEdge(g, in_ce, in_ge,
                                                  compact_to_graph /*, &tcd*/);

        // No turn costs except blocked turns for non-time metrics.
        if (!is_time_metric) {
          for (uint32_t pos = 0; pos < tcd.turn_costs.size(); ++pos) {
            if (tcd.turn_costs.at(pos) != TURN_COST_INFINITY_COMPRESSED) {
              tcd.turn_costs.at(pos) = TURN_COST_ZERO_COMPRESSED;
            }
          }
        }

        in_ce.turn_cost_idx = deduper.Add(tcd);
      }
    }

    CHECK_LT_S(deduper.num_unique(), MAX_TURN_COST_IDX);
    deduper.SortByPopularity();
    auto sort_mapping = deduper.GetSortMapping();
    for (PartialEdge& ce : edges_) {
      ce.turn_cost_idx = sort_mapping.at(ce.turn_cost_idx);
    }
    turn_costs_ = deduper.GetObjVector();
    LOG_S(INFO) << absl::StrFormat(
        "DeDuperWithIds<TurnCostData>: unique:%u tot:%u", deduper.num_unique(),
        deduper.num_added());
  }

  // Add complex turn restrictions from the standard Graph format, which means
  // that node references have to be converted to reference within the
  // compact_graph.
  void AddComplexTurnRestrictions(
      const std::vector<TurnRestriction>& graph_based_trs,
      const absl::flat_hash_map<uint32_t, uint32_t>& graph_to_compact_nodemap) {
    // Fill complex_trs_.
    for (const TurnRestriction& g_tr : graph_based_trs) {
      ComplexTurnRestriction cg_tr = {.forbidden = g_tr.forbidden};

      for (const TurnRestriction::TREdge& tr_edge : g_tr.path) {
        TurnRestriction::TREdge cg_edge;
        if (!ConvertGraphBasedTurnRestrictionEdge(
                tr_edge, graph_to_compact_nodemap, &cg_edge)) {
          break;
        }
        cg_tr.path.push_back(cg_edge.edge_idx);
      }
      if (cg_tr.path.size() == g_tr.path.size()) {
        // All references were resolved.
        complex_trs_.push_back(cg_tr);
      }
    }

    // Sort complex_trs_ by trigger edge indexes.
    std::sort(
        complex_trs_.begin(), complex_trs_.end(),
        [](const ComplexTurnRestriction& a, const ComplexTurnRestriction& b) {
          return a.GetTriggerEdgeIdx() < b.GetTriggerEdgeIdx();
        });

    // Fill complex_tr_map_. It maps from trigger edge index to the position
    // of the first complex turn restriction in complex_trs_.
    size_t start = 0;
    for (size_t i = 0; i < complex_trs_.size(); ++i) {
      const ComplexTurnRestriction& tr = complex_trs_.at(i);
      CHECK_GT_S(tr.path.size(), 2);
      if (i == complex_trs_.size() - 1 ||
          complex_trs_.at(i).GetTriggerEdgeIdx() !=
              complex_trs_.at(i + 1).GetTriggerEdgeIdx()) {
        complex_tr_map_[complex_trs_.at(start).GetTriggerEdgeIdx()] = start;
        edges_.at(complex_trs_.at(start).GetTriggerEdgeIdx())
            .complex_tr_trigger = 1;
        start = i + 1;
      }
    }

    LOG_S(INFO) << complex_trs_.size() << " of " << graph_based_trs.size()
                << " complex turn restrictions added";
  }

  inline const std::vector<ComplexTurnRestriction>& GetComplexTRS() const {
    return complex_trs_;
  }

  inline const absl::flat_hash_map<uint32_t, uint32_t>& GetComplexTRMap()
      const {
    return complex_tr_map_;
  }

  // Log stats about the graph.
  void LogStats() const {
    uint32_t min_weight = std::numeric_limits<uint32_t>::max();
    uint32_t max_weight = 0;
    uint32_t restricted_access = 0;
    uint32_t uturn = 0;
    for (const PartialEdge& e : edges_) {
      if (e.weight < min_weight) min_weight = e.weight;
      if (e.weight > max_weight) max_weight = e.weight;
      restricted_access += e.restricted_access;
      uturn += e.uturn_allowed;
    }
    LOG_S(INFO) << absl::StrFormat(
        "CompactGraph #nodes:%u #edges:%u restricted:%u uturn:%u #turncosts:%u "
        "mem:%u weight=[%u,%u]",
        edges_start_.size() - 1, edges_.size(), restricted_access, uturn,
        turn_costs_.size(),
        edges_start_.size() * sizeof(uint32_t) +
            edges_.size() * sizeof(PartialEdge) + sizeof(CompactDirectedGraph),
        min_weight, max_weight);
  }

  void DebugPrint() const {
    LOG_S(INFO) << absl::StrFormat("Compact Graph %u nodes %u edges",
                                   num_nodes_, edges_.size());
    for (uint32_t node_idx = 0; node_idx < num_nodes_; ++node_idx) {
      for (size_t i = edges_start_.at(node_idx);
           i < edges_start_.at(node_idx + 1); ++i) {
        const PartialEdge& e = edges_.at(i);
        LOG_S(INFO) << absl::StrFormat(
            "  Edge %u to %u way_idx:%u ra:%u str:%u ctr:%u", node_idx,
            e.to_c_idx, e.way_idx, e.restricted_access, e.simple_tr_trigger,
            e.complex_tr_trigger);
      }
    }
  }

 private:
  // Nodes are numbered [0..num_nodes). For each one, we want to store
  // the start of its edges in the edge array.
  // In the end we add one more element with value full_edges.size(), to allow
  // easy iteration.
  void BuildGraph(const std::vector<FullEdge>& full_edges) {
    CHECK_S(edges_start_.empty());
    CHECK_LT_S(full_edges.size(), INFU32);

    edges_start_.reserve(num_nodes_ + 1);
    size_t current_start = 0;
    for (size_t c_idx = 0; c_idx < num_nodes_; ++c_idx) {
      edges_start_.push_back(current_start);
      // LOG_S(INFO) << absl::StrFormat("Cluster:%u node:%u edge_start:%u",
      //                                cluster.cluster_id, c_idx,
      //                                current_start);
      while (current_start < full_edges.size() &&
             full_edges.at(current_start).from_c_idx == c_idx) {
        current_start++;
      }
    }
    CHECK_EQ_S(current_start, full_edges.size());
    edges_start_.push_back(full_edges.size());  // Sentinel.

    edges_.reserve(full_edges.size());
    uint32_t full_restricted = 0;
    uint32_t partial_restricted = 0;
    for (const FullEdge& e : full_edges) {
      // LOG_S(INFO) << absl::StrFormat("Cluster:%u pos:%u from:%u to:%u
      // w:%d",
      //                                cluster.cluster_id, edges_.size(),
      //                                e.from_c_idx, e.to_c_idx,
      //                                e.weight);
      // edges_.push_back({e.to_c_idx, e.weight, e.restricted_access});
      edges_.push_back({.to_c_idx = e.to_c_idx,
                        .weight = e.weight,
                        .way_idx = e.way_idx,
                        .restricted_access = e.restricted_access,
                        .uturn_allowed = e.uturn_allowed,
                        .simple_tr_trigger = 0,
                        .complex_tr_trigger = 0});
      full_restricted += e.restricted_access;
      partial_restricted += edges_.back().restricted_access;
    }
    CHECK_EQ_S(num_nodes_ + 1, edges_start_.size());
  }

  bool ConvertGraphBasedTurnRestrictionEdge(
      const TurnRestriction::TREdge& tr_edge,
      const absl::flat_hash_map<uint32_t, uint32_t>& graph_to_compact_nodemap,
      TurnRestriction::TREdge* converted_edge) const {
    const auto iter_from = graph_to_compact_nodemap.find(tr_edge.from_node_idx);
    const auto iter_to = graph_to_compact_nodemap.find(tr_edge.to_node_idx);
    if (iter_from == graph_to_compact_nodemap.end() ||
        iter_to == graph_to_compact_nodemap.end()) {
      return false;
    }
    converted_edge->from_node_idx = iter_from->second,
    converted_edge->way_idx = tr_edge.way_idx,
    converted_edge->to_node_idx = iter_to->second;
    int64_t edge_idx =
        FindEdge(converted_edge->from_node_idx, converted_edge->to_node_idx,
                 converted_edge->way_idx);
    if (edge_idx < 0) {
      return false;
    }
    converted_edge->edge_idx = edge_idx;
    return true;
  }

  // Number of nodes in the graph.
  // The border nodes of the cluster are located at the beginning and have
  // ids 0..cluster.border_nodes.size()-1.
  const uint32_t num_nodes_;
  // Start position of the edges of each node. Has num_nodes_+1 elements, the
  // last element is a sentinel. The edges of node i are in the range
  // [edges_start_[i], edges_start_[i+1]).
  std::vector<uint32_t> edges_start_;
  std::vector<PartialEdge> edges_;

  // Simple turn restrictions (3 nodes involved). Key is the edge index of
  // the trigger edge. Value is the allowed offsets of outgoing edges at the
  // via node.
  // absl::flat_hash_map<uint32_t, uint32_t> simple_tr_map_;

  // Complex turn restrictions, involving more than 3 nodes.
  // The map is indexed by the first trigger edge index and contains an index
  // to the first complex turn restriction (starting at the trigger edge) in
  // the vector below.
  absl::flat_hash_map<uint32_t, uint32_t> complex_tr_map_;
  std::vector<ComplexTurnRestriction> complex_trs_;

  // Turn costs indexed by edges, see 'PartialEdge.turn_cost_idx'.
  std::vector<TurnCostData> turn_costs_;
};

inline bool operator<(const CompactDirectedGraph::FullEdge& a,
                      const CompactDirectedGraph::FullEdge& b) {
  return std::tie(a.from_c_idx, a.to_c_idx, a.restricted_access, a.weight) <
         std::tie(b.from_c_idx, b.to_c_idx, b.restricted_access, b.weight);
}

// Visit all reachable nodes using a BFS, starting from the nodes in
// 'routing_nodes', using 'opt' to limit expansion. Assigns a serial id=0..N-1
// to each visted node. The routing nodes are mapped to indices
// [0..deduped_routing_nodes_size-1] in the given order, ignoring duplicate
// nodes.
//
// Note: If a routing node is in a dead end (and opt.avoid_dead_end is true),
// then the specific bridge is allowed to be traversed in both directions.
// This way, the resulting graph contains all dead ends which contain routing
// nodes.
//
// 'undirected_expand': If 'false', then only forward edges are followed, i.e.
// only nodes reachable by forward routing from one of the start nodes will be
// considered. If 'true', then all nodes that are connected through forward
// and/or backward routable edges are allowed.
//
// Returns the number of visited nodes in 'num_nodes' and the edges between
// visited nodes in 'full_edges'.
//
// If 'graph_to_compact_nodemap' is not nullptr, then it will contain a
// mapping from indexes in graph.nodes to indexes in the compact graph
// [0..num_nodes-1].
inline void CollectEdgesForCompactGraph(
    const Graph& g, const RoutingMetric& metric, RoutingOptions opt,
    const std::vector<std::uint32_t>& routing_nodes, bool undirected_expand,
    uint32_t* num_nodes,
    std::vector<CompactDirectedGraph::FullEdge>* full_edges,
    absl::flat_hash_map<uint32_t, uint32_t>* graph_to_compact_nodemap =
        nullptr) {
  CHECK_S(!routing_nodes.empty());
  // Map from node index in g.nodes to the node index in the compact graph.
  absl::flat_hash_map<uint32_t, uint32_t> internal_nodemap;
  if (graph_to_compact_nodemap == nullptr) {
    graph_to_compact_nodemap = &internal_nodemap;
  } else {
    CHECK_S(graph_to_compact_nodemap->empty());
  }
  // FIFO queue for bfs, containing node indices in g.nodes.
  std::queue<uint32_t> q;

  // Preallocate ids for all (non-duplicate) nodes in routing_nodes.
  for (uint32_t node_idx : routing_nodes) {
    // Check for duplicates.
    if (!graph_to_compact_nodemap->contains(node_idx)) {
      /*
      LOG_S(INFO) << absl::StrFormat(
          "Add mapping node_idx %lld(%u) to c_idx %lld",
          GetGNodeIdSafe(g, node_idx), node_idx,
          graph_to_compact_nodemap->size());
          */
      (*graph_to_compact_nodemap)[node_idx] = graph_to_compact_nodemap->size();
      q.push(node_idx);
    }
  }
  const uint32_t deduped_routing_nodes_size = graph_to_compact_nodemap->size();

  // Special case, which handles routing nodes in dead-ends while
  // opt.avoid_dead_end is true. We find the dead-end bridge node
  // for the routing node and preallocate a compact index 'c_idx' for it. This
  // way they allocate c_idx values for these special bridges in a defined
  // range in the compact graph, which allows fast checks (see
  // max_bridge_c_idx below).
  if (opt.avoid_dead_end) {
    for (uint32_t node_idx : routing_nodes) {
      if (g.nodes.at(node_idx).dead_end) {
        CHECK_S(!opt.restrict_to_cluster)
            << "Restricted cluster not allowed when routing node in dead end "
            << g.nodes.at(node_idx).node_id;
        uint32_t bridge_idx;
        FindBridge(g, node_idx, nullptr, &bridge_idx);
        if (!graph_to_compact_nodemap->contains(bridge_idx)) {
          /*
          LOG_S(INFO) << absl::StrFormat(
              "Add mapping bridge_idx %lld(%u) to c_idx %lld",
              GetGNodeIdSafe(g, bridge_idx), bridge_idx,
              graph_to_compact_nodemap->size());
              */
          (*graph_to_compact_nodemap)[bridge_idx] =
              graph_to_compact_nodemap->size();
          q.push(bridge_idx);
        }
      }
    }
  }
  // The nodes in range [deduped_routing_nodes_size, max_bridge_c_idx] are
  // allowed bridges.
  const uint32_t max_bridge_c_idx = graph_to_compact_nodemap->size() - 1;

  // Allocate all the other nodes, i.e. do a BFS.
  uint32_t check_idx = 0;
  while (!q.empty()) {
    uint32_t node_idx = q.front();
    q.pop();
    const GNode& node = g.nodes.at(node_idx);
    // By construction, this number is strictly increasing, i.e. +1 in every
    // loop.
    uint32_t c_idx = (*graph_to_compact_nodemap)[node_idx];
    CHECK_EQ_S(c_idx, check_idx);
    check_idx++;

    // Check if this node is at a bridge that is allowed to traverse into the
    // dead end.
    if (c_idx <= max_bridge_c_idx && c_idx >= deduped_routing_nodes_size) {
      // This allows RoutingRejectEdge() below to traverse the bridge.
      opt.allow_bridge_node_idx = node_idx;
    }

    // Examine neighbours.
    // LOG_S(INFO) << "Expanding node " << GetGNodeIdSafe(g, node_idx);
    for (const GEdge& edge : gnode_forward_edges(g, node_idx)) {
      // LOG_S(INFO) << "Edge to " << GetGNodeIdSafe(g, edge.target_idx);

      const WaySharedAttrs& wsa = GetWSA(g, edge.way_idx);
      if (RoutingRejectEdge(g, opt, node, node_idx, edge, wsa,
                            EDGE_DIR(edge))) {
#if 0
        LOG_S(INFO) << "Reject edge from " << GetGNodeIdSafe(g, node_idx)
                    << " to " << GetGNodeIdSafe(g, edge.target_idx);
#endif
        continue;
      }

      uint32_t other_c_idx;
      auto iter = graph_to_compact_nodemap->find(edge.target_idx);
      if (iter == graph_to_compact_nodemap->end()) {
        // The node hasn't been seen before. This means we need to allocate a
        // new id, and we need to enqueue the node because it hasn't been
        // handled yet.
        /*
        LOG_S(INFO) << absl::StrFormat(
            "Add mapping node_idx %lld(%u) to c_idx %lld",
            GetGNodeIdSafe(g, edge.target_idx), edge.target_idx,
            graph_to_compact_nodemap->size());
            */
        other_c_idx = graph_to_compact_nodemap->size();
        (*graph_to_compact_nodemap)[edge.target_idx] =
            graph_to_compact_nodemap->size();
        q.push(edge.target_idx);
      } else {
        other_c_idx = iter->second;
      }

#if 0
      LOG_S(INFO) << "Add edge from " << GetGNodeIdSafe(g, node_idx) << " to "
                  << GetGNodeIdSafe(g, edge.target_idx);
#endif
      full_edges->push_back(
          {.from_c_idx = c_idx,
           .to_c_idx = other_c_idx,
           .weight = metric.Compute(wsa, opt.vt, EDGE_DIR(edge), edge),
           .way_idx = edge.way_idx,
           .restricted_access = edge.car_label != GEdge::LABEL_FREE,
           .uturn_allowed = edge.car_uturn_allowed});
    }

    // Expand in inverse direction if necessary.
    // Special case: Expand border nodes of clusters in inverse direction too,
    // because we need the incoming edges.
    //
    // TODO: This is actually too complicated. Move cluster edge collection
    // somewhere else.
    if (undirected_expand ||
        (opt.restrict_to_cluster && c_idx < routing_nodes.size())) {
      // Examine neighbours connected by backward edges and add them to the
      // queue if they haven't been seen yet.
      // LOG_S(INFO) << "Inverse Expanding node " << GetGNodeIdSafe(g,
      // node_idx);
      for (const GEdge& edge : gnode_inverted_edges(g, node_idx)) {
        // for (size_t i = node.num_edges_out; i < gnode_total_edges(node);
        // ++i) { const GEdge& edge = node.edges[i];
        // LOG_S(INFO) << "Edge to " << GetGNodeIdSafe(g, edge.target_idx);
        const WaySharedAttrs& wsa = GetWSA(g, edge.way_idx);
        if (RoutingRejectEdge(g, opt, node, node_idx, edge, wsa,
                              EDGE_DIR(edge))) {
          continue;
        }

        auto iter = graph_to_compact_nodemap->find(edge.target_idx);
        if (iter == graph_to_compact_nodemap->end()) {
          /*
          LOG_S(INFO) << absl::StrFormat(
              "Add mapping node_idx %lld(%u) to c_idx %lld",
              GetGNodeIdSafe(g, edge.target_idx), edge.target_idx,
              graph_to_compact_nodemap->size());
              */
          (*graph_to_compact_nodemap)[edge.target_idx] =
              graph_to_compact_nodemap->size();
          q.push(edge.target_idx);
        }
      }
    }
  }
  // TODO: graph_to_compact_nodemap->size() is sometimes smaller than
  // cluster.num_nodes. Investigate why this happens. Maybe some nodes in the
  // cluster are not reachable when using the directed graph - clustering is
  // done on the undirected graph.
  *num_nodes = graph_to_compact_nodemap->size();
  CHECK_LT_S(*num_nodes, 1 << 31) << "Not supported, Internal data has 31 bits";

  // Add all bridges for routing nodes.
}
