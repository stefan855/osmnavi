#pragma once

#include <vector>

#include "absl/container/flat_hash_map.h"
#include "base/util.h"
#include "graph/graph_def.h"
#include "osm/turn_restriction_defs.h"

// This is a compact, i.e. memory efficient view on the nodes and edges of a
// graph, with precomputed weights for every edge. The nodes in the graph have
// ids 0..num_nodes()-1.
class CompactDirectedGraph {
 public:
  struct FullEdge {
    uint32_t from_c_idx;
    uint32_t to_c_idx;
    uint32_t weight;
    uint32_t way_idx : WAY_IDX_BITS;  // 31 bits.
    // Edge has restricted car_label (LABEL_RESTRICTED or
    // LABEL_RESTRICTED_SECONDARY) set.
    uint8_t restricted_access : 1;
  };

  struct PartialEdge {
    uint32_t to_c_idx;
    uint32_t weight;
    uint32_t way_idx : WAY_IDX_BITS;  // 31 bits.
    // Edge has restricted car_label (LABEL_RESTRICTED or
    // LABEL_RESTRICTED_SECONDARY) set.
    uint8_t restricted_access : 1;
    uint8_t simple_turn_restriction_trigger : 1;
    uint8_t complex_turn_restriction_trigger : 1;
  };

  using CompactSimpleTurnRestrictionMap =
      absl::flat_hash_map<uint32_t, uint32_t>;

  // Create a graph with the given number of nodes and edges in 'full_edges'.
  // 'full_edges' must be sorted non-decreasing by (from_c_idx, to_c_idx,
  // way_idx, restricted_access), see SortAndCleanupEdges().
  //
  // Note: this file implements operator "<" for full edges, i.e. you can sort
  // the edges with std::sort(full_edges.begin(), full_edges.end());
  CompactDirectedGraph(uint32_t num_nodes,
                       const std::vector<FullEdge>& full_edges)
      : num_nodes_(num_nodes) {
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
    for (size_t i = 0; i < num_nodes; ++i) {
      CHECK_NE_S(node_refs.at(i), INFU32);
    }
    return node_refs;
  }

  // Log stats about the graph.
  void LogStats() const {
    uint32_t min_weight = std::numeric_limits<uint32_t>::max();
    uint32_t max_weight = 0;
    uint32_t restricted_access = 0;
    for (const PartialEdge& e : edges_) {
      if (e.weight < min_weight) min_weight = e.weight;
      if (e.weight > max_weight) max_weight = e.weight;
      restricted_access += e.restricted_access;
    }
    LOG_S(INFO) << absl::StrFormat(
        "CompactGraph #nodes:%u #edges:%u restricted:%u mem:%u weight=[%u,%u]",
        edges_start_.size() - 1, edges_.size(), restricted_access,
        edges_start_.size() * sizeof(uint32_t) +
            edges_.size() * sizeof(PartialEdge) + sizeof(CompactDirectedGraph),
        min_weight, max_weight);
  }

  // After creating the compact graph, add complex turn restrictions from the
  // standard Graph, which means that node references have to be converted to
  // reference the compact_graph.
  void AddComplexTurnRestrictions(
      const std::vector<TurnRestriction>& graph_based_trs,
      const absl::flat_hash_map<uint32_t, uint32_t>& graph_to_compact_nodemap) {
    for (const TurnRestriction& g_tr : graph_based_trs) {
      TurnRestriction cg_tr = {.relation_id = g_tr.relation_id,
                               .from_way_id = g_tr.from_way_id,
                               .via_ids = g_tr.via_ids,
                               .to_way_id = g_tr.to_way_id,
                               .via_is_node = g_tr.via_is_node,
                               .forbidden = g_tr.forbidden,
                               .direction = g_tr.direction};
      for (const TurnRestriction::TREdge& tr_edge : g_tr.path) {
        TurnRestriction::TREdge converted;
        if (!ConvertGraphBasedTurnRestrictionEdge(
                tr_edge, graph_to_compact_nodemap, &converted)) {
          break;
        }
        cg_tr.path.push_back(converted);
      }
      if (cg_tr.path.size() == g_tr.path.size()) {
        // All references were resolved.
        complex_turn_restrictions_.push_back(cg_tr);
      }
    }
    SortTurnRestrictions(&complex_turn_restrictions_);
    complex_turn_restriction_map_ = ComputeComplexTurnRestrictionMap(
        Verbosity::Trace, complex_turn_restrictions_);
    MarkComplexTriggerEdges();
  }

  // Add simple turn restrictions, transforming from Graph format to
  // CompactGraph format.
  void AddSimpleTurnRestrictions(
      const Graph& g,
      const SimpleTurnRestrictionMap& simple_turn_restriction_map,
      const absl::flat_hash_map<uint32_t, uint32_t>& graph_to_compact_nodemap) {
    CHECK_S(compact_simple_turn_restriction_map_.empty());
    for (const auto& [g_key, g_data] : simple_turn_restriction_map) {
      TurnRestriction::TREdge new_key;
      if (!ConvertGraphBasedTurnRestrictionEdge(g_key, graph_to_compact_nodemap,
                                                &new_key)) {
        continue;
      }
      const int64_t key_edge_idx =
          FindEdge(new_key.from_node_idx, new_key.to_node_idx, new_key.way_idx);
      if (key_edge_idx < 0) {
        continue;
      }

      uint32_t allowed_edge_bits = 0;
      uint32_t bits = g_data.allowed_edge_bits;
      uint64_t g_start = g.nodes.at(g_key.to_node_idx).edges_start_pos;
      uint64_t cg_start = edges_start_.at(new_key.to_node_idx);
      bool error = false;
      // Iterate edges starting at new_key.to_node_idx (via-node).
      // Iterates over the offsets of the allowed edges.
      while (!error && bits != 0) {
        uint32_t g_offset = __builtin_ctz(bits);
        bits = bits - (1u << g_offset);

        const GEdge& g_edge = g.edges.at(g_start + g_offset);
        const auto iter = graph_to_compact_nodemap.find(g_edge.other_node_idx);
        if (iter == graph_to_compact_nodemap.end()) {
          error = true;
          break;
        }
        // Now find this edge in the compact graph.
        const int64_t to_edge_idx =
            FindEdge(new_key.to_node_idx, iter->second, g_edge.way_idx);
        if (to_edge_idx < 0) {
          error = true;
          break;
        }

        // Encode offset into the new data;
        allowed_edge_bits |= (1u << (to_edge_idx - cg_start));
      }
      if (!error) {
        compact_simple_turn_restriction_map_[static_cast<uint32_t>(
            key_edge_idx)] = allowed_edge_bits;
        edges_.at(key_edge_idx).simple_turn_restriction_trigger = 1;
      }
    }
  }

  const CompactSimpleTurnRestrictionMap& GetCompactSimpleTurnRestrictionMap()
      const {
    return compact_simple_turn_restriction_map_;
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
      // LOG_S(INFO) << absl::StrFormat("Cluster:%u pos:%u from:%u to:%u w:%d",
      //                                cluster.cluster_id, edges_.size(),
      //                                e.from_c_idx, e.to_c_idx,
      //                                e.weight);
      // edges_.push_back({e.to_c_idx, e.weight, e.restricted_access});
      edges_.push_back({.to_c_idx = e.to_c_idx,
                        .weight = e.weight,
                        .way_idx = e.way_idx,
                        .restricted_access = e.restricted_access});
      full_restricted += e.restricted_access;
      partial_restricted += edges_.back().restricted_access;
    }
    CHECK_EQ_S(num_nodes_ + 1, edges_start_.size());
  }

  bool ConvertGraphBasedTurnRestrictionEdge(
      const TurnRestriction::TREdge& tr_edge,
      const absl::flat_hash_map<uint32_t, uint32_t>& graph_to_compact_nodemap,
      TurnRestriction::TREdge* converted_edge) {
    const auto iter_from = graph_to_compact_nodemap.find(tr_edge.from_node_idx);
    const auto iter_to = graph_to_compact_nodemap.find(tr_edge.to_node_idx);
    if (iter_from == graph_to_compact_nodemap.end() ||
        iter_to == graph_to_compact_nodemap.end()) {
      return false;
    }
    converted_edge->from_node_idx = iter_from->second,
    converted_edge->way_idx = tr_edge.way_idx,
    converted_edge->to_node_idx = iter_to->second;
    return true;
  }

  void MarkComplexTriggerEdges() {
#if 0
    for (const auto& [key, pos] : g->complex_turn_restriction_map) {
      gnode_find_edge(*g, key.from_node_idx, key.to_node_idx, key.way_idx)
          .complex_turn_restriction_trigger = 1;
    }
#endif
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
  // the trigger edge. Value is the allowed offsets of outgoing edges at the via
  // node.
  CompactSimpleTurnRestrictionMap compact_simple_turn_restriction_map_;

  // Complex turn restrictions, involving more than 3 nodes.
  // The map is indexed by the first trigger edge and contains an index to the
  // first complex turn restriction in the vector below.
  ComplexTurnRestrictionMap complex_turn_restriction_map_;
  std::vector<TurnRestriction> complex_turn_restrictions_;
};

inline bool operator<(const CompactDirectedGraph::FullEdge& a,
                      const CompactDirectedGraph::FullEdge& b) {
  return std::tie(a.from_c_idx, a.to_c_idx, a.restricted_access, a.weight) <
         std::tie(b.from_c_idx, b.to_c_idx, b.restricted_access, b.weight);
}

// Visit all reachable nodes using a BFS, starting from the nodes in
// 'start_nodes', using 'opt' to limit expansion. Assigns a serial id=0..N-1 to
// each node. The start nodes are mapped to indices [0..start_node.size()-1] in
// the given order.
//
// 'undirected_expand': If 'false', then only forward edges are followed, i.e.
// only nodes reachable by forward routing from one of the start nodes will be
// considered. If 'true', then all nodes that are connected through forward
// and/or backward routable edges are allowed.
//
// Returns the number of visited nodes in 'num_nodes' and the edges between
// visited nodes in 'full_edges'.
//
// If 'graph_to_compact_nodemap' is not nullptr, then it will contain a mapping
// from indexes in graph.nodes to indexes in the compact graph [0..num_nodes-1].
inline void CollectEdgesForCompactGraph(
    const Graph& g, const RoutingMetric& metric, const RoutingOptions& opt,
    const std::vector<std::uint32_t>& start_nodes, bool undirected_expand,
    uint32_t* num_nodes,
    std::vector<CompactDirectedGraph::FullEdge>* full_edges,
    absl::flat_hash_map<uint32_t, uint32_t>* graph_to_compact_nodemap =
        nullptr) {
  // Map from node index in g.nodes to the node index in the compact graph.
  absl::flat_hash_map<uint32_t, uint32_t> internal_nodemap;
  if (graph_to_compact_nodemap == nullptr) {
    graph_to_compact_nodemap = &internal_nodemap;
  } else {
    CHECK_S(graph_to_compact_nodemap->empty());
  }
  // FIFO queue for bfs, containing node indices in g.nodes.
  std::queue<uint32_t> q;

  // Preallocate ids for all start nodes.
  for (uint32_t pos = 0; pos < start_nodes.size(); ++pos) {
    uint32_t node_idx = start_nodes.at(pos);
    (*graph_to_compact_nodemap)[node_idx] = pos;
    q.push(node_idx);
  }

  // Do a BFS.
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

    // Examine neighbours.
    for (const GEdge& edge : gnode_forward_edges(g, node_idx)) {
      // for (size_t i = 0; i < node.num_edges_out; ++i) {
      // const GEdge& edge = node.edges[i];
      const WaySharedAttrs& wsa = GetWSA(g, edge.way_idx);
      if (RoutingRejectEdge(g, opt, node, node_idx, edge, wsa,
                            EDGE_DIR(edge))) {
        continue;
      }

      uint32_t other_c_idx;
      auto iter = graph_to_compact_nodemap->find(edge.other_node_idx);
      if (iter == graph_to_compact_nodemap->end()) {
        // The node hasn't been seen before. This means we need to allocate a
        // new id, and we need to enqueue the node because it hasn't been
        // handled yet.
        other_c_idx = graph_to_compact_nodemap->size();
        (*graph_to_compact_nodemap)[edge.other_node_idx] =
            graph_to_compact_nodemap->size();
        q.push(edge.other_node_idx);
      } else {
        other_c_idx = iter->second;
      }

      full_edges->push_back(
          {.from_c_idx = c_idx,
           .to_c_idx = other_c_idx,
           .weight = metric.Compute(wsa, opt.vt, EDGE_DIR(edge), edge),
           .way_idx = edge.way_idx,
           .restricted_access = edge.car_label != GEdge::LABEL_FREE});
    }

    if (undirected_expand) {
      // Examine neighbours connected by backward edges and add them to the
      // queue if they haven't been seen yet.
      for (const GEdge& edge : gnode_inverted_edges(g, node_idx)) {
        // for (size_t i = node.num_edges_out; i < gnode_total_edges(node); ++i)
        // { const GEdge& edge = node.edges[i];
        const WaySharedAttrs& wsa = GetWSA(g, edge.way_idx);
        if (RoutingRejectEdge(g, opt, node, node_idx, edge, wsa,
                              EDGE_INVERSE_DIR(edge))) {
          continue;
        }

        auto iter = graph_to_compact_nodemap->find(edge.other_node_idx);
        if (iter == graph_to_compact_nodemap->end()) {
          (*graph_to_compact_nodemap)[edge.other_node_idx] =
              graph_to_compact_nodemap->size();
          q.push(edge.other_node_idx);
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
}
